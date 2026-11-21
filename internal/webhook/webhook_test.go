package webhook

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
)

// run starts the worker on a background context that outlives the test. It
// is deliberately never cancelled: a request whose response the client is
// still reading when the *test* returns is exactly the "delivery already
// reached the peer, client still inside Do()" case, and cancelling the very
// context passed to that in-flight request would turn a real success into a
// misleading "context canceled" failure — a test-harness race, not anything
// Notifier.Run itself needs to guard against. The worker goroutine just
// blocks forever on an empty channel once the test's httptest server is
// closed, which costs nothing but a parked goroutine for the life of the
// test binary.
func run(n *Notifier) {
	go n.Run(context.Background())
}

// TestEventNeverBlocks proves the binding property (spec §11): Event never
// blocks regardless of queue depth OR worker absence. In production it is
// called concurrently from the loop goroutine and several executor
// goroutines, so this drives it from 8 goroutines at once — with no Run()
// worker ever consuming — rather than from a single caller, which would
// only prove the queue's own select+default returns and would leave the
// drop counter's atomic.Int64 untested under -race.
func TestEventNeverBlocks(t *testing.T) {
	n := New(config.WebhookConfig{Promote: "https://example.invalid/promote"}, clock.NewFake())

	const goroutines = 8
	const perGoroutine = 125 // 8 * 125 = 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				n.Event("promote", map[string]any{"n": i})
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Event blocked with no Run() worker consuming and a full queue")
	}
}

func TestDeliveryPostsJSONToTheRightURL(t *testing.T) {
	received := make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding posted body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		received <- body
	}))
	defer srv.Close()

	n := New(config.WebhookConfig{Promote: srv.URL}, clock.NewFake())
	run(n)

	payload := map[string]any{
		"host":    "host-a",
		"service": "web",
		"event":   "promote",
		"reason":  "lease granted",
		"lease":   "active",
		"wall":    1_700_000_000.0,
	}
	n.Event("promote", payload)

	select {
	case got := <-received:
		want, _ := json.Marshal(payload)
		gotJSON, _ := json.Marshal(got)
		if string(gotJSON) != string(want) {
			t.Fatalf("posted body = %s, want %s", gotJSON, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery to the promote URL")
	}
}

func TestFailedDeliveryIsLoggedNotRetried(t *testing.T) {
	var failCount atomic.Int64
	failHit := make(chan struct{}, 1)
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		failHit <- struct{}{}
	}))
	defer failSrv.Close()

	nextHit := make(chan struct{}, 1)
	nextSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		nextHit <- struct{}{}
	}))
	defer nextSrv.Close()

	n := New(config.WebhookConfig{Demote: failSrv.URL, Reclaim: nextSrv.URL}, clock.NewFake())
	run(n)

	n.Event("demote", map[string]any{"event": "demote"})
	select {
	case <-failHit:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the failing delivery")
	}

	// The worker is single and serial: once a second, differently-routed
	// event has been delivered, the worker has moved past the failed one
	// without requeueing it — proving no retry happened, without sleeping.
	n.Event("reclaim", map[string]any{"event": "reclaim"})
	select {
	case <-nextHit:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second delivery")
	}

	if got := failCount.Load(); got != 1 {
		t.Fatalf("failing URL got %d requests, want exactly 1 (a failed delivery must never be retried)", got)
	}
}

func TestUnconfiguredKindIsInert(t *testing.T) {
	n := New(config.WebhookConfig{Promote: "https://example.invalid/promote"}, clock.NewFake())

	// demote has no configured URL: Event must not panic, and must not even
	// enqueue anything (there is nowhere for it to go and nothing to log).
	n.Event("demote", map[string]any{"event": "demote"})

	if got := len(n.queue); got != 0 {
		t.Fatalf("queue has %d items after an unconfigured-kind Event, want 0", got)
	}
}

// recordingHandler captures every log record the notifier emits, so the
// latch's "once per streak" can be asserted on the log itself rather than on
// a counter the production code would only carry for a test's benefit.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// count is how many records of this level name kind, matched on the "kind"
// attribute the latch always carries rather than on the message text.
func (h *recordingHandler) count(level slog.Level, kind string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level != level {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "kind" && a.Value.String() == kind {
				n++
			}
			return true
		})
	}
	return n
}

// messages flattens every record — its message and every attribute — into one
// string, for a test that asserts a value does NOT appear anywhere in the log.
func (h *recordingHandler) messages() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	for _, r := range h.records {
		b.WriteString(r.Message)
		r.Attrs(func(a slog.Attr) bool {
			b.WriteString(" ")
			b.WriteString(a.Key)
			b.WriteString("=")
			b.WriteString(a.Value.String())
			return true
		})
		b.WriteString("\n")
	}
	return b.String()
}

// capture points a notifier's logger at a recording handler. New hardcodes
// slog.Default(), and this test lives in the same package, so the field is
// assigned directly rather than growing an option nothing in production would
// ever set.
func capture(n *Notifier) *recordingHandler {
	h := &recordingHandler{}
	n.log = slog.New(h)
	return h
}

func TestRepeatedDeliveryFailuresWarnOncePerStreak(t *testing.T) {
	var status atomic.Int64
	status.Store(http.StatusInternalServerError)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	// The sentinel is how this test knows a delivery has been LOGGED, not
	// merely received: the worker is single and serial, so once a later
	// event's request arrives, every earlier delivery has returned — logging
	// included. Waiting on the failing server's own request instead would
	// race the log line that follows it.
	sentinel := make(chan struct{}, 1)
	sentinelSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		sentinel <- struct{}{}
	}))
	defer sentinelSrv.Close()

	n := New(config.WebhookConfig{Demote: srv.URL, Reclaim: sentinelSrv.URL}, clock.NewFake())
	handler := capture(n)
	run(n)

	drain := func(what string) {
		t.Helper()
		n.Event("reclaim", map[string]any{"sentinel": what})
		select {
		case <-sentinel:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out draining the queue after %s", what)
		}
	}

	// Three consecutive failures for one kind: one warning, not three.
	for i := 0; i < 3; i++ {
		n.Event("demote", map[string]any{"n": i})
	}
	drain("three failures")
	if got := handler.count(slog.LevelWarn, "demote"); got != 1 {
		t.Fatalf("three consecutive failures produced %d warnings, want 1", got)
	}

	// The receiver comes back: the recovery is said once, at Info.
	status.Store(http.StatusOK)
	n.Event("demote", map[string]any{"n": "recovery"})
	drain("the recovery")
	if got := handler.count(slog.LevelInfo, "demote"); got != 1 {
		t.Fatalf("a recovered endpoint logged %d Info records, want 1", got)
	}

	// A further success says nothing more — the streak is over, not ongoing.
	n.Event("demote", map[string]any{"n": "still fine"})
	drain("a second success")
	if got := handler.count(slog.LevelInfo, "demote"); got != 1 {
		t.Fatalf("a healthy endpoint logged %d Info records, want still 1", got)
	}

	// And it goes down again: a new streak is news again.
	status.Store(http.StatusInternalServerError)
	n.Event("demote", map[string]any{"n": "down again"})
	drain("the second streak")
	if got := handler.count(slog.LevelWarn, "demote"); got != 2 {
		t.Fatalf("a second failure streak produced %d warnings in total, want 2", got)
	}
}

func TestTheFailureLatchIsPerKind(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sentinel := make(chan struct{}, 1)
	sentinelSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		sentinel <- struct{}{}
	}))
	defer sentinelSrv.Close()

	// Two kinds pointed at the same dead receiver: each reports its own first
	// failure, and neither kind's latch silences the other.
	n := New(config.WebhookConfig{
		Promote: srv.URL + "/promote",
		Demote:  srv.URL + "/demote",
		Reclaim: sentinelSrv.URL,
	}, clock.NewFake())
	handler := capture(n)
	run(n)

	for i := 0; i < 2; i++ {
		n.Event("promote", map[string]any{"n": i})
		n.Event("demote", map[string]any{"n": i})
	}
	n.Event("reclaim", map[string]any{"sentinel": "two kinds, two failures each"})
	select {
	case <-sentinel:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out draining the queue")
	}

	if got := handler.count(slog.LevelWarn, "promote"); got != 1 {
		t.Fatalf("promote warned %d times, want 1", got)
	}
	if got := handler.count(slog.LevelWarn, "demote"); got != 1 {
		t.Fatalf("demote warned %d times, want 1", got)
	}
}

func TestFingerprintMismatchRoutesToAlertURL(t *testing.T) {
	received := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		received <- struct{}{}
	}))
	defer srv.Close()

	// fingerprint_mismatch has no config field of its own (spec §11): it is
	// one of the alert-class events and must land on the alert URL.
	n := New(config.WebhookConfig{Alert: srv.URL}, clock.NewFake())
	run(n)

	n.Event("fingerprint_mismatch", map[string]any{"peer": "host-b"})

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fingerprint_mismatch to reach the alert URL")
	}
}

// A webhook URL may embed a bearer secret in its path — Slack, Discord and
// Teams receivers all do — so no log line, on a failed delivery or on the
// recovery from one, may echo it. The host stays, because that is what an
// operator debugs with; the path does not.
func TestWebhookURLsAreRedactedInLogs(t *testing.T) {
	// A receiver path shaped like a Slack incoming-webhook token.
	const secretPath = "/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"

	status := atomic.Int64{}
	status.Store(http.StatusInternalServerError)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	sentinel := make(chan struct{}, 1)
	sentinelSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		sentinel <- struct{}{}
	}))
	defer sentinelSrv.Close()

	n := New(config.WebhookConfig{Demote: srv.URL + secretPath, Reclaim: sentinelSrv.URL}, clock.NewFake())
	handler := capture(n)
	run(n)

	// The worker is single and serial, so once the sentinel's request arrives,
	// the demote delivery before it — its log line included — has completed.
	drain := func(what string) {
		t.Helper()
		n.Event("reclaim", map[string]any{"sentinel": what})
		select {
		case <-sentinel:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out draining the queue after %s", what)
		}
	}

	n.Event("demote", map[string]any{"n": 1}) // a failure: Warn path
	drain("the failure")
	status.Store(http.StatusOK)
	n.Event("demote", map[string]any{"n": 2}) // the recovery: Info path
	drain("the recovery")

	joined := handler.messages()
	if strings.Contains(joined, secretPath) {
		t.Fatalf("a webhook secret path reached the log:\n%s", joined)
	}
	if !strings.Contains(joined, srv.URL) {
		t.Fatalf("the redacted host must still be logged for debugging:\n%s", joined)
	}
}
