// Package webhook is conch's best-effort, off-loop notifier (spec §11). It
// posts one event's payload, verbatim, as JSON to the URL configured for
// that event's kind. Delivery is bounded to 5s and handled entirely by a
// single background worker goroutine, so a slow, dead, or misbehaving
// receiver can never touch the loop that is trying to report on it: the
// incident a webhook reports must not be worsened by the reporting.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
)

// queueDepth bounds how many undelivered events Notifier holds before it
// starts dropping. It only needs to smooth over ordinary delivery latency,
// not survive a receiver that is down for good — a receiver down for good is
// exactly what the drop counter and its periodic log line are for.
const queueDepth = 64

// deliveryTimeout is spec §11's per-attempt bound.
const deliveryTimeout = 5 * time.Second

// post is one queued delivery: the event's kind, the URL that kind resolved
// to, and the payload to send. The kind travels with the post only so the
// worker can latch its warnings per kind (see failing) — routing was already
// done by the time a post exists.
type post struct {
	kind    string
	url     string
	payload map[string]any
}

// Notifier routes event kinds to per-kind webhook URLs and delivers them
// off the caller's goroutine. The zero value is not usable; build one with
// New.
type Notifier struct {
	urls   map[string]string
	client *http.Client
	queue  chan post
	clk    clock.Clock
	log    *slog.Logger

	dropped atomic.Int64

	// failing is the per-KIND delivery-failure latch: true while that kind's
	// consecutive failures are still running. A receiver that is down fails
	// every delivery to it, and a demotion storm can queue a dozen events in
	// a second, so warning per failed delivery turns one dead endpoint into a
	// log flood about a condition that has not changed since the first line.
	// The first failure of a streak warns, the rest are silent, and the
	// delivery that succeeds again logs the recovery at Info — so the log
	// says when the endpoint went away and when it came back, once each.
	//
	// Per kind, not per URL, and the difference is visible: `alert` and
	// `fingerprint_mismatch` share one configured URL (see New), so one dead
	// alert endpoint warns twice, once under each kind. That is deliberate —
	// a kind is what an operator configures and reads — and it is two lines,
	// not two per burst.
	//
	// No lock, and none is needed: only deliver touches this map, deliver is
	// only called from Run, and Run is the single worker goroutine (its own
	// doc comment is the guarantee). Event must never touch it — that is what
	// keeps the never-blocking path free of a mutex a slow receiver could
	// hold.
	failing map[string]bool
}

// New builds a Notifier from a host's configured per-kind webhook URLs. An
// empty URL for a kind means that kind is silently dropped (spec §11).
//
// clk is accepted for symmetry with the rest of the supervisor's time
// discipline (spec §12: time is injected everywhere) but is not read here —
// the wall timestamp in every payload comes from the caller, which already
// holds the clock this notifier would otherwise have to duplicate.
func New(cfg config.WebhookConfig, clk clock.Clock) *Notifier {
	return &Notifier{
		urls: map[string]string{
			"promote":  cfg.Promote,
			"demote":   cfg.Demote,
			"evacuate": cfg.Evacuate,
			"reclaim":  cfg.Reclaim,
			// alert is every notification with no lease transition of its
			// own to route on: decide's Alert action, an unmeasurable disk
			// floor, a wedged-daemon stop escalation, and a cross-host
			// fingerprint mismatch all share the one "yell loudly" URL
			// (spec §11).
			"alert":                cfg.Alert,
			"fingerprint_mismatch": cfg.Alert,
		},
		client:  &http.Client{Timeout: deliveryTimeout},
		queue:   make(chan post, queueDepth),
		clk:     clk,
		log:     slog.Default(),
		failing: map[string]bool{},
	}
}

// Event queues kind's payload for delivery. It never blocks, regardless of
// queue depth or whether Run's worker is even running: an unconfigured kind
// returns immediately without touching the queue, and a full queue drops the
// event and counts it rather than waiting for room. Drops are logged
// sampled, not per-drop, so a receiver that is down for good cannot turn
// into a log-spam incident of its own.
func (n *Notifier) Event(kind string, payload map[string]any) {
	url := n.urls[kind]
	if url == "" {
		return
	}
	select {
	case n.queue <- post{kind: kind, url: url, payload: payload}:
	default:
		d := n.dropped.Add(1)
		if d == 1 || d%100 == 0 {
			n.log.Warn("webhook queue full: dropping events, the receiver is not keeping up",
				"kind", kind, "dropped_total", d)
		}
	}
}

// Run delivers queued events one at a time until ctx is done. It is the
// only goroutine that ever touches the network on this notifier's behalf,
// so delivery is always serial and always off every other goroutine's
// critical path.
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-n.queue:
			n.deliver(ctx, p)
		}
	}
}

// deliver makes exactly one attempt. A failure — building the request,
// reaching the receiver, or a non-2xx response — is logged and dropped:
// spec §11 is explicit that a failed delivery is never retried into a
// stall. The delivery behaviour is untouched by the latch below; only how
// often a run of identical failures is reported changes.
func (n *Notifier) deliver(ctx context.Context, p post) {
	// The two Error paths below are deliberately NOT latched. A payload that
	// will not marshal is a conch bug and a URL that will not parse is a
	// config error: neither is the burst of a receiver being down, both are
	// permanent until a human acts, and both are rare enough per event that
	// silencing them would only hide the one line that names the cause.
	body, err := json.Marshal(p.payload)
	if err != nil {
		n.log.Error("webhook payload could not be marshalled: the event is lost, and this is a conch bug",
			"url", p.url, "error", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		n.log.Error("webhook request could not be built: check this url in the config", "url", p.url, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		n.failed(p.kind, "webhook delivery failed: the event is dropped, never retried; check this url",
			"url", p.url, "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort drain to let the connection be reused

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		n.failed(p.kind, "the webhook receiver rejected the event: check this url's own logs",
			"url", p.url, "status", resp.StatusCode)
		return
	}
	// Only a 2xx ends a streak. A receiver answering 500 is as down, from
	// conch's side, as one that refuses the connection.
	n.succeeded(p.kind, p.url)
}

// failed reports one kind's failed delivery, at most once per streak of
// them. args are appended to the log line as key/value pairs, so each caller
// keeps its own detail.
func (n *Notifier) failed(kind, msg string, args ...any) {
	if n.failing[kind] {
		return
	}
	n.failing[kind] = true
	n.log.Warn(msg, append([]any{"kind", kind}, args...)...)
}

// succeeded closes a kind's failure streak. It logs only when there was one
// to close: an endpoint that has been fine all along has nothing to recover
// from, and this runs on every successful delivery.
func (n *Notifier) succeeded(kind, url string) {
	if !n.failing[kind] {
		return
	}
	delete(n.failing, kind)
	n.log.Info("webhook delivery is working again", "kind", kind, "url", url)
}
