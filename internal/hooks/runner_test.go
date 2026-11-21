package hooks_test

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/dockerapi"
	"github.com/scytherswings/conch/internal/hooks"
)

// discardLogger is what every test in this file builds a Runner with except
// the ones below that assert on the hook start/finish narration itself —
// those supply a capturingLogHandler instead.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// logRecord is one captured slog record, flattened to key/value pairs for
// simple assertions.
type logRecord struct {
	msg   string
	attrs map[string]string
}

// capturingLogHandler is this package's own minimal slog.Handler, recording
// every record passed to it. It is deliberately NOT internal/supervisor's
// recordingLogHandler: reaching into another package's test internals from
// here would be a reverse dependency this package must not carry.
type capturingLogHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *capturingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, logRecord{msg: r.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h *capturingLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *capturingLogHandler) WithGroup(name string) slog.Handler       { return h }

// of collects every record with exactly this message, in the order Handle
// saw them — hook start/finish messages are fixed strings, not prefixes, so
// unlike the supervisor package's equivalent there is no wording-drift
// concern to hedge against with a prefix match.
func (h *capturingLogHandler) of(msg string) []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []logRecord
	for _, r := range h.records {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// fakeDocker is a recording fake of the narrow docker interface hooks.Runner
// consumes. Every call is logged as "<verb> <name>" (e.g. "create
// conch-hook-web-restore") in call order, so tests can assert both that a
// call happened and where it fell relative to the others.
type fakeDocker struct {
	mu    sync.Mutex
	calls []string

	created []dockerapi.RunSpec

	startErr  map[string]error
	createErr map[string]error
	removeErr map[string]error
	waitExit  map[string]int
	waitErr   map[string]error
	logsOut   map[string]string

	orphans []string

	// waitBlockUntil, if set for a name, makes Wait(name, ...) block until
	// the channel is closed — used to hold a hook "running" so a concurrent
	// Abort has something to kill.
	waitBlockUntil map[string]chan struct{}
	// waitStarted is signalled (non-blocking, buffered) the first time Wait
	// is called, so a test can synchronize on "Run has actually reached
	// Wait" instead of polling or sleeping.
	waitStarted chan struct{}
	// waitHits carries the NAME of every hook that reached Wait, which is
	// what a test driving two chains at once has to synchronize on: one
	// shared "something started" signal cannot tell them apart.
	waitHits chan string

	// clk and waitAdvance exist for the one test that checks the finish
	// log's duration_seconds is a real elapsed reading rather than a
	// constant: if clk is set, Wait(name, ...) advances it by
	// waitAdvance[name] before returning, standing in for the hook's own
	// running time on a clock the test controls.
	clk         *clock.Fake
	waitAdvance map[string]float64
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		startErr:       map[string]error{},
		createErr:      map[string]error{},
		removeErr:      map[string]error{},
		waitExit:       map[string]int{},
		waitErr:        map[string]error{},
		logsOut:        map[string]string{},
		waitBlockUntil: map[string]chan struct{}{},
		waitStarted:    make(chan struct{}, 1),
		waitHits:       make(chan string, 16),
	}
}

func (f *fakeDocker) record(s string) {
	f.mu.Lock()
	f.calls = append(f.calls, s)
	f.mu.Unlock()
}

func (f *fakeDocker) Create(spec dockerapi.RunSpec) error {
	f.record("create " + spec.Name)
	f.mu.Lock()
	f.created = append(f.created, spec)
	err := f.createErr[spec.Name]
	f.mu.Unlock()
	return err
}

// Start always reports a transition: a hook container is created one line
// before it is started, so the already-running 304 dockerapi's bool exists
// for is not a state this sequence can reach.
func (f *fakeDocker) Start(name string) (bool, error) {
	f.record("start " + name)
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.startErr[name]
	return err == nil, err
}

func (f *fakeDocker) Wait(name string, timeoutSeconds float64) (int, error) {
	f.record("wait " + name)

	select {
	case f.waitStarted <- struct{}{}:
	default:
	}
	select {
	case f.waitHits <- name:
	default:
	}

	f.mu.Lock()
	block := f.waitBlockUntil[name]
	f.mu.Unlock()
	if block != nil {
		<-block
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clk != nil {
		if d, ok := f.waitAdvance[name]; ok {
			f.clk.Advance(d)
		}
	}
	if err, ok := f.waitErr[name]; ok {
		return 0, err
	}
	return f.waitExit[name], nil
}

func (f *fakeDocker) Logs(name string, tail int) string {
	f.record("logs " + name)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logsOut[name]
}

func (f *fakeDocker) Remove(name string) error {
	f.record("remove " + name)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.removeErr[name]
}

func (f *fakeDocker) Kill(name string) error {
	f.record("kill " + name)
	return nil
}

func (f *fakeDocker) ListByLabel(key, value string) []string {
	f.record(fmt.Sprintf("list %s=%s", key, value))
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.orphans
}

// awaitWait blocks until every named hook has reached Wait. Names are
// collected into a set rather than matched one at a time, because two chains
// running at once reach Wait in whatever order the scheduler picks.
func (f *fakeDocker) awaitWait(t *testing.T, names ...string) {
	t.Helper()
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	deadline := time.After(5 * time.Second)
	for len(want) > 0 {
		select {
		case got := <-f.waitHits:
			delete(want, got)
		case <-deadline:
			t.Fatalf("these hooks never reached Wait: %v (calls: %v)", want, f.callLog())
		}
	}
}

func (f *fakeDocker) hasCall(s string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == s {
			return true
		}
	}
	return false
}

func (f *fakeDocker) countCalls(s string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == s {
			n++
		}
	}
	return n
}

func (f *fakeDocker) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func TestHooksRunInOrderAndStopAtFirstFailure(t *testing.T) {
	f := newFakeDocker()
	f.waitExit["conch-hook-web-restore"] = 1
	f.logsOut["conch-hook-web-restore"] = "restore: cannot find backup"

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{
		{Name: "restore", Image: "img", Timeout: 5},
		{Name: "verify", Image: "img", Timeout: 5},
	})

	if result.OK {
		t.Fatal("want OK false")
	}
	if result.FailedHook != "restore" {
		t.Fatalf("want FailedHook restore, got %q", result.FailedHook)
	}
	if !strings.Contains(result.Detail, "restore: cannot find backup") {
		t.Fatalf("want Detail to contain the fake's log output, got %q", result.Detail)
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the second hook must never be created once the first failed")
	}
}

func TestHookContainerNamingAndLabel(t *testing.T) {
	f := newFakeDocker()
	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())

	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})
	if !result.OK {
		t.Fatalf("want success, got %+v", result)
	}

	if len(f.created) != 1 {
		t.Fatalf("want 1 create call, got %d", len(f.created))
	}
	spec := f.created[0]
	if spec.Name != "conch-hook-web-restore" {
		t.Fatalf("want name conch-hook-web-restore, got %q", spec.Name)
	}
	if spec.Labels["conch.hook"] != "true" {
		t.Fatalf("want label conch.hook=true, got %q", spec.Labels["conch.hook"])
	}
	if spec.Labels["conch.service"] != "web" {
		t.Fatalf("want label conch.service=web, got %q", spec.Labels["conch.service"])
	}
}

func TestFailedHookContainerIsKeptForReading(t *testing.T) {
	f := newFakeDocker()
	f.waitExit["conch-hook-web-restore"] = 1
	f.logsOut["conch-hook-web-restore"] = "boom"

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})
	if result.OK {
		t.Fatal("want failure")
	}

	// Exactly one Remove call is expected: the remove-if-exists step that
	// runs BEFORE create. There must be no second Remove after the failed
	// Wait/Logs — that container's logs have to survive for the operator,
	// cleaned up only by the NEXT run's own remove-if-exists.
	if n := f.countCalls("remove conch-hook-web-restore"); n != 1 {
		t.Fatalf("want exactly 1 remove call (the pre-create one), got %d: %v", n, f.callLog())
	}
}

func TestTimeoutKillsAndFails(t *testing.T) {
	f := newFakeDocker()
	f.waitErr["conch-hook-web-restore"] = fmt.Errorf("context deadline exceeded")

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})

	if result.OK {
		t.Fatal("want failure")
	}
	if !f.hasCall("kill conch-hook-web-restore") {
		t.Fatal("want Kill called on a Wait timeout/error")
	}
	if !strings.Contains(strings.ToLower(result.Detail), "timeout") {
		t.Fatalf("want Detail to mention timeout, got %q", result.Detail)
	}
	// Kept for reading: no Remove beyond the pre-create remove-if-exists.
	if n := f.countCalls("remove conch-hook-web-restore"); n != 1 {
		t.Fatalf("want exactly 1 remove call, got %d: %v", n, f.callLog())
	}
}

func TestSuccessfulHooksAreRemoved(t *testing.T) {
	f := newFakeDocker()
	f.waitExit["conch-hook-web-restore"] = 0
	f.logsOut["conch-hook-web-restore"] = "ok"

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})
	if !result.OK {
		t.Fatalf("want success, got %+v", result)
	}

	want := []string{
		"remove conch-hook-web-restore", // remove-if-exists
		"create conch-hook-web-restore",
		"start conch-hook-web-restore",
		"wait conch-hook-web-restore",
		"logs conch-hook-web-restore",
		"remove conch-hook-web-restore", // confirmed-success cleanup
	}
	if got := f.callLog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got call order %v, want %v", got, want)
	}
}

// TestHooksRunAllInOrderOnFullSuccess pins the "in order" half of Run's
// contract for the all-success case, not just the stop-at-first-failure
// half: a chain of two passing hooks must run BOTH, restore fully before
// verify starts. A specs[:1]-style bug (or a stray early return after the
// first hook) would still pass every other test in this file — this is the
// one that catches it.
func TestHooksRunAllInOrderOnFullSuccess(t *testing.T) {
	f := newFakeDocker()
	f.waitExit["conch-hook-web-restore"] = 0
	f.waitExit["conch-hook-web-verify"] = 0

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{
		{Name: "restore", Image: "img", Timeout: 5},
		{Name: "verify", Image: "img", Timeout: 5},
	})
	if !result.OK {
		t.Fatalf("want success, got %+v", result)
	}

	want := []string{
		"remove conch-hook-web-restore", "create conch-hook-web-restore", "start conch-hook-web-restore",
		"wait conch-hook-web-restore", "logs conch-hook-web-restore", "remove conch-hook-web-restore",
		"remove conch-hook-web-verify", "create conch-hook-web-verify", "start conch-hook-web-verify",
		"wait conch-hook-web-verify", "logs conch-hook-web-verify", "remove conch-hook-web-verify",
	}
	if got := f.callLog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got call order %v, want %v", got, want)
	}
}

func TestCreateFailureStopsTheChain(t *testing.T) {
	f := newFakeDocker()
	f.createErr["conch-hook-web-restore"] = fmt.Errorf("no such image")

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{
		{Name: "restore", Image: "img", Timeout: 5},
		{Name: "verify", Image: "img", Timeout: 5},
	})
	if result.OK {
		t.Fatal("want failure")
	}
	if result.FailedHook != "restore" {
		t.Fatalf("want FailedHook restore, got %q", result.FailedHook)
	}
	if !strings.Contains(result.Detail, "no such image") {
		t.Fatalf("want Detail to carry the create error, got %q", result.Detail)
	}
	if f.hasCall("start conch-hook-web-restore") {
		t.Fatal("Start must not be called when Create failed")
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the chain must stop, not skip ahead to the next hook")
	}
}

func TestStartFailureStopsTheChain(t *testing.T) {
	f := newFakeDocker()
	f.startErr["conch-hook-web-restore"] = fmt.Errorf("cannot join network")

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})
	if result.OK {
		t.Fatal("want failure")
	}
	if result.FailedHook != "restore" {
		t.Fatalf("want FailedHook restore, got %q", result.FailedHook)
	}
	if !strings.Contains(result.Detail, "cannot join network") {
		t.Fatalf("want Detail to carry the start error, got %q", result.Detail)
	}
	if f.hasCall("wait conch-hook-web-restore") {
		t.Fatal("Wait must not be called when Start failed")
	}
}

func TestRemoveIfExistsFailureStopsTheChain(t *testing.T) {
	f := newFakeDocker()
	f.removeErr["conch-hook-web-restore"] = fmt.Errorf("daemon unreachable")

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	result := r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 5}})
	if result.OK {
		t.Fatal("want failure")
	}
	if !strings.Contains(result.Detail, "daemon unreachable") {
		t.Fatalf("want Detail to carry the remove-if-exists error, got %q", result.Detail)
	}
	if f.hasCall("create conch-hook-web-restore") {
		t.Fatal("Create must not be called when remove-if-exists failed")
	}
}

func TestKillOrphansReturnsTheNames(t *testing.T) {
	f := newFakeDocker()
	f.orphans = []string{"conch-hook-web-restore", "conch-hook-blog-sync"}

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	got := r.KillOrphans([]string{"web", "blog"})

	for _, name := range f.orphans {
		if !f.hasCall("kill " + name) {
			t.Fatalf("want kill %s, calls: %v", name, f.callLog())
		}
		if !f.hasCall("remove " + name) {
			t.Fatalf("want remove %s, calls: %v", name, f.callLog())
		}
	}
	want := []string{"web", "blog"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestKillOrphansMapsContainersToServices(t *testing.T) {
	f := newFakeDocker()
	f.orphans = []string{
		"conch-hook-web-restore",
		"conch-hook-my-blog-sync",
	}

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	got := r.KillOrphans([]string{"web", "my-blog"})

	want := []string{"web", "my-blog"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for _, name := range f.orphans {
		if !f.hasCall("kill "+name) || !f.hasCall("remove "+name) {
			t.Fatalf("want kill+remove for %s, calls: %v", name, f.callLog())
		}
	}
}

// TestKillOrphansResolvesHyphenAmbiguityByLongestMatch pins the exact
// judgment call documented on KillOrphans: "my" is itself a valid prefix of
// "my-blog", so a container belonging to "my-blog" must not be misattributed
// to "my" when both are known services.
func TestKillOrphansResolvesHyphenAmbiguityByLongestMatch(t *testing.T) {
	f := newFakeDocker()
	f.orphans = []string{"conch-hook-my-blog-sync"}

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	got := r.KillOrphans([]string{"my", "my-blog"})

	want := []string{"my-blog"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v (must attribute to the longer, more specific service)", got, want)
	}
}

// TestUnattributableOrphanIsKilledButNotReturned covers the edge case noted
// in KillOrphans's doc comment: an orphaned container whose name matches
// none of the caller's known services is still killed and removed — spec §8
// rule 4 says every conch-hook=true container dies before any lease is
// read, unconditionally — but has no service to attribute it to, so it is
// silently absent from the returned slice rather than reported under a
// wrong or empty service name.
func TestUnattributableOrphanIsKilledButNotReturned(t *testing.T) {
	f := newFakeDocker()
	f.orphans = []string{
		"conch-hook-web-restore",
		"conch-hook-decommissioned-cleanup", // no longer a configured service
	}

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	got := r.KillOrphans([]string{"web"}) // "decommissioned" isn't known anymore

	want := []string{"web"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v (the unattributable orphan must not appear)", got, want)
	}
	for _, name := range f.orphans {
		if !f.hasCall("kill "+name) || !f.hasCall("remove "+name) {
			t.Fatalf("want kill+remove for %s regardless of attribution, calls: %v", name, f.callLog())
		}
	}
}

func TestAbortKillsTheInFlightHook(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-restore"] = block

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())

	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 30}})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("web")

	if !f.hasCall("kill conch-hook-web-restore") {
		t.Fatalf("want Kill recorded for the in-flight hook, calls: %v", f.callLog())
	}

	close(block) // let Run's blocked Wait return so its goroutine can finish
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned after Abort unblocked it")
	}
}

func TestAbortIsANoOpForADifferentService(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-restore"] = block

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())

	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("web", []config.HookSpec{{Name: "restore", Image: "img", Timeout: 30}})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("blog") // unrelated service: must not kill web's hook

	if f.hasCall("kill conch-hook-web-restore") {
		t.Fatal("Abort must not kill a hook belonging to a different service")
	}

	close(block)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned")
	}
}

// TestAbortDoesNotMatchAHyphenatedPrefix pins the fix for the same
// ambiguity KillOrphans resolves by taking a known-services list: Abort must
// compare the service it was called with against the service Run is
// actually processing by exact equality, never by treating one name as a
// textual prefix of another. "my" is a prefix of "my-blog", but a hook
// running for "my-blog" must survive Abort("my") untouched.
func TestAbortDoesNotMatchAHyphenatedPrefix(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-my-blog-sync"] = block
	f.waitExit["conch-hook-my-blog-sync"] = 0

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("my-blog", []config.HookSpec{{Name: "sync", Image: "img", Timeout: 30}})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("my") // a different, shorter service that happens to prefix "my-blog"

	if f.hasCall("kill conch-hook-my-blog-sync") {
		t.Fatal(`Abort("my") must not kill a hook belonging to service "my-blog"`)
	}

	close(block)
	result := <-done
	if !result.OK {
		t.Fatalf("want the unrelated abort to have no effect on the run, got %+v", result)
	}
}

// TestAbortStopsTheChainBeforeTheNextHook closes the race window between two
// hooks: inFlight is "" in the gap after one hook clears it and before the
// next sets it, so a Kill-only Abort landing exactly there would find
// nothing to kill and the chain would sail on into the next hook regardless.
// Here Abort lands while the first hook's Wait is still blocked; the fake
// then reports it exiting 0 as if the kill changed nothing about its result
// (Docker's actual behavior after a kill varies by container, so this
// deliberately doesn't rely on the exit code alone to stop the chain) — only
// the sticky aborted flag can be responsible for the second hook never
// starting.
func TestAbortStopsTheChainBeforeTheNextHook(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-restore"] = block
	f.waitExit["conch-hook-web-restore"] = 0

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("web", []config.HookSpec{
			{Name: "restore", Image: "img", Timeout: 30},
			{Name: "verify", Image: "img", Timeout: 30},
		})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("web")
	close(block)

	result := <-done
	if result.OK {
		t.Fatal("want failure: the chain must not continue past an abort")
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the next hook must never start once the chain has been aborted")
	}
}

// TestAbortStopsAllRemainingHooksInALongerChain is the regression test for
// the TOCTOU fixed in Run (the abort-check and the inFlight publish are now
// one atomic critical section, guarded by the same mu Abort locks — see the
// comment at the top of Run's loop). The bug it guards against: an earlier
// version checked "aborted" and then published inFlight as two separate
// lock/unlock pairs, leaving a window in which Abort could land, see nothing
// yet published to kill, and have its flag set too late to stop the hook
// Run was already committed to starting — one hook would run fully
// uncancelled before the sticky flag caught the one after it.
//
// A precisely-timed reproduction of that exact window is not something this
// test — or any test — can construct anymore: merging the check and the
// publish into one critical section makes the two operations mutually
// exclusive by construction (Abort's own critical section also holds mu),
// so there is no longer an intermediate state for a scheduler interleaving
// to expose, deterministically or otherwise. What this test does instead is
// raise confidence that the fix holds across more than the one iteration
// immediately after the abort: a three-hook chain, aborted while the first
// is still running, must never start the second OR the third — if the
// atomicity argument were somehow wrong for a later iteration (e.g. a
// future edit reintroduced a gap only on iteration 2+), this is the test
// that would catch it where a two-hook chain could not.
func TestAbortStopsAllRemainingHooksInALongerChain(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-restore"] = block
	f.waitExit["conch-hook-web-restore"] = 0

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("web", []config.HookSpec{
			{Name: "restore", Image: "img", Timeout: 30},
			{Name: "verify", Image: "img", Timeout: 30},
			{Name: "warm-cache", Image: "img", Timeout: 30},
		})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("web")
	close(block)

	result := <-done
	if result.OK {
		t.Fatal("want failure: the chain must not continue past an abort")
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the second hook must never start once the chain has been aborted")
	}
	if f.hasCall("create conch-hook-web-warm-cache") {
		t.Fatal("the third hook must never start once the chain has been aborted")
	}
}

func TestAbortIsANoOpWhenNothingIsInFlight(t *testing.T) {
	f := newFakeDocker()
	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	r.Abort("web") // must not panic or record a spurious kill
	if len(f.callLog()) != 0 {
		t.Fatalf("want no calls, got %v", f.callLog())
	}
}

// ---------------------------------------------------------------------------
// Two chains at once. `conch evacuate --all` drains every service on a host,
// each on its own executor, so concurrent chains are routine rather than
// exotic — and a Runner that keeps ONE record of "the chain in flight" is
// wrong in three separate ways when they overlap.
// ---------------------------------------------------------------------------

func twoHooks(first string) []config.HookSpec {
	return []config.HookSpec{
		{Name: first, Image: "img", Timeout: 30},
		{Name: "verify", Image: "img", Timeout: 30},
	}
}

// TestAbortHitsOnlyItsOwnServicesChain: with one shared inFlight, Abort("web")
// kills whichever container happened to be published last — which may be
// blog's — and Abort's own match test looks at whichever service started
// last, so the abort can miss its target entirely. An evacuation whose drain
// was never actually cancelled reports success and releases the lease
// believing the final sync finished.
func TestAbortHitsOnlyItsOwnServicesChain(t *testing.T) {
	f := newFakeDocker()
	webBlock, blogBlock := make(chan struct{}), make(chan struct{})
	f.waitBlockUntil["conch-hook-web-drain"] = webBlock
	f.waitBlockUntil["conch-hook-blog-drain"] = blogBlock

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	webDone := make(chan hooks.Result, 1)
	blogDone := make(chan hooks.Result, 1)
	go func() { webDone <- r.Run("web", twoHooks("drain")) }()
	go func() { blogDone <- r.Run("blog", twoHooks("drain")) }()
	f.awaitWait(t, "conch-hook-web-drain", "conch-hook-blog-drain")

	r.Abort("web")

	if !f.hasCall("kill conch-hook-web-drain") {
		t.Fatalf("the aborted service's own hook must be killed, calls: %v", f.callLog())
	}
	if f.hasCall("kill conch-hook-blog-drain") {
		t.Fatalf("another service's hook must never be killed by it, calls: %v", f.callLog())
	}

	close(webBlock)
	close(blogBlock)
	web, blog := <-webDone, <-blogDone

	if web.OK {
		t.Fatal("an aborted chain must report failure: a caller that believes its final sync " +
			"succeeded will release the lease")
	}
	if !blog.OK {
		t.Fatalf("the untouched chain must run to completion, got %+v", blog)
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the aborted chain must not start its next hook")
	}
	if !f.hasCall("create conch-hook-blog-verify") {
		t.Fatalf("the untouched chain must start its next hook, calls: %v", f.callLog())
	}
}

// TestASecondChainDoesNotClobberAnotherServicesAbort: Run resets the abort
// flag on entry, which is right for its OWN chain and catastrophic for
// somebody else's. A second service starting a chain a moment after an abort
// landed would un-abort the first, and its remaining hooks would run.
func TestASecondChainDoesNotClobberAnotherServicesAbort(t *testing.T) {
	f := newFakeDocker()
	webBlock := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-drain"] = webBlock

	r := hooks.NewRunner(f, clock.NewFake(), discardLogger())
	webDone := make(chan hooks.Result, 1)
	go func() { webDone <- r.Run("web", twoHooks("drain")) }()
	f.awaitWait(t, "conch-hook-web-drain")

	r.Abort("web")
	if res := r.Run("blog", []config.HookSpec{{Name: "drain", Image: "img", Timeout: 30}}); !res.OK {
		t.Fatalf("the second service's chain is unrelated and must succeed, got %+v", res)
	}

	close(webBlock)
	res := <-webDone
	if res.OK {
		t.Fatal("web's abort must survive another service's chain")
	}
	if f.hasCall("create conch-hook-web-verify") {
		t.Fatal("the aborted chain must not start its next hook")
	}
}

// ---------------------------------------------------------------------------
// Start/finish narration (RCA R5b): a 7-hook restore chain that runs with
// zero log output is the exact failure this Runner must no longer be
// capable of. Failure DETAIL stays the executor's to log — these tests only
// pin the two facts this package alone can see: that a hook started, and how
// long it ran.
// ---------------------------------------------------------------------------

// TestRunLogsStartAndFinishForEachSuccessfulHook pins the happy-path shape:
// one start and one finish record per hook, in chain order, each finish
// carrying the elapsed time THAT hook took (not the whole chain's), which is
// why the fake advances a controlled clock inside Wait rather than leaving
// duration a constant zero every test would trivially satisfy.
func TestRunLogsStartAndFinishForEachSuccessfulHook(t *testing.T) {
	clk := clock.NewFake()
	f := newFakeDocker()
	f.clk = clk
	f.waitAdvance = map[string]float64{
		"conch-hook-web-restore": 3,
		"conch-hook-web-verify":  2,
	}
	f.waitExit["conch-hook-web-restore"] = 0
	f.waitExit["conch-hook-web-verify"] = 0

	handler := &capturingLogHandler{}
	r := hooks.NewRunner(f, clk, slog.New(handler))

	result := r.Run("web", []config.HookSpec{
		{Name: "restore", Image: "restore-img", Timeout: 30},
		{Name: "verify", Image: "verify-img", Timeout: 30},
	})
	if !result.OK {
		t.Fatalf("want success, got %+v", result)
	}

	starts := handler.of("hook start")
	if len(starts) != 2 {
		t.Fatalf("want 2 start records, got %d: %+v", len(starts), starts)
	}
	if starts[0].attrs["service"] != "web" || starts[0].attrs["hook"] != "restore" || starts[0].attrs["image"] != "restore-img" {
		t.Fatalf("restore start record wrong: %+v", starts[0].attrs)
	}
	if starts[1].attrs["hook"] != "verify" || starts[1].attrs["image"] != "verify-img" {
		t.Fatalf("verify start record wrong: %+v", starts[1].attrs)
	}

	finishes := handler.of("hook finish")
	if len(finishes) != 2 {
		t.Fatalf("want 2 finish records, got %d: %+v", len(finishes), finishes)
	}
	if finishes[0].attrs["hook"] != "restore" || finishes[0].attrs["outcome"] != "ok" {
		t.Fatalf("restore finish record wrong: %+v", finishes[0].attrs)
	}
	if got := finishes[0].attrs["duration_seconds"]; got != "3" {
		t.Fatalf("want restore duration_seconds 3 (its own Wait's advance), got %q", got)
	}
	if finishes[1].attrs["hook"] != "verify" || finishes[1].attrs["outcome"] != "ok" {
		t.Fatalf("verify finish record wrong: %+v", finishes[1].attrs)
	}
	if got := finishes[1].attrs["duration_seconds"]; got != "2" {
		t.Fatalf("want verify duration_seconds 2, not the chain's cumulative 5, got %q", got)
	}
}

// TestRunLogsFinishAsFailedWithoutDuplicatingDetail covers a failing hook:
// it still gets a start and a finish record (outcome "failed"), but the
// finish record carries no failure detail — that stays the executor's job
// (executor.go:373/:409) — and the chain's second hook, never reached, gets
// no start/finish pair of its own.
func TestRunLogsFinishAsFailedWithoutDuplicatingDetail(t *testing.T) {
	f := newFakeDocker()
	f.waitExit["conch-hook-web-restore"] = 1
	f.logsOut["conch-hook-web-restore"] = "restore: cannot find backup"

	handler := &capturingLogHandler{}
	r := hooks.NewRunner(f, clock.NewFake(), slog.New(handler))

	result := r.Run("web", []config.HookSpec{
		{Name: "restore", Image: "img", Timeout: 5},
		{Name: "verify", Image: "img", Timeout: 5},
	})
	if result.OK {
		t.Fatal("want failure")
	}

	starts := handler.of("hook start")
	if len(starts) != 1 || starts[0].attrs["hook"] != "restore" {
		t.Fatalf("want exactly 1 start record, for restore only, got %+v", starts)
	}

	finishes := handler.of("hook finish")
	if len(finishes) != 1 {
		t.Fatalf("want exactly 1 finish record, got %d: %+v", len(finishes), finishes)
	}
	if finishes[0].attrs["hook"] != "restore" || finishes[0].attrs["outcome"] != "failed" {
		t.Fatalf("want restore finish outcome failed, got %+v", finishes[0].attrs)
	}
	if strings.Contains(finishes[0].attrs["outcome"], "backup") {
		t.Fatalf("the finish record must not carry the hook's own failure detail: %+v", finishes[0].attrs)
	}
	for k := range finishes[0].attrs {
		if k != "service" && k != "hook" && k != "outcome" && k != "duration_seconds" {
			t.Fatalf("finish record carries an unexpected attr %q: %+v — detail belongs to the executor's Error log, not here", k, finishes[0].attrs)
		}
	}
}

// TestAbortedHookGetsNoStartOrFinishRecord: a hook killed by Abort before
// Run's loop ever reaches it never actually started, so it must not appear
// in the start/finish narration either — logging a start for work that never
// happened would be its own small lie.
func TestAbortedHookGetsNoStartOrFinishRecord(t *testing.T) {
	f := newFakeDocker()
	block := make(chan struct{})
	f.waitBlockUntil["conch-hook-web-restore"] = block
	f.waitExit["conch-hook-web-restore"] = 0

	handler := &capturingLogHandler{}
	r := hooks.NewRunner(f, clock.NewFake(), slog.New(handler))

	done := make(chan hooks.Result, 1)
	go func() {
		done <- r.Run("web", []config.HookSpec{
			{Name: "restore", Image: "img", Timeout: 30},
			{Name: "verify", Image: "img", Timeout: 30},
		})
	}()

	select {
	case <-f.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never reached Wait")
	}

	r.Abort("web")
	close(block)

	result := <-done
	if result.OK {
		t.Fatal("want failure: the chain must not continue past an abort")
	}

	if got := handler.of("hook start"); len(got) != 1 {
		t.Fatalf("want exactly 1 start record (restore only, never verify), got %d: %+v", len(got), got)
	}
	if got := handler.of("hook finish"); len(got) != 1 {
		t.Fatalf("want exactly 1 finish record (restore only, never verify), got %d: %+v", len(got), got)
	}
}
