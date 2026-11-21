package supervisor

import (
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/decide"
)

// ---------------------------------------------------------------------------
// fakeFleet: a port of arbiter/src/lease.js, not an impression of it.
//
// Task 11 cost three real bugs to fakes that were *nearly* the arbiter, so
// every rule below is transcribed from the file rather than remembered:
// view()'s four cases, renewOne()'s bar check and bar-clearing rules,
// releaseOne()'s not_holder refusal and its always-free-shaped answer, and
// expireIfDue()'s lazy expiry of both the lease and the bar. The evacuation
// path lives entirely inside those rules, so a fake that got any one of them
// wrong would make this whole file agree with itself and with nothing else.
//
// The one deliberate simplification is duplicate-host detection, which
// evacuation never touches and which has its own tests in internal/arbiter.
// ---------------------------------------------------------------------------

const fakeTTL = 60.0 // testConfig's timing.ttl; the fake arbiter never sees the request's

type fakeLease struct {
	holder      string // "" is free
	serving     bool
	epoch       int
	expiresAt   float64 // 0 = unset
	evacuatedBy string
	barHold     bool
	barExpires  float64 // 0 = no expiry: the bar outlives time, not a peer's claim
}

type fakeFleet struct {
	mu     sync.Mutex
	clk    *clock.Fake
	us     string
	leases map[string]*fakeLease
}

func newFakeFleet(clk *clock.Fake) *fakeFleet {
	return &fakeFleet{clk: clk, us: "host-a", leases: map[string]*fakeLease{}}
}

func (f *fakeFleet) lease(key string) *fakeLease {
	l, ok := f.leases[key]
	if !ok {
		l = &fakeLease{}
		f.leases[key] = l
	}
	return l
}

// expireIfDue is lease.js's lazy expiry: the read that discovers an expired
// lease is the cleanup, and the bar has its own, separate expiry.
func (f *fakeFleet) expireIfDue(now float64, l *fakeLease) {
	if l.holder != "" && l.expiresAt != 0 && now >= l.expiresAt {
		l.holder, l.serving, l.expiresAt = "", false, 0
	}
	if l.evacuatedBy != "" && l.barExpires != 0 && now >= l.barExpires {
		l.evacuatedBy, l.barExpires, l.barHold = "", 0, false
	}
}

// view is lease.js's view(): a holder reads `yours` (with NO ttl — only a
// granted renew carries one), everyone else reads `held`, the barred host
// reads `barred`, and everybody else reads `free`.
func (f *fakeFleet) view(l *fakeLease, forHost string) arbiter.Verdict {
	switch {
	case l.holder == forHost:
		return arbiter.Verdict{Lease: decide.Yours, Epoch: l.epoch}
	case l.holder != "":
		return arbiter.Verdict{Lease: decide.Held, Holder: l.holder, Serving: l.serving, Epoch: l.epoch}
	case l.evacuatedBy == forHost:
		return arbiter.Verdict{Lease: decide.Barred, EvacuatedBy: l.evacuatedBy, Epoch: l.epoch}
	}
	return arbiter.Verdict{Lease: decide.Free, Epoch: l.epoch, EvacuatedBy: l.evacuatedBy}
}

func (f *fakeFleet) renewFor(now float64, host string, l *fakeLease, op arbiter.Op) arbiter.Verdict {
	if l.holder == "" && l.evacuatedBy == host && !op.Override {
		return arbiter.Verdict{Lease: decide.Barred, EvacuatedBy: l.evacuatedBy, Epoch: l.epoch}
	}
	if l.holder != "" && l.holder != host {
		return arbiter.Verdict{Lease: decide.Held, Holder: l.holder, Serving: l.serving, Epoch: l.epoch}
	}
	if l.holder == "" {
		l.epoch++
		if l.evacuatedBy != "" {
			ownOverride := l.evacuatedBy == host && op.Override
			peerClaim := l.evacuatedBy != host && !l.barHold
			if ownOverride || peerClaim {
				l.evacuatedBy, l.barExpires, l.barHold = "", 0, false
			}
			// A hold bar under a peer's claim stays recorded: the lease is
			// granted, the bar outlives it (lease.js, renewOne).
		}
	}
	l.holder = host
	l.expiresAt = now + fakeTTL
	l.serving = op.Serving
	return arbiter.Verdict{Lease: decide.Yours, TTL: fakeTTL, Epoch: l.epoch}
}

func (f *fakeFleet) releaseFor(now float64, host string, l *fakeLease, op arbiter.Op) arbiter.Verdict {
	if l.holder == "" || l.holder != host {
		// lease.js returns {state:"error", error:"not_holder"}, and the client
		// maps every error but duplicate_host to Unreachable with Err kept.
		return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
	}
	l.holder, l.serving, l.expiresAt = "", false, 0
	if op.Evacuate {
		l.evacuatedBy = host
		l.barHold = op.Hold
		l.barExpires = 0
		if op.BarTTL > 0 {
			l.barExpires = now + op.BarTTL
		}
	}
	// Always free-shaped: this is the releaser's confirmation that the release
	// happened, not a status check on themselves. view() would say `barred`.
	return arbiter.Verdict{Lease: decide.Free, Epoch: l.epoch, Released: true, EvacuatedBy: l.evacuatedBy}
}

// reply is the fakeArbiter script: every op from OUR host.
func (f *fakeFleet) reply(check int, key string, op arbiter.Op) arbiter.Verdict {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clk.Mono()
	if op.Op == "read" {
		l, ok := f.leases[key]
		if !ok {
			return arbiter.Verdict{Lease: decide.Free}
		}
		f.expireIfDue(now, l)
		return f.view(l, f.us)
	}
	l := f.lease(key)
	f.expireIfDue(now, l)
	if op.Op == "renew" {
		return f.renewFor(now, f.us, l, op)
	}
	return f.releaseFor(now, f.us, l, op)
}

// peerRenews is the peer doing what a peer does: it goes through the very same
// renewOne rules, so a bar that would stop it stops it here too.
func (f *fakeFleet) peerRenews(key string, serving bool) arbiter.Verdict {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clk.Mono()
	l := f.lease(key)
	f.expireIfDue(now, l)
	return f.renewFor(now, "host-b", l, arbiter.Op{Op: "renew", Serving: serving})
}

func (f *fakeFleet) snapshot(key string) fakeLease {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.lease(key)
	f.expireIfDue(f.clk.Mono(), l)
	return *l
}

// ---------------------------------------------------------------------------
// Fake-fidelity tests: the bar rules, read off lease.js, pinned before
// anything is built on top of them.
// ---------------------------------------------------------------------------

func TestFakeArbiterBarsOnlyTheEvacuatingHost(t *testing.T) {
	f := newFakeFleet(clock.NewFake())
	f.reply(0, "web", arbiter.Op{Op: "renew"})
	v := f.reply(1, "web", arbiter.Op{Op: "release", Evacuate: true, BarTTL: 1800})
	if v.Lease != decide.Free || !v.Released {
		t.Fatalf("a release answers free-shaped and released, got %+v", v)
	}

	// Our own read reads BARRED, not free: view() checks evacuatedBy === forHost.
	if got := f.reply(2, "web", arbiter.Op{Op: "read"}); got.Lease != decide.Barred {
		t.Fatalf("the evacuating host reads barred, got %+v", got)
	}
	if got := f.reply(3, "web", arbiter.Op{Op: "renew"}); got.Lease != decide.Barred {
		t.Fatalf("the bar refuses our renew, got %+v", got)
	}
	// The peer is not barred at all.
	if got := f.peerRenews("web", false); got.Lease != decide.Yours {
		t.Fatalf("the bar is not the peer's problem, got %+v", got)
	}
	if l := f.snapshot("web"); l.evacuatedBy != "" {
		t.Fatalf("a peer's claim clears an ordinary bar, got %+v", l)
	}
}

func TestFakeArbiterHoldBarSurvivesAPeerClaimAndDoesNotExpire(t *testing.T) {
	clk := clock.NewFake()
	f := newFakeFleet(clk)
	f.reply(0, "web", arbiter.Op{Op: "renew"})
	// --hold sends NO bar_ttl: barExpiresAt and barHold are independent in
	// releaseOne, and expireIfDue clears the WHOLE bar — hold included — once
	// bar_ttl lapses. A hold bar with a ttl is not a hold bar.
	f.reply(1, "web", arbiter.Op{Op: "release", Evacuate: true, Hold: true})

	if got := f.peerRenews("web", true); got.Lease != decide.Yours {
		t.Fatalf("a hold bar still lets the peer take over, got %+v", got)
	}
	if l := f.snapshot("web"); l.evacuatedBy != "host-a" || !l.barHold {
		t.Fatalf("a hold bar outlives the peer's claim, got %+v", l)
	}
	clk.Advance(86400)
	if l := f.snapshot("web"); l.evacuatedBy != "host-a" {
		t.Fatalf("a hold bar has no expiry to reach, got %+v", l)
	}
	// And only an override renew from us clears it.
	f.mu.Lock()
	f.leases["web"].holder = "" // the peer's lease lapses
	f.mu.Unlock()
	if got := f.reply(2, "web", arbiter.Op{Op: "renew"}); got.Lease != decide.Barred {
		t.Fatalf("the hold bar still refuses us, got %+v", got)
	}
	if got := f.reply(3, "web", arbiter.Op{Op: "renew", Override: true}); got.Lease != decide.Yours {
		t.Fatalf("an override renew is what reclaim is, got %+v", got)
	}
	if l := f.snapshot("web"); l.evacuatedBy != "" || l.barHold {
		t.Fatalf("the override cleared the bar, got %+v", l)
	}
}

func TestFakeArbiterBarTTLExpiresOnItsOwn(t *testing.T) {
	clk := clock.NewFake()
	f := newFakeFleet(clk)
	f.reply(0, "web", arbiter.Op{Op: "renew"})
	f.reply(1, "web", arbiter.Op{Op: "release", Evacuate: true, BarTTL: 1800})

	clk.Advance(1799)
	if got := f.reply(2, "web", arbiter.Op{Op: "read"}); got.Lease != decide.Barred {
		t.Fatalf("the bar holds until bar_ttl, got %+v", got)
	}
	clk.Advance(2)
	if got := f.reply(3, "web", arbiter.Op{Op: "read"}); got.Lease != decide.Free {
		t.Fatalf("auto-reclaim is the bar expiring, got %+v", got)
	}
}

func TestFakeArbiterRefusesAReleaseFromANonHolder(t *testing.T) {
	f := newFakeFleet(clock.NewFake())
	got := f.reply(0, "web", arbiter.Op{Op: "release", Evacuate: true})
	if got.Lease != decide.Unreachable || got.Err != "not_holder" {
		t.Fatalf("a release from a non-holder is refused, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Harness helpers for the evacuation sequence
// ---------------------------------------------------------------------------

// evacConfig is testConfig plus a drain hook and an auto-reclaim window, the
// two things an evacuation actually reads off a service.
func evacConfig(names ...string) config.Config {
	cfg := testConfig(names...)
	for i := range cfg.Services {
		name := cfg.Services[i].Name
		cfg.Services[i].Hooks.Evacuate = []config.HookSpec{
			{Name: "drain", Image: "drain-" + name + ":1", Timeout: 60},
		}
		cfg.Services[i].Evacuation = config.EvacConfig{AutoReclaimAfter: "30m"}
	}
	return cfg
}

// driveActive is driveToActive for a fleet where only SOME services are ours
// to claim: it runs the same claim-promote-serve sequence and then asserts on
// exactly the services named.
func (h *harness) driveActive(keys ...string) {
	h.t.Helper()
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	for _, key := range keys {
		if p := h.sup.Status()[key].(map[string]any)["phase"]; p != string(decide.Active) {
			h.t.Fatalf("%s should be ACTIVE by now: %v", key, h.sup.Status()[key])
		}
	}
}

// livePeer is the pre-flight condition: a peer that checked in within
// 3*interval.
func (h *harness) livePeer() {
	h.arb.setPeers(map[string]float64{"host-b": 1})
}

func drainEvents(ch <-chan EvacEvent) []EvacEvent {
	var out []EvacEvent
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		default:
			return out
		}
	}
}

// runEvac runs checks until the evacuation reports a verdict, and returns the
// whole transcript. maxChecks bounds it so a stuck sequence fails loudly.
func (h *harness) runEvac(ch <-chan EvacEvent, maxChecks int, between func(check int)) (EvacEvent, []EvacEvent) {
	h.t.Helper()
	var all []EvacEvent
	for i := 0; i < maxChecks; i++ {
		if between != nil {
			between(i)
		}
		h.check()
		for _, ev := range drainEvents(ch) {
			all = append(all, ev)
			if ev.Done {
				return ev, all
			}
		}
	}
	h.t.Fatalf("the evacuation never reported a verdict in %d checks: %v", maxChecks, all)
	return EvacEvent{}, all
}

func transcriptOf(events []EvacEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Service != "" {
			b.WriteString(ev.Service + ": ")
		}
		b.WriteString(ev.Message + "\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// The brief's tests
// ---------------------------------------------------------------------------

func TestEvacuateRefusesWithNoLivePeer(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.driveToActive()

	before := len(h.dk.mutations())
	_, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err == nil {
		t.Fatal("evacuating into a void stops everything and strands it barred: it must be refused")
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Fatalf("the refusal must name the override, got %q", err)
	}
	h.check()
	if got := h.arb.countOps("web", "release"); got != 0 {
		t.Fatalf("a refused evacuation releases nothing, got %d releases", got)
	}
	if !h.dk.running("cf-web") {
		t.Fatal("a refused evacuation leaves the connector alone")
	}
	if len(h.dk.mutations()) != before {
		t.Fatalf("a refused evacuation touches nothing: %v", h.dk.mutations()[before:])
	}
}

func TestEvacuateRunsHooksStopsConnectorReleasesWithBar(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	// The drain hook wedges, so the lease's renewals under it are observable.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-drain"] = gate
	h.dk.mu.Unlock()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}

	h.checkNoWait() // the hooks start on this check
	awaitGate(t, h.dk.waitHit, "the drain hook to start")

	// Spec §9: the lease is renewed throughout the hooks, serving:true until
	// the connector stops. An expired lease mid-sync would let the peer restore
	// from a replica missing the very data the sync exists to hand over.
	for i := 0; i < 3; i++ {
		at := h.arb.nBatches()
		h.checkNoWait()
		op := h.arb.opStructAt(at, "web")
		if op.Op != "renew" || !op.Serving {
			t.Fatalf("the lease must be renewed serving under the hooks, got %+v (ops %v)",
				op, h.arb.ops("web"))
		}
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the connector serves until the hooks are done")
	}

	close(gate)
	h.waitIdle()

	stopCheck := h.arb.nBatches()
	h.check() // the hooks are done: stop the connector
	if h.dk.running("cf-web") {
		t.Fatalf("the connector stops once the hooks finish (ops %v)", h.arb.ops("web"))
	}
	if op := h.arb.opStructAt(stopCheck, "web"); op.Op == "release" {
		t.Fatal("the release must never overtake the connector stop")
	}

	releaseCheck := h.arb.nBatches()
	h.check() // the stop is confirmed: the release goes out
	op := h.arb.opStructAt(releaseCheck, "web")
	if op.Op != "release" || !op.Evacuate {
		t.Fatalf("the release carries the evacuation flag, got %+v (ops %v)", op, h.arb.ops("web"))
	}
	if op.Hold {
		t.Fatalf("no --hold was asked for, got %+v", op)
	}
	if op.BarTTL != 1800 {
		t.Fatalf("auto_reclaim_after 30m is a 1800s bar, got %+v", op)
	}

	mustContainInOrder(t, h.dk.calls(), []string{
		"create conch-hook-web-drain", // drain and final sync, while still serving
		"stop cf-web",                 // only then does the connector go
	})

	if l := fleet.snapshot("web"); l.holder != "" || l.evacuatedBy != "host-a" {
		t.Fatalf("the lease is free and barred to us, got %+v", l)
	}
	_ = ch
}

func TestEvacuateHoldSendsAPermanentBar(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, true, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })

	var release arbiter.Op
	for i := 0; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "web"); op.Op == "release" {
			release = op
		}
	}
	if !release.Evacuate || !release.Hold {
		t.Fatalf("--hold sends hold:true, got %+v", release)
	}
	if release.BarTTL != 0 {
		t.Fatalf("--hold must send NO bar_ttl: bar_ttl expiry clears the hold with it, got %+v", release)
	}
	if l := fleet.snapshot("web"); l.evacuatedBy != "host-a" || !l.barHold {
		t.Fatalf("a hold bar survives the peer's claim, got %+v", l)
	}
}

func TestEvacuateWaitsForPeerServing(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}

	// Phase 1: the release lands and NOBODY has claimed. Our own reads say
	// `barred` — view() answers the barred host with its own bar, never `free`
	// (arbiter/src/lease.js).
	var events []EvacEvent
	for i := 0; i < 6; i++ {
		h.check()
		events = append(events, drainEvents(ch)...)
	}
	for _, ev := range events {
		if ev.Done {
			t.Fatalf("holding is not serving, and nobody is even holding yet: %v", events)
		}
	}
	if l := fleet.snapshot("web"); l.holder != "" || l.evacuatedBy != "host-a" {
		t.Fatalf("the lease should be released and barred by now, got %+v (ops %v)", l, h.arb.ops("web"))
	}

	// Phase 2: the peer claims but is not serving yet — its own hooks may still
	// fail, so holding is not confirmation.
	fleet.peerRenews("web", false)
	for i := 0; i < 3; i++ {
		h.check()
		for _, ev := range drainEvents(ch) {
			if ev.Done {
				t.Fatalf("a holder that is not serving is not a hand-off: %+v", ev)
			}
			events = append(events, ev)
		}
	}

	// Phase 3: the peer serves.
	fleet.peerRenews("web", true)
	final, all := h.runEvac(ch, 4, nil)
	if !final.OK {
		t.Fatalf("the hand-off completed: %v", transcriptOf(all))
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
		t.Fatalf("an evacuated service stands by, got %v", p)
	}
	if h.sup.Status()["web"].(map[string]any)["evacuating"] != false {
		t.Fatal("a finished evacuation clears the flag: a leaked one bricks the host")
	}
}

// TestEvacuationDoesNotAlsoFireDemote pins the no-double-emit rule: a
// completed hand-off already reports itself as an "evacuate" event
// (finishServiceLocked), and must never ALSO fire "demote" — an operator
// would otherwise see one failover reported twice, on two different webhook
// URLs. What this actually exercises is the ordinary was==now path:
// finishServiceLocked mutates u.state (and clears u.evacuating) to Standby
// before commit() ever runs, so by the time notifyTransition sees this
// transition was and now already agree, and it exits via the kind switch's
// `default: return` — not via the wasEvacuating early return at
// notifyTransition's top. That guard exists for a narrower case (decideOne's
// "owed" branch, a release re-send while still evacuating) that this test
// does not reach; see notifyTransition's own wasEvacuating comment in
// supervisor.go for that case.
func TestEvacuationDoesNotAlsoFireDemote(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()
	h.nt.mu.Lock()
	h.nt.events = nil // only the evacuation's own events matter here
	h.nt.mu.Unlock()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("the hand-off should have completed: %v", transcriptOf(all))
	}

	if got := h.nt.of("evacuate"); len(got) != 1 || got[0].payload["ok"] != true {
		t.Fatalf("want exactly one successful evacuate event, got %v", got)
	}
	if got := h.nt.of("demote"); len(got) != 0 {
		t.Fatalf("a completed evacuation must not also fire demote: %v", got)
	}
}

// TestEvacuationDoesNotLogDemoteAtInfo is TestEvacuationDoesNotAlsoFireDemote's
// log-line half: a completed hand-off must not log "demote" from
// notifyTransition, for the same reason the webhook must not double-fire
// (finishServiceLocked already said "evacuate"). Like its twin, this reaches
// notifyTransition's ordinary was==now `default: return` — finishServiceLocked
// has already moved u.state to Standby and cleared u.evacuating before
// commit() runs, so the wasEvacuating early return itself is NOT exercised
// here.
//
// NOTE: nothing in this package currently exercises that early return.
// Reaching it needs a transition that would otherwise classify as promote or
// demote (e.g. Active -> Standby) while u.evacuating is still true, which
// requires racing the async stopConfirmed callback (noteConnectorStopped,
// supervisor.go:1312-1318, invoked by the executor when the connector's stop
// escalation finishes) against evacuateAdvance, so a release-owed re-send
// lands before finishServiceLocked flips the flag. No harness support for
// that race exists yet; whoever builds it should add the true regression
// test here.
func TestEvacuationDoesNotLogDemoteAtInfo(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, evacConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()
	handler.mu.Lock()
	handler.records = nil // only the evacuation's own transition matters here
	handler.mu.Unlock()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("the hand-off should have completed: %v", transcriptOf(all))
	}

	if got := handler.of("demote"); len(got) != 0 {
		t.Fatalf("a completed evacuation must not also log demote from notifyTransition: %+v", got)
	}
	if got := handler.of("promote"); len(got) != 0 {
		t.Fatalf("a completed evacuation must not log promote either: %+v", got)
	}
}

func TestEvacuateTimeoutNamesTheRemedy(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	// Nobody ever claims: our reads stay `barred` for the whole window.
	final, all := h.runEvac(ch, 8, func(i int) {
		if i == 5 {
			h.clk.Advance(evacuationTimeout)
		}
	})
	if final.OK {
		t.Fatalf("nobody took the service over: %v", transcriptOf(all))
	}
	if !strings.Contains(transcriptOf(all), "conch reclaim web") {
		t.Fatalf("a failed evacuation must say what to do about it: %v", transcriptOf(all))
	}
	if h.sup.Status()["web"].(map[string]any)["evacuating"] != false {
		t.Fatal("a timed-out evacuation still ends: a leaked flag bricks the host")
	}
}

func TestEvacuatingHostClaimsNothing(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()

	// web goes active; blog stays free and claimable throughout.
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("web should be ACTIVE, got %v", p)
	}
	// blog was claimed too (both leases were free), so hand it to the peer.
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{holder: "host-b", serving: true, expiresAt: h.clk.Mono() + 1e6}
	fleet.mu.Unlock()
	h.check()
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{} // and now it is free and claimable
	fleet.mu.Unlock()

	if _, err := h.sup.Evacuate([]string{"web"}, false, false, false); err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	at := h.arb.nBatches()
	h.checks(10)
	for i := at; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "blog"); op.Op == "renew" {
			t.Fatalf("an evacuating host claims nothing, however free a lease reads: batch %d sent %+v",
				i, op)
		}
	}
}

func TestAnEvacuatedHostSupervisesAgain(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()

	// web is ours; blog is the peer's.
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{holder: "host-b", serving: true, expiresAt: 1e9}
	fleet.mu.Unlock()
	h.checks(h.cfg.Timing.PromoteAfter + 2)

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("the hand-off should have completed: %v", transcriptOf(all))
	}

	// blog's lease frees up. The suppression was for the evacuation, and the
	// evacuation is over: this host is ordinary standby capacity again.
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{}
	fleet.mu.Unlock()
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if p := h.sup.Status()["blog"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("a host that finished evacuating claims again, got %v (ops %v)", p, h.arb.ops("blog"))
	}
}

func TestReleaseUnreachableRollsBack(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	var mu sync.Mutex
	dark := false
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		mu.Lock()
		down := dark
		mu.Unlock()
		if down && op.Op == "release" {
			return arbiter.Verdict{Lease: decide.Unreachable}
		}
		if down {
			return arbiter.Verdict{Lease: decide.Unreachable}
		}
		return fleet.reply(check, key, op)
	}
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	// The hooks run and the connector stops, then the arbiter goes dark before
	// the release can be delivered.
	h.check() // hooks
	h.check() // stop
	mu.Lock()
	dark = true
	mu.Unlock()

	final, all := h.runEvac(ch, 6, nil)
	if final.OK {
		t.Fatalf("a release nobody received is not a hand-off: %v", transcriptOf(all))
	}
	if !h.dk.running("cf-web") {
		t.Fatalf("spec §9: the connector is restarted and ACTIVE renewal resumes. calls: %v", h.dk.calls())
	}
	st := h.sup.Status()["web"].(map[string]any)
	if st["phase"] != string(decide.Active) {
		t.Fatalf("a stopped service holding a ticking lease is an outage nobody can end, got %v", st)
	}
	if st["evacuating"] != false || st["release_owed"] != false {
		t.Fatalf("the rolled-back evacuation must leave nothing behind, got %v", st)
	}
	at := h.arb.nBatches()
	h.check()
	if op := h.arb.opStructAt(at, "web"); op.Op != "renew" {
		t.Fatalf("ACTIVE renewal resumes, got %+v (ops %v)", op, h.arb.ops("web"))
	}
}

// TestAnEvacuatingHostNeverRetakesTheLeaseItGaveBack pins the one guard the
// bar itself would otherwise hide. decide's rule 19 names `renew` on every
// check of an evacuating service — right while the hooks drain under a live
// lease, wrong afterwards — and a bar normally refuses the stray renew that
// results. It does not here: the lease expired server-side a moment before the
// release, so the arbiter refuses the release as `not_holder` and NO BAR IS
// EVER SET. Without the supervisor's own rule, the very next check re-claims
// the service onto a host that is about to go away.
func TestAnEvacuatingHostNeverRetakesTheLeaseItGaveBack(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		if op.Op == "release" {
			fleet.mu.Lock()
			fleet.leases[key].holder, fleet.leases[key].expiresAt = "", 0
			fleet.mu.Unlock()
		}
		return fleet.reply(check, key, op)
	}
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.check() // hooks
	h.check() // connector stop
	release := h.arb.nBatches()
	h.check() // the release, refused: not_holder
	if op := h.arb.opAt(release, "web"); op != "release" {
		t.Fatalf("the release should have gone out on batch %d, got %q (%v)", release, op, h.arb.ops("web"))
	}
	if l := fleet.snapshot("web"); l.evacuatedBy != "" {
		t.Fatalf("a refused release sets no bar, so nothing protects us from ourselves: %+v", l)
	}

	h.checks(5)
	for i := release; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "web"); op.Op == "renew" {
			t.Fatalf("batch %d re-took the lease this host is giving away: %+v (ops %v)",
				i, op, h.arb.ops("web"))
		}
	}

	// And the peer takes over unimpeded.
	fleet.peerRenews("web", true)
	final, all := h.runEvac(ch, 4, nil)
	if !final.OK {
		t.Fatalf("the peer holds and serves it: %v", transcriptOf(all))
	}
}

func TestReclaimSendsOverride(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, true, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })

	// The peer's lease lapses; the hold bar is still ours, so nothing here can
	// claim it — that is what reclaim is for.
	fleet.mu.Lock()
	fleet.leases["web"].holder = ""
	fleet.leases["web"].expiresAt = 0
	fleet.mu.Unlock()
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
		t.Fatalf("the bar keeps this host off its own service, got %v", p)
	}

	if err := h.sup.Reclaim("web"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	at := h.arb.nBatches()
	h.check()
	op := h.arb.opStructAt(at, "web")
	if op.Op != "renew" || !op.Override {
		t.Fatalf("reclaim is a renew with override, got %+v (ops %v)", op, h.arb.ops("web"))
	}
	// And the ordinary promote path runs from there: hooks and all.
	h.checks(2)
	mustContainInOrder(t, h.dk.calls(), []string{"create conch-hook-web-restore", "start cf-web"})
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("a reclaimed service promotes normally, got %v", p)
	}
	// One-shot: the override is not sent again.
	overrides := 0
	for i := 0; i < h.arb.nBatches(); i++ {
		if h.arb.opStructAt(i, "web").Override {
			overrides++
		}
	}
	if overrides != 1 {
		t.Fatalf("the override is one-shot, got %d", overrides)
	}
}

// ---------------------------------------------------------------------------
// The rest of the sequence's rules
// ---------------------------------------------------------------------------

func TestEvacuatingAServiceWeDoNotHoldIsANoOp(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	fleet.mu.Lock()
	fleet.leases["web"] = &fakeLease{holder: "host-b", serving: true, expiresAt: 1e9}
	fleet.mu.Unlock()
	h.arb.reply = fleet.reply
	h.livePeer()
	h.checks(3)

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final := EvacEvent{}
	var all []EvacEvent
	for _, ev := range drainEvents(ch) {
		all = append(all, ev)
		if ev.Done {
			final = ev
		}
	}
	if !final.Done || !final.OK {
		t.Fatalf("nothing to evacuate is a success, got %v", transcriptOf(all))
	}
	if !strings.Contains(transcriptOf(all), "not held") {
		t.Fatalf("a no-op is reported as such, got %v", transcriptOf(all))
	}
	if h.sup.Status()["web"].(map[string]any)["evacuating"] != false {
		t.Fatal("a service we do not hold is never marked evacuating")
	}
}

func TestEvacuateWaitsForAPromotionInFlight(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()

	// Wedge the restore hook so the evacuation lands mid-PROMOTING.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-restore"] = gate
	h.dk.mu.Unlock()

	h.checkUntilReadyToClaim("web")
	h.checkNoWait() // the claim is granted: PROMOTING, restore running
	awaitGate(t, h.dk.waitHit, "the restore hook to start")
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Promoting) {
		t.Fatalf("expected PROMOTING, got %v", p)
	}

	if _, err := h.sup.Evacuate([]string{"web"}, false, false, false); err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	// The evacuation waits: the drain hook must not run under a restore that is
	// still writing (spec §10: waits for, or cleanly aborts — never both).
	for i := 0; i < 3; i++ {
		h.checkNoWait()
	}
	for _, c := range h.dk.calls() {
		if c == "create conch-hook-web-drain" {
			t.Fatal("the drain hook must wait for the restore in flight")
		}
	}
	close(gate)
	h.waitIdle()

	h.check()
	found := false
	for _, c := range h.dk.calls() {
		if c == "create conch-hook-web-drain" {
			found = true
		}
	}
	if !found {
		t.Fatalf("once the restore finished the evacuation proceeds: %v", h.dk.calls())
	}
}

func TestAFailedDrainHookAbortsTheEvacuation(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()
	h.dk.mu.Lock()
	h.dk.waitCode["conch-hook-web-drain"] = 1 // the final sync fails
	h.dk.mu.Unlock()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 6, nil)
	if final.OK {
		t.Fatalf("a hand-off whose final sync failed is not a hand-off: %v", transcriptOf(all))
	}
	if got := h.arb.countOps("web", "release"); got != 0 {
		t.Fatalf("a failed drain releases nothing: the peer would restore from a replica "+
			"missing the very data the sync exists to hand over. releases: %d", got)
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the service goes on serving here: the evacuation failed, it did not happen")
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("still ACTIVE, got %v", p)
	}
}

// TestALongDrainIsNotCutShortByTheConfirmBudget is why the 120s budget bounds
// the POLL and not the whole sequence. Spec §9 says outright that a final sync
// may take longer than the TTL, and config's default hook timeout is 300s: an
// overall budget would abandon the drain of exactly the stateful service this
// feature exists for — and abandoning it mid-drain would hand the unit back to
// decide in STANDBY holding a `yours` verdict, which is rule 6, which stops
// the live service container and starts a restore on a host that was serving
// perfectly well.
func TestALongDrainIsNotCutShortByTheConfirmBudget(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-drain"] = gate
	h.dk.mu.Unlock()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.checkNoWait()
	awaitGate(t, h.dk.waitHit, "the drain hook to start")

	before := len(h.dk.mutations())
	h.clk.Advance(2 * evacuationTimeout) // a long, legitimate final sync
	at := h.arb.nBatches()
	h.checkNoWait()

	for _, ev := range drainEvents(ch) {
		if ev.Done {
			t.Fatalf("the drain has its own timeout; this budget is the peer poll's: %+v", ev)
		}
	}
	if op := h.arb.opStructAt(at, "web"); op.Op != "renew" || !op.Serving {
		t.Fatalf("the lease is renewed serving for as long as the sync takes, got %+v", op)
	}
	for _, c := range h.dk.mutations()[before:] {
		if c == "stop app-web" || c == "create conch-hook-web-restore" {
			t.Fatalf("a slow drain must never be turned into a promotion: %v", h.dk.mutations()[before:])
		}
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("still ACTIVE and still serving, got %v", p)
	}

	close(gate)
	h.waitIdle()
	final, all := h.runEvac(ch, 8, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("and the hand-off still completes: %v", transcriptOf(all))
	}
}

// TestALeaseLostMidDrainEndsTheEvacuation closes the window decide's rule 19
// opens: it hands off for EVERY reading that is not unreachable, so without
// the supervisor noticing, a renew refused mid-drain would leave our connector
// serving under a lease a peer already holds until the hooks happened to
// finish. Ending the sequence lets rule 15 stop it on the same check.
func TestALeaseLostMidDrainEndsTheEvacuation(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	// Our lease expired during an arbiter outage and the peer took it.
	fleet.mu.Lock()
	fleet.leases["web"] = &fakeLease{holder: "host-b", serving: true, expiresAt: 1e9}
	fleet.mu.Unlock()

	final, all := h.runEvac(ch, 3, nil)
	if final.OK {
		t.Fatalf("a lease lost mid-drain is not a hand-off we made: %v", transcriptOf(all))
	}
	if !strings.Contains(transcriptOf(all), "lost") {
		t.Fatalf("say what happened: %v", transcriptOf(all))
	}
	if h.dk.running("cf-web") {
		t.Fatal("a peer holds the lease: this connector must stop at once (rule 15)")
	}
	if got := h.arb.countOps("web", "release"); got != 0 {
		t.Fatalf("there is nothing to release: the lease is not ours. releases: %d", got)
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
		t.Fatalf("standing by, got %v", p)
	}
}

// TestASuspendEndsTheEvacuation: decide's rule 1 outranks rule 19, and it is
// about to stop every connector and re-read every lease. A hand-off cannot be
// driven across that gap.
func TestASuspendEndsTheEvacuation(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.check() // the drain hook runs and finishes

	h.clk.AdvanceWallOnly(h.cfg.Slop() + 10) // unknown time has passed
	final, all := h.runEvac(ch, 2, nil)
	if final.OK {
		t.Fatalf("an evacuation cannot outlive a suspend: %v", transcriptOf(all))
	}
	if !strings.Contains(transcriptOf(all), "suspend") {
		t.Fatalf("say why: %v", transcriptOf(all))
	}
	st := h.sup.Status()["web"].(map[string]any)
	if st["evacuating"] != false || st["release_owed"] != false {
		t.Fatalf("nothing may be left armed across a suspend, got %v", st)
	}
	if h.dk.running("cf-web") {
		t.Fatal("rule 1 stops the connector: every held lease is unproven")
	}
	for i := 0; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "web"); op.Op == "release" {
			t.Fatalf("batch %d released the lease anyway: %+v", i, op)
		}
	}
}

// TestReclaimNeverOutranksAReleaseObligation pins the ordering the ops loop
// gives the two. Reclaim() refuses up front while a release is owed, but the
// loop drops s.mu between building a service's op and committing the decision
// that arms the obligation, so a reclaim landing in that window sees nothing
// owed. The flag is therefore armed here directly, which is exactly the state
// that interleaving produces.
func TestReclaimNeverOutranksAReleaseObligation(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter) // demote: connector stopped, release owed

	h.sup.mu.Lock()
	owed := h.sup.units["web"].releasePending
	h.sup.units["web"].reclaim = true
	h.sup.mu.Unlock()
	if !owed {
		t.Fatal("this test needs a release obligation to be pending")
	}

	at := h.arb.nBatches()
	h.check()
	if op := h.arb.opStructAt(at, "web"); op.Op != "release" || op.Override {
		t.Fatalf("the obligation outranks the override: a renew here re-takes a lease we are "+
			"giving back. got %+v", op)
	}

	// And the one-shot was not silently swallowed: it fires once the lease is
	// actually ours to take again.
	next := h.arb.nBatches()
	h.check()
	if op := h.arb.opStructAt(next, "web"); op.Op != "renew" || !op.Override {
		t.Fatalf("a reclaim that had to wait still happens, got %+v (ops %v)", op, h.arb.ops("web"))
	}
}

// TestEvacuateAllLeavesAMidDemotionServiceAlone is why a service this host
// does not hold must not be *touched*, only reported. `evacuate --all` sweeps
// up services in every state, and one of those states is mid-demotion: the
// connector is already stopped, a release is owed, and the lease still reads
// `yours` until it lands. Retiring that unit through the ordinary finish path
// would drop the obligation and force STANDBY — and a STANDBY unit holding a
// `yours` verdict is decide's rule 6, which RESTORES and re-promotes the
// unhealthy service on the very host being evacuated.
func TestEvacuateAllLeavesAMidDemotionServiceAlone(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	// web goes sick and demotes: its connector stops and a release is owed but
	// has not been sent yet.
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter)
	if h.sup.Status()["web"].(map[string]any)["release_owed"] != true {
		t.Fatalf("this test needs web mid-demotion, got %v", h.sup.Status()["web"])
	}
	before := len(h.dk.mutations())
	releasesBefore := h.arb.countOps("web", "release")

	ch, err := h.sup.Evacuate(nil, true, false, false) // --all
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	if h.sup.Status()["web"].(map[string]any)["release_owed"] != true {
		t.Fatal("a release owed by a demotion is not the evacuation's to cancel")
	}

	h.checks(3)
	for _, c := range h.dk.mutations()[before:] {
		if c == "stop app-web" || c == "create conch-hook-web-restore" {
			t.Fatalf("the service we are giving up was restored and re-promoted on the host "+
				"being evacuated: %v", h.dk.mutations()[before:])
		}
	}
	if got := h.arb.countOps("web", "release") - releasesBefore; got != 1 {
		t.Fatalf("the demotion's own release must still go out exactly once, got %d (ops %v)",
			got, h.arb.ops("web"))
	}
	for i := 0; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "web"); op.Op == "release" && op.Evacuate {
			t.Fatalf("batch %d turned a demotion's release into an evacuation's: %+v", i, op)
		}
	}

	var all []EvacEvent
	for i := 0; i < 12 && len(all) == 0 || i < 12; i++ {
		fleet.peerRenews("blog", true)
		h.check()
		all = append(all, drainEvents(ch)...)
		done := false
		for _, ev := range all {
			done = done || ev.Done
		}
		if done {
			break
		}
	}
	if !strings.Contains(transcriptOf(all), "already being given back") {
		t.Fatalf("say what is actually true of that service: %v", transcriptOf(all))
	}
}

// TestEvacuateAllWalksAMixedFleet: one service held here, one held by the
// peer. The held one is handed over; the other is reported as the no-op it is,
// and nothing about it changes.
func TestEvacuateAllWalksAMixedFleet(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{holder: "host-b", serving: true, expiresAt: 1e9}
	fleet.mu.Unlock()
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveActive("web") // blog is the peer's and stays STANDBY

	blogBefore := h.sup.Status()["blog"].(map[string]any)
	ch, err := h.sup.Evacuate(nil, true, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("one hand-off and one no-op is a success: %v", transcriptOf(all))
	}
	if !strings.Contains(transcriptOf(all), "not held here") {
		t.Fatalf("the no-op must be reported as one: %v", transcriptOf(all))
	}
	if got := h.arb.countOps("blog", "release"); got != 0 {
		t.Fatalf("nothing was released for the service we never held, got %d", got)
	}
	after := h.sup.Status()["blog"].(map[string]any)
	if after["phase"] != blogBefore["phase"] || after["evacuating"] != false {
		t.Fatalf("the peer's service was touched: before %v, after %v", blogBefore, after)
	}
}

// TestEvacuateAllDrainsEveryServiceAtOnce: two drain chains in flight
// simultaneously is the normal shape of `evacuate --all`, and both must reach
// their own release with their own bar.
func TestEvacuateAllDrainsEveryServiceAtOnce(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	webGate, blogGate := make(chan struct{}), make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-drain"] = webGate
	h.dk.waitGate["conch-hook-blog-drain"] = blogGate
	h.dk.mu.Unlock()

	if _, err := h.sup.Evacuate(nil, true, false, false); err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.checkNoWait()
	awaitGate(t, h.dk.waitHit, "the first drain hook")
	awaitGate(t, h.dk.waitHit, "the second drain hook") // both chains, at once

	close(webGate)
	close(blogGate)
	h.waitIdle()
	h.checks(3) // stop, then release, for both

	for _, key := range []string{"web", "blog"} {
		found := false
		for i := 0; i < h.arb.nBatches(); i++ {
			if op := h.arb.opStructAt(i, key); op.Op == "release" {
				found = true
				if !op.Evacuate || op.BarTTL != 1800 {
					t.Fatalf("%s released without its own bar: %+v", key, op)
				}
			}
		}
		if !found {
			t.Fatalf("%s never released (ops %v)", key, h.arb.ops(key))
		}
	}
}

// TestAnEvacuatingHostDoesNotHonourAPendingReclaim: "an evacuating host claims
// nothing" is about the host, not about the services named in the evacuation,
// and a reclaim armed a moment earlier is a claim waiting to happen.
func TestAnEvacuatingHostDoesNotHonourAPendingReclaim(t *testing.T) {
	h := newHarness(t, evacConfig("web", "blog"), nil)
	fleet := newFakeFleet(h.clk)
	fleet.mu.Lock()
	fleet.leases["blog"] = &fakeLease{evacuatedBy: "host-a", barHold: true} // barred to us
	fleet.mu.Unlock()
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveActive("web") // blog is barred here and stays STANDBY

	if err := h.sup.Reclaim("blog"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	// Armed again mid-evacuation, which Reclaim itself refuses to do — the op
	// loop's own gate is what has to hold here.
	h.sup.mu.Lock()
	h.sup.units["blog"].reclaim = true
	h.sup.mu.Unlock()

	at := h.arb.nBatches()
	h.checks(4)
	for i := at; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "blog"); op.Override {
			t.Fatalf("batch %d claimed a lease while this host was evacuating: %+v", i, op)
		}
	}

	// The evacuation ends; the deferred reclaim is not swallowed.
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("the hand-off should have completed: %v", transcriptOf(all))
	}
	next := h.arb.nBatches()
	h.check()
	if op := h.arb.opStructAt(next, "blog"); op.Op != "renew" || !op.Override {
		t.Fatalf("a reclaim that had to wait still happens, got %+v (ops %v)", op, h.arb.ops("blog"))
	}
}

// TestAStaleStopConfirmationCannotReleaseEarly: stopConfirmed is set by every
// connector stop and cleared by nothing, so a service that demoted and later
// re-promoted carries a `true` from the previous cycle. If the evacuation
// trusted it, the release would go out while THIS stop was still draining —
// publishing a FREE lease a peer can claim while our connector is still up,
// which is the dual-serve window the whole obligation mechanism exists to
// close.
func TestAStaleStopConfirmationCannotReleaseEarly(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	// Demote (confirms a stop), release, then re-promote.
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)
	h.pr.setDenied("http://app-web/up", false)
	h.checks(h.cfg.Timing.PromoteAfter + 3)
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("this test needs web ACTIVE again, got %v (ops %v)", p, h.arb.ops("web"))
	}
	h.sup.mu.Lock()
	stale := h.sup.units["web"].stopConfirmed
	h.sup.mu.Unlock()
	if !stale {
		t.Fatal("this test needs a stale stop confirmation left by the earlier demotion")
	}

	// Now evacuate, with the connector's stop wedged.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["cf-web"] = gate
	h.dk.mu.Unlock()

	if _, err := h.sup.Evacuate([]string{"web"}, false, false, false); err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	h.check()       // the drain hook runs and finishes
	h.checkNoWait() // the connector stop is dispatched, and wedges
	awaitGate(t, h.dk.gateHit, "the executor to enter Stop")

	at := h.arb.nBatches()
	for i := 0; i < 4; i++ {
		h.checkNoWait()
	}
	for i := at; i < h.arb.nBatches(); i++ {
		if op := h.arb.opStructAt(i, "web"); op.Op == "release" {
			t.Fatalf("batch %d released while this evacuation's connector stop was still draining: "+
				"a stop confirmed by an earlier demotion proves nothing about this one. ops: %v",
				i, h.arb.ops("web"))
		}
	}

	close(gate)
	h.waitIdle()
	next := h.arb.nBatches()
	h.checkNoWait()
	if op := h.arb.opStructAt(next, "web"); op.Op != "release" || !op.Evacuate {
		t.Fatalf("once THIS stop completed the release must go out, got %+v (ops %v)",
			op, h.arb.ops("web"))
	}
}

func TestOnlyOneEvacuationAtATime(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	if _, err := h.sup.Evacuate([]string{"web"}, false, false, false); err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	if _, err := h.sup.Evacuate([]string{"web"}, false, false, false); err == nil {
		t.Fatal("a second evacuation must be refused while one is in flight")
	}
	if err := h.sup.Reclaim("web"); err == nil {
		t.Fatal("reclaim must be refused while an evacuation is in flight")
	}
	if _, err := h.sup.Evacuate([]string{"nope"}, false, false, false); err == nil {
		t.Fatal("an unknown service is an error, not a silent no-op")
	}
}

func TestEvacuateAndReclaimAreRefusedInADryRun(t *testing.T) {
	h := newHarness(t, evacConfig("web"), func(o *Options) { o.DryRun = true })
	h.livePeer()
	h.checks(3)
	if _, err := h.sup.Evacuate([]string{"web"}, true, false, true); err == nil {
		t.Fatal("spec §10: a dry run executes none of the actions it describes")
	}
	if err := h.sup.Reclaim("web"); err == nil {
		t.Fatal("a dry run must not send an override renew either")
	}
}
