package supervisor

import (
	"strings"
	"sync"

	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/decide"
)

// executor is one service's hands. It exists because of one line in spec §5:
// "stops must not queue behind the loop" — two services demoting with 35s
// stop timeouts must not delay a third service's renewal or its deadline
// stop. Every service therefore gets a goroutine of its own, and the loop's
// only interaction with it is a non-blocking submit.
//
// The queue is one slot deep and newest wins. A depth-1 queue is not a
// simplification, it is the correct semantics: if the loop has decided
// something new for this service while the previous batch was still being
// applied, the older batch describes a world that no longer exists. Actions
// within ONE decision are applied in order — decide emits them ordered
// (stop the service, then run the restore; start the service, then the
// connector) and that order is the whole point of them.
//
// The executor is also where the release obligation gets its evidence: a
// StopConnector that runs to the end of its escalation reports back to the
// unit, and only then may the loop send that service's release. The ordering
// guarantee in spec §5 is about the stop having HAPPENED, which is a fact
// only this goroutine can establish.
type executor struct {
	sup *Supervisor
	u   *unit

	mu      sync.Mutex
	cond    *sync.Cond
	pending []decide.Action
	// pendingResolved is the resolved service-container name (supervisor.go's
	// resolveContainer) that goes with pending: the check that produced this
	// batch of actions is the same check that resolved this name, and the two
	// travel together through this queue exactly like actions themselves,
	// guarded by e.mu rather than read off the unit at apply time. The unit's
	// own copy is overwritten every check by the loop goroutine; reading THAT
	// from here, unlocked, once this goroutine is off doing a slow Stop/Start
	// call, is the race this field exists to avoid — apply() gets a value
	// snapshotted at the moment its batch was popped, not whatever the loop
	// has since moved on to.
	pendingResolved string
	// pendingStop is why this batch's stops are happening (stops.go), and it
	// travels with pending for exactly the reasons pendingResolved does: the
	// decision that named these actions is the decision that knows which rule
	// asked for them, and apply() — whose switch sees nothing but
	// decide.Action values — could not work it out from the actions alone if
	// it wanted to. A StopConnector is a fence, a demotion, a refusal or a
	// hand-off depending entirely on context this goroutine does not have.
	pendingStop stopIntent
	// pendingStops is the label sweep's payload: container names to stop with
	// the connector escalation and nothing else (sweep.go). It rides this
	// queue rather than one of its own so the sweep inherits the property the
	// whole executor exists for — the loop never blocks on a stop — and it is
	// a SEPARATE field from pending rather than another action because its
	// payload is a list of names, and because the sweep runs on an executor of
	// its own (Supervisor.sweeper): a service executor's pendingStops is
	// always empty, and the sweeper's pending is always empty, so the two
	// payload kinds never have to share a slot's semantics.
	pendingStops []string
	queued       bool
	running      bool
	hooking      bool // a hook chain is in flight in its own goroutine
	closed       bool
}

func newExecutor(s *Supervisor, u *unit) *executor {
	e := &executor{sup: s, u: u}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *executor) start() { go e.loop() }

// submit hands one decision's actions over. It never blocks: that is the
// whole contract.
//
// Overwriting a batch that was queued but not yet applied is the intended
// semantics — an older decision describes a world that no longer exists — and
// for every action but one it is also harmless, because the loop re-derives
// what it wants from fresh readings every check and will name the action
// again if it still wants it. STANDBY re-issues StopConnector while it sees a
// connector up; ACTIVE re-issues StartConnector while it sees one down.
//
// AbortHooks is the exception, and it is carried forward rather than dropped.
// It is named ONCE, by the decision that abandons a promotion, and never
// again — the phase has left PROMOTING by the next check, so no later
// decision will re-issue it. Two arguments say a dropped abort is *usually*
// harmless: the chain may not have started (runPromoteHooks re-checks the
// committed phase before spawning, so an abandoned promotion never starts
// one), and the executor is normally idle when the abort arrives, since
// RunPromoteHooks returns as soon as it has spawned. But "usually" here means
// "unless two checks land inside one scheduler wakeup", which is a property
// of the interval and the Go runtime rather than of this code. A restore
// container writing under a service the supervisor has already abandoned is
// too expensive to leave resting on that, and hooks.Abort on a service with
// no chain in flight is a documented no-op (Task 10), so carrying it costs
// nothing.
// resolvedContainer is this batch's resolved service-container name (see
// pendingResolved), carried alongside actions so a queued-but-not-yet-
// applied batch that gets overwritten by a newer one also has its name
// overwritten in lockstep — an older batch describes a world that no longer
// exists, container name included.
//
// stop is this batch's stop reasons (stops.go), overwritten in the same
// lockstep and for the same reason: the reason belongs to the decision that
// named the actions, so a batch that supersedes another supersedes its
// explanation too.
func (e *executor) submit(actions []decide.Action, resolvedContainer string, stop stopIntent) {
	if len(actions) == 0 {
		return
	}
	e.mu.Lock()
	if e.queued && containsAction(e.pending, decide.AbortHooks) && !containsAction(actions, decide.AbortHooks) {
		actions = append([]decide.Action{decide.AbortHooks}, actions...)
	}
	e.pending = actions
	e.pendingResolved = resolvedContainer
	e.pendingStop = stop
	e.queued = true
	e.cond.Broadcast()
	e.mu.Unlock()
}

// submitStops hands the label sweep's stop list over, with submit's contract
// exactly: never blocks, newest wins. Newest-wins is right here for the same
// reason it is right for actions — the sweep re-derives its list from a fresh
// listing every check, so a batch that never got applied described a set of
// containers this host has since re-measured.
func (e *executor) submitStops(names []string) {
	if len(names) == 0 {
		return
	}
	e.mu.Lock()
	e.pendingStops = names
	e.queued = true
	e.cond.Broadcast()
	e.mu.Unlock()
}

func (e *executor) close() {
	e.mu.Lock()
	e.closed = true
	e.cond.Broadcast()
	e.mu.Unlock()
}

// waitIdle blocks until nothing is queued, nothing is being applied and no
// hook chain is in flight. Tests use it; the loop must not.
func (e *executor) waitIdle() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for e.queued || e.running || e.hooking {
		e.cond.Wait()
	}
}

func (e *executor) loop() {
	for {
		e.mu.Lock()
		for !e.queued && !e.closed {
			e.cond.Wait()
		}
		if e.closed && !e.queued {
			e.mu.Unlock()
			return
		}
		batch := e.pending
		resolvedContainer := e.pendingResolved
		stop := e.pendingStop
		stops := e.pendingStops
		e.pending, e.pendingResolved, e.pendingStops = nil, "", nil
		e.pendingStop = stopIntent{}
		e.queued, e.running = false, true
		e.mu.Unlock()

		e.apply(batch, resolvedContainer, stop)
		e.applyStops(stops)

		e.mu.Lock()
		e.running = false
		e.cond.Broadcast()
		e.mu.Unlock()
	}
}

// resolvedContainer is the service container name this specific batch's
// StopService/StartService must act on — this check's answer from
// supervisor.go's resolveContainer, snapshotted by loop() out of
// pendingResolved at the moment this batch was popped (see that field's
// comment for why it must not be read off the unit here instead). The
// connector actions below never use it: connector.container is never a
// prefix pattern (spec: Kamal accessories have stable names), so
// svc.Connector.Container is always the right, exact name.
//
// stop is why this batch's stops are happening, decided in commit (stops.go).
// Each stop below picks the field that names ITS intent: the two are not
// interchangeable, because one batch can carry both — rule 6 stops a stale
// service in the same decision rule 3 fences a connector — and each carries
// its own "was it actually running" reading beside it, since decide names
// both stops unconditionally and the escalation is therefore routinely run
// against a container that was already down. The accessories take stop.fence
// with an observed-up of true, which is the documented asymmetry: their
// reading is a count, not a per-container state, so "conch ordered this stop
// and the container is now down" is the strongest thing that batch knows.
func (e *executor) apply(actions []decide.Action, resolvedContainer string, stop stopIntent) {
	svc := e.u.svc
	for _, a := range actions {
		switch a {
		case decide.StopConnector:
			e.stopContainer(svc.Connector.Container, int(svc.Connector.StopTimeout),
				stop.fence, stop.orderedAt, stop.connectorUp)
			// The evidence the release obligation waits on: the stop
			// escalation has run to completion for this service's connector.
			e.sup.noteConnectorStopped(e.u.key)
		case decide.StartConnector:
			e.startContainer(svc.Connector.Container)
		case decide.StopService:
			e.stopContainer(resolvedContainer, int(svc.Connector.StopTimeout),
				stop.service, stop.orderedAt, stop.serviceUp)
		case decide.StartService:
			e.startContainer(resolvedContainer)
		case decide.StartAccessories:
			// In the order decide named them relative to the connector
			// (accessories first, rules 8 and 12): replication up before
			// traffic. Exact configured names always — an accessory is never
			// a prefix pattern — so there is nothing to resolve, and start
			// failures are surfaced by startContainer exactly as a
			// connector's would be.
			for _, name := range svc.Accessories {
				e.startContainer(name)
			}
		case decide.StopAccessories:
			// svc.Accessories alone, and that is the whole of why a warm
			// accessory is never stopped by conch: there is no action that
			// names one. See startWarmAccessories in supervisor.go.
			for _, name := range svc.Accessories {
				e.stopContainer(name, accessoryStopTimeout, stop.fence, stop.orderedAt, true)
			}
		case startWarmAccessories:
			// The supervisor's own action, prepended by commit on a check that
			// found one of these down (supervisor.go). Every configured warm
			// accessory, not only the ones observed down — the same shape as
			// StartAccessories above, since a start against a container that
			// is already running changes nothing — and exact configured names
			// always, because Validate refuses a pattern here too.
			for _, name := range svc.WarmAccessories {
				e.startWarmContainer(name)
			}
		case decide.RunPromoteHooks:
			e.runPromoteHooks(resolvedContainer)
		case decide.AbortHooks:
			e.abortHooks()
		case decide.Release:
			// Nothing to do here, deliberately. A release is an arbiter op,
			// not a docker call. Naming it here is how decide records the
			// OBLIGATION; the loop records it in commit and sends it on a
			// later batch, once the StopConnector above has actually
			// completed and not merely been dispatched (see the op model in
			// supervisor.go). Doing it any sooner would publish a FREE lease
			// while our connector was still draining.
		case decide.Alert:
			e.sup.notify("alert", map[string]any{
				"service": e.u.key,
				"host":    e.sup.cfg.Host,
				"phase":   string(e.sup.phaseOf(e.u.key)),
				"reason":  e.reasonOf(),
			})
		}
	}
}

// accessoryStopTimeout is the grace, in seconds, an accessory's stop gives
// it before the escalation's kill. Deliberately its own number rather than
// the connector's stop_timeout or docker's 10s default: replicators flush
// their last segments on SIGTERM, and 10s is too tight for that flush on a
// slow uplink — the very moment (a demotion) when losing the tail of the
// replica costs the most.
const accessoryStopTimeout = 30

// applyStops is the label sweep's whole hands: stop each container with the
// same stop/verify/kill/verify escalation a connector demotion gets, in the
// order the sweep listed them. There is no start, no hook, no lease op and no
// second verb of any kind here, deliberately — see sweep.go on why a sweep
// that can only stop is a sweep that cannot cause an outage.
//
// The timeout comes from this executor's unit, which for the sweeper is the
// synthetic one built in newSweeper: an unsupervised connector's own
// configured stop_timeout left with the config, so the default (35s) is the
// only honest budget left to give it.
//
// No stop reason travels with these, and that is the sweep being out of scope
// rather than an omission: a swept container belongs to no service in this
// config, and this executor's unit is the synthetic sweepScope, so there is no
// status entry it could honestly be recorded against (stops.go). The sweep
// says what it did in its own Warn and its own alert.
//
// The observed-up argument is true, and truthfully so: the sweep only ever
// lists a container it classified as sweepStop, and classify's first branch
// leaves anything not RUNNING alone (sweep.go). So these stops narrate as
// ordinary stops — it is the empty reason beside them, not a missing reading,
// that keeps them out of status.
func (e *executor) applyStops(names []string) {
	for _, name := range names {
		e.stopContainer(name, int(e.u.svc.Connector.StopTimeout), "", 0, true)
	}
}

func (e *executor) reasonOf() string {
	e.sup.mu.Lock()
	defer e.sup.mu.Unlock()
	return e.u.reason
}

// stopContainer is spec §9's wedged-daemon escalation: stop, verify, kill,
// verify, then the loudest notification conch has. The verifies are not
// optional politeness — the stop deadline's "provably stopped" claim runs
// through the same daemon whose failure often caused the demotion, so a
// successful-looking Stop is not evidence of a stopped container.
//
// timeout is the caller's, because the honest grace differs by what is being
// stopped: a connector drains at its configured stop_timeout, an accessory
// flushes at accessoryStopTimeout.
//
// why is which rule asked for this stop, recorded against the container for
// status (stops.go), and orderedAt is the wall clock of the check that asked.
// An empty why records nothing at all — the label sweep's stops, which belong
// to no service.
//
// observedUp is whether the check that ORDERED this stop saw the container
// running. decide names StopConnector and StopService unconditionally (rules
// 14 and 6), so this escalation is routinely run against a container that had
// already fallen over, and both surfaces have to say the same thing about
// that. Nothing is claimed until the escalation confirms the container is
// down; then:
//
//   - observedUp: conch is what transitioned it. "stopped container", and a
//     status row if why names one.
//   - not observedUp: the stop was ordered and ran, but the container was
//     already down. It still narrates — a docker call conch made is a thing
//     conch did — in wording that claims no transition, and it writes NO row,
//     which is the same gate stops.go describes.
//
// Two conditions, then, not one: observedUp keeps a crashed connector from
// being credited to a demotion, and an empty why keeps the sweep out of a
// service's status. The sweep passes observedUp=true (it only stops what it
// listed as running) precisely so it keeps the plain wording while recording
// nothing.
//
// Both gates sit downstream of confirmation: a dry run returns before
// anything is stopped, and a stop the escalation could not confirm falls
// through to the alert saying nothing at all. "conch stopped this" must never
// be said about a container conch did not observe stop.
func (e *executor) stopContainer(name string, timeout int, why stopReason, orderedAt float64, observedUp bool) {
	if name == "" {
		return
	}
	if e.sup.dryRun {
		e.sup.log.Info("dry run: would stop container", "service", e.u.key, "container", name)
		return
	}
	// confirmed is the one place a stop is narrated: both verify branches below
	// reach it, and nothing outside it may say a stop happened.
	confirmed := func() {
		if !observedUp {
			e.sup.log.Info("the container was already down when conch went to stop it",
				"service", e.u.key, "container", name)
			return
		}
		e.sup.log.Info("stopped container", "service", e.u.key, "container", name)
		e.sup.noteStopped(e.u.key, name, why, orderedAt)
	}
	if err := e.sup.dk.Stop(name, timeout); err != nil {
		e.sup.log.Warn("docker stop returned an error: escalating to kill",
			"service", e.u.key, "container", name, "error", err)
	}

	// Info fires only on a VERIFIED stop: an Inspect that errored is not
	// evidence the container is down (this function's own doc comment) — it
	// falls through to the exact same escalation as still-running, but never
	// claims a stop this executor could not confirm.
	state, err := e.sup.dk.Inspect(name)
	if err != nil {
		return
	}
	if !state.Running {
		confirmed()
		return
	}
	e.sup.log.Warn("the container is still running after a stop: killing it",
		"service", e.u.key, "container", name)
	if err := e.sup.dk.Kill(name); err != nil {
		e.sup.log.Warn("docker kill returned an error: re-checking whether the container is still running",
			"service", e.u.key, "container", name, "error", err)
	}

	state, err = e.sup.dk.Inspect(name)
	if err != nil {
		return
	}
	if !state.Running {
		confirmed()
		return
	}
	e.sup.log.Error("the container survived stop and kill: check the docker daemon on this host",
		"service", e.u.key, "container", name)
	e.sup.notify("alert", map[string]any{
		"service":   e.u.key,
		"host":      e.sup.cfg.Host,
		"container": name,
		"reason":    "the container survived both stop and kill: the docker daemon cannot stop it",
	})
}

// startWarmContainer is startContainer with its FAILURE narration latched per
// container. The two differ because the two starts differ in cadence: an
// ordinary start is named by a decision, on a check that changed something,
// while a warm start is re-derived on every check that finds the container
// down — so a container that exists and cannot start (a bad mount, a port
// already taken) would otherwise log an Error and page a human once per
// interval, forever, on both hosts at once. The ATTEMPT is not latched, only
// the telling: retrying is the reconcile working, and the latch clears on the
// first start that succeeds, so a failure that comes back is news again.
//
// The same shape as this package's other streak latches (the arbiter's, the
// sweep's), with one difference: this one runs on executor goroutines rather
// than the loop's, so its map is under s.mu rather than loop-goroutine-only.
func (e *executor) startWarmContainer(name string) {
	if name == "" {
		return
	}
	if e.sup.dryRun {
		e.sup.log.Info("dry run: would start container", "service", e.u.key, "container", name)
		return
	}
	started, err := e.sup.dk.Start(name)
	if err != nil {
		if !e.sup.noteWarmStartFailed(name) {
			return // same failure, still failing: said once already
		}
		e.sup.log.Error("docker start failed for a warm accessory: check this container's own logs on this host, conch keeps retrying every check",
			"service", e.u.key, "container", name, "error", err)
		e.sup.notify("alert", map[string]any{
			"service":   e.u.key,
			"host":      e.sup.cfg.Host,
			"container": name,
			"error":     err.Error(),
			"reason":    "a warm accessory conch was told to start did not start",
		})
		return
	}
	// The latch clears on any start that did not fail, INCLUDING a 304: the
	// latch is about failure narration, and a container that is up is a
	// container that is no longer failing, however it got there. Clearing it
	// only on a transition would leave a container that recovered on its own
	// still latched, and swallow the next genuine failure.
	e.sup.noteWarmStartOK(name)
	if !started {
		return // already running: startWarmAccessories starts the whole list
	}
	e.sup.log.Info("started container", "service", e.u.key, "container", name)
}

// startContainer runs one start and narrates only what it CHANGED. That is
// why dockerapi.Start reports the daemon's already-running 304 distinctly:
// StartAccessories and startWarmAccessories each start a whole configured
// list whenever any ONE of its members is found down, so a sibling that never
// stopped would otherwise log "started container" on every check for as long
// as its neighbour crash-loops — a log full of starts that did not happen, on
// exactly the checks an operator is reading hardest. The start itself is
// still issued: reconciling towards a state costs one idempotent call and
// heals a stale reading, and it is only the claim that is gated.
func (e *executor) startContainer(name string) {
	if name == "" {
		return
	}
	if e.sup.dryRun {
		e.sup.log.Info("dry run: would start container", "service", e.u.key, "container", name)
		return
	}
	started, err := e.sup.dk.Start(name)
	if err != nil {
		e.sup.log.Error("docker start failed: check this container's own logs on this host",
			"service", e.u.key, "container", name, "error", err)
		e.sup.notify("alert", map[string]any{
			"service":   e.u.key,
			"host":      e.sup.cfg.Host,
			"container": name,
			"error":     err.Error(),
			"reason":    "a container conch was told to start did not start",
		})
		return
	}
	if !started {
		return // already running: nothing transitioned, so there is no event
	}
	e.sup.log.Info("started container", "service", e.u.key, "container", name)
}

// runPromoteHooks starts the hook chain in a goroutine of its own rather than
// on the executor. Hooks run for minutes on a stateful service, and the one
// action that must reach this service during those minutes is AbortHooks —
// which would otherwise sit in the queue behind the very chain it is meant to
// cancel. hooks.Abort is built to be called from another goroutine while Run
// is in flight (Task 10), and this is that goroutine.
//
// resolvedContainer is this batch's resolved service-container name — the
// same value apply's own parameter carries, this check's answer from
// resolveContainer, snapshotted out of the executor's queue before the
// StopService above ever ran (see apply's own comment on why that snapshot,
// not a fresh read off the unit, is the only race-free source). It is
// substituted for every literal "{container}" in each promote hook's Argv,
// by substitutePromoteArgv below, right here at the call site rather than
// inside hooks.Runner.Run — deliberately: Run's signature (and the
// hookRunner interface) stay exactly as they were, which keeps the property
// that makes the evacuate side of this feature refuse the placeholder rather
// than silently drop it: startEvacuateHooks (below) has no resolved
// container in scope AT ALL, because it runs off the check path this value
// comes from, and it calls Run with its hooks' Argv completely untouched.
// There is no code path anywhere that could feed a resolved name to an
// evacuate hook even by accident.
func (e *executor) runPromoteHooks(resolvedContainer string) {
	if e.sup.dryRun {
		e.sup.log.Info("dry run: would run the promote hooks", "service", e.u.key,
			"hooks", len(e.u.svc.Hooks.Promote))
		e.sup.setHookStatus(e.u.key, hookDone, "")
		return
	}

	e.mu.Lock()
	if e.hooking {
		e.mu.Unlock()
		return // a chain is already in flight; decide only asks once per promotion
	}
	e.mu.Unlock()

	// The loop commits state before it dispatches, so this reads the phase the
	// decision that queued these actions produced — or a newer one. If the
	// promotion has already been abandoned (a deadline reached, the lease
	// lost, an abort queued behind us), starting the chain now would mean a
	// restore writing under a service the supervisor has given up, with an
	// Abort that already ran and matched nothing.
	if e.sup.phaseOf(e.u.key) != decide.Promoting {
		e.sup.log.Info("the promotion was abandoned before its hooks started", "service", e.u.key)
		return
	}

	e.mu.Lock()
	e.hooking = true
	e.mu.Unlock()

	go func() {
		specs := substitutePromoteArgv(e.u.svc.Hooks.Promote, resolvedContainer)
		result := e.sup.hooks.Run(e.u.key, specs)
		status, detail := hookDone, ""
		if !result.OK {
			status, detail = hookFailed, result.FailedHook+": "+result.Detail
			e.sup.log.Error("a promote hook failed: this promotion is abandoned, the hook's own output is in detail",
				"service", e.u.key, "hook", result.FailedHook, "detail", result.Detail)
		}
		e.sup.setHookStatus(e.u.key, status, detail)

		e.mu.Lock()
		e.hooking = false
		e.cond.Broadcast()
		e.mu.Unlock()
	}()
}

// substitutePromoteArgv returns specs with every literal "{container}" in
// each hook's Argv replaced by resolvedContainer — the SAME placeholder,
// same strings.ReplaceAll, same this-check's resolved name that readings
// substitutes into svc.Health.URL (supervisor.go), a different consumer of
// the identical value. Config validation (internal/config/validate.go's
// hookArgvPlaceholderFault) already refuses every near-miss casing
// ({Container}, ${container}, {{container}}) at load time, so the only
// placeholder shape that ever reaches here is the exact one this line
// substitutes.
//
// specs is never mutated in place: HookSpec.Argv is a slice this service's
// config owns for the life of the process, read again on every promotion,
// and a substitution that clobbered it in place would leave the SECOND
// promotion substituting into an already-substituted argv (or, worse, racing
// a concurrent read of the same slice from another goroutine). A fresh
// []config.HookSpec, with a fresh []string Argv wherever a replacement
// actually happened, is copy-on-write's whole point here.
//
// A hook with no Argv at all (nil) is left with a nil Argv, not promoted to
// a non-nil empty slice: dockerapi.RunSpec.Argv reaches the Engine API's Cmd
// field, where nil and an empty-but-present array are not always the same
// thing to docker, and "no placeholder" must mean "byte-for-byte untouched,"
// full stop, not "untouched except for this allocation." Defensive:
// unreachable via Validate-passed configs — validateHooks faults empty Argv
// for both phases, so a config that cleared Validate never has a HookSpec
// with a nil/empty Argv in the first place.
func substitutePromoteArgv(specs []config.HookSpec, resolvedContainer string) []config.HookSpec {
	out := make([]config.HookSpec, len(specs))
	for i, spec := range specs {
		out[i] = spec
		if len(spec.Argv) == 0 {
			continue
		}
		argv := make([]string, len(spec.Argv))
		for j, a := range spec.Argv {
			argv[j] = strings.ReplaceAll(a, "{container}", resolvedContainer)
		}
		out[i].Argv = argv
	}
	return out
}

// startEvacuateHooks runs the drain-and-final-sync chain, in a goroutine of
// its own for the same reason a promotion's restore gets one: it runs for
// minutes, and the executor's queue must stay free for the connector stop that
// follows it. It is called by the loop rather than reached through an action
// because an evacuation is a supervisor-owned sequence — decide never names
// it, and the evacuation record is the only authority on whether it should
// run. The caller (evacuate.go, under s.mu) has already established that this
// service is ours and that no other chain is in flight, and has set the unit's
// hook status to "running"; this method only takes the executor's own lock, so
// it can be called with s.mu held.
//
// Deliberately NOT substitutePromoteArgv: an evacuation runs off this
// service's own record (evacuate.go), never through readings/resolveContainer
// at all, so there is no resolved container name in scope here to substitute
// even if this method wanted to — which is exactly why a "{container}" in
// evacuate.Hooks[].Argv is a config-validation fault (validate.go's
// hookArgvPlaceholderFault) rather than a runtime no-op: left unrefused,
// it would reach docker as the eleven-character literal string "{container}",
// not as any container this host actually has.
func (e *executor) startEvacuateHooks() {
	e.mu.Lock()
	if e.hooking {
		e.mu.Unlock()
		return
	}
	e.hooking = true
	e.mu.Unlock()

	go func() {
		result := e.sup.hooks.Run(e.u.key, e.u.svc.Hooks.Evacuate)
		status, detail := hookDone, ""
		if !result.OK {
			status, detail = hookFailed, result.FailedHook+": "+result.Detail
			e.sup.log.Error("an evacuate hook failed: this service is not handed over, the hook's own output is in detail",
				"service", e.u.key, "hook", result.FailedHook, "detail", result.Detail)
		}
		e.sup.setHookStatus(e.u.key, status, detail)

		e.mu.Lock()
		e.hooking = false
		e.cond.Broadcast()
		e.mu.Unlock()
	}()
}

func (e *executor) abortHooks() {
	if e.sup.dryRun {
		e.sup.log.Info("dry run: would abort the promote hooks", "service", e.u.key)
		return
	}
	e.sup.hooks.Abort(e.u.key)
}
