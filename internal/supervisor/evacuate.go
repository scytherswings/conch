package supervisor

// Evacuation is spec §9's deliberate move, and the whole of it lives here.
//
// Two properties shape every line below.
//
//  1. THE SUPERVISOR OWNS THE SEQUENCE. The CLI only requests and watches: a
//     dropped SSH session must not leave a host half-evacuated with nothing
//     driving it. Evacuate() therefore does almost nothing — it validates, it
//     pre-flights, it marks units — and the loop advances the sequence at
//     check boundaries, exactly like every other decision conch makes. That is
//     also why nothing in here reads the clock off the loop goroutine: the
//     deadline is armed by the first check that sees the evacuation.
//
//  2. EVERY EXIT PATH ENDS THE EVACUATION. finishService is the only way a
//     service leaves the sequence, and it always clears unit.evacuating.
//     claimsSuppressed() scans every unit, so ONE leaked flag would mean this
//     host never claims another lease, silently, until it is restarted — the
//     worst failure this file can produce, and much worse than a hand-off that
//     reports failure honestly.
//
// The sequence itself, per service (spec §9):
//
//	drain   evacuate hooks run while the lease is RENEWED under them and the
//	        connector still serves — the example final-sync timeout exceeds
//	        the TTL, and a lease that expired mid-sync would let the peer
//	        restore from a replica missing the very data the sync exists to
//	        hand over.
//	stop    the connector goes down, and nothing is released until the stop
//	        escalation has actually completed (the release obligation's own
//	        precondition; see the op model in supervisor.go).
//	release the ordinary, sticky release obligation — with evacuate/hold/
//	        bar_ttl stamped on it. It is the same mechanism as a demotion's
//	        release for a reason: it already survives unreachable batches,
//	        replies that still read `yours`, and `not_holder` refusals.
//	confirm poll until a PEER holds the lease AND reports serving:true.
//	        Holding is not serving — its hooks may yet fail.
//
// What we read during `confirm` is `barred`, not `free`: view() answers the
// barred host with its own bar (arbiter/src/lease.js), so `free` and `yours`
// during confirm both mean the bar came off and the hand-off went wrong.

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/decide"
)

// evacuationTimeout is spec §9's "exit non-zero on timeout" budget, and it
// bounds the POLL: "then poll read until the peer holds the lease and reports
// serving:true … exit non-zero on timeout". It is deliberately not an overall
// budget for the whole sequence. Spec §9 says outright that a final sync may
// take longer than the TTL, and config's own default hook timeout is 300s, so
// a 120s cap on the drain would abandon every evacuation of exactly the
// stateful service this feature exists for. Each earlier step is bounded by
// something real already: the hooks by their own enforced timeouts (§8 rule
// 5), the connector stop by its stop/verify/kill escalation, and the release
// by the rollback that fires the moment the arbiter cannot be reached.
//
// It is a constant rather than a knob because it bounds an operator sitting at
// a terminal, not a safety property, and every timing knob that can be tuned
// is one more way to widen a window that must not widen.
const evacuationTimeout = 120.0

// evacEventBuffer is generous enough that a whole fleet's evacuation cannot
// fill it (a service emits ~5 lines), because emitting must never block the
// loop and a dropped line is a line an operator does not get to read.
const evacEventBuffer = 256

// EvacEvent is one line of an evacuation's progress, as the admin surface
// streams it. The last event of an evacuation always has Done set and carries
// the verdict; the channel is closed immediately after it.
type EvacEvent struct {
	Service string `json:"service,omitempty"`
	Message string `json:"message"`
	Done    bool   `json:"done,omitempty"`
	OK      bool   `json:"ok,omitempty"`
}

type evacStep int

const (
	evacDrain evacStep = iota
	evacStop
	evacRelease
	evacConfirm
	evacFinished
)

// evacUnit is one service's place in the sequence.
type evacUnit struct {
	step           evacStep
	hooksStarted   bool // the EVACUATE chain has been started (as opposed to a promotion's)
	stopDispatched bool
	// confirmAt is when this service's poll for a peer began, and the only
	// deadline in the sequence is measured from it.
	confirmAt float64
}

type evacuation struct {
	hold     bool
	units    map[string]*evacUnit
	live     int
	failures []string
	events   chan EvacEvent
}

// emit is best-effort by construction: the loop may never block on a consumer
// that has gone away (a dropped SSH session is the normal case, not the
// exceptional one). With evacEventBuffer slots the drop cannot happen in
// practice, and the Done event is what carries the verdict regardless.
func (e *evacuation) emit(ev EvacEvent) {
	select {
	case e.events <- ev:
	default:
	}
}

// evacTick collects the two things the sequence produces that must NOT happen
// under s.mu: executor actions (dispatched with the check's own decision, in
// one submit, so a newest-wins queue cannot drop them) and notifications
// (which call out to code that may want the lock back).
type evacTick struct {
	extra map[string][]decide.Action
	notes []evacNote
}

type evacNote struct {
	kind    string
	payload map[string]any
}

func newEvacTick() *evacTick {
	return &evacTick{extra: map[string][]decide.Action{}}
}

func (t *evacTick) flush(s *Supervisor) {
	for _, n := range t.notes {
		s.notify(n.kind, n.payload)
	}
	t.notes = nil
}

// Evacuate is spec §9's deliberate move: hand every named service to a peer
// and bar this host from taking them back. It returns as soon as the sequence
// is under way; the returned channel carries progress and, last, the verdict.
//
// The error return is for refusals only — nothing has been started when it is
// non-nil.
func (s *Supervisor) Evacuate(services []string, all, hold, force bool) (<-chan EvacEvent, error) {
	if s.dryRun {
		return nil, errors.New("this conch is running with --dry-run: it executes none of the actions it describes (spec §10)")
	}

	tick := newEvacTick()
	defer tick.flush(s)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evac != nil {
		return nil, errors.New("an evacuation is already in flight on this host")
	}
	keys, err := s.resolveLocked(services, all)
	if err != nil {
		return nil, err
	}
	if !force {
		if err := s.preflightLocked(); err != nil {
			return nil, err
		}
	}

	ev := &evacuation{
		hold:   hold,
		units:  make(map[string]*evacUnit, len(keys)),
		live:   len(keys),
		events: make(chan EvacEvent, evacEventBuffer),
	}
	s.evac = ev
	for _, key := range keys {
		ev.units[key] = &evacUnit{}
	}
	// An evacuating host claims nothing, ANYWHERE (spec §9), and a reclaim
	// armed a moment before this call is a claim waiting to happen — on any
	// service, not only the ones named here. Disarm every one-shot: Reclaim
	// refuses while an evacuation is in flight, so this closes the only window
	// left, and the operator can ask again afterwards.
	for _, u := range s.units {
		u.reclaim = false
	}
	// Marking is a second pass so that ev.units is complete before any service
	// can finish (a fleet where nothing is held finishes inside this call).
	for _, key := range keys {
		u := s.units[key]
		if why := notHeld(u); why != "" {
			// A service this host does not hold never ENTERS the sequence, so
			// nothing about it is touched: not its phase, not its release
			// obligation, not its hook status. `evacuate --all` sweeps up
			// services in every state, and one of those states is mid-demotion
			// — connector already stopped, release owed, lease still reading
			// `yours` until it lands. Retiring that unit through the ordinary
			// finish path would drop the obligation and force STANDBY, and a
			// STANDBY unit holding a `yours` verdict is decide's rule 6: the
			// unhealthy service we were giving up gets RESTORED and re-promoted
			// on the very host being evacuated, while the evacuation reports
			// success.
			s.retireServiceLocked(ev, tick, key, true, why)
			continue
		}
		u.evacuating = true
		ev.emit(EvacEvent{Service: key, Message: "evacuating"})
	}
	s.log.Info("evacuating", "services", strings.Join(keys, ","), "hold", hold, "force", force)
	return ev.events, nil
}

// Reclaim is spec §10's undo: the next batch sends this service's renew with
// override:true, which is the one thing that clears this host's own bar
// (arbiter/src/lease.js, renewOne). From there the ordinary promote path runs
// — hooks and all — because a lease granted in STANDBY is decide's rule 6 and
// nothing here is special-cased around it.
func (s *Supervisor) Reclaim(service string) error {
	if s.dryRun {
		return errors.New("this conch is running with --dry-run: it never sends a renew (spec §10)")
	}

	tick := newEvacTick()
	defer tick.flush(s)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evac != nil {
		return errors.New("an evacuation is in flight on this host: an evacuating host claims nothing")
	}
	keys, err := s.resolveLocked([]string{service}, false)
	if err != nil {
		return err
	}
	u := s.units[keys[0]]
	if u.releasePending {
		// An override renew would re-take a lease we are in the middle of
		// giving back, which is the one way this verb can cause the dual-serve
		// window the whole system exists to prevent.
		return fmt.Errorf("%s still owes a release: it cannot be reclaimed until that lands", keys[0])
	}
	u.reclaim = true
	tick.notes = append(tick.notes, evacNote{kind: "reclaim", payload: map[string]any{
		"service": keys[0],
		"host":    s.cfg.Host,
	}})
	s.log.Info("reclaiming: the next batch overrides this host's own evacuation bar", "service", keys[0])
	return nil
}

// resolveLocked turns operator-facing names (a service name or a lease key)
// into lease keys. An unknown name is an error rather than a silent no-op: a
// typo in `conch evacuate wbe` must not report a successful evacuation of
// nothing.
func (s *Supervisor) resolveLocked(services []string, all bool) ([]string, error) {
	if all {
		return append([]string(nil), s.order...), nil
	}
	if len(services) == 0 {
		return nil, errors.New("name at least one service, or pass --all")
	}
	var keys []string
	for _, name := range services {
		key := ""
		for _, k := range s.order {
			if k == name || s.units[k].svc.Name == name {
				key = k
				break
			}
		}
		if key == "" {
			return nil, fmt.Errorf("no service named %q is supervised here", name)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// preflightLocked is spec §9's pre-flight: refuse to start unless a peer has
// checked in recently. Evacuating into a void stops everything and strands it
// barred, which is the opposite of what the operator asked for.
func (s *Supervisor) preflightLocked() error {
	window := 3 * s.cfg.Timing.Interval
	for host, lastSeen := range s.lastPeers {
		if lastSeen <= window {
			s.log.Info("evacuation pre-flight: a peer is live", "peer", host, "last_seen", lastSeen)
			return nil
		}
	}
	return fmt.Errorf(
		"no peer has checked in with the arbiter within %.0fs: evacuating into a void would stop "+
			"every service here and strand it barred. Pass --force if you really do want this host down",
		window)
}

// notHeld answers "is there anything here to hand over", and says why not when
// there isn't. Evacuating a service this host does not hold is a no-op, and an
// accurate one matters: "not held here" is simply false for a service whose
// lease still reads `yours` because its release has not landed yet, and an
// operator reading that line would go looking for the wrong problem.
func notHeld(u *unit) string {
	switch {
	case u == nil:
		return "not supervised here: nothing to evacuate"
	case u.releasePending:
		return "this lease is already being given back (a release is owed): nothing to evacuate"
	case u.state.Phase != decide.Active && u.state.Phase != decide.Promoting:
		return "not held here: nothing to evacuate"
	case u.lastLease != decide.Yours:
		return "the lease does not read ours (" + string(u.lastLease) + "): nothing to evacuate"
	}
	return ""
}

// evacuateAdvance is the sequence's own check, run at the top of every loop
// iteration: hooks start here, the connector stop is dispatched here, the
// release obligation is armed here, and the overall deadline is enforced here.
func (s *Supervisor) evacuateAdvance(now float64, suspended bool, tick *evacTick) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ev := s.evac
	if ev == nil {
		return
	}

	for _, key := range s.order {
		eu := ev.units[key]
		if eu == nil || eu.step == evacFinished {
			continue
		}
		u := s.units[key]

		if suspended {
			// decide's rule 1 outranks everything, including rule 19: unknown
			// time has passed, every held lease is unproven, and the connectors
			// are going down. A hand-off cannot be driven across that gap — the
			// lease we were giving away may already be somebody else's — so the
			// sequence ends and spec §5's reconcile takes it from here.
			if eu.hooksStarted && u.hooks == hookRunning {
				// Our own drain chain. A promotion's restore is aborted by
				// rule 1 itself, which also records the torn-data evidence;
				// this one tears nothing local, so it is only cancelled.
				tick.extra[key] = append(tick.extra[key], decide.AbortHooks)
			}
			s.finishServiceLocked(ev, tick, key, false, true,
				"unknown time has passed (a suspend): the evacuation is abandoned and every held lease re-read")
			continue
		}

		if eu.step == evacConfirm && eu.confirmAt != 0 && now-eu.confirmAt >= evacuationTimeout {
			s.finishServiceLocked(ev, tick, key, false, false, fmt.Sprintf(
				"timed out after %.0fs: no peer is holding and serving it. This service is now down "+
					"and barred here — `conch reclaim %s` brings it back, or fix the peer",
				evacuationTimeout, u.svc.Name))
			continue
		}

		if eu.step == evacDrain {
			if !eu.hooksStarted {
				if u.hooks == hookRunning {
					// Spec §10: an evacuate arriving mid-PROMOTING waits for the
					// hook in flight. Waiting, not aborting — the two are
					// explicitly never both, and waiting keeps the restore's own
					// invariants (nothing half-written, no orphan evidence to
					// record) instead of creating a torn service on a host we
					// are about to walk away from. decide's rule 19 has already
					// frozen the promotion in place, so nothing starts under it
					// while we wait, and the restore's own hook timeouts (§8
					// rule 5) are what bound the wait — the 120s budget below
					// does not start until the poll does.
					continue
				}
				eu.hooksStarted = true
				if len(u.svc.Hooks.Evacuate) > 0 {
					u.hooks, u.hookErr = hookRunning, ""
					u.ex.startEvacuateHooks()
					ev.emit(EvacEvent{Service: key,
						Message: "running the evacuate hooks: the lease is renewed under them and the connector still serves"})
					continue
				}
				u.hooks, u.hookErr = "", ""
			}
			switch u.hooks {
			case hookRunning:
				continue
			case hookFailed:
				// The drain and the final sync are what make the peer's copy
				// current. Handing over anyway would let the peer serve a
				// replica missing exactly the data this hook existed to send,
				// so the evacuation fails and this host goes on serving —
				// which is safe, and visible, and reversible.
				s.finishServiceLocked(ev, tick, key, false, true, fmt.Sprintf(
					"the evacuate hooks failed (%s): the lease was NOT released and this host goes on serving",
					u.hookErr))
				continue
			}
			eu.step = evacStop
		}

		if eu.step == evacStop {
			if !eu.stopDispatched {
				eu.stopDispatched = true
				// A stop confirmed for some earlier demotion says nothing about
				// this one: the release obligation's precondition must be THIS
				// stop having completed.
				u.stopConfirmed = false
				tick.extra[key] = append(tick.extra[key], decide.StopConnector)
				ev.emit(EvacEvent{Service: key, Message: "the hooks are done: stopping the connector"})
				continue
			}
			if !u.stopConfirmed {
				continue
			}
			u.releasePending = true
			eu.step = evacRelease
			ev.emit(EvacEvent{Service: key, Message: "the connector is down: releasing the lease with the bar"})
		}
	}
}

// evacuateVerdicts is the other half of the sequence's check: what this
// batch's answers mean for a service mid-evacuation. It runs before the
// post-batch decision so a rollback is already in force when decide runs.
func (s *Supervisor) evacuateVerdicts(now float64, reply arbiter.Reply, tick *evacTick) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ev := s.evac
	if ev == nil {
		return
	}
	for _, key := range s.order {
		eu := ev.units[key]
		if eu == nil || eu.step == evacFinished {
			continue
		}
		u := s.units[key]
		v := reply.Services[key]

		// Before the release, the op is a renew, and any of the arbiter's three
		// not-ours answers means that renew was REFUSED: the lease we are in
		// the middle of handing over is already somebody else's. decide's rule
		// 19 hands off for every reading that is not Unreachable, so without
		// this the connector would go on serving under a lease a peer holds
		// until the hooks happened to finish — the dual-serve window rule 15
		// exists to close. Ending the sequence here lets rule 15 run on this
		// very check.
		if eu.step < evacRelease &&
			(v.Lease == decide.Held || v.Lease == decide.Free || v.Lease == decide.Barred) {
			if eu.hooksStarted && u.hooks == hookRunning {
				tick.extra[key] = append(tick.extra[key], decide.AbortHooks)
			}
			s.finishServiceLocked(ev, tick, key, false, true, fmt.Sprintf(
				"the lease was lost before the hand-off could be made: it reads %s", v.Lease))
			continue
		}

		switch eu.step {
		case evacRelease:
			switch {
			case v.Lease == decide.Unreachable && v.Err != "not_holder":
				// Spec §9: if the final release itself cannot be sent, the
				// evacuation fails — restart the connector, resume ACTIVE
				// renewal, report. A stopped service holding a ticking lease is
				// an outage nobody can end.
				tick.extra[key] = append(tick.extra[key], decide.StartConnector)
				s.finishServiceLocked(ev, tick, key, false, true,
					"the arbiter could not be reached to release the lease: restarting the connector "+
						"and resuming ACTIVE renewal here")
			case v.Released || v.Err == "not_holder" || v.Lease == decide.Free ||
				v.Lease == decide.Held || v.Lease == decide.Barred:
				eu.step, eu.confirmAt = evacConfirm, now
				ev.emit(EvacEvent{Service: key,
					Message: "the lease is released and barred: waiting for a peer to hold it and report serving"})
			}

		case evacConfirm:
			switch {
			case v.Lease == decide.Held && v.Serving:
				s.finishServiceLocked(ev, tick, key, true, false,
					"handed over to "+v.Holder+", which reports serving")
			case v.Lease == decide.Yours:
				// We never renew past the release, so this is the bar having
				// come off and something here having taken the lease back —
				// the hand-off did not happen.
				s.finishServiceLocked(ev, tick, key, false, false,
					"failed hand-off: this host holds the lease again rather than a peer")
			}
		}
	}
}

// finishServiceLocked is the ONLY way a service leaves the sequence.
//
// resume is the difference between the two shapes of ending, and it turns on
// ONE question: is the lease provably not ours any more?
//
//   - resume=false — it is not ours: the hand-off completed, or it timed out
//     after a confirmed release, or a peer's claim came back to us. Force
//     STANDBY, because the state we were holding describes a lease we gave
//     away.
//   - resume=true — we still hold it, or we cannot tell: the drain failed, the
//     release could not be delivered, a suspend intervened, or the renew was
//     refused mid-drain. Leave the phase alone and let this check's ordinary
//     decision run on the real readings — which is how the connector gets
//     restarted after a rollback, and stopped after a lost lease, without this
//     file second-guessing decide.
//
// Forcing STANDBY in the resume cases is the bug this distinction exists to
// prevent: a STANDBY unit holding a `yours` verdict is decide's rule 6, and
// rule 6 stops the service container and starts a restore — on a host that was
// serving perfectly well a moment earlier.
func (s *Supervisor) finishServiceLocked(ev *evacuation, tick *evacTick, key string, ok, resume bool, detail string) {
	if eu := ev.units[key]; eu == nil || eu.step == evacFinished {
		return
	}

	if u := s.units[key]; u != nil {
		u.evacuating = false
		u.hooks, u.hookErr = "", ""
		// The evacuation's release obligation dies with the evacuation, on
		// every path. An abandoned sequence must not leave one armed: it would
		// go out on a later check stripped of the evacuate/hold/bar_ttl fields
		// that were its entire purpose, giving away a lease this host is still
		// serving under. On the paths that succeeded it is already clear.
		u.releasePending, u.stopConfirmed = false, false
		if !resume {
			u.state = decide.State{Phase: decide.Standby, BackoffUntil: u.state.BackoffUntil}
			u.nextOp = opRead
		}
	}
	tick.notes = append(tick.notes, evacNote{kind: "evacuate", payload: map[string]any{
		"service": key,
		"host":    s.cfg.Host,
		"ok":      ok,
		"reason":  detail,
	}})

	s.retireServiceLocked(ev, tick, key, ok, detail)
}

// retireServiceLocked is the bookkeeping half of finishing: the progress line,
// the log, the failure tally, and the overall verdict once the last service is
// done. It touches the EVACUATION's state and never the unit's, which is what
// makes it usable on its own for a service that never entered the sequence —
// see the not-held branch in Evacuate.
func (s *Supervisor) retireServiceLocked(ev *evacuation, tick *evacTick, key string, ok bool, detail string) {
	eu := ev.units[key]
	if eu == nil || eu.step == evacFinished {
		return
	}
	eu.step = evacFinished

	ev.emit(EvacEvent{Service: key, Message: detail})
	if ok {
		s.log.Info("evacuation", "service", key, "detail", detail)
	} else {
		ev.failures = append(ev.failures, key+": "+detail)
		s.log.Error("evacuation failed: this host may still hold the service, check conch status",
			"service", key, "detail", detail)
	}

	ev.live--
	if ev.live <= 0 {
		verdict := "every named service was handed over"
		if len(ev.failures) > 0 {
			verdict = strings.Join(ev.failures, "; ")
		}
		ev.emit(EvacEvent{Done: true, OK: len(ev.failures) == 0, Message: verdict})
		close(ev.events)
		if s.evac == ev {
			s.evac = nil
		}
	}
}

// evacOp is the evacuation's contribution to one service's op for this batch.
type evacOp struct {
	// pastRelease is the invariant that keeps an evacuation from undoing
	// itself. decide's rule 19 names `renew` for every check of an evacuating
	// service — which is exactly right while the hooks drain under a live
	// lease, and catastrophic afterwards: a renew sent after the release would
	// re-take the lease we just gave back, on a host that is about to go away.
	pastRelease bool
	evacuate    bool
	hold        bool
	barTTL      float64
}

// evacOpLocked is called with s.mu held, from the op-building loop.
func (s *Supervisor) evacOpLocked(u *unit) evacOp {
	if s.evac == nil {
		return evacOp{}
	}
	eu := s.evac.units[u.key]
	if eu == nil || eu.step < evacRelease || eu.step == evacFinished {
		return evacOp{}
	}
	return evacOp{
		pastRelease: true,
		evacuate:    true,
		hold:        s.evac.hold,
		barTTL:      barTTL(u.svc, s.evac.hold),
	}
}

// barTTL turns a service's auto_reclaim_after into the release's bar_ttl.
//
// --hold sends NO bar_ttl at all, and that is not an optimisation. In
// arbiter/src/lease.js, expireIfDue clears the WHOLE bar once barExpiresAt
// passes — evacuatedBy, barExpiresAt and barHold together — so a hold bar with
// a ttl is simply a bar that stops holding after auto_reclaim_after. An absent
// bar_ttl leaves barExpiresAt null, which is the only shape that means "until
// a reclaim".
//
// An absent auto_reclaim_after is also 0, and means something different but
// equally correct: the bar has no clock on it, and the first peer claim clears
// it (renewOne clears a non-hold bar on any peer's claim).
func barTTL(svc config.Service, hold bool) float64 {
	if hold || svc.Evacuation.AutoReclaimAfter == "" {
		return 0
	}
	d, err := time.ParseDuration(svc.Evacuation.AutoReclaimAfter)
	if err != nil || d <= 0 {
		// config.Validate already reports this; here it can only mean a config
		// that was never validated, and a bar with no clock is the safe reading.
		return 0
	}
	return d.Seconds()
}

// isEvacuating is the post-batch pass's fresh read of the flag: a rollback
// decided from this batch's verdicts must be visible to this batch's decision,
// not one check later.
func (s *Supervisor) isEvacuating(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.units[key]; ok {
		return u.evacuating
	}
	return false
}
