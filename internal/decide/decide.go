// Package decide is the whole decision for one service on one host: a pure
// function from (state, this check's readings, policy) to what to do next.
// It imports nothing but fmt — no clock, no docker, no arbiter — so the state
// machine of spec §5 runs at full speed in tests, and the one hard rule that
// the whole system exists to keep, never serve past your lease, can be read
// off a single file instead of inferred from I/O.
package decide

import "fmt"

// Phase is where this service is in its life on this host (spec §5).
// Evacuation is a supervisor-driven sequence, not a resting state, and the
// post-evacuation bar lives in the arbiter, so neither is a phase here.
type Phase string

const (
	Standby   Phase = "STANDBY"
	Promoting Phase = "PROMOTING"
	Active    Phase = "ACTIVE"
)

// LeaseState is what the arbiter said about our lease this check — including
// the two answers that are not the arbiter's opinion at all: Unreachable (we
// never got one) and Conflict (two processes are answering to one host name).
type LeaseState string

const (
	Yours       LeaseState = "yours"
	Held        LeaseState = "held"
	Free        LeaseState = "free"
	Barred      LeaseState = "barred"
	Unreachable LeaseState = "unreachable"
	Conflict    LeaseState = "conflict" // duplicate_host
)

// State is everything conch remembers between checks about one service. It is
// deliberately tiny and in-memory only: every shutdown, reboot and redeploy
// case falls out of lease expiry rather than out of stored client state (§9).
type State struct {
	Phase        Phase
	OKStreak     int     // standby: consecutive claimable checks
	BadStreak    int     // active: consecutive unhealthy checks
	LeaseExpiry  float64 // mono seconds; 0 = unarmed, which proves nothing
	BackoffUntil float64 // mono; standby may not claim before this (hook failure)
	HooksDone    bool    // promoting: hooks finished, containers may start
	LossAlerted  bool    // active: this stretch of arbiter silence was already signalled
}

// Inputs are this check's readings. Health, eligibility and the lease answer
// are all measured fresh every check (spec §4) — nothing here is remembered.
type Inputs struct {
	NowMono   float64
	Suspended bool // wall/mono divergence beyond slop detected this check
	Healthy   bool // host + service health (spec §4), connector-ready folded in
	Eligible  bool // spec §4 eligibility
	Lease     LeaseState
	// LeaseTTL is the granted ttl, and a Yours reading must carry it: a
	// TTL-less Yours is not a grant, so it leaves the previous deadline in
	// force rather than overwriting a deadline we proved earlier.
	LeaseTTL    float64
	SentAt      float64 // mono when the batch was sent
	ConnectorUp *bool   // nil = docker couldn't say
	ServiceUp   *bool
	// AccessoriesUp is how many of the service's configured accessory
	// containers (replication sidecars and their kin) are running, out of
	// AccessoriesTotal. nil = docker couldn't say for at least one of them,
	// and a count we could not finish justifies no accessory action at all —
	// the same conservatism as ConnectorUp. Two questions get asked of it:
	// ACTIVE asks "are all of them up" (anything less gets started, rule 12),
	// everything that stops the connector asks "is any of them up" (anything
	// running gets stopped) — an accessory replicating on a host that is not
	// serving is a second writer on the replica the serving host owns.
	AccessoriesUp    *int
	AccessoriesTotal int
	HookStatus       string // "", "running", "done", "failed"
	Evacuating       bool   // this service is mid-evacuation (supervisor-owned sequence)
}

// Policy is the timing model as plain numbers. Four knobs and one derived
// deadline (spec §6); a fifth knob that could quietly widen the window in
// which two hosts both serve is exactly the knob that must not exist.
type Policy struct {
	DemoteAfter, PromoteAfter int
	Interval, StopDeadline    float64 // StopDeadline is per-service (spec §6)
	FreezeOnLoss              bool    // arbiter.on_loss == "freeze"
	BackoffSeconds            float64 // after hook failure; 3*PromoteAfter*Interval
}

// Action is one side effect for the supervisor to carry out. Decide only ever
// names them; it never performs one.
type Action string

const (
	StartService     Action = "START_SERVICE"
	StopService      Action = "STOP_SERVICE"
	StartConnector   Action = "START_CONNECTOR"
	StopConnector    Action = "STOP_CONNECTOR"
	StartAccessories Action = "START_ACCESSORIES"
	StopAccessories  Action = "STOP_ACCESSORIES"
	RunPromoteHooks  Action = "RUN_PROMOTE_HOOKS"
	AbortHooks       Action = "ABORT_HOOKS"
	Release          Action = "RELEASE"
	Alert            Action = "ALERT"
)

// Decision is the whole answer: the state to carry into the next check, the
// side effects, the arbiter op for the NEXT batch ("renew", "read" or
// "release"), and why — the reason is what a human reads at 3am.
type Decision struct {
	State   State
	Actions []Action
	NextOp  string
	Reason  string
}

// Decide runs one check for one service. Order is fixed: the two rules that
// hold in every phase, then the evacuation hands-off, then lease bookkeeping,
// then the phase itself.
func Decide(s State, in Inputs, p Policy) Decision {
	// Going to STANDBY is always the safe answer, so every abandonment shares
	// this shape: streaks and hook progress describe a life we are no longer
	// living, and an expiry we no longer hold proves nothing. The backoff is
	// the one thing that outlives the transition — it exists precisely to stop
	// us coming straight back.
	standby := func(actions []Action, op, reason string) Decision {
		return Decision{
			State:   State{Phase: Standby, BackoffUntil: s.BackoffUntil},
			Actions: actions,
			NextOp:  op,
			Reason:  reason,
		}
	}
	// An abandoned promotion also arms the backoff: a host whose hooks just
	// failed must not re-claim in a tight claim/restore/release loop against a
	// healthy peer (spec §5).
	abandon := func(actions []Action, reason string) Decision {
		d := standby(actions, "release", reason)
		d.State.BackoffUntil = in.NowMono + p.BackoffSeconds
		return d
	}
	// Hooks are the one thing in flight that a phase-independent rule has to
	// clean up after itself.
	aborting := func() []Action {
		if s.Phase == Promoting {
			return []Action{AbortHooks}
		}
		return nil
	}
	// Every path that stops (or keeps stopped) the connector owes the
	// accessories the same answer: a replication sidecar still running where
	// the service does not serve is a second writer on the replica the serving
	// host owns, which corrupts the very copy a failover would restore from.
	// So the connector stop and the accessory stop travel together, with one
	// difference — the accessory half is a reconcile conditioned on a reading
	// we actually have (any accessory RUNNING), never a reflex, because nil
	// means docker could not say and a reading we do not have justifies
	// nothing.
	stops := func(rest ...Action) []Action {
		acts := []Action{StopConnector}
		if in.AccessoriesUp != nil && *in.AccessoriesUp > 0 {
			acts = append(acts, StopAccessories)
		}
		return append(acts, rest...)
	}

	// Default-deny on the lease reading itself. Every rule below is written
	// against the six answers the arbiter can give; a seventh — the zero value,
	// a typo, a protocol change — must not fall through the guards into the
	// serving path, where it would mean holding a connector up with no deadline
	// ever tested. A reading we cannot recognise is a reading we do not have,
	// which is exactly what Unreachable means, so it earns the same rules: the
	// stop deadline under `stop`, holding shape under `freeze`.
	switch in.Lease {
	case Yours, Held, Free, Barred, Unreachable, Conflict:
	default:
		in.Lease = Unreachable
	}

	// Rule 1 — suspend. Monotonic clocks do not advance while a machine is
	// asleep, so a wall/mono divergence beyond slop means unknown time has
	// passed and every held lease is unproven. Stop connectors first, then
	// re-read the arbiter and reconcile.
	if in.Suspended {
		return standby(append(aborting(), stops(Alert)...), "read",
			"unknown time has passed: every held lease is unproven")
	}

	// Rule 2 — duplicate_host. Two processes answering to one host name cannot
	// both be trusted with a lease, so both fail closed. Nothing is released:
	// the arbiter refuses everything from a conflicted name anyway.
	if in.Lease == Conflict {
		return standby(append(aborting(), stops(Alert)...), "read",
			"two processes share this host name: failing closed")
	}

	// Rule 19 — an evacuation is a supervisor-owned sequence (spec §9), not a
	// resting state: its hooks drain and sync while the connector still serves
	// and the lease is renewed under them. Decide must not fight it, and an
	// evacuating host claims nothing, however free a lease reads.
	//
	// But an evacuation cannot outlive contact with the arbiter. Spec §9 is
	// explicit that losing the arbiter ends the evacuation — restart the
	// connector, resume ACTIVE renewal, report — because a hand-off nobody can
	// confirm is not a hand-off, and a stopped service holding a ticking lease
	// is an outage nobody can end. So the hands-off applies only while we are
	// still getting answers; once the reading is Unreachable the ordinary rules
	// take over, and in `stop` mode that includes the deadline. The supervisor
	// owns the other half — ending the sequence and reporting it (Task 11) —
	// but the deadline holds here whether or not it does.
	if in.Evacuating && in.Lease != Unreachable {
		return Decision{State: s, NextOp: "renew",
			Reason: "evacuating: the supervisor drives this sequence"}
	}

	// Lease bookkeeping, before any deadline is tested. Expiry is anchored at
	// the moment the request was SENT, never at receipt, so response latency
	// can only shorten the lease we believe we hold, never extend it (spec §6).
	// Doing it here is what lets a renewal that arrives in the lease's last
	// moment save the host instead of stopping it. A grant carrying no ttl is
	// not a grant, and must not overwrite an expiry we proved earlier.
	if in.Lease == Yours && in.LeaseTTL > 0 {
		s.LeaseExpiry = in.SentAt + in.LeaseTTL
	}
	// Any answer at all is contact restored: re-arm the loss signal, so the
	// next stretch of silence gets its own alert.
	if in.Lease != Unreachable {
		s.LossAlerted = false
	}

	switch s.Phase {

	case Standby:
		var acts []Action
		// Rule 3 — reconcile on every check, not only at startup: a connector
		// something else started here (a kamal accessory reboot on the wrong
		// host) must not go on serving indefinitely (spec §4). nil means docker
		// could not say, and a reading we do not have justifies nothing. The
		// same rule, for the same reason, covers the service's accessories: a
		// replication sidecar a reboot resurrected on the standby writes the
		// replica the active host's sidecar owns.
		if in.ConnectorUp != nil && *in.ConnectorUp {
			acts = append(acts, StopConnector)
		}
		if in.AccessoriesUp != nil && *in.AccessoriesUp > 0 {
			acts = append(acts, StopAccessories)
		}
		// Rule 6 — the claim was granted. Stop the service container first if
		// a deploy or a reboot left it running: a restore must never write
		// under a live process (spec §5).
		if in.Lease == Yours {
			return Decision{
				State:   State{Phase: Promoting, LeaseExpiry: s.LeaseExpiry, BackoffUntil: s.BackoffUntil},
				Actions: append(acts, StopService, RunPromoteHooks, Alert),
				NextOp:  "renew",
				Reason:  "the lease was granted: stopping the service before the restore",
			}
		}
		// Rule 4 — eligible, free, and off the backoff: tick. Claiming takes
		// promote_after consecutive good checks and promote_after is
		// deliberately larger than demote_after, so a flapping link is quick
		// to stop serving and slow to take over (spec §6).
		if in.Eligible && in.Lease == Free && in.NowMono >= s.BackoffUntil {
			s.OKStreak++
			op := "read"
			if s.OKStreak >= p.PromoteAfter {
				op = "renew" // send the claim in the next batch
			}
			return Decision{State: s, Actions: acts, NextOp: op,
				Reason: fmt.Sprintf("free and eligible: %d/%d consecutive good checks",
					s.OKStreak, p.PromoteAfter)}
		}
		// Rule 5 — any other reading resets it, and the streak has to be
		// consecutive to mean anything.
		s.OKStreak = 0
		reason := fmt.Sprintf("standing by: the lease reads %s, eligible=%v", in.Lease, in.Eligible)
		if in.NowMono < s.BackoffUntil {
			reason = "standing by: backed off after a failed promotion"
		}
		return Decision{State: s, Actions: acts, NextOp: "read", Reason: reason}

	case Promoting:
		// Rule 11 — the lease is not ours any more. There is nothing to
		// release; abort the hooks and go read who holds it now.
		if in.Lease == Held || in.Lease == Barred || in.Lease == Free {
			return standby([]Action{AbortHooks, Alert}, "read",
				fmt.Sprintf("the lease was lost mid-hooks: it reads %s", in.Lease))
		}
		// Rule 10 — the hard rule, identical in both on_loss modes: freeze
		// protects only what is already serving, it never authorizes a start.
		// No container starts once now ≥ expiry − stop_deadline, and a hook
		// still running there is aborted and the promotion abandoned — a
		// restore that outlived its lease must never turn into a connector
		// start on a lease the peer may already hold. An expiry we never armed
		// proves nothing, which earns the same answer.
		if s.LeaseExpiry == 0 || in.NowMono >= s.LeaseExpiry-p.StopDeadline {
			return abandon([]Action{AbortHooks, Release, Alert},
				"the promotion reached the stop deadline: nothing may start now")
		}
		// Rule 9 — the hooks failed, or verification did. Give the lease back
		// so the peer can try, notify loudly, and back off.
		if in.HookStatus == "failed" {
			return abandon([]Action{Release, Alert}, "the promote hooks failed")
		}
		// Rule 8 — restore, then the service, then the accessories, then the
		// connector: that order always. The restore first or the edge gets
		// whatever it happened to produce; the accessories before the
		// connector because replication must be back up before traffic — a
		// write taken before the sidecar runs is a write the replica does not
		// have. The accessory start is conditioned like rule 12's: only what a
		// reading we actually have shows down.
		if in.HookStatus == "done" {
			s.Phase = Active
			s.HooksDone = true
			s.BadStreak = 0
			acts := []Action{StartService}
			if in.AccessoriesUp != nil && *in.AccessoriesUp < in.AccessoriesTotal {
				acts = append(acts, StartAccessories)
			}
			return Decision{State: s,
				Actions: append(acts, StartConnector, Alert), NextOp: "renew",
				Reason: "the hooks finished inside the lease: starting the service, then the connector"}
		}
		// Rule 7 — hooks run for minutes on a stateful service, so keep
		// renewing under them: the lease must survive the operation it
		// authorized.
		return Decision{State: s, NextOp: "renew",
			Reason: "the promote hooks are still running: renewing under them"}

	case Active:
		// Rule 15 — a refusal is the arbiter saying it has already reassigned
		// the lease: stop now, no hysteresis, nothing to release. Free and
		// barred say the same thing in different words — whatever we are
		// serving, we are not serving it under a lease of ours.
		if in.Lease == Held || in.Lease == Free || in.Lease == Barred {
			return standby(stops(Alert), "read",
				fmt.Sprintf("the renewal was refused: the lease reads %s", in.Lease))
		}
		// Health is counted before anything is decided on it, so the streak
		// keeps running through arbiter silence too.
		if in.Healthy {
			s.BadStreak = 0
		} else {
			s.BadStreak++
		}
		// Rule 14 — sick for demote_after consecutive checks. The connector
		// stops before the release, so a FREE lease keeps meaning the previous
		// holder already stopped. Stopping is always safe, which is why this
		// outranks freeze: a locally-sick service demotes with or without the
		// arbiter, and the release goes out when contact resumes.
		if !in.Healthy && s.BadStreak >= p.DemoteAfter {
			return standby(stops(Release, Alert), "release",
				fmt.Sprintf("unhealthy for %d consecutive checks: giving up the lease", s.BadStreak))
		}
		if in.Lease == Unreachable {
			// Rule 16 — freeze, the default. Renewals failing says nothing
			// about the peer, and promotion requires a successful grant, so
			// while nobody can reach the arbiter nobody can promote and
			// holding our shape cannot create dual-serve. Signal once per
			// stretch of silence rather than once per check, and keep trying.
			if p.FreezeOnLoss {
				var acts []Action
				if !s.LossAlerted {
					acts = append(acts, Alert)
					s.LossAlerted = true
				}
				return Decision{State: s, Actions: acts, NextOp: "renew",
					Reason: "the arbiter is unreachable: holding shape and yelling"}
			}
			// Rule 17 — the strict mode. Stop at expiry − stop_deadline on the
			// local monotonic clock, no denial ever required: never serve past
			// your lease. An expiry we never armed cannot be proven either.
			if s.LeaseExpiry == 0 || in.NowMono >= s.LeaseExpiry-p.StopDeadline {
				return standby(stops(Alert), "read",
					"the lease cannot be proven past its stop deadline")
			}
			// Rule 18 — still inside the lease we were granted. Keep serving,
			// keep trying.
			return Decision{State: s, NextOp: "renew",
				Reason: "the arbiter is unreachable but the lease still holds"}
		}
		// Rule 13 — inside the hysteresis window. Still renewing: a blip that
		// recovers must cost nothing, and we may well still want this lease.
		if !in.Healthy {
			return Decision{State: s, NextOp: "renew",
				Reason: fmt.Sprintf("unhealthy %d/%d: inside the hysteresis window",
					s.BadStreak, p.DemoteAfter)}
		}
		// Rule 12 — healthy and holding the lease. Starting a connector we
		// find stopped is safe precisely because the lease is ours, and it
		// heals a crash between the stop and the release, which would
		// otherwise be a silent zero-traffic outage with every indicator
		// green (spec §4). The same reconcile covers the accessories, and it
		// is what makes an ADOPTED lease safe to resume: hooks never run on
		// adoption, so this check is the only thing standing between an
		// adopted-ACTIVE host and serving with replication silently down.
		// Accessories before the connector, as in rule 8 — replication before
		// traffic — and only ever here, under a lease that read Yours this
		// check: the unreachable rules above return first, so freeze's "never
		// authorizes a start" holds for accessories exactly as it does for the
		// connector.
		var acts []Action
		if in.AccessoriesUp != nil && *in.AccessoriesUp < in.AccessoriesTotal {
			acts = append(acts, StartAccessories)
		}
		if in.ConnectorUp != nil && !*in.ConnectorUp {
			acts = append(acts, StartConnector)
		}
		return Decision{State: s, Actions: acts, NextOp: "renew",
			Reason: "healthy and holding the lease"}
	}

	// An unrecognised phase is a bug in the supervisor, and the safe answer to
	// a bug in a failover system is to stop serving.
	return standby(stops(Alert), "read",
		fmt.Sprintf("unrecognised phase %q: failing closed", s.Phase))
}
