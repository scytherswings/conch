package supervisor

import (
	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/decide"
)

// Stop reasons: why conch last stopped each container it manages.
//
// A stopped container carries no evidence of who stopped it or why. Conch
// fences containers deliberately and constantly — a standby stops any
// connector it finds running, a demotion stops the shipper, a promotion stops
// a stale service before the restore writes under it — and from outside every
// one of those looks exactly like a crash. That ambiguity is not theoretical:
// it is how a conch-unaware deploy agent read a deliberately fenced standby
// and concluded that production had no database.
//
// So each stop this process performs records the rule that asked for it,
// against the container it acted on, and status hands that back. Four
// properties keep it honest:
//
//   - INTENT, NOT OUTCOME. A record means "conch asked for this stop and the
//     stop was confirmed". A dry run records nothing (it stops nothing), and
//     a container that survived both stop and kill records nothing either —
//     that failure has its own alert, and claiming a stop conch could not
//     perform would be the same false story told the other way round.
//
//   - AND CONCH HAS TO HAVE ACTUALLY STOPPED IT. decide's stopping rules name
//     StopConnector and StopService UNCONDITIONALLY — rule 14 demotes whether
//     or not the connector was up, rule 6 stops the service container whether
//     or not one was running — so the stop escalation is routinely run against
//     a container that was already down. Crediting conch with those would be
//     this feature's own anti-goal: a connector that CRASHED would come back
//     labelled `demotion`, and every promotion over an already-stopped service
//     would claim it found a stale one. So the record is gated on this check's
//     own reading (decide.Inputs' ConnectorUp / ServiceUp): no row unless the
//     target container was observed RUNNING on the check that ordered the
//     stop. A reading docker could not finish (nil) is not evidence it was
//     running, and gets no row either.
//
//     THE LOG SAYS THE SAME THING. The Info that narrates a stop answers to
//     this same reading (executor.stopContainer): a stop run against a
//     container that was already down narrates as exactly that, rather than
//     as "stopped container", so a live log and a later status read of the
//     same host never tell two stories about one stop.
//
//     THE ACCESSORIES ARE THE ONE EXCEPTION, and the asymmetry is honest
//     rather than hidden: their reading is a COUNT (decide.Inputs.AccessoriesUp),
//     not a per-container state, so "at least one was running" is all this
//     check knows about a list of them. An accessory row therefore means
//     "conch ordered this stop and the container is now down" — not "conch is
//     what transitioned it". A connector or service row means the stronger
//     thing. Discriminating them would mean widening readings' return to a
//     per-accessory set, which is a bigger change than the claim is worth.
//
//   - IN MEMORY ONLY. This is a record of what THIS process did. A restart
//     clears it, deliberately: containers a conch fenced before it died are
//     not facts this conch can vouch for.
//
//   - THE LAST STOP ONLY, per container. Not a history, not a count, and not
//     a claim about the container's current state — a service stopped for a
//     restore is started again seconds later and keeps its promote_stale row,
//     because the row answers "why did conch stop this", which a later start
//     does not un-answer. An operator pairs it with the container's actual
//     state; that is what the rest of status is for.

// stopReason is one of decide's rules, named for the human reading it. The
// set is closed on purpose: every value here is a rule that actually names a
// stop, derived from internal/decide rather than from what would be nice to
// distinguish. Two rules that commit cannot tell apart share a name rather
// than getting an invented signal.
type stopReason string

const (
	// reasonStandbyFence is rule 3: a host that holds no lease stops the
	// connector (and any accessory) it finds running, however it got started.
	// Also the fallback for every other path that stops while standing by,
	// including decideOne's orphan-evidence release — the container is down
	// because this host is not the serving host, which is what the name says,
	// and the unit's own `reason` beside it carries the longer sentence.
	reasonStandbyFence stopReason = "standby_fence"
	// reasonDemotion is rule 14: unhealthy for demote_after consecutive
	// checks, so the connector stops and the lease goes back.
	reasonDemotion stopReason = "demotion"
	// reasonRefused is rule 15: the arbiter has already reassigned the lease.
	// No hysteresis, nothing to release — the abruptest stop conch performs,
	// and the one most easily misread as a crash.
	reasonRefused stopReason = "refused"
	// reasonStopDeadline is rule 17: on_loss=stop, the arbiter is unreachable
	// and the lease cannot be proven past its stop deadline. Never serve past
	// your lease.
	reasonStopDeadline stopReason = "stop_deadline"
	// reasonSuspend is rule 1: wall/mono divergence beyond slop, so unknown
	// time has passed and every held lease is unproven.
	reasonSuspend stopReason = "suspend"
	// reasonDuplicateHost is rule 2: two processes answering to one host name,
	// both failing closed.
	reasonDuplicateHost stopReason = "duplicate_host"
	// reasonEvacuation is the operator-initiated hand-off (evacuate.go), whose
	// connector stop rides a check's actions like any other.
	reasonEvacuation stopReason = "evacuation"
	// reasonPromoteStale is rule 6: the claim was granted and the service
	// container was still running from a deploy or a reboot, so it is stopped
	// before the restore — a restore must never write under a live process.
	reasonPromoteStale stopReason = "promote_stale"
)

// stopIntent is why THIS batch's stops are happening. It is computed in
// commit, where the rule context lives, and carried through the executor's
// queue exactly like the resolved container name — apply() sees only
// decide.Action values, which cannot say whether a StopConnector is a fence,
// a demotion or a refusal.
//
// Two fields rather than one because one batch can genuinely carry two
// intents: rule 6 names StopService alongside rule 3's StopConnector, so a
// single reason would mislabel one of them.
type stopIntent struct {
	// fence is why a StopConnector or StopAccessories in this batch stops.
	// Every rule that stops the connector also stops any running accessory
	// (decide's `stops` helper), so the two never disagree.
	fence stopReason
	// service is why a StopService in this batch stops. Constant today:
	// decide names STOP_SERVICE from rule 6 and nowhere else, so the action
	// alone is the reason. It is set here anyway, rather than hardcoded in
	// the executor, so that all reason policy stays in one place — a second
	// STOP_SERVICE site in decide is a change this field would have to
	// answer for.
	service stopReason
	// orderedAt is the wall clock of the CHECK THAT ORDERED these stops, not
	// of their completion: the stop escalation can take a connector's whole
	// stop_timeout to run, so the recorded time is up to that much earlier
	// than the container's actual exit. It is the check's own single wall
	// reading (spec §12), which is what keeps this off the clock entirely on
	// the executor goroutines.
	orderedAt float64
	// connectorUp and serviceUp are this check's readings for the two
	// containers whose stops are named unconditionally — decide.Inputs'
	// ConnectorUp and ServiceUp, flattened to "observed RUNNING", so a nil
	// (docker could not say) reads as not-observed rather than as up. They
	// gate what conch CLAIMS, never the stop itself: the stop is decide's
	// decision and running it against an already-stopped container is a
	// harmless no-op that also heals a stale reading, while a row claiming
	// conch stopped a container it merely found stopped is a lie the whole
	// feature exists to prevent.
	//
	// They are handed to executor.stopContainer beside the reason rather than
	// folded into it, because BOTH surfaces answer to them: a stop against a
	// container that was already down writes no row and says so in the log
	// too. Folding the reading into the reason (an empty reason for "was
	// already down") would collapse it with the label sweep's empty reason,
	// which means something else entirely — a confirmed stop of a running
	// container that belongs to no service in this config.
	connectorUp bool
	serviceUp   bool
}

// stopIntentFor reads the reason off the decision commit is about to apply.
// Every branch below is grounded in a signal decide actually produced — the
// phase transition, the lease reading, the presence of a Release action —
// rather than in Decision.Reason's prose, which is written for humans and
// gets edited.
func stopIntentFor(
	was decide.Phase, d decide.Decision, v arbiter.Verdict, in decide.Inputs,
	extra []decide.Action, wall float64,
) stopIntent {
	si := stopIntent{
		fence:       reasonStandbyFence,
		service:     reasonPromoteStale,
		orderedAt:   wall,
		connectorUp: in.ConnectorUp != nil && *in.ConnectorUp,
		serviceUp:   in.ServiceUp != nil && *in.ServiceUp,
	}
	switch {
	case containsAction(extra, decide.StopConnector):
		// The evacuation sequence's own stop, and it wins outright. extra is
		// non-empty only while this host is deliberately handing the service
		// over, and that intent is the one an operator is looking for even on
		// the rare check where a decide rule names a stop beside it.
		si.fence = reasonEvacuation
	case in.Suspended:
		si.fence = reasonSuspend
	case v.Lease == decide.Conflict:
		si.fence = reasonDuplicateHost
	case was == decide.Active && d.State.Phase == decide.Standby:
		// The three ways an ACTIVE host stops serving. Rule 14 is the only one
		// that hands the lease back, which is what tells it apart from rule 17
		// — both can fire while unhealthy AND unreachable, and Decision.Reason
		// is the only other thing that differs.
		switch {
		case containsAction(d.Actions, decide.Release):
			si.fence = reasonDemotion
		case v.Lease == decide.Held || v.Lease == decide.Free || v.Lease == decide.Barred:
			si.fence = reasonRefused
		case v.Lease == decide.Unreachable:
			si.fence = reasonStopDeadline
		}
		// Anything else here — an ACTIVE host going to STANDBY on a `yours`
		// reading, which is decideOne's release re-send — falls through to the
		// fence, which is what it is.
	}
	return si
}

// stopRecord is one container's last conch-ordered stop.
type stopRecord struct {
	container string
	reason    stopReason
	wall      float64
}

// maxStopRecords bounds one service's rows. A service names a fixed handful
// of containers — except the service container itself, which for a Kamal-style
// trailing-* prefix is versioned on every deploy, so the set of names conch
// has stopped grows slowly and forever. Newest wins and the oldest row is
// dropped: an operator reading this at 3am wants the last few stops, and a
// process that has been up for a year must not be carrying a row per deploy.
const maxStopRecords = 8

// noteStopped records a CONFIRMED stop. Called from executor goroutines, which
// is why it takes s.mu and why the wall clock arrives as an argument rather
// than off s.clk (see stopIntent.orderedAt).
//
// An unknown key is silently ignored, which is what makes the label sweep's
// exclusion structural: the sweeper's unit is the synthetic sweepScope, not a
// service, so there is nothing here for it to record against — and the sweep
// passes no reason at all (executor.applyStops), so it never gets this far.
func (s *Supervisor) noteStopped(key, container string, why stopReason, wall float64) {
	if why == "" || container == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.units[key]
	if !ok {
		return
	}
	rows := make([]stopRecord, 0, len(u.lastStops)+1)
	rows = append(rows, stopRecord{container: container, reason: why, wall: wall})
	for _, r := range u.lastStops {
		if r.container == container {
			continue // the LAST stop only: this container's older row is gone
		}
		if len(rows) == maxStopRecords {
			break
		}
		rows = append(rows, r)
	}
	u.lastStops = rows
}
