// This file is arbiter/src/lease.js's renewOne/releaseOne/view, moved to the
// client. The DO could answer in one place because it was the only writer
// and it had a clock; here every host runs the same judgement against the
// item it last polled, and the conditional write is what makes two hosts
// reaching different conclusions harmless — at most one of them lands.
//
// So classify decides, and never acts: it takes an observed entry plus the
// observer's silence/bar/conflict readings and returns the action a caller
// must attempt, together with the verdict that action EARNS IF ITS WRITE
// SUCCEEDS. It is pure — no I/O, no clock (the caller passes now), no
// mutation of the observer — because the whole protocol is then testable as
// a table, and because the caller must be free to run it for every service
// before deciding which writes to batch.
//
// Two translations of lease.js are worth naming up front, since they explain
// most of what follows:
//
//   - Expiry. lease.js runs expireIfDue first, so every later check sees a
//     lapsed holder as no holder and a lapsed bar as no bar. Here that is
//     silence past ttl and bar age past bar_ttl, measured by the observer —
//     with the one exception below for our OWN hold.
//   - Grant. lease.js writes the grant itself; classify only says which
//     write to attempt. The epoch a claim's verdict carries is therefore the
//     epoch that write will set, not one we have seen in the table.
package dynarbiter

import (
	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/decide"
)

// actionKind is which write, if any, a classification calls for.
type actionKind int

const (
	actNone     actionKind = iota // verdict is final, no write needed
	actRenew                      // join the batched renew UpdateItem
	actClaim                      // own buildClaim write
	actTakeover                   // own buildTakeover write
	actRelease                    // own buildRelease write
)

// action is one service's classification: the write to attempt, whether that
// write should clear a bar on the way through, and the verdict the caller
// reports once the write lands (immediately, for actNone).
type action struct {
	Kind     actionKind
	ClearBar bool
	Verdict  arbiter.Verdict
}

// classify maps one service's op and observed state to an action. There is
// no in-process "do we hold it" flag anywhere in here: every path trusts the
// host NAME on the record exactly as the DO trusts req.host, and the
// builders' CAS conditions are what make a stale opinion harmless — a write
// on a lease that moved is refused, and the next poll's re-read produces the
// verdict from fresh state. exists says whether the fleet item carried this
// key at all, a distinction only the read path makes (lease.js refuses to
// create a record for a key nobody has ever claimed).
func classify(now, ttl float64, key string, op arbiter.Op, e entry, exists bool,
	obs *observer, me hostSpec) action {

	switch op.Op {
	case "read":
		return classifyRead(now, ttl, key, e, exists, obs, me)
	case "release":
		return classifyRelease(op, e, me)
	case "renew":
		return classifyRenew(now, ttl, key, op, e, obs, me)
	default:
		// lease.js's VALID_OPS check, and the same default-deny decide.go
		// applies to a lease reading: the zero value, a typo or a protocol
		// change must not fall through into a branch that WRITES. Unreachable
		// with the error preserved is what toVerdict does with invalid_op.
		return final(arbiter.Verdict{Lease: decide.Unreachable, Err: "invalid_op"})
	}
}

// classifyRead mirrors view() after expireIfDue: it never writes and never
// claims, so its only job is to say what the item says about us.
func classifyRead(now, ttl float64, key string, e entry, exists bool,
	obs *observer, me hostSpec) action {

	if !exists {
		// Non-creating lookup: a key nobody has ever claimed is free at
		// epoch 0, and reading it leaves the table untouched.
		return final(arbiter.Verdict{Lease: decide.Free})
	}
	if e.Holder == me.Host {
		// By host NAME, and by nothing else — lease.js's view() trusts
		// `l.holder === forHost` with no instance check, and that is
		// exactly what lets a restarted conch adopt its own still-live lease:
		// the reconcile read must say Yours, because decide never promotes a
		// read to a renew off a Held reading, so a read that answered Held
		// here would leave the takeover renew unsent and the host would wait
		// out its own TTL and lose the lease to a peer. Two processes sharing
		// the name still converge on Conflict, on the renew path where the
		// flip-flop is detectable, exactly as the DO does it.
		//
		// TTL 0 on purpose: a read grants nothing, so decide keeps whatever
		// deadline its last renewal proved. Only a renew carries a grant.
		return final(arbiter.Verdict{Lease: decide.Yours, Epoch: int(e.Epoch)})
	}
	if heldByOther(now, ttl, key, e, obs, me) {
		return final(heldVerdict(e))
	}
	if barsUs(now, key, e, obs, me) {
		return final(arbiter.Verdict{
			Lease: decide.Barred, EvacuatedBy: e.EvacuatedBy, Epoch: int(e.Epoch)})
	}
	return final(arbiter.Verdict{
		Lease: decide.Free, Epoch: int(e.Epoch), EvacuatedBy: e.EvacuatedBy})
}

// classifyRenew mirrors renewOne. The order is lease.js's, with one
// deliberate deviation and one deliberate addition, both marked below.
func classifyRenew(now, ttl float64, key string, op arbiter.Op, e entry,
	obs *observer, me hostSpec) action {

	// Deviation from lease.js's order: there the duplicate-host check sits
	// after the bar and other-holder returns, so a conflicted host under a
	// live peer hold is told "held". Client-side the conflict outranks both.
	// Nothing about safety turns on it — all three answers write nothing —
	// but Conflict is the only one that reaches decide's fail-closed rule
	// for two processes sharing a host name, and a host that has proven it
	// is duplicated should hear that whatever else is true of the lease.
	if obs.Conflicted(now, key, me.Host) {
		return final(arbiter.Verdict{Lease: decide.Conflict, Err: "duplicate_host"})
	}

	// Addition: our own name on the record is checked BEFORE silence can
	// retire it. lease.js re-grants a self-expired hold at epoch+1; we renew
	// at the same epoch instead, which keeps a host whose writes are merely
	// slow inside the batched UpdateItem rather than sending it round the
	// claim path. The version CAS still decides whether the write lands, and
	// fencing only needs the epoch to advance when the HOLDER changes.
	if e.Holder == me.Host {
		kind := actRenew
		if e.Instance != me.Instance {
			// Same host name, new process: restart adoption. Epoch is left
			// alone — lease.js bumps only when granting a free lease.
			kind = actTakeover
		}
		return action{Kind: kind, Verdict: arbiter.Verdict{
			Lease: decide.Yours, TTL: ttl, Epoch: int(e.Epoch)}}
	}

	if heldByOther(now, ttl, key, e, obs, me) {
		return final(heldVerdict(e))
	}

	// Nobody holds it (or the holder has gone silent, which lease.js's lazy
	// expiry treats the same way). A bar we left behind refuses us here even
	// when the peer that stepped over it is the one that just went quiet:
	// coming back to a lease we evacuated takes an explicit override.
	if barsUs(now, key, e, obs, me) && !op.Override {
		return final(arbiter.Verdict{
			Lease: decide.Barred, EvacuatedBy: e.EvacuatedBy, Epoch: int(e.Epoch)})
	}

	return action{
		Kind:     actClaim,
		ClearBar: clearsBar(now, key, e, obs, me, op.Override),
		Verdict: arbiter.Verdict{
			Lease: decide.Yours, TTL: ttl, Epoch: int(e.Epoch) + 1},
	}
}

// classifyRelease mirrors releaseOne, whose refusal is the client's too: a
// release is a write conditioned on holding the lease, and asking to give up
// what we cannot prove we have is our own bug, not a peer's claim — hence
// Unreachable with the error preserved, exactly as toVerdict maps not_holder.
//
// The name on the record is the whole test, as it is in releaseOne. A
// release must not wait on a write of ours having landed first: the
// supervisor's startup reconcile releases from STANDBY on orphan evidence (an
// orphaned promote hook proves the previous conch died mid-restore, so the
// local data is torn and the peer must restore from the clean replica), and
// nothing of ours has been written at that point. Refusing there would pin
// the lease under our own name for a full TTL on the one path whose purpose
// is handing torn data over fast. buildRelease's (h, v) CAS is what keeps
// this safe if the lease moved under us.
func classifyRelease(op arbiter.Op, e entry, me hostSpec) action {
	if e.Holder != me.Host {
		return final(arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"})
	}
	// Always free-shaped and epoch-unbumped: this is our own confirmation
	// that the release happened, not a status check on ourselves. The bar we
	// are leaving behind would make our NEXT read say barred; it does not
	// make this answer barred. Without an evacuation there is no bar to
	// report at all, because buildRelease removes any that was there.
	v := arbiter.Verdict{Lease: decide.Free, Epoch: int(e.Epoch), Released: true}
	if op.Evacuate {
		v.EvacuatedBy = me.Host
	}
	return action{Kind: actRelease, Verdict: v}
}

// final wraps a verdict that needs no write.
func final(v arbiter.Verdict) action { return action{Kind: actNone, Verdict: v} }

// heldByOther reports whether the record shows a live hold under another
// host's name. Both callers have already answered for our own name, so this
// only has to decide whether a peer's hold is still live — silence past ttl
// is lease.js's expireIfDue, which retires the holder before any bar or
// grant check downstream ever sees it.
func heldByOther(now, ttl float64, key string, e entry, obs *observer, me hostSpec) bool {
	if e.Holder == "" || e.Holder == me.Host {
		return false
	}
	return obs.Silence(now, key) < ttl
}

func heldVerdict(e entry) arbiter.Verdict {
	return arbiter.Verdict{
		Lease: decide.Held, Holder: e.Holder, Serving: e.Serving, Epoch: int(e.Epoch)}
}

// barsUs reports whether a live evacuation bar names us. Bar liveness is its
// own clock, not the lease's: BarTTL 0 is an ABSENT bar_ttl (`conch evacuate
// --hold` sends none), which in lease.js leaves barExpiresAt null — a bar
// with no clock on it, standing until a reclaim clears it. Comparing age
// against 0 instead would make every hold bar dead the moment it was written.
func barsUs(now float64, key string, e entry, obs *observer, me hostSpec) bool {
	return e.EvacuatedBy == me.Host && barLive(now, key, e, obs)
}

func barLive(now float64, key string, e entry, obs *observer) bool {
	if e.EvacuatedBy == "" {
		return false
	}
	if e.BarTTL <= 0 {
		return true
	}
	return obs.BarAge(now, key) < e.BarTTL
}

// clearsBar decides whether a claim should take the bar attributes with it,
// mirroring renewOne's grant path: the barred host's own override clears the
// bar it left, and any peer's claim clears a bar that was not a hold. A hold
// bar under a peer's claim stays recorded — the lease goes to the peer, the
// bar outlives it until the barred host overrides or bar_ttl lapses. A bar
// that HAS lapsed is cleared by whoever claims next, since lease.js's
// expireIfDue would already have dropped it and only a write can drop it
// here.
func clearsBar(now float64, key string, e entry, obs *observer, me hostSpec, override bool) bool {
	if e.EvacuatedBy == "" {
		return false
	}
	if !barLive(now, key, e, obs) {
		return true
	}
	if e.EvacuatedBy == me.Host {
		return override
	}
	return !e.BarHold
}
