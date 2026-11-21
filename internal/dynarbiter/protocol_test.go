package dynarbiter

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/decide"
)

// me is the calling host in every case below; peers are named outright so a
// fixture's holder never has to be read twice to see whose it is.
var meHost = hostSpec{Host: "me", Instance: "inst-2", Fingerprint: "fp"}

// poll is one Observe call: the fixture's key as the table held it at time
// at. Observer state is built ONLY through these — silence, bar age and the
// conflict window are all derived from a sequence of sightings, so a
// hand-poked svcState would test nothing about the coupling classify relies
// on.
type poll struct {
	at float64
	e  entry
}

type classifyCase struct {
	name   string
	now    float64
	ttl    float64
	op     arbiter.Op
	e      entry
	exists bool
	polls  []poll
	want   action
}

func (c classifyCase) observer(key string) *observer {
	o := newObserver()
	for _, p := range c.polls {
		o.Observe(p.at, c.ttl, map[string]entry{key: p.e}, nil)
	}
	return o
}

const caseKey = "web"

// A peer holding "web" at epoch 4, seen once so silence is measurable.
func peerHeld() entry {
	return entry{Holder: "peer", Instance: "peer-1", Serving: true, Epoch: 4, Version: 9}
}

// oursHeld is the same key with our own name on it.
func oursHeld() entry {
	return entry{Holder: "me", Instance: "inst-2", Serving: true, Epoch: 4, Version: 9}
}

// classifyCases is shared by the table test and by
// TestClaimVerdictEpochMatchesWrite, which re-runs every claim-producing case
// against the write buildClaim would emit for it.
func classifyCases() []classifyCase {
	read := arbiter.Op{Op: "read"}
	renew := arbiter.Op{Op: "renew", Serving: true}
	reclaim := arbiter.Op{Op: "renew", Serving: true, Override: true}

	// A bar left by a release: peerBar is a peer's, ourBar is our own. Both
	// carry a 300s clock unless the case says otherwise.
	ourBar := func(e entry) entry {
		e.EvacuatedBy, e.BarTTL, e.BarVersion = "me", 300, 9
		return e
	}
	peerBar := func(e entry) entry {
		e.EvacuatedBy, e.BarTTL, e.BarVersion = "peer", 300, 9
		return e
	}
	freed := entry{Epoch: 4, Version: 9} // released: h/i gone, epoch kept

	return []classifyCase{
		// ---- read (lease.js view, after expireIfDue) ----
		{
			name: "row01_read_no_entry", now: 10, ttl: 60, op: read,
			want: action{Kind: actNone, Verdict: arbiter.Verdict{Lease: decide.Free}},
		},
		{
			name: "row02_read_yours", now: 10, ttl: 60, op: read,
			e: oursHeld(), exists: true,
			polls: []poll{{at: 5, e: oursHeld()}},
			// The steady state: our name, our instance, freshly seen. Row 2
			// turns on the name alone, which the restart pair below pins.
			// TTL 0 because a read grants nothing: decide's prior deadline
			// stands, exactly as it does under the HTTP client's view
			// response.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{Lease: decide.Yours, Epoch: 4}},
		},
		{
			name: "row03_read_held", now: 10, ttl: 60, op: read,
			e: peerHeld(), exists: true,
			polls: []poll{{at: 5, e: peerHeld()}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Held, Holder: "peer", Serving: true, Epoch: 4}},
		},
		{
			name: "row04_read_free_after_silence", now: 100, ttl: 60, op: read,
			e: peerHeld(), exists: true,
			polls: []poll{{at: 0, e: peerHeld()}},
			want:  action{Kind: actNone, Verdict: arbiter.Verdict{Lease: decide.Free, Epoch: 4}},
		},
		{
			name: "row05_read_barred", now: 10, ttl: 60, op: read,
			e: ourBar(freed), exists: true,
			polls: []poll{{at: 0, e: ourBar(freed)}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Barred, EvacuatedBy: "me", Epoch: 4}},
		},
		{
			name: "row06_read_free_after_bar_expiry", now: 400, ttl: 60, op: read,
			e: ourBar(freed), exists: true,
			polls: []poll{{at: 0, e: ourBar(freed)}},
			// The bar aged out but nothing has written yet, so the attribute
			// is still on the item and the verdict still reports it.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Free, Epoch: 4, EvacuatedBy: "me"}},
		},
		{
			name: "row07_read_free_under_peer_bar", now: 10, ttl: 60, op: read,
			e: peerBar(freed), exists: true,
			polls: []poll{{at: 0, e: peerBar(freed)}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Free, Epoch: 4, EvacuatedBy: "peer"}},
		},

		// ---- renew (lease.js renewOne) ----
		{
			name: "row08_renew_duplicate_host", now: 10, ttl: 60, op: renew,
			e: entry{Holder: "me", Instance: "inst-1", Epoch: 4, Version: 11}, exists: true,
			// A -> B -> A within one ttl on our own host name.
			polls: []poll{
				{at: 0, e: entry{Holder: "me", Instance: "inst-1", Epoch: 4, Version: 9}},
				{at: 3, e: entry{Holder: "me", Instance: "inst-2", Epoch: 4, Version: 10}},
				{at: 6, e: entry{Holder: "me", Instance: "inst-1", Epoch: 4, Version: 11}},
			},
			// No epoch: toVerdict drops it on the error branch.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Conflict, Err: "duplicate_host"}},
		},
		{
			name: "row09_renew_claims_new_key", now: 10, ttl: 60, op: renew,
			want: action{Kind: actClaim, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 1}},
		},
		{
			name: "row10_renew_ours", now: 10, ttl: 60, op: renew,
			e: oursHeld(), exists: true,
			polls: []poll{{at: 5, e: oursHeld()}},
			want: action{Kind: actRenew, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 4}},
		},
		{
			name: "row11_renew_takeover_after_restart", now: 10, ttl: 60, op: renew,
			e: entry{Holder: "me", Instance: "inst-1", Epoch: 4, Version: 9}, exists: true,
			polls: []poll{{at: 5, e: entry{Holder: "me", Instance: "inst-1", Epoch: 4, Version: 9}}},
			// Epoch is NOT bumped: lease.js only bumps on granting a free
			// lease, never on the same host name re-registering.
			want: action{Kind: actTakeover, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 4}},
		},
		{
			name: "row12_renew_held_by_peer", now: 10, ttl: 60, op: renew,
			e: peerHeld(), exists: true,
			polls: []poll{{at: 5, e: peerHeld()}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Held, Holder: "peer", Serving: true, Epoch: 4}},
		},
		{
			name: "row13_renew_claims_silent_peer", now: 100, ttl: 60, op: renew,
			e: peerBar(peerHeld()), exists: true,
			polls: []poll{{at: 0, e: peerBar(peerHeld())}},
			// A peer's non-hold bar goes with the peer's dead lease.
			want: action{Kind: actClaim, ClearBar: true, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 5}},
		},
		{
			name: "row14_renew_barred", now: 10, ttl: 60, op: renew,
			e: ourBar(freed), exists: true,
			polls: []poll{{at: 0, e: ourBar(freed)}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Barred, EvacuatedBy: "me", Epoch: 4}},
		},
		{
			name: "row15_renew_override_reclaims", now: 10, ttl: 60, op: reclaim,
			e: ourBar(freed), exists: true,
			polls: []poll{{at: 0, e: ourBar(freed)}},
			want: action{Kind: actClaim, ClearBar: true, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 5}},
		},
		{
			name: "row16_renew_claims_after_bar_expiry", now: 400, ttl: 60, op: renew,
			e: ourBar(freed), exists: true,
			polls: []poll{{at: 0, e: ourBar(freed)}},
			want: action{Kind: actClaim, ClearBar: true, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 5}},
		},
		{
			name: "row17_renew_claims_over_peer_bar", now: 10, ttl: 60, op: renew,
			e: peerBar(freed), exists: true,
			polls: []poll{{at: 0, e: peerBar(freed)}},
			want: action{Kind: actClaim, ClearBar: true, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 5}},
		},
		{
			name: "row17_renew_leaves_peer_hold_bar_recorded", now: 10, ttl: 60, op: renew,
			e: func() entry { e := peerBar(freed); e.BarHold = true; return e }(), exists: true,
			polls: []poll{{at: 0, e: func() entry { e := peerBar(freed); e.BarHold = true; return e }()}},
			// The lease is granted to us, but a hold bar outlives it: only
			// the barred host's own override, or bar_ttl, clears it.
			want: action{Kind: actClaim, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 5}},
		},

		// ---- release (lease.js releaseOne) ----
		{
			name: "row18_release_evacuating", now: 10, ttl: 60,
			op: arbiter.Op{Op: "release", Evacuate: true, BarTTL: 1800},
			e:  oursHeld(), exists: true,
			polls: []poll{{at: 5, e: oursHeld()}},
			// Free-shaped and epoch-unbumped: this is our own confirmation
			// that the release happened, not a status check on ourselves.
			want: action{Kind: actRelease, Verdict: arbiter.Verdict{
				Lease: decide.Free, Epoch: 4, Released: true, EvacuatedBy: "me"}},
		},
		{
			name: "row18_release_plain", now: 10, ttl: 60,
			op: arbiter.Op{Op: "release"},
			e:  oursHeld(), exists: true,
			polls: []poll{{at: 5, e: oursHeld()}},
			// No evacuation: buildRelease REMOVEs the bar attributes, so no
			// EvacuatedBy can survive this write to be reported.
			want: action{Kind: actRelease, Verdict: arbiter.Verdict{
				Lease: decide.Free, Epoch: 4, Released: true}},
		},
		{
			name: "row19_release_not_holder", now: 10, ttl: 60,
			op: arbiter.Op{Op: "release", Evacuate: true},
			e:  peerHeld(), exists: true,
			polls: []poll{{at: 5, e: peerHeld()}},
			// Unreachable, not Conflict: toVerdict maps every error but
			// duplicate_host to "a reading we cannot act on".
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Unreachable, Err: "not_holder"}},
		},

		{
			name: "row19_release_no_entry", now: 10, ttl: 60,
			op: arbiter.Op{Op: "release"},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Unreachable, Err: "not_holder"}},
		},
		{
			name: "restart_release_of_orphaned_lease_needs_no_landed_write",
			now:  10, ttl: 60,
			op:     arbiter.Op{Op: "release", Evacuate: true, Hold: true},
			e:      entry{Holder: "me", Instance: "inst-1", Serving: true, Epoch: 4, Version: 9},
			exists: true,
			// Fresh observer, nothing polled, nothing of ours written: the
			// startup reconcile's release. An orphaned promote hook proves
			// the previous conch died mid-restore, so the supervisor releases
			// out of STANDBY (supervisor.go decideOne) to get the peer
			// restoring from the clean replica. Waiting for a write of ours
			// to land first would pin the lease under our own name for a full
			// TTL on the one path whose purpose is speed; the record's name
			// is the whole test, and buildRelease's (h, v) CAS covers a lease
			// that moved.
			want: action{Kind: actRelease, Verdict: arbiter.Verdict{
				Lease: decide.Free, Epoch: 4, Released: true, EvacuatedBy: "me"}},
		},

		{
			name: "unrecognised_op_writes_nothing", now: 10, ttl: 60,
			op: arbiter.Op{}, // the zero value: no op at all
			e:  peerBar(freed), exists: true,
			polls: []poll{{at: 0, e: peerBar(freed)}},
			// lease.js's VALID_OPS refusal. The point is the Kind: an op we
			// do not recognise must not fall through into a branch that
			// claims a lease nobody asked for.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Unreachable, Err: "invalid_op"}},
		},

		// ---- decisions the table does not spell out ----
		// The restart story, in the two classifications it passes through. A
		// conch that has just restarted on the active host has written
		// nothing yet: all it has is its own name on a record it did not
		// write. If the reconcile read answered anything but Yours, decide
		// would never promote it to a renew (rule 4 promotes off Free, rule 5
		// stops on a read that reads Held), the takeover below would never be
		// sent, and the host would sit out its own TTL and lose its lease to
		// a peer. So the read trusts the host name, exactly as lease.js's
		// view() does.
		{
			name: "restart_read_adopts_own_record_before_any_write_lands",
			now:  10, ttl: 60, op: read,
			e:      entry{Holder: "me", Instance: "inst-1", Serving: true, Epoch: 4, Version: 9},
			exists: true,
			polls:  []poll{{at: 5, e: entry{Holder: "me", Instance: "inst-1", Serving: true, Epoch: 4, Version: 9}}},
			want:   action{Kind: actNone, Verdict: arbiter.Verdict{Lease: decide.Yours, Epoch: 4}},
		},
		{
			name: "restart_renew_after_that_read_takes_over",
			now:  10, ttl: 60, op: renew,
			e:      entry{Holder: "me", Instance: "inst-1", Serving: true, Epoch: 4, Version: 9},
			exists: true,
			polls:  []poll{{at: 5, e: entry{Holder: "me", Instance: "inst-1", Serving: true, Epoch: 4, Version: 9}}},
			// Zero-blip adoption: same name, new instance, same epoch.
			want: action{Kind: actTakeover, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 4}},
		},

		// Read-side ordering of the live-holder check against the bar check,
		// the pair that has to move together: a bar of ours on a record a
		// peer still holds must not read as Barred, and the same bar once
		// that peer goes quiet must not read as Free.
		{
			name: "read_live_peer_hold_outranks_our_own_bar",
			now:  10, ttl: 60, op: read,
			e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}(), exists: true,
			polls: []poll{{at: 5, e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}()}},
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Held, Holder: "peer", Serving: true, Epoch: 4}},
		},
		{
			name: "read_our_bar_outlives_the_peer_that_stepped_over_it",
			now:  100, ttl: 60, op: read,
			e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}(), exists: true,
			polls: []poll{{at: 0, e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}()}},
			// Silence retires the holder first (expireIfDue), and what view()
			// then sees is our own bar.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Barred, EvacuatedBy: "me", Epoch: 4}},
		},
		{
			name: "renew_hold_bar_without_ttl_never_ages_out", now: 100000, ttl: 60, op: renew,
			e: func() entry {
				e := freed
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}(), exists: true,
			polls: []poll{{at: 0, e: func() entry {
				e := freed
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}()}},
			// bar_ttl 0 is an ABSENT bar_ttl (evacuate --hold sends none),
			// which in lease.js leaves barExpiresAt null: a bar with no clock
			// on it, held until a reclaim. Comparing age against 0 would make
			// every hold bar dead on arrival.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Barred, EvacuatedBy: "me", Epoch: 4}},
		},
		{
			name: "renew_our_own_bar_outlives_the_peer_that_stepped_over_it", now: 100, ttl: 60, op: renew,
			e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}(), exists: true,
			polls: []poll{{at: 0, e: func() entry {
				e := peerHeld()
				e.EvacuatedBy, e.BarHold, e.BarVersion = "me", true, 9
				return e
			}()}},
			// lease.js expires the silent holder BEFORE the bar check, so a
			// hold bar we left behind still bars us once the peer that took
			// the lease goes quiet. Coming back needs an override.
			want: action{Kind: actNone, Verdict: arbiter.Verdict{
				Lease: decide.Barred, EvacuatedBy: "me", Epoch: 4}},
		},
		{
			name: "renew_keeps_the_batch_when_our_own_hold_has_gone_quiet", now: 100, ttl: 60, op: renew,
			e: oursHeld(), exists: true,
			polls: []poll{{at: 0, e: oursHeld()}},
			// Our own name on the record outranks silence: the version CAS
			// decides whether the renew lands, and staying in the batched
			// write keeps the epoch stable (lease.js would re-grant at
			// epoch+1 here; fencing only needs a bump when the holder
			// changes).
			want: action{Kind: actRenew, Verdict: arbiter.Verdict{
				Lease: decide.Yours, TTL: 60, Epoch: 4}},
		},
	}
}

func TestClassify(t *testing.T) {
	for _, c := range classifyCases() {
		t.Run(c.name, func(t *testing.T) {
			got := classify(c.now, c.ttl, caseKey, c.op, c.e, c.exists, c.observer(caseKey), meHost)
			if got != c.want {
				t.Fatalf("classify:\n got %+v\nwant %+v", got, c.want)
			}
		})
	}
}

// TestClaimVerdictEpochMatchesWrite pins the one invariant a claim cannot get
// wrong: Task 6 returns classify's verdict only when the write succeeds, so
// the epoch the verdict promises must be the epoch the write actually puts in
// the table. A mismatch is a fencing bug — decide would believe an epoch no
// peer will ever see.
func TestClaimVerdictEpochMatchesWrite(t *testing.T) {
	var sawWholeMap, sawCAS bool
	for _, c := range classifyCases() {
		got := classify(c.now, c.ttl, caseKey, c.op, c.e, c.exists, c.observer(caseKey), meHost)
		if got.Kind != actClaim {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			in := buildClaim("leases", caseKey, c.e, meHost, c.op.Serving, got.ClearBar)
			values := in.ExpressionAttributeValues
			if _, wholeMap := values[":entry"]; wholeMap {
				sawWholeMap = true
			} else {
				sawCAS = true
			}
			want := strconv.Itoa(got.Verdict.Epoch)
			if wrote := claimEpoch(t, values); wrote != want {
				t.Fatalf("verdict promises epoch %s, the write sets %s", want, wrote)
			}
		})
	}
	// Both buildClaim branches must be exercised, and by name rather than by
	// count: a never-seen key writes its epoch inside a whole-map :entry, and
	// every other claim writes :newEpoch under a version CAS. A suite that
	// only ever hit one of them would leave the other's epoch unpinned.
	if !sawWholeMap {
		t.Error("no claim case exercised buildClaim's never-seen (:entry) branch")
	}
	if !sawCAS {
		t.Error("no claim case exercised buildClaim's version-CAS (:newEpoch) branch")
	}
}

// claimEpoch digs the epoch out of whichever shape buildClaim emitted.
func claimEpoch(t *testing.T, values map[string]types.AttributeValue) string {
	t.Helper()
	if entryAV, ok := values[":entry"]; ok {
		m, ok := entryAV.(*types.AttributeValueMemberM)
		if !ok {
			t.Fatalf(":entry is not a map: %T", entryAV)
		}
		n, ok := m.Value[svcEpoch].(*types.AttributeValueMemberN)
		if !ok {
			t.Fatalf(":entry has no numeric epoch: %+v", m.Value)
		}
		return n.Value
	}
	n, ok := values[":newEpoch"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("claim has neither :entry nor :newEpoch: %+v", values)
	}
	return n.Value
}

// splitUpdate breaks a builder's UpdateExpression into its SET and REMOVE
// clauses (REMOVE may be empty), so a test can check which svc.#k.<attr>
// paths a write actually touches without parsing the whole expression by
// hand each time.
func splitUpdate(t *testing.T, in *dynamodb.UpdateItemInput) (set, remove string) {
	t.Helper()
	full := *in.UpdateExpression
	parts := strings.SplitN(full, " REMOVE ", 2)
	set = strings.TrimPrefix(parts[0], "SET ")
	if len(parts) == 2 {
		remove = parts[1]
	}
	return set, remove
}

// fieldTouched reports whether clause (a SET or REMOVE half from
// splitUpdate) assigns or removes svc.#k.<field> — comparing the path before
// any " = " rather than substring-matching, so "e" does not false-match
// "eb".
func fieldTouched(clause, field string) bool {
	want := "svc.#k." + field
	for _, part := range strings.Split(clause, ", ") {
		name := strings.SplitN(strings.TrimSpace(part), " = ", 2)[0]
		if name == want {
			return true
		}
	}
	return false
}

// TestReleaseVerdictAgreesWithWrite pins actRelease's verdict against what
// buildRelease actually writes: EvacuatedBy set iff the op evacuated (and the
// bv it pins the bar to, only present on that branch), Released always true,
// and the epoch UNBUMPED — a release never advances fencing, so buildRelease
// must never touch svc.#k.e.
func TestReleaseVerdictAgreesWithWrite(t *testing.T) {
	var sawEvacuate, sawPlain bool
	for _, c := range classifyCases() {
		got := classify(c.now, c.ttl, caseKey, c.op, c.e, c.exists, c.observer(caseKey), meHost)
		if got.Kind != actRelease {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			in := buildRelease("leases", caseKey, c.e, meHost, c.op.Evacuate, c.op.Hold, c.op.BarTTL)
			set, remove := splitUpdate(t, in)

			if fieldTouched(set, "e") || fieldTouched(remove, "e") {
				t.Fatalf("release must never touch the epoch: %s", *in.UpdateExpression)
			}
			if got.Verdict.Epoch != int(c.e.Epoch) {
				t.Fatalf("verdict epoch %d does not match observed epoch %d (release must not bump it)",
					got.Verdict.Epoch, c.e.Epoch)
			}
			// Released is tied to the write actually giving the lease up —
			// checking got.Verdict.Released alone would only restate
			// classify's own output (TestClassify already pins that); the
			// agreement claim needs the REMOVE side too, or a buildRelease
			// that stopped dropping h/i would still leave the lease held
			// while the verdict promises Free.
			if !got.Verdict.Released {
				t.Fatal("actRelease verdict must report Released")
			}
			if !fieldTouched(remove, "h") || !fieldTouched(remove, "i") {
				t.Fatalf("a Released verdict requires the write to REMOVE holder and instance: %s", remove)
			}

			if c.op.Evacuate {
				sawEvacuate = true
				// :host backs both the eb = :host SET and the svc.#k.h =
				// :host CAS, so digging it out and comparing to the verdict
				// confirms the bar this write leaves behind is stamped with
				// the SAME host the verdict claims evacuated it.
				hostAV, ok := in.ExpressionAttributeValues[":host"].(*types.AttributeValueMemberS)
				if !ok {
					t.Fatalf(":host is not a string value: %+v", in.ExpressionAttributeValues[":host"])
				}
				if got.Verdict.EvacuatedBy != meHost.Host || hostAV.Value != got.Verdict.EvacuatedBy {
					t.Fatalf("verdict says EvacuatedBy %q, the write sets eb to %q (me.Host %q)",
						got.Verdict.EvacuatedBy, hostAV.Value, meHost.Host)
				}
				if !fieldTouched(set, "eb") {
					t.Fatalf("evacuating release must SET svc.#k.eb: %s", set)
				}
				if fieldTouched(remove, "eb") {
					t.Fatalf("evacuating release must not REMOVE svc.#k.eb: %s", remove)
				}
				if !fieldTouched(set, "bv") {
					t.Fatalf("evacuating release must SET svc.#k.bv: %s", set)
				}
			} else {
				sawPlain = true
				if got.Verdict.EvacuatedBy != "" {
					t.Fatalf("non-evacuating release must not report EvacuatedBy, got %q", got.Verdict.EvacuatedBy)
				}
				if !fieldTouched(remove, "eb") {
					t.Fatalf("non-evacuating release must REMOVE svc.#k.eb: %s", remove)
				}
				if fieldTouched(set, "eb") {
					t.Fatalf("non-evacuating release must not SET svc.#k.eb: %s", set)
				}
			}
		})
	}
	if !sawEvacuate {
		t.Error("no release case exercised the evacuating branch")
	}
	if !sawPlain {
		t.Error("no release case exercised the non-evacuating branch")
	}
}

// TestTakeoverVerdictAgreesWithWrite pins actTakeover's verdict epoch against
// buildTakeover's write: a same-host restart re-registers under a new
// instance without bumping fencing, so buildTakeover must never touch
// svc.#k.e and the verdict must carry the SAME epoch already on the record.
func TestTakeoverVerdictAgreesWithWrite(t *testing.T) {
	var saw bool
	for _, c := range classifyCases() {
		got := classify(c.now, c.ttl, caseKey, c.op, c.e, c.exists, c.observer(caseKey), meHost)
		if got.Kind != actTakeover {
			continue
		}
		saw = true
		t.Run(c.name, func(t *testing.T) {
			in := buildTakeover("leases", caseKey, c.e, meHost, c.op.Serving)
			set, _ := splitUpdate(t, in)
			if fieldTouched(set, "e") {
				t.Fatalf("takeover must never touch the epoch: %s", set)
			}
			if got.Verdict.Epoch != int(c.e.Epoch) {
				t.Fatalf("verdict epoch %d does not match observed epoch %d (takeover must not bump it)",
					got.Verdict.Epoch, c.e.Epoch)
			}
		})
	}
	if !saw {
		t.Error("no case exercised actTakeover")
	}
}
