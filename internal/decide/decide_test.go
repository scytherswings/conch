package decide

import (
	"go/parser"
	"go/token"
	"testing"
)

var pol = Policy{DemoteAfter: 3, PromoteAfter: 6, Interval: 3,
	StopDeadline: 41, FreezeOnLoss: true, BackoffSeconds: 54}

func active(expiry float64) State { return State{Phase: Active, LeaseExpiry: expiry} }
func boolp(b bool) *bool          { return &b }
func intp(n int) *int             { return &n }

func fired(actions []Action, a Action) bool {
	for _, got := range actions {
		if got == a {
			return true
		}
	}
	return false
}

// assertActions: every wanted action is present, in any order.
func assertActions(t *testing.T, d Decision, want ...Action) {
	t.Helper()
	for _, w := range want {
		if !fired(d.Actions, w) {
			t.Fatalf("want action %s, got %v", w, d.Actions)
		}
	}
}

// assertNoAction: none of these may fire.
func assertNoAction(t *testing.T, d Decision, unwanted ...Action) {
	t.Helper()
	for _, u := range unwanted {
		if fired(d.Actions, u) {
			t.Fatalf("action %s must not fire, got %v", u, d.Actions)
		}
	}
}

// assertOrdered: the wanted actions appear in this relative order.
func assertOrdered(t *testing.T, got []Action, want ...Action) {
	t.Helper()
	i := 0
	for _, a := range got {
		if i < len(want) && a == want[i] {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("want %v in that order, got %v", want, got)
	}
}

// ---------------------------------------------------------------- scenarios

func Test_a_standby_needs_promote_after_consecutive_free_checks(t *testing.T) {
	s := State{Phase: Standby}
	for i := 0; i < 5; i++ {
		d := Decide(s, Inputs{NowMono: float64(i * 3), Eligible: true, Lease: Free}, pol)
		if d.NextOp == "renew" {
			t.Fatalf("claimed after only %d checks", i+1)
		}
		s = d.State
	}
	d := Decide(s, Inputs{NowMono: 15, Eligible: true, Lease: Free}, pol)
	if d.NextOp != "renew" {
		t.Fatalf("6th consecutive free check must claim, got %q", d.NextOp)
	}
}

func Test_any_non_free_reading_resets_the_streak(t *testing.T) {
	s := State{Phase: Standby, OKStreak: 5}
	d := Decide(s, Inputs{Eligible: true, Lease: Held}, pol)
	if d.State.OKStreak != 0 {
		t.Fatal("held must reset the streak")
	}
	d = Decide(State{Phase: Standby, OKStreak: 5}, Inputs{Eligible: false, Lease: Free}, pol)
	if d.State.OKStreak != 0 {
		t.Fatal("ineligible must reset the streak")
	}
}

func Test_grant_promotes_through_hooks_stopping_the_service_first(t *testing.T) {
	d := Decide(State{Phase: Standby, OKStreak: 6},
		Inputs{SentAt: 100, Lease: Yours, LeaseTTL: 60, Eligible: true}, pol)
	if d.State.Phase != Promoting {
		t.Fatalf("got %s", d.State.Phase)
	}
	if d.State.LeaseExpiry != 160 {
		t.Fatalf("expiry anchored at send: want 160, got %v", d.State.LeaseExpiry)
	}
	assertActions(t, d, StopService, RunPromoteHooks) // stop BEFORE restore (spec §5)
}

func Test_hooks_done_starts_service_then_connector(t *testing.T) {
	d := Decide(State{Phase: Promoting, LeaseExpiry: 160},
		Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "done"}, pol)
	if d.State.Phase != Active {
		t.Fatalf("got %s", d.State.Phase)
	}
	assertOrdered(t, d.Actions, StartService, StartConnector)
}

func Test_no_starts_past_the_stop_deadline_even_with_hooks_done(t *testing.T) {
	// expiry 160, deadline 160-41=119. At now=120 nothing may start.
	d := Decide(State{Phase: Promoting, LeaseExpiry: 160},
		Inputs{NowMono: 120, Lease: Unreachable, HookStatus: "done"}, pol)
	if d.State.Phase != Standby {
		t.Fatalf("must abandon, got %s", d.State.Phase)
	}
	assertNoAction(t, d, StartService, StartConnector)
	assertActions(t, d, Release)
}

func Test_a_hook_still_running_at_the_deadline_is_aborted(t *testing.T) {
	d := Decide(State{Phase: Promoting, LeaseExpiry: 160},
		Inputs{NowMono: 119, Lease: Unreachable, HookStatus: "running"}, pol)
	assertActions(t, d, AbortHooks, Release)
	if d.State.BackoffUntil == 0 {
		t.Fatal("abandoned promotion must back off")
	}
}

func Test_hook_failure_releases_and_backs_off(t *testing.T) {
	d := Decide(State{Phase: Promoting, LeaseExpiry: 160},
		Inputs{NowMono: 110, Lease: Yours, HookStatus: "failed"}, pol)
	if d.State.Phase != Standby || d.State.BackoffUntil != 110+54 {
		t.Fatalf("want standby with backoff until 164, got %+v", d.State)
	}
	// and while backed off, a free lease does not tick the streak:
	d2 := Decide(d.State, Inputs{NowMono: 120, Eligible: true, Lease: Free}, pol)
	if d2.State.OKStreak != 0 {
		t.Fatal("backoff must suppress claiming")
	}
}

func Test_active_demotes_after_demote_after_consecutive_bad_checks(t *testing.T) {
	s := active(1000)
	for i := 0; i < 2; i++ {
		d := Decide(s, Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900}, pol)
		if d.State.Phase != Active {
			t.Fatalf("demoted after %d bad checks", i+1)
		}
		if d.NextOp != "renew" {
			t.Fatal("hysteresis window still renews")
		}
		s = d.State
	}
	d := Decide(s, Inputs{NowMono: 906, Healthy: false, Lease: Yours}, pol)
	if d.State.Phase != Standby {
		t.Fatal("3rd consecutive bad check must demote")
	}
	assertOrdered(t, d.Actions, StopConnector, Release)
}

func Test_one_bad_check_then_recovery_resets_the_streak(t *testing.T) {
	s := State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000}
	d := Decide(s, Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900}, pol)
	if d.State.BadStreak != 0 {
		t.Fatal("recovery must reset the bad streak")
	}
}

func Test_refusal_stops_immediately_no_hysteresis(t *testing.T) {
	d := Decide(active(1000), Inputs{NowMono: 900, Healthy: true, Lease: Held}, pol)
	if d.State.Phase != Standby {
		t.Fatal("held while active = the arbiter reassigned it")
	}
	assertActions(t, d, StopConnector)
}

func Test_freeze_keeps_serving_past_expiry_on_unreachable(t *testing.T) {
	d := Decide(active(1000), Inputs{NowMono: 2000, Healthy: true, Lease: Unreachable}, pol)
	if d.State.Phase != Active {
		t.Fatal("freeze: hold shape, yell")
	}
	assertNoAction(t, d, StopConnector)
}

func Test_stop_mode_stops_at_the_deadline(t *testing.T) {
	strict := pol
	strict.FreezeOnLoss = false
	// expiry 1000 minus the 41s stop deadline puts the deadline at exactly 959, and the
	// contract stops at ≥ — so 958 is the last check inside the lease, not 959.
	d := Decide(active(1000), Inputs{NowMono: 958, Healthy: true, Lease: Unreachable}, strict)
	if d.State.Phase != Active {
		t.Fatal("958 < 1000-41: still inside the lease")
	}
	d = Decide(active(1000), Inputs{NowMono: 959, Healthy: true, Lease: Unreachable}, strict)
	if d.State.Phase != Standby {
		t.Fatal("959 >= deadline: never serve past your lease")
	}
	d = Decide(active(1000), Inputs{NowMono: 960, Healthy: true, Lease: Unreachable}, strict)
	if d.State.Phase != Standby {
		t.Fatal("960 >= deadline: never serve past your lease")
	}
}

func Test_freeze_still_demotes_a_sick_service(t *testing.T) {
	s := State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000}
	d := Decide(s, Inputs{NowMono: 2000, Healthy: false, Lease: Unreachable}, pol)
	if d.State.Phase != Standby {
		t.Fatal("stopping is always safe, even frozen")
	}
}

func Test_suspend_stops_first_and_rereads(t *testing.T) {
	d := Decide(active(1000), Inputs{NowMono: 500, Suspended: true, Healthy: true, Lease: Yours}, pol)
	if d.State.Phase != Standby {
		t.Fatal("unknown time passed: lease unproven")
	}
	assertActions(t, d, StopConnector)
}

func Test_conflict_demotes_and_alerts(t *testing.T) {
	d := Decide(active(1000), Inputs{NowMono: 500, Healthy: true, Lease: Conflict}, pol)
	if d.State.Phase != Standby {
		t.Fatal("duplicate_host fails closed")
	}
	assertActions(t, d, StopConnector, Alert)
}

func Test_active_restarts_a_stopped_connector(t *testing.T) {
	d := Decide(active(1000),
		Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900,
			ConnectorUp: boolp(false)}, pol)
	assertActions(t, d, StartConnector) // we hold the lease: starting is safe (spec §4)
}

func Test_standby_stops_a_connector_something_else_started(t *testing.T) {
	d := Decide(State{Phase: Standby}, Inputs{Lease: Held, ConnectorUp: boolp(true)}, pol)
	assertActions(t, d, StopConnector) // kamal accessory reboot on the wrong host (spec §4)
}

func Test_promotion_starts_accessories_before_the_connector(t *testing.T) {
	// Replication up before traffic: a write taken before the sidecar runs is
	// a write the replica does not have.
	d := Decide(State{Phase: Promoting, LeaseExpiry: 160},
		Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "done",
			AccessoriesUp: intp(0), AccessoriesTotal: 1}, pol)
	if d.State.Phase != Active {
		t.Fatalf("got %s", d.State.Phase)
	}
	assertOrdered(t, d.Actions, StartService, StartAccessories, StartConnector)
}

func Test_a_resumed_active_restarts_a_stopped_accessory(t *testing.T) {
	// The adoption case: hooks never run on adoption, so an adopted-ACTIVE
	// host with its replicator down would serve with replication silently
	// down forever if rule 12's reconcile did not cover accessories.
	d := Decide(active(1000),
		Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900,
			ConnectorUp: boolp(true), AccessoriesUp: intp(0), AccessoriesTotal: 1}, pol)
	assertActions(t, d, StartAccessories)
	assertNoAction(t, d, StartConnector)
}

// ----------------------------------------------------- the contract, by row

type contractCase struct {
	name    string
	s       State
	in      Inputs
	p       *Policy // nil = pol (freeze mode)
	phase   Phase
	op      string
	does    []Action
	doesNot []Action
	then    func(t *testing.T, d Decision)
}

func Test_the_contract_table(t *testing.T) {
	strict := pol
	strict.FreezeOnLoss = false
	every := []Action{StartService, StopService, StartConnector, StopConnector,
		StartAccessories, StopAccessories, RunPromoteHooks, AbortHooks, Release, Alert}

	cases := []contractCase{
		// ---- row 1: suspend, in every phase
		{
			name:  "row1/suspended standby stops the connector and re-reads",
			s:     State{Phase: Standby, OKStreak: 4},
			in:    Inputs{NowMono: 10, Suspended: true, Eligible: true, Lease: Free, ConnectorUp: boolp(true)},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 0 {
					t.Fatalf("unknown time passed: the streak proves nothing, got %d", d.State.OKStreak)
				}
			},
		},
		{
			name:  "row1/suspend during a promotion aborts the hooks",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 100, Suspended: true, Lease: Yours, LeaseTTL: 60, SentAt: 100, HookStatus: "running"},
			phase: Standby, op: "read",
			does:    []Action{AbortHooks, StopConnector, Alert},
			doesNot: []Action{StartService, StartConnector},
		},

		// ---- row 2: conflict, in every phase
		{
			name:  "row2/conflict in standby fails closed",
			s:     State{Phase: Standby, OKStreak: 3},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Conflict},
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{Release}, // a duplicate name is refused anyway
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 0 {
					t.Fatal("conflict must reset the streak")
				}
			},
		},
		{
			name:  "row2/conflict during a promotion aborts the hooks",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 100, Lease: Conflict, HookStatus: "running"},
			phase: Standby, op: "read",
			does:    []Action{AbortHooks, StopConnector, Alert},
			doesNot: []Action{Release, StartService, StartConnector},
		},

		// ---- row 3: standby reconciles the connector on every check
		{
			name:  "row3/a stray connector is stopped while the streak ticks",
			s:     State{Phase: Standby},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free, ConnectorUp: boolp(true)},
			phase: Standby, op: "read",
			does: []Action{StopConnector},
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 1 {
					t.Fatalf("reconciling does not interrupt the streak, got %d", d.State.OKStreak)
				}
			},
		},
		{
			name:  "row3/docker could not say, so nothing is touched",
			s:     State{Phase: Standby},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free},
			phase: Standby, op: "read",
			doesNot: every,
		},

		// ---- row 4: the claim streak
		{
			name:  "row4/the streak ticks without claiming early",
			s:     State{Phase: Standby, OKStreak: 4},
			in:    Inputs{NowMono: 30, Eligible: true, Lease: Free},
			phase: Standby, op: "read",
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 5 {
					t.Fatalf("want 5, got %d", d.State.OKStreak)
				}
			},
		},
		{
			name:  "row4/the sixth check sends the claim",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 30, Eligible: true, Lease: Free},
			phase: Standby, op: "renew",
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 6 {
					t.Fatalf("want 6, got %d", d.State.OKStreak)
				}
			},
		},
		{
			name:  "row4/a live backoff suppresses the tick",
			s:     State{Phase: Standby, BackoffUntil: 200},
			in:    Inputs{NowMono: 199, Eligible: true, Lease: Free},
			phase: Standby, op: "read",
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 0 {
					t.Fatal("a host whose hooks just failed may not re-claim")
				}
			},
		},
		{
			name:  "row4/the backoff expires on its own",
			s:     State{Phase: Standby, BackoffUntil: 200},
			in:    Inputs{NowMono: 200, Eligible: true, Lease: Free},
			phase: Standby, op: "read",
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 1 {
					t.Fatalf("want the streak ticking again, got %d", d.State.OKStreak)
				}
			},
		},

		// ---- row 5: every other reading resets the streak
		{
			name:  "row5/held by the peer",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Held},
			phase: Standby, op: "read", doesNot: every,
			then: wantOKStreak(0),
		},
		{
			name:  "row5/barred by our own evacuation",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Barred},
			phase: Standby, op: "read", doesNot: every,
			then: wantOKStreak(0),
		},
		{
			name:  "row5/the arbiter could not be reached",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Unreachable},
			phase: Standby, op: "read", doesNot: every,
			then: wantOKStreak(0),
		},
		{
			name:  "row5/free but not eligible",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: false, Lease: Free},
			phase: Standby, op: "read", doesNot: every,
			then: wantOKStreak(0),
		},

		// ---- row 6: the grant
		{
			name:  "row6/the grant arms the expiry from the send and stops a stray connector",
			s:     State{Phase: Standby, OKStreak: 6},
			in:    Inputs{NowMono: 103, SentAt: 100, Lease: Yours, LeaseTTL: 60, Eligible: true, ConnectorUp: boolp(true)},
			phase: Promoting, op: "renew",
			does: []Action{StopConnector, StopService, RunPromoteHooks, Alert},
			then: func(t *testing.T, d Decision) {
				if d.State.LeaseExpiry != 160 {
					t.Fatalf("send-anchored expiry: want 160, got %v", d.State.LeaseExpiry)
				}
				if d.State.OKStreak != 0 || d.State.HooksDone {
					t.Fatalf("promotion starts from a clean slate, got %+v", d.State)
				}
			},
		},

		// ---- row 7: hooks running inside the lease
		{
			name:  "row7/a running hook keeps the lease alive under it",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "running"},
			phase: Promoting, op: "renew",
			doesNot: every,
			then: func(t *testing.T, d Decision) {
				if d.State.LeaseExpiry != 170 {
					t.Fatalf("the renewal pushes the expiry out: want 170, got %v", d.State.LeaseExpiry)
				}
			},
		},
		{
			name:  "row7/an unreachable arbiter mid-hooks waits until the deadline",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 118, Lease: Unreachable, HookStatus: "running"},
			phase: Promoting, op: "renew",
			doesNot: every,
		},
		{
			name:  "row7/hooks not yet reporting are waited on, not restarted",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110},
			phase: Promoting, op: "renew",
			doesNot: every,
		},

		// ---- row 8: hooks done
		{
			name:  "row8/hooks done renews while the containers come up",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "done"},
			phase: Active, op: "renew",
			does:    []Action{StartService, StartConnector, Alert},
			doesNot: []Action{Release, AbortHooks},
			then: func(t *testing.T, d Decision) {
				if !d.State.HooksDone {
					t.Fatal("the hooks finished: record it")
				}
				if d.State.BadStreak != 0 {
					t.Fatal("a fresh ACTIVE starts with a clean health streak")
				}
			},
		},

		// ---- row 9: hook failure
		{
			name:  "row9/a failed hook releases and backs off without an abort",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 110, Lease: Yours, HookStatus: "failed"},
			phase: Standby, op: "release",
			does:    []Action{Release, Alert},
			doesNot: []Action{AbortHooks, StartService, StartConnector},
			then: func(t *testing.T, d Decision) {
				if d.State.BackoffUntil != 164 {
					t.Fatalf("want backoff until 164, got %v", d.State.BackoffUntil)
				}
			},
		},

		// ---- row 10: the deadline dominates the promotion
		{
			name:  "row10/at the deadline the promotion is abandoned",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 119, Lease: Yours, HookStatus: "running"},
			phase: Standby, op: "release",
			does:    []Action{AbortHooks, Release, Alert},
			doesNot: []Action{StartService, StartConnector},
			then: func(t *testing.T, d Decision) {
				if d.State.BackoffUntil != 173 {
					t.Fatalf("want backoff until 173, got %v", d.State.BackoffUntil)
				}
			},
		},
		{
			name:  "row10/a promotion on an unarmed expiry proves nothing",
			s:     State{Phase: Promoting},
			in:    Inputs{NowMono: 5, Lease: Unreachable, HookStatus: "done"},
			phase: Standby, op: "release",
			does:    []Action{AbortHooks, Release, Alert},
			doesNot: []Action{StartService, StartConnector},
		},

		// ---- row 11: the lease is lost mid-hooks
		{
			name:  "row11/held by someone else mid-hooks",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 100, Lease: Held, HookStatus: "running"},
			phase: Standby, op: "read",
			does:    []Action{AbortHooks, Alert},
			doesNot: []Action{Release, StartService, StartConnector},
		},
		{
			name:  "row11/an evacuation bar lands mid-hooks",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 100, Lease: Barred, HookStatus: "done"},
			phase: Standby, op: "read",
			does:    []Action{AbortHooks, Alert},
			doesNot: []Action{Release, StartService, StartConnector},
		},

		// ---- row 12: healthy and serving
		{
			name:  "row12/a healthy active with its connector up just renews",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900, ConnectorUp: boolp(true)},
			phase: Active, op: "renew",
			doesNot: every,
			then: func(t *testing.T, d Decision) {
				if d.State.LeaseExpiry != 960 {
					t.Fatalf("every renewal re-anchors at the send: want 960, got %v", d.State.LeaseExpiry)
				}
			},
		},
		{
			name:  "row12/docker could not say, so no connector is started",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900},
			phase: Active, op: "renew",
			doesNot: every,
		},
		{
			name:  "row12/a refusal beats the connector restart",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Held, ConnectorUp: boolp(false)},
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{StartConnector},
		},

		// ---- row 13: the hysteresis window
		{
			name:  "row13/one bad check keeps renewing",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900, ConnectorUp: boolp(false)},
			phase: Active, op: "renew",
			doesNot: every, // not even the connector reconcile: we are on our way out
			then: func(t *testing.T, d Decision) {
				if d.State.BadStreak != 1 {
					t.Fatalf("want 1, got %d", d.State.BadStreak)
				}
			},
		},

		// ---- row 14: the demotion
		{
			name:  "row14/the third bad check releases",
			s:     State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900},
			phase: Standby, op: "release",
			does:    []Action{StopConnector, Release, Alert},
			doesNot: []Action{StartConnector},
			then: func(t *testing.T, d Decision) {
				if d.State.BadStreak != 0 {
					t.Fatal("STANDBY starts from a clean slate")
				}
			},
		},

		// ---- row 15: the refusal
		{
			name:  "row15/a refusal reads next, it does not release",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Held},
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{Release},
		},
		{
			name:  "row15/a free lease while serving is not ours either",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Free},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},
		{
			name:  "row15/a bar landing while serving stops us",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Barred},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},

		// ---- row 16: freeze
		{
			name:  "row16/freeze holds the shape and yells once, and starts nothing",
			s:     active(1000),
			in:    Inputs{NowMono: 2000, Healthy: true, Lease: Unreachable, ConnectorUp: boolp(false)},
			phase: Active, op: "renew",
			does:    []Action{Alert},
			doesNot: []Action{StopConnector, Release, StartConnector}, // freeze never authorizes a start
			then: func(t *testing.T, d Decision) {
				if !d.State.LossAlerted {
					t.Fatal("the first unreachable check must record that it alerted")
				}
			},
		},
		{
			name:  "row16/the second unreachable check does not yell again",
			s:     State{Phase: Active, LeaseExpiry: 1000, LossAlerted: true},
			in:    Inputs{NowMono: 2003, Healthy: true, Lease: Unreachable},
			phase: Active, op: "renew",
			doesNot: every,
		},
		{
			name:  "row16/contact restored re-arms the alert",
			s:     State{Phase: Active, LeaseExpiry: 1000, LossAlerted: true},
			in:    Inputs{NowMono: 2003, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 2003},
			phase: Active, op: "renew",
			then: func(t *testing.T, d Decision) {
				if d.State.LossAlerted {
					t.Fatal("a reachable arbiter clears the alert latch")
				}
			},
		},
		{
			name:  "row16/freeze does not need an armed expiry to keep serving",
			s:     State{Phase: Active},
			in:    Inputs{NowMono: 5, Healthy: true, Lease: Unreachable},
			phase: Active, op: "renew",
			does:    []Action{Alert},
			doesNot: []Action{StopConnector},
		},

		// ---- rows 17 and 18: stop mode
		{
			name:  "row17/stop mode stops at the deadline",
			s:     active(1000),
			in:    Inputs{NowMono: 959, Healthy: true, Lease: Unreachable},
			p:     &strict,
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{Release}, // there is nobody to release to
		},
		{
			name:  "row17/an unarmed expiry proves nothing, so stop",
			s:     State{Phase: Active},
			in:    Inputs{NowMono: 5, Healthy: true, Lease: Unreachable},
			p:     &strict,
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},
		{
			name:  "row18/inside the lease stop mode keeps serving but starts nothing",
			s:     active(1000),
			in:    Inputs{NowMono: 958, Healthy: true, Lease: Unreachable, ConnectorUp: boolp(false)},
			p:     &strict,
			phase: Active, op: "renew",
			doesNot: every, // a lease we cannot confirm authorizes no start either
		},

		// ---- row 19: an evacuation belongs to the supervisor
		{
			name:  "row19/an evacuating active is left alone, bookkeeping and all",
			s:     State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000, LossAlerted: true},
			in:    Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900, Evacuating: true},
			phase: Active, op: "renew",
			doesNot: every,
			then: func(t *testing.T, d Decision) {
				want := State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000, LossAlerted: true}
				if d.State != want {
					t.Fatalf("state must be untouched, got %+v", d.State)
				}
			},
		},
		{
			name:  "row19/an evacuation does not outlive the arbiter under freeze",
			s:     State{Phase: Active, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 2000, Healthy: true, Lease: Unreachable, Evacuating: true, ConnectorUp: boolp(false)},
			phase: Active, op: "renew",
			does:    []Action{Alert}, // spec §9: the evacuation ends and gets reported
			doesNot: []Action{StartConnector, StopConnector, Release},
		},
		{
			name:  "row19/an evacuation does not outlive the deadline in stop mode",
			s:     State{Phase: Active, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 959, Healthy: true, Lease: Unreachable, Evacuating: true},
			p:     &strict,
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},
		{
			name:  "row19/an evacuating standby with no arbiter still just stands by",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Unreachable, Evacuating: true},
			phase: Standby, op: "read",
			then: wantOKStreak(0),
		},

		// ---- the readings that are not readings at all
		{
			name:  "unknown/a lease value outside the enum is not a lease",
			s:     active(1000),
			in:    Inputs{NowMono: 959, Healthy: true, Lease: LeaseState("kinda-yours"), ConnectorUp: boolp(false)},
			p:     &strict,
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{StartConnector},
		},
		{
			name:  "unknown/the zero value is not a lease either",
			s:     active(1000),
			in:    Inputs{NowMono: 959, Healthy: true, ConnectorUp: boolp(false)},
			p:     &strict,
			phase: Standby, op: "read",
			does:    []Action{StopConnector, Alert},
			doesNot: []Action{StartConnector},
		},
		{
			name:  "unknown/under freeze an unreadable lease holds shape, it does not start things",
			s:     active(1000),
			in:    Inputs{NowMono: 2000, Healthy: true, Lease: LeaseState("???"), ConnectorUp: boolp(false)},
			phase: Active, op: "renew",
			does:    []Action{Alert},
			doesNot: []Action{StartConnector, StopConnector},
		},
		{
			name:  "unknown/an unreadable lease never ticks a claim",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: LeaseState("free-ish")},
			phase: Standby, op: "read",
			then: wantOKStreak(0),
		},
		{
			name:  "unknown/an unrecognised phase fails closed",
			s:     State{Phase: Phase("SERVING"), LeaseExpiry: 1000},
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},

		// ---- bookkeeping happens before the deadline is tested
		{
			name:  "expiry/a renewal arriving in the lease's last moment saves the host",
			s:     State{Phase: Promoting, LeaseExpiry: 100},
			in:    Inputs{NowMono: 90, Lease: Yours, LeaseTTL: 60, SentAt: 90, HookStatus: "running"},
			phase: Promoting, op: "renew", // old deadline 59 is long past; the new one is 109
			doesNot: every,
			then: func(t *testing.T, d Decision) {
				if d.State.LeaseExpiry != 150 {
					t.Fatalf("the renewal must be booked before the deadline is tested, got %v", d.State.LeaseExpiry)
				}
			},
		},
		{
			name:  "expiry/a ttl-less grant does not extend the deadline",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 119, Lease: Yours, SentAt: 119, HookStatus: "running"},
			phase: Standby, op: "release",
			does: []Action{AbortHooks, Release, Alert},
			then: func(t *testing.T, d Decision) {
				if d.State.LeaseExpiry != 0 {
					t.Fatalf("a lease we no longer hold proves nothing, got %v", d.State.LeaseExpiry)
				}
			},
		},
		{
			name:  "row19/an evacuating host claims nothing",
			s:     State{Phase: Standby, OKStreak: 5},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free, Evacuating: true, ConnectorUp: boolp(true)},
			phase: Standby, op: "renew",
			doesNot: every,
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 5 {
					t.Fatal("a host on its way out must not tick toward a claim")
				}
			},
		},
		{
			name:  "row19/suspend still wins over an evacuation",
			s:     State{Phase: Active, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 500, Suspended: true, Healthy: true, Lease: Yours, Evacuating: true},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},
		{
			name:  "row19/conflict still wins over an evacuation",
			s:     State{Phase: Active, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 500, Healthy: true, Lease: Conflict, Evacuating: true},
			phase: Standby, op: "read",
			does: []Action{StopConnector, Alert},
		},

		// ---- accessories ride the same rows the connector does. A sidecar
		// runs on exactly the host that serves, so every stop path that names
		// StopConnector owes StopAccessories when one is provably running,
		// and the two reconciles that start a stopped connector under a lease
		// that is ours (rules 8 and 12) start a stopped accessory too. nil is
		// docker not saying, which justifies no accessory action at all.
		{
			name:  "row1/a suspend stops a running accessory with the connector",
			s:     active(1000),
			in:    Inputs{NowMono: 500, Suspended: true, Healthy: true, Lease: Yours, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Standby, op: "read",
			does: []Action{StopConnector, StopAccessories, Alert},
		},
		{
			name:  "row2/a conflict stops a running accessory with the connector",
			s:     active(1000),
			in:    Inputs{NowMono: 500, Healthy: true, Lease: Conflict, AccessoriesUp: intp(1), AccessoriesTotal: 2},
			phase: Standby, op: "read",
			does: []Action{StopConnector, StopAccessories, Alert},
		},
		{
			name:  "row3/a standby stops an accessory something else started",
			s:     State{Phase: Standby},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Standby, op: "read",
			does:    []Action{StopAccessories},
			doesNot: []Action{StopConnector}, // the connector reading is nil: not its business
			then: func(t *testing.T, d Decision) {
				if d.State.OKStreak != 1 {
					t.Fatalf("reconciling does not interrupt the streak, got %d", d.State.OKStreak)
				}
			},
		},
		{
			name:  "row3/accessories all stopped is the standby steady state",
			s:     State{Phase: Standby},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free, AccessoriesUp: intp(0), AccessoriesTotal: 2},
			phase: Standby, op: "read",
			doesNot: every,
		},
		{
			name:  "row3/docker could not count the accessories, so none is touched",
			s:     State{Phase: Standby},
			in:    Inputs{NowMono: 10, Eligible: true, Lease: Free, AccessoriesTotal: 2},
			phase: Standby, op: "read",
			doesNot: every,
		},
		{
			name:  "row6/the grant's promotion stops a running accessory before the restore",
			s:     State{Phase: Standby, OKStreak: 6},
			in:    Inputs{NowMono: 103, SentAt: 100, Lease: Yours, LeaseTTL: 60, Eligible: true, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Promoting, op: "renew",
			does: []Action{StopAccessories, StopService, RunPromoteHooks},
			then: func(t *testing.T, d Decision) {
				assertOrdered(t, d.Actions, StopAccessories, RunPromoteHooks)
			},
		},
		{
			name:  "row8/hooks done with uncountable accessories starts everything but them",
			s:     State{Phase: Promoting, LeaseExpiry: 160},
			in:    Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "done", AccessoriesTotal: 1},
			phase: Active, op: "renew",
			does:    []Action{StartService, StartConnector},
			doesNot: []Action{StartAccessories}, // rule 12 heals it once docker answers
		},
		{
			name:  "row12/an active with every accessory up starts nothing",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900, ConnectorUp: boolp(true), AccessoriesUp: intp(2), AccessoriesTotal: 2},
			phase: Active, op: "renew",
			doesNot: every,
		},
		{
			name:  "row12/docker could not count the accessories, so none is started",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900, ConnectorUp: boolp(true), AccessoriesTotal: 1},
			phase: Active, op: "renew",
			doesNot: every,
		},
		{
			name:  "row14/the demotion stops a running accessory with the connector",
			s:     State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000},
			in:    Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Standby, op: "release",
			does: []Action{StopConnector, StopAccessories, Release, Alert},
		},
		{
			name:  "row15/a refusal stops a running accessory with the connector",
			s:     active(1000),
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Held, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Standby, op: "read",
			does: []Action{StopConnector, StopAccessories, Alert},
		},
		{
			name:  "row16/freeze never authorizes an accessory start either",
			s:     active(1000),
			in:    Inputs{NowMono: 2000, Healthy: true, Lease: Unreachable, AccessoriesUp: intp(0), AccessoriesTotal: 1},
			phase: Active, op: "renew",
			does:    []Action{Alert},
			doesNot: []Action{StartAccessories, StopAccessories},
		},
		{
			name:  "row17/the stop deadline stops a running accessory with the connector",
			s:     active(1000),
			in:    Inputs{NowMono: 959, Healthy: true, Lease: Unreachable, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			p:     &strict,
			phase: Standby, op: "read",
			does: []Action{StopConnector, StopAccessories, Alert},
		},
		{
			name:  "row18/inside the lease stop mode starts no accessory either",
			s:     active(1000),
			in:    Inputs{NowMono: 958, Healthy: true, Lease: Unreachable, AccessoriesUp: intp(0), AccessoriesTotal: 1},
			p:     &strict,
			phase: Active, op: "renew",
			doesNot: every, // a lease we cannot confirm authorizes no start
		},
		{
			name:  "unknown/an unrecognised phase stops a running accessory as it fails closed",
			s:     State{Phase: Phase("SERVING"), LeaseExpiry: 1000},
			in:    Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900, AccessoriesUp: intp(1), AccessoriesTotal: 1},
			phase: Standby, op: "read",
			does: []Action{StopConnector, StopAccessories, Alert},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := pol
			if c.p != nil {
				p = *c.p
			}
			d := Decide(c.s, c.in, p)
			if d.State.Phase != c.phase {
				t.Fatalf("phase: want %s, got %s (%s)", c.phase, d.State.Phase, d.Reason)
			}
			if d.NextOp != c.op {
				t.Fatalf("next op: want %q, got %q (%s)", c.op, d.NextOp, d.Reason)
			}
			assertActions(t, d, c.does...)
			assertNoAction(t, d, subtract(c.doesNot, c.does)...)
			if d.Reason == "" {
				t.Fatal("every decision must say why")
			}
			if c.then != nil {
				c.then(t, d)
			}
		})
	}
}

// A lease we no longer hold proves nothing, so every route out of a held lease
// drops the expiry with it. Carrying a stale expiry into STANDBY would let a
// later ACTIVE — reached without a grant of its own — test a deadline it never
// earned, which is the one arithmetic that must never be wrong.
func Test_every_road_to_standby_drops_the_expiry(t *testing.T) {
	strict := pol
	strict.FreezeOnLoss = false
	roads := []struct {
		name string
		s    State
		in   Inputs
		p    Policy
	}{
		{"demoted while unhealthy",
			State{Phase: Active, BadStreak: 2, LeaseExpiry: 1000},
			Inputs{NowMono: 900, Healthy: false, Lease: Yours, LeaseTTL: 60, SentAt: 900}, pol},
		{"the renewal was refused",
			active(1000), Inputs{NowMono: 900, Healthy: true, Lease: Held}, pol},
		{"the stop deadline passed with no arbiter",
			active(1000), Inputs{NowMono: 959, Healthy: true, Lease: Unreachable}, strict},
		{"a duplicate host name",
			active(1000), Inputs{NowMono: 900, Healthy: true, Lease: Conflict}, pol},
		{"unknown time passed",
			active(1000), Inputs{NowMono: 900, Suspended: true, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900}, pol},
		{"the promote hooks failed",
			State{Phase: Promoting, LeaseExpiry: 160},
			Inputs{NowMono: 110, Lease: Yours, LeaseTTL: 60, SentAt: 110, HookStatus: "failed"}, pol},
		{"the promotion hit its deadline",
			State{Phase: Promoting, LeaseExpiry: 160},
			Inputs{NowMono: 119, Lease: Unreachable, HookStatus: "running"}, pol},
		{"the lease was lost mid-hooks",
			State{Phase: Promoting, LeaseExpiry: 160},
			Inputs{NowMono: 110, Lease: Held, HookStatus: "running"}, pol},
		{"the phase made no sense",
			State{Phase: Phase("SERVING"), LeaseExpiry: 1000},
			Inputs{NowMono: 900, Healthy: true, Lease: Yours, LeaseTTL: 60, SentAt: 900}, pol},
	}
	for _, r := range roads {
		t.Run(r.name, func(t *testing.T) {
			d := Decide(r.s, r.in, r.p)
			if d.State.Phase != Standby {
				t.Fatalf("expected STANDBY, got %s (%s)", d.State.Phase, d.Reason)
			}
			if d.State.LeaseExpiry != 0 {
				t.Fatalf("stale expiry carried into STANDBY: %v", d.State.LeaseExpiry)
			}
			if d.State.OKStreak != 0 || d.State.BadStreak != 0 || d.State.HooksDone || d.State.LossAlerted {
				t.Fatalf("STANDBY starts from a clean slate, got %+v", d.State)
			}
		})
	}
}

func wantOKStreak(n int) func(*testing.T, Decision) {
	return func(t *testing.T, d Decision) {
		t.Helper()
		if d.State.OKStreak != n {
			t.Fatalf("OKStreak: want %d, got %d", n, d.State.OKStreak)
		}
	}
}

func subtract(from, remove []Action) []Action {
	out := []Action{}
	for _, a := range from {
		if !fired(remove, a) {
			out = append(out, a)
		}
	}
	return out
}

// ------------------------------------------------------------- purity guard

func Test_decide_is_pure(t *testing.T) {
	allowed := map[string]bool{`"fmt"`: true, `"strings"`: true}
	f, err := parser.ParseFile(token.NewFileSet(), "decide.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse decide.go: %v", err)
	}
	for _, imp := range f.Imports {
		if !allowed[imp.Path.Value] {
			t.Fatalf("the decision core must stay pure: %s is a leak", imp.Path.Value)
		}
	}
}
