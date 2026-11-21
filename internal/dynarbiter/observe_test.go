package dynarbiter

import "testing"

func TestSilenceGrowsUntilVersionAdvances(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Version: 1}}, nil)
	if s := o.Silence(4, "web"); s != 4 {
		t.Fatalf("no version change yet: want 4, got %v", s)
	}
	// Same version reobserved: silence keeps growing off the original mark.
	o.Observe(5, 6, map[string]entry{"web": {Holder: "host-a", Version: 1}}, nil)
	if s := o.Silence(7, "web"); s != 7 {
		t.Fatalf("unchanged version must not reset silence: want 7, got %v", s)
	}
	o.Observe(8, 6, map[string]entry{"web": {Holder: "host-a", Version: 2}}, nil)
	if s := o.Silence(9, "web"); s != 1 {
		t.Fatalf("version advanced at t=8: want 1 at t=9, got %v", s)
	}
}

func TestFirstSightingRequiresFullTTL(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Version: 1}}, nil)
	if s := o.Silence(3, "web"); s != 3 {
		t.Fatalf("first sighting counts as advancing: want 3, got %v", s)
	}
}

func TestBarAgeResetsWhenBarIdentityChanges(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{
		"web": {Holder: "", EvacuatedBy: "host-a", BarTTL: 300, BarVersion: 1},
	}, nil)
	if a := o.BarAge(5, "web"); a != 5 {
		t.Fatalf("want 5, got %v", a)
	}
	// Same bar identity reobserved: age keeps growing off the first sighting.
	o.Observe(6, 6, map[string]entry{
		"web": {Holder: "", EvacuatedBy: "host-a", BarTTL: 300, BarVersion: 1},
	}, nil)
	if a := o.BarAge(9, "web"); a != 9 {
		t.Fatalf("unchanged bar identity must not reset age: want 9, got %v", a)
	}
	// BarVersion changes: a new bar, age resets.
	o.Observe(10, 6, map[string]entry{
		"web": {Holder: "", EvacuatedBy: "host-a", BarTTL: 300, BarVersion: 2},
	}, nil)
	if a := o.BarAge(12, "web"); a != 2 {
		t.Fatalf("changed bar identity resets age: want 2, got %v", a)
	}
	// Bar clears: no bar, age reports 0.
	o.Observe(13, 6, map[string]entry{"web": {Holder: "host-b"}}, nil)
	if a := o.BarAge(20, "web"); a != 0 {
		t.Fatalf("no bar: want 0, got %v", a)
	}
}

func TestFlipFlopWithinTTLOpensConflictWindow(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	o.Observe(1, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i2"}}, nil)
	o.Observe(4, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	if !o.Conflicted(5, "web", "host-a") {
		t.Fatal("want conflict open at t=5")
	}
	if o.Conflicted(5, "web", "some-other-host") {
		t.Fatal("conflict window is keyed to the conflicted host, not any host")
	}
	if o.Conflicted(11, "web", "host-a") {
		t.Fatal("want conflict closed at t=11 (opened at t=4, ttl 6 -> closes at 10)")
	}
}

func TestSingleInstanceChangeIsNotConflict(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	o.Observe(1, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i2"}}, nil)
	if o.Conflicted(2, "web", "host-a") {
		t.Fatal("a single instance change is a normal restart, not a conflict")
	}
	if o.Conflicted(100, "web", "host-a") {
		t.Fatal("no flip-flop was ever observed: never a conflict")
	}
}

func TestConflictWindowSurvivesHolderChange(t *testing.T) {
	o := newObserver()
	// host-a flip-flops i1 -> i2 -> i1 within the ttl: a conflict window
	// opens for host-a, closing at t=4+6=10.
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	o.Observe(1, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i2"}}, nil)
	o.Observe(4, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	if !o.Conflicted(5, "web", "host-a") {
		t.Fatal("want conflict open at t=5")
	}
	// A real peer takes the lease over: failover, not a reopening of the
	// conflict. observeInstance's holder-change branch resets flip-flop
	// history but deliberately leaves hasConflict/conflictUntil/conflictHost
	// untouched (see the comment on svcState and at observe.go:157-165) — the
	// window is keyed to host-a by name, independent of who currently holds
	// the lease, and must survive until it naturally expires.
	o.Observe(6, 6, map[string]entry{"web": {Holder: "host-b", Instance: "i3"}}, nil)
	if !o.Conflicted(9, "web", "host-a") {
		t.Fatal("conflict window must survive the holder changing to host-b")
	}
	if o.Conflicted(10, "web", "host-a") {
		t.Fatal("want conflict closed at t=10 (opened at t=4, ttl 6 -> closes at 10)")
	}
}

func TestHolderChangeClearsFlipFlopHistory(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	o.Observe(1, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i2"}}, nil)
	// A different host takes the lease: the i1<->i2 history belonged to
	// host-a's run and must not haunt host-b's.
	o.Observe(2, 6, map[string]entry{"web": {Holder: "host-b", Instance: "i3"}}, nil)
	o.Observe(3, 6, map[string]entry{"web": {Holder: "host-b", Instance: "i1"}}, nil)
	if o.Conflicted(4, "web", "host-b") {
		t.Fatal("holder change must clear flip-flop history: i1 reappearing is not a flip-flop")
	}
}

func TestBlankInstanceDoesNotStartOrCompleteAFlipFlop(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	// Instance goes blank, then back to i1: lease.js only runs its
	// detection when both the old and new instance are non-blank, so this
	// must not be recorded as a flip-flop step at all.
	o.Observe(1, 6, map[string]entry{"web": {Holder: "host-a", Instance: ""}}, nil)
	o.Observe(2, 6, map[string]entry{"web": {Holder: "host-a", Instance: "i1"}}, nil)
	if o.Conflicted(3, "web", "host-a") {
		t.Fatal("a blank instance in the middle must not count toward a flip-flop")
	}
}

func TestHostSilenceUnknownHostIsMinusOne(t *testing.T) {
	o := newObserver()
	if s := o.HostSilence(5, "nowhere"); s != -1 {
		t.Fatalf("want -1, got %v", s)
	}
}

func TestHostSilenceGrowsUntilBeatAdvances(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, nil, map[string]hostEntry{"host-a": {Beat: 1}})
	if s := o.HostSilence(3, "host-a"); s != 3 {
		t.Fatalf("first sighting counts as advancing: want 3, got %v", s)
	}
	o.Observe(4, 6, nil, map[string]hostEntry{"host-a": {Beat: 1}})
	if s := o.HostSilence(5, "host-a"); s != 5 {
		t.Fatalf("unchanged beat must not reset silence: want 5, got %v", s)
	}
	o.Observe(6, 6, nil, map[string]hostEntry{"host-a": {Beat: 2}})
	if s := o.HostSilence(7, "host-a"); s != 1 {
		t.Fatalf("beat advanced at t=6: want 1 at t=7, got %v", s)
	}
}

func TestObserveMissingKeysAreFine(t *testing.T) {
	o := newObserver()
	o.Observe(0, 6, map[string]entry{
		"web":  {Holder: "host-a", Version: 1},
		"blog": {Holder: "host-a", Version: 1},
	}, nil)
	// "blog" absent from this poll: its tracked state must be untouched,
	// not deleted or reset.
	o.Observe(3, 6, map[string]entry{"web": {Holder: "host-a", Version: 2}}, nil)
	if s := o.Silence(4, "blog"); s != 4 {
		t.Fatalf("missing key must retain prior state: want 4, got %v", s)
	}
	if s := o.Silence(4, "web"); s != 1 {
		t.Fatalf("observed key updates normally: want 1, got %v", s)
	}
}
