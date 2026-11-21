// Package dynarbiter judges lease expiry the way the DynamoDB backend must:
// there is no server clock behind a conditional write, only a shared item
// each host polls. So expiry is SILENCE — seconds since a counter last
// advanced, measured on the observing host's own monotonic clock, never on
// a deadline anyone wrote into the table. A fresh process has no history to
// measure against, so its first sighting of a key counts as that key
// advancing: silence starts at zero and must accrue a full TTL before the
// key can be judged expired, the same way a freshly started supervisor must
// not decide anything off a single poll.
//
// This file holds the observer only: pure state, folded in by Observe, read
// back by Silence/BarAge/Conflicted/HostSilence. No network, no AWS types,
// no goroutines — Task 3 owns decoding the wire item into an entry and
// calling Observe on a schedule.
package dynarbiter

// entry is one service's decoded state from the fleet item (Task 3 defines
// the wire attributes; this is the in-memory shape). Not every field is
// read by the observer — Epoch, Serving, BarHold, Released and PrevInstance
// exist here because they round-trip through the same decode, not because
// this file consumes them.
type entry struct {
	Holder       string
	Instance     string
	PrevInstance string
	Epoch        int64
	Version      int64
	Serving      bool
	EvacuatedBy  string
	BarHold      bool
	BarTTL       float64
	BarVersion   int64
	Released     bool
}

// hostEntry is one host's presence row: a counter (Beat) the host advances
// on every poll, observed the same silence-based way as a service's Version.
type hostEntry struct {
	Instance    string
	Fingerprint string
	Beat        int64
}

// barIdentity names one evacuation bar. BarTTL and BarVersion come from the
// entry itself, never from Observe's ttl argument — an operator changing
// the silence knob must not reset every bar's age.
type barIdentity struct {
	by      string
	ttl     float64
	version int64
}

// svcState is what the observer remembers about one service key between
// polls.
type svcState struct {
	version   int64
	versionAt float64

	holder   string
	instance string

	// Flip-flop history: the instance value this key held before its most
	// recent change, and when that change happened. Cleared on a holder
	// change (a different host entirely starts a new run) but NOT cleared
	// when a conflict window opens — conflictHost is deliberately its own
	// field, independent of the current holder, because the window must
	// survive the underlying lease expiring and a real peer taking it over
	// (that is failover working, not the conflict reopening).
	hasPrevInstance   bool
	prevInstance      string
	instanceChangedAt float64

	hasConflict   bool
	conflictUntil float64
	conflictHost  string

	hasBar    bool
	bar       barIdentity
	barSeenAt float64
}

// hostState is what the observer remembers about one host's presence beat.
type hostState struct {
	beat   int64
	beatAt float64
}

// observer accumulates poll snapshots into silence/bar/conflict state, keyed
// by service key and by host name. Zero value is not usable; use
// newObserver.
type observer struct {
	svcs  map[string]*svcState
	hosts map[string]*hostState
}

func newObserver() *observer {
	return &observer{svcs: map[string]*svcState{}, hosts: map[string]*hostState{}}
}

// Observe folds one poll's snapshot in. now is the observing host's own
// monotonic clock reading; ttl is the current silence threshold, needed
// only to size a flip-flop's conflict window. Missing keys are fine — a key
// absent from this poll simply isn't touched, its prior state stands.
func (o *observer) Observe(now, ttl float64, svcs map[string]entry, hosts map[string]hostEntry) {
	for key, e := range svcs {
		ss, existed := o.svcs[key]
		if !existed {
			ss = &svcState{}
			o.svcs[key] = ss
		}

		if !existed || e.Version != ss.version {
			ss.versionAt = now
		}
		ss.version = e.Version

		observeInstance(ss, now, ttl, existed, e)

		if e.EvacuatedBy == "" {
			ss.hasBar = false
		} else {
			id := barIdentity{by: e.EvacuatedBy, ttl: e.BarTTL, version: e.BarVersion}
			if !ss.hasBar || ss.bar != id {
				ss.barSeenAt = now
				ss.bar = id
			}
			ss.hasBar = true
		}
	}

	for h, he := range hosts {
		hs, existed := o.hosts[h]
		if !existed {
			hs = &hostState{}
			o.hosts[h] = hs
		}
		if !existed || he.Beat != hs.beat {
			hs.beatAt = now
		}
		hs.beat = he.Beat
	}
}

// observeInstance mirrors arbiter/src/lease.js renewOne's duplicate-host
// detection, translated from a write-time refusal to a read-time
// observation: renewOne checks the incoming instance against the lease's
// recorded prevInstance BEFORE overwriting it, so we do the same — check,
// then record, so a flip-flop is judged against the value that changed
// away, not the value just arrived.
func observeInstance(ss *svcState, now, ttl float64, existed bool, e entry) {
	if !existed {
		ss.holder = e.Holder
		ss.instance = e.Instance
		return
	}

	if e.Holder != ss.holder {
		// A different host holding the name starts a new run: the old
		// run's flip-flop history no longer means anything (spec: the
		// concept only spans a continuously-held run by one host name).
		ss.hasPrevInstance = false
		ss.holder = e.Holder
		ss.instance = e.Instance
		return
	}

	if e.Instance == ss.instance {
		return
	}

	// lease.js only runs this detection when both the old and new instance
	// are non-blank (l.instance !== null && req.instance); an unset
	// instance on either side of the change is not a value that can flip
	// back to, so it can't start or complete a flip-flop.
	if ss.instance != "" && e.Instance != "" {
		if ss.hasPrevInstance && ss.prevInstance == e.Instance && now-ss.instanceChangedAt < ttl {
			// A -> B -> A within one TTL: two live processes fighting over
			// this host name. lease.js returns early here, refusing the
			// write and freezing its recorded instance; there is no write
			// to refuse on the observing side, so ss.instance still moves
			// to what the table actually says (below) — only the conflict
			// window, not the stored value, is this branch's output.
			ss.hasConflict = true
			ss.conflictUntil = now + ttl
			ss.conflictHost = e.Holder
		} else {
			ss.hasPrevInstance = true
			ss.prevInstance = ss.instance
			ss.instanceChangedAt = now
		}
	}
	ss.instance = e.Instance
}

// Silence reports seconds since key's Version last advanced. First sighting
// counts as advancing, so a fresh process must observe a full ttl before it
// may claim anything off this reading. An unknown key reads as 0: never
// silent long enough to be claimable, since nothing has been observed at
// all.
func (o *observer) Silence(now float64, key string) float64 {
	ss, ok := o.svcs[key]
	if !ok {
		return 0
	}
	return now - ss.versionAt
}

// BarAge reports seconds since the bar currently on key (identified by its
// EvacuatedBy host, BarTTL and BarVersion together) was first observed. A
// changed identity — any of the three — resets the age, because it is a new
// bar even if EvacuatedBy repeats. 0 if key carries no bar right now.
func (o *observer) BarAge(now float64, key string) float64 {
	ss, ok := o.svcs[key]
	if !ok || !ss.hasBar {
		return 0
	}
	return now - ss.barSeenAt
}

// Conflicted reports whether key is inside a duplicate-host conflict window
// for host h: Instance flip-flopped A->B->A on a held lease within one ttl,
// as detected by observeInstance. The window is keyed to the host name that
// was conflicted, not to whoever currently holds the lease — see the
// comment on svcState's conflict fields for why those can diverge.
func (o *observer) Conflicted(now float64, key, h string) bool {
	ss, ok := o.svcs[key]
	if !ok || !ss.hasConflict {
		return false
	}
	return h == ss.conflictHost && now < ss.conflictUntil
}

// HostSilence reports seconds since h's Beat last advanced, first sighting
// counting as advancing exactly as in Silence. An unknown host reads as -1,
// not 0 — distinct from Silence's unknown-key 0, because a host that has
// never been observed is not "just claimed", it is absent from the fleet
// this observer has any opinion about.
func (o *observer) HostSilence(now float64, h string) float64 {
	hs, ok := o.hosts[h]
	if !ok {
		return -1
	}
	return now - hs.beatAt
}
