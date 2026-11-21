package dynarbiter

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/decide"
)

// ---- test harness -------------------------------------------------

const testTTL = 30.0

var (
	opRenew   = arbiter.Op{Op: "renew", Serving: true}
	opRead    = arbiter.Op{Op: "read"}
	opRelease = arbiter.Op{Op: "release"}
)

// countingDDB wraps the fake with the call counter the brief asks for, plus
// a record of every expression sent (so a test can prove WHICH writes went
// out, not just how many) and an UpdateItem failure injector the fake's own
// errNext/errAlways can't express: those fire on the next call whatever it
// is, and several tests need GetItem to keep working while writes fail.
type countingDDB struct {
	inner *fakeDDB

	mu         sync.Mutex
	gets       int
	updates    int
	updateExpr []string
	condExpr   []string

	failUpdate error  // returned instead of calling through, when set
	before     func() // runs before every call (clock advancing, etc.)
}

func (c *countingDDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	c.mu.Lock()
	c.gets++
	before := c.before
	c.mu.Unlock()
	if before != nil {
		before()
	}
	return c.inner.GetItem(ctx, in, opts...)
}

func (c *countingDDB) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	c.mu.Lock()
	c.updates++
	c.updateExpr = append(c.updateExpr, *in.UpdateExpression)
	cond := ""
	if in.ConditionExpression != nil {
		cond = *in.ConditionExpression
	}
	c.condExpr = append(c.condExpr, cond)
	before, fail := c.before, c.failUpdate
	c.mu.Unlock()
	if before != nil {
		before()
	}
	if fail != nil {
		return nil, fail
	}
	return c.inner.UpdateItem(ctx, in, opts...)
}

func (c *countingDDB) setBefore(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.before = fn
}

func (c *countingDDB) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets, c.updates = 0, 0
	c.updateExpr, c.condExpr = nil, nil
}

func (c *countingDDB) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.updates
}

func (c *countingDDB) wrote(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.updateExpr {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func (c *countingDDB) conditioned(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.condExpr {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// seedExpr and seedHostExpr are what a seed write looks like on the wire;
// tests assert on their presence and absence, so they are spelled once.
const (
	seedFleetExpr = "SET svc = if_not_exists(svc, :empty)"
	seedHostExpr  = "SET host.#me = if_not_exists(host.#me, :entry)"
)

func newTestClient(ddb DDB, host, instance string, clk clock.Clock) *Client {
	return New(ddb, "conch", host, instance, 5, clk)
}

func fleetState(t *testing.T, f *fakeDDB) (map[string]entry, map[string]hostEntry) {
	t.Helper()
	svcs, hosts, err := decodeFleet(f.GetFleetForTest(t))
	if err != nil {
		t.Fatalf("decode fleet: %v", err)
	}
	return svcs, hosts
}

func wantVerdict(t *testing.T, r arbiter.Reply, key string, lease decide.LeaseState) arbiter.Verdict {
	t.Helper()
	v, ok := r.Services[key]
	if !ok {
		t.Fatalf("reply has no verdict for %q: %v", key, r.Services)
	}
	if v.Lease != lease {
		t.Fatalf("%s: got %v (%+v) want %v", key, v.Lease, v, lease)
	}
	return v
}

func wantAllUnreachable(t *testing.T, r arbiter.Reply, ops map[string]arbiter.Op) {
	t.Helper()
	if len(r.Services) != len(ops) {
		t.Fatalf("verdict count: got %d want %d (%v)", len(r.Services), len(ops), r.Services)
	}
	for key := range ops {
		if v := r.Services[key]; v.Lease != decide.Unreachable {
			t.Fatalf("%s: got %v want unreachable", key, v.Lease)
		}
	}
}

// svcEntryLocked reaches into the fake's live item for the tests that have
// to mutate a service behind a client's back. Callers must already hold
// f.mu — every use here is from inside the onUpdate hook, which the fake
// runs with the lock held.
func svcEntryLocked(f *fakeDDB, key string) map[string]types.AttributeValue {
	svcs := f.items["fleet"][attrSvc].(*types.AttributeValueMemberM)
	return svcs.Value[key].(*types.AttributeValueMemberM).Value
}

func numLocked(m map[string]types.AttributeValue, attr string) int64 {
	n, err := strconv.ParseInt(m[attr].(*types.AttributeValueMemberN).Value, 10, 64)
	if err != nil {
		panic(err)
	}
	return n
}

// stealLocked is a peer's successful claim, applied straight to the item:
// new holder, new instance, epoch+1, version+1.
func stealLocked(f *fakeDDB, key, host, instance string) {
	e := svcEntryLocked(f, key)
	e[svcHolder] = avS(host)
	e[svcInstance] = avS(instance)
	e[svcEpoch] = avN(numLocked(e, svcEpoch) + 1)
	e[svcVersion] = avN(numLocked(e, svcVersion) + 1)
}

// releaseLocked is a peer's release applied straight to the item: holder
// and instance gone, version bumped, no bar.
func releaseLocked(f *fakeDDB, key string) {
	e := svcEntryLocked(f, key)
	delete(e, svcHolder)
	delete(e, svcInstance)
	e[svcVersion] = avN(numLocked(e, svcVersion) + 1)
}

// setHolderLocked moves the lease to a host without touching epoch or
// version — the minimum a peer's write has to change for our CAS to be
// refused, and reversible, which is what lets a test refuse the same key
// twice while every read still shows it as ours.
func setHolderLocked(f *fakeDDB, key, host, instance string) {
	e := svcEntryLocked(f, key)
	e[svcHolder] = avS(host)
	e[svcInstance] = avS(instance)
}

// bumpVersionLocked is any successful write by the current holder — a peer's
// renew landing between our read and our write.
func bumpVersionLocked(f *fakeDDB, key string) {
	e := svcEntryLocked(f, key)
	e[svcVersion] = avN(numLocked(e, svcVersion) + 1)
}

// ---- seeding (step 2) -------------------------------------------------

func TestFirstBatchClaimsFreeFleet(t *testing.T) {
	f := newFakeDDB() // a table with no fleet item at all
	clk := clock.NewFake()
	c := newTestClient(f, "host-a", "inst-1", clk)

	ops := map[string]arbiter.Op{"web": opRenew, "api": opRenew}
	reply := c.Batch(testTTL, "fp-1", ops)

	for _, key := range []string{"web", "api"} {
		v := wantVerdict(t, reply, key, decide.Yours)
		if v.Epoch != 1 {
			t.Fatalf("%s: epoch %d want 1", key, v.Epoch)
		}
		if v.TTL != testTTL {
			t.Fatalf("%s: ttl %v want %v", key, v.TTL, testTTL)
		}
	}
	if reply.SentAt != 0 {
		t.Fatalf("SentAt: got %v want the clock reading before any I/O (0)", reply.SentAt)
	}

	svcs, hosts := fleetState(t, f)
	for _, key := range []string{"web", "api"} {
		e := svcs[key]
		if e.Holder != "host-a" || e.Instance != "inst-1" || e.Epoch != 1 || e.Version != 1 || !e.Serving {
			t.Fatalf("%s entry: %+v", key, e)
		}
	}
	// The beat is the presence signal every peer reads us by, so it must
	// have advanced even on a poll whose only writes were claims.
	if hosts["host-a"].Beat != 1 || hosts["host-a"].Fingerprint != "fp-1" || hosts["host-a"].Instance != "inst-1" {
		t.Fatalf("host entry: %+v", hosts["host-a"])
	}
}

func TestFirstPollSeedsHostEntryBeforeRenew(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	c := newTestClient(cd, "host-a", "inst-1", clk)

	// Unseeded, the renew this poll ends with would not have failed its CAS
	// — it would have failed the document-path rule, which is not a race and
	// must never be retried as one.
	_, err := newFakeDDB().UpdateItem(context.Background(),
		buildRenew("conch", nil, hostSpec{Host: "host-a", Instance: "inst-1"}))
	if err == nil {
		t.Fatalf("expected the unseeded renew to fail (the whole reason seeding exists)")
	}

	ops := map[string]arbiter.Op{"web": opRenew}
	reply := c.Batch(testTTL, "fp-1", ops)
	wantVerdict(t, reply, "web", decide.Yours)

	cd.mu.Lock()
	first := append([]string(nil), cd.updateExpr...)
	cd.mu.Unlock()
	if len(first) < 2 || !strings.HasPrefix(first[0], seedFleetExpr) || first[1] != seedHostExpr {
		t.Fatalf("the two seed writes must come first, got %q", first)
	}

	// Once per process: the second poll goes straight to the read.
	cd.reset()
	reply = c.Batch(testTTL, "fp-1", ops)
	wantVerdict(t, reply, "web", decide.Yours)
	if cd.wrote("if_not_exists(svc,") || cd.wrote(seedHostExpr) {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		t.Fatalf("second Batch re-seeded: %q", cd.updateExpr)
	}
}

func TestSeedFailureIsUnreachableThenRetried(t *testing.T) {
	f := newFakeDDB()
	f.errNext = errors.New("throttled")
	clk := clock.NewFake()
	c := newTestClient(f, "host-a", "inst-1", clk)

	ops := map[string]arbiter.Op{"web": opRenew, "api": opRead}
	reply := c.Batch(testTTL, "fp-1", ops)
	wantAllUnreachable(t, reply, ops)
	if reply.Peers != nil || reply.Fingerprints != nil {
		t.Fatalf("a failed poll must claim nothing about the fleet: %+v", reply)
	}
	if got := f.GetFleetForTest(t); len(got) != 0 {
		t.Fatalf("nothing should have been written: %v", got)
	}

	// Seeding is retried at the top of the next poll, not skipped for the
	// life of the process.
	reply = c.Batch(testTTL, "fp-1", ops)
	wantVerdict(t, reply, "web", decide.Yours)
	if _, hosts := fleetState(t, f); hosts["host-a"].Beat != 1 {
		t.Fatalf("host entry after the retried seed: %+v", hosts)
	}
}

// TestOutOfBandDeletionSelfHeals pins the fix for a wedge that only shows up
// after seeding: once c.seeded is true, seed() short-circuits on every later
// poll, so if an operator's stray DeleteItem removes the fleet item, the
// document-path ancestors buildRenew/buildClaim/etc. all assume are there
// are gone too — every write comes back a ValidationException, forever,
// because nothing was left to flip c.seeded back to false. write() clearing
// c.seeded on any non-condition-failed error is what lets the very next
// poll's seed() actually run again and recreate what's missing.
func TestOutOfBandDeletionSelfHeals(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	c := newTestClient(cd, "host-a", "inst-1", clk)

	ops := map[string]arbiter.Op{"web": opRenew}
	reply := c.Batch(testTTL, "fp-1", ops)
	wantVerdict(t, reply, "web", decide.Yours)

	// Operator error: the whole item disappears out from under an already-
	// seeded client.
	f.mu.Lock()
	delete(f.items, "fleet")
	f.mu.Unlock()

	reply = c.Batch(testTTL, "fp-1", ops)
	wantAllUnreachable(t, reply, ops)

	// Without a process restart: the next poll re-seeds — both seed writes
	// must be on the wire, not just a passing verdict, or a coincidental
	// success (e.g. buildClaim's never-seen branch tolerating a missing
	// parent some other way) could pass this test for the wrong reason.
	cd.reset()
	reply = c.Batch(testTTL, "fp-1", ops)
	wantVerdict(t, reply, "web", decide.Yours)
	if !cd.wrote(seedFleetExpr) || !cd.wrote(seedHostExpr) {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		t.Fatalf("the healing poll must re-seed, got %q", cd.updateExpr)
	}
}

// ---- call budget (steps 3 and 6) -------------------------------------

func TestSteadyStateIsOneReadOneWrite(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	c := newTestClient(cd, "host-a", "inst-1", clk)

	ops := map[string]arbiter.Op{"web": opRenew, "api": opRenew}
	c.Batch(testTTL, "fp-1", ops) // claims both

	cd.reset()
	clk.Advance(5)
	reply := c.Batch(testTTL, "fp-1", ops)

	gets, updates := cd.counts()
	if gets != 1 || updates != 1 {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		t.Fatalf("steady state: got %d GetItem + %d UpdateItem, want 1 + 1 (%q)", gets, updates, cd.updateExpr)
	}
	for _, key := range []string{"web", "api"} {
		v := wantVerdict(t, reply, key, decide.Yours)
		if v.Epoch != 1 {
			t.Fatalf("%s: renewing must not move the epoch, got %d", key, v.Epoch)
		}
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Version != 2 || svcs["api"].Version != 2 {
		t.Fatalf("one batched renew must bump both versions: %+v %+v", svcs["web"], svcs["api"])
	}
}

// A host that only READS still has to say it is alive: presence is what
// spec §9's evacuation pre-flight asks about ("is there a peer to hand this
// to?") and what fingerprint-drift detection reads, and lease.js records it
// on every /batch regardless of the ops in it. So every poll carries the
// beat write, batched with whatever renews there are or on its own.
func TestReadOnlyPollStillWritesTheBeat(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	c := newTestClient(cd, "host-b", "inst-b", clk)

	ops := map[string]arbiter.Op{"web": opRead}
	c.Batch(testTTL, "fp-b", ops)
	if _, hosts := fleetState(t, f); hosts["host-b"].Beat != 1 {
		t.Fatalf("read-only poll must still advance the beat: %+v", hosts["host-b"])
	}

	cd.reset()
	clk.Advance(5)
	reply := c.Batch(testTTL, "fp-b", ops)
	wantVerdict(t, reply, "web", decide.Free) // nobody has ever claimed it

	gets, updates := cd.counts()
	if gets != 1 || updates != 1 {
		t.Fatalf("read-only poll: got %d GetItem + %d UpdateItem, want 1 + 1", gets, updates)
	}
	if _, hosts := fleetState(t, f); hosts["host-b"].Beat != 2 {
		t.Fatalf("beat did not advance on the second poll: %+v", hosts["host-b"])
	}
}

// A batched renew is capped so one fenced service costs a bounded number of
// its neighbours a round trip. The supervisor's own limit is 64 services, so
// a full fleet is four chunks — and every one of them still has to land.
func TestRenewsChunkAtTwenty(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	c := newTestClient(cd, "host-a", "inst-a", clk)

	ops := map[string]arbiter.Op{}
	for i := 0; i < 45; i++ {
		ops["svc-"+strconv.Itoa(i)] = opRenew
	}
	c.Batch(testTTL, "fp-a", ops) // claims all 45, one write each

	cd.reset()
	clk.Advance(5)
	reply := c.Batch(testTTL, "fp-a", ops)

	gets, updates := cd.counts()
	if gets != 1 || updates != 3 {
		t.Fatalf("45 renews: got %d GetItem + %d UpdateItem, want 1 + 3", gets, updates)
	}
	if len(reply.Services) != len(ops) {
		t.Fatalf("verdict count: got %d want %d", len(reply.Services), len(ops))
	}
	svcs, _ := fleetState(t, f)
	for key := range ops {
		wantVerdict(t, reply, key, decide.Yours)
		if svcs[key].Version != 2 {
			t.Fatalf("%s: version %d want 2", key, svcs[key].Version)
		}
	}
	// Three chunks, three beat bumps: the host entry rides every renew.
	if _, hosts := fleetState(t, f); hosts["host-a"].Beat != 4 {
		t.Fatalf("beat: %+v", hosts["host-a"])
	}
}

// ---- contention (steps 6-8) -------------------------------------------

func TestContenderClaimsAfterSilence(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(f, "host-b", "inst-b", clk)

	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Yours)

	// B's first sighting counts as the key advancing, so it must wait out a
	// full TTL of its own before it may judge A silent.
	v := wantVerdict(t, b.Batch(testTTL, "fp-b", ops), "web", decide.Held)
	if v.Holder != "host-a" {
		t.Fatalf("holder: got %q want host-a", v.Holder)
	}
	clk.Advance(testTTL - 1)
	wantVerdict(t, b.Batch(testTTL, "fp-b", ops), "web", decide.Held)

	// A has stopped renewing; past the TTL the lease is B's to take, at a
	// bumped epoch so A's own next renew is fenced off.
	clk.Advance(2)
	v = wantVerdict(t, b.Batch(testTTL, "fp-b", ops), "web", decide.Yours)
	if v.Epoch != 2 {
		t.Fatalf("epoch: got %d want 2", v.Epoch)
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "host-b" || svcs["web"].Epoch != 2 {
		t.Fatalf("web after takeover: %+v", svcs["web"])
	}

	// And A, still believing it holds the lease, is refused and told the
	// truth on the same poll — never its own optimistic verdict.
	v = wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Held)
	if v.Holder != "host-b" || v.Epoch != 2 {
		t.Fatalf("fenced holder's verdict: %+v", v)
	}
}

// TestFencedHolderRetriesAndDrops is step 7's whole point: one batched
// renew covers many services, so ONE stolen service must not cost the
// others their leases.
func TestFencedHolderRetriesAndDrops(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	a := newTestClient(cd, "host-a", "inst-a", clk)

	ops := map[string]arbiter.Op{"web": opRenew, "api": opRenew}
	a.Batch(testTTL, "fp-a", ops) // A holds both

	// host-b claims api in the window between A's read and A's write: the
	// hook fires inside UpdateItem, before the condition is evaluated.
	stolen := false
	f.mu.Lock()
	f.onUpdate = func() {
		if !stolen {
			stolen = true
			stealLocked(f, "api", "host-b", "inst-b")
		}
	}
	f.mu.Unlock()

	cd.reset()
	clk.Advance(5)
	reply := a.Batch(testTTL, "fp-a", ops)

	v := wantVerdict(t, reply, "web", decide.Yours)
	if v.Epoch != 1 {
		t.Fatalf("surviving service: epoch %d want 1", v.Epoch)
	}
	v = wantVerdict(t, reply, "api", decide.Held)
	if v.Holder != "host-b" || v.Epoch != 2 {
		t.Fatalf("stolen service: %+v", v)
	}

	gets, updates := cd.counts()
	if gets != 2 || updates != 2 {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		t.Fatalf("retry round: got %d GetItem + %d UpdateItem, want 2 + 2 (%q)", gets, updates, cd.updateExpr)
	}

	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "host-a" || svcs["web"].Version != 2 {
		t.Fatalf("web after the retry: %+v", svcs["web"])
	}
	if svcs["api"].Holder != "host-b" {
		t.Fatalf("api must be left alone: %+v", svcs["api"])
	}
}

// TestTwiceRefusedRenewIsUnreachable covers the one branch where a wrong
// verdict turns into split-brain rather than a wasted poll: the retry chunk
// is refused too. Reporting the retry's optimistic Yours{TTL} there would
// have decide arm LeaseExpiry = SentAt + TTL on a lease another host has
// already taken, and we would serve a full TTL past losing it. There is no
// third read, so the only answer left is "no answer".
func TestTwiceRefusedRenewIsUnreachable(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	a := newTestClient(cd, "host-a", "inst-a", clk)

	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Yours)

	// A peer that holds the lease every time we WRITE and has handed it back
	// every time we READ: the CAS can never land, and no re-read ever tells
	// us why. Contrived on purpose — it is the only way to reach the second
	// refusal, and the branch has to answer correctly when it happens.
	prevUpdates := 0
	cd.setBefore(func() {
		_, updates := cd.counts()
		isWrite := updates > prevUpdates
		prevUpdates = updates
		f.mu.Lock()
		defer f.mu.Unlock()
		if isWrite {
			setHolderLocked(f, "web", "host-b", "inst-b")
		} else {
			setHolderLocked(f, "web", "host-a", "inst-a")
		}
	})

	cd.reset()
	clk.Advance(5)
	reply := a.Batch(testTTL, "fp-a", ops)

	v := reply.Services["web"]
	if v.Lease != decide.Unreachable {
		t.Fatalf("twice-refused renew: got %+v, want unreachable — no write landed, so no verdict was earned", v)
	}
	if v.TTL != 0 {
		t.Fatalf("twice-refused renew must grant nothing: %+v", v)
	}
	if gets, updates := cd.counts(); gets != 2 || updates != 2 {
		t.Fatalf("twice-refused renew: %d GetItem + %d UpdateItem, want 2 + 2 (one retry round, no third read)", gets, updates)
	}
}

// When every renew in the poll is refused, the beat rolls back with them —
// the host entry rides the same atomic UpdateItem. The retry round is what
// puts presence back, even with nothing left to retry, so losing every lease
// at once does not also make us look dead to the peer taking over.
func TestRefusedRenewStillRecordsPresence(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	a := newTestClient(cd, "host-a", "inst-a", clk)

	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Yours)
	_, hosts := fleetState(t, f)
	beatBefore := hosts["host-a"].Beat

	stolen := false
	f.mu.Lock()
	f.onUpdate = func() {
		if !stolen {
			stolen = true
			stealLocked(f, "web", "host-b", "inst-b")
		}
	}
	f.mu.Unlock()

	cd.reset()
	clk.Advance(5)
	v := wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Held)
	if v.Holder != "host-b" {
		t.Fatalf("holder: %+v", v)
	}
	if gets, updates := cd.counts(); gets != 2 || updates != 2 {
		t.Fatalf("all-refused poll: %d GetItem + %d UpdateItem, want 2 + 2 (the refused chunk, then the beat)", gets, updates)
	}
	if _, hosts := fleetState(t, f); hosts["host-a"].Beat != beatBefore+1 {
		t.Fatalf("presence must survive losing every lease: beat %d want %d", hosts["host-a"].Beat, beatBefore+1)
	}
}

// TestRefusedRenewNeverReturnsAnUnwrittenClaim is the invariant the brief's
// step 7 states too loosely ("keys reclassified to anything else get that
// verdict"). A verdict is earned by a write that LANDED; a fresh
// classification that calls for a write we have not sent is not a reading,
// it is a plan. Reachable trace: a peer steals the lease and releases it
// between our read and our renew, so the re-read shows it free — and
// classify says "claim it, you'd be Yours at epoch+1". Returning that
// without the write would hand the supervisor a lease nobody granted.
func TestRefusedRenewNeverReturnsAnUnwrittenClaim(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)

	ops := map[string]arbiter.Op{"web": opRenew, "api": opRenew}
	a.Batch(testTTL, "fp-a", ops)

	moved := false
	f.mu.Lock()
	f.onUpdate = func() {
		if !moved {
			moved = true
			stealLocked(f, "api", "host-b", "inst-b")
			releaseLocked(f, "api") // ...and host-b gives it straight back
		}
	}
	f.mu.Unlock()

	clk.Advance(5)
	reply := a.Batch(testTTL, "fp-a", ops)

	wantVerdict(t, reply, "web", decide.Yours)
	if v := reply.Services["api"]; v.Lease != decide.Unreachable {
		t.Fatalf("api: got %v (%+v), want unreachable — the claim was never sent", v.Lease, v)
	}
	svcs, _ := fleetState(t, f)
	if svcs["api"].Holder != "" {
		t.Fatalf("no claim should have been written for api: %+v", svcs["api"])
	}
}

func TestNContendersOneWinner(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()

	// A lease held by a host that is about to go quiet forever.
	dead := newTestClient(f, "host-dead", "inst-dead", clk)
	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, dead.Batch(testTTL, "fp-dead", ops), "web", decide.Yours)

	const n = 4
	clients := make([]*Client, n)
	for i := range clients {
		host := "host-" + strconv.Itoa(i)
		clients[i] = newTestClient(f, host, host+"-inst", clk)
		// First sighting: everyone starts their own silence clock here.
		wantVerdict(t, clients[i].Batch(testTTL, "fp-"+host, ops), "web", decide.Held)
	}

	// Advance before the goroutines start: clock.Fake is not safe to mutate
	// while it is being read.
	clk.Advance(testTTL + 1)

	// The hook cannot block until its peers arrive — it runs holding the
	// fake's lock, so waiting there deadlocks every other writer. A start
	// barrier plus a yield inside the hook is what interleaves them.
	f.mu.Lock()
	f.onUpdate = func() { runtime.Gosched() }
	f.mu.Unlock()

	start := make(chan struct{})
	replies := make([]arbiter.Reply, n)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			replies[i] = clients[i].Batch(testTTL, "fp", ops)
		}(i)
	}
	close(start)
	wg.Wait()

	winners, losers := 0, 0
	for i, r := range replies {
		v := r.Services["web"]
		switch v.Lease {
		case decide.Yours:
			winners++
			if v.Epoch != 2 {
				t.Fatalf("client %d won at epoch %d, want 2", i, v.Epoch)
			}
		case decide.Held:
			// A refused claim means the winner's write landed, so the loser's
			// re-read sees a version that just moved: silence 0, hence Held
			// and never anything weaker.
			losers++
			if v.Epoch != 2 {
				t.Fatalf("client %d saw epoch %d, want the winner's 2", i, v.Epoch)
			}
			if v.Holder == "" || v.Holder == "host-dead" {
				t.Fatalf("client %d: holder %q", i, v.Holder)
			}
		default:
			t.Fatalf("client %d: unexpected verdict %+v", i, v)
		}
	}
	if winners != 1 || losers != n-1 {
		t.Fatalf("winners %d losers %d, want 1 and %d", winners, losers, n-1)
	}

	svcs, _ := fleetState(t, f)
	if svcs["web"].Epoch != 2 || svcs["web"].Holder == "host-dead" {
		t.Fatalf("web after the race: %+v", svcs["web"])
	}
}

// A conch that restarts finds its own name on the record under a dead
// instance id. That is the takeover path — the third write kind, and the
// only one that must NOT move the epoch: lease.js bumps the fencing token
// when a holder CHANGES, and a restart is the same holder.
func TestSameHostRestartTakesOverItsOwnLease(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()

	old := newTestClient(f, "host-a", "inst-1", clk)
	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, old.Batch(testTTL, "fp-a", ops), "web", decide.Yours)

	// The new process: its own observer, its own seeded flag, nothing
	// carried over but the host name.
	fresh := newTestClient(cd, "host-a", "inst-2", clk)
	clk.Advance(5)
	v := wantVerdict(t, fresh.Batch(testTTL, "fp-a2", ops), "web", decide.Yours)
	if v.Epoch != 1 {
		t.Fatalf("restart adoption must not move the epoch: got %d want 1", v.Epoch)
	}

	svcs, hosts := fleetState(t, f)
	e := svcs["web"]
	if e.Holder != "host-a" || e.Instance != "inst-2" || e.PrevInstance != "inst-1" || e.Epoch != 1 || e.Version != 2 {
		t.Fatalf("web after takeover: %+v", e)
	}
	// The new process re-seeds, and that seed must be a no-op: a beat of 2
	// (not 1) proves host.<me> was left exactly as the old process left it.
	if hosts["host-a"].Beat != 2 || hosts["host-a"].Fingerprint != "fp-a2" {
		t.Fatalf("host entry after a restart's re-seed: %+v", hosts["host-a"])
	}

	// Once the instances agree it drops back into the batched renew.
	cd.reset()
	clk.Advance(5)
	v = wantVerdict(t, fresh.Batch(testTTL, "fp-a2", ops), "web", decide.Yours)
	if v.Epoch != 1 {
		t.Fatalf("steady renew after a restart: epoch %d want 1", v.Epoch)
	}
	if gets, updates := cd.counts(); gets != 1 || updates != 1 {
		t.Fatalf("post-restart steady state: %d GetItem + %d UpdateItem, want 1 + 1", gets, updates)
	}
	if svcs, _ := fleetState(t, f); svcs["web"].Version != 3 {
		t.Fatalf("version after the renew: %+v", svcs["web"])
	}
}

// A refused RELEASE gets the same treatment as a refused claim: no verdict
// the write did not earn. Reporting Free/Released here would tell the
// supervisor it had handed the lease over when it had not.
func TestRefusedReleaseIsNeverReportedAsReleased(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)

	wantVerdict(t, a.Batch(testTTL, "fp-a", map[string]arbiter.Op{"web": opRenew}), "web", decide.Yours)

	// Something else writes web between A's read and A's release, so the
	// (holder, version) CAS no longer matches what A saw.
	bumped := false
	f.mu.Lock()
	f.onUpdate = func() {
		if !bumped {
			bumped = true
			bumpVersionLocked(f, "web")
		}
	}
	f.mu.Unlock()

	clk.Advance(5)
	reply := a.Batch(testTTL, "fp-a", map[string]arbiter.Op{"web": opRelease})
	v := reply.Services["web"]
	if v.Lease != decide.Unreachable || v.Released {
		t.Fatalf("refused release: got %+v, want unreachable and not released", v)
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "host-a" || !svcs["web"].Serving {
		t.Fatalf("the lease must be untouched by a refused release: %+v", svcs["web"])
	}
}

// ---- failure handling (step 8) ---------------------------------------

func TestAnyAWSErrorIsAllUnreachable(t *testing.T) {
	ops := map[string]arbiter.Op{"web": opRenew, "api": opRead, "db": opRenew}
	boom := errors.New("throttled")

	t.Run("seed", func(t *testing.T) {
		f := newFakeDDB()
		f.errAlways = boom
		c := newTestClient(f, "host-a", "inst-a", clock.NewFake())
		wantAllUnreachable(t, c.Batch(testTTL, "fp", ops), ops)
	})

	t.Run("get", func(t *testing.T) {
		f := newFakeDDB()
		c := newTestClient(f, "host-a", "inst-a", clock.NewFake())
		c.Batch(testTTL, "fp", ops) // seeds and claims
		f.mu.Lock()
		f.errAlways = boom
		f.mu.Unlock()
		wantAllUnreachable(t, c.Batch(testTTL, "fp", ops), ops)
	})

	t.Run("update", func(t *testing.T) {
		f := newFakeDDB()
		cd := &countingDDB{inner: f}
		c := newTestClient(cd, "host-a", "inst-a", clock.NewFake())
		c.Batch(testTTL, "fp", ops)

		cd.mu.Lock()
		cd.failUpdate = boom
		cd.mu.Unlock()
		// Even the key that only wanted a READ goes unreachable: partial
		// truth is worse than no truth.
		wantAllUnreachable(t, c.Batch(testTTL, "fp", ops), ops)
	})
}

// A ValidationException is a bug in the request, not a lost race. Treating
// one as a condition failure would send the client round the re-read/retry
// path forever against a request that can never succeed.
func TestValidationErrorIsNeverAConditionRace(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	c := newTestClient(cd, "host-a", "inst-a", clock.NewFake())

	ops := map[string]arbiter.Op{"web": opRenew}
	c.Batch(testTTL, "fp", ops)

	cd.reset()
	cd.mu.Lock()
	cd.failUpdate = newValidationErr("The document path provided in the update expression is invalid for update")
	cd.mu.Unlock()

	wantAllUnreachable(t, c.Batch(testTTL, "fp", ops), ops)
	if gets, _ := cd.counts(); gets != 1 {
		t.Fatalf("a ValidationException must not trigger the retry round's re-read: %d GetItems", gets)
	}
}

func TestStaleReadNeverStealsALiveLease(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(cd, "host-b", "inst-b", clk)

	ops := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Yours)
	wantVerdict(t, b.Batch(testTTL, "fp-b", ops), "web", decide.Held)

	// Freeze what B's GetItem is served at the snapshot it has just seen,
	// while A keeps renewing against current state. The fake ignores
	// ConsistentRead (documented divergence), so the test retires the stale
	// window itself at the moment real DynamoDB would: B's first write of
	// the poll proves B is talking to the live table.
	f.mu.Lock()
	frozen := len(f.versions) - 1
	f.onUpdate = func() { f.staleReads = 0 }
	f.mu.Unlock()

	pin := func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.staleReads = len(f.versions) - 1 - frozen
	}

	// B's stale view of web froze at t=0, so its silence reading reaches the
	// TTL on the third of these polls — the poll where B genuinely believes
	// the lease has lapsed and goes for it.
	for i := 0; i < 3; i++ {
		clk.Advance(10)
		wantVerdict(t, a.Batch(testTTL, "fp-a", ops), "web", decide.Yours)
		pin()
		if i == 2 {
			cd.reset()
		}
		reply := b.Batch(testTTL, "fp-b", ops)
		if i < 2 {
			// Silence is accruing on a version B is no longer being told
			// about, but the answer is still Held until it passes the TTL.
			wantVerdict(t, reply, "web", decide.Held)
			continue
		}
		// The CAS is on the stale version, so the claim is refused, and the
		// re-read is what turns the answer back into the truth.
		v := wantVerdict(t, reply, "web", decide.Held)
		if v.Holder != "host-a" {
			t.Fatalf("holder after the refused claim: %+v", v)
		}
	}

	if !cd.conditioned("svc.#k.v = :obsV") {
		t.Fatalf("B never actually attempted the claim, so nothing was refused")
	}
	if gets, updates := cd.counts(); gets != 2 || updates != 2 {
		t.Fatalf("refused claim: %d GetItem + %d UpdateItem, want 2 + 2 (beat, claim, re-read — the retry round adds no write of its own)", gets, updates)
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "host-a" || svcs["web"].Epoch != 1 {
		t.Fatalf("the live lease must be untouched: %+v", svcs["web"])
	}
}

// ---- release and bars -------------------------------------------------

func TestReleaseWithBarThenPeerClaim(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(f, "host-b", "inst-b", clk)

	renewOps := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", renewOps), "web", decide.Yours)

	evac := map[string]arbiter.Op{"web": {Op: "release", Evacuate: true, BarTTL: 60}}
	v := wantVerdict(t, a.Batch(testTTL, "fp-a", evac), "web", decide.Free)
	if !v.Released || v.EvacuatedBy != "host-a" {
		t.Fatalf("release verdict: %+v", v)
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "" || svcs["web"].EvacuatedBy != "host-a" || svcs["web"].BarHold || svcs["web"].BarTTL != 60 {
		t.Fatalf("evacuating release: %+v", svcs["web"])
	}

	// The bar was not a hold, so the peer's claim takes it with it.
	v = wantVerdict(t, b.Batch(testTTL, "fp-b", renewOps), "web", decide.Yours)
	if v.Epoch != 2 {
		t.Fatalf("peer claim epoch: %d want 2", v.Epoch)
	}
	svcs, _ = fleetState(t, f)
	if svcs["web"].Holder != "host-b" || svcs["web"].EvacuatedBy != "" || svcs["web"].BarTTL != 0 {
		t.Fatalf("peer claim must clear a non-hold bar: %+v", svcs["web"])
	}

	// And A, coming back with a plain renew, is simply told the truth.
	wantVerdict(t, a.Batch(testTTL, "fp-a", renewOps), "web", decide.Held)
}

func TestReleaseWithHoldBarSurvivesPeerClaim(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(f, "host-b", "inst-b", clk)

	renewOps := map[string]arbiter.Op{"web": opRenew}
	wantVerdict(t, a.Batch(testTTL, "fp-a", renewOps), "web", decide.Yours)

	// `conch evacuate --hold` sends no bar_ttl at all: a bar with no clock.
	hold := map[string]arbiter.Op{"web": {Op: "release", Evacuate: true, Hold: true}}
	wantVerdict(t, a.Batch(testTTL, "fp-a", hold), "web", decide.Free)
	svcs, _ := fleetState(t, f)
	barVersion := svcs["web"].BarVersion
	if !svcs["web"].BarHold || svcs["web"].EvacuatedBy != "host-a" || barVersion == 0 {
		t.Fatalf("hold release: %+v", svcs["web"])
	}

	// The lease goes to the peer; the bar outlives it, so A cannot drift
	// back onto the data it evacuated without an explicit override.
	wantVerdict(t, b.Batch(testTTL, "fp-b", renewOps), "web", decide.Yours)
	svcs, _ = fleetState(t, f)
	if svcs["web"].Holder != "host-b" {
		t.Fatalf("peer claim: %+v", svcs["web"])
	}
	if !svcs["web"].BarHold || svcs["web"].EvacuatedBy != "host-a" || svcs["web"].BarVersion != barVersion {
		t.Fatalf("hold bar must survive the peer's claim intact: %+v", svcs["web"])
	}

	// A's own return is refused by its own bar until it overrides.
	v := wantVerdict(t, a.Batch(testTTL, "fp-a", renewOps), "web", decide.Held)
	if v.Holder != "host-b" {
		t.Fatalf("A's verdict under the peer's live hold: %+v", v)
	}
}

// A release we are not entitled to send is our own bug, not a peer's claim
// — and it must cost nothing on the wire beyond the poll's own beat.
func TestReleaseOfSomebodyElsesLeaseIsRefusedLocally(t *testing.T) {
	f := newFakeDDB()
	cd := &countingDDB{inner: f}
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(cd, "host-b", "inst-b", clk)

	wantVerdict(t, a.Batch(testTTL, "fp-a", map[string]arbiter.Op{"web": opRenew}), "web", decide.Yours)
	b.Batch(testTTL, "fp-b", map[string]arbiter.Op{"web": opRead}) // seeds B

	cd.reset()
	ops := map[string]arbiter.Op{"web": opRelease}
	v := wantVerdict(t, b.Batch(testTTL, "fp-b", ops), "web", decide.Unreachable)
	if v.Err != "not_holder" {
		t.Fatalf("err: got %q want not_holder", v.Err)
	}
	if _, updates := cd.counts(); updates != 1 {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		t.Fatalf("only the beat should have been written: %q", cd.updateExpr)
	}
	svcs, _ := fleetState(t, f)
	if svcs["web"].Holder != "host-a" {
		t.Fatalf("web must be untouched: %+v", svcs["web"])
	}
}

// ---- reply assembly (step 9) -----------------------------------------

func TestReplyKeysExactlyMatchOps(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	other := newTestClient(f, "host-other", "inst-o", clk)
	other.Batch(testTTL, "fp-o", map[string]arbiter.Op{"web": opRenew, "cache": opRenew})

	c := newTestClient(f, "host-a", "inst-a", clk)
	ops := map[string]arbiter.Op{
		"web":     opRead,
		"api":     opRenew,
		"unknown": {Op: "wat"},
	}
	reply := c.Batch(testTTL, "fp-a", ops)

	if len(reply.Services) != len(ops) {
		t.Fatalf("verdict count: got %d want %d (%v)", len(reply.Services), len(ops), reply.Services)
	}
	for key := range ops {
		if _, ok := reply.Services[key]; !ok {
			t.Fatalf("missing verdict for %q", key)
		}
	}
	if _, ok := reply.Services["cache"]; ok {
		t.Fatalf("a service we did not ask about must not appear: %v", reply.Services)
	}
	v := wantVerdict(t, reply, "unknown", decide.Unreachable)
	if v.Err != "invalid_op" {
		t.Fatalf("unknown op: %+v", v)
	}
	wantVerdict(t, reply, "web", decide.Held)
	wantVerdict(t, reply, "api", decide.Yours)
}

func TestPeersOmitSelfAndSilentHour(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	a := newTestClient(f, "host-a", "inst-a", clk)
	b := newTestClient(f, "host-b", "inst-b", clk)

	ops := map[string]arbiter.Op{"web": opRead}
	a.Batch(testTTL, "fp-a", ops)
	b.Batch(testTTL, "fp-b", ops)
	// A's first sighting of host-b's beat counts as that beat advancing, so
	// silence is measured from this poll, not from B's.
	a.Batch(testTTL, "fp-a", ops)

	clk.Advance(5)
	reply := a.Batch(testTTL, "fp-a", ops)
	if _, ok := reply.Peers["host-a"]; ok {
		t.Fatalf("peers must omit self: %v", reply.Peers)
	}
	if sil, ok := reply.Peers["host-b"]; !ok || sil != 5 {
		t.Fatalf("host-b silence: got %v (present %v) want 5", sil, ok)
	}
	if reply.Fingerprints["host-b"] != "fp-b" {
		t.Fatalf("fingerprints: %v", reply.Fingerprints)
	}
	if _, ok := reply.Fingerprints["host-a"]; ok {
		t.Fatalf("fingerprints must omit self: %v", reply.Fingerprints)
	}

	// An hour of silence and the host is not a peer any more — the same
	// window lease.js forgets a host after (PRESENCE_TTL_MS).
	clk.Advance(3600)
	reply = a.Batch(testTTL, "fp-a", ops)
	if _, ok := reply.Peers["host-b"]; ok {
		t.Fatalf("host-b silent for an hour must be dropped: %v", reply.Peers)
	}
	if _, ok := reply.Fingerprints["host-b"]; ok {
		t.Fatalf("a forgotten host's fingerprint must go with it: %v", reply.Fingerprints)
	}

	// ...and comes straight back when it polls again.
	b.Batch(testTTL, "fp-b", ops)
	reply = a.Batch(testTTL, "fp-a", ops)
	if sil, ok := reply.Peers["host-b"]; !ok || sil != 0 {
		t.Fatalf("host-b after polling again: got %v (present %v) want 0", sil, ok)
	}
}

// SentAt is the anchor every believed deadline is measured from, so it must
// be read before the first byte goes out: latency can only ever shorten a
// lease we think we hold, never extend it.
func TestSentAtIsCapturedBeforeAnyIO(t *testing.T) {
	f := newFakeDDB()
	clk := clock.NewFake()
	cd := &countingDDB{inner: f, before: func() { clk.Advance(1) }}
	c := newTestClient(cd, "host-a", "inst-a", clk)

	clk.Advance(100)
	reply := c.Batch(testTTL, "fp-a", map[string]arbiter.Op{"web": opRenew})

	if reply.SentAt != 100 {
		t.Fatalf("SentAt: got %v want 100 (the reading before any call)", reply.SentAt)
	}
	if clk.Mono() <= 100 {
		t.Fatalf("the test's own premise failed: the clock did not advance during I/O")
	}
}
