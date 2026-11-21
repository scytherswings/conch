// This file proves the client stack (client.go, creds.go, protocol.go,
// expr.go, observe.go, schema.go) against a REAL DynamoDB — DynamoDB Local
// in docker, driven by scripts/dynamo-local-test.sh — rather than the
// in-process fake every other test in this package uses. It exists to catch
// exactly what a fake can't: real conditional-write semantics, real
// document-path rules, and real wall-clock convergence, on the drills the
// unit suite already proves in fake time (contention in
// TestContenderClaimsAfterSilence, bars in TestReleaseWithBarThenPeerClaim,
// duplicate hosts nowhere else at all — the flip-flop detector has never run
// against a real table before this file. That test
// (TestIntegrationDuplicateHostFlipFlop) scripts a fixed serial call order
// rather than two goroutines racing on a timer, for reasons its own doc
// comment explains; it is not a concurrency test).
//
// Every test is guarded by DYNAMO_LOCAL_ENDPOINT: unset (the default for
// `go test ./...`), each Test*Integration* skips immediately, so the
// package's normal test run never needs docker. Set it — the script does —
// and they run for real, on real time: the aggressive knobs below (ttl 1.5s,
// interval 250ms) keep the wall-clock cost small while leaving enough margin
// (assertions bound convergence by ttl+2×interval, never an exact tick) that
// a loaded CI box or a slow docker daemon doesn't turn a correct client into
// a flaky test.
package dynarbiter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/decide"
)

// The brief's aggressive knobs: fast enough that these tests finish in a
// handful of seconds even though they run on the wall clock, not a fake one.
const (
	integTTL      = 1.5
	integInterval = 0.25
)

func seconds(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

// pollUntil calls fn every interval until it returns true or deadline
// passes, and reports which happened. Real-time convergence (a takeover
// waiting out a silence window, a bar's peer-claim aftermath) has no fake
// clock to advance here, so every "wait for X" in this file goes through
// this loop instead of a fixed sleep-then-check.
func pollUntil(deadline time.Time, interval time.Duration, fn func() bool) bool {
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// setupIntegration skips the test unless DYNAMO_LOCAL_ENDPOINT is set, then
// creates a fresh table for it — one test, one table, so two tests' fleet
// items (host names, service keys) can never bleed into each other even
// though every fleet item lives at the same pk. Table creation and teardown
// use CreateTable/DeleteTable directly against the concrete SDK client:
// those two calls exist ONLY here, never in the runtime DDB interface
// (client.go's contract stays GetItem+UpdateItem — Task 7's whole point).
func setupIntegration(t *testing.T) (DDB, string) {
	t.Helper()
	endpoint := os.Getenv("DYNAMO_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMO_LOCAL_ENDPOINT not set; skipping DynamoDB Local integration test (see scripts/dynamo-local-test.sh)")
	}

	ddb := NewDDB("us-west-2", endpoint, "local", "local")
	sdk, ok := ddb.(*dynamodb.Client)
	if !ok {
		t.Fatalf("NewDDB returned %T, want *dynamodb.Client (CreateTable needs the concrete SDK type)", ddb)
	}

	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	table := fmt.Sprintf("conch-it-%s-%d", name, time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sdk.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("CreateTable %s: %v", table, err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	if err := dynamodb.NewTableExistsWaiter(sdk).Wait(waitCtx,
		&dynamodb.DescribeTableInput{TableName: aws.String(table)}, 15*time.Second); err != nil {
		t.Fatalf("waiting for %s to become ACTIVE: %v", table, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = sdk.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	})

	return ddb, table
}

// readRawEntry does its own strongly consistent GetItem + decode, bypassing
// Client entirely — for the one assertion in this file that needs the
// literal wire state ("did anything write here at all") rather than a
// poll's classified opinion of it.
func readRawEntry(t *testing.T, ddb DDB, table, key string) entry {
	t.Helper()
	out, err := ddb.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(table),
		Key:            keyFleet(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	svcs, _, err := decodeFleet(out.Item)
	if err != nil {
		t.Fatalf("decodeFleet: %v", err)
	}
	return svcs[key]
}

// TestIntegrationTakeover is the basic failover drill against a real table:
// A holds two services and renews them steadily; A stops; B, polling
// independently, must claim both once A's silence passes ttl — and the
// epoch must show exactly one takeover, not a claim race or a double-hop.
func TestIntegrationTakeover(t *testing.T) {
	ddb, table := setupIntegration(t)
	clk := clock.NewSystem()

	a := New(ddb, table, "host-a", "inst-a", 5, clk)
	b := New(ddb, table, "host-b", "inst-b", 5, clk)

	keys := []string{"svc1", "svc2"}
	ops := map[string]arbiter.Op{"svc1": opRenew, "svc2": opRenew}

	// A claims and renews steadily for a few cycles — long enough to prove
	// it actually held the lease, not just claimed it once — then stops.
	// Only the LAST poll before stopping is asserted strictly: a single
	// transient blip earlier must not fail a test whose real subject is
	// B's takeover.
	stopA := make(chan struct{})
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		var last arbiter.Reply
		for {
			last = a.Batch(integTTL, "fp-a", ops)
			select {
			case <-stopA:
				for _, key := range keys {
					if v := last.Services[key]; v.Lease != decide.Yours {
						t.Errorf("A no longer held %s at the moment it was stopped: %+v", key, v)
					}
				}
				return
			case <-time.After(seconds(integInterval)):
			}
		}
	}()

	time.Sleep(seconds(integInterval) * 5) // a few renew cycles established
	close(stopA)
	<-doneA
	if t.Failed() {
		t.FailNow() // A never held the lease cleanly; the takeover assertion below would prove nothing
	}

	deadline := time.Now().Add(seconds(integTTL + 2*integInterval))
	var last arbiter.Reply
	took := pollUntil(deadline, seconds(integInterval), func() bool {
		last = b.Batch(integTTL, "fp-b", ops)
		for _, key := range keys {
			if last.Services[key].Lease != decide.Yours {
				return false
			}
		}
		return true
	})
	if !took {
		t.Fatalf("B never took over both keys within ttl+2*interval (%v): last reply %+v",
			integTTL+2*integInterval, last.Services)
	}
	for _, key := range keys {
		v := wantVerdict(t, last, key, decide.Yours)
		if v.Epoch != 2 {
			t.Fatalf("%s: epoch %d after takeover, want 2 (advanced exactly once: A's claim then B's)", key, v.Epoch)
		}
	}
}

// TestIntegrationReleaseWithBarHonored drills evacuation: A releases with a
// bar, immediately reads its own release as Barred, B (unnamed by the bar)
// claims the freed lease right away, and once B goes quiet A can reclaim —
// the same silence-based convergence as a takeover, on the far side of the
// bar B's own claim already cleared.
func TestIntegrationReleaseWithBarHonored(t *testing.T) {
	ddb, table := setupIntegration(t)
	clk := clock.NewSystem()

	a := New(ddb, table, "host-a", "inst-a", 5, clk)
	b := New(ddb, table, "host-b", "inst-b", 5, clk)
	ops := map[string]arbiter.Op{"web": opRenew}

	v := wantVerdict(t, a.Batch(integTTL, "fp-a", ops), "web", decide.Yours)
	if v.Epoch != 1 {
		t.Fatalf("initial claim epoch: %d want 1", v.Epoch)
	}

	release := map[string]arbiter.Op{"web": {Op: "release", Evacuate: true, BarTTL: 1}}
	rv := a.Batch(integTTL, "fp-a", release).Services["web"]
	if rv.Lease != decide.Free || !rv.Released || rv.EvacuatedBy != "host-a" {
		t.Fatalf("release verdict: %+v", rv)
	}

	// A's own immediate renew reads the bar it just set.
	bv := a.Batch(integTTL, "fp-a", ops).Services["web"]
	if bv.Lease != decide.Barred || bv.EvacuatedBy != "host-a" {
		t.Fatalf("A under its own bar: %+v", bv)
	}

	// B is not named by the bar, so it claims the freed lease immediately —
	// no waiting on silence, no waiting on the bar's own clock.
	cv := b.Batch(integTTL, "fp-b", ops).Services["web"]
	if cv.Lease != decide.Yours || cv.Epoch != 2 {
		t.Fatalf("B's claim under the bar: %+v", cv)
	}

	// B goes quiet from here. Note the bar A left behind was already
	// cleared by B's own claim — a peer's claim always clears a non-hold
	// bar (protocol.go's clearsBar), so the numeric BarTTL never actually
	// gated B. What stands between A and reclaiming now is purely B's own
	// silence past ttl, exactly like a plain takeover — this is the
	// scenario's honest mechanics, not a shortcut around the bar.
	deadline := time.Now().Add(seconds(integTTL + 2*integInterval))
	var last arbiter.Verdict
	took := pollUntil(deadline, seconds(integInterval), func() bool {
		last = a.Batch(integTTL, "fp-a", ops).Services["web"]
		return last.Lease == decide.Yours
	})
	if !took {
		t.Fatalf("A never reclaimed web after B went quiet, within ttl+2*interval (%v): last %+v",
			integTTL+2*integInterval, last)
	}
	if last.Epoch != 3 {
		t.Fatalf("reclaim epoch: %d want 3 (A claims at 1, B takes it at 2, A reclaims at 3)", last.Epoch)
	}

	// The "web" scenario above never actually exercises BarTTL's numeric
	// clock: B's claim clears a non-hold bar outright regardless of its TTL
	// (protocol.go's clearsBar returns true whenever !e.BarHold, live bar or
	// not), so B's mere presence makes the number moot. A second key that
	// only A ever touches is what proves the brief's third clause — "after
	// bar expiry A's renew claims" — for real: nobody clears the bar early,
	// so A must wait out its own BarAge reaching BarTTL before its own renew
	// can reclaim.
	appOps := map[string]arbiter.Op{"app": opRenew}
	v = wantVerdict(t, a.Batch(integTTL, "fp-a", appOps), "app", decide.Yours)
	if v.Epoch != 1 {
		t.Fatalf("app initial claim epoch: %d want 1", v.Epoch)
	}

	appRelease := map[string]arbiter.Op{"app": {Op: "release", Evacuate: true, BarTTL: 1}}
	arv := a.Batch(integTTL, "fp-a", appRelease).Services["app"]
	if arv.Lease != decide.Free || !arv.Released || arv.EvacuatedBy != "host-a" {
		t.Fatalf("app release verdict: %+v", arv)
	}

	// A's own polls from here ARE the bar's clock: BarAge is measured from
	// A's first observation of the bar (observe.go's barSeenAt, set on the
	// poll that first sees this exact bar identity), not from the release
	// write itself, so this loop's first iteration is what starts it.
	appDeadline := time.Now().Add(seconds(integTTL + 2*integInterval))
	var lastApp arbiter.Verdict
	sawBarred := false
	appTook := pollUntil(appDeadline, seconds(integInterval), func() bool {
		lastApp = a.Batch(integTTL, "fp-a", appOps).Services["app"]
		if lastApp.Lease == decide.Barred {
			sawBarred = true
		}
		return lastApp.Lease == decide.Yours
	})
	if !sawBarred {
		t.Fatalf("A never observed its own bar on app before reclaiming: last %+v", lastApp)
	}
	if !appTook {
		t.Fatalf("A never reclaimed app once its own BarTTL expired, within ttl+2*interval (%v): last %+v",
			integTTL+2*integInterval, lastApp)
	}
	if lastApp.Epoch != 2 {
		t.Fatalf("app reclaim epoch: %d want 2 (one claim, one bar-expiry reclaim, nobody else involved)", lastApp.Epoch)
	}
}

// TestIntegrationDuplicateHostFlipFlop is the one drill the fake-driven
// suite has never run: two live *Client processes sharing a host name,
// fighting over the same key. Each renew that sees the OTHER instance's name
// on the record takes it right back (same-host takeover, epoch unmoved), and
// each side's own observer, watching only its OWN reads (never its own
// writes — observeInstance folds in what read() sees, and a client never
// reads its own write back until its NEXT poll), must catch an A->B->A
// pattern in that sequence and report Conflict.
//
// This is scripted as a fixed serial call order rather than two goroutines
// racing on a timer, after two real, empirically-confirmed failure modes in
// that form:
//
//  1. Two independent loops on the same nominal interval can lock into a
//     stable alternation where each side's read always lands on exactly the
//     value the OTHER side just settled it to. The table genuinely flips
//     every poll, but each observer only ever sees "still what I last saw"
//     (observeInstance's `if e.Instance == ss.instance { return }`), so the
//     change-detection Conflict depends on never fires. (Jittering the
//     interval papers over this sometimes, not reliably.)
//  2. Once one side detects Conflict it goes silent for one ttl
//     (obs.Conflicted's conflictUntil) — genuinely correct, that pause is
//     the point. But it means whichever side detects first stops
//     contesting, so the other side can only complete ITS OWN pattern off
//     that side's post-pause resume — landing its qualifying read at
//     roughly first-detection-time + ttl, while its own conflict window
//     (opened by whatever it last saw change) closes at roughly the SAME
//     first-detection-time + ttl. The two boundaries coincide almost
//     exactly, so no fixed outer timeout reliably wins that race — it was
//     observed to fail intermittently even at 3*ttl.
//
// The fix is to control the interleaving instead of hoping for it. The
// sequence below (verified step by step against observeInstance) gets BOTH
// sides their A-B-A within a couple hundred milliseconds, all real
// GetItem/UpdateItem calls against DynamoDB Local, no fake clock involved:
//
//  1. c1 claims "web" (table: i1). c1 has no baseline yet (the read that
//     preceded the claim saw no such key).
//  2. c1 renews again, reading its own claim back — this is what gives c1 a
//     baseline of its OWN instance, the thing that lets c1 complete its
//     pattern later purely off ITS OWN takeover writes.
//  3. c2's first-ever read sees i1 (baseline=i1), takes it over (table: i2).
//  4. c2 renews again, reading its own takeover back (i2 != baseline i1) —
//     this primes c2's observer with prevInstance=i1 BEFORE c1 has detected
//     anything, which is what lets c2 complete off c1's very NEXT write
//     instead of needing to outlast a whole extra pause cycle.
//  5. c1 reads i2 (a change from its baseline i1), takes it back (table: i1).
//  6. c2 reads i1 — matches c2's prevInstance from step 4, within ttl of
//     step 4's instanceChangedAt — Conflict. No write, so the table stays i1.
//  7. c1 reads its own step-5 write, still standing — matches c1's
//     prevInstance from step 5, within ttl — Conflict.
func TestIntegrationDuplicateHostFlipFlop(t *testing.T) {
	ddb, table := setupIntegration(t)
	clk := clock.NewSystem()

	c1 := New(ddb, table, "home", "inst-1", 5, clk)
	c2 := New(ddb, table, "home", "inst-2", 5, clk)
	ops := map[string]arbiter.Op{"web": opRenew}

	step := func(who string, c *Client, fp string, want decide.LeaseState) arbiter.Verdict {
		t.Helper()
		v := c.Batch(integTTL, fp, ops).Services["web"]
		if v.Lease != want {
			t.Fatalf("%s: got %v (%+v), want %v", who, v.Lease, v, want)
		}
		return v
	}

	step("1: c1 claims", c1, "fp-1", decide.Yours)
	step("2: c1's own baseline", c1, "fp-1", decide.Yours)
	step("3: c2 takes over", c2, "fp-2", decide.Yours)
	step("4: c2's own baseline", c2, "fp-2", decide.Yours)
	step("5: c1 takes back", c1, "fp-1", decide.Yours)
	step("6: c2 detects", c2, "fp-2", decide.Conflict)
	step("7: c1 detects", c1, "fp-1", decide.Conflict)

	// Both sides have now proven Conflict once. "Neither renews after" is a
	// real, deterministic claim from here: a handful more rounds from each,
	// still comfortably inside both sides' one-ttl conflict windows (these
	// calls take milliseconds, not the ~1.5s ttl), and every verdict must
	// stay Conflict — no write escapes to move the entry.
	before := readRawEntry(t, ddb, table, "web")
	for i := 0; i < 3; i++ {
		step(fmt.Sprintf("post-conflict round %d: c1", i), c1, "fp-1", decide.Conflict)
		step(fmt.Sprintf("post-conflict round %d: c2", i), c2, "fp-2", decide.Conflict)
	}
	after := readRawEntry(t, ddb, table, "web")
	if before.Version != after.Version || before.Instance != after.Instance {
		t.Fatalf("web changed even though both sides stayed in Conflict: %+v -> %+v", before, after)
	}
}
