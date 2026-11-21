// This file is the DynamoDB backend's answer to internal/arbiter's HTTP
// client: same Batch signature, same hard rule at the bottom of it — a
// failure of any kind reads as decide.Unreachable for every key the caller
// asked about, built from the request's own op map, never from whatever the
// backend did or didn't say. Everything else in the package exists to make
// one poll's worth of work here possible: observe.go counts silence,
// schema.go decodes the item, protocol.go decides, expr.go writes.
//
// The one structural difference from the HTTP client is that there is no
// server to be the single writer, so the judgement and the write are ours
// and they are separated by a network. That gap is the whole design:
// classify's verdict is what a write WOULD earn, and only a write that
// actually lands earns it. A refused write is never reported as its own
// optimistic verdict — the client re-reads, re-classifies, and reports what
// the table says now (Task 5's load-bearing ruling; see earnedVerdict).
package dynarbiter

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/decide"
)

const (
	// maxRenewChunk caps how many services ride one batched renew. The
	// limit is not DynamoDB's (the item is far from 400KB at this size) but
	// blast radius: every service in a chunk shares one ConditionExpression,
	// so one fenced service costs the whole chunk a round trip.
	maxRenewChunk = 20

	// presenceTTL is lease.js's PRESENCE_TTL_MS: a host silent for longer
	// than this is not a peer any more. The DO forgets such hosts outright;
	// we cannot delete anyone's row from a shared item, so we filter them
	// out of the reply instead, which is the same thing to every caller.
	presenceTTL = 3600.0
)

// DDB is the slice of *dynamodb.Client this package uses — two calls, both
// against one item. Task 7 passes the real client; the tests pass fakeDDB.
type DDB interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Client is one host's view of the fleet item. Its only cross-poll state is
// the observer (silence, bar age, conflict windows — all sighting-derived,
// all meaningless without history) and the seeded flag. There is
// deliberately no record of which leases we hold: every path trusts the
// host name on the record and lets the CAS refuse a stale opinion, exactly
// as protocol.go does (Task 5's ruling — an in-process held flag deadlocks
// restart adoption and blocks the orphan-evidence release).
//
// Batch is not safe for concurrent use by multiple goroutines: it folds
// each poll into the observer. The supervisor calls it from one goroutine
// (supervisor.go's RunOnce), which is the same contract the HTTP client has.
type Client struct {
	ddb      DDB
	table    string
	host     string
	instance string
	timeout  float64
	clk      clock.Clock

	obs    *observer
	seeded bool
}

// The supervisor's arbiterAPI (supervisor.go) is this one method, and it is
// unexported there, so the two backends can only be kept interchangeable by
// stating the shape here. Task 7's wiring fails to compile if this drifts.
var _ interface {
	Batch(ttl float64, fingerprint string, ops map[string]arbiter.Op) arbiter.Reply
} = (*Client)(nil)

func New(ddb DDB, table, host, instance string, timeout float64, clk clock.Clock) *Client {
	return &Client{
		ddb:      ddb,
		table:    table,
		host:     host,
		instance: instance,
		timeout:  timeout,
		clk:      clk,
		obs:      newObserver(),
	}
}

// Batch runs one poll: seed if we never have, read the item, classify every
// op against it, send the writes those classifications call for, and report
// one verdict per requested key. The numbered comments are the plan's own
// numbered algorithm.
func (c *Client) Batch(ttl float64, fingerprint string, ops map[string]arbiter.Op) arbiter.Reply {
	// 1. Before any I/O: everything after this line is latency, and a lease
	// deadline anchored here can only ever come out shorter than the truth.
	sentAt := c.clk.Mono()
	me := hostSpec{Host: c.host, Instance: c.instance, Fingerprint: fingerprint}

	// 2. Seeding, once per process, retried at the top of every poll until
	// it lands. Every conditioned write below depends on it: without
	// host.<me> and the top-level svc map, DynamoDB rejects the renew for a
	// bad document path — a ValidationException, not a condition failure —
	// and because the renew is one atomic UpdateItem that takes every
	// service's version bump down with it.
	if err := c.seed(me); err != nil {
		return unreachableReply(ops, sentAt)
	}

	// 3 + 4. One eventually-consistent read, decoded and folded into the
	// observer. EVERY poll's snapshot goes in, including a poll whose ops
	// are all reads: silence, bar age and the conflict window are all
	// sighting-derived, so a skipped fold is stale state, not neutral state.
	svcs, hosts, err := c.read(false, sentAt, ttl)
	if err != nil {
		return unreachableReply(ops, sentAt)
	}
	observedAt := sentAt

	// 5. Classify in sorted order so a batch is a deterministic function of
	// the poll, not of map iteration.
	acts := make(map[string]action, len(ops))
	verdicts := make(map[string]arbiter.Verdict, len(ops))
	var renews []renewSpec
	var singles []string
	for _, key := range sortedKeys(ops) {
		e, exists := svcs[key]
		act := classify(sentAt, ttl, key, ops[key], e, exists, c.obs, me)
		acts[key] = act
		switch act.Kind {
		case actNone:
			verdicts[key] = act.Verdict
		case actRenew:
			renews = append(renews, renewSpecFor(key, ops[key], e))
		default:
			singles = append(singles, key)
		}
	}

	// 6a. The batched renews. There is always at least one of these, even
	// with nothing to renew: it carries the host beat, which is the only
	// thing that tells peers we are alive (spec §9's evacuation pre-flight
	// asks exactly that, and lease.js records presence on every /batch
	// regardless of what the ops were). An empty batch carries no
	// ConditionExpression, so it cannot be refused.
	var refusedRenew []string
	beatLanded := false
	for _, chunk := range renewChunks(renews) {
		err := c.write(buildRenew(c.table, chunk, me))
		switch {
		case err == nil:
			beatLanded = true
			for _, spec := range chunk {
				verdicts[spec.Key] = acts[spec.Key].Verdict
			}
		case isConditionFailed(err):
			for _, spec := range chunk {
				refusedRenew = append(refusedRenew, spec.Key)
			}
		default:
			return unreachableReply(ops, sentAt) // 8
		}
	}

	// 6b. Claims, takeovers and releases each go alone: their conditions
	// are per-service and their failures must not be shared.
	var refusedSingle []string
	for _, key := range singles {
		in := singleWrite(c.table, key, ops[key], acts[key], svcs[key], me)
		if in == nil {
			// Unreachable in practice (singles only ever holds the three
			// write kinds); fail closed rather than report a verdict for a
			// write nobody built.
			verdicts[key] = arbiter.Verdict{Lease: decide.Unreachable}
			continue
		}
		err := c.write(in)
		switch {
		case err == nil:
			verdicts[key] = acts[key].Verdict
		case isConditionFailed(err):
			refusedSingle = append(refusedSingle, key)
		default:
			return unreachableReply(ops, sentAt) // 8
		}
	}

	// 7. One retry round, and only one. A refused write means the table
	// disagreed with what we read, so the first thing to do is read it
	// again — strongly consistent this time, because we are about to make a
	// decision that a stale replica would get wrong twice in a row.
	if len(refusedRenew)+len(refusedSingle) > 0 {
		now := c.clk.Mono()
		fresh, freshHosts, err := c.read(true, now, ttl)
		if err != nil {
			return unreachableReply(ops, sentAt) // 8
		}
		svcs, hosts, observedAt = fresh, freshHosts, now

		var retry []renewSpec
		for _, key := range refusedRenew {
			e, exists := svcs[key]
			act := classify(now, ttl, key, ops[key], e, exists, c.obs, me)
			acts[key] = act
			if act.Kind == actRenew {
				// Rebuilt from the FRESH entry: retrying against the epoch
				// that just lost would fail identically, on purpose.
				retry = append(retry, renewSpecFor(key, ops[key], e))
				continue
			}
			verdicts[key] = earnedVerdict(act)
		}
		for _, key := range refusedSingle {
			e, exists := svcs[key]
			verdicts[key] = earnedVerdict(classify(now, ttl, key, ops[key], e, exists, c.obs, me))
		}

		// renewChunks always yields one chunk so that a poll with nothing to
		// renew still writes its beat — but here the main pass has usually
		// written it already, and a second empty chunk would be a pure waste
		// of a WCU on every contended poll. The one case that still needs it
		// is a poll whose every renew chunk was refused (the beat rides the
		// same atomic UpdateItem, so it rolled back with them) and whose keys
		// then all reclassified away, leaving nothing to retry.
		retryChunks := renewChunks(retry)
		if len(retry) == 0 && beatLanded {
			retryChunks = nil
		}
		for _, chunk := range retryChunks {
			err := c.write(buildRenew(c.table, chunk, me))
			switch {
			case err == nil:
				for _, spec := range chunk {
					verdicts[spec.Key] = acts[spec.Key].Verdict
				}
			case isConditionFailed(err):
				// Refused twice in one poll. There is no third read: this
				// key simply has no answer we can stand behind right now.
				for _, spec := range chunk {
					verdicts[spec.Key] = arbiter.Verdict{Lease: decide.Unreachable}
				}
			default:
				return unreachableReply(ops, sentAt) // 8
			}
		}
	}

	// 9. Exactly the requested keys, never more, never fewer. The loop is a
	// backstop for a key no branch above answered: silence would be the one
	// failure mode worse than Unreachable, since decide reads a missing
	// verdict as the zero LeaseState.
	for key := range ops {
		if _, ok := verdicts[key]; !ok {
			verdicts[key] = arbiter.Verdict{Lease: decide.Unreachable}
		}
	}

	peers, fingerprints := c.presence(observedAt, hosts)
	return arbiter.Reply{
		Services:     verdicts,
		Peers:        peers,
		Fingerprints: fingerprints,
		SentAt:       sentAt,
	}
}

// earnedVerdict is the rule a refused write leaves behind: a verdict is
// earned by a write that LANDED, so only actNone verdicts — the ones that
// asked for no write at all — are self-certifying. A fresh classification
// that calls for a write we are not going to send this poll (the lease
// moved and then went free again, say) describes a plan, not a reading, and
// reporting it would hand the supervisor a lease nobody granted. The lost
// race we care most about reads as Held, which this passes through
// untouched.
func earnedVerdict(act action) arbiter.Verdict {
	if act.Kind == actNone {
		return act.Verdict
	}
	return arbiter.Verdict{Lease: decide.Unreachable}
}

// seed sends the two writes every conditioned write depends on. Lazy
// because a fleet that has been running for a year needs neither; retried
// on every poll until one poll's pair succeeds, because a client that gave
// up would spend the rest of the process failing every renew with a
// ValidationException it could not act on.
func (c *Client) seed(me hostSpec) error {
	if c.seeded {
		return nil
	}
	if err := c.write(buildSeedFleet(c.table)); err != nil {
		return err
	}
	if err := c.write(buildSeedHost(c.table, me)); err != nil {
		return err
	}
	c.seeded = true
	return nil
}

// read fetches the fleet item, decodes it and folds it into the observer.
// A decode failure is schema drift, not a race: it comes back as an error
// and takes the whole poll down to Unreachable, which is the honest answer
// to "the item does not mean what this code thinks it means".
func (c *Client) read(consistent bool, now, ttl float64) (map[string]entry, map[string]hostEntry, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	in := &dynamodb.GetItemInput{TableName: aws.String(c.table), Key: keyFleet()}
	if consistent {
		in.ConsistentRead = aws.Bool(true)
	}
	out, err := c.ddb.GetItem(ctx, in)
	if err != nil {
		return nil, nil, err
	}
	svcs, hosts, err := decodeFleet(out.Item)
	if err != nil {
		return nil, nil, err
	}
	c.obs.Observe(now, ttl, svcs, hosts)
	return svcs, hosts, nil
}

func (c *Client) write(in *dynamodb.UpdateItemInput) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.ddb.UpdateItem(ctx, in)
	if err != nil && !isConditionFailed(err) {
		// Any write this client sends can fail this way if the fleet item
		// itself is gone out from under us (an operator's stray DeleteItem,
		// say): the document-path ancestors buildRenew/buildClaim/etc. all
		// assume are there no longer exist, so every write becomes a
		// ValidationException, forever, because c.seeded staying true skips
		// seed() on every later poll. Clearing it here — on the single
		// funnel every write in this package passes through — makes the
		// NEXT poll's seed() actually re-run buildSeedFleet/buildSeedHost,
		// which recreates what's missing. Seeding is idempotent
		// (if_not_exists), so clearing this on an unrelated, transient
		// write error just costs one harmless extra pair of seed writes.
		c.seeded = false
	}
	return err
}

// ctx gives every call the same budget, one call at a time — the HTTP
// client's http.Client.Timeout, expressed the way the AWS SDK takes it.
func (c *Client) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(c.timeout*float64(time.Second)))
}

// presence turns the host map into the reply's peer view. Self is never a
// peer, and a host we have not seen advance in an hour is not one either —
// lease.js deletes such hosts outright, which drops them from its peers AND
// its fingerprints, so the same filter applies to both here. HostSilence
// reads -1 for a host this observer has never seen, which is not a small
// silence but an absence, hence the explicit lower bound.
func (c *Client) presence(now float64, hosts map[string]hostEntry) (map[string]float64, map[string]string) {
	peers := make(map[string]float64, len(hosts))
	fingerprints := make(map[string]string, len(hosts))
	for host, he := range hosts {
		if host == c.host {
			continue
		}
		silence := c.obs.HostSilence(now, host)
		if silence < 0 || silence >= presenceTTL {
			continue
		}
		peers[host] = silence
		if he.Fingerprint != "" {
			fingerprints[host] = he.Fingerprint
		}
	}
	return peers, fingerprints
}

// renewSpecFor builds one service's slot in a batched renew from the entry
// that was just observed: the CAS rides on that epoch, so it must be the
// epoch the classification was made against and nothing else.
func renewSpecFor(key string, op arbiter.Op, e entry) renewSpec {
	return renewSpec{Key: key, Epoch: e.Epoch, Serving: op.Serving}
}

// renewChunks splits the renew list into UpdateItem-sized batches. An empty
// list still produces one (empty) chunk: that write is the poll's presence
// beat, and skipping it would make a standby host — which only ever sends
// read ops — invisible to every peer that might need to evacuate to it.
func renewChunks(specs []renewSpec) [][]renewSpec {
	if len(specs) == 0 {
		return [][]renewSpec{nil}
	}
	var chunks [][]renewSpec
	for start := 0; start < len(specs); start += maxRenewChunk {
		end := start + maxRenewChunk
		if end > len(specs) {
			end = len(specs)
		}
		chunks = append(chunks, specs[start:end])
	}
	return chunks
}

// singleWrite builds the one write a non-renew action calls for. nil means
// "no write exists for this action", which only the actNone kinds can
// produce and which the caller treats as fail-closed.
func singleWrite(table, key string, op arbiter.Op, act action, e entry, me hostSpec) *dynamodb.UpdateItemInput {
	switch act.Kind {
	case actClaim:
		return buildClaim(table, key, e, me, op.Serving, act.ClearBar)
	case actTakeover:
		return buildTakeover(table, key, e, me, op.Serving)
	case actRelease:
		return buildRelease(table, key, e, me, op.Evacuate, op.Hold, op.BarTTL)
	default:
		return nil
	}
}

// isConditionFailed is the one error the client is allowed to interpret: a
// lost CAS, which means the table disagrees with what we read and a
// re-read will say what is true now. EVERYTHING else — a throttle, a
// timeout, a 5xx, and above all a ValidationException from a malformed
// request or a missing document path — is not a race and must never be
// retried as one.
func isConditionFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// unreachableReply is the shape every failure takes: one Unreachable per
// REQUESTED key, ranged over ops and never over anything a response said,
// with no peer claims attached. Same contract as internal/arbiter's
// allUnreachable, for the same reason.
func unreachableReply(ops map[string]arbiter.Op, sentAt float64) arbiter.Reply {
	out := make(map[string]arbiter.Verdict, len(ops))
	for key := range ops {
		out[key] = arbiter.Verdict{Lease: decide.Unreachable}
	}
	return arbiter.Reply{Services: out, SentAt: sentAt}
}

func sortedKeys(ops map[string]arbiter.Op) []string {
	keys := make([]string, 0, len(ops))
	for key := range ops {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
