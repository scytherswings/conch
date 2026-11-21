// This file builds the four UpdateItem shapes the client sends (spec §6):
// batched renew, claim, same-host takeover, and release. Every builder
// returns a ready-to-send *dynamodb.UpdateItemInput; the client (a later
// task) only has to fill in context and call DynamoDB. ReturnValues is
// always NONE — nobody here reads the post-write item back, the next poll's
// GetItem does that — and every write is a plain UpdateItem, never a
// PutItem: the fleet item needs no bootstrap write because UpdateItem
// upserts nested map keys that don't exist yet.
//
// The one rule every builder obeys without exception: a service name (or a
// host name) is user input, so it is NEVER spliced into UpdateExpression or
// ConditionExpression text. It only ever appears as the value side of an
// ExpressionAttributeNames entry (#k, #k0, #k1, ..., #me); the expression
// strings themselves are built entirely from fixed attribute names and
// placeholder tokens.
package dynarbiter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// buildSeedFleet and buildSeedHost are the two writes every conditioned
// write in this file depends on having already landed. DynamoDB's
// document-path rule (see buildClaim's comment) means `SET svc.#k = :entry`
// needs the top-level svc map to exist and `SET host.#me.b = ...` needs
// host.#me itself to exist — and it reports both failures as a
// ValidationException, not a ConditionalCheckFailedException, so a client
// that skipped seeding could not tell "this key has never existed" from
// "we lost the CAS". Worse, buildRenew is one atomic UpdateItem: the host
// beat's missing parent takes every service's version bump down with it.
//
// They are two writes rather than one because DynamoDB rejects an
// UpdateExpression whose target paths overlap, and `host` is a prefix of
// `host.#me`. Both are idempotent by construction (if_not_exists on the
// whole value, no condition at all), so re-running them against a fleet
// item another host seeded years ago is a no-op that cannot clobber
// anything: a live svc map, a peer's host entry, even our own beat all
// survive untouched.
//
// Neither carries a ConditionExpression on purpose. There is nothing to
// CAS against — "make sure this exists" is exactly what if_not_exists says —
// and a condition here would be the one place in this file where a map
// value reached a comparison.
func buildSeedFleet(table string) *dynamodb.UpdateItemInput {
	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET svc = if_not_exists(svc, :empty), host = if_not_exists(host, :empty)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":empty": avEmptyMap()},
		ReturnValues:              types.ReturnValueNone,
	}
}

// buildSeedHost creates this host's presence row if it has none. The
// fingerprint written here is only ever the seed value — every renew
// overwrites host.#me.f unconditionally — so a stale fingerprint cannot
// survive a poll.
func buildSeedHost(table string, me hostSpec) *dynamodb.UpdateItemInput {
	return &dynamodb.UpdateItemInput{
		TableName:                aws.String(table),
		Key:                      keyFleet(),
		UpdateExpression:         aws.String("SET host.#me = if_not_exists(host.#me, :entry)"),
		ExpressionAttributeNames: map[string]string{"#me": me.Host},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":entry": avHostEntryMap(me.Instance, me.Fingerprint),
		},
		ReturnValues: types.ReturnValueNone,
	}
}

// renewSpec is one service this host wants to renew within a single batched
// UpdateItem (spec §6 "Renew (batched)"). There is no Instance field: the CAS
// buildRenew writes checks svc.%s.i against me.Instance, never against
// anything per-service, because classify guarantees e.Instance == me.Instance
// on every actRenew (see classifyRenew's "our own name" branch) — carrying it
// here again would be a value nothing ever reads.
type renewSpec struct {
	Key     string
	Epoch   int64
	Serving bool
}

// hostSpec identifies the calling host to every builder: Host/Instance
// drive the CAS conditions on svc.<key>.h/.i, and (buildRenew only)
// Instance/Fingerprint are written under Host's own key in the host map.
type hostSpec struct {
	Host        string
	Instance    string
	Fingerprint string
}

// buildRenew batches every service this host currently holds into one
// UpdateItem: each service's version bump is conditioned on this host still
// holding it at the epoch and instance last observed (a per-service AND,
// joined with AND across services), while the host's own beat/instance/
// fingerprint SET is unconditional — a duplicate host claiming this same
// name is caught by the observer reading the NEXT poll (Task 2), not
// refused synchronously the way the single-threaded DO refuses it.
//
// Placeholder numbering is assigned after sorting batch by Key, so the same
// batch always produces the same request regardless of map/slice
// iteration order upstream — needed for both testability and for retries
// after a partial ConditionalCheckFailed to behave predictably.
func buildRenew(table string, batch []renewSpec, me hostSpec) *dynamodb.UpdateItemInput {
	sorted := append([]renewSpec(nil), batch...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	names := map[string]string{"#me": me.Host}
	values := map[string]types.AttributeValue{
		":one":  avN(1),
		":zero": avN(0),
		":inst": avS(me.Instance),
		":fp":   avS(me.Fingerprint),
	}

	var setParts, condParts []string
	for idx, spec := range sorted {
		kName := fmt.Sprintf("#k%d", idx)
		sVal := fmt.Sprintf(":s%d", idx)
		eVal := fmt.Sprintf(":e%d", idx)
		names[kName] = spec.Key
		values[":host"] = avS(me.Host) // only referenced when there's at least one service condition
		values[sVal] = avBool(spec.Serving)
		values[eVal] = avN(spec.Epoch)

		setParts = append(setParts,
			fmt.Sprintf("svc.%s.v = svc.%s.v + :one", kName, kName),
			fmt.Sprintf("svc.%s.s = %s", kName, sVal),
		)
		condParts = append(condParts, fmt.Sprintf(
			"(svc.%s.h = :host AND svc.%s.e = %s AND svc.%s.i = :inst)",
			kName, kName, eVal, kName,
		))
	}

	setParts = append(setParts,
		"host.#me.b = if_not_exists(host.#me.b, :zero) + :one",
		"host.#me.i = :inst",
		"host.#me.f = :fp",
	)

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET " + strings.Join(setParts, ", ")),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueNone,
	}
	if len(condParts) > 0 {
		input.ConditionExpression = aws.String(strings.Join(condParts, " AND "))
	}
	return input
}

// buildClaim attempts to grant key to me. The CAS is on the exact version
// last observed (attribute_not_exists for a service nobody has ever
// touched — observed.Version is 0 only in that case, since every claimed
// service leaves the table with v >= 1 forever after). The epoch bump is
// the fencing token: the previous holder's own renewal, still conditioned
// on the old epoch, fails cleanly on its next poll. clearBar drops a bar
// this claim is stepping over (an overriding claim by the barred host, or
// a peer claim against a non-hold bar); a claim that leaves the bar alone
// (e.g. none was present) must not touch eb/bh/bt/bv at all.
//
// The never-seen branch (observed.Version == 0) writes svc.#k as ONE new
// map value instead of field-by-field: DynamoDB requires every ancestor of
// a SET target except its last path segment to already exist, so
// `SET svc.#k.h = ...` fails outright when svc.#k itself is still absent
// (confirmed against DynamoDB Local — this is not the brief's literal
// per-field expression, which cannot work for a first claim; see the
// Task 3 report for the empirical trace). A REMOVE clause in the same
// expression would collide with that whole-map SET (DynamoDB rejects two
// target paths where one is a prefix of the other), which is moot anyway
// since a freshly built map has no pi/eb/bh/bt/bv to remove.
func buildClaim(table, key string, observed entry, me hostSpec, serving, clearBar bool) *dynamodb.UpdateItemInput {
	names := map[string]string{"#k": key}

	if observed.Version == 0 {
		return &dynamodb.UpdateItemInput{
			TableName:                aws.String(table),
			Key:                      keyFleet(),
			UpdateExpression:         aws.String("SET svc.#k = :entry"),
			ConditionExpression:      aws.String("attribute_not_exists(svc.#k)"),
			ExpressionAttributeNames: names,
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":entry": avEntryMap(me.Host, me.Instance, serving, observed.Epoch+1, observed.Version+1),
			},
			ReturnValues: types.ReturnValueNone,
		}
	}

	values := map[string]types.AttributeValue{
		":host":     avS(me.Host),
		":inst":     avS(me.Instance),
		":serving":  avBool(serving),
		":newEpoch": avN(observed.Epoch + 1),
		":newV":     avN(observed.Version + 1),
		":obsV":     avN(observed.Version),
	}

	setParts := []string{
		"svc.#k.h = :host",
		"svc.#k.i = :inst",
		"svc.#k.s = :serving",
		"svc.#k.e = :newEpoch",
		"svc.#k.v = :newV",
	}
	removeParts := []string{"svc.#k.pi"}
	if clearBar {
		removeParts = append(removeParts, "svc.#k.eb", "svc.#k.bh", "svc.#k.bt", "svc.#k.bv")
	}

	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET " + strings.Join(setParts, ", ") + " REMOVE " + strings.Join(removeParts, ", ")),
		ConditionExpression:       aws.String("svc.#k.v = :obsV"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueNone,
	}
}

// buildTakeover re-registers the SAME host's own hold on key after a
// process restart. The CAS re-checks holder, instance AND version because
// the observed state must match exactly what the last poll read, but epoch
// is deliberately left alone: lease.js only bumps epoch when a null holder
// is granted, never when the same host name re-registers under a new
// instance id.
func buildTakeover(table, key string, observed entry, me hostSpec, serving bool) *dynamodb.UpdateItemInput {
	names := map[string]string{"#k": key}
	values := map[string]types.AttributeValue{
		":host":        avS(me.Host),
		":obsInstance": avS(observed.Instance),
		":obsV":        avN(observed.Version),
		":myInstance":  avS(me.Instance),
		":newV":        avN(observed.Version + 1),
		":serving":     avBool(serving),
	}
	setParts := []string{
		"svc.#k.i = :myInstance",
		"svc.#k.pi = :obsInstance",
		"svc.#k.v = :newV",
		"svc.#k.s = :serving",
	}

	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET " + strings.Join(setParts, ", ")),
		ConditionExpression:       aws.String("svc.#k.h = :host AND svc.#k.i = :obsInstance AND svc.#k.v = :obsV"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueNone,
	}
}

// buildRelease gives key up. The CAS is on holder AND the exact version
// last observed — releasing is still a write like any other, so a stale
// caller must not blow away a version it never saw. evacuate leaves a bar
// behind (eb/bh/bt/bv all SET, bv pinned to the same new version the
// release just wrote, so a contender's bar-identity check and this
// release's fencing agree); without evacuate those four attributes are
// REMOVEd outright so a stale bar from a previous evacuation can't survive
// an ordinary release.
func buildRelease(table, key string, observed entry, me hostSpec, evacuate, hold bool, barTTL float64) *dynamodb.UpdateItemInput {
	names := map[string]string{"#k": key}
	values := map[string]types.AttributeValue{
		":host":  avS(me.Host),
		":obsV":  avN(observed.Version),
		":false": avBool(false),
		":newV":  avN(observed.Version + 1),
	}

	setParts := []string{"svc.#k.s = :false", "svc.#k.v = :newV"}
	removeParts := []string{"svc.#k.h", "svc.#k.i", "svc.#k.pi"}
	if evacuate {
		values[":hold"] = avBool(hold)
		values[":barTTL"] = avF(barTTL)
		setParts = append(setParts, "svc.#k.eb = :host", "svc.#k.bh = :hold", "svc.#k.bt = :barTTL", "svc.#k.bv = :newV")
	} else {
		removeParts = append(removeParts, "svc.#k.eb", "svc.#k.bh", "svc.#k.bt", "svc.#k.bv")
	}

	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET " + strings.Join(setParts, ", ") + " REMOVE " + strings.Join(removeParts, ", ")),
		ConditionExpression:       aws.String("svc.#k.h = :host AND svc.#k.v = :obsV"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueNone,
	}
}
