package dynarbiter

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestBuildRenewSortsAndBatchesConditions(t *testing.T) {
	batch := []renewSpec{
		{Key: "web", Epoch: 3, Serving: true},
		{Key: "api", Epoch: 5, Serving: false},
	}
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}

	got := buildRenew("conch", batch, me)

	if *got.TableName != "conch" {
		t.Fatalf("table: got %q", *got.TableName)
	}
	if got.ReturnValues != types.ReturnValueNone {
		t.Fatalf("ReturnValues: got %v", got.ReturnValues)
	}
	if s, ok := got.Key["pk"].(*types.AttributeValueMemberS); !ok || s.Value != "fleet" {
		t.Fatalf("Key: got %v", got.Key)
	}

	// "api" sorts before "web" alphabetically, so it must claim #k0/:s0/:e0.
	if got.ExpressionAttributeNames["#k0"] != "api" || got.ExpressionAttributeNames["#k1"] != "web" {
		t.Fatalf("placeholder ordering: got %v", got.ExpressionAttributeNames)
	}
	if got.ExpressionAttributeNames["#me"] != "host-a" {
		t.Fatalf("#me: got %v", got.ExpressionAttributeNames)
	}
	if len(got.ExpressionAttributeNames) != 3 {
		t.Fatalf("want exactly #k0, #k1, #me; got %v", got.ExpressionAttributeNames)
	}

	wantUpdate := "SET svc.#k0.v = svc.#k0.v + :one, svc.#k0.s = :s0, " +
		"svc.#k1.v = svc.#k1.v + :one, svc.#k1.s = :s1, " +
		"host.#me.b = if_not_exists(host.#me.b, :zero) + :one, host.#me.i = :inst, host.#me.f = :fp"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("UpdateExpression:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	wantCond := "(svc.#k0.h = :host AND svc.#k0.e = :e0 AND svc.#k0.i = :inst) AND " +
		"(svc.#k1.h = :host AND svc.#k1.e = :e1 AND svc.#k1.i = :inst)"
	if *got.ConditionExpression != wantCond {
		t.Fatalf("ConditionExpression:\n got  %q\n want %q", *got.ConditionExpression, wantCond)
	}

	wantValues := map[string]types.AttributeValue{
		":one":  &types.AttributeValueMemberN{Value: "1"},
		":zero": &types.AttributeValueMemberN{Value: "0"},
		":host": &types.AttributeValueMemberS{Value: "host-a"},
		":inst": &types.AttributeValueMemberS{Value: "inst-1"},
		":fp":   &types.AttributeValueMemberS{Value: "fp-1"},
		":s0":   &types.AttributeValueMemberBOOL{Value: false}, // api
		":e0":   &types.AttributeValueMemberN{Value: "5"},
		":s1":   &types.AttributeValueMemberBOOL{Value: true}, // web
		":e1":   &types.AttributeValueMemberN{Value: "3"},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}
}

func TestBuildRenewEmptyBatchHasNoCondition(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}
	got := buildRenew("conch", nil, me)

	if got.ConditionExpression != nil {
		t.Fatalf("empty batch: want nil condition, got %q", *got.ConditionExpression)
	}
	want := "SET host.#me.b = if_not_exists(host.#me.b, :zero) + :one, host.#me.i = :inst, host.#me.f = :fp"
	if *got.UpdateExpression != want {
		t.Fatalf("UpdateExpression:\n got  %q\n want %q", *got.UpdateExpression, want)
	}

	// DynamoDB rejects a request carrying an ExpressionAttributeValues
	// entry no clause in the expression references (verified against
	// DynamoDB Local: "Value provided in ExpressionAttributeValues unused
	// in expressions"). :host is only used inside a service condition, so
	// an empty batch must not carry it.
	if _, ok := got.ExpressionAttributeValues[":host"]; ok {
		t.Fatalf("empty batch: :host is unused and must be absent, got %v", got.ExpressionAttributeValues)
	}
}

func TestBuildRenewDoesNotMutateCallerSlice(t *testing.T) {
	batch := []renewSpec{{Key: "web"}, {Key: "api"}}
	buildRenew("conch", batch, hostSpec{})
	if batch[0].Key != "web" || batch[1].Key != "api" {
		t.Fatalf("buildRenew must not sort the caller's slice in place: got %v", batch)
	}
}

// TestBuildClaimNeverSeenWritesWholeEntryMap covers the branch DynamoDB
// forces to differ structurally from a normal claim: svc.#k does not exist
// yet, so the write must create it as one whole map value rather than
// setting individual sub-fields (verified against DynamoDB Local — see
// expr.go's buildClaim comment). No REMOVE clause: a freshly built map
// never has pi/eb/bh/bt/bv to begin with, and a REMOVE on a sub-path of
// the same key SET in this call would collide with it anyway.
func TestBuildClaimNeverSeenWritesWholeEntryMap(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-1"}
	got := buildClaim("conch", "web", entry{}, me, true, false)

	if got.ExpressionAttributeNames["#k"] != "web" {
		t.Fatalf("#k: got %v", got.ExpressionAttributeNames)
	}
	if want := "attribute_not_exists(svc.#k)"; *got.ConditionExpression != want {
		t.Fatalf("Condition: got %q want %q", *got.ConditionExpression, want)
	}

	wantUpdate := "SET svc.#k = :entry"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	wantValues := map[string]types.AttributeValue{
		":entry": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"h": &types.AttributeValueMemberS{Value: "host-a"},
			"i": &types.AttributeValueMemberS{Value: "inst-1"},
			"s": &types.AttributeValueMemberBOOL{Value: true},
			"e": &types.AttributeValueMemberN{Value: "1"},
			"v": &types.AttributeValueMemberN{Value: "1"},
		}},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}
	if got.ReturnValues != types.ReturnValueNone {
		t.Fatalf("ReturnValues: got %v", got.ReturnValues)
	}
}

// clearBar must not change anything about the never-seen branch: there is
// no bar on a key that has never existed, and (per the comment on
// buildClaim) a REMOVE clause literally cannot coexist with the whole-map
// SET in the same expression.
func TestBuildClaimNeverSeenIgnoresClearBar(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-1"}
	withClear := buildClaim("conch", "web", entry{}, me, true, true)
	withoutClear := buildClaim("conch", "web", entry{}, me, true, false)
	if *withClear.UpdateExpression != *withoutClear.UpdateExpression {
		t.Fatalf("clearBar changed the never-seen branch: %q vs %q", *withClear.UpdateExpression, *withoutClear.UpdateExpression)
	}
}

func TestBuildClaimObservedVersionUsesCASAndClearsBar(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-1"}
	observed := entry{Epoch: 3, Version: 7}
	got := buildClaim("conch", "web", observed, me, false, true)

	if want := "svc.#k.v = :obsV"; *got.ConditionExpression != want {
		t.Fatalf("Condition: got %q want %q", *got.ConditionExpression, want)
	}

	wantUpdate := "SET svc.#k.h = :host, svc.#k.i = :inst, svc.#k.s = :serving, " +
		"svc.#k.e = :newEpoch, svc.#k.v = :newV REMOVE svc.#k.pi, svc.#k.eb, svc.#k.bh, svc.#k.bt, svc.#k.bv"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	if v := got.ExpressionAttributeValues[":obsV"].(*types.AttributeValueMemberN).Value; v != "7" {
		t.Fatalf(":obsV: got %v", v)
	}
	if v := got.ExpressionAttributeValues[":newEpoch"].(*types.AttributeValueMemberN).Value; v != "4" {
		t.Fatalf(":newEpoch: got %v", v)
	}
	if v := got.ExpressionAttributeValues[":newV"].(*types.AttributeValueMemberN).Value; v != "8" {
		t.Fatalf(":newV: got %v", v)
	}
}

func TestBuildTakeoverSameHostRestart(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-2"}
	observed := entry{Holder: "host-a", Instance: "inst-1", Epoch: 3, Version: 7}
	got := buildTakeover("conch", "web", observed, me, true)

	wantCond := "svc.#k.h = :host AND svc.#k.i = :obsInstance AND svc.#k.v = :obsV"
	if *got.ConditionExpression != wantCond {
		t.Fatalf("Condition: got %q want %q", *got.ConditionExpression, wantCond)
	}

	wantUpdate := "SET svc.#k.i = :myInstance, svc.#k.pi = :obsInstance, svc.#k.v = :newV, svc.#k.s = :serving"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	wantValues := map[string]types.AttributeValue{
		":host":        &types.AttributeValueMemberS{Value: "host-a"},
		":obsInstance": &types.AttributeValueMemberS{Value: "inst-1"},
		":obsV":        &types.AttributeValueMemberN{Value: "7"},
		":myInstance":  &types.AttributeValueMemberS{Value: "inst-2"},
		":newV":        &types.AttributeValueMemberN{Value: "8"},
		":serving":     &types.AttributeValueMemberBOOL{Value: true},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}

	// Epoch must be left completely alone: same-host restart never bumps
	// the fencing token.
	if _, ok := got.ExpressionAttributeValues[":newEpoch"]; ok {
		t.Fatalf("takeover must not touch epoch: %v", got.ExpressionAttributeValues)
	}
	if strings.Contains(*got.UpdateExpression, "svc.#k.e") {
		t.Fatalf("takeover UpdateExpression must not mention epoch: %q", *got.UpdateExpression)
	}
}

func TestBuildReleasePlainRemovesBarAttrs(t *testing.T) {
	me := hostSpec{Host: "host-a"}
	observed := entry{Holder: "host-a", Version: 7}
	got := buildRelease("conch", "web", observed, me, false, false, 0)

	wantCond := "svc.#k.h = :host AND svc.#k.v = :obsV"
	if *got.ConditionExpression != wantCond {
		t.Fatalf("Condition: got %q want %q", *got.ConditionExpression, wantCond)
	}

	wantUpdate := "SET svc.#k.s = :false, svc.#k.v = :newV " +
		"REMOVE svc.#k.h, svc.#k.i, svc.#k.pi, svc.#k.eb, svc.#k.bh, svc.#k.bt, svc.#k.bv"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	if _, ok := got.ExpressionAttributeValues[":hold"]; ok {
		t.Fatalf("non-evacuate release must not carry :hold")
	}
	if _, ok := got.ExpressionAttributeValues[":barTTL"]; ok {
		t.Fatalf("non-evacuate release must not carry :barTTL")
	}
}

func TestBuildReleaseEvacuateSetsBarAttrs(t *testing.T) {
	me := hostSpec{Host: "host-a"}
	observed := entry{Holder: "host-a", Version: 7}
	got := buildRelease("conch", "web", observed, me, true, true, 300.5)

	wantUpdate := "SET svc.#k.s = :false, svc.#k.v = :newV, " +
		"svc.#k.eb = :host, svc.#k.bh = :hold, svc.#k.bt = :barTTL, svc.#k.bv = :newV " +
		"REMOVE svc.#k.h, svc.#k.i, svc.#k.pi"
	if *got.UpdateExpression != wantUpdate {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, wantUpdate)
	}

	wantValues := map[string]types.AttributeValue{
		":host":   &types.AttributeValueMemberS{Value: "host-a"},
		":obsV":   &types.AttributeValueMemberN{Value: "7"},
		":false":  &types.AttributeValueMemberBOOL{Value: false},
		":newV":   &types.AttributeValueMemberN{Value: "8"},
		":hold":   &types.AttributeValueMemberBOOL{Value: true},
		":barTTL": &types.AttributeValueMemberN{Value: "300.5"},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}
}

// TestServiceNamesNeverAppearRawInExpressions guards the one hard rule
// every builder must obey: a service name is user input (an operator can
// name a service anything up to the config's own limits), so it may never
// be spliced into UpdateExpression/ConditionExpression text — only ever
// carried as the value of an ExpressionAttributeNames placeholder.
func TestServiceNamesNeverAppearRawInExpressions(t *testing.T) {
	const evil = `evil.name[0]`
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp"}

	checks := []*dynamodb.UpdateItemInput{
		buildRenew("conch", []renewSpec{{Key: evil, Epoch: 1}}, me),
		buildClaim("conch", evil, entry{}, me, true, false),
		buildTakeover("conch", evil, entry{Holder: "host-a", Instance: "inst-1", Version: 1}, me, true),
		buildRelease("conch", evil, entry{Holder: "host-a", Version: 1}, me, true, false, 60),
	}
	for i, in := range checks {
		if in.UpdateExpression != nil && strings.Contains(*in.UpdateExpression, evil) {
			t.Fatalf("case %d: raw service name leaked into UpdateExpression: %q", i, *in.UpdateExpression)
		}
		if in.ConditionExpression != nil && strings.Contains(*in.ConditionExpression, evil) {
			t.Fatalf("case %d: raw service name leaked into ConditionExpression: %q", i, *in.ConditionExpression)
		}
		found := false
		for _, v := range in.ExpressionAttributeNames {
			if v == evil {
				found = true
			}
		}
		if !found {
			t.Fatalf("case %d: service name missing from ExpressionAttributeNames: %v", i, in.ExpressionAttributeNames)
		}
	}
}

// The two seed writes (Task 6 step 2) are the only unconditional writes in
// this file: they exist to make every OTHER write's document path valid, so
// a condition on them would be both meaningless and, in the fleet case,
// impossible to express (there is nothing to compare a map against).
func TestBuildSeedFleetDefaultsBothMaps(t *testing.T) {
	got := buildSeedFleet("conch")

	if *got.TableName != "conch" {
		t.Fatalf("table: got %q", *got.TableName)
	}
	if s, ok := got.Key["pk"].(*types.AttributeValueMemberS); !ok || s.Value != "fleet" {
		t.Fatalf("Key: got %v", got.Key)
	}
	if got.ConditionExpression != nil {
		t.Fatalf("seed must be unconditional, got %q", *got.ConditionExpression)
	}
	if got.ReturnValues != types.ReturnValueNone {
		t.Fatalf("ReturnValues: got %v", got.ReturnValues)
	}
	if len(got.ExpressionAttributeNames) != 0 {
		t.Fatalf("seed references no user input, so it needs no names: %v", got.ExpressionAttributeNames)
	}

	want := "SET svc = if_not_exists(svc, :empty), host = if_not_exists(host, :empty)"
	if *got.UpdateExpression != want {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, want)
	}

	wantValues := map[string]types.AttributeValue{
		":empty": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}
}

func TestBuildSeedHostWritesWholeEntryMap(t *testing.T) {
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}
	got := buildSeedHost("conch", me)

	if got.ConditionExpression != nil {
		t.Fatalf("seed must be unconditional, got %q", *got.ConditionExpression)
	}
	if got.ReturnValues != types.ReturnValueNone {
		t.Fatalf("ReturnValues: got %v", got.ReturnValues)
	}
	if got.ExpressionAttributeNames["#me"] != "host-a" || len(got.ExpressionAttributeNames) != 1 {
		t.Fatalf("names: got %v", got.ExpressionAttributeNames)
	}

	want := "SET host.#me = if_not_exists(host.#me, :entry)"
	if *got.UpdateExpression != want {
		t.Fatalf("Update:\n got  %q\n want %q", *got.UpdateExpression, want)
	}

	// Beat 0, not 1: the first renew's if_not_exists(host.#me.b, :zero) + :one
	// must land on 1 whether it finds this seed or an older process's entry.
	wantValues := map[string]types.AttributeValue{
		":entry": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"i": &types.AttributeValueMemberS{Value: "inst-1"},
			"f": &types.AttributeValueMemberS{Value: "fp-1"},
			"b": &types.AttributeValueMemberN{Value: "0"},
		}},
	}
	if !reflect.DeepEqual(got.ExpressionAttributeValues, wantValues) {
		t.Fatalf("Values:\n got  %#v\n want %#v", got.ExpressionAttributeValues, wantValues)
	}
}

// A host name is user input exactly as a service name is (config.Host), so
// it gets the same treatment: placeholder only, never spliced into the
// expression text.
func TestBuildSeedHostNeverSplicesHostName(t *testing.T) {
	const evil = `evil.host[0]`
	got := buildSeedHost("conch", hostSpec{Host: evil, Instance: "inst-1"})
	if strings.Contains(*got.UpdateExpression, evil) {
		t.Fatalf("raw host name leaked into UpdateExpression: %q", *got.UpdateExpression)
	}
	if got.ExpressionAttributeNames["#me"] != evil {
		t.Fatalf("host name missing from ExpressionAttributeNames: %v", got.ExpressionAttributeNames)
	}
}
