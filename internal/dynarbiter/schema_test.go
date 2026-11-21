package dynarbiter

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDecodeFleetRoundTrip(t *testing.T) {
	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "fleet"},
		"svc": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"web": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"h":  &types.AttributeValueMemberS{Value: "host-a"},
				"i":  &types.AttributeValueMemberS{Value: "inst-1"},
				"pi": &types.AttributeValueMemberS{Value: "inst-0"},
				"e":  &types.AttributeValueMemberN{Value: "3"},
				"v":  &types.AttributeValueMemberN{Value: "7"},
				"s":  &types.AttributeValueMemberBOOL{Value: true},
				"eb": &types.AttributeValueMemberS{Value: "host-b"},
				"bh": &types.AttributeValueMemberBOOL{Value: true},
				"bt": &types.AttributeValueMemberN{Value: "300.5"},
				"bv": &types.AttributeValueMemberN{Value: "2"},
			}},
			// A key that has never been claimed: every attr absent must
			// decode as that field's zero value, not an error.
			"idle": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
		}},
		"host": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"host-a": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"i": &types.AttributeValueMemberS{Value: "inst-1"},
				"f": &types.AttributeValueMemberS{Value: "fp-123"},
				"b": &types.AttributeValueMemberN{Value: "42"},
			}},
		}},
	}

	svcs, hosts, err := decodeFleet(item)
	if err != nil {
		t.Fatalf("decodeFleet: %v", err)
	}

	wantWeb := entry{
		Holder: "host-a", Instance: "inst-1", PrevInstance: "inst-0",
		Epoch: 3, Version: 7, Serving: true,
		EvacuatedBy: "host-b", BarHold: true, BarTTL: 300.5, BarVersion: 2,
	}
	if got := svcs["web"]; got != wantWeb {
		t.Fatalf("web entry: want %+v, got %+v", wantWeb, got)
	}
	if got := svcs["idle"]; got != (entry{}) {
		t.Fatalf("absent attrs must decode as zero values: got %+v", got)
	}

	wantHost := hostEntry{Instance: "inst-1", Fingerprint: "fp-123", Beat: 42}
	if got := hosts["host-a"]; got != wantHost {
		t.Fatalf("host-a: want %+v, got %+v", wantHost, got)
	}
}

func TestDecodeFleetMissingSvcAndHostIsEmpty(t *testing.T) {
	svcs, hosts, err := decodeFleet(map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "fleet"},
	})
	if err != nil {
		t.Fatalf("decodeFleet: %v", err)
	}
	if len(svcs) != 0 {
		t.Fatalf("want no services, got %v", svcs)
	}
	if len(hosts) != 0 {
		t.Fatalf("want no hosts, got %v", hosts)
	}
}

func TestDecodeFleetWrongTopLevelTypeErrors(t *testing.T) {
	_, _, err := decodeFleet(map[string]types.AttributeValue{
		"svc": &types.AttributeValueMemberS{Value: "not-a-map"},
	})
	if err == nil {
		t.Fatal("want error when svc attribute is not a map")
	}
}

func TestDecodeFleetWrongEntryTypeErrors(t *testing.T) {
	_, _, err := decodeFleet(map[string]types.AttributeValue{
		"svc": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"web": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"v": &types.AttributeValueMemberS{Value: "not-a-number"},
			}},
		}},
	})
	if err == nil {
		t.Fatal("want error when a scalar attr has the wrong wire type")
	}
}
