// This file holds the fleet item's wire schema: the fixed attribute names
// (plan Task 3 — do not rename or add) and the conversion between the
// item's map[string]types.AttributeValue and the entry/hostEntry shapes
// observe.go already defines. Names are deliberately terse (h, i, pi, e, v,
// s, eb, bh, bt, bv / i, f, b) because every one of them is billed on every
// poll's item size (design spec §5) — this is the one place in the
// codebase where an abbreviation is the right call, not a shortcut.
package dynarbiter

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Fixed wire attribute names.
const (
	attrPK   = "pk"
	attrSvc  = "svc"
	attrHost = "host"

	svcHolder      = "h"
	svcInstance    = "i"
	svcPrevInst    = "pi"
	svcEpoch       = "e"
	svcVersion     = "v"
	svcServing     = "s"
	svcEvacuatedBy = "eb"
	svcBarHold     = "bh"
	svcBarTTL      = "bt"
	svcBarVersion  = "bv"

	hostInstance    = "i"
	hostFingerprint = "f"
	hostBeat        = "b"
)

// decodeFleet reads the fleet item's svc and host maps into the in-memory
// shapes the observer consumes. A svc/host attribute absent from the item
// entirely decodes as an empty map — a brand-new table has neither yet.
// Within an entry, an absent attribute decodes as that field's zero value
// (design: "Absent attrs = zero values"; a released lease REMOVEs h/i
// rather than writing empties, and decoding must treat that the same as a
// key nobody has ever claimed). A PRESENT attribute of the wrong wire type
// is not covered by that rule — it is a decode error, since that can only
// mean the schema itself has drifted.
func decodeFleet(item map[string]types.AttributeValue) (map[string]entry, map[string]hostEntry, error) {
	svcs := map[string]entry{}
	if raw, ok := item[attrSvc]; ok {
		m, ok := raw.(*types.AttributeValueMemberM)
		if !ok {
			return nil, nil, fmt.Errorf("dynarbiter: %q attribute is not a map", attrSvc)
		}
		for name, rawEntry := range m.Value {
			em, ok := rawEntry.(*types.AttributeValueMemberM)
			if !ok {
				return nil, nil, fmt.Errorf("dynarbiter: svc[%q] is not a map", name)
			}
			e, err := decodeEntry(em.Value)
			if err != nil {
				return nil, nil, fmt.Errorf("dynarbiter: svc[%q]: %w", name, err)
			}
			svcs[name] = e
		}
	}

	hosts := map[string]hostEntry{}
	if raw, ok := item[attrHost]; ok {
		m, ok := raw.(*types.AttributeValueMemberM)
		if !ok {
			return nil, nil, fmt.Errorf("dynarbiter: %q attribute is not a map", attrHost)
		}
		for name, rawEntry := range m.Value {
			hm, ok := rawEntry.(*types.AttributeValueMemberM)
			if !ok {
				return nil, nil, fmt.Errorf("dynarbiter: host[%q] is not a map", name)
			}
			h, err := decodeHostEntry(hm.Value)
			if err != nil {
				return nil, nil, fmt.Errorf("dynarbiter: host[%q]: %w", name, err)
			}
			hosts[name] = h
		}
	}

	return svcs, hosts, nil
}

func decodeEntry(m map[string]types.AttributeValue) (entry, error) {
	var e entry
	var err error
	if e.Holder, err = decodeString(m, svcHolder); err != nil {
		return entry{}, err
	}
	if e.Instance, err = decodeString(m, svcInstance); err != nil {
		return entry{}, err
	}
	if e.PrevInstance, err = decodeString(m, svcPrevInst); err != nil {
		return entry{}, err
	}
	if e.Epoch, err = decodeInt(m, svcEpoch); err != nil {
		return entry{}, err
	}
	if e.Version, err = decodeInt(m, svcVersion); err != nil {
		return entry{}, err
	}
	if e.Serving, err = decodeBool(m, svcServing); err != nil {
		return entry{}, err
	}
	if e.EvacuatedBy, err = decodeString(m, svcEvacuatedBy); err != nil {
		return entry{}, err
	}
	if e.BarHold, err = decodeBool(m, svcBarHold); err != nil {
		return entry{}, err
	}
	if e.BarTTL, err = decodeFloat(m, svcBarTTL); err != nil {
		return entry{}, err
	}
	if e.BarVersion, err = decodeInt(m, svcBarVersion); err != nil {
		return entry{}, err
	}
	// Released has no wire attribute of its own — a caller who cares
	// derives it from Holder being empty. Left at its zero value here.
	return e, nil
}

func decodeHostEntry(m map[string]types.AttributeValue) (hostEntry, error) {
	var h hostEntry
	var err error
	if h.Instance, err = decodeString(m, hostInstance); err != nil {
		return hostEntry{}, err
	}
	if h.Fingerprint, err = decodeString(m, hostFingerprint); err != nil {
		return hostEntry{}, err
	}
	if h.Beat, err = decodeInt(m, hostBeat); err != nil {
		return hostEntry{}, err
	}
	return h, nil
}

func decodeString(m map[string]types.AttributeValue, key string) (string, error) {
	raw, ok := m[key]
	if !ok {
		return "", nil
	}
	s, ok := raw.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("attribute %q is not a string", key)
	}
	return s.Value, nil
}

func decodeBool(m map[string]types.AttributeValue, key string) (bool, error) {
	raw, ok := m[key]
	if !ok {
		return false, nil
	}
	b, ok := raw.(*types.AttributeValueMemberBOOL)
	if !ok {
		return false, fmt.Errorf("attribute %q is not a bool", key)
	}
	return b.Value, nil
}

func decodeInt(m map[string]types.AttributeValue, key string) (int64, error) {
	raw, ok := m[key]
	if !ok {
		return 0, nil
	}
	n, ok := raw.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attribute %q is not a number", key)
	}
	v, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("attribute %q: %w", key, err)
	}
	return v, nil
}

func decodeFloat(m map[string]types.AttributeValue, key string) (float64, error) {
	raw, ok := m[key]
	if !ok {
		return 0, nil
	}
	n, ok := raw.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attribute %q is not a number", key)
	}
	v, err := strconv.ParseFloat(n.Value, 64)
	if err != nil {
		return 0, fmt.Errorf("attribute %q: %w", key, err)
	}
	return v, nil
}

// avS, avN, avF and avBool build the leaf AttributeValues the expression
// builders need for ExpressionAttributeValues. Kept here, next to the
// decode side, so every wire-type decision for this schema lives in one
// file.
func avS(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func avN(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func avF(f float64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(f, 'f', -1, 64)}
}

func avBool(b bool) types.AttributeValue { return &types.AttributeValueMemberBOOL{Value: b} }

// avEntryMap builds a brand-new svc.<key> value from scratch: h, i, s, e, v
// only — pi/eb/bh/bt/bv are simply absent, exactly as decodeEntry treats a
// never-claimed key. Used the one place a claim creates svc.<key> instead
// of updating it: DynamoDB requires every ancestor of a SET target except
// the final path segment to already exist (verified against DynamoDB
// Local — SET svc.#k.h fails with "document path ... invalid for update"
// when svc.#k itself is absent, even though the grandparent svc map
// exists), so a never-seen key can only be written as one whole map value,
// never field-by-field.
func avEntryMap(holder, instance string, serving bool, epoch, version int64) types.AttributeValue {
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		svcHolder:   avS(holder),
		svcInstance: avS(instance),
		svcServing:  avBool(serving),
		svcEpoch:    avN(epoch),
		svcVersion:  avN(version),
	}}
}

// avHostEntryMap builds a brand-new host.<name> value from scratch, the
// same whole-map trick avEntryMap uses and for the same reason: the seed
// write that creates a host's presence row cannot set i/f/b field by field
// while host.<name> itself is still absent. Beat starts at 0 so the very
// first renew's `if_not_exists(host.#me.b, :zero) + :one` lands on 1 either
// way — whether it finds the seeded 0 or an entry an older process left.
func avHostEntryMap(instance, fingerprint string) types.AttributeValue {
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		hostInstance:    avS(instance),
		hostFingerprint: avS(fingerprint),
		hostBeat:        avN(0),
	}}
}

// avEmptyMap is the value both halves of the fleet seed default to.
func avEmptyMap() types.AttributeValue {
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}
}

// keyFleet is the primary key of the one hot item every builder writes.
func keyFleet() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: avS("fleet")}
}
