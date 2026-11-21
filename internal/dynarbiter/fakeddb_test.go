// fakeDDB is an in-process stand-in for a DynamoDB table, used only by
// tests (this file's _test.go suffix keeps it out of the production
// binary — go build never compiles it in). It is NOT a general DynamoDB
// emulator: it understands exactly the UpdateExpression/ConditionExpression
// grammar expr.go's builders emit —
//
//	SET path = :value
//	SET path = path + :value
//	SET path = if_not_exists(path, :zero) + :value
//	SET path = if_not_exists(path, :default)   (Task 6's two seed writes)
//	SET path = :value  (whole-map-entry — same shape as the plain form)
//	REMOVE path[, path...]
//	condition: AND-list of `path = :value` and `attribute_not_exists(path)`,
//	           optionally grouped in parens (buildRenew's per-service ANDs)
//
// and panics on anything outside that grammar, on purpose: if a future
// change to expr.go's builders drifts from what this fake understands, the
// panic is the signal, not a silently-wrong test result.
//
// Two behaviors are load-bearing because real DynamoDB enforces them (Task
// 3's report has the exact strings, reproduced against DynamoDB Local):
//
//  1. Document-path ancestor rule: a SET whose target's ancestors (every
//     path segment except the last) don't already exist as a map fails
//     with a ValidationException-shaped error — never a
//     ConditionalCheckFailedException, so Task 6's client can tell "no
//     such key yet" apart from "lost the CAS".
//  2. A request whose ExpressionAttributeValues carries an entry no clause
//     in UpdateExpression/ConditionExpression references is rejected,
//     also as a ValidationException-shaped error.
//  3. Overlapping target paths — one clause's path being a prefix of (or
//     equal to) another's, e.g. `SET host = ..., host.#me = ...` — are
//     rejected the same way. This is the ONLY reason expr.go's seed is two
//     writes instead of one, so a fake that quietly accepted the collapsed
//     form would greenlight a change that fails every seed in production,
//     which by the client's own rules is an unreachable arbiter for the
//     life of the process.
package dynarbiter

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
)

// fakeDDB implements the DDB interface a later task defines (the same two
// methods *dynamodb.Client exposes) in-process, with real conditional-write
// semantics for exactly the grammar above.
type fakeDDB struct {
	mu    sync.Mutex
	items map[string]map[string]types.AttributeValue

	// failure injection
	errNext   error // returned once by the next call, then cleared
	errAlways error // returned by every call while set
	// staleReads makes GetItem serve a snapshot this many writes behind
	// current — it takes effect regardless of ConsistentRead, unlike real
	// DynamoDB where ConsistentRead=true always reads current: a CAS
	// client that sets ConsistentRead=true against this fake will still
	// see stale data if staleReads is set. That divergence is deliberate
	// (this knob exists to test how callers cope with a stale observation
	// racing a write, not to model DynamoDB's consistency modes), but a
	// caller of this fake must know it, not discover it.
	staleReads int
	// onUpdate runs inside f.mu on every UpdateItem call, before the
	// condition is evaluated — a hook for tests to inject timing (e.g.
	// block until other goroutines have queued on the lock) around a
	// write. It runs WITH THE LOCK HELD: it must never call back into f
	// (GetItem/UpdateItem) or it deadlocks.
	onUpdate func()

	// versions is whole-table snapshots taken after each successful
	// UpdateItem, oldest first, backing staleReads. Not part of the
	// brief's literal field list, but the only way to serve GetItem from
	// "N versions old" while UpdateItem still CASes against the live
	// items map.
	versions []map[string]map[string]types.AttributeValue
}

func newFakeDDB() *fakeDDB {
	return &fakeDDB{items: map[string]map[string]types.AttributeValue{}}
}

// nextErr returns the injected error for this call, if any, clearing
// errNext after one use. Must be called with mu held.
func (f *fakeDDB) nextErr() error {
	if f.errAlways != nil {
		return f.errAlways
	}
	if f.errNext != nil {
		err := f.errNext
		f.errNext = nil
		return err
	}
	return nil
}

func (f *fakeDDB) GetItem(_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.nextErr(); err != nil {
		return nil, err
	}

	pk := pkOf(params.Key)

	src := f.items
	if f.staleReads > 0 && len(f.versions) > 0 {
		idx := len(f.versions) - 1 - f.staleReads
		if idx < 0 {
			idx = 0
		}
		src = f.versions[idx]
	}

	item, ok := src[pk]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: deepCopyItem(item)}, nil
}

func (f *fakeDDB) UpdateItem(_ context.Context, params *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.nextErr(); err != nil {
		return nil, err
	}
	if f.onUpdate != nil {
		f.onUpdate()
	}

	if params.UpdateExpression == nil {
		panic("fakeDDB: UpdateItemInput missing UpdateExpression")
	}
	// None of expr.go's builders ever ask for anything back (ReturnValues
	// is always NONE — see expr.go's file comment), and this fake never
	// implements the ALL_NEW/ALL_OLD/UPDATED_* projections. Silently
	// returning an empty Attributes map for one of those would pass a test
	// against data that was never actually computed, so panic instead:
	// this is exactly the kind of drift the grammar panics are also for.
	if params.ReturnValues != "" && params.ReturnValues != types.ReturnValueNone {
		panic(fmt.Sprintf("fakeDDB: ReturnValues %q is not implemented (only NONE is)", params.ReturnValues))
	}
	if err := validateUnusedValues(params); err != nil {
		return nil, err
	}
	sets, removes := parseUpdateExpression(*params.UpdateExpression)
	if err := validateOverlappingPaths(sets, removes, params.ExpressionAttributeNames); err != nil {
		return nil, err
	}

	pk := pkOf(params.Key)
	current := f.items[pk]

	// NOTE (deliberate, unverified against real DynamoDB): the condition
	// check runs before the document-path ancestor check below, so a
	// request that both loses its CAS AND targets a missing parent path
	// surfaces as ConditionalCheckFailedException, never ValidationException.
	// Every case this fake's own tests exercise only ever hits one failure
	// mode at a time, so this ordering is untested against the real
	// service (Task 3's DynamoDB Local rig would be needed to confirm it).
	// Callers relying on which error wins when BOTH conditions are true at
	// once should verify against DynamoDB Local rather than trust this.
	if params.ConditionExpression != nil {
		ok, err := evalCondition(*params.ConditionExpression, current, params.ExpressionAttributeNames, params.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
		}
	}

	// versions[0] is a snapshot of the table exactly as it stood before
	// this call — the seeded/bootstrap state on the very first write ever
	// made. Without it, staleReads=1 after a single write would have
	// nothing older than "current" to fall back to and would silently
	// serve current instead of complaining or genuinely going back a
	// version.
	if len(f.versions) == 0 {
		f.versions = append(f.versions, deepCopyItemsTable(f.items))
	}

	newItem := deepCopyItem(current)
	if newItem == nil {
		newItem = map[string]types.AttributeValue{}
	}
	for k, v := range params.Key {
		newItem[k] = deepCopyAttr(v)
	}

	for _, sc := range sets {
		if err := applySet(newItem, sc, params.ExpressionAttributeNames, params.ExpressionAttributeValues); err != nil {
			return nil, err
		}
	}
	for _, path := range removes {
		applyRemove(newItem, path, params.ExpressionAttributeNames)
	}

	f.items[pk] = newItem
	f.versions = append(f.versions, deepCopyItemsTable(f.items))

	return &dynamodb.UpdateItemOutput{}, nil
}

// ---- key / value helpers -------------------------------------------------

func pkOf(key map[string]types.AttributeValue) string {
	v, ok := key[attrPK]
	if !ok {
		panic("fakeDDB: key missing pk attribute")
	}
	s, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		panic("fakeDDB: pk attribute is not a string")
	}
	return s.Value
}

func deepCopyAttr(v types.AttributeValue) types.AttributeValue {
	switch t := v.(type) {
	case *types.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: t.Value}
	case *types.AttributeValueMemberN:
		return &types.AttributeValueMemberN{Value: t.Value}
	case *types.AttributeValueMemberBOOL:
		return &types.AttributeValueMemberBOOL{Value: t.Value}
	case *types.AttributeValueMemberNULL:
		return &types.AttributeValueMemberNULL{Value: t.Value}
	case *types.AttributeValueMemberM:
		m := make(map[string]types.AttributeValue, len(t.Value))
		for k, vv := range t.Value {
			m[k] = deepCopyAttr(vv)
		}
		return &types.AttributeValueMemberM{Value: m}
	default:
		panic(fmt.Sprintf("fakeDDB: unsupported AttributeValue type %T (grammar drift)", v))
	}
}

func deepCopyItem(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	if item == nil {
		return nil
	}
	out := make(map[string]types.AttributeValue, len(item))
	for k, v := range item {
		out[k] = deepCopyAttr(v)
	}
	return out
}

func deepCopyItemsTable(items map[string]map[string]types.AttributeValue) map[string]map[string]types.AttributeValue {
	out := make(map[string]map[string]types.AttributeValue, len(items))
	for pk, item := range items {
		out[pk] = deepCopyItem(item)
	}
	return out
}

func attrEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)
		return ok && av.Value == bv.Value
	default:
		panic(fmt.Sprintf("fakeDDB: unsupported AttributeValue comparison type %T (grammar drift)", a))
	}
}

func numericValue(v types.AttributeValue) (int64, error) {
	n, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0, newValidationErr("An operand in the update expression has an incorrect data type")
	}
	i, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, newValidationErr("An operand in the update expression has an incorrect data type")
	}
	return i, nil
}

func newValidationErr(msg string) error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: msg}
}

// ---- path resolution -------------------------------------------------

// resolvePath turns a dotted expression path like "svc.#k0.v" into real
// attribute-name segments, resolving every #-placeholder through names.
func resolvePath(path string, names map[string]string) []string {
	segs := strings.Split(path, ".")
	out := make([]string, len(segs))
	for i, s := range segs {
		if strings.HasPrefix(s, "#") {
			real, ok := names[s]
			if !ok {
				panic(fmt.Sprintf("fakeDDB: %q missing from ExpressionAttributeNames", s))
			}
			out[i] = real
		} else {
			out[i] = s
		}
	}
	return out
}

// getAtPath reads segs out of item without requiring ancestors to exist —
// a missing ancestor just means "not found", the right answer for both
// condition evaluation and if_not_exists's existence check.
func getAtPath(item map[string]types.AttributeValue, segs []string) (types.AttributeValue, bool) {
	cur := item
	for i, s := range segs {
		v, ok := cur[s]
		if !ok {
			return nil, false
		}
		if i == len(segs)-1 {
			return v, true
		}
		m, ok := v.(*types.AttributeValueMemberM)
		if !ok {
			return nil, false
		}
		cur = m.Value
	}
	return nil, false
}

// navigateAncestors returns the map that must directly hold segs' last
// element, enforcing the real-DynamoDB rule that every ancestor except the
// final segment must already exist as a map (Task 3 report: a SET whose
// parent map is absent fails with a ValidationException, not silently
// creating intermediate maps).
func navigateAncestors(item map[string]types.AttributeValue, segs []string) (map[string]types.AttributeValue, error) {
	cur := item
	for _, s := range segs[:len(segs)-1] {
		v, ok := cur[s]
		if !ok {
			return nil, newValidationErr("The document path provided in the update expression is invalid for update")
		}
		m, ok := v.(*types.AttributeValueMemberM)
		if !ok {
			return nil, newValidationErr("The document path provided in the update expression is invalid for update")
		}
		cur = m.Value
	}
	return cur, nil
}

// ---- expression tokenizing -------------------------------------------------

// splitTopLevel splits s on sep, ignoring any sep that falls inside
// parentheses (needed for if_not_exists(a, :z)'s internal comma and for
// buildRenew's parenthesized per-service AND groups).
func splitTopLevel(s, sep string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i+len(sep) <= len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			i += len(sep) - 1
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func parenWraps(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// ---- UpdateExpression parsing -------------------------------------------------

type setKind int

const (
	setPlain setKind = iota
	setIncrement
	setIfNotExists
	// setDefault is `path = if_not_exists(path, :value)` with no arithmetic
	// suffix: keep what is there, otherwise write the default. Unlike
	// setIfNotExists it is not numeric — the seed writes default whole maps —
	// so it never goes near numericValue.
	setDefault
)

type setClause struct {
	path  string
	kind  setKind
	value string // placeholder assigned (plain) or added (increment/ifNotExists)
	zero  string // if_not_exists's default-value placeholder; only for setIfNotExists
}

func splitSetRemove(expr string) (setPart, removePart string) {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.Contains(expr, " REMOVE "):
		idx := strings.Index(expr, " REMOVE ")
		setPart = strings.TrimSpace(expr[:idx])
		removePart = strings.TrimSpace(expr[idx+len(" REMOVE "):])
	case strings.HasPrefix(expr, "REMOVE "):
		removePart = strings.TrimSpace(expr[len("REMOVE "):])
	default:
		setPart = expr
	}
	setPart = strings.TrimSpace(strings.TrimPrefix(setPart, "SET "))
	return setPart, removePart
}

func parseUpdateExpression(expr string) ([]setClause, []string) {
	setPart, removePart := splitSetRemove(expr)

	var sets []setClause
	if setPart != "" {
		for _, c := range splitTopLevel(setPart, ", ") {
			sets = append(sets, parseSetClause(strings.TrimSpace(c)))
		}
	}

	var removes []string
	if removePart != "" {
		for _, p := range splitTopLevel(removePart, ", ") {
			removes = append(removes, strings.TrimSpace(p))
		}
	}
	return sets, removes
}

func parseSetClause(clause string) setClause {
	eq := strings.Index(clause, " = ")
	if eq < 0 {
		panic("fakeDDB: unsupported SET clause (no top-level '='): " + clause)
	}
	path := clause[:eq]
	rhs := clause[eq+len(" = "):]

	switch {
	case strings.HasPrefix(rhs, ":"):
		return setClause{path: path, kind: setPlain, value: rhs}

	case strings.HasPrefix(rhs, "if_not_exists("):
		closeIdx := strings.Index(rhs, ")")
		if closeIdx < 0 {
			panic("fakeDDB: unterminated if_not_exists(...) in SET clause: " + clause)
		}
		inner := rhs[len("if_not_exists("):closeIdx]
		innerParts := strings.SplitN(inner, ", ", 2)
		if len(innerParts) != 2 || innerParts[0] != path {
			panic("fakeDDB: unsupported if_not_exists(...) shape (path must match LHS): " + clause)
		}
		rest := strings.TrimSpace(rhs[closeIdx+1:])
		if rest == "" {
			return setClause{path: path, kind: setDefault, value: innerParts[1]}
		}
		val := strings.TrimPrefix(rest, "+ ")
		if val == rest || !strings.HasPrefix(val, ":") {
			panic("fakeDDB: unsupported if_not_exists(...) suffix (want '+ :value'): " + clause)
		}
		return setClause{path: path, kind: setIfNotExists, zero: innerParts[1], value: val}

	case strings.Contains(rhs, " + :"):
		parts := strings.SplitN(rhs, " + ", 2)
		if parts[0] != path || !strings.HasPrefix(parts[1], ":") {
			panic("fakeDDB: unsupported increment shape (want '<path> + :value'): " + clause)
		}
		return setClause{path: path, kind: setIncrement, value: parts[1]}

	default:
		panic("fakeDDB: unsupported SET rhs (grammar drift): " + rhs)
	}
}

func applySet(item map[string]types.AttributeValue, sc setClause, names map[string]string, values map[string]types.AttributeValue) error {
	segs := resolvePath(sc.path, names)
	parent, err := navigateAncestors(item, segs)
	if err != nil {
		return err
	}
	leaf := segs[len(segs)-1]

	switch sc.kind {
	case setPlain:
		v, ok := values[sc.value]
		if !ok {
			panic("fakeDDB: SET references unknown value placeholder " + sc.value)
		}
		parent[leaf] = deepCopyAttr(v)

	case setDefault:
		if _, ok := parent[leaf]; ok {
			return nil // already there: keep it, byte for byte
		}
		v, ok := values[sc.value]
		if !ok {
			panic("fakeDDB: if_not_exists references unknown default placeholder " + sc.value)
		}
		parent[leaf] = deepCopyAttr(v)

	case setIncrement:
		cur, ok := parent[leaf]
		if !ok {
			return newValidationErr(fmt.Sprintf("The provided expression refers to an attribute that does not exist in the item: %s", sc.path))
		}
		base, err := numericValue(cur)
		if err != nil {
			return err
		}
		delta, ok := values[sc.value]
		if !ok {
			panic("fakeDDB: SET increment references unknown value placeholder " + sc.value)
		}
		deltaN, err := numericValue(delta)
		if err != nil {
			return err
		}
		parent[leaf] = avN(base + deltaN)

	case setIfNotExists:
		var base int64
		if cur, ok := parent[leaf]; ok {
			base, err = numericValue(cur)
			if err != nil {
				return err
			}
		} else {
			z, ok := values[sc.zero]
			if !ok {
				panic("fakeDDB: if_not_exists references unknown zero placeholder " + sc.zero)
			}
			base, err = numericValue(z)
			if err != nil {
				return err
			}
		}
		delta, ok := values[sc.value]
		if !ok {
			panic("fakeDDB: SET if_not_exists references unknown value placeholder " + sc.value)
		}
		deltaN, err := numericValue(delta)
		if err != nil {
			return err
		}
		parent[leaf] = avN(base + deltaN)
	}
	return nil
}

func applyRemove(item map[string]types.AttributeValue, path string, names map[string]string) {
	segs := resolvePath(path, names)
	cur := item
	for _, s := range segs[:len(segs)-1] {
		v, ok := cur[s]
		if !ok {
			return // ancestor absent: nothing to remove, same as real DynamoDB
		}
		m, ok := v.(*types.AttributeValueMemberM)
		if !ok {
			return
		}
		cur = m.Value
	}
	delete(cur, segs[len(segs)-1])
}

// ---- ConditionExpression evaluation -------------------------------------------------

// evalCondition evaluates expr against item atomically: the caller holds
// f.mu for the whole read-then-write, so this always sees CURRENT state,
// never a state another goroutine's UpdateItem could still be mutating.
func evalCondition(expr string, item map[string]types.AttributeValue, names map[string]string, values map[string]types.AttributeValue) (bool, error) {
	for _, term := range flattenAndTerms(expr) {
		ok, err := evalTerm(term, item, names, values)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func flattenAndTerms(expr string) []string {
	var terms []string
	for _, tok := range splitTopLevel(expr, " AND ") {
		tok = strings.TrimSpace(tok)
		if parenWraps(tok) {
			terms = append(terms, flattenAndTerms(tok[1:len(tok)-1])...)
		} else {
			terms = append(terms, tok)
		}
	}
	return terms
}

func evalTerm(term string, item map[string]types.AttributeValue, names map[string]string, values map[string]types.AttributeValue) (bool, error) {
	if strings.HasPrefix(term, "attribute_not_exists(") && strings.HasSuffix(term, ")") {
		path := term[len("attribute_not_exists(") : len(term)-1]
		_, exists := getAtPath(item, resolvePath(path, names))
		return !exists, nil
	}
	if idx := strings.Index(term, " = "); idx >= 0 {
		path := term[:idx]
		placeholder := term[idx+len(" = "):]
		val, exists := getAtPath(item, resolvePath(path, names))
		if !exists {
			return false, nil
		}
		want, ok := values[placeholder]
		if !ok {
			panic("fakeDDB: condition references unknown value placeholder " + placeholder)
		}
		return attrEqual(val, want), nil
	}
	panic("fakeDDB: unsupported condition term (grammar drift): " + term)
}

// ---- unused ExpressionAttributeValues rejection -------------------------------------------------

var placeholderRe = regexp.MustCompile(`:[A-Za-z0-9_]+`)

// validateUnusedValues reproduces the real-DynamoDB rejection of a request
// whose ExpressionAttributeValues carries an entry no clause references
// (Task 3 report: "Value provided in ExpressionAttributeValues unused in
// expressions: keys: {:host}", reproduced against DynamoDB Local).
func validateUnusedValues(params *dynamodb.UpdateItemInput) error {
	used := map[string]bool{}
	if params.UpdateExpression != nil {
		for _, tok := range placeholderRe.FindAllString(*params.UpdateExpression, -1) {
			used[tok] = true
		}
	}
	if params.ConditionExpression != nil {
		for _, tok := range placeholderRe.FindAllString(*params.ConditionExpression, -1) {
			used[tok] = true
		}
	}

	var unused []string
	for k := range params.ExpressionAttributeValues {
		if !used[k] {
			unused = append(unused, k)
		}
	}
	if len(unused) == 0 {
		return nil
	}
	sort.Strings(unused)
	return newValidationErr(fmt.Sprintf("Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}", strings.Join(unused, ", ")))
}

// ---- overlapping target paths -------------------------------------------------

// validateOverlappingPaths reproduces DynamoDB's refusal to update two paths
// where one contains the other ("Invalid UpdateExpression: Two document
// paths overlap with each other"). Comparison is on RESOLVED segments, so
// #me and the host name it stands for are the same path.
func validateOverlappingPaths(sets []setClause, removes []string, names map[string]string) error {
	var paths [][]string
	for _, sc := range sets {
		paths = append(paths, resolvePath(sc.path, names))
	}
	for _, p := range removes {
		paths = append(paths, resolvePath(p, names))
	}
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if pathsOverlap(paths[i], paths[j]) {
				return newValidationErr("Invalid UpdateExpression: Two document paths overlap with each other; must remove or rewrite one of these paths")
			}
		}
	}
	return nil
}

// pathsOverlap is true when either path is a prefix of the other, equality
// included — DynamoDB rejects both.
func pathsOverlap(a, b []string) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// =====================================================================
// Tests
// =====================================================================

// newBootstrappedFakeDDB seeds an empty fleet item (svc:{}, host:{}) the
// way deploy/dynamo/bootstrap.sh's one-time PutItem must (Task 3 report):
// SET svc.#k = :entry still needs the top-level svc map itself to already
// exist, so a truly empty fake reproduces real DynamoDB's rejection of the
// very first-ever claim. Tests that aren't specifically exercising that
// bootstrap gap start from here instead.
func newBootstrappedFakeDDB() *fakeDDB {
	f := newFakeDDB()
	f.items["fleet"] = map[string]types.AttributeValue{
		attrPK:   avS("fleet"),
		attrSvc:  &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
		attrHost: &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
	}
	return f
}

// seedHostForTest writes host.<name> = {} directly, mimicking the one-time
// per-host write Task 3's report says buildRenew's own client needs before
// a host's first-ever renew: host.#me.b = if_not_exists(host.#me.b, :zero)
// + :one still requires host.#me itself to already exist as a map.
func seedHostForTest(t *testing.T, f *fakeDDB, host string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	hostsAttr := f.items["fleet"][attrHost].(*types.AttributeValueMemberM)
	hostsAttr.Value[host] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}
}

func TestFakeConditionalSuccessMutates(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	got := f.GetFleetForTest(t)
	svcs, _, err := decodeFleet(got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	e, ok := svcs["web"]
	if !ok {
		t.Fatalf("web entry missing after successful claim: %v", svcs)
	}
	if e.Holder != "host-a" || e.Version != 1 || e.Epoch != 1 {
		t.Fatalf("web entry: got %+v", e)
	}

	// Now a CAS'd takeover, observed correctly: must succeed and mutate.
	observed := e
	if _, err := f.UpdateItem(ctx, buildTakeover("conch", "web", observed, hostSpec{Host: "host-a", Instance: "inst-2"}, true)); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	svcs, _, _ = decodeFleet(f.GetFleetForTest(t))
	if svcs["web"].Instance != "inst-2" || svcs["web"].Version != 2 {
		t.Fatalf("takeover did not mutate as expected: %+v", svcs["web"])
	}
}

func TestFakeConditionalFailureMutatesNothing(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	before := deepCopyItem(f.GetFleetForTest(t))

	// Stale observed version: the CAS must fail and leave state untouched.
	stale := entry{Epoch: 1, Version: 99}
	_, err := f.UpdateItem(ctx, buildClaim("conch", "web", stale, hostSpec{Host: "host-b", Instance: "inst-9"}, true, false))
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		t.Fatalf("want ConditionalCheckFailedException, got %v (%T)", err, err)
	}

	after := f.GetFleetForTest(t)
	if !itemsEqualForTest(before, after) {
		t.Fatalf("failed conditional write mutated state:\n before %#v\n after  %#v", before, after)
	}
}

func TestFakeIncrementAddsOne(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	svcs, _, _ := decodeFleet(f.GetFleetForTest(t))
	claimed := svcs["web"]

	// buildRenew's host-beat bump needs host.#me itself to already exist
	// (Task 3 report's documented gap: if_not_exists only defaults the LEAF,
	// not its parent map) — seed it the way the real client's bootstrap
	// write would on this host's first-ever poll.
	seedHostForTest(t, f, "host-a")

	batch := []renewSpec{{Key: "web", Epoch: claimed.Epoch, Serving: true}}
	if _, err := f.UpdateItem(ctx, buildRenew("conch", batch, me)); err != nil {
		t.Fatalf("renew: %v", err)
	}

	svcs, hosts, _ := decodeFleet(f.GetFleetForTest(t))
	if svcs["web"].Version != claimed.Version+1 {
		t.Fatalf("v = v + :one: got %d want %d", svcs["web"].Version, claimed.Version+1)
	}
	// host.#me.b = if_not_exists(host.#me.b, :zero) + :one on a never-seen
	// host must land on 1, exercising the if_not_exists branch.
	if hosts["host-a"].Beat != 1 {
		t.Fatalf("host beat via if_not_exists(...)+1: got %d want 1", hosts["host-a"].Beat)
	}

	// A second renew must increment the beat again (existing-value branch
	// of if_not_exists), not reset it.
	if _, err := f.UpdateItem(ctx, buildRenew("conch", batch, me)); err != nil {
		t.Fatalf("second renew: %v", err)
	}
	_, hosts, _ = decodeFleet(f.GetFleetForTest(t))
	if hosts["host-a"].Beat != 2 {
		t.Fatalf("host beat after second renew: got %d want 2", hosts["host-a"].Beat)
	}
}

func TestFakeRemoveDeletesNestedPaths(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	svcs, _, _ := decodeFleet(f.GetFleetForTest(t))
	claimed := svcs["web"]

	// Evacuating release sets the bar attributes (eb/bh/bt/bv).
	if _, err := f.UpdateItem(ctx, buildRelease("conch", "web", claimed, me, true, true, 60)); err != nil {
		t.Fatalf("evacuate release: %v", err)
	}
	svcs, _, _ = decodeFleet(f.GetFleetForTest(t))
	released := svcs["web"]
	if released.EvacuatedBy != "host-a" || !released.BarHold {
		t.Fatalf("evacuate release did not set bar attrs: %+v", released)
	}

	// A fresh claim with clearBar=true must REMOVE eb/bh/bt/bv along with pi.
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", released, hostSpec{Host: "host-b", Instance: "inst-2"}, true, true)); err != nil {
		t.Fatalf("re-claim clearing bar: %v", err)
	}
	svcs, _, _ = decodeFleet(f.GetFleetForTest(t))
	reclaimed := svcs["web"]
	if reclaimed.EvacuatedBy != "" || reclaimed.BarHold || reclaimed.BarTTL != 0 || reclaimed.BarVersion != 0 {
		t.Fatalf("REMOVE did not clear nested bar attrs: %+v", reclaimed)
	}
	if reclaimed.PrevInstance != "" {
		t.Fatalf("REMOVE did not clear svc.#k.pi: %+v", reclaimed)
	}
}

// TestFakeCASSingleWinner is the brief's headline concurrency test: 8
// goroutines race to claim the same never-seen key with
// attribute_not_exists(svc.#k) as their CAS. Exactly one must succeed.
func TestFakeCASSingleWinner(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	var successes int64
	var failures int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			me := hostSpec{Host: fmt.Sprintf("host-%d", i), Instance: fmt.Sprintf("inst-%d", i)}
			_, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false))
			if err == nil {
				atomic.AddInt64(&successes, 1)
				return
			}
			var ccf *types.ConditionalCheckFailedException
			if !errors.As(err, &ccf) {
				t.Errorf("goroutine %d: unexpected error type %T: %v", i, err, err)
				return
			}
			atomic.AddInt64(&failures, 1)
		}(i)
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes: got %d want 1", successes)
	}
	if failures != n-1 {
		t.Fatalf("failures: got %d want %d", failures, n-1)
	}

	svcs, _, err := decodeFleet(f.GetFleetForTest(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if svcs["web"].Version != 1 {
		t.Fatalf("winner's write: got version %d want 1", svcs["web"].Version)
	}
}

// TestFakeCASSingleWinnerOnExistingKey repeats the race, but on a key that
// already exists and is being fought over with version-CAS (buildClaim's
// other branch), the more common shape once a key has ever been claimed.
func TestFakeCASSingleWinnerOnExistingKey(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	seed := hostSpec{Host: "seed", Instance: "inst-0"}
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, seed, true, false)); err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	svcs, _, _ := decodeFleet(f.GetFleetForTest(t))
	observed := svcs["web"]

	const n = 8
	var wg sync.WaitGroup
	var successes int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			me := hostSpec{Host: fmt.Sprintf("host-%d", i), Instance: fmt.Sprintf("inst-%d", i)}
			_, err := f.UpdateItem(ctx, buildClaim("conch", "web", observed, me, true, false))
			if err == nil {
				atomic.AddInt64(&successes, 1)
			}
		}(i)
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes: got %d want 1", successes)
	}
	svcs, _, _ = decodeFleet(f.GetFleetForTest(t))
	if svcs["web"].Version != observed.Version+1 {
		t.Fatalf("version after race: got %d want %d", svcs["web"].Version, observed.Version+1)
	}
}

// TestFakeStaleReadsOneVersionBackAfterSingleWrite guards the off-by-one a
// naive "index len(versions)-1-staleReads" would hit with a short history:
// after exactly ONE write, staleReads=1 must serve the PRE-write (seeded)
// state, not silently fall back to current because there was nowhere else
// to clamp to.
func TestFakeStaleReadsOneVersionBackAfterSingleWrite(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	preWrite := deepCopyItem(f.GetFleetForTest(t))
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim: %v", err)
	}

	f.staleReads = 1
	got, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if !itemsEqualForTest(preWrite, got.Item) {
		t.Fatalf("staleReads=1 after one write: got %#v want the pre-write seed state %#v", got.Item, preWrite)
	}
}

func TestFakeStaleReadsServeOldSnapshotsWritesUseCurrent(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim: %v", err) // v=1
	}
	seedHostForTest(t, f, "host-a")
	v1, _, _ := decodeFleet(f.GetFleetForTest(t))
	batch := []renewSpec{{Key: "web", Epoch: v1["web"].Epoch}}
	if _, err := f.UpdateItem(ctx, buildRenew("conch", batch, me)); err != nil {
		t.Fatalf("renew 1: %v", err) // v=2
	}
	if _, err := f.UpdateItem(ctx, buildRenew("conch", batch, me)); err != nil {
		t.Fatalf("renew 2: %v", err) // v=3
	}

	f.staleReads = 2
	staleItem, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
	if err != nil {
		t.Fatalf("stale GetItem: %v", err)
	}
	staleSvcs, _, err := decodeFleet(staleItem.Item)
	if err != nil {
		t.Fatalf("decode stale: %v", err)
	}
	if staleSvcs["web"].Version != 1 {
		t.Fatalf("staleReads=2: got version %d want 1", staleSvcs["web"].Version)
	}

	f.staleReads = 0
	currentItem, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
	if err != nil {
		t.Fatalf("current GetItem: %v", err)
	}
	currentSvcs, _, _ := decodeFleet(currentItem.Item)
	if currentSvcs["web"].Version != 3 {
		t.Fatalf("staleReads=0: got version %d want 3", currentSvcs["web"].Version)
	}

	// Writes must still CAS against CURRENT (v=3) regardless of staleReads
	// being set for reads: a claim conditioned on the stale v=1 must fail...
	f.staleReads = 2
	_, err = f.UpdateItem(ctx, buildClaim("conch", "web", entry{Epoch: staleSvcs["web"].Epoch, Version: 1}, hostSpec{Host: "host-b", Instance: "inst-2"}, true, false))
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		t.Fatalf("write against stale observation: want ConditionalCheckFailedException, got %v", err)
	}
	// ...while one conditioned on the true current v=3 must succeed.
	_, err = f.UpdateItem(ctx, buildClaim("conch", "web", entry{Epoch: currentSvcs["web"].Epoch, Version: 3}, hostSpec{Host: "host-b", Instance: "inst-2"}, true, false))
	if err != nil {
		t.Fatalf("write against current observation: %v", err)
	}
}

func TestFakeSetOnMissingAncestorFailsValidationNotConditional(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()

	// No claim has ever happened, so svc.#k does not exist. A per-field SET
	// (the shape buildClaim's never-seen branch deliberately avoids, per
	// its comment and the Task 3 report) must fail as a ValidationException,
	// not a ConditionalCheckFailedException.
	input := &dynamodb.UpdateItemInput{
		TableName:                aws.String("conch"),
		Key:                      keyFleet(),
		UpdateExpression:         aws.String("SET svc.#k.h = :host"),
		ExpressionAttributeNames: map[string]string{"#k": "web"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":host": avS("host-a"),
		},
	}
	_, err := f.UpdateItem(ctx, input)
	if err == nil {
		t.Fatalf("want error, got nil")
	}

	var ccf *types.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
		t.Fatalf("missing ancestor must not surface as ConditionalCheckFailedException: %v", err)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want a smithy API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("ErrorCode: got %q want ValidationException", apiErr.ErrorCode())
	}
}

func TestFakeSetOnMissingAncestorFailsEvenWhenGrandparentExists(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	// Claim "api" so the top-level svc map exists, but "web" still doesn't.
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "api", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim api: %v", err)
	}
	before := deepCopyItem(f.GetFleetForTest(t))

	input := &dynamodb.UpdateItemInput{
		TableName:                aws.String("conch"),
		Key:                      keyFleet(),
		UpdateExpression:         aws.String("SET svc.#k.h = :host"),
		ExpressionAttributeNames: map[string]string{"#k": "web"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":host": avS("host-a"),
		},
	}
	_, err := f.UpdateItem(ctx, input)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("want ValidationException even with svc map present, got %v", err)
	}

	// A ValidationException must be as atomic as a ConditionalCheckFailed:
	// applySet only ever mutates the local newItem copy, so a failure
	// midway through a multi-clause SET must leave the live item untouched.
	after := f.GetFleetForTest(t)
	if !itemsEqualForTest(before, after) {
		t.Fatalf("failed ancestor validation mutated state:\n before %#v\n after  %#v", before, after)
	}
}

// TestFakeClaimOnUnbootstrappedFleetFailsValidation documents the gap Task
// 3's report flags for deploy/dynamo/bootstrap.sh: SET svc.#k = :entry still
// needs the top-level svc map to exist, so even the very first claim of the
// very first service fails without a one-time bootstrap PutItem. The fake
// must reproduce this rather than paper over it, since Task 6's seeding
// logic is tested against exactly this behavior.
func TestFakeClaimOnUnbootstrappedFleetFailsValidation(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	_, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false))
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("want ValidationException on an unbootstrapped fleet item, got %v", err)
	}
}

// TestFakeRenewOnNeverSeenHostFailsValidation documents the second gap
// Task 3's report flags: host.#me.b = if_not_exists(host.#me.b, :zero) +
// :one still requires host.#me itself to already exist, so a host's very
// first renew fails until something writes host.<name> = {} once.
func TestFakeRenewOnNeverSeenHostFailsValidation(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}

	_, err := f.UpdateItem(ctx, buildRenew("conch", nil, me))
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("want ValidationException on a never-seen host's first renew, got %v", err)
	}
}

// TestFakeSeedWritesUnblockConditionedWrites is the other half of
// TestFakeClaimOnUnbootstrappedFleetFailsValidation and
// TestFakeRenewOnNeverSeenHostFailsValidation: the two seed writes are
// exactly what turns those ValidationExceptions into a working first poll,
// which is why Task 6 sends them before any conditioned write.
func TestFakeSeedWritesUnblockConditionedWrites(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}

	if _, err := f.UpdateItem(ctx, buildSeedFleet("conch")); err != nil {
		t.Fatalf("seed fleet: %v", err)
	}
	if _, err := f.UpdateItem(ctx, buildSeedHost("conch", me)); err != nil {
		t.Fatalf("seed host: %v", err)
	}
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim after seeding: %v", err)
	}
	batch := []renewSpec{{Key: "web", Epoch: 1, Serving: true}}
	if _, err := f.UpdateItem(ctx, buildRenew("conch", batch, me)); err != nil {
		t.Fatalf("renew after seeding: %v", err)
	}

	svcs, hosts, err := decodeFleet(f.GetFleetForTest(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if svcs["web"].Version != 2 || hosts["host-a"].Beat != 1 {
		t.Fatalf("after seed+claim+renew: svc %+v host %+v", svcs["web"], hosts["host-a"])
	}
}

// Re-seeding must be a no-op against a fleet item that already has state:
// the seeds run again on every process start, and clobbering a live svc map
// or a peer's host entry would be catastrophic.
func TestFakeSeedWritesAreIdempotent(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}

	for _, in := range []*dynamodb.UpdateItemInput{buildSeedFleet("conch"), buildSeedHost("conch", me)} {
		if _, err := f.UpdateItem(ctx, in); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.UpdateItem(ctx, buildRenew("conch", []renewSpec{{Key: "web", Epoch: 1}}, me)); err != nil {
		t.Fatalf("renew: %v", err)
	}
	before := f.GetFleetForTest(t)

	// A second process (different instance, different fingerprint) seeding
	// the same item must change nothing at all.
	for _, in := range []*dynamodb.UpdateItemInput{
		buildSeedFleet("conch"),
		buildSeedHost("conch", hostSpec{Host: "host-a", Instance: "inst-9", Fingerprint: "fp-9"}),
	} {
		if _, err := f.UpdateItem(ctx, in); err != nil {
			t.Fatalf("re-seed: %v", err)
		}
	}
	if !itemsEqualForTest(before, f.GetFleetForTest(t)) {
		t.Fatalf("re-seeding mutated state:\n before %#v\n after  %#v", before, f.GetFleetForTest(t))
	}
}

// TestFakeRejectsCollapsedSeed pins the reason expr.go sends TWO seed
// writes. Collapsing them into one is the obvious "optimisation", it saves a
// write once per process, and it fails in production every single time:
// `host` is a prefix of `host.#me`. The two-write form must keep passing
// (TestFakeSeedWritesUnblockConditionedWrites), and this must not.
func TestFakeRejectsCollapsedSeed(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()

	collapsed := &dynamodb.UpdateItemInput{
		TableName: aws.String("conch"),
		Key:       keyFleet(),
		UpdateExpression: aws.String("SET svc = if_not_exists(svc, :empty), " +
			"host = if_not_exists(host, :empty), host.#me = if_not_exists(host.#me, :entry)"),
		ExpressionAttributeNames: map[string]string{"#me": "host-a"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":empty": avEmptyMap(),
			":entry": avHostEntryMap("inst-1", "fp-1"),
		},
	}
	_, err := f.UpdateItem(ctx, collapsed)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("overlapping target paths must be rejected, got %v", err)
	}
	if got := f.GetFleetForTest(t); len(got) != 0 {
		t.Fatalf("rejected update mutated state: %v", got)
	}
}

// A REMOVE that sits under a SET of the same key is the other shape of the
// same rule — buildClaim's never-seen branch avoids it by construction (see
// its comment), and this is what would happen if it did not.
func TestFakeRejectsRemoveUnderSetOfSameKey(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()

	input := &dynamodb.UpdateItemInput{
		TableName:                aws.String("conch"),
		Key:                      keyFleet(),
		UpdateExpression:         aws.String("SET svc.#k = :entry REMOVE svc.#k.pi"),
		ExpressionAttributeNames: map[string]string{"#k": "web"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":entry": avEntryMap("host-a", "inst-1", true, 1, 1),
		},
	}
	_, err := f.UpdateItem(ctx, input)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("a REMOVE under a SET of the same path must be rejected, got %v", err)
	}
}

func TestFakeRejectsUnusedExpressionAttributeValues(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()

	input := &dynamodb.UpdateItemInput{
		TableName:                aws.String("conch"),
		Key:                      keyFleet(),
		UpdateExpression:         aws.String("SET host.#me.b = :one"),
		ExpressionAttributeNames: map[string]string{"#me": "host-a"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":  avN(1),
			":host": avS("unused"), // never referenced by the expression
		},
	}
	_, err := f.UpdateItem(ctx, input)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("want ValidationException, got %v (%T)", err, err)
	}
	if !strings.Contains(apiErr.ErrorMessage(), ":host") {
		t.Fatalf("error message should name the unused key: %v", apiErr.ErrorMessage())
	}

	// Must not have mutated state (host.#me.b unset — item wasn't touched).
	if got := f.GetFleetForTest(t); len(got) != 0 {
		t.Fatalf("rejected update mutated state: %v", got)
	}
}

// buildRenew's real empty-batch output must be accepted (it deliberately
// omits :host precisely to satisfy this rule — see expr_test.go).
func TestFakeAcceptsRenewEmptyBatch(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1", Fingerprint: "fp-1"}
	seedHostForTest(t, f, "host-a")

	_, err := f.UpdateItem(ctx, buildRenew("conch", nil, me))
	if err != nil {
		t.Fatalf("empty-batch renew should be accepted: %v", err)
	}
}

func TestFakeErrNextFiresOnceThenClears(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()
	boom := errors.New("boom")
	f.errNext = boom

	_, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
	if !errors.Is(err, boom) {
		t.Fatalf("first call: got %v want boom", err)
	}

	_, err = f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
	if err != nil {
		t.Fatalf("second call should be clean: %v", err)
	}
}

func TestFakeErrAlwaysPersists(t *testing.T) {
	f := newFakeDDB()
	ctx := context.Background()
	boom := errors.New("always boom")
	f.errAlways = boom

	for i := 0; i < 3; i++ {
		_, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("conch"), Key: keyFleet()})
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: got %v want boom", i, err)
		}
	}
}

func TestFakeOnUpdateHookRunsUnderLock(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()
	me := hostSpec{Host: "host-a", Instance: "inst-1"}

	var calls int64
	f.onUpdate = func() { atomic.AddInt64(&calls, 1) }

	if _, err := f.UpdateItem(ctx, buildClaim("conch", "web", entry{}, me, true, false)); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if calls != 1 {
		t.Fatalf("onUpdate calls: got %d want 1", calls)
	}
}

// No current builder ever requests anything but ReturnValues: NONE (see
// expr.go's file comment). If that ever changes, this fake must fail loudly
// rather than silently hand back an empty Attributes map for a projection
// it never computed.
func TestFakeUpdateItemPanicsOnUnsupportedReturnValues(t *testing.T) {
	f := newBootstrappedFakeDDB()
	ctx := context.Background()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("want panic for an unimplemented ReturnValues")
		}
	}()

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String("conch"),
		Key:                       keyFleet(),
		UpdateExpression:          aws.String("SET host.#me.b = :one"),
		ReturnValues:              types.ReturnValueAllNew,
		ExpressionAttributeNames:  map[string]string{"#me": "host-a"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": avN(1)},
	}
	_, _ = f.UpdateItem(ctx, input)
}

// ---- test-only accessors -------------------------------------------------

// GetFleetForTest returns the fake's live "fleet" item for white-box
// assertions. Exists only so tests don't have to reach into f.items
// directly at every call site.
func (f *fakeDDB) GetFleetForTest(t *testing.T) map[string]types.AttributeValue {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	return deepCopyItem(f.items["fleet"])
}

func itemsEqualForTest(a, b map[string]types.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !attrDeepEqualForTest(av, bv) {
			return false
		}
	}
	return true
}

func attrDeepEqualForTest(a, b types.AttributeValue) bool {
	am, aIsMap := a.(*types.AttributeValueMemberM)
	bm, bIsMap := b.(*types.AttributeValueMemberM)
	if aIsMap != bIsMap {
		return false
	}
	if aIsMap {
		return itemsEqualForTest(am.Value, bm.Value)
	}
	return attrEqual(a, b)
}
