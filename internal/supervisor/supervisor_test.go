package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/decide"
	"github.com/scytherswings/conch/internal/dockerapi"
	"github.com/scytherswings/conch/internal/hooks"
)

// ---------------------------------------------------------------------------
// Fakes. One recording Docker fake serves both this package's `docker`
// interface and the hook runner's, so the tests below drive a REAL
// hooks.Runner over it: "create-hook" in the call log is the actual hook
// container the real runner created, not a stub's idea of one.
// ---------------------------------------------------------------------------

type fakeDocker struct {
	mu       sync.Mutex
	log      []string
	states   map[string]dockerapi.ContainerState
	images   map[string]bool
	daemonUp bool
	healthyN int
	imageN   int

	inspectErr map[string]bool
	// startErr is a container whose Start FAILS — a bad mount, a port already
	// taken, an image that will not run. The container stays exactly as it
	// was, which is what makes the retry-every-check reconcile observable.
	startErr  map[string]bool
	stopStays map[string]bool // Stop returns nil but the container keeps running
	killStays map[string]bool // Kill returns nil but the container keeps running
	// stopTimeouts is the grace each Stop was called with, last call wins —
	// the timeout is part of the contract (a connector drains at its
	// configured stop_timeout, an accessory flushes at 30s), and the call log
	// alone cannot show it.
	stopTimeouts map[string]int
	gate         map[string]chan struct{}
	gateHit      chan string
	waitGate     map[string]chan struct{}
	waitHit      chan string
	waitCode     map[string]int
	orphans      []string

	// resolutions is a pure lookup table, deliberately NOT a scan over
	// states: a test registers exactly what ResolveByPrefix should answer
	// for a given prefix, the same way the real daemon's selection logic
	// (running preferred, then newest) is dockerapi's own tested behavior,
	// not this package's. An unregistered prefix resolves to ("", 0, nil) —
	// no match, the same "absence is a fact" contract dockerapi.Client uses.
	resolutions    map[string]string
	resolveRunning map[string]int // defaults set by setResolution; setResolutionRunning overrides
	resolveErr     map[string]error

	// labels is what ListWithLabel answers from: container name -> its whole
	// label map, registered by setLabeled/setHookLabeled. Empty by default,
	// so a test that says nothing about labels sees no sweep.
	labels map[string]map[string]string

	// createdArgv is the Argv Create was actually called with, per container
	// name — the one field of dockerapi.RunSpec the call log (a plain string)
	// cannot show, and what the {container}-in-hook-argv tests need to see:
	// not just that a hook container was created, but what command line it
	// was created with.
	createdArgv map[string][]string
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		states:         map[string]dockerapi.ContainerState{},
		images:         map[string]bool{},
		daemonUp:       true,
		inspectErr:     map[string]bool{},
		startErr:       map[string]bool{},
		stopStays:      map[string]bool{},
		killStays:      map[string]bool{},
		stopTimeouts:   map[string]int{},
		gate:           map[string]chan struct{}{},
		gateHit:        make(chan string, 8),
		waitGate:       map[string]chan struct{}{},
		waitHit:        make(chan string, 8),
		waitCode:       map[string]int{},
		resolutions:    map[string]string{},
		resolveRunning: map[string]int{},
		resolveErr:     map[string]error{},
		labels:         map[string]map[string]string{},
		createdArgv:    map[string][]string{},
	}
}

// setResolution registers what ResolveByPrefix must answer for prefix, and
// the ordinary 1-running/0-running default that goes with it: a name means
// one live match, "" means none. Not calling this for a prefix leaves it
// resolving to ("", 0, nil) — no match.
func (d *fakeDocker) setResolution(prefix, name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolutions[prefix] = name
	if name != "" {
		d.resolveRunning[prefix] = 1
	} else {
		d.resolveRunning[prefix] = 0
	}
}

// setResolutionRunning overrides how many RUNNING containers match prefix,
// independent of setResolution's 1-or-0 default — for tests simulating the
// blue/green overlap window, where more than one live match exists even
// though only one is ever selected. Exited previous versions are invisible
// here by construction, exactly as dockerapi.ResolveByPrefix reports them.
func (d *fakeDocker) setResolutionRunning(prefix string, n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolveRunning[prefix] = n
}

// setResolutionErr makes ResolveByPrefix(prefix) fail as a transport error
// would — distinct from an absent match, which is ("", 0, nil).
func (d *fakeDocker) setResolutionErr(prefix string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolveErr[prefix] = err
}

func (d *fakeDocker) ResolveByPrefix(prefix string) (string, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("resolve " + prefix)
	if err := d.resolveErr[prefix]; err != nil {
		return "", 0, err
	}
	return d.resolutions[prefix], d.resolveRunning[prefix], nil
}

func (d *fakeDocker) record(line string) {
	d.log = append(d.log, line) // callers hold d.mu
}

func (d *fakeDocker) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.log...)
}

// mutations is every call that changes the world, which is what "the executor
// did nothing" assertions are really about — the loop's own inspects are
// reads and happen in dry-run too.
func (d *fakeDocker) mutations() []string {
	var out []string
	for _, c := range d.calls() {
		switch strings.Fields(c)[0] {
		case "start", "stop", "kill", "create", "remove":
			out = append(out, c)
		}
	}
	return out
}

// setStartErr makes Start(name) fail until it is turned off again.
func (d *fakeDocker) setStartErr(name string, fails bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.startErr[name] = fails
}

func (d *fakeDocker) setRunning(name string, exists, running bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.states[name] = dockerapi.ContainerState{Exists: exists, Running: running}
}

// setImage sets what Inspect reports as the container's configured image ref,
// keeping whatever exists/running state is already recorded.
func (d *fakeDocker) setImage(name, image string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.states[name]
	st.Image = image
	d.states[name] = st
}

func (d *fakeDocker) running(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.states[name].Running
}

func (d *fakeDocker) stopTimeout(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopTimeouts[name]
}

func (d *fakeDocker) Inspect(name string) (dockerapi.ContainerState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("inspect " + name)
	if d.inspectErr[name] {
		return dockerapi.ContainerState{}, errors.New("inspect " + name + ": daemon says no")
	}
	return d.states[name], nil
}

// Start models the daemon's 304 the way dockerapi maps it: started is false
// when the container was ALREADY running, which is success and not failure.
// Without that, every "does conch narrate a start that never happened" test
// would be vacuous — the fake would report a transition the daemon never
// performed.
func (d *fakeDocker) Start(name string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("start " + name)
	if d.startErr[name] {
		return false, errors.New("start " + name + ": daemon says no")
	}
	st := d.states[name]
	already := st.Running
	st.Exists, st.Running = true, true
	d.states[name] = st
	return !already, nil
}

func (d *fakeDocker) Stop(name string, timeoutSeconds int) error {
	d.mu.Lock()
	d.record("stop " + name)
	d.stopTimeouts[name] = timeoutSeconds
	gate, stays := d.gate[name], d.stopStays[name]
	d.mu.Unlock()

	if gate != nil {
		select {
		case d.gateHit <- name:
		default:
		}
		<-gate
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if !stays {
		st := d.states[name]
		st.Running = false
		d.states[name] = st
	}
	return nil
}

func (d *fakeDocker) Kill(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("kill " + name)
	if !d.killStays[name] {
		st := d.states[name]
		st.Running = false
		d.states[name] = st
	}
	return nil
}

func (d *fakeDocker) ImagePresent(ref string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.imageN++
	return d.images[ref]
}

func (d *fakeDocker) Healthy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.healthyN++
	return d.daemonUp
}

// --- the hook runner's half of the interface ---

func (d *fakeDocker) Create(spec dockerapi.RunSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("create " + spec.Name)
	d.states[spec.Name] = dockerapi.ContainerState{Exists: true}
	d.createdArgv[spec.Name] = append([]string(nil), spec.Argv...)
	return nil
}

// argvOf is the Argv the most recent Create(name) call actually carried —
// nil if Create was never called for name.
func (d *fakeDocker) argvOf(name string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.createdArgv[name]
}

func (d *fakeDocker) Wait(name string, timeoutSeconds float64) (int, error) {
	d.mu.Lock()
	d.record("wait " + name)
	gate := d.waitGate[name]
	d.mu.Unlock()

	if gate != nil {
		select {
		case d.waitHit <- name:
		default:
		}
		<-gate
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.waitCode[name], nil
}

func (d *fakeDocker) Logs(name string, tail int) string { return "" }

func (d *fakeDocker) Remove(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("remove " + name)
	delete(d.states, name)
	return nil
}

// setLabeled registers a container carrying conch.service=value, as a deploy
// template's `labels:` block would. Its running state is not stored here: the
// listing below reads it from d.states, the same place every other reading of
// this fake comes from, so a container the sweep stops reports itself stopped
// on the next check without a test having to say so twice.
func (d *fakeDocker) setLabeled(name, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.labels[name] = map[string]string{"conch.service": value}
}

// setHookLabeled registers a container carrying BOTH labels, which is exactly
// what hooks.Run stamps on every hook container it creates — the case the
// sweep has to exclude or it would kill a restore mid-flight.
func (d *fakeDocker) setHookLabeled(name, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.labels[name] = map[string]string{"conch.service": value, "conch.hook": "true"}
}

// ListWithLabel answers only for containers a test registered with
// setLabeled/setHookLabeled — nothing is labeled by default, so every test
// written before the sweep existed sees an empty list and no sweep at all.
func (d *fakeDocker) ListWithLabel(key string) []dockerapi.LabeledContainer {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("list-with-label " + key)
	names := make([]string, 0, len(d.labels))
	for name := range d.labels {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]dockerapi.LabeledContainer, 0, len(names))
	for _, name := range names {
		labels := d.labels[name]
		if _, ok := labels[key]; !ok {
			continue
		}
		out = append(out, dockerapi.LabeledContainer{
			Name:    name,
			Value:   labels[key],
			Running: d.states[name].Running,
			Labels:  labels,
		})
	}
	return out
}

func (d *fakeDocker) ListByLabel(key, value string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("list " + key + "=" + value)
	return append([]string(nil), d.orphans...)
}

// fakeArbiter answers each batch from a script keyed by check number, and
// records every request so a test can assert on the exact ops sent.
type fakeArbiter struct {
	mu           sync.Mutex
	clk          clock.Clock
	n            int
	requests     []map[string]arbiter.Op
	reply        func(check int, key string, op arbiter.Op) arbiter.Verdict
	fingerprints map[string]string
	peers        map[string]float64
	onBatch      func(check int)
}

func (a *fakeArbiter) Batch(ttl float64, fingerprint string, ops map[string]arbiter.Op) arbiter.Reply {
	a.mu.Lock()
	check := a.n
	a.n++
	snapshot := make(map[string]arbiter.Op, len(ops))
	for k, v := range ops {
		snapshot[k] = v
	}
	a.requests = append(a.requests, snapshot)
	reply := a.reply
	onBatch := a.onBatch
	peers := a.peers
	fingerprints := a.fingerprints
	a.mu.Unlock()

	services := make(map[string]arbiter.Verdict, len(ops))
	for key, op := range ops {
		if reply == nil {
			services[key] = arbiter.Verdict{Lease: decide.Free}
			continue
		}
		services[key] = reply(check, key, op)
	}
	if onBatch != nil {
		onBatch(check)
	}
	return arbiter.Reply{
		Services:     services,
		Peers:        peers,
		Fingerprints: fingerprints,
		SentAt:       a.clk.Mono(),
	}
}

// setPeers is the presence data spec §9's pre-flight reads. It is a setter
// rather than a field poke because evacuation tests set it while the loop may
// be reading it, and the whole suite is -race clean.
func (a *fakeArbiter) setPeers(peers map[string]float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.peers = peers
}

func (a *fakeArbiter) opAt(check int, key string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if check >= len(a.requests) {
		return "<no such batch>"
	}
	return a.requests[check][key].Op
}

func (a *fakeArbiter) ops(key string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, r := range a.requests {
		out = append(out, r[key].Op)
	}
	return out
}

func (a *fakeArbiter) countOps(key, op string) int {
	n := 0
	for _, o := range a.ops(key) {
		if o == op {
			n++
		}
	}
	return n
}

func (a *fakeArbiter) opStructAt(check int, key string) arbiter.Op {
	a.mu.Lock()
	defer a.mu.Unlock()
	if check >= len(a.requests) {
		return arbiter.Op{}
	}
	return a.requests[check][key]
}

// nBatches is how many checks have talked to the arbiter, which is what test
// assertions index against — hardcoded batch numbers would be a second
// implementation of the loop's own arithmetic.
func (a *fakeArbiter) nBatches() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests)
}

type fakeProber struct {
	mu    sync.Mutex
	deny  map[string]bool
	bound map[string]string // url -> the container whose running state gates it
	dk    *fakeDocker
	calls []string
	hosts map[string]string // url -> the host it was last probed with
}

func newFakeProber(dk *fakeDocker) *fakeProber {
	return &fakeProber{deny: map[string]bool{}, bound: map[string]string{}, dk: dk, hosts: map[string]string{}}
}

// bind ties a URL to a container, the way a real endpoint is tied to the
// process serving it: a container that is not running answers nothing. Every
// connector ready_url is bound by default — a fake that answered a ready probe
// for a stopped container is precisely what hid the ready-fold bug, since it
// made "the connector is down" and "the connector cannot register with the
// edge" look like the same reading.
func (p *fakeProber) bind(url, container string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bound[url] = container
}

func (p *fakeProber) Probe(url, host string, timeoutSeconds float64, expect []int) bool {
	p.mu.Lock()
	p.calls = append(p.calls, url)
	p.hosts[url] = host
	denied, container := p.deny[url], p.bound[url]
	p.mu.Unlock()

	if container != "" && !p.dk.running(container) {
		return false
	}
	return !denied
}

// hostFor is the Host this url was last probed with — "" both when the url
// was never probed and when it was probed with no override, which is fine:
// every assertion using this pins a specific call that has already happened.
func (p *fakeProber) hostFor(url string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hosts[url]
}

func (p *fakeProber) setDenied(url string, denied bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deny[url] = denied
}

func (p *fakeProber) countCalls(url string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if c == url {
			n++
		}
	}
	return n
}

// recordingHooks is the real hook runner with its Abort calls recorded — the
// one hook-runner call whose effect is invisible in the docker log when no
// chain happens to be in flight.
type recordingHooks struct {
	*hooks.Runner
	mu     sync.Mutex
	aborts []string
}

func (r *recordingHooks) Abort(service string) {
	r.mu.Lock()
	r.aborts = append(r.aborts, service)
	r.mu.Unlock()
	r.Runner.Abort(service)
}

func (r *recordingHooks) abortCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.aborts)
}

type event struct {
	kind    string
	payload map[string]any
}

type fakeNotifier struct {
	mu     sync.Mutex
	events []event
}

func (n *fakeNotifier) Event(kind string, payload map[string]any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, event{kind: kind, payload: payload})
}

func (n *fakeNotifier) of(kind string) []event {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []event
	for _, e := range n.events {
		if e.kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// logRecord is one captured slog record, flattened to key/value pairs for
// simple assertions. level is carried so a test can pin the RECORD'S LEVEL,
// not just its message and attrs — the RCA this package's transition-logging
// tests guard against is a log call that silently regresses to Debug while
// still matching a message-prefix filter.
type logRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// recordingLogHandler is a minimal slog.Handler that records every record
// passed to it. Mutex-guarded like every other fake in this file: the loop
// goroutine and an executor goroutine can both log at once, and the whole
// suite runs -race.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, logRecord{level: r.Level, msg: r.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h *recordingLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *recordingLogHandler) WithGroup(name string) slog.Handler       { return h }

// of collects every record whose message STARTS WITH prefix. A prefix rather
// than the whole message on purpose: a log line's job is to say what happened
// and where to look next, and the second half of that sentence gets edited as
// the code learns better answers. Pinning a test to the whole sentence makes
// the wording load-bearing; pinning it to the opening clause pins the event.
func (h *recordingLogHandler) of(prefix string) []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []logRecord
	for _, r := range h.records {
		if strings.HasPrefix(r.msg, prefix) {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func testConfig(names ...string) config.Config {
	cfg := config.Config{
		Host:    "host-a",
		Arbiter: config.ArbiterConfig{URL: "https://arbiter.example/batch", OnLoss: "freeze", TimeoutSeconds: 5},
		Timing:  config.TimingConfig{TTL: 60, Interval: 3, DemoteAfter: 3, PromoteAfter: 6},
	}
	for _, name := range names {
		cfg.Services = append(cfg.Services, config.Service{
			Name:      name,
			Container: "app-" + name,
			Connector: config.ConnectorConfig{
				Container:   "cf-" + name,
				ReadyURL:    "http://cf-" + name + ":2000/ready",
				StopTimeout: 35,
			},
			Health: config.HealthConfig{URL: "http://app-" + name + "/up", Expect: []int{200}, TimeoutSeconds: 2},
			Hooks: config.HookSet{Promote: []config.HookSpec{
				{Name: "restore", Image: "restore-" + name + ":1", Timeout: 60},
			}},
		})
	}
	return cfg
}

type harness struct {
	t   *testing.T
	cfg config.Config
	sup *Supervisor
	dk  *fakeDocker
	arb *fakeArbiter
	pr  *fakeProber
	nt  *fakeNotifier
	hk  *recordingHooks
	clk *clock.Fake
}

// newRig builds the fakes but not the supervisor, so a test can arrange the
// world New() itself will read — orphaned hook containers, above all — before
// New() reads it.
func newRig(t *testing.T, cfg config.Config) *harness {
	t.Helper()
	clk := clock.NewFake()
	dk := newFakeDocker()
	arb := &fakeArbiter{clk: clk}
	pr := newFakeProber(dk)
	nt := &fakeNotifier{}

	// Every service starts out as a plausible standby: both containers exist,
	// the service container runs (replication keeps it warm), the connector is
	// stopped, any accessories exist and are stopped, and the hook images are
	// on disk.
	for _, svc := range cfg.Services {
		dk.setRunning(svc.Container, true, true)
		dk.setRunning(svc.Connector.Container, true, false)
		for _, acc := range svc.Accessories {
			dk.setRunning(acc, true, false)
		}
		// Warm accessories start out exactly as the others do — existing and
		// stopped — so the reconcile that brings them up on every host is
		// something a test observes rather than something the rig assumes.
		for _, acc := range svc.WarmAccessories {
			dk.setRunning(acc, true, false)
		}
		if svc.Connector.ReadyURL != "" {
			pr.bind(svc.Connector.ReadyURL, svc.Connector.Container)
		}
		for _, h := range svc.Hooks.Promote {
			dk.images[h.Image] = true
		}
		for _, h := range svc.Hooks.Evacuate {
			dk.images[h.Image] = true
		}
	}

	return &harness{t: t, cfg: cfg, dk: dk, arb: arb, pr: pr, nt: nt,
		// This Runner's own start/finish narration is internal/hooks' own
		// tests to cover (package-local capture there) — discarded here so
		// it never leaks into this package's transition-logging assertions.
		hk: &recordingHooks{Runner: hooks.NewRunner(dk, clk, slog.New(slog.DiscardHandler))}, clk: clk}
}

// build constructs the supervisor over the rig's fakes.
func (h *harness) build(tweak func(*Options)) *harness {
	h.t.Helper()
	o := Options{
		Config:     h.cfg,
		Arbiter:    h.arb,
		Docker:     h.dk,
		Hooks:      h.hk,
		Clock:      h.clk,
		Notify:     h.nt,
		Probe:      h.pr,
		DiskFreeGB: func(string) (float64, error) { return 500, nil },
		Instance:   "i-test",
		Logger:     slog.New(slog.DiscardHandler),
	}
	if tweak != nil {
		tweak(&o)
	}
	sup, err := New(o)
	if err != nil {
		h.t.Fatalf("New: %v", err)
	}
	h.sup = sup
	h.t.Cleanup(sup.Close)
	return h
}

func newHarness(t *testing.T, cfg config.Config, tweak func(*Options)) *harness {
	t.Helper()
	return newRig(t, cfg).build(tweak)
}

// check advances fake time by one interval, runs one check, and waits for
// every executor to go idle — the whole suite is channel/condvar-gated, never
// timed, so there is not one sleep in it.
func (h *harness) check() map[string]decide.Decision {
	h.t.Helper()
	h.clk.Advance(h.cfg.Timing.Interval)
	d := h.sup.RunOnce()
	h.waitIdle()
	return d
}

// gateTimeout bounds every channel wait in this file. Nothing here should ever
// take real time — the suite runs on fake time and condvars — so this exists
// only so that a mutation which stops the event ever happening fails loudly
// instead of hanging until the package timeout.
const gateTimeout = 5 * time.Second

func awaitGate(t *testing.T, ch <-chan string, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(gateTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitIdle is Supervisor.waitIdle with the same bound. It cannot be a select
// (it waits on a condvar), so it runs on its own goroutine; on the failure
// path that goroutine is leaked, which is acceptable in a test that is already
// failing.
func (h *harness) waitIdle() {
	h.t.Helper()
	done := make(chan struct{})
	go func() {
		h.sup.waitIdle()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gateTimeout):
		h.t.Fatal("the executors never went idle")
	}
}

// checkNoWait is for the one test that deliberately blocks an executor: it
// must not wait for the executor it just wedged.
func (h *harness) checkNoWait() map[string]decide.Decision {
	h.t.Helper()
	h.clk.Advance(h.cfg.Timing.Interval)
	return h.sup.RunOnce()
}

func (h *harness) checks(n int) map[string]decide.Decision {
	h.t.Helper()
	var d map[string]decide.Decision
	for i := 0; i < n; i++ {
		d = h.check()
	}
	return d
}

// checkUntilReadyToClaim runs checks until the standby streak is one short of
// promote_after — the state in which the NEXT check's pre-batch pass sends the
// claim. Tests that must arrange the world immediately before a claim use this
// rather than counting checks, because the count depends on where the streak
// happened to start.
func (h *harness) checkUntilReadyToClaim(key string) {
	h.t.Helper()
	for i := 0; i < 50; i++ {
		st := h.sup.Status()[key].(map[string]any)
		if st["phase"] == string(decide.Standby) && st["ok_streak"] == h.cfg.Timing.PromoteAfter-1 {
			return
		}
		h.check()
	}
	h.t.Fatalf("%s never reached a claimable streak: %v", key, h.sup.Status())
}

// driveToActive runs the standard claim-promote-serve sequence under
// freeThenGrant(0) until every service is ACTIVE and has been measured once in
// that state (health on the check that promotes is measured before the
// containers start, so one further check is what makes the snapshot true).
// Tests index later batches off nBatches() rather than hardcoding, so the
// arithmetic lives in one place.
func (h *harness) driveToActive() {
	h.t.Helper()
	if h.arb.reply == nil {
		h.arb.reply = freeThenGrant(0)
	}
	// The claim goes out on the check whose own pre-batch pass completes the
	// streak, so promote_after checks reach PROMOTING; one more finishes the
	// hooks and starts serving; one more measures the running service.
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	for key, entry := range h.sup.Status() {
		if key == statusPeersKey {
			continue // the one top-level key that is not a service (Status's own comment)
		}
		if entry.(map[string]any)["phase"] != string(decide.Active) {
			h.t.Fatalf("%s should be ACTIVE by now: %v", key, entry)
		}
	}
}

// freeThenGrant is the ordinary failover script: the lease reads held on the
// first heldChecks checks, free after that, and any renew is granted.
//
// It tracks whether we hold the lease, because the real arbiter does and the
// difference is load-bearing: `read` on a lease we hold answers `yours`, not
// `free` (arbiter/src/lease.js, view()). It answers it with NO ttl — only a
// granted renew carries one — which is exactly the "a TTL-less Yours is not a
// grant" case decide's lease bookkeeping is written for. A fake that returned
// `free` to a holder's read would quietly send every demotion down decide's
// rule 15 instead of rule 14 and never exercise the release path at all.
func freeThenGrant(heldChecks int) func(int, string, arbiter.Op) arbiter.Verdict {
	var mu sync.Mutex
	held := map[string]bool{}
	return func(check int, key string, op arbiter.Op) arbiter.Verdict {
		mu.Lock()
		defer mu.Unlock()
		switch op.Op {
		case "renew":
			held[key] = true
			return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
		case "release":
			if !held[key] {
				// The arbiter refuses a release from a host that is not the
				// holder (spec §7, releaseOne). The client maps every error
				// but duplicate_host to Unreachable with Err preserved, so
				// this is what a stale release actually looks like coming
				// back — NOT a clean free/released.
				return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
			}
			held[key] = false
			return arbiter.Verdict{Lease: decide.Free, Released: true}
		}
		if held[key] {
			return arbiter.Verdict{Lease: decide.Yours} // a read never grants a ttl
		}
		if check < heldChecks {
			return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
		}
		return arbiter.Verdict{Lease: decide.Free}
	}
}

// alwaysYours is the shape of a lease that is still ours: what a conch that
// restarted over its own live service reads on its very first batch.
func alwaysYours(check int, key string, op arbiter.Op) arbiter.Verdict {
	switch op.Op {
	case "release":
		return arbiter.Verdict{Lease: decide.Free, Released: true}
	case "renew":
		return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
	}
	return arbiter.Verdict{Lease: decide.Yours} // a read never grants a ttl
}

// containsInOrder reports whether want appears in got as a subsequence —
// other calls may fall between the ones named, but never out of order.
func containsInOrder(got, want []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}

func mustContainInOrder(t *testing.T, got, want []string) {
	t.Helper()
	if !containsInOrder(got, want) {
		t.Fatalf("docker calls out of order or missing\nwant subsequence: %v\ngot: %v", want, got)
	}
}

// ---------------------------------------------------------------------------
// The brief's tests
// ---------------------------------------------------------------------------

func TestFailoverEndToEnd(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(1)

	// Batch 0 reads held; batches 1..6 read free. The check whose own pre-batch
	// pass completes the streak is the one that claims, so the claim rides
	// batch 6 — computed from this check's readings, not last check's.
	h.checks(7)
	if got := h.arb.opAt(6, "web"); got != "renew" {
		t.Fatalf("batch 6 should be the standby's first claim, got %q (ops: %v)", got, h.arb.ops("web"))
	}
	if got := h.arb.countOps("web", "renew"); got != 1 {
		t.Fatalf("exactly one claim should have been sent by now, got %d (ops: %v)", got, h.arb.ops("web"))
	}

	// Batch 6 got `yours` back and promoted in the same check: the service
	// container is stopped before the restore hook is created.
	d := h.checks(1)
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("the service should be ACTIVE once its hooks finished inside the lease, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}

	mustContainInOrder(t, h.dk.calls(), []string{
		"stop app-web",                  // the service stops before the restore writes
		"create conch-hook-web-restore", // the promote hook
		"start app-web",                 // then the service
		"start cf-web",                  // then, last, the connector
	})
	if !h.dk.running("cf-web") {
		t.Fatal("the connector should be running on the active host")
	}

	// Once the connector is up, the renewals say so: `serving` is the
	// container-level boolean a peer polls for hand-off confirmation (spec §9).
	h.check()
	last := h.arb.opStructAt(h.arb.nBatches()-1, "web")
	if last.Op != "renew" || !last.Serving {
		t.Fatalf("an active host renews with serving:true, got %+v", last)
	}
}

func TestClaimGrantedThisCheckStartsHooksThisCheck(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	h.checkUntilReadyToClaim("web")
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
		t.Fatalf("still standing by until the claim is granted, got %v", p)
	}
	before := len(h.dk.mutations())
	claim := h.arb.nBatches()

	d := h.check() // ONE check: sends the claim, gets the grant, starts the hooks
	if got := h.arb.opAt(claim, "web"); got != "renew" {
		t.Fatalf("this check should have sent the claim, got %q", got)
	}
	if d["web"].State.Phase != decide.Promoting {
		t.Fatalf("the grant must be acted on in the same check it arrived, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	after := h.dk.mutations()[before:]
	mustContainInOrder(t, after, []string{"stop app-web", "create conch-hook-web-restore"})
}

func TestAnAbandonedPromotionNeverStartsItsHooks(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	lost := false
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		h.dk.mu.Lock()
		gone := lost
		h.dk.mu.Unlock()
		if gone {
			return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
		}
		if op.Op == "renew" {
			return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
		}
		return arbiter.Verdict{Lease: decide.Free}
	}

	h.checkUntilReadyToClaim("web") // the next check claims

	// The service container's stop wedges, so the promotion is still inside
	// [StopService, RunPromoteHooks] when the lease is lost underneath it.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["app-web"] = gate
	h.dk.mu.Unlock()

	d := h.checkNoWait() // claim granted: PROMOTING, stop the service, then hooks
	if d["web"].State.Phase != decide.Promoting {
		t.Fatalf("expected PROMOTING, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}
	awaitGate(t, h.dk.gateHit, "the executor to enter Stop")

	h.dk.mu.Lock()
	lost = true
	h.dk.mu.Unlock()
	d = h.checkNoWait() // the lease reads held: abort and stand by
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a lease lost mid-hooks abandons the promotion, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}

	close(gate)
	h.waitIdle()

	for _, c := range h.dk.calls() {
		if c == "create conch-hook-web-restore" {
			t.Fatal("a restore must never start under a promotion the supervisor has already abandoned")
		}
	}
}

func TestAFailedPromoteHookReleasesAndBacksOff(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.dk.mu.Lock()
	h.dk.waitCode["conch-hook-web-restore"] = 1 // the restore exits non-zero
	h.dk.mu.Unlock()

	h.checks(h.cfg.Timing.PromoteAfter) // claim granted, hooks run and fail
	d := h.check()

	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a failed restore abandons the promotion, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	if d["web"].State.BackoffUntil == 0 {
		t.Fatal("a host whose hooks just failed must back off, not re-claim in a tight loop (spec §5)")
	}
	release := h.arb.nBatches()
	renewsBefore := h.arb.countOps("web", "renew")

	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if got := h.arb.opAt(release, "web"); got != "release" {
		t.Fatalf("the lease must go back so the peer can try, got %q (ops %v)", got, h.arb.ops("web"))
	}
	if got := h.arb.countOps("web", "renew"); got != renewsBefore {
		t.Fatalf("the backoff must suppress re-claiming: %d new renews (%v)", got-renewsBefore, h.arb.ops("web"))
	}
}

// TestASecondPromotionRestoresAgain guards a silent data-loss path: a service
// that was demoted and later claims the lease again must run its restore
// again. A hook result left over from the previous promotion would make
// decide's rule 8 fire immediately — service and connector started on data
// nothing restored.
// ---------------------------------------------------------------------------
// Spec §5's reconcile: what a `yours` reading MEANS depends on when it arrives
// ---------------------------------------------------------------------------

func TestARestartOverALiveServiceResumesActiveWithoutRestoring(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.dk.setRunning("cf-web", true, true) // we were serving when the old conch died
	h.arb.reply = alwaysYours

	d := h.check()

	if d["web"].State.Phase != decide.Active {
		t.Fatalf("a conch that restarted over its own live service resumes ACTIVE (spec §5), got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.mutations() {
		t.Fatalf("a redeploy of conch must take nothing down and restore nothing (spec §9's zero blip): %q", c)
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the connector was serving and must keep serving")
	}

	// The read that adopted carried no ttl — only a renew grants one — so the
	// deadline arms on the first renewal, one check later.
	if d["web"].State.LeaseExpiry != 0 {
		t.Fatalf("a ttl-less yours must not manufacture a deadline, got %v", d["web"].State.LeaseExpiry)
	}
	if got := h.arb.opAt(0, "web"); got != "read" {
		t.Fatalf("the reconcile batch is a read, got %q", got)
	}
	d = h.check()
	if got := h.arb.opAt(1, "web"); got != "renew" {
		t.Fatalf("an adopted service renews from the next check on, got %q", got)
	}
	if want := h.clk.Mono() + 60; d["web"].State.LeaseExpiry != want {
		t.Fatalf("the lease deadline should be armed send-anchored at %v, got %v",
			want, d["web"].State.LeaseExpiry)
	}
}

func TestARestartAdoptsEvenWhenTheServiceIsDown(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.dk.setRunning("app-web", true, false) // exists, but not running
	h.arb.reply = alwaysYours

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("adoption is not gated on the service being up, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}

	// ...and the ordinary health hysteresis then demotes it, which is the safe
	// path: the peer promotes properly, with its hooks, from the clean replica.
	d = h.checks(h.cfg.Timing.DemoteAfter)
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a service that is down must demote on hysteresis, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls() {
		if c == "create conch-hook-web-restore" {
			t.Fatal("nothing here ever justified a restore on this host")
		}
	}
}

func TestAdoptionNeverFiresAfterTheFirstReading(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	h.checkUntilReadyToClaim("web") // the first readings were free: nothing to adopt
	d := h.check()

	if d["web"].State.Phase != decide.Promoting {
		t.Fatalf("a mid-life granted claim goes through rule 6 and PROMOTING, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	if !containsInOrder(h.dk.calls(), []string{"create conch-hook-web-restore"}) {
		t.Fatal("a lease won from FREE must run its promote hooks: the replica is newer than the local disk")
	}
}

func TestASuspendResumesAnUnchangedLeaseWithoutRestoring(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	mark := len(h.dk.calls())
	h.clk.AdvanceWallOnly(120)
	d := h.check() // unknown time passed: stop the connector, distrust the lease
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("every held lease is unproven after a suspend, got %s", d["web"].State.Phase)
	}
	if !containsInOrder(h.dk.calls()[mark:], []string{"stop cf-web"}) {
		t.Fatalf("connectors stop first: %v", h.dk.calls()[mark:])
	}

	// Spec §5: "then re-read the arbiter and reconcile". The lease turns out to
	// still be ours, and a service we were serving seconds ago must be resumed,
	// not restored over.
	next := h.arb.nBatches()
	d = h.check()
	if got := h.arb.opAt(next, "web"); got != "read" {
		t.Fatalf("the batch after a suspend re-reads, got %q (ops %v)", got, h.arb.ops("web"))
	}
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("a lease that survived the suspend is resumed, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls()[mark:] {
		if c == "create conch-hook-web-restore" {
			t.Fatal("a suspend is not a reason to restore over live data")
		}
	}

	h.check() // rule 12: we hold the lease, so the connector comes back
	if !h.dk.running("cf-web") {
		t.Fatalf("the connector should be serving again: %v", h.dk.calls()[mark:])
	}
}

// ---------------------------------------------------------------------------
// The release obligation
// ---------------------------------------------------------------------------

func TestAReleaseIsRetriedUntilTheArbiterConfirmsIt(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter) // demote: stop the connector, owe a release

	// The arbiter goes dark. The obligation must survive: a stopped service
	// holding a ticking lease is an outage nobody can end (spec §9).
	h.arb.mu.Lock()
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.arb.mu.Unlock()
	for i := 0; i < 3; i++ {
		at := h.arb.nBatches()
		h.check()
		if got := h.arb.opAt(at, "web"); got != "release" {
			t.Fatalf("an unreachable batch must not drop the release, got %q", got)
		}
	}

	// Contact returns, but the arbiter still shows the lease as ours: the
	// release has not landed, so it is still owed.
	h.arb.mu.Lock()
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Yours}
	}
	h.arb.mu.Unlock()
	for i := 0; i < 2; i++ {
		at := h.arb.nBatches()
		h.check()
		if got := h.arb.opAt(at, "web"); got != "release" {
			t.Fatalf("a reply that still reads yours means the release has not landed, got %q", got)
		}
		if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
			t.Fatalf("a lease we are giving back must never be re-adopted, phase %v", p)
		}
	}

	// Finally it lands, and the arbiter says so: this is the `released: true`
	// path. (A refusal — `not_holder` — clears the obligation too, and has its
	// own test: TestARefusedReleaseIsNotAFailedOne.)
	h.arb.mu.Lock()
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		if op.Op == "release" {
			return arbiter.Verdict{Lease: decide.Free, Released: true}
		}
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.arb.mu.Unlock()
	h.check()
	if h.sup.Status()["web"].(map[string]any)["release_owed"] != false {
		t.Fatal("a confirmed release is no longer owed")
	}
	at := h.arb.nBatches()
	h.check()
	if got := h.arb.opAt(at, "web"); got != "read" {
		t.Fatalf("nothing is owed any more, got %q", got)
	}
}

// TestARefusedReleaseIsNotAFailedOne is the difference between "we never
// asked" and "we asked and were told no". Both arrive as `unreachable` — the
// client maps every arbiter error but duplicate_host that way so a bug of ours
// can never read as a lease state — and only Verdict.Err tells them apart. A
// lease that expires during an arbiter outage can never be released once
// contact returns, so treating the refusal as a failed round trip pins the op
// at `release` forever and the host never supervises anything again.
func TestARefusedReleaseIsNotAFailedOne(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)

	var mu sync.Mutex
	held, reachable := false, true
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		mu.Lock()
		defer mu.Unlock()
		if !reachable {
			return arbiter.Verdict{Lease: decide.Unreachable}
		}
		switch op.Op {
		case "renew":
			held = true
			return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
		case "release":
			if !held {
				return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
			}
			held = false
			return arbiter.Verdict{Lease: decide.Free, Released: true}
		}
		if held {
			return arbiter.Verdict{Lease: decide.Yours}
		}
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.driveToActive()

	// The service goes sick and the arbiter goes away in the same window.
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter) // demote: connector stopped, release owed
	mu.Lock()
	reachable = false
	mu.Unlock()
	for i := 0; i < 3; i++ {
		at := h.arb.nBatches()
		h.check()
		if got := h.arb.opAt(at, "web"); got != "release" {
			t.Fatalf("the release is still owed while the arbiter is silent, got %q", got)
		}
	}

	// The lease expires server-side during the outage, so when contact returns
	// the arbiter refuses the release: we are not the holder any more.
	mu.Lock()
	held, reachable = false, true
	mu.Unlock()
	at := h.arb.nBatches()
	h.check()
	if got := h.arb.opAt(at, "web"); got != "release" {
		t.Fatalf("the release should have been retried the moment contact returned, got %q", got)
	}
	if h.sup.Status()["web"].(map[string]any)["release_owed"] != false {
		t.Fatal("`not_holder` is the arbiter's definitive answer that the lease is not ours: nothing is owed any more")
	}

	// And the host supervises again: it is ordinary standby capacity.
	h.pr.setDenied("http://app-web/up", false)
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if got := h.arb.opAt(h.arb.nBatches()-1, "web"); got == "release" {
		t.Fatalf("the op is still pinned to release: %v", h.arb.ops("web"))
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("the host should have re-claimed the free lease normally, phase %v (ops %v)",
			p, h.arb.ops("web"))
	}
}

func TestRefusedReleaseAlsoRetiresOrphanEvidence(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.orphans = []string{"conch-hook-web-restore"}
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		switch op.Op {
		case "release":
			// The lease expired while the old conch was dying mid-restore.
			return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
		case "renew":
			return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
		}
		if check == 0 {
			return arbiter.Verdict{Lease: decide.Yours}
		}
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.build(nil)

	h.check() // torn data: release rather than adopt
	h.check() // ...refused, because the lease is not ours any more

	// The evidence has done its job. The host is now an ordinary standby, and
	// claiming from FREE runs the promote hooks, which is the right way to get
	// this data back: the replica is newer than the local disk.
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if !containsInOrder(h.dk.calls(), []string{"create conch-hook-web-restore"}) {
		t.Fatalf("a host whose evidence is retired must be able to promote again: %v", h.arb.ops("web"))
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("expected ACTIVE, got %v (ops %v)", p, h.arb.ops("web"))
	}
}

// TestAnAbortedRestoreIsItsOwnOrphanEvidence closes the other half of spec
// §5's crash-mid-restore rule. The startup sweep finds a hook container that
// outlived a dead conch — but when THIS conch aborts a restore, it kills that
// container itself, so the evidence never survives to be swept. It has to be
// recorded at the moment of the abort or the next `yours` reading adopts a
// half-restored service.
func TestAnAbortedRestoreIsItsOwnOrphanEvidence(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	waitGate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-restore"] = waitGate
	h.dk.mu.Unlock()

	h.checkUntilReadyToClaim("web")
	h.checkNoWait() // the claim is granted: the restore starts
	awaitGate(t, h.dk.waitHit, "the restore to start")

	h.clk.AdvanceWallOnly(120) // the VM pauses in the middle of the restore
	d := h.checkNoWait()
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a suspend abandons the promotion, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}
	close(waitGate)
	h.waitIdle()

	// The lease is still ours, and the reconcile is owed — but the data under
	// it is half-restored, so this must be a release, not an adoption.
	d = h.check()
	if d["web"].State.Phase == decide.Active {
		t.Fatalf("adopted a service whose restore was cut off part way: %s", d["web"].Reason)
	}
	if d["web"].NextOp != "release" {
		t.Fatalf("torn data goes back to the peer, got next op %q (%s)", d["web"].NextOp, d["web"].Reason)
	}
}

// TestACrashedConnectorIsRestartedNotDemoted is decide's rule 12 — "starting a
// connector we find stopped is safe precisely because the lease is ours, and
// it heals a crash between the stop and the release, which would otherwise be
// a silent zero-traffic outage with every indicator green". It is reachable
// only if a connector that is DOWN is distinguished from one that is up and
// cannot register with the edge; folding a down container into the ready check
// makes rule 12 dead code on every service with a ready_url.
func TestACrashedConnectorIsRestartedNotDemoted(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// The connector goes away under us — a crash between a stop and a release,
	// a stray `docker stop`. Its ready endpoint necessarily stops answering.
	h.dk.setRunning("cf-web", true, false)
	mark := len(h.dk.calls())

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("a connector conch can simply restart is not a demotion, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	if !containsInOrder(h.dk.calls()[mark:], []string{"start cf-web"}) {
		t.Fatalf("the connector must be restarted (rule 12): %v", h.dk.calls()[mark:])
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the connector should be serving again")
	}

	// And a connector that IS up but cannot register is still a demotion.
	h.pr.setDenied("http://cf-web:2000/ready", true)
	d = h.checks(h.cfg.Timing.DemoteAfter)
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a connector the edge cannot see serves nothing: expected demotion, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
}

func TestASecondPromotionRestoresAgain(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	restores := func() int {
		n := 0
		for _, c := range h.dk.calls() {
			if c == "create conch-hook-web-restore" {
				n++
			}
		}
		return n
	}
	if got := restores(); got != 1 {
		t.Fatalf("the first promotion should have restored exactly once, got %d", got)
	}

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter) // demote: stop the connector, give the lease back
	h.check()                          // the release goes out
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Standby) {
		t.Fatalf("expected STANDBY after the demotion, got %v", p)
	}

	h.pr.setDenied("http://app-web/up", false)
	h.checks(h.cfg.Timing.PromoteAfter + 2) // claim again, promote again

	if got := restores(); got != 2 {
		t.Fatalf("a second promotion must restore again, not reuse the first one's result: %d restores", got)
	}
	if p := h.sup.Status()["web"].(map[string]any)["phase"]; p != string(decide.Active) {
		t.Fatalf("expected ACTIVE again, got %v", p)
	}
}

// TestNothingStartsUntilTheRestoreActuallyFinishes is the other half of the
// same guarantee, and the one that bites: it re-promotes a service that has
// already been promoted once, with the second restore blocked. Nothing may
// start — not the service, not the connector — until that restore reports
// done. A hook result carried over from the FIRST promotion would start both
// on data the second restore is still writing (spec §5: "restore before the
// service runs, verify before the connector starts").
func TestNothingStartsUntilTheRestoreActuallyFinishes(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter)
	h.check() // the release goes out; back to standby
	h.pr.setDenied("http://app-web/up", false)

	// The second restore blocks inside Wait.
	waitGate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-restore"] = waitGate
	h.dk.mu.Unlock()

	// Stop short of the claim: the check that grants it starts a restore that
	// will not finish, and waiting for that executor to go idle would be
	// waiting forever.
	h.checkUntilReadyToClaim("web")
	mark := len(h.dk.calls())
	h.checkNoWait() // the claim is granted: PROMOTING, stop the service, restore
	awaitGate(t, h.dk.waitHit, "the restore to start")
	h.checkNoWait() // ... and the loop keeps checking under it
	d := h.checkNoWait()

	if d["web"].State.Phase != decide.Promoting {
		t.Fatalf("the promotion must wait for its restore, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls()[mark:] {
		if c == "start app-web" || c == "start cf-web" {
			t.Fatalf("%q happened while the restore was still running", c)
		}
	}
	if got := h.arb.opAt(h.arb.nBatches()-1, "web"); got != "renew" {
		t.Fatalf("the lease must be renewed under the hooks (spec §5), got %q", got)
	}

	close(waitGate)
	h.waitIdle()
	d = h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("expected ACTIVE once the restore finished, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	mustContainInOrder(t, h.dk.calls()[mark:], []string{"start app-web", "start cf-web"})
}

// TestAQueuedAbortIsCarriedForwardNotDropped drives the executor's queue
// directly, because the interleaving it pins — a newer decision arriving while
// an abort is still queued behind a wedged docker call — is a property of
// submit, not of any particular loop timing. Every other action is re-derived
// from fresh readings every check and so may safely be dropped; AbortHooks is
// named once, by the decision that abandons a promotion, and never again.
func TestAQueuedAbortIsCarriedForwardNotDropped(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	e := h.sup.units["web"].ex

	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["cf-web"] = gate
	h.dk.mu.Unlock()

	e.submit([]decide.Action{decide.StopConnector}, "app-web", stopIntent{}) // the executor wedges in here
	awaitGate(t, h.dk.gateHit, "the executor to enter Stop")
	e.submit([]decide.Action{decide.AbortHooks}, "app-web", stopIntent{})  // queued behind the wedged stop
	e.submit([]decide.Action{decide.StopService}, "app-web", stopIntent{}) // and superseded by a newer decision

	close(gate)
	e.waitIdle()

	if h.hk.abortCount() != 1 {
		t.Fatalf("the abort was dropped when a newer decision superseded it: %d aborts", h.hk.abortCount())
	}
	if !containsInOrder(h.dk.calls(), []string{"stop cf-web", "stop app-web"}) {
		t.Fatalf("the newer decision must still be applied: %v", h.dk.calls())
	}
}

func TestDryRunSendsOnlyReads(t *testing.T) {
	h := newHarness(t, testConfig("web"), func(o *Options) { o.DryRun = true })
	h.arb.reply = freeThenGrant(0)
	// A connector left running is exactly what a standby would act on, and a
	// peer fingerprint that does not match is exactly what would page someone.
	h.dk.setRunning("cf-web", true, true)
	h.dk.orphans = []string{"conch-hook-web-restore"}
	h.arb.fingerprints = map[string]string{"host-b": "0000deadbeef0000"}

	h.checks(10)

	for i, op := range h.arb.ops("web") {
		if op != "read" {
			t.Fatalf("dry run sent %q on batch %d; a dry run must be incapable of claiming a lease (spec §10)", op, i)
		}
	}
	if got := h.dk.mutations(); len(got) != 0 {
		t.Fatalf("dry run touched docker: %v", got)
	}
	for _, c := range h.dk.calls() {
		if strings.HasPrefix(c, "list ") {
			t.Fatal("dry run must not sweep orphans: killing them is an action")
		}
	}
	h.nt.mu.Lock()
	fired := len(h.nt.events)
	h.nt.mu.Unlock()
	if fired != 0 {
		t.Fatalf("a dry run executes none of the actions it describes, and a webhook pages a human: %d events",
			fired)
	}
}

func TestASlowStopDoesNotBlockAnotherService(t *testing.T) {
	h := newHarness(t, testConfig("web", "blog"), nil)
	h.arb.reply = freeThenGrant(0)

	h.driveToActive() // both services claim, promote and go active together

	// web's connector stop wedges. blog must not notice.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["cf-web"] = gate
	h.dk.mu.Unlock()
	h.pr.setDenied("http://app-web/up", true)

	h.checkNoWait() // unhealthy 1/3
	h.checkNoWait() // 2/3
	h.checkNoWait() // 3/3: stop the connector — and block in it
	awaitGate(t, h.dk.gateHit, "web's executor to enter Stop")

	// blog's renew count IS the proof that the loop advanced: it is active and
	// healthy, so its op comes from its own committed decision every check and
	// one renew per check is exact. A loop that had blocked behind web's stop
	// would simply stop producing them.
	blogBefore := h.arb.countOps("blog", "renew")
	for i := 0; i < 5; i++ {
		h.checkNoWait()
	}
	blogAfter := h.arb.countOps("blog", "renew")
	if blogAfter-blogBefore != 5 {
		t.Fatalf("blog's renewals must keep flowing while web's stop is wedged: %d renewals over 5 checks",
			blogAfter-blogBefore)
	}
	if h.sup.Status()["blog"].(map[string]any)["phase"] != string(decide.Active) {
		t.Fatal("blog should still be active")
	}

	close(gate)
	h.waitIdle()
}

func TestOrphanEvidenceReleasesInsteadOfAdopting(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.orphans = []string{"conch-hook-web-restore"}
	// The lease is still ours across the crash — the tempting, wrong, adoption.
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		if op.Op == "release" {
			return arbiter.Verdict{Lease: decide.Free, Released: true}
		}
		return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
	}
	h.build(nil)

	// New() must sweep the orphans before the first lease read (spec §5).
	if !containsInOrder(h.dk.calls(), []string{"list conch.hook=true", "kill conch-hook-web-restore"}) {
		t.Fatalf("orphans must be killed at startup, before any lease is read: %v", h.dk.calls())
	}

	d := h.check()
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a crash mid-restore must not adopt the lease, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	if got := h.dk.mutations(); containsInOrder(got, []string{"create conch-hook-web-restore"}) {
		t.Fatalf("no hooks may run for a torn service: %v", got)
	}

	h.check()
	if got := h.arb.opAt(1, "web"); got != "release" {
		t.Fatalf("the lease must be handed back so the peer can restore from the clean replica, got %q (ops %v)",
			got, h.arb.ops("web"))
	}
}

func TestOrphanEvidenceSurvivesAFailedRelease(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.orphans = []string{"conch-hook-web-restore"}
	// The release never lands: every answer keeps saying the lease is ours.
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Yours, TTL: 60}
	}
	h.build(nil)

	for i := 0; i < 5; i++ {
		d := h.check()
		if d["web"].State.Phase != decide.Standby {
			t.Fatalf("check %d adopted a torn service: %s (%s)", i, d["web"].State.Phase, d["web"].Reason)
		}
	}
	if got := h.dk.mutations(); containsInOrder(got, []string{"create conch-hook-web-restore"}) {
		t.Fatalf("no hooks may run for a torn service: %v", got)
	}
}

func TestSuspendDetectionStopsFirst(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	before := len(h.dk.calls())
	h.clk.AdvanceWallOnly(120) // the VM was paused; monotonic time did not move
	d := h.check()

	if !containsInOrder(h.dk.calls()[before:], []string{"stop cf-web"}) {
		t.Fatalf("unknown time passed: connectors stop first (spec §5): %v", h.dk.calls()[before:])
	}
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("every held lease is unproven after a suspend, got %s", d["web"].State.Phase)
	}
	if d["web"].NextOp != "read" {
		t.Fatalf("the next batch must re-read the arbiter, got %q", d["web"].NextOp)
	}
	next := h.arb.nBatches()
	h.check()
	if got := h.arb.opAt(next, "web"); got != "read" {
		t.Fatalf("the batch after a suspend must be a read, got %q (ops %v)", got, h.arb.ops("web"))
	}
}

func TestHostFactsGatheredOncePerCheck(t *testing.T) {
	diskReads := 0
	cfg := testConfig("web", "blog", "api")
	cfg.Disk = config.DiskConfig{Path: "/var/lib/docker", MinFreeGB: 10}
	h := newHarness(t, cfg, func(o *Options) {
		o.DiskFreeGB = func(string) (float64, error) { diskReads++; return 500, nil }
	})

	h.check()

	h.dk.mu.Lock()
	healthyN := h.dk.healthyN
	h.dk.mu.Unlock()
	if healthyN != 1 {
		t.Fatalf("host health is measured once per check and shared by every service (spec §4), got %d", healthyN)
	}
	if diskReads != 1 {
		t.Fatalf("the disk floor is measured once per check, got %d", diskReads)
	}
}

func TestConnectorReadyFoldsIntoActiveHealth(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	h.checkUntilReadyToClaim("web") // still standing by
	if got := h.pr.countCalls("http://cf-web:2000/ready"); got != 0 {
		t.Fatalf("a standby has no connector to be ready: %d ready probes", got)
	}

	h.driveToActive()
	if got := h.pr.countCalls("http://cf-web:2000/ready"); got == 0 {
		t.Fatal("the active host must probe connector readiness (spec §4)")
	}

	// Service health stays 200; only the connector cannot register with the edge.
	h.pr.setDenied("http://cf-web:2000/ready", true)
	before := len(h.dk.calls())
	d := h.checks(h.cfg.Timing.DemoteAfter)

	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("a connector the edge cannot see serves nothing: expected demotion, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	if !containsInOrder(h.dk.calls()[before:], []string{"stop cf-web"}) {
		t.Fatalf("the connector must be stopped on demotion: %v", h.dk.calls()[before:])
	}
	next := h.arb.nBatches()
	h.check()
	if got := h.arb.opAt(next, "web"); got != "release" {
		t.Fatalf("the lease is given up after the connector stopped, got %q (ops %v)", got, h.arb.ops("web"))
	}
}

func TestReleaseNeverOvertakesTheConnectorStop(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// The connector's stop wedges. Nothing about the release may depend on the
	// loop happening to be faster than the executor — the guarantee is that
	// the stop has HAPPENED, so a wedged stop must hold the release
	// indefinitely, not merely for one check.
	gate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["cf-web"] = gate
	h.dk.mu.Unlock()

	h.pr.setDenied("http://app-web/up", true)
	for i := 0; i < h.cfg.Timing.DemoteAfter; i++ {
		h.checkNoWait()
	}
	demote := h.arb.nBatches() - 1 // the batch that decided the demotion
	awaitGate(t, h.dk.gateHit, "the executor to enter Stop")

	for i := 0; i < 5; i++ {
		h.checkNoWait()
	}
	for i := demote; i < h.arb.nBatches(); i++ {
		if got := h.arb.opAt(i, "web"); got == "release" {
			t.Fatalf("batch %d released the lease while our connector was still draining: "+
				"a FREE lease must keep meaning the previous holder already stopped (spec §5). ops: %v",
				i, h.arb.ops("web"))
		}
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the fake should still be holding the connector up")
	}

	close(gate)
	h.waitIdle()

	at := h.arb.nBatches()
	h.checkNoWait()
	if got := h.arb.opAt(at, "web"); got != "release" {
		t.Fatalf("once the stop has actually completed the release must go out, got %q (ops %v)",
			got, h.arb.ops("web"))
	}
}

// TestArbiterRefusalLogsOnceThenResetsThenLogsAgain pins the split: not_holder
// and invalid_op are the arbiter REFUSING with an opinion, not the arbiter
// being unreachable, so they must say "refused", never "unreachable...check
// this url" — but still only once per streak, so a host stuck this way does
// not fill the log with one warning per interval forever.
func TestArbiterRefusalLogsOnceThenResetsThenLogsAgain(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	refused := func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
	}
	h.arb.reply = refused

	// Three straight refusal checks must produce exactly one warning, not
	// three.
	h.checks(3)
	warns := handler.of("arbiter refused")
	if len(warns) != 1 {
		t.Fatalf("want exactly 1 warn across 3 refused checks, got %d: %+v", len(warns), warns)
	}
	if strings.Contains(warns[0].msg, "check this url") {
		t.Fatalf("a refusal is not an unreachable arbiter and must not point at the url: %q", warns[0].msg)
	}
	if !strings.Contains(warns[0].msg, "reachable") {
		t.Fatalf("a refusal warn must say the arbiter is reachable, got %q", warns[0].msg)
	}
	if got := warns[0].attrs["err"]; got != "not_holder" {
		t.Fatalf("warn must carry the verdict's Err, got attrs %+v", warns[0].attrs)
	}
	if got := warns[0].attrs["services"]; got != "web" {
		t.Fatalf("warn must name the affected service, got attrs %+v", warns[0].attrs)
	}
	if len(handler.of("arbiter unreachable")) != 0 {
		t.Fatalf("a refusal must never also log as unreachable")
	}

	// A reachable check resets the streak...
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.check()
	if got := len(handler.of("arbiter refused")); got != 1 {
		t.Fatalf("a reachable check must not itself warn, got %d warns", got)
	}

	// ...so the next refusal streak logs once more, not zero more times.
	h.arb.reply = refused
	h.checks(2)
	if got := len(handler.of("arbiter refused")); got != 2 {
		t.Fatalf("a new refusal streak after a reachable check must warn again, got %d warns total", got)
	}
}

// TestArbiterRefusalNamesAllAffectedServices pins the collect-ALL-keys fix:
// the old code took the first Unreachable-with-Err match and broke, so a
// batch with more than one refused service silently dropped every name past
// the first.
func TestArbiterRefusalNamesAllAffectedServices(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("db", "web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
	}
	h.checks(1)

	warns := handler.of("arbiter refused")
	if len(warns) != 1 {
		t.Fatalf("want exactly 1 warn, got %d: %+v", len(warns), warns)
	}
	got := warns[0].attrs["services"]
	if !strings.Contains(got, "db") || !strings.Contains(got, "web") {
		t.Fatalf("warn must name every affected service, got services=%q", got)
	}
}

// TestArbiterOtherErrKeepsUnreachableWording pins the other half of the
// split: an Err value that is neither not_holder nor invalid_op (a future
// protocol addition, or a state string this client does not recognise) is
// not the confident "we asked and were told no" of a refusal, so it keeps the
// old unreachable-flavoured wording rather than claiming reachability it
// cannot back up.
func TestArbiterOtherErrKeepsUnreachableWording(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable, Err: "some_future_state"}
	}
	h.checks(1)

	warns := handler.of("arbiter unreachable")
	if len(warns) != 1 {
		t.Fatalf("want exactly 1 warn, got %d: %+v", len(warns), warns)
	}
	if got := warns[0].attrs["err"]; got != "some_future_state" {
		t.Fatalf("warn must carry the verdict's Err, got attrs %+v", warns[0].attrs)
	}
	if got := warns[0].attrs["url"]; got != h.cfg.Arbiter.URL {
		t.Fatalf("warn must carry the configured arbiter url, got attrs %+v", warns[0].attrs)
	}
	if len(handler.of("arbiter refused")) != 0 {
		t.Fatalf("an unrecognised error must not be logged as a confident refusal")
	}
}

// TestArbiterTransportSilenceWarnsAtThresholdThenRecovers pins the
// deliberate extension: a pure transport failure carries no Err by
// construction (arbiter/client.go's allUnreachable), so it is structurally
// silent under the refusal/error logging above. Below demote_after
// consecutive checks it must still say nothing — the ordinary blip a network
// hiccup produces is not an outage — but at the threshold it must log once,
// naming every affected service and how long the streak has run, and
// recovery must log once more with the total duration so post-incident work
// has both ends of the window.
func TestArbiterTransportSilenceWarnsAtThresholdThenRecovers(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	transport := func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.arb.reply = transport

	// demote_after is 3 (testConfig): two checks must stay silent.
	h.checks(2)
	if got := len(handler.of("arbiter")); got != 0 {
		t.Fatalf("below the threshold nothing should be logged, got %d records", got)
	}

	// The third crosses the threshold.
	h.check()
	warns := handler.of("arbiter")
	if len(warns) != 1 {
		t.Fatalf("want exactly 1 warn at the threshold, got %d: %+v", len(warns), warns)
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("the threshold record must be a Warn, got %v", warns[0].level)
	}
	if got := warns[0].attrs["services"]; got != "web" {
		t.Fatalf("warn must name every affected service, got attrs %+v", warns[0].attrs)
	}
	if got := warns[0].attrs["streak"]; got != "3" {
		t.Fatalf("warn must carry the streak length, got attrs %+v", warns[0].attrs)
	}

	// Two more transport-silent checks must not re-fire the latch.
	h.checks(2)
	if got := len(handler.of("arbiter")); got != 1 {
		t.Fatalf("the latch must not re-fire mid-streak, got %d records total", got)
	}

	// Recovery logs exactly one Info with the total duration: 5 checks at
	// interval 3 = 15 seconds from the first unreachable check to this one.
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.check()
	all := handler.of("arbiter")
	if len(all) != 2 {
		t.Fatalf("want exactly 2 arbiter records total (threshold warn + recovery info), got %d: %+v", len(all), all)
	}
	recovery := all[1]
	if recovery.level != slog.LevelInfo {
		t.Fatalf("recovery must be an Info, got %v", recovery.level)
	}
	if got := recovery.attrs["duration_seconds"]; got != "15" {
		t.Fatalf("recovery must carry the streak's total duration, got attrs %+v", recovery.attrs)
	}
}

// TestArbiterTransportSilenceBelowThresholdNeverWarnsOnRecovery pins the
// other edge: a streak that recovers before ever crossing demote_after must
// not produce a recovery Info either, since no Warn ever fired for it — an
// Info with no matching Warn would be a "recovery" from nothing.
func TestArbiterTransportSilenceBelowThresholdNeverWarnsOnRecovery(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.checks(2) // demote_after is 3: never crosses the threshold

	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Free}
	}
	h.check()
	if got := len(handler.of("arbiter")); got != 0 {
		t.Fatalf("a sub-threshold blip must never log, got %d records: %+v", got, handler.of("arbiter"))
	}
}

// TestArbiterMixedBatchClassifiesPerService pins the rule for a batch that
// mixes categories across services in the SAME reply: each service's own
// verdict decides its own category (refusal, other-err, or Err-less
// transport), independent of what the rest of the batch looks like. A
// refusal on one service warns immediately; a sibling service that is purely
// transport-unreachable in the same batch only ever contributes to that
// category's own streak counter and does not warn until IT crosses
// demote_after — the two categories share no latch and do not gate each
// other.
func TestArbiterMixedBatchClassifiesPerService(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("db", "web"), func(o *Options) {
		o.Logger = slog.New(handler)
	})
	h.arb.reply = func(check int, key string, op arbiter.Op) arbiter.Verdict {
		if key == "web" {
			return arbiter.Verdict{Lease: decide.Unreachable, Err: "not_holder"}
		}
		return arbiter.Verdict{Lease: decide.Unreachable} // db: pure transport
	}

	// Check 1: web's refusal must warn immediately; db's transport streak is
	// only at 1, well below demote_after (3).
	h.checks(1)
	refusals := handler.of("arbiter refused")
	if len(refusals) != 1 {
		t.Fatalf("want exactly 1 refusal warn after check 1, got %d: %+v", len(refusals), refusals)
	}
	if got := refusals[0].attrs["services"]; got != "web" {
		t.Fatalf("refusal must name only the refused service, got attrs %+v", refusals[0].attrs)
	}
	if got := len(handler.of("arbiter unreachable")); got != 0 {
		t.Fatalf("db's transport streak must not warn before the threshold, got %d", got)
	}

	// Checks 2 and 3: web's refusal stays latched (no new warn); db's
	// transport streak reaches demote_after and warns once, naming only db.
	h.checks(2)
	if got := len(handler.of("arbiter refused")); got != 1 {
		t.Fatalf("the refusal latch must not re-fire mid-streak, got %d", got)
	}
	transportWarns := handler.of("arbiter unreachable with no error")
	if len(transportWarns) != 1 {
		t.Fatalf("want exactly 1 transport warn at db's threshold, got %d: %+v", len(transportWarns), transportWarns)
	}
	if got := transportWarns[0].attrs["services"]; got != "db" {
		t.Fatalf("transport warn must name only the transport-silent service, got attrs %+v", transportWarns[0].attrs)
	}
}

func TestFingerprintMismatchWarnsButNeverActs(t *testing.T) {
	control := newHarness(t, testConfig("web"), nil)
	control.arb.reply = freeThenGrant(0)
	want := control.checks(9)

	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.arb.fingerprints = map[string]string{
		"host-a": h.cfg.Fingerprint(),
		"host-b": "0000deadbeef0000",
	}
	got := h.checks(9)

	if len(h.nt.of("fingerprint_mismatch")) == 0 {
		t.Fatal("a peer supervising a different set of services must be reported")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a fingerprint mismatch must never change a decision\n got: %+v\nwant: %+v", got, want)
	}
	if !reflect.DeepEqual(control.dk.mutations(), h.dk.mutations()) {
		t.Fatalf("a fingerprint mismatch must never change an action\n got: %v\nwant: %v",
			h.dk.mutations(), control.dk.mutations())
	}
}

func TestStopEscalatesToKillAndAlerts(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// The daemon accepts the stop and the kill, and the container keeps running.
	h.dk.mu.Lock()
	h.dk.stopStays["cf-web"] = true
	h.dk.killStays["cf-web"] = true
	h.dk.mu.Unlock()

	h.pr.setDenied("http://app-web/up", true)
	before := len(h.dk.calls())
	h.checks(h.cfg.Timing.DemoteAfter)

	var seq []string
	for _, c := range h.dk.calls()[before:] {
		if strings.HasSuffix(c, " cf-web") {
			seq = append(seq, c)
		}
	}
	mustContainInOrder(t, seq, []string{"stop cf-web", "inspect cf-web", "kill cf-web", "inspect cf-web"})

	alerts := h.nt.of("alert")
	found := false
	for _, a := range alerts {
		if a.payload["container"] == "cf-web" {
			found = true
		}
	}
	if !found {
		t.Fatalf("a container that survives stop AND kill earns the loudest notification conch has (spec §9): %v", alerts)
	}
}

func TestEligibilityRequiresContainersAndHookImages(t *testing.T) {
	cases := []struct {
		name       string
		breakIt    func(*harness)
		wantStreak int
	}{
		{"service container missing", func(h *harness) { h.dk.setRunning("app-web", false, false) }, 0},
		{"connector container missing", func(h *harness) { h.dk.setRunning("cf-web", false, false) }, 0},
		{"hook image absent", func(h *harness) {
			h.dk.mu.Lock()
			h.dk.images["restore-web:1"] = false
			h.dk.mu.Unlock()
		}, 0},
		{"everything present", func(h *harness) {}, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig("web"), nil)
			h.arb.reply = freeThenGrant(0)
			tc.breakIt(h)

			d := h.checks(4)
			if got := d["web"].State.OKStreak; got != tc.wantStreak {
				t.Fatalf("standby streak %d, want %d (%s)", got, tc.wantStreak, d["web"].Reason)
			}
			if tc.wantStreak == 0 && h.arb.countOps("web", "renew") != 0 {
				t.Fatalf("an ineligible host must never claim: %v", h.arb.ops("web"))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Accessories: long-lived sidecars (a litestream replicator, say) that must
// run on exactly the host that serves the service, reconciled like the
// connector — started with the service, stopped wherever it does not serve,
// existence a promotion prerequisite.
// ---------------------------------------------------------------------------

// accessoryTestConfig is testConfig with one accessory, the replication-
// sidecar shape the feature exists for.
func accessoryTestConfig(name string) config.Config {
	cfg := testConfig(name)
	cfg.Services[0].Accessories = []string{name + "-litestream"}
	return cfg
}

func TestPromotionStartsAccessoriesBetweenServiceAndConnector(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// Restore, then the service, then replication, then — last — traffic.
	mustContainInOrder(t, h.dk.calls(), []string{
		"stop app-web",
		"create conch-hook-web-restore",
		"start app-web",
		"start web-litestream",
		"start cf-web",
	})
	if !h.dk.running("web-litestream") {
		t.Fatal("the accessory should be running on the active host")
	}
}

// TestAnAdoptedActiveRestartsItsAccessory is the reason this feature exists:
// hooks do not run on adoption, so before the accessory reconcile an
// adopted-ACTIVE host served with replication silently down until the next
// failover.
func TestAnAdoptedActiveRestartsItsAccessory(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.dk.setRunning("cf-web", true, true) // we were serving when the old conch died
	h.arb.reply = alwaysYours

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("expected the adoption, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}
	// The adoption check itself takes nothing down and restores nothing (spec
	// §9's zero blip); the accessory comes back through the ACTIVE reconcile
	// on the next check, exactly like a stopped connector would.
	h.check()
	if !h.dk.running("web-litestream") {
		t.Fatalf("an adopted ACTIVE must restart its replication sidecar: %v", h.dk.calls())
	}
	for _, c := range h.dk.calls() {
		if c == "create conch-hook-web-restore" {
			t.Fatal("adoption must not restore; only the accessory comes back")
		}
	}
}

func TestActiveRestartsAStoppedAccessory(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.dk.setRunning("web-litestream", true, false) // crashed, or a stray docker stop
	h.check()
	if !h.dk.running("web-litestream") {
		t.Fatalf("the ACTIVE reconcile must restart a stopped accessory: %v", h.dk.calls())
	}
}

func TestDemotionStopsTheAccessoryWithItsOwnGrace(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter)

	if h.dk.running("web-litestream") {
		t.Fatal("a demoted host must not keep replicating: the peer's sidecar owns the replica now")
	}
	// The connector drains at its configured stop_timeout; the accessory
	// flushes at the 30s accessory grace — not docker's 10s default, and not
	// the connector's number.
	if got := h.dk.stopTimeout("web-litestream"); got != 30 {
		t.Fatalf("accessory stop grace: want 30, got %d", got)
	}
	if got := h.dk.stopTimeout("cf-web"); got != 35 {
		t.Fatalf("connector stop grace: want the configured 35, got %d", got)
	}
}

func TestStandbyStopsAnAccessorySomethingElseStarted(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	// The lease is held by the peer; a reboot's restart policy (or a human)
	// brought the replicator up here anyway.
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
	}
	h.dk.setRunning("web-litestream", true, true)

	h.check()
	if h.dk.running("web-litestream") {
		t.Fatalf("a standby must stop a running accessory — two replicators corrupt the replica: %v", h.dk.calls())
	}
}

func TestAMissingAccessoryContainerIsIneligible(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.dk.setRunning("web-litestream", false, false) // does not exist

	d := h.checks(4)
	if got := d["web"].State.OKStreak; got != 0 {
		t.Fatalf("a host that cannot replicate must not win a promotion, want streak 0, got %d (%s)",
			got, d["web"].Reason)
	}
	if h.arb.countOps("web", "renew") != 0 {
		t.Fatalf("an ineligible host must never claim: %v", h.arb.ops("web"))
	}
}

// TestAnUncountableAccessoryJustifiesNoAccessoryAction pins decide's nil
// contract end to end: an Inspect failure on the accessory must produce no
// accessory start or stop this check — in ACTIVE it also must not demote
// (eligibility is a standby question), so the service keeps serving while
// docker gets its act together.
func TestAnUncountableAccessoryJustifiesNoAccessoryAction(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.dk.setRunning("web-litestream", true, false) // down, and about to be unreadable
	h.dk.mu.Lock()
	h.dk.inspectErr["web-litestream"] = true
	h.dk.mu.Unlock()
	mark := len(h.dk.calls())

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("an uncountable accessory must not demote a serving host, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls()[mark:] {
		if c == "start web-litestream" || c == "stop web-litestream" {
			t.Fatalf("a reading docker could not give justifies no accessory action: %q", c)
		}
	}
}

func TestStatusReportsAccessories(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()
	h.check() // one more check measures the accessory the promotion started

	web := h.sup.Status()["web"].(map[string]any)
	want := map[string]any{"running": 1, "total": 1}
	if !reflect.DeepEqual(web["accessories"], want) {
		t.Fatalf("want accessories %v, got %v", want, web["accessories"])
	}

	// A service with no accessories has no entry at all: a 0/0 row on every
	// service would read like something is missing.
	plain := newHarness(t, testConfig("blog"), nil)
	if entry, ok := plain.sup.Status()["blog"].(map[string]any)["accessories"]; ok {
		t.Fatalf("a service without accessories must not report them, got %v", entry)
	}
}

// ---------------------------------------------------------------------------
// Kamal-style container-prefix resolution: a service container: written as
// "web-web-*" (a trailing "*") is resolved fresh every check against
// whichever concretely-named container currently matches, rather than
// matched by exact name — the fix for a deploy tool that appends a version
// suffix to the container name on every deploy.
// ---------------------------------------------------------------------------

// prefixTestConfig is testConfig with the service container rewritten as a
// prefix pattern and the health URL rewritten to use the {container}
// placeholder, the shape a Kamal-deployed service's config actually takes.
func prefixTestConfig(name string) config.Config {
	cfg := testConfig(name)
	cfg.Services[0].Container = "app-" + name + "-*"
	cfg.Services[0].Health.URL = "http://{container}:3000/up"
	return cfg
}

func TestPrefixContainerResolvesAndBecomesEligible(t *testing.T) {
	cfg := prefixTestConfig("web")
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-abc123")
	h.dk.setRunning("app-web-abc123", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(0)

	d := h.checks(4)
	if got := d["web"].State.OKStreak; got != 4 {
		t.Fatalf("a resolved prefix container should be eligible and healthy, got streak %d (%s)",
			got, d["web"].Reason)
	}
	if !containsInOrder(h.dk.calls(), []string{"resolve app-web-", "inspect app-web-abc123"}) {
		t.Fatalf("want the prefix resolved and the resolved name inspected, got %v", h.dk.calls())
	}
	if h.pr.countCalls("http://app-web-abc123:3000/up") == 0 {
		t.Fatalf("the {container} placeholder must substitute the RESOLVED name, calls: %v", h.pr.calls)
	}
}

// TestUnresolvedPrefixIsIneligible pins the no-match case: a prefix that
// matches nothing means the service container does not exist, which is the
// same ineligibility as today's missing-exact-name path — not an error, and
// never a claim.
func TestUnresolvedPrefixIsIneligible(t *testing.T) {
	h := newHarness(t, prefixTestConfig("web"), nil) // no resolution registered: no match
	h.arb.reply = freeThenGrant(0)

	d := h.checks(4)
	if got := d["web"].State.OKStreak; got != 0 {
		t.Fatalf("an unresolved prefix means the service container doesn't exist, want streak 0, got %d (%s)",
			got, d["web"].Reason)
	}
	if h.arb.countOps("web", "renew") != 0 {
		t.Fatalf("an ineligible host must never claim: %v", h.arb.ops("web"))
	}
}

// TestResolveByPrefixTransportErrorIsIneligibleNotACrash pins the other half
// of "absence is a fact, transport errors are errors": a daemon that could
// not even answer the resolve call must make the service ineligible, exactly
// like an Inspect failure would, not panic or claim on stale data.
func TestResolveByPrefixTransportErrorIsIneligibleNotACrash(t *testing.T) {
	h := newHarness(t, prefixTestConfig("web"), nil)
	h.dk.setResolutionErr("app-web-", errors.New("resolve: daemon unreachable"))
	h.arb.reply = freeThenGrant(0)

	d := h.checks(4)
	if got := d["web"].State.OKStreak; got != 0 {
		t.Fatalf("a resolve transport failure must be ineligible, want streak 0, got %d (%s)", got, d["web"].Reason)
	}
}

// TestMultipleRunningMatchesAlertOncePerStreak is the blue/green overlap
// window this exists to surface: more than one LIVE prefix match must not
// resolve silently — the supervisor alerts so an operator sees that
// resolution can flip rather than discovering it later as one. Selection
// itself is unaffected — dockerapi's own tests already pin that a single name
// is still chosen — this only checks the supervisor notices, reports, and
// then shuts up: an overlap that lasts (a stalled deploy) would otherwise
// alert every interval for as long as it lasts.
func TestMultipleRunningMatchesAlertOncePerStreak(t *testing.T) {
	h := newRig(t, prefixTestConfig("web"))
	h.dk.setResolution("app-web-", "app-web-new")
	h.dk.setRunning("app-web-new", true, true)
	h.dk.setResolutionRunning("app-web-", 2) // the overlap window: two live matches
	h.build(nil)
	h.arb.reply = freeThenGrant(0)

	h.check()

	overlaps := func() []event {
		var out []event
		for _, a := range h.nt.of("alert") {
			if a.payload["prefix"] == "app-web-" {
				out = append(out, a)
			}
		}
		return out
	}
	got := overlaps()
	if len(got) != 1 {
		t.Fatalf("want exactly one overlap alert, got %v", got)
	}
	if got[0].payload["running"] != 2 {
		t.Fatalf("want running:2 in the alert payload, got %+v", got[0].payload)
	}

	h.checks(3)
	if len(overlaps()) != 1 {
		t.Fatalf("the streak must alert once, not once per check, got %v", overlaps())
	}

	// The overlap ends, and a later one is news again.
	h.dk.setResolutionRunning("app-web-", 1)
	h.check()
	h.dk.setResolutionRunning("app-web-", 2)
	h.check()
	if len(overlaps()) != 2 {
		t.Fatalf("a new overlap after the count came back down must alert again, got %v", overlaps())
	}
}

// TestExitedPreviousVersionsDoNotAlert is the live-fleet non-regression: a
// host several Kamal deploys old has every retired version's container still
// sitting there, exited, next to the one live one. That is the ordinary
// steady state — dockerapi.ResolveByPrefix reports the RUNNING count for
// exactly this reason — and it must never page anyone.
func TestExitedPreviousVersionsDoNotAlert(t *testing.T) {
	h := newRig(t, prefixTestConfig("web"))
	h.dk.setResolution("app-web-", "app-web-abc123")
	h.dk.setRunning("app-web-abc123", true, true)
	h.dk.setResolutionRunning("app-web-", 1) // five exited siblings are invisible here
	h.build(nil)
	h.arb.reply = freeThenGrant(0)

	h.checks(3)

	for _, a := range h.nt.of("alert") {
		if a.payload["prefix"] == "app-web-" {
			t.Fatalf("one running container among exited previous versions must not alert, got %+v", a.payload)
		}
	}
}

// TestHealthPlaceholderSubstitutesExactContainerNameToo pins the cheaper
// half of the {container} feature: it works for an ordinary exact
// container: too, with no prefix pattern and no daemon-side resolution
// call at all.
func TestHealthPlaceholderSubstitutesExactContainerNameToo(t *testing.T) {
	cfg := testConfig("web")
	cfg.Services[0].Health.URL = "http://{container}:3000/up"
	h := newHarness(t, cfg, nil)
	h.arb.reply = freeThenGrant(0)

	h.check()
	if h.pr.countCalls("http://app-web:3000/up") == 0 {
		t.Fatalf("the {container} placeholder must substitute the exact container name, calls: %v", h.pr.calls)
	}
	for _, c := range h.dk.calls() {
		if strings.HasPrefix(c, "resolve ") {
			t.Fatalf("an exact container name must never call ResolveByPrefix, got %v", h.dk.calls())
		}
	}
}

// TestExecutorActsOnTheResolvedContainerName is TestFailoverEndToEnd's shape
// with a prefix-pattern container: the executor's StopService/StartService
// must use the RESOLVED name, never the "*" pattern literal.
func TestExecutorActsOnTheResolvedContainerName(t *testing.T) {
	cfg := prefixTestConfig("web")
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-abc123")
	h.dk.setRunning("app-web-abc123", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(1)

	h.checks(7)
	d := h.checks(1)
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("want ACTIVE, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}

	mustContainInOrder(t, h.dk.calls(), []string{
		"stop app-web-abc123",
		"create conch-hook-web-restore",
		"start app-web-abc123",
		"start cf-web",
	})
	for _, c := range h.dk.calls() {
		if strings.Contains(c, "app-web-*") {
			t.Fatalf("the executor must never act on the unresolved pattern, got %v", h.dk.calls())
		}
	}
}

// TestResolutionCanFlipBetweenTheStopAndStartChecks pins the actual,
// documented behavior of resolving fresh every check during a Kamal
// blue/green overlap, rather than leaving it as folklore: the check that
// issues StopService and the later check that issues StartService (once the
// restore hooks finish) each resolve independently. If Kamal finishes
// retiring the old container and promoting the new one WHILE the restore is
// still running, the stop-issuing check acts on the old name and the
// start-issuing check acts on the new one — conch never stops the container
// it ends up starting. This is a direct, deliberate consequence of "prefer
// running, then newest, resolved fresh every check" (dockerapi.
// ResolveByPrefix's own contract) and the deploy-tool overlap window is
// exactly when it can happen.
func TestResolutionCanFlipBetweenTheStopAndStartChecks(t *testing.T) {
	cfg := prefixTestConfig("web")
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-aaaa") // the older container, running: about to be retired
	h.dk.setRunning("app-web-aaaa", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(1)

	// Block the restore hook's Wait so the promotion's stop-and-restore check
	// and its later start check can be observed as two separate,
	// independently-resolved checks instead of both landing before either can
	// be inspected.
	waitGate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.waitGate["conch-hook-web-restore"] = waitGate
	h.dk.mu.Unlock()

	h.checks(6) // batch 0 held, batches 1..5 free: not yet claimable
	mark := len(h.dk.calls())
	h.checkNoWait() // batch 6: the claim lands — PROMOTING, stops app-web-aaaa, starts the restore
	awaitGate(t, h.dk.waitHit, "the restore to start")

	if !containsInOrder(h.dk.calls()[mark:], []string{"stop app-web-aaaa"}) {
		t.Fatalf("the stop-issuing check must act on the container resolved AT THAT CHECK: %v",
			h.dk.calls()[mark:])
	}

	// Kamal finishes its deploy while the restore is still running: the
	// older container this check just stopped exits for good, and a newer
	// one becomes the prefix's only running match.
	h.dk.setRunning("app-web-aaaa", true, false)
	h.dk.setResolution("app-web-", "app-web-bbbb")
	h.dk.setRunning("app-web-bbbb", true, true)

	close(waitGate)
	h.waitIdle()
	d := h.check() // the restore is done: StartService/StartConnector, resolved fresh
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("expected ACTIVE once the restore finished, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}

	// The documented behavior, not folklore: the start-issuing check acts on
	// WHATEVER the prefix resolves to at THAT check — the newer container —
	// even though it is not the one this promotion's own StopService stopped
	// a moment earlier.
	if !containsInOrder(h.dk.calls(), []string{"start app-web-bbbb"}) {
		t.Fatalf("want the start on the newly-resolved container, got %v", h.dk.calls())
	}
	for _, c := range h.dk.calls() {
		if c == "start app-web-aaaa" {
			t.Fatalf("the older container that was stopped must never be the one started: %v", h.dk.calls())
		}
	}
}

// ---------------------------------------------------------------------------
// {container} in promote-hook argv: the same substitution readings applies
// to health.url, applied to a different consumer — the promote hook chain's
// Argv, so a hook like `kamal-proxy deploy web --target {container}` can
// re-register the freshly resolved app container (the incident this task's
// RCA plan traces back to).
// ---------------------------------------------------------------------------

// promoteArgvTestConfig is cfg with the first service's one promote hook's
// Argv replaced by argv, everything else exactly testConfig's shape.
func promoteArgvTestConfig(cfg config.Config, argv []string) config.Config {
	cfg.Services[0].Hooks.Promote[0].Argv = argv
	return cfg
}

// TestPromoteHookArgvSubstitutesExactContainerName pins the cheap half:
// {container} in a promote hook's Argv becomes the exact configured
// container name, with no prefix pattern and no daemon-side resolution call
// at all — the same split TestHealthPlaceholderSubstitutesExactContainerNameToo
// pins for health.url.
func TestPromoteHookArgvSubstitutesExactContainerName(t *testing.T) {
	cfg := promoteArgvTestConfig(testConfig("web"),
		[]string{"restore", "--target", "{container}"})
	h := newHarness(t, cfg, nil)
	h.driveToActive()

	got := h.dk.argvOf("conch-hook-web-restore")
	want := []string{"restore", "--target", "app-web"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want the promote hook argv substituted with the exact container name %v, got %v", want, got)
	}
}

// TestPromoteHookArgvSubstitutesPrefixResolvedContainerName is
// TestPrefixContainerResolvesAndBecomesEligible's shape for hook argv rather
// than health.url: a Kamal-versioned service container resolved fresh THIS
// check (supervisor.go's resolveContainer) is the name the promote hook's
// {container} must carry — the exact fix the RCA traces back to: a
// kamal-proxy re-registration hook that needs the freshly resolved app
// container, not the "*" pattern literal.
func TestPromoteHookArgvSubstitutesPrefixResolvedContainerName(t *testing.T) {
	cfg := promoteArgvTestConfig(prefixTestConfig("web"),
		[]string{"kamal-proxy", "deploy", "web", "--target", "{container}"})
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-abc123")
	h.dk.setRunning("app-web-abc123", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(1)

	h.checks(7)
	d := h.checks(1)
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("want ACTIVE, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}

	got := h.dk.argvOf("conch-hook-web-restore")
	want := []string{"kamal-proxy", "deploy", "web", "--target", "app-web-abc123"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want the promote hook argv substituted with the RESOLVED container name %v, got %v", want, got)
	}
}

// TestPromoteHookArgvUsesThisChecksResolution pins the property that makes
// resolvedContainer worth threading through apply()/runPromoteHooks as a
// parameter rather than reading it back off the unit or the daemon later:
// the hook argv must carry the name resolved by THIS check's own readings
// pass — the same snapshot StopService acted on a moment earlier — even if
// the daemon's live resolution moves on before the hook's container is
// actually created. It is TestResolutionCanFlipBetweenTheStopAndStartChecks'
// shape, gated one step earlier (on the service stop rather than the
// restore's Wait) so the mutation lands between "this check resolved a
// name" and "the hook chain used it."
func TestPromoteHookArgvUsesThisChecksResolution(t *testing.T) {
	cfg := promoteArgvTestConfig(prefixTestConfig("web"),
		[]string{"kamal-proxy", "deploy", "web", "--target", "{container}"})
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-aaaa") // about to be retired
	h.dk.setRunning("app-web-aaaa", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(1)

	stopGate := make(chan struct{})
	h.dk.mu.Lock()
	h.dk.gate["app-web-aaaa"] = stopGate
	h.dk.mu.Unlock()

	h.checks(6)     // batch 0 held, batches 1..5 free: not yet claimable
	h.checkNoWait() // batch 6: claims, PROMOTING, StopService blocks on the gate
	awaitGate(t, h.dk.gateHit, "the service stop")

	// While THIS check's own StopService is still in flight, the daemon's
	// resolution moves on — Kamal retires app-web-aaaa and promotes a new
	// container, exactly the blue/green overlap TestResolutionCanFlipBetween
	// TheStopAndStartChecks exercises for StartService. It must not change
	// what THIS check's hook argv substitutes: runPromoteHooks never re-reads
	// the daemon, only the resolvedContainer parameter apply() already froze.
	h.dk.setRunning("app-web-aaaa", true, false)
	h.dk.setResolution("app-web-", "app-web-bbbb")
	h.dk.setRunning("app-web-bbbb", true, true)

	close(stopGate)
	h.waitIdle()

	got := h.dk.argvOf("conch-hook-web-restore")
	want := []string{"kamal-proxy", "deploy", "web", "--target", "app-web-aaaa"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the promote hook argv must use THIS check's resolved name, not a later one: want %v, got %v", want, got)
	}
}

// TestPromoteHookArgvWithNoPlaceholderIsByteIdentical is the non-regression
// every existing hook config (nothing in this repo's shipped examples uses
// {container} in argv today) must clear: a hook with no placeholder reaches
// docker with its Argv untouched.
func TestPromoteHookArgvWithNoPlaceholderIsByteIdentical(t *testing.T) {
	want := []string{"restore", "--from", "s3://backups/web/latest"}
	cfg := promoteArgvTestConfig(testConfig("web"), want)
	h := newHarness(t, cfg, nil)
	h.driveToActive()

	got := h.dk.argvOf("conch-hook-web-restore")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a placeholder-free argv must reach docker byte-identical, want %v, got %v", want, got)
	}
}

// TestSubstitutePromoteArgvPreservesNilArgv is substitutePromoteArgv's own
// unit test, for the one thing a full-flow test can't see:
// fakeDocker.Create normalizes both nil and empty Argv to nil on its way into
// the call log, so TestPromoteHookArgvWithNoPlaceholderIsByteIdentical cannot
// tell "left nil" apart from "promoted to a non-nil empty slice." A hook with
// no Argv at all (defensive: unreachable via Validate-passed configs, since
// validateHooks faults empty Argv for both phases) must come out with Argv
// still nil, not make([]string, 0): dockerapi.RunSpec.Argv reaches the
// Engine API's Cmd field, where nil and an empty-but-present array are not
// always interchangeable.
//
// It also pins the input-not-mutated half of substitutePromoteArgv's own doc
// comment: the caller's specs slice (and its Argv slices) must read back
// exactly as passed, since HookSpec.Argv is the service's own config, read
// again on every future promotion.
func TestSubstitutePromoteArgvPreservesNilArgv(t *testing.T) {
	specs := []config.HookSpec{
		{Name: "restore", Image: "img"},                                           // Argv: nil
		{Name: "register", Image: "img", Argv: []string{"deploy", "{container}"}}, // Argv: substituted
	}
	before := append([]string(nil), specs[1].Argv...)

	out := substitutePromoteArgv(specs, "app-web-abc123")

	if out[0].Argv != nil {
		t.Fatalf("a hook with no Argv must stay nil, got %#v", out[0].Argv)
	}
	if !reflect.DeepEqual(specs[1].Argv, before) {
		t.Fatalf("the input specs must be left untouched, want %v, got %v", before, specs[1].Argv)
	}
}

// ---------------------------------------------------------------------------
// Loop mechanics beyond the brief's list
// ---------------------------------------------------------------------------

func TestFreshReadingsVetoAStaleRenew(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	h.checkUntilReadyToClaim("web") // the next check would carry the claim
	next := h.arb.nBatches()
	h.dk.setRunning("app-web", false, false)
	h.check()

	if got := h.arb.opAt(next, "web"); got != "read" {
		t.Fatalf("this check's own readings must stop a claim the streak had earned, got %q", got)
	}
	if h.arb.countOps("web", "renew") != 0 {
		t.Fatalf("no claim may be sent while ineligible: %v", h.arb.ops("web"))
	}
}

func TestArbiterLossFreezesAndAlertsOnce(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.arb.mu.Lock()
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.arb.mu.Unlock()

	before := len(h.dk.calls())
	d := h.checks(4)
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("freeze holds shape while the arbiter is silent, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls()[before:] {
		if c == "stop cf-web" {
			t.Fatal("freeze must not stop a healthy, serving connector")
		}
	}
	if got := h.arb.countOps("web", "renew"); got < 4 {
		t.Fatalf("conch keeps trying while the arbiter is silent, got %d renews", got)
	}
}

// ---------------------------------------------------------------------------
// The promote/demote webhook kinds (spec §11): the transition webhook is how
// a human learns a failover happened at all. See notifyTransition's own
// comment in supervisor.go for the exact rule and why an evacuating unit is
// excluded (evacuate_test.go pins the no-double-emit case).
// ---------------------------------------------------------------------------

func TestPromoteWebhookFiresOnceOnAFreshClaim(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	got := h.nt.of("promote")
	if len(got) != 1 {
		t.Fatalf("want exactly one promote event for the Standby->Promoting->Active claim, got %d: %v",
			len(got), got)
	}
	p := got[0].payload
	if p["host"] != "host-a" || p["service"] != "web" {
		t.Fatalf("promote payload missing host/service: %+v", p)
	}
	if p["lease"] != string(decide.Yours) {
		t.Fatalf("promote payload's lease state should be %q, got %+v", decide.Yours, p)
	}
	if _, ok := p["reason"]; !ok {
		t.Fatalf("promote payload missing reason: %+v", p)
	}
	if _, ok := p["wall"]; !ok {
		t.Fatalf("promote payload missing a wall timestamp: %+v", p)
	}
	if len(h.nt.of("demote")) != 0 {
		t.Fatalf("a fresh claim must never also fire demote: %v", h.nt.of("demote"))
	}
}

// TestPromoteWebhookFiresOnAResumeWithoutHooks covers the other shape spec
// §5 promotes without ever visiting PROMOTING: a restart over a lease that
// is still ours resumes straight from Standby to Active (decideOne's
// reconciling branch, see TestARestartOverALiveServiceResumesActiveWithout-
// Restoring). The brief names Standby *and* Promoting as promote's source
// phases for exactly this case.
func TestPromoteWebhookFiresOnAResumeWithoutHooks(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.dk.setRunning("cf-web", true, true) // we were serving when the old conch died
	h.arb.reply = alwaysYours

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("the resume should land straight on Active, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}

	got := h.nt.of("promote")
	if len(got) != 1 {
		t.Fatalf("want exactly one promote event for the resume, got %d: %v", len(got), got)
	}
	if len(h.nt.of("demote")) != 0 {
		t.Fatalf("a resume must never also fire demote: %v", h.nt.of("demote"))
	}
}

func TestDemoteWebhookFiresOnAnAbandonedPromotion(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.dk.mu.Lock()
	h.dk.waitCode["conch-hook-web-restore"] = 1 // the restore exits non-zero
	h.dk.mu.Unlock()

	h.checks(h.cfg.Timing.PromoteAfter) // claim granted, hooks run and fail
	h.check()                           // Promoting -> Standby, release owed

	got := h.nt.of("demote")
	if len(got) != 1 {
		t.Fatalf("want exactly one demote event for the abandoned promotion, got %d: %v", len(got), got)
	}
	p := got[0].payload
	if p["host"] != "host-a" || p["service"] != "web" {
		t.Fatalf("demote payload missing host/service: %+v", p)
	}
	if _, ok := p["reason"]; !ok {
		t.Fatalf("demote payload missing reason: %+v", p)
	}
	if _, ok := p["wall"]; !ok {
		t.Fatalf("demote payload missing a wall timestamp: %+v", p)
	}
	if len(h.nt.of("promote")) != 0 {
		t.Fatalf("an abandoned promotion never reached Active: no promote should have fired: %v",
			h.nt.of("promote"))
	}

	// The retried release re-sends on later checks with the phase already
	// Standby; that must not re-fire demote.
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if got := h.nt.of("demote"); len(got) != 1 {
		t.Fatalf("a release retry holding the same phase must not re-fire demote, got %d: %v", len(got), got)
	}
}

// TestPromoteTransitionLogsAtInfo pins the RCA fix: notifyTransition's webhook
// was the only signal a promote ever produced, so a 4-service failover with a
// dead or misconfigured webhook printed nothing to the log at all. The
// transition must also be said at Info, independent of whether notify's HTTP
// call ever lands.
func TestPromoteTransitionLogsAtInfo(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	got := handler.of("promote")
	if len(got) != 1 {
		t.Fatalf("want exactly one promote log record for the Standby->Promoting->Active claim, got %d: %+v",
			len(got), got)
	}
	if got[0].level != slog.LevelInfo {
		t.Fatalf("the promote transition must log at Info, got %v", got[0].level)
	}
	if got[0].attrs["service"] != "web" {
		t.Fatalf("promote record missing service attr: %+v", got[0].attrs)
	}
	if len(handler.of("demote")) != 0 {
		t.Fatalf("a fresh claim must never also log demote: %+v", handler.of("demote"))
	}
}

// TestDemoteTransitionLogsAtInfo is TestPromoteTransitionLogsAtInfo's demote
// half, driven the same way TestDemoteWebhookFiresOnAnAbandonedPromotion is.
func TestDemoteTransitionLogsAtInfo(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = freeThenGrant(0)
	h.dk.mu.Lock()
	h.dk.waitCode["conch-hook-web-restore"] = 1 // the restore exits non-zero
	h.dk.mu.Unlock()

	h.checks(h.cfg.Timing.PromoteAfter) // claim granted, hooks run and fail
	h.check()                           // Promoting -> Standby, release owed

	got := handler.of("demote")
	if len(got) != 1 {
		t.Fatalf("want exactly one demote log record for the abandoned promotion, got %d: %+v",
			len(got), got)
	}
	if got[0].level != slog.LevelInfo {
		t.Fatalf("the demote transition must log at Info, got %v", got[0].level)
	}
	if got[0].attrs["service"] != "web" {
		t.Fatalf("demote record missing service attr: %+v", got[0].attrs)
	}
	if len(handler.of("promote")) != 0 {
		t.Fatalf("an abandoned promotion never reached Active: no promote should have logged: %+v",
			handler.of("promote"))
	}

	// The retried release re-sends on later checks with the phase already
	// Standby; that must not re-log demote (same no-double-log rule the
	// webhook has — notifyTransition's default: return on a held phase).
	h.checks(h.cfg.Timing.PromoteAfter + 2)
	if got := handler.of("demote"); len(got) != 1 {
		t.Fatalf("a release retry holding the same phase must not re-log demote, got %d: %+v", len(got), got)
	}
}

func TestStatusIsASnapshot(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	st := h.sup.Status()
	web, ok := st["web"].(map[string]any)
	if !ok {
		t.Fatalf("status must carry every service: %v", st)
	}
	for _, field := range []string{"phase", "lease", "healthy", "reason"} {
		if _, ok := web[field]; !ok {
			t.Fatalf("status is missing %q: %v", field, web)
		}
	}
	if web["phase"] != string(decide.Active) || web["lease"] != string(decide.Yours) {
		t.Fatalf("status does not match the machine: %v", web)
	}
	if web["healthy"] != true {
		t.Fatalf("status health is wrong: %v", web)
	}
}

func TestStatusIsRaceFreeAgainstTheLoop(t *testing.T) {
	h := newHarness(t, testConfig("web", "blog"), nil)
	h.arb.reply = freeThenGrant(0)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = h.sup.Status()
			}
		}
	}()
	h.checks(10)
	close(done)
	wg.Wait()
}

func TestRunForeverReturnsOnContextCancel(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	ctx, cancel := context.WithCancel(context.Background())
	h.arb.onBatch = func(check int) {
		if check == 2 {
			cancel()
		}
	}

	h.sup.RunForever(ctx) // returns, or the test times out

	if got := len(h.arb.ops("web")); got != 3 {
		t.Fatalf("the loop should stop at the cancelled context, ran %d checks", got)
	}
	// The executors are the loop's, and they must not outlive it.
	ex := h.sup.units["web"].ex
	ex.mu.Lock()
	closed := ex.closed
	ex.mu.Unlock()
	if !closed {
		t.Fatal("RunForever must close its executors on the way out, or every embedding leaks goroutines")
	}
}

func TestMoreServicesThanTheBatchCapIsRefused(t *testing.T) {
	names := make([]string, 0, maxBatchServices+1)
	for i := 0; i <= maxBatchServices; i++ {
		names = append(names, fmt.Sprintf("svc%02d", i))
	}
	cfg := testConfig(names...)
	_, err := New(Options{
		Config: cfg, Arbiter: &fakeArbiter{clk: clock.NewFake()}, Docker: newFakeDocker(),
		Hooks: hooks.NewRunner(newFakeDocker(), clock.NewFake(), slog.New(slog.DiscardHandler)), Clock: clock.NewFake(),
	})
	if err == nil {
		t.Fatal("more services than the arbiter's batch cap must be refused at New, not silently truncated")
	}
	if !strings.Contains(err.Error(), "64") {
		t.Fatalf("the error should name the cap, got %v", err)
	}
}

func TestDiskFloorGatesEligibility(t *testing.T) {
	cfg := testConfig("web")
	cfg.Disk = config.DiskConfig{Path: "/var/lib/docker", MinFreeGB: 10}
	h := newHarness(t, cfg, func(o *Options) {
		o.DiskFreeGB = func(string) (float64, error) { return 2, nil }
	})
	h.arb.reply = freeThenGrant(0)

	d := h.checks(4)
	if d["web"].State.OKStreak != 0 {
		t.Fatalf("a host below the disk floor is not eligible, streak %d", d["web"].State.OKStreak)
	}
}

func TestUnmeasurableDiskIsNotHealthy(t *testing.T) {
	cfg := testConfig("web")
	cfg.Disk = config.DiskConfig{Path: "/nope", MinFreeGB: 10}
	h := newHarness(t, cfg, func(o *Options) {
		o.DiskFreeGB = func(string) (float64, error) { return 0, errors.New("no such path") }
	})
	h.arb.reply = freeThenGrant(0)

	d := h.checks(2)
	if d["web"].State.OKStreak != 0 {
		t.Fatalf("a disk floor conch cannot measure is a floor it cannot clear, streak %d", d["web"].State.OKStreak)
	}
	if len(h.nt.of("alert")) == 0 {
		t.Fatal("an unmeasurable disk floor must be reported")
	}
}

// TestUnmeasurableDiskAlertsOncePerStreak is the noise half of the same
// fact: a disk.path that does not exist on this host is a config typo, and
// it is unmeasurable on EVERY check for as long as the typo lives — so an
// alert per check is one webhook every interval, forever, about a condition
// that has not changed since the first one. Once per streak, like the
// arbiter-unreachable and prefix-overlap warnings; a floor that measures
// again and then breaks again is genuinely new, and says so.
func TestUnmeasurableDiskAlertsOncePerStreak(t *testing.T) {
	broken := true
	cfg := testConfig("web")
	cfg.Disk = config.DiskConfig{Path: "/nope", MinFreeGB: 10}
	h := newHarness(t, cfg, func(o *Options) {
		o.DiskFreeGB = func(string) (float64, error) {
			if broken {
				return 0, errors.New("no such path")
			}
			return 500, nil
		}
	})
	h.arb.reply = freeThenGrant(0)

	diskAlerts := func() []event {
		var out []event
		for _, a := range h.nt.of("alert") {
			if a.payload["path"] == "/nope" {
				out = append(out, a)
			}
		}
		return out
	}

	h.checks(3)
	got := diskAlerts()
	if len(got) != 1 {
		t.Fatalf("want exactly 1 disk alert across 3 unmeasurable checks, got %d: %+v", len(got), got)
	}

	broken = false
	h.check()
	if len(diskAlerts()) != 1 {
		t.Fatalf("a floor that measures again is not an alert, got %+v", diskAlerts())
	}

	broken = true
	h.checks(2)
	if len(diskAlerts()) != 2 {
		t.Fatalf("breaking again after recovering is news, want 2 alerts, got %+v", diskAlerts())
	}
}

// ---------------------------------------------------------------------------
// health.go
// ---------------------------------------------------------------------------

func TestProbeDoesNotFollowRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusMovedPermanently)
	}))
	defer sick.Close()

	p := newHTTPProber()
	if p.Probe(sick.URL, "", 2, []int{200}) {
		t.Fatal("a 301 to a working page must not mask a sick service (spec §4)")
	}
	if !p.Probe(sick.URL, "", 2, []int{301}) {
		t.Fatal("a redirect is an answer: an operator who expects 301 gets 301")
	}
	if !p.Probe(target.URL, "", 2, []int{200}) {
		t.Fatal("a healthy service answers healthy")
	}
}

func TestProbeIsBoundedByItsTimeout(t *testing.T) {
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer server.Close()
	defer close(block)

	p := newHTTPProber()
	if p.Probe(server.URL, "", 0.05, []int{200}) {
		t.Fatal("a server that never answers is not healthy")
	}
}

func TestProbeOfAnUnconfiguredURLIsNotAFailure(t *testing.T) {
	p := newHTTPProber()
	if !p.Probe("", "", 1, []int{200}) {
		t.Fatal("a service with no health URL is not thereby unhealthy")
	}
}

// TestProbeCarriesConfiguredHost pins A6: a probe's Host header, when
// configured, is what the server actually sees — req.Host, not a header-map
// entry net/http silently ignores. The URL still addresses wherever the
// proxy actually listens (here, the httptest server's own address); Host is
// what tells that proxy which vhost to route on, the same split kamal-proxy
// and any shared reverse proxy in front of the app container require.
func TestProbeCarriesConfiguredHost(t *testing.T) {
	var gotHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p := newHTTPProber()
	if !p.Probe(server.URL, "app.internal.example", 2, []int{200}) {
		t.Fatal("a healthy service behind the configured vhost answers healthy")
	}
	if gotHost != "app.internal.example" {
		t.Fatalf("want the server to see Host %q, got %q", "app.internal.example", gotHost)
	}
}

// TestProbeUnsetHostIsByteIdenticalToBeforeThisField pins the other half:
// an unconfigured Host must not perturb the request at all. The server sees
// exactly what it always saw — the URL's own host:port — because req.Host
// stays its zero value and the transport falls back to the URL.
func TestProbeUnsetHostIsByteIdenticalToBeforeThisField(t *testing.T) {
	var gotHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p := newHTTPProber()
	if !p.Probe(server.URL, "", 2, []int{200}) {
		t.Fatal("a healthy service answers healthy")
	}
	wantHost := strings.TrimPrefix(server.URL, "http://")
	if gotHost != wantHost {
		t.Fatalf("an unset Host must leave the request exactly as before, want %q got %q", wantHost, gotHost)
	}
}

// TestHealthProbeAppliesHostWithStaticURL pins A6's wiring at the
// supervisor level for a plain, non-placeholder health.url: the configured
// Host reaches the prober call unchanged, alongside the static URL.
func TestHealthProbeAppliesHostWithStaticURL(t *testing.T) {
	cfg := testConfig("web")
	cfg.Services[0].Health.Host = "app.internal.example"
	h := newHarness(t, cfg, nil)
	h.arb.reply = freeThenGrant(0)

	h.check()
	if got := h.pr.hostFor("http://app-web/up"); got != "app.internal.example" {
		t.Fatalf("want the health probe called with the configured Host, got %q", got)
	}
}

// TestHealthProbeAppliesHostWithContainerPlaceholder is the {container}
// counterpart: the URL substitution (strings.ReplaceAll in readings) and the
// Host override are independent — a service using the Kamal-style prefix
// placeholder still gets its configured Host on every probe.
func TestHealthProbeAppliesHostWithContainerPlaceholder(t *testing.T) {
	cfg := prefixTestConfig("web")
	cfg.Services[0].Health.Host = "app.internal.example"
	h := newRig(t, cfg)
	h.dk.setResolution("app-web-", "app-web-abc123")
	h.dk.setRunning("app-web-abc123", true, true)
	h.build(nil)
	h.arb.reply = freeThenGrant(0)

	h.check()
	if got := h.pr.hostFor("http://app-web-abc123:3000/up"); got != "app.internal.example" {
		t.Fatalf("want the resolved-container URL probed with the configured Host, got %q", got)
	}
}

// TestReadyURLProbeNeverReceivesHost pins the brief's explicit scope line:
// health.host reaches only the health probe. The connector's ready_url
// call — decide's rule-12 readiness fold — is a second, independent call to
// the same prober, and it must keep getting "" even when this service's
// health.host is configured, because the brief scopes Host to the health
// check only and a shared proxy in front of the app is not the same thing
// as the connector's own readiness endpoint.
func TestReadyURLProbeNeverReceivesHost(t *testing.T) {
	cfg := testConfig("web")
	cfg.Services[0].Health.Host = "app.internal.example"
	h := newHarness(t, cfg, nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	if h.pr.countCalls("http://cf-web:2000/ready") == 0 {
		t.Fatal("the ready probe must actually have run for this pin to mean anything")
	}
	if got := h.pr.hostFor("http://cf-web:2000/ready"); got != "" {
		t.Fatalf("ready_url is out of scope for health.host, got Host %q", got)
	}
	if got := h.pr.hostFor("http://app-web/up"); got != "app.internal.example" {
		t.Fatalf("meanwhile the health probe itself must still carry the configured Host, got %q", got)
	}
}

// TestStatusReportsPeerPresence pins what a live evacuation refusal needed
// and did not have: spec §9's pre-flight refuses when no peer has checked in
// recently, and until this key existed there was no way to see the presence
// data it read. It is the arbiter's own map, verbatim — the same one
// preflightLocked consults — sitting at the top level next to the services.
func TestStatusReportsPeerPresence(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)

	// Before any batch has landed there is no presence data, and an absent
	// map must read as an empty one rather than a null an operator has to
	// tell apart from "asked and nobody was there".
	peers, ok := h.sup.Status()["peers"].(map[string]float64)
	if !ok {
		t.Fatalf("status must always carry a peers map, got %#v", h.sup.Status()["peers"])
	}
	if len(peers) != 0 {
		t.Fatalf("no batch has landed yet, want no peers, got %v", peers)
	}

	h.arb.setPeers(map[string]float64{"host-b": 2.5})
	h.check()

	peers, _ = h.sup.Status()["peers"].(map[string]float64)
	if peers["host-b"] != 2.5 {
		t.Fatalf("want host-b last seen 2.5s ago, got %v", peers)
	}
	if _, ok := h.sup.Status()["web"]; !ok {
		t.Fatalf("the per-service entries must still be there: %v", h.sup.Status())
	}

	// A copy, not the loop's own map: a reader must not be able to mutate
	// what the pre-flight reads.
	peers["host-b"] = 999
	if again, _ := h.sup.Status()["peers"].(map[string]float64); again["host-b"] != 2.5 {
		t.Fatalf("status handed out its own live map, got %v", again)
	}
}

// ---------------------------------------------------------------------------
// Warm accessories: containers that run on EVERY host whatever the lease says
// — a service's own database above all. The standby's copy is a restore
// target, not a replica anything ships to, so keeping it up costs nothing and
// buys the incident this feature exists to end: a deploy against the standby
// fails when the database it builds against is not running there.
//
// Every test here is one half of a single sentence: conch STARTS a warm
// accessory it finds down, anywhere, in any phase — and no conch path ever
// stops one.
// ---------------------------------------------------------------------------

// warmTestConfig is accessoryTestConfig plus a warm accessory, which is the
// real shape: the litestream shipper follows the lease, the database it ships
// does not.
func warmTestConfig(name string) config.Config {
	cfg := accessoryTestConfig(name)
	cfg.Services[0].WarmAccessories = []string{name + "-db"}
	return cfg
}

// warmActions is every start or stop the fake was asked for against name.
func warmActions(h *harness, name string) []string {
	var out []string
	for _, c := range h.dk.calls() {
		if c == "start "+name || c == "stop "+name || c == "kill "+name {
			out = append(out, c)
		}
	}
	return out
}

// TestAStandbyStartsADownedWarmAccessory is the root-cause fix: the peer holds
// the lease and this host is standing by, which is exactly where the old code
// left the database stopped and every deploy against this host failing.
func TestAStandbyStartsADownedWarmAccessory(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
	}

	d := h.check()
	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("this host must be standing by: %s", d["web"].State.Phase)
	}
	if !h.dk.running("web-db") {
		t.Fatalf("a standby must run its warm accessory: %v", h.dk.calls())
	}
	// And the lease-following one stays down here, which is the whole reason
	// the two lists are separate.
	if h.dk.running("web-litestream") {
		t.Fatal("a standby must not run the shipper: two shippers corrupt the replica")
	}

	// It stays up without being started again every check: a container that is
	// already running is not down, and re-starting it forever would be a daemon
	// call per interval for no change in the world.
	mark := len(h.dk.calls())
	h.checks(3)
	for _, c := range h.dk.calls()[mark:] {
		if c == "start web-db" {
			t.Fatalf("a running warm accessory must not be started again: %v", h.dk.calls()[mark:])
		}
	}
}

// TestADownedWarmAccessoryIsRestartedInEveryPhase walks the phases a host
// passes through and pins that each one restarts it: the reconcile is the
// supervisor's, not decide's, so no phase can opt out.
func TestADownedWarmAccessoryIsRestartedInEveryPhase(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// ACTIVE.
	h.dk.setRunning("web-db", true, false)
	h.check()
	if !h.dk.running("web-db") {
		t.Fatalf("an ACTIVE host must restart its warm accessory: %v", h.dk.calls())
	}

	// Demoted back to STANDBY, warm accessory knocked over again.
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)
	if h.sup.Status()["web"].(map[string]any)["phase"] != string(decide.Standby) {
		t.Fatalf("expected a demotion by now: %v", h.sup.Status()["web"])
	}
	h.dk.setRunning("web-db", true, false)
	h.check()
	if !h.dk.running("web-db") {
		t.Fatalf("a demoted host must restart its warm accessory: %v", h.dk.calls())
	}
}

// TestAnAdoptedActiveStartsItsWarmAccessory: adoption runs no hooks, so a
// database left down by whatever killed the previous conch has nothing else
// to bring it back.
func TestAnAdoptedActiveStartsItsWarmAccessory(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.dk.setRunning("cf-web", true, true) // we were serving when the old conch died
	h.arb.reply = alwaysYours

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("expected the adoption, got %s (%s)", d["web"].State.Phase, d["web"].Reason)
	}
	if !h.dk.running("web-db") {
		t.Fatalf("an adopted ACTIVE must start its warm accessory: %v", h.dk.calls())
	}
}

// TestDemotionStopsTheShipperButNeverTheWarmAccessory is the invariant the
// whole feature rests on, from the side that used to break it.
func TestDemotionStopsTheShipperButNeverTheWarmAccessory(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()
	mark := len(h.dk.calls())

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 2)

	if h.dk.running("web-litestream") {
		t.Fatal("a demoted host must stop the shipper: the peer's shipper owns the replica now")
	}
	if !h.dk.running("web-db") {
		t.Fatalf("a demotion must not take the database down: %v", h.dk.calls()[mark:])
	}
	for _, c := range h.dk.calls()[mark:] {
		if c == "stop web-db" || c == "kill web-db" {
			t.Fatalf("no conch path may stop a warm accessory, got %q in %v", c, h.dk.calls()[mark:])
		}
	}
}

// TestAStandbyNeverStopsAWarmAccessoryItFindsRunning is the fencing case: a
// STANDBY stops any shipper it finds running, however it got started, and must
// leave the warm accessory exactly where it is.
func TestAStandbyNeverStopsAWarmAccessoryItFindsRunning(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Barred, Holder: "host-b"}
	}
	h.dk.setRunning("web-litestream", true, true)
	h.dk.setRunning("web-db", true, true)

	h.checks(3)

	if h.dk.running("web-litestream") {
		t.Fatalf("a barred standby must stop the shipper: %v", h.dk.calls())
	}
	if got := warmActions(h, "web-db"); len(got) != 0 {
		t.Fatalf("a running warm accessory needs no action at all, got %v", got)
	}
}

// TestPromotionWithTheWarmAccessoryAlreadyUpIsClean: the ordinary case in
// production, since the standby has been running it all along. The promotion
// must not restart it, must not stop it, and must not be delayed by it.
func TestPromotionWithTheWarmAccessoryAlreadyUpIsClean(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.dk.setRunning("web-db", true, true)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	if got := warmActions(h, "web-db"); len(got) != 0 {
		t.Fatalf("a warm accessory that was already up needs no action, got %v", got)
	}
	// The rest of the promotion is untouched: restore, service, shipper, then
	// traffic.
	mustContainInOrder(t, h.dk.calls(), []string{
		"create conch-hook-web-restore",
		"start app-web",
		"start web-litestream",
		"start cf-web",
	})
}

// TestAMissingWarmAccessoryContainerIsIneligible: a host whose database
// container does not exist cannot serve this service, so it must not win a
// promotion — the same ruling as a missing shipper, for the same reason.
func TestAMissingWarmAccessoryContainerIsIneligible(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.dk.setRunning("web-db", false, false) // does not exist

	d := h.checks(4)
	if got := d["web"].State.OKStreak; got != 0 {
		t.Fatalf("a host missing its database must not win a promotion, want streak 0, got %d (%s)",
			got, d["web"].Reason)
	}
	if h.arb.countOps("web", "renew") != 0 {
		t.Fatalf("an ineligible host must never claim: %v", h.arb.ops("web"))
	}
	// And conch does not spend a daemon call, a log line and an alert every
	// interval starting a container that is not there. Absence is what
	// eligibility reports; a start would only fail, loudly, forever.
	if got := warmActions(h, "web-db"); len(got) != 0 {
		t.Fatalf("a container that does not exist cannot be started, got %v", got)
	}
	if got := len(h.nt.of("alert")); got != 0 {
		t.Fatalf("a missing warm accessory must not page anyone every check, got %d alerts", got)
	}
}

// TestAnUnreadableWarmAccessoryJustifiesNoWarmAction is the nil contract, warm
// side: an Inspect that failed is not evidence of anything, so no warm start
// this check — and, because the two lists are measured separately, the
// SHIPPER's own reading is untouched by it.
func TestAnUnreadableWarmAccessoryJustifiesNoWarmAction(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.dk.setRunning("web-db", true, false)         // down, and about to be unreadable
	h.dk.setRunning("web-litestream", true, false) // down, and perfectly readable
	h.dk.mu.Lock()
	h.dk.inspectErr["web-db"] = true
	h.dk.mu.Unlock()
	mark := len(h.dk.calls())

	d := h.check()
	if d["web"].State.Phase != decide.Active {
		t.Fatalf("an uncountable warm accessory must not demote a serving host, got %s (%s)",
			d["web"].State.Phase, d["web"].Reason)
	}
	for _, c := range h.dk.calls()[mark:] {
		if c == "start web-db" {
			t.Fatalf("a reading docker could not give justifies no warm action: %v", h.dk.calls()[mark:])
		}
	}
	// The regular accessory count is decide's, and a warm inspect failure must
	// not poison it: the shipper is provably down and comes back this check.
	if !h.dk.running("web-litestream") {
		t.Fatalf("the shipper's own reading must be untouched by the warm one: %v", h.dk.calls()[mark:])
	}
}

func TestStatusReportsWarmAccessoriesSeparately(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()
	h.check() // one more check measures what the promotion started

	web := h.sup.Status()["web"].(map[string]any)
	want := map[string]any{"running": 2, "total": 2, "warm_running": 1, "warm_total": 1}
	if !reflect.DeepEqual(web["accessories"], want) {
		t.Fatalf("want accessories %v, got %v", want, web["accessories"])
	}

	// On a standby the same row reads 1/2 with the warm half full, which is
	// the HEALTHY standby reading — the split is there so nobody reads it as
	// something being broken.
	standby := newHarness(t, warmTestConfig("web"), nil)
	standby.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
	}
	standby.checks(2)
	entry := standby.sup.Status()["web"].(map[string]any)
	want = map[string]any{"running": 1, "total": 2, "warm_running": 1, "warm_total": 1}
	if !reflect.DeepEqual(entry["accessories"], want) {
		t.Fatalf("want a standby's accessories %v, got %v", want, entry["accessories"])
	}
}

// TestAConfigWithoutWarmAccessoriesIsUnchanged is the byte-identical claim,
// pinned against the fixture with the most accessory machinery in it: the
// exact ordered list of everything a promote-then-demote cycle changes about
// the world. If a warm accessory ever leaks into decide's own counts — the
// one shape that would make this feature start or stop the WRONG containers —
// this list grows a start or a stop and this test says so.
func TestAConfigWithoutWarmAccessoriesIsUnchanged(t *testing.T) {
	h := newHarness(t, accessoryTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 2)

	// Captured against the code that predates warm accessories, verbatim: the
	// pre-run removal of a stale hook container, the restore itself, the
	// service, the shipper, traffic — then the demotion's two stops.
	want := []string{
		"stop app-web",
		"remove conch-hook-web-restore",
		"create conch-hook-web-restore",
		"start conch-hook-web-restore",
		"remove conch-hook-web-restore",
		"start app-web",
		"start web-litestream",
		"start cf-web",
		"stop cf-web",
		"stop web-litestream",
	}
	if got := h.dk.mutations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("a config with no warm accessories must behave exactly as it did before the field existed\nwant %v\ngot  %v",
			want, got)
	}
}

// TestAWarmAccessoryThatCannotStartIsToldAboutOnce is the cadence problem the
// latch exists for: this reconcile retries on every check, on both hosts, in
// every phase, so an unlatched failure would be an Error and a webhook every
// interval, forever, about a condition that has not changed. The retry is not
// latched — that is the reconcile working — only the telling.
func TestAWarmAccessoryThatCannotStartIsToldAboutOnce(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, warmTestConfig("web"))
	h.build(func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
	}
	h.dk.setStartErr("web-db", true)

	h.checks(4)

	warmAlerts := func() []event {
		var out []event
		for _, e := range h.nt.of("alert") {
			if c, _ := e.payload["container"].(string); c == "web-db" {
				out = append(out, e)
			}
		}
		return out
	}
	if got := len(warmAlerts()); got != 1 {
		t.Fatalf("a warm accessory that cannot start must page once per streak, got %d alerts", got)
	}
	if got := len(handler.of("docker start failed for a warm accessory")); got != 1 {
		t.Fatalf("one Error per failure streak, got %d", got)
	}
	// The attempt itself repeats: a start that failed because the daemon was
	// busy must be retried, and this reconcile is the only thing that will.
	starts := 0
	for _, c := range h.dk.calls() {
		if c == "start web-db" {
			starts++
		}
	}
	if starts < 4 {
		t.Fatalf("the start must be retried every check, got %d attempts in 4 checks", starts)
	}

	// A failure that comes back after a success is a new streak and is news
	// again — the latch is about repetition, not about ever speaking twice.
	h.dk.setStartErr("web-db", false)
	h.check()
	if !h.dk.running("web-db") {
		t.Fatalf("the retry must take once the daemon recovers: %v", h.dk.calls())
	}
	h.dk.setRunning("web-db", true, false)
	h.dk.setStartErr("web-db", true)
	h.checks(2)
	if got := len(warmAlerts()); got != 2 {
		t.Fatalf("a new failure streak is news again, got %d alerts", got)
	}
}

// TestAnEvacuatingServiceSubmitsNoWarmStart is the guard on the one window
// where this reconcile could do harm. An evacuation names its actions once —
// the connector stop above all — and submit is newest-wins TOTAL replacement,
// so a warm start landing while that batch is still being applied would drop
// the stop, and the stop is never named again: the connector keeps serving,
// the lease keeps renewing, and the hand-off needs a human. Nothing could
// submit into that window before warm accessories existed.
func TestAnEvacuatingServiceSubmitsNoWarmStart(t *testing.T) {
	cfg := evacConfig("web")
	cfg.Services[0].WarmAccessories = []string{"web-db"}
	h := newHarness(t, cfg, nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	// The database goes down mid-evacuation, which is exactly when this
	// reconcile would otherwise fire on every remaining check of the sequence.
	h.dk.setRunning("web-db", true, false)
	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	mark := len(h.dk.calls())

	// inFlight is where the call log stood at the start of the LAST check,
	// which is the window the guard covers: the check that confirms the
	// hand-off clears the evacuating flag in evacuateAdvance, before readings
	// run, so a warm start on that check is this reconcile resuming, not the
	// window this test is about.
	inFlight := mark
	final, all := h.runEvac(ch, 12, func(int) {
		inFlight = len(h.dk.calls())
		fleet.peerRenews("web", true)
	})
	if !final.OK {
		t.Fatalf("the evacuation must still complete: %s", transcriptOf(all))
	}
	for _, c := range h.dk.calls()[mark:inFlight] {
		if c == "start web-db" {
			t.Fatalf("an evacuating service must submit no warm start: %v", h.dk.calls()[mark:inFlight])
		}
	}
	// The hand-off itself: the connector stopped and the lease went back.
	if h.dk.running("cf-web") {
		t.Fatalf("the connector must have stopped: %s", transcriptOf(all))
	}
	if h.arb.countOps("web", "release") == 0 {
		t.Fatalf("the release must have gone out: %v", h.arb.ops("web"))
	}

	// And once the host supervises again, the database comes straight back —
	// the guard is about the sequence, not about the container.
	h.check()
	if !h.dk.running("web-db") {
		t.Fatalf("an evacuated host reconciles its warm accessory again: %v", h.dk.calls())
	}
}

// TestNoWarmStartEverStopsAWarmAccessoryDuringAnEvacuation pins the other half
// of the sentence over the whole sequence: no path in an evacuation names a
// warm accessory for stopping either.
func TestNoWarmStartEverStopsAWarmAccessoryDuringAnEvacuation(t *testing.T) {
	cfg := evacConfig("web")
	cfg.Services[0].WarmAccessories = []string{"web-db"}
	h := newHarness(t, cfg, nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	final, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) })
	if !final.OK {
		t.Fatalf("the evacuation must complete: %s", transcriptOf(all))
	}
	h.checks(2)

	for _, c := range warmActions(h, "web-db") {
		if strings.HasPrefix(c, "stop") || strings.HasPrefix(c, "kill") {
			t.Fatalf("no conch path may stop a warm accessory, got %q", c)
		}
	}
	if !h.dk.running("web-db") {
		t.Fatal("the database must still be up on an evacuated host: the deploy it serves does not care about the lease")
	}
}

// TestAWarmStartPrecedesThePromoteHooks pins the order within one submit: the
// warm start goes first, ahead of the decision's own actions. On the check a
// claim is granted those actions include the restore, and a restore racing
// the database it restores INTO is a promotion that fails for no reason.
//
// What this pins is the DETERMINISTIC order the prepend gives, not the bug:
// runPromoteHooks spawns its chain on a goroutine of its own, so with the
// warm start ordered after it the two are genuinely concurrent and either
// order is possible. Deterministic-with-the-fix is the property worth having
// in CI; the race is what the fix removes.
func TestAWarmStartPrecedesThePromoteHooks(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.checkUntilReadyToClaim("web")

	// The database goes down on the very check that claims, so both the warm
	// start and the promotion's actions are named by the same decision.
	h.dk.setRunning("web-db", true, false)
	mark := len(h.dk.calls())

	h.check()
	mustContainInOrder(t, h.dk.calls()[mark:], []string{
		"start web-db",
		"create conch-hook-web-restore",
	})
}

// ---------------------------------------------------------------------------
// Stop reasons: WHY conch last stopped each container it manages.
//
// The incident these tests exist for had nothing to do with conch failing. A
// conch-unaware deploy agent found a stopped container on the standby, could
// not tell a deliberate fence from a crash, and reasoned its way to
// "production has no database". A stop with no recorded intent is
// indistinguishable from a failure, and an agent reading it will invent the
// worse of the two stories. So every stop conch actually performs now says
// which rule asked for it, in the one place an operator (or an agent) already
// looks.
//
// The reason travels with the batch that named the stop, so these tests read
// it through the real status surface — marshalled and unmarshalled, because
// the shape is a wire contract — rather than off the unit.
// ---------------------------------------------------------------------------

// lastStops is one service's last_stops rows as a JSON client sees them. The
// round trip is the point: Status hands out Go maps, and a projection with
// unexported fields would marshal to nothing at all without any test noticing.
func lastStops(t *testing.T, sup *Supervisor, key string) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(sup.Status())
	if err != nil {
		t.Fatalf("status must marshal: %v", err)
	}
	var whole map[string]any
	if err := json.Unmarshal(raw, &whole); err != nil {
		t.Fatalf("status must unmarshal: %v", err)
	}
	entry, ok := whole[key].(map[string]any)
	if !ok {
		t.Fatalf("no status entry for %q: %s", key, raw)
	}
	rows, ok := entry["last_stops"]
	if !ok {
		return nil // conch has stopped nothing for this service since it started
	}
	list, ok := rows.([]any)
	if !ok {
		t.Fatalf("last_stops must be a list, got %#v", rows)
	}
	out := make([]map[string]any, 0, len(list))
	for _, row := range list {
		m, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("every last_stops row must be an object, got %#v", row)
		}
		out = append(out, m)
	}
	return out
}

// stopReasonOf is the question an agent actually asks: did conch stop this
// container on purpose, and why. "" means it has no record of stopping it.
func stopReasonOf(t *testing.T, sup *Supervisor, key, container string) string {
	t.Helper()
	for _, row := range lastStops(t, sup, key) {
		if row["container"] == container {
			reason, _ := row["reason"].(string)
			return reason
		}
	}
	return ""
}

// TestAFencedConnectorSaysConchStoppedItAndWhy is the whole feature in one
// test, and it pins the wire shape: three keys, no more, with the wall clock
// of the check that ORDERED the stop.
func TestAFencedConnectorSaysConchStoppedItAndWhy(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Barred, Holder: "host-b"}
	}
	// A connector running on a host that holds no lease: spec §4's fence, and
	// the exact shape that reads as a mystery outage from outside.
	h.dk.setRunning("cf-web", true, true)

	if got := lastStops(t, h.sup, "web"); len(got) != 0 {
		t.Fatalf("conch has stopped nothing yet, got %v", got)
	}

	h.check()

	rows := lastStops(t, h.sup, "web")
	if len(rows) != 1 {
		t.Fatalf("one stop, one row, got %v", rows)
	}
	row := rows[0]
	if len(row) != 3 {
		t.Fatalf("a row is {container, reason, wall} and nothing else, got %v", row)
	}
	if row["container"] != "cf-web" {
		t.Fatalf("the row must name the container it stopped, got %v", row)
	}
	if row["reason"] != "standby_fence" {
		t.Fatalf("a standby stopping a connector it found running is a fence, got %v", row)
	}
	if row["wall"] != h.clk.Wall() {
		t.Fatalf("the row carries the check's wall clock (%v), got %v", h.clk.Wall(), row)
	}
}

// TestADemotionIsRecordedAsADemotion: rule 14, the sick-service path, which
// stops the connector and gives the lease back.
func TestADemotionIsRecordedAsADemotion(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "demotion" {
		t.Fatalf("an unhealthy service giving up its lease is a demotion, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestARefusedRenewalIsRecordedAsARefusal: rule 15, which is the one an
// operator is most likely to misread — the arbiter reassigned the lease, so
// this host stops immediately with no hysteresis and nothing to release. From
// outside it looks exactly like a crash.
func TestARefusedRenewalIsRecordedAsARefusal(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b"}
	}
	h.check()

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "refused" {
		t.Fatalf("a lease that reads held under an ACTIVE host is a refusal, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestTheServiceStoppedForARestoreSaysItWasStale: rule 6 is the ONLY rule in
// decide that names STOP_SERVICE, and it names it alongside rule 3's connector
// stop — two stops in one batch with two different intents, which is why the
// reason is per-action rather than per-batch.
func TestTheServiceStoppedForARestoreSaysItWasStale(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.checkUntilReadyToClaim("web")
	// A connector left running on the check the claim lands: rule 3 fences it
	// in the same decision rule 6 stops the service in.
	h.dk.setRunning("cf-web", true, true)

	h.check()

	if got := stopReasonOf(t, h.sup, "web", "app-web"); got != "promote_stale" {
		t.Fatalf("the service is stopped so the restore never writes under a live process, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "standby_fence" {
		t.Fatalf("the connector in that same batch is still the standby fence, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestAnEvacuationsStopSaysItWasAnEvacuation: the operator-initiated
// hand-off, and the one reason a human is most likely to be looking for —
// "did somebody evacuate this host" is the first question a stopped connector
// raises.
func TestAnEvacuationsStopSaysItWasAnEvacuation(t *testing.T) {
	h := newHarness(t, evacConfig("web"), nil)
	fleet := newFakeFleet(h.clk)
	h.arb.reply = fleet.reply
	h.livePeer()
	h.driveToActive()

	ch, err := h.sup.Evacuate([]string{"web"}, false, false, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	if done, all := h.runEvac(ch, 12, func(int) { fleet.peerRenews("web", true) }); !done.OK {
		t.Fatalf("the evacuation must succeed: %s", transcriptOf(all))
	}

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "evacuation" {
		t.Fatalf("the connector stopped by the hand-off sequence must say so, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestStopReasonsAreInMemoryAndARestartClearsThem: this is a record of what
// THIS process did, not a durable log. A conch that restarts has stopped
// nothing yet, and must not claim otherwise — the containers it fenced last
// week are somebody else's story now.
func TestStopReasonsAreInMemoryAndARestartClearsThem(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-web", true, true)
	h.build(nil)
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Barred, Holder: "host-b"}
	}

	h.check()
	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "standby_fence" {
		t.Fatalf("the fence must be recorded first, got %q", got)
	}

	// A restart: a second supervisor over the same world, which still has the
	// stopped connector in it. The world remembers; this process does not.
	h.build(nil)
	if h.dk.running("cf-web") {
		t.Fatal("the connector really is still down: the record is what cleared, not the state")
	}
	if got := lastStops(t, h.sup, "web"); len(got) != 0 {
		t.Fatalf("a restarted conch has stopped nothing, got %v", got)
	}
}

// TestNoWarmAccessoryEverAppearsInLastStops is the A4 invariant read from the
// other end: no conch path stops a warm accessory, so no warm accessory can
// ever acquire a stop reason. If one ever shows up here, something started
// stopping the standby's database.
func TestNoWarmAccessoryEverAppearsInLastStops(t *testing.T) {
	h := newHarness(t, warmTestConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 2)

	// Non-vacuous: the shipper beside it WAS stopped by the same demotion.
	if got := stopReasonOf(t, h.sup, "web", "web-litestream"); got != "demotion" {
		t.Fatalf("the shipper must have been stopped by the demotion, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
	if got := stopReasonOf(t, h.sup, "web", "web-db"); got != "" {
		t.Fatalf("no conch path stops a warm accessory, so none may have a stop reason, got %q", got)
	}
}

// TestTheLabelSweepRecordsNoStopReason: the sweep's unit is the synthetic
// sweepScope, which is deliberately not a service — it has no status entry to
// record against, and inventing one would put a container this config does not
// name under a service that does not own it.
//
// Its LOG line is unchanged by the observed-up gate, and deliberately: the
// sweep only ever stops a container it classified as running (sweep.go), so
// "stopped container" is the truth about it. It is the empty reason beside
// it, not a missing reading, that keeps it out of status.
func TestTheLabelSweepRecordsNoStopReason(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-blog", true, true)
	h.dk.setLabeled("cf-blog", "blog")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(1)

	if h.dk.running("cf-blog") {
		t.Fatal("the sweep must actually have stopped it, or this test proves nothing")
	}
	got := handler.of("stopped container")
	if len(got) != 1 || got[0].attrs["container"] != "cf-blog" {
		t.Fatalf("a swept container still narrates as an ordinary stop, got %+v", got)
	}
	if told := handler.of("the container was already down"); len(told) != 0 {
		t.Fatalf("the sweep stops only what it listed as running: %+v", told)
	}
	raw, err := json.Marshal(h.sup.Status())
	if err != nil {
		t.Fatalf("status must marshal: %v", err)
	}
	if strings.Contains(string(raw), "cf-blog") {
		t.Fatalf("a swept container belongs to no service and must appear nowhere in status: %s", raw)
	}
}

// TestADryRunRecordsNoStopReason: a dry run stops nothing, so it has nothing
// to report. A record written where no container moved would be the same
// false story this feature exists to prevent, told the other way round.
func TestADryRunRecordsNoStopReason(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-web", true, true)
	h.build(func(o *Options) { o.DryRun = true })
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Barred, Holder: "host-b"}
	}

	h.check()

	if !h.dk.running("cf-web") {
		t.Fatal("a dry run stops nothing")
	}
	if got := lastStops(t, h.sup, "web"); len(got) != 0 {
		t.Fatalf("a dry run has nothing to record, got %v", got)
	}
}

// TestAStopThatDidNotTakeRecordsNothing: the escalation ran to its end and the
// container is STILL running, which is the wedged-daemon alert's business, not
// this record's. "conch stopped it" must never be said about a container conch
// could not stop.
func TestAStopThatDidNotTakeRecordsNothing(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Barred, Holder: "host-b"}
	}
	h.dk.setRunning("cf-web", true, true)
	h.dk.mu.Lock()
	h.dk.stopStays["cf-web"] = true
	h.dk.killStays["cf-web"] = true
	h.dk.mu.Unlock()

	h.check()

	if !h.dk.running("cf-web") {
		t.Fatal("the fake was told to survive both stop and kill")
	}
	if got := lastStops(t, h.sup, "web"); len(got) != 0 {
		t.Fatalf("a stop that did not take is not a stop, got %v", got)
	}
}

// TestASuspendsStopSaysItWasASuspend: rule 1. The machine was asleep, so every
// held lease is unproven and the connectors go down — on a host whose clock
// just jumped, which is exactly the moment an operator has the least idea what
// happened.
func TestASuspendsStopSaysItWasASuspend(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.clk.AdvanceWallOnly(120) // the VM was paused; monotonic time did not move
	h.check()

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "suspend" {
		t.Fatalf("a connector stopped because unknown time passed must say so, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestADuplicateHostsStopSaysSo: rule 2. Two processes answering to one host
// name both fail closed, which on the innocent one looks like an unexplained
// outage — the name collision is somewhere else entirely.
func TestADuplicateHostsStopSaysSo(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Conflict}
	}
	h.check()

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "duplicate_host" {
		t.Fatalf("failing closed on a name collision must say which collision, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestAStopDeadlineStopSaysSo: rule 17, on_loss=stop. Never serve past your
// lease — so a host stops itself while everything local still looks perfect,
// which is the single most alarming stop conch performs and the one that most
// needs to name itself.
func TestAStopDeadlineStopSaysSo(t *testing.T) {
	cfg := testConfig("web")
	cfg.Arbiter.OnLoss = "stop"
	h := newHarness(t, cfg, nil)
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// The arbiter goes dark. The lease was granted for 60s and the interval is
	// 3s, so the deadline lands well inside this many checks.
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.checks(25)

	if h.dk.running("cf-web") {
		t.Fatal("on_loss=stop must have stopped the connector by now")
	}
	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "stop_deadline" {
		t.Fatalf("a lease that cannot be proven past its deadline must say so, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestLastStopsKeepsTheNewestAndDropsTheOldest: a service named with a
// trailing-* prefix gets a new versioned container name on every deploy, so
// the set of names conch has stopped grows slowly and forever. The cap is what
// keeps a process that has been up for a year from carrying a row per deploy —
// and re-stopping a container conch already has a row for must REPLACE that
// row, not push an older one off the end.
func TestLastStopsKeepsTheNewestAndDropsTheOldest(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	ex := h.sup.units["web"].ex

	for i := 0; i < maxStopRecords+3; i++ {
		name := fmt.Sprintf("app-web-%02d", i)
		h.dk.setRunning(name, true, true)
		ex.stopContainer(name, 5, reasonPromoteStale, float64(i), true)
	}

	rows := lastStops(t, h.sup, "web")
	if len(rows) != maxStopRecords {
		t.Fatalf("want the cap (%d) rows, got %d: %v", maxStopRecords, len(rows), rows)
	}
	if rows[0]["container"] != "app-web-10" {
		t.Fatalf("newest first, got %v", rows)
	}
	if rows[len(rows)-1]["container"] != "app-web-03" {
		t.Fatalf("the oldest rows are the ones dropped, got %v", rows)
	}

	// The last stop only: a container that already has a row keeps exactly one,
	// updated in place at the front, and pushes nothing off the end. Deliberately
	// a container from the MIDDLE of the list — replacing the oldest row would
	// pass with no de-duplication at all, since the cap would have dropped the
	// stale copy anyway.
	h.dk.setRunning("app-web-07", true, true)
	ex.stopContainer("app-web-07", 5, reasonStandbyFence, 99, true)

	rows = lastStops(t, h.sup, "web")
	if len(rows) != maxStopRecords {
		t.Fatalf("re-stopping a known container must not change the count, got %d: %v", len(rows), rows)
	}
	if rows[0]["container"] != "app-web-07" || rows[0]["reason"] != "standby_fence" || rows[0]["wall"] != 99.0 {
		t.Fatalf("the newer stop replaces the older row and moves to the front, got %v", rows)
	}
	for _, r := range rows[1:] {
		if r["container"] == "app-web-07" {
			t.Fatalf("one row per container, got %v", rows)
		}
	}
	if rows[len(rows)-1]["container"] != "app-web-03" {
		t.Fatalf("replacing a row must push nothing off the end, got %v", rows)
	}
}

// TestACrashedConnectorIsNotCreditedToADemotion is the anti-goal, pinned:
// decide's rule 14 names StopConnector unconditionally, so the escalation runs
// against a connector that had ALREADY fallen over. Recording that as
// `demotion` would hand an investigator the exact wrong answer — "conch
// stopped it" about the one container that genuinely crashed — which is worse
// than saying nothing at all.
//
// The log is held to the same standard, and for the same reason: README
// promises that a live log and a later status read of one host tell one
// story, so a stop that writes no row must not narrate as one that did.
func TestACrashedConnectorIsNotCreditedToADemotion(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	mark := len(handler.of("stopped container")) // driveToActive stops nothing, but pin from here
	h.dk.setRunning("cf-web", true, false)       // it fell over on its own
	h.pr.setDenied("http://app-web/up", true)
	d := h.checks(h.cfg.Timing.DemoteAfter + 1)

	if d["web"].State.Phase != decide.Standby {
		t.Fatalf("the demotion must still happen, got %s", d["web"].State.Phase)
	}
	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "" {
		t.Fatalf("conch did not stop a container that was already down, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
	if got := handler.of("stopped container"); len(got) != mark {
		t.Fatalf("the log must not claim the stop the row declines to claim, got %+v", got[mark:])
	}
	told := handler.of("the container was already down")
	if len(told) != 1 {
		t.Fatalf("the stop still ran and still narrates, exactly once: got %d: %+v", len(told), told)
	}
	if told[0].attrs["container"] != "cf-web" {
		t.Fatalf("the line must name the connector it went to stop, got %+v", told[0].attrs)
	}
}

// TestARealDemotionSaysStoppedContainerAndRecordsIt is the other half of the
// same sentence, and the reason the one above cannot be satisfied by simply
// going quiet: when conch IS what took the connector down, both surfaces say
// so, in today's wording, with today's row.
func TestARealDemotionSaysStoppedContainerAndRecordsIt(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// The promotion itself stops a stale service container, which is its own
	// story; this test is about the demotion, so it reads from here on.
	mark := len(handler.of("stopped container"))
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "demotion" {
		t.Fatalf("the row must say demotion, got %q: %v", got, lastStops(t, h.sup, "web"))
	}
	got := handler.of("stopped container")[mark:]
	if len(got) != 1 {
		t.Fatalf("want exactly one stopped-container Info beside that row, got %d: %+v", len(got), got)
	}
	if got[0].attrs["container"] != "cf-web" {
		t.Fatalf("the line must name the connector, got %+v", got[0].attrs)
	}
	if told := handler.of("the container was already down"); len(told) != 0 {
		t.Fatalf("conch is what stopped it, so the already-down wording must not appear: %+v", told)
	}
}

// TestAnAccessoryStopStillNarratesAsAStop pins the documented asymmetry
// UNCHANGED. An accessory's reading is a count, not a per-container state
// (stops.go), so an accessory stop passes observed-up unconditionally: even
// the sibling that was already down keeps today's wording and today's row,
// because "at least one was running" is genuinely all this check knows. The
// stronger claim was never available here, and narrowing the log to match the
// connector's standard would mean inventing a reading conch does not take.
func TestAnAccessoryStopStillNarratesAsAStop(t *testing.T) {
	cfg := accessoryTestConfig("web")
	cfg.Services[0].Accessories = append(cfg.Services[0].Accessories, "web-garage")
	handler := &recordingLogHandler{}
	h := newHarness(t, cfg, func(o *Options) { o.Logger = slog.New(handler) })
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	// One accessory falls over on its own; the other is still up, so the count
	// decide reads is 1 and rule 14's StopAccessories still fires for both.
	mark := len(handler.of("stopped container")) // the promotion's own stops are not this test's
	h.dk.setRunning("web-garage", true, false)
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)

	for _, name := range []string{"web-litestream", "web-garage"} {
		if got := stopReasonOf(t, h.sup, "web", name); got != "demotion" {
			t.Fatalf("%s must still carry the demotion row, got %q: %v",
				name, got, lastStops(t, h.sup, "web"))
		}
	}
	var said []string
	for _, r := range handler.of("stopped container")[mark:] {
		said = append(said, r.attrs["container"])
	}
	if len(said) != 3 {
		t.Fatalf("the connector and both accessories all narrate as stops, got %v", said)
	}
	if told := handler.of("the container was already down"); len(told) != 0 {
		t.Fatalf("an accessory never takes the per-container reading the wording needs: %+v", told)
	}
}

// TestAPromotionOverNoStaleServiceWritesNoRow is the same rule on the other
// container: rule 6 names StopService on every granted claim, whether or not a
// service container was left running. Only a claim that actually found one
// running may say it stopped a stale service.
func TestAPromotionOverNoStaleServiceWritesNoRow(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	h.arb.reply = freeThenGrant(0)
	h.checkUntilReadyToClaim("web")
	h.dk.setRunning("app-web", true, false) // nothing stale to stop

	d := h.check()

	if d["web"].State.Phase != decide.Promoting {
		t.Fatalf("the claim must still be granted, got %s", d["web"].State.Phase)
	}
	if got := stopReasonOf(t, h.sup, "web", "app-web"); got != "" {
		t.Fatalf("there was no stale service to stop, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}

// TestAnUnhealthyDemotionInTheDarkIsStillADemotion pins the ORDER of the
// classifier's inner switch, not just its branches. A locally-sick service
// demotes with or without the arbiter (decide's rule 14 is tested before the
// unreachable block), so this overlap — unhealthy AND unreachable — is the one
// case where reading the lease first would answer `stop_deadline` for a stop
// that was nothing of the kind. Both readings are true here; only one is the
// reason.
func TestAnUnhealthyDemotionInTheDarkIsStillADemotion(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil) // on_loss=freeze
	h.arb.reply = freeThenGrant(0)
	h.driveToActive()

	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Unreachable}
	}
	h.pr.setDenied("http://app-web/up", true)
	h.checks(h.cfg.Timing.DemoteAfter + 1)

	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "demotion" {
		t.Fatalf("a sick service gives up its lease with or without the arbiter, got %q: %v",
			got, lastStops(t, h.sup, "web"))
	}
}
