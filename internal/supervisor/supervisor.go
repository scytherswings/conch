// Package supervisor is the effectful layer: the loop that turns readings
// into one batched arbiter call and a set of side effects, per check, per
// service (spec §3). Every decision it makes is made by internal/decide,
// which is pure; everything here is I/O, ordering and concurrency.
//
// Three rules shape the whole file:
//
//  1. ONE batch per check. Every service is in it, always — a read at
//     minimum — because the free-tier budget (spec §6) is a per-request
//     budget, not a per-service one.
//
//  2. The loop never blocks on Docker. Every action goes to a per-service
//     executor with a depth-1, newest-wins queue (executor.go): two services
//     demoting with 35s stop timeouts must not delay a third service's
//     renewal (spec §5, "stops must not queue behind the loop"). The
//     timing arithmetic in spec §6 is only true if this holds.
//
//  3. A release is an obligation with a precondition, not an op like the
//     others. See "the op model" below — this is the one place where the
//     loop's shape is load-bearing for safety rather than merely tidy.
//
// # The op model
//
// Each check runs decide twice for each service:
//
//   - The PRE-BATCH pass runs against this check's FRESH readings and the
//     lease reading we last observed. Its NextOp is where `read` and `renew`
//     come from, so a claim reflects the host as it is at the instant the
//     batch is built — an image that vanished, an eligibility that lapsed or
//     a suspend detected this check all stop the claim on the spot rather
//     than one check later.
//
//   - The POST-BATCH pass runs against this check's verdict and is what
//     commits: its State, Actions and Reason are the check's answer.
//
// `release` comes from NEITHER pass directly. It is an OBLIGATION recorded
// when a committed decision names decide.Release, and it is sent only once
// two conditions hold:
//
//   - The connector stop that decide named alongside it has actually
//     COMPLETED — not been dispatched to an executor, completed, evidenced by
//     the executor's own stop/verify/kill/verify escalation. decide's rule 14
//     says why in its own comment: "the connector stops before the release,
//     so a FREE lease keeps meaning the previous holder already stopped." A
//     release that went out while our connector was still draining would let
//     a peer claim on that FREE lease and start serving into the overlap —
//     the dual-serve window the whole system exists to prevent.
//
//   - It has not yet been confirmed. The obligation is STICKY: it survives
//     unreachable batches and replies that still read `yours`, and clears
//     only when the arbiter says the lease is no longer ours (free, held,
//     barred) or explicitly acknowledges the release. A release dropped
//     because one batch failed would leave a stopped service holding a
//     ticking lease — an outage nobody can end (spec §9).
//
// While an obligation is pending, nothing may re-take the lease: the op is
// never `renew`, decide's rule 6 (a `yours` reading in STANDBY means the
// claim was granted) is suppressed, and startup adoption is suppressed too.
// We are giving this lease back; a reply that says it is still ours is not
// news, it is the release not having landed yet.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/decide"
	"github.com/scytherswings/conch/internal/dockerapi"
	"github.com/scytherswings/conch/internal/hooks"
	"github.com/scytherswings/conch/internal/updates"
)

// The three ops the arbiter understands (spec §7, Appendix A).
const (
	opRead    = "read"
	opRenew   = "renew"
	opRelease = "release"
)

// maxBatchServices mirrors the arbiter's own MAX_SERVICES. Exceeding it does
// not fail one service, it fails the whole request — which the client turns
// into `unreachable` for every key (Task 8) — so a config with more services
// than this would look exactly like a permanent arbiter outage. Refusing at
// New is cheap and honest, and config.Validate carries the same check so
// `conch validate` catches it in CI rather than at boot.
const maxBatchServices = 64

// startWarmAccessories is the supervisor's own action, and deliberately not
// one of decide's. It is declared here, as a value of decide.Action, so that
// warm accessories can be reconciled without touching internal/decide at all:
// decide.Inputs stays about the ordinary accessories — the lease-following
// shippers, whose whole rule is that they run on exactly the serving host —
// and this start rides a decision's actions the way an evacuation's do
// (commit's `extra`), appended on every check that finds a warm accessory
// down, in any phase, on either host, adoption included.
//
// Two consequences of keeping it out of decide are load-bearing:
//
//   - A config with no warm_accessories can never produce this action, so its
//     behavior is byte-identical to the code that predates the field. That is
//     true by construction rather than by test.
//   - decide's argument that a freeze never authorizes a start is untouched.
//     It is an argument about the ordinary accessories, the connector and the
//     service, all of which are still started only by decide's own rules; a
//     warm accessory is not a lease-following container, so a frozen lease has
//     nothing to say about it.
//
// There is deliberately NO stopping counterpart, and there must never be one.
// No conch path stops a warm accessory: decide.StopAccessories iterates
// svc.Accessories alone (executor.go), evacuation names only connector and
// hook actions (evacuate.go), and the label sweep shields every config-named
// container (sweep.go's namedByConfig). A warm accessory is the standby's
// database, and stopping it is the outage this feature exists to end.
const startWarmAccessories decide.Action = "START_WARM_ACCESSORIES"

// hookStatus values, matching decide.Inputs.HookStatus.
const (
	hookRunning = "running"
	hookDone    = "done"
	hookFailed  = "failed"
)

// arbiterAPI is the one call this package makes to the arbiter. Batch never
// returns an error: a transport failure is `unreachable` for every requested
// key, which is a reading, not an exception (see internal/arbiter).
type arbiterAPI interface {
	Batch(ttl float64, fingerprint string, ops map[string]arbiter.Op) arbiter.Reply
}

// FleetReader is the optional fleet capability, and it is deliberately NOT
// part of arbiterAPI. dynarbiter satisfies arbiterAPI structurally with Batch
// alone; adding Fleet to that interface would break it, and every dynamo host
// with it. So a backend that can answer GET /fleet offers this separately, and
// a backend that cannot — dynamo — leaves the supervisor's fleet field nil.
// nil is not an error: it is "this backend has no fleet view", which local
// mode reports as a note rather than a failure (D8).
type FleetReader interface {
	Fleet(ctx context.Context) (arbiter.FleetReply, error)
}

// docker is the slice of dockerapi.Client the loop and the executors need.
type docker interface {
	Inspect(name string) (dockerapi.ContainerState, error)
	// Start's first return is whether this call TRANSITIONED the container
	// (dockerapi's 304 mapping): false with a nil error is "it was already
	// running". The executors' start narration hangs on it — see
	// startContainer — so a warm accessory started beside a crash-looping
	// sibling every check does not log a start that never happened.
	Start(name string) (started bool, err error)
	Stop(name string, timeoutSeconds int) error
	Kill(name string) error
	ImagePresent(ref string) bool
	Healthy() bool
	// ResolveByPrefix is only ever called for a service container written as
	// a trailing-* prefix pattern (config.Service.ContainerPrefix) — Kamal's
	// versioned container names. An exact container name never reaches this
	// method at all. running is how many genuine (HasPrefix-passing)
	// candidates were RUNNING, independent of which one was selected —
	// resolveContainer alerts when it's more than one.
	ResolveByPrefix(prefix string) (name string, running int, err error)
	// ListWithLabel is the conch.service label sweep's one daemon call
	// (sweep.go): every container carrying the label at all, whatever its
	// value, once per check. A failure reads as an empty list, which makes
	// the sweep a no-op for that check.
	ListWithLabel(key string) []dockerapi.LabeledContainer
}

// hookRunner is internal/hooks. KillOrphans takes the known service list and
// returns the SERVICE names that had an orphaned hook container (Task 10).
type hookRunner interface {
	Run(service string, specs []config.HookSpec) hooks.Result
	Abort(service string)
	KillOrphans(services []string) []string
}

// notifier is the webhook layer, consumed as an interface here and defaulted
// to a no-op so the loop never depends on it existing. The real
// implementation is internal/webhook.Notifier — best-effort, bounded, off
// this package's critical path (spec §11) — which Task 14 constructs from
// config.WebhookConfig and wires in here at Options.Notify.
type notifier interface {
	Event(kind string, payload map[string]any)
}

// prober answers "did this URL respond acceptably" (health.go). host, when
// non-empty, is the Host header the probe request carries — see
// HealthConfig.Host's comment for why this exists and req.Host (not a
// header-map entry) is the only mechanism that sets it.
type prober interface {
	Probe(url, host string, timeoutSeconds float64, expect []int) bool
}

var (
	_ arbiterAPI = (*arbiter.Client)(nil)
	_ docker     = (*dockerapi.Client)(nil)
	_ hookRunner = (*hooks.Runner)(nil)
)

// Options is everything the supervisor cannot invent for itself. Probe,
// DiskFreeGB, Notify and Logger all have working defaults, so a caller only
// has to supply the four clients and the config.
type Options struct {
	Config     config.Config
	Arbiter    arbiterAPI
	Docker     docker
	Hooks      hookRunner
	Clock      clock.Clock
	Notify     notifier
	Probe      prober
	DiskFreeGB func(path string) (float64, error)
	Logger     *slog.Logger
	// Fleet is the optional read capability for the local `conch fleet` view.
	// It is separate from Arbiter on purpose (see FleetReader): nil is a
	// backend without a fleet endpoint, and local mode says so rather than
	// failing. It is only ever used by the admin goroutine, never the loop.
	Fleet FleetReader
	// Updates is the version source for the periodic update check
	// (config.Updates). Nil with an enabled config builds the default Docker
	// Hub source; nil or non-nil with a disabled config means no checks run.
	// It is never on the loop's path.
	Updates  updates.Source
	DryRun   bool
	Instance string
}

// unit is everything the supervisor remembers about one service between
// checks. Everything here is guarded by Supervisor.mu: the loop writes it,
// Status and the executors read and (for hook status) write it.
type unit struct {
	svc config.Service
	key string // svc.Key(): the lease key, and this package's only identity
	ex  *executor

	state     decide.State
	lastLease decide.LeaseState // the reading the pre-batch pass believes
	nextOp    string            // what the last committed decision named, for Status
	hooks     string            // "", running, done, failed
	hookErr   string
	healthy   bool
	reason    string
	// accessoriesUp is the running-accessory count from the most recent check
	// whose reading could be completed, kept for Status. A check whose
	// inspects failed leaves the previous count in place rather than zeroing
	// it: "docker could not say" is not "none are running", and Status is
	// what an operator reads at 3am.
	accessoriesUp int
	// warmUp is the same reading for the WARM accessories, kept apart from
	// accessoriesUp for the same reason the two config lists are apart: this
	// count never reaches decide (see startWarmAccessories), and on a standby
	// a full warm count next to an empty accessory count is the HEALTHY
	// reading, not a half-broken one.
	warmUp int

	// lastStops is why conch last stopped each container of this service's,
	// newest first, one row per container (stops.go). Written by the executor
	// goroutines through noteStopped, read by Status, in memory only.
	lastStops []stopRecord

	// releasePending is the release obligation described in the package
	// comment: recorded when a committed decision names decide.Release, sent
	// only once stopConfirmed, and cleared only when the arbiter says the
	// lease is no longer ours. stopConfirmed is set by the executor when the
	// connector stop that decide named alongside the release has actually
	// finished — dispatching it is not evidence of anything.
	releasePending bool
	stopConfirmed  bool

	// awaitingReconcile means spec §5's reconcile is owed for this service:
	// re-read the arbiter and work out what a lease that is still ours MEANS,
	// which is not the question decide's rule 6 answers. Two events owe it —
	// process start, and a suspend — and both have the same shape: we hold
	// state we cannot vouch for, and a `yours` verdict says "this was already
	// ours" rather than "a claim was granted". While it is owed the op is only
	// ever a read (spec §5: "re-read the arbiter and reconcile"), and it
	// clears on the first answer that is actually the arbiter's opinion.
	awaitingReconcile bool

	// orphanEvidence is spec §5's crash-mid-restore evidence: an orphaned
	// promote-hook container was found for this service at startup, so the
	// local data may be torn and a `yours` reading must be released rather
	// than adopted. It is sticky until the arbiter tells us the lease is no
	// longer ours — a release that never lands (arbiter unreachable, or a
	// reply that still says `yours`) must not become an adoption on the next
	// check just because the first one has passed.
	orphanEvidence bool

	// evacuating means this service is mid-evacuation: decide's rule 19 hands
	// the sequence to the supervisor, and this host claims nothing anywhere
	// while it is set. It is never set for a service this host does not hold,
	// and evacuate.go's finishService is the only thing that clears it —
	// deliberately, because one leaked flag suppresses every claim on this
	// host until the process restarts.
	evacuating bool

	// reclaim is spec §10's one-shot override: the next batch's op for this
	// service is a renew with override:true, which is the only thing that
	// clears this host's own evacuation bar (arbiter/src/lease.js, renewOne).
	// Consumed by the op-building loop, so it costs exactly one batch.
	reclaim bool
}

// Supervisor is one host's loop. RunOnce is not safe for concurrent use —
// the loop owns it — but Status may be called from any goroutine.
type Supervisor struct {
	cfg         config.Config
	fingerprint string
	instance    string
	dryRun      bool

	arb   arbiterAPI
	fleet FleetReader
	dk    docker
	hooks hookRunner
	clk   clock.Clock
	note  notifier
	probe prober
	disk  func(string) (float64, error)
	log   *slog.Logger

	mu    sync.Mutex
	units map[string]*unit
	order []string // stable iteration: lease keys, sorted

	// warmStartFailed latches the narration of a warm accessory's failed
	// start, per container name, so a container that cannot start does not
	// page a human once per interval forever (executor.startWarmContainer).
	// Under mu because executor goroutines write it, unlike the loop-only
	// latches further down.
	warmStartFailed map[string]bool

	// sweeper is the conch.service label sweep's own executor, over a
	// synthetic unit that is deliberately not in units or order (sweep.go).
	// It is a host-level pair of hands: the sweep belongs to no service, and
	// giving it one service's executor would let an unsupervised connector's
	// 35s stop delay that service's own actions.
	sweeper *executor

	// evac is the evacuation in flight, or nil. At most one at a time: two
	// overlapping evacuations would fight over the same units' flags, and an
	// operator watching one of them would be reading the other's transcript.
	// lastPeers is the presence data from the most recent reply, which is what
	// spec §9's pre-flight asks about (evacuate.go).
	evac      *evacuation
	lastPeers map[string]float64

	// Loop-goroutine-only state: the suspend-detection anchor, the
	// fingerprints already reported, whether the current run of
	// refused or arbiter-errored batches has already been logged (see
	// logArbiterUnreachable), which services are mid-overlap-streak (see
	// reportPrefixOverlap), whether the disk floor is mid-unmeasurable-
	// streak (see hostFacts), and which unsupervised labeled containers are
	// mid-streak (see sweep.go). Written only from RunOnce's own goroutine,
	// which is why none of it is under s.mu.
	lastMono, lastWall float64
	haveAnchor         bool
	reportedFP         map[string]string
	// arbRefusedLogged and arbErrLogged latch logArbiterUnreachable's two
	// Err-bearing categories independently, so a batch that refuses some
	// services while erroring on others logs both, once each per streak.
	arbRefusedLogged bool
	arbErrLogged     bool
	// arbTransport* latches logArbiterUnreachable's pure-transport-silence
	// streak: see that function's own comment for why this exists as a
	// deliberate extension. Streak counts consecutive checks with at least
	// one Err-less Unreachable verdict; since is the Mono reading at the
	// streak's first check (its true onset, not the later threshold
	// crossing), so the recovery Info's duration covers the whole outage,
	// not just the part of it this host had already flagged; warned latches
	// the threshold Warn; keys is the last known affected set, carried
	// forward for the recovery Info once the streak itself has gone empty.
	arbTransportStreak int
	arbTransportSince  float64
	arbTransportWarned bool
	arbTransportKeys   []string
	overlapLogged      map[string]bool
	diskUnmeasurable   bool
	// sweepLogged latches per container AND per verdict: the value is what was
	// last said about that container, so a repeat says nothing and a CHANGE —
	// above all a container that was being shielded and is now being stopped —
	// is news again (sweep.go).
	sweepLogged map[string]verdict

	// updates is the version source for the update check (nil = disabled).
	// updateVerdict is the last completed check's answer, guarded by mu for
	// Status; updateNotified and updateSourceFailed are the runUpdates
	// goroutine's own latches, written only there, the same single-writer
	// rule the loop's latches live by.
	updates            updates.Source
	updateVerdict      *updateVerdict
	updateNotified     string
	updateSourceFailed bool
}

// updateVerdict is one completed cloudflared version check's answer, as
// Status reports it: the newest tag this host runs, the newest stable tag
// the source reported, and whether the running tag is at least as new.
// OK=false is the fleet's "there is a bump to review" prompt.
type updateVerdict struct {
	Running string
	Latest  string
	OK      bool
}

type noopNotifier struct{}

func (noopNotifier) Event(string, map[string]any) {}

// New builds a supervisor and performs spec §5's startup reconcile step that
// must happen before any lease is read: orphaned hook containers are killed,
// and the services they belonged to are marked as torn.
//
// It returns an error rather than a bare *Supervisor for one reason: a config
// with more services than the arbiter's batch cap cannot be supervised at
// all, and finding that out as a permanent, unexplained arbiter outage would
// be cruel.
func New(o Options) (*Supervisor, error) {
	if len(o.Config.Services) > maxBatchServices {
		return nil, fmt.Errorf(
			"%d services: the arbiter caps one batch at %d, and conch sends every service in one batch",
			len(o.Config.Services), maxBatchServices)
	}
	if o.Arbiter == nil || o.Docker == nil || o.Hooks == nil || o.Clock == nil {
		return nil, errors.New("supervisor: arbiter, docker, hooks and clock are all required")
	}

	s := &Supervisor{
		cfg:           o.Config,
		fingerprint:   o.Config.Fingerprint(),
		instance:      o.Instance,
		dryRun:        o.DryRun,
		arb:           o.Arbiter,
		fleet:         o.Fleet,
		dk:            o.Docker,
		hooks:         o.Hooks,
		clk:           o.Clock,
		note:          o.Notify,
		probe:         o.Probe,
		disk:          o.DiskFreeGB,
		log:           o.Logger,
		units:         map[string]*unit{},
		reportedFP:    map[string]string{},
		overlapLogged: map[string]bool{},
	}
	if s.note == nil {
		s.note = noopNotifier{}
	}
	if s.probe == nil {
		s.probe = newHTTPProber()
	}
	if s.disk == nil {
		s.disk = diskFreeGB
	}
	if s.log == nil {
		s.log = slog.Default()
	}

	// The version check's source: an injected one wins; otherwise the default
	// Docker Hub source is built when the check is enabled. A disabled check
	// (interval zero) gets no source at all, so no construction cost and no
	// possible surprise fetch.
	interval := o.Config.Updates.IntervalDuration()
	if o.Updates != nil {
		s.updates = o.Updates
	} else if interval > 0 && o.Config.Updates.TimeoutSeconds > 0 {
		timeout := o.Config.Updates.TimeoutSeconds
		if timeout <= 0 {
			timeout = 5
		}
		s.updates = updates.NewDockerHub(time.Duration(timeout * float64(time.Second)))
	}

	for _, svc := range o.Config.Services {
		u := &unit{
			svc:               svc,
			key:               svc.Key(),
			state:             decide.State{Phase: decide.Standby},
			lastLease:         decide.Unreachable, // no reading yet, and that proves nothing
			nextOp:            opRead,
			awaitingReconcile: true,
		}
		u.ex = newExecutor(s, u)
		s.units[u.key] = u
		s.order = append(s.order, u.key)
	}
	sort.Strings(s.order)

	// Spec §8 rule 4 / §5: kill orphans BEFORE the first lease read. In a dry
	// run there is nothing to kill — killing is an action — so the evidence
	// is simply unavailable, which is one more reason a dry run must never
	// claim anything.
	if !s.dryRun {
		for _, key := range s.hooks.KillOrphans(s.order) {
			if u, ok := s.units[key]; ok {
				u.orphanEvidence = true
				s.log.Warn("an orphaned promote hook was found: the previous conch died mid-restore, so this service releases instead of adopting",
					"service", key)
			}
		}
	}

	for _, key := range s.order {
		s.units[key].ex.start()
	}
	s.sweeper = newSweeper(s)
	s.sweeper.start()
	s.log.Info("supervisor ready",
		"host", s.cfg.Host, "instance", s.instance, "services", len(s.order),
		"fingerprint", s.fingerprint, "dry_run", s.dryRun, "on_loss", s.cfg.Arbiter.OnLoss)
	return s, nil
}

// Close stops the executor goroutines once they finish whatever they are
// doing. It is not part of shutdown in the spec's sense: SIGTERM does nothing
// to the world (spec §9), and neither does this — it touches goroutines, not
// containers or leases.
func (s *Supervisor) Close() {
	for _, key := range s.order {
		s.units[key].ex.close()
	}
	s.sweeper.close()
}

// RunForever runs the loop until ctx is cancelled, which is all SIGTERM does:
// conch exits, leases untouched, containers untouched (spec §9).
func (s *Supervisor) RunForever(ctx context.Context) {
	// The executors are this loop's, and they must not outlive it: a
	// cancelled context ends the process's supervision, and a goroutine still
	// waiting for work nobody will send is a leak in every embedding of this
	// package (a test, the admin surface, a future multi-config host). Close
	// only ends goroutines — it touches no container and no lease, exactly as
	// spec §9 requires of anything SIGTERM triggers.
	defer s.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		s.RunOnce()
		if ctx.Err() != nil {
			return
		}
		s.sleepInterval(ctx)
	}
}

// sleepInterval waits out one interval in slices, so a cancelled context is
// noticed in a fraction of a second rather than after a full interval. The
// total time slept is exactly the interval either way, which keeps fake-clock
// tests exact.
func (s *Supervisor) sleepInterval(ctx context.Context) {
	const slice = 0.25
	remaining := s.cfg.Timing.Interval
	for remaining > 0 {
		if ctx.Err() != nil {
			return
		}
		d := slice
		if remaining < slice {
			d = remaining
		}
		s.clk.Sleep(d)
		remaining -= d
	}
}

// RunUpdates is the version check's cadence goroutine. It runs one check at
// boot — an operator who enables the check wants the first verdict promptly,
// not after a full interval — then one per configured interval, sleeping in
// slices on the same injected clock the loop uses, so the fake clock in
// tests drives this beat on exactly the loop's cadence. The check itself
// never touches the loop: all of its I/O happens here, off RunOnce's
// goroutine, exactly as webhook delivery is off every critical path.
func (s *Supervisor) RunUpdates(ctx context.Context) {
	interval := s.cfg.Updates.IntervalDuration()
	if interval <= 0 || s.updates == nil {
		return
	}
	s.checkCloudflaredUpdates(ctx)
	for {
		if ctx.Err() != nil {
			return
		}
		remaining := interval.Seconds()
		const slice = 0.25
		for remaining > 0 {
			if ctx.Err() != nil {
				return
			}
			d := slice
			if remaining < slice {
				d = remaining
			}
			s.clk.Sleep(d)
			remaining -= d
		}
		s.checkCloudflaredUpdates(ctx)
	}
}

// checkCloudflaredUpdates is one best-effort pass of the cloudflared version
// check: the newest cloudflared tag this host runs (from the connector
// containers it supervises) against the newest stable tag the source reports.
// A newer tag becomes a verdict in Status ("there is a bump to review") and
// is said once — a log Warn and an alert webhook per changed running/latest
// pair, never per check, exactly the once-per-change discipline of
// checkFingerprints. Only the source fetch can fail, and a failure is logged
// once per streak and leaves the previous verdict standing: a source conch
// cannot reach is a network problem, and "the last good answer" beats both
// "all clear" and silence for a verdict the operator reads at 3am.
func (s *Supervisor) checkCloudflaredUpdates(ctx context.Context) {
	if s.updates == nil || s.dryRun {
		return // a dry run has nothing to report and nothing to reach out for
	}
	var newest updates.Tag
	found := false
	for _, key := range s.order {
		st, err := s.dk.Inspect(s.units[key].svc.Connector.Container)
		if err != nil || !st.Exists {
			continue
		}
		tag, ok := updates.CloudflaredTag(st.Image)
		if !ok {
			continue
		}
		if !found || newest.Before(tag) {
			newest, found = tag, true
		}
	}
	if !found {
		// No supervised connector runs cloudflared on this host: nothing for
		// the check to prompt about. A verdict of "none" (nil) is honestly
		// different from "up to date".
		s.mu.Lock()
		s.updateVerdict = nil
		s.mu.Unlock()
		return
	}

	latest, err := s.updates.LatestCloudflared(ctx)
	if err != nil {
		if !s.updateSourceFailed {
			s.updateSourceFailed = true
			s.log.Warn("the update check could not reach its version source: the last verdict stands",
				"error", err)
		}
		return
	}
	lt, ok := updates.ParseTag(latest)
	if !ok {
		if !s.updateSourceFailed {
			s.updateSourceFailed = true
			s.log.Warn("the update source returned no parseable stable tag; the last verdict stands",
				"latest", latest)
		}
		return
	}
	s.updateSourceFailed = false

	// OK means the running tag is at least as new as the source's newest:
	// the running tag is not older than latest.
	verdict := &updateVerdict{Running: newest.String(), Latest: lt.String(), OK: !newest.Before(lt)}
	s.mu.Lock()
	s.updateVerdict = verdict
	s.mu.Unlock()

	if !verdict.OK {
		key := verdict.Running + " -> " + verdict.Latest
		if s.updateNotified == key {
			return
		}
		s.updateNotified = key
		s.log.Warn("a newer cloudflared release is available: review and bump the pinned connector image on both hosts",
			"running", verdict.Running, "latest", verdict.Latest)
		s.notify("alert", map[string]any{
			"host":      s.cfg.Host,
			"component": "cloudflared",
			"running":   verdict.Running,
			"latest":    verdict.Latest,
			"reason":    "a newer cloudflared release is available; the fleet pins cloudflared deliberately",
		})
		return
	}
	s.updateNotified = ""
}

// RunOnce is one check for every service: suspend detection, host facts,
// per-service readings, the pre-batch pass, ONE batch, then the post-batch
// pass whose actions are dispatched to the executors. The returned map is
// keyed by lease key and is a snapshot of what was decided, for the caller's
// logs and for tests.
func (s *Supervisor) RunOnce() map[string]decide.Decision {
	now := s.clk.Mono()
	wall := s.clk.Wall()
	suspended := s.detectSuspend(now, wall)
	host := s.hostFacts()

	// An evacuation is a sequence, and this is its check: it starts hooks,
	// dispatches the connector stop and arms the release obligation BEFORE
	// this batch's ops are built, so each step costs one check rather than
	// two. Its side effects ride the check's own dispatch (tick.extra) and its
	// notifications fire after the lock is gone (tick.flush).
	tick := newEvacTick()
	defer tick.flush(s)
	s.evacuateAdvance(now, suspended, tick)

	type prepared struct {
		u  *unit
		in decide.Inputs
		// resolvedContainer is this check's answer to "which concrete
		// container is svc.Container right now" — itself, unchanged, for an
		// exact name, or this check's dockerapi.ResolveByPrefix answer for a
		// trailing-* Kamal-style pattern. Computed once here, in readings,
		// and carried through commit into the executor's submit so a stop or
		// start dispatched from THIS check's decision always acts on THIS
		// check's resolution, never a later check's (supervisor.go's package
		// comment, "cache the resolved name for that check only").
		resolvedContainer string
		// warm is this check's warm-accessory reading, carried to commit for
		// the supervisor-owned start it may append (startWarmAccessories) and
		// for Status's count. It takes no part in the op below: a warm
		// accessory has nothing to do with the lease.
		warm warmReading
	}
	preps := make([]prepared, 0, len(s.order))
	ops := make(map[string]arbiter.Op, len(s.order))
	suppressed := s.claimsSuppressed()

	for _, key := range s.order {
		u := s.units[key]
		in, resolvedContainer, warm := s.readings(u, host, now, suspended)

		s.mu.Lock()
		state, belief := u.state, u.lastLease
		owed, stopped := u.releasePending, u.stopConfirmed
		reconciling := u.awaitingReconcile
		evac := s.evacOpLocked(u)
		// The one-shot override, consumed only on a check where it can
		// actually be sent. Reclaim refuses up front while a release is owed,
		// but the flag can outlive that check: the loop drops s.mu between
		// building this service's op and committing the decision that arms the
		// obligation, so a reclaim arriving in that window sees no obligation
		// and a later check would find both set. An override renew on a lease
		// we owe a release for is the one way this verb can open the dual-serve
		// window, so the obligation wins and the reclaim simply waits for it.
		//
		// `suppressed` is the same argument for the other verb: an evacuating
		// host claims nothing, however the claim was asked for. Evacuate()
		// disarms every one-shot as it starts, so this is the belt to that
		// braces — and like the obligation, it defers the reclaim rather than
		// consuming it.
		reclaim := u.reclaim && !u.releasePending && !suppressed && !s.dryRun
		if reclaim {
			u.reclaim = false
		}
		s.mu.Unlock()

		// The pre-batch pass, on this check's fresh readings. LeaseTTL stays
		// 0: no grant has arrived this check, and a TTL-less Yours
		// deliberately leaves the deadline we proved earlier in force
		// (decide's own contract).
		pre := in
		pre.Lease = belief
		op := decide.Decide(state, pre, s.policy(u.svc)).NextOp

		if op == opRelease {
			// A release is an obligation created by a committed decision,
			// never a preference read off this check's inputs — the decision
			// that names it also names the connector stop that must precede
			// it, and that stop has not happened yet. Recorded below, in
			// commit; sent here only via the branch that follows.
			op = opRead
		}
		if reconciling && op == opRenew {
			// Spec §5: re-READ the arbiter and reconcile. A `yours` belief
			// carried across a process start or a suspend makes decide's rule
			// 6 name a renew, but we have not yet worked out what that lease
			// MEANS; asking is the whole point of this check.
			op = opRead
		}
		if evac.pastRelease && op == opRenew {
			// decide's rule 19 names `renew` for every check of an evacuating
			// service. That is exactly right while the hooks drain under a live
			// lease — and catastrophic once the release has gone out, because a
			// renew would re-take the lease we just handed over, on a host that
			// is about to go away. Past the release, an evacuating service only
			// ever reads (or re-sends the release, below).
			op = opRead
		}
		if owed {
			// The obligation outranks everything the readings suggest,
			// including a `renew` from decide's rule 6 on a lease that is
			// still nominally ours. Until the connector stop is confirmed, a
			// read is all we are entitled to send.
			op = opRead
			if stopped {
				op = opRelease
			}
		}
		if op == opRenew && state.Phase == decide.Standby && suppressed {
			// An evacuating host claims nothing, however free a lease reads
			// (spec §9). Task 12 makes claimsSuppressed capable of true.
			op = opRead
		}
		if reclaim {
			// Spec §10's reclaim, and the one op the readings can never ask
			// for: only the barred host's own override renew clears its bar
			// (arbiter/src/lease.js, renewOne). It lands here, after the
			// reconcile and suppression guards that would otherwise downgrade
			// it to a read, because an operator asking for this by name has
			// answered the question those guards exist to ask.
			op = opRenew
		}
		if s.dryRun {
			// Spec §10: a dry run pointed at a production arbiter must be
			// incapable of claiming a lease out from under the real host.
			op = opRead
		}

		a := arbiter.Op{
			Op: op,
			// Serving is a container-level boolean, stale by up to one
			// interval — that is exactly what spec §9 says a peer polling for
			// hand-off confirmation is entitled to.
			Serving: state.Phase == decide.Active && in.ConnectorUp != nil && *in.ConnectorUp,
		}
		if op == opRenew && reclaim {
			a.Override = true
		}
		if op == opRelease && evac.evacuate {
			a.Evacuate, a.Hold, a.BarTTL = true, evac.hold, evac.barTTL
		}
		ops[key] = a
		preps = append(preps, prepared{u: u, in: in, resolvedContainer: resolvedContainer, warm: warm})
	}

	// The conch.service label sweep: host-level, once per check, one listing
	// for every container at once (sweep.go). "Is anything on this host
	// wearing a label this config does not account for" is a question about
	// the HOST, not one any single service can be asked — so it sits here
	// rather than in the loop above, but it sits AFTER that loop rather than
	// beside hostFacts because it needs what the loop produced: the concrete
	// container name each service's `container:` resolved to on this check,
	// which for a trailing-* pattern is the only name the config can be said
	// to name. Its stops go to their own executor, so the batch below never
	// waits on one.
	resolved := make(map[string]bool, len(preps))
	for _, p := range preps {
		if p.resolvedContainer != "" {
			resolved[p.resolvedContainer] = true
		}
	}
	s.sweepUnsupervised(resolved)

	reply := s.arb.Batch(s.cfg.Timing.TTL, s.fingerprint, ops)
	s.logArbiterUnreachable(now, reply)
	s.checkFingerprints(reply)
	s.mu.Lock()
	s.lastPeers = reply.Peers // spec §9's pre-flight reads the LATEST presence
	s.mu.Unlock()
	// Before the post-batch pass, not after: a rollback decided from these
	// verdicts (spec §9's "the release cannot be delivered") must be in force
	// when decide runs, so the fall-through to ACTIVE renewal happens on this
	// check rather than the next one.
	s.evacuateVerdicts(now, reply, tick)

	decisions := make(map[string]decide.Decision, len(preps))
	for _, p := range preps {
		verdict := reply.Services[p.u.key]
		in := p.in
		in.Lease = verdict.Lease
		in.Evacuating = s.isEvacuating(p.u.key)
		if verdict.Lease == decide.Yours {
			// A grant carries its TTL, anchored at the moment the request was
			// SENT, so response latency can only shorten the lease we believe
			// we hold (spec §6).
			in.LeaseTTL = verdict.TTL
			in.SentAt = reply.SentAt
		}

		d := s.decideOne(p.u, in, verdict)
		decisions[p.u.key] = d
		s.commit(p.u, d, verdict, in, tick.extra[p.u.key], wall, p.resolvedContainer, p.warm)
	}
	return decisions
}

// decideOne is decide.Decide plus the three things decide cannot know,
// all of them about what a `yours` reading MEANS on this particular check.
// decide reads `yours` in STANDBY as "the claim was granted" and promotes,
// which is right in every case but these, so they live here rather than being
// smuggled into Inputs — decide stays a pure function of the state machine in
// spec §5's table, and the supervisor owns the startup reconcile and the
// release obligation, which are sequences, not states.
func (s *Supervisor) decideOne(u *unit, in decide.Inputs, v arbiter.Verdict) decide.Decision {
	s.mu.Lock()
	state := u.state
	evidence, owed, reconciling := u.orphanEvidence, u.releasePending, u.awaitingReconcile
	s.mu.Unlock()

	if v.Lease == decide.Yours {
		switch {
		case evidence:
			// Spec §5: lease still mine, but an orphaned promote hook for it
			// was found — the previous conch died MID-RESTORE, so the local
			// data is torn. Adopting would serve the torn file and let the
			// service's own replication push the corruption over the good
			// copy.
			return decide.Decision{
				State:   decide.State{Phase: decide.Standby, BackoffUntil: state.BackoffUntil},
				Actions: []decide.Action{decide.StopConnector, decide.Release, decide.Alert},
				NextOp:  opRelease,
				Reason: "an orphaned promote hook proves the previous conch died mid-restore: " +
					"releasing instead of adopting, so the peer restores from the clean replica",
			}

		case owed:
			// We are in the middle of giving this lease back. A reply that
			// says it is still ours is not news — it is the release not
			// having landed yet — and rule 6 would read it as a granted claim
			// and start a restore on a service we just demoted.
			return decide.Decision{
				State:   decide.State{Phase: decide.Standby, BackoffUntil: state.BackoffUntil},
				Actions: reconcileStandby(in),
				NextOp:  opRelease,
				Reason:  "the lease is still ours only because the release has not landed yet: re-sending it",
			}

		case reconciling && state.Phase == decide.Standby:
			// Spec §5's reconcile, and spec §9's "zero blip": the lease is
			// still ours and no orphaned hook was found. Either this conch
			// restarted over its own live service (a redeploy, a crash, a
			// container restart), or a suspend made us stop the connector and
			// distrust a lease that turns out to still be ours. Both resume
			// ACTIVE as-is. NO hooks: the service is (or was) already serving
			// here, and a restore would destroy the very data it is serving.
			// A connector we stopped comes back on the next check through
			// decide's rule 12 — safe precisely because we hold the lease.
			//
			// Deliberately NOT gated on ServiceUp. If the service is in fact
			// down, adopting is still the right move: ACTIVE's ordinary health
			// hysteresis demotes in demote_after checks (~9s at defaults) and
			// the peer then promotes properly, with its hooks, from the clean
			// replica. Gating on ServiceUp instead would send us down the
			// PROMOTING path here — restoring over local data whose state we
			// have no reason to distrust — which is the outcome this whole
			// branch exists to prevent.
			// Send-anchored, exactly as decide's own bookkeeping would arm
			// it — and only when the verdict actually carried a ttl. A `read`
			// answers `yours` WITHOUT one (the arbiter grants a ttl only on a
			// renew; arbiter/src/lease.js, view()), and decide is explicit
			// that a TTL-less Yours is not a grant: it must not manufacture a
			// deadline we cannot prove. The first renewal, one check later,
			// arms it for real; until then an unarmed expiry reads as
			// unprovable, which is the fail-safe direction in both on_loss
			// modes.
			expiry := 0.0
			if in.LeaseTTL > 0 {
				expiry = in.SentAt + in.LeaseTTL
			}
			return decide.Decision{
				State: decide.State{
					Phase:        decide.Active,
					LeaseExpiry:  expiry,
					BackoffUntil: state.BackoffUntil,
				},
				NextOp: opRenew,
				Reason: "this conch restarted over a service that is already ours: " +
					"resuming ACTIVE as-is, without hooks",
			}
		}
	}

	return decide.Decide(state, in, s.policy(u.svc))
}

// reconcileStandby is decide's rule 3, which the hand-built decisions above
// still owe: a STANDBY host stops a connector it finds running, however it
// got started (spec §4) — and any accessory still replicating, for the same
// reason and under the same nil-means-nothing reading.
func reconcileStandby(in decide.Inputs) []decide.Action {
	var acts []decide.Action
	if in.ConnectorUp != nil && *in.ConnectorUp {
		acts = append(acts, decide.StopConnector)
	}
	if in.AccessoriesUp != nil && *in.AccessoriesUp > 0 {
		acts = append(acts, decide.StopAccessories)
	}
	return acts
}

// commit stores the decision and dispatches its actions. Hook status is
// supervisor state rather than decide state: decide only reads it.
//
// extra is the evacuation sequence's own actions for this check (evacuate.go).
// They are dispatched in the SAME submit as the decision's, never a second
// one: the executor's queue is depth-1 and newest-wins, so a separate submit
// would be silently eaten by whichever of the two arrived last. They take no
// part in the state bookkeeping below — that is the decision's business, and
// an evacuation's stop is not decide's rule 14's stop.
//
// wall is this check's wall-clock reading (RunOnce measures it once, spec
// §12), threaded through only for the promote/demote webhook payloads below.
//
// resolvedContainer is this check's readings' answer to "which concrete
// container is svc.Container right now" (see prepared.resolvedContainer in
// RunOnce), passed straight through to the executor's submit so a
// StopService/StartService this decision names acts on the container this
// check actually observed, not whatever a later check's resolution happens
// to be.
//
// warm is this check's warm-accessory reading. Like extra it is dispatched in
// the SAME submit and takes no part in the state bookkeeping — a warm
// accessory has no lease, no phase and no hooks — and like extra it never
// touches d.Actions: the Decision this function was handed is what decide
// decided, and it is returned to RunOnce's caller and logged verbatim.
func (s *Supervisor) commit(
	u *unit, d decide.Decision, v arbiter.Verdict, in decide.Inputs, extra []decide.Action, wall float64,
	resolvedContainer string, warm warmReading,
) {
	s.mu.Lock()
	was := u.state.Phase
	// wasEvacuating is read here, alongside was, for notifyTransition below:
	// evacuate.go's finishServiceLocked already emits its own "evacuate"
	// webhook for a hand-off, and it always mutates u.state (and clears
	// u.evacuating) BEFORE this check's commit runs — RunOnce calls
	// evacuateAdvance/evacuateVerdicts ahead of the decide+commit loop — so a
	// transition finishServiceLocked just made is invisible to the was/now
	// comparison below by the time commit sees it. The one path that can
	// still show a transition here while the unit is evacuating is
	// decideOne's "owed" branch (a release re-send), which can name a
	// Standby state without going through finishServiceLocked at all; this
	// flag is what keeps that from being reported twice.
	wasEvacuating := u.evacuating
	// Captured at entry: the branches below both rewrite u.hooks, and whether
	// a chain was in flight when this decision landed is a fact about the
	// moment BEFORE any of them ran.
	wasHooking := u.hooks == hookRunning
	u.state = d.State
	u.nextOp = d.NextOp
	u.lastLease = v.Lease
	u.reason = d.Reason
	u.healthy = in.Healthy
	if in.AccessoriesUp != nil {
		u.accessoriesUp = *in.AccessoriesUp
	}
	if warm.known {
		u.warmUp = warm.up
	}
	// Any of the arbiter's four real opinions is a reading; unreachable and
	// conflict are the two answers that are not the arbiter's opinion at all
	// (see decide.LeaseState), and a reconcile owed against a reading we never
	// got is still owed.
	switch v.Lease {
	case decide.Yours, decide.Held, decide.Free, decide.Barred:
		u.awaitingReconcile = false
	}
	if in.Suspended {
		// Unknown time has passed: every held lease is unproven and the
		// connectors are going down. What comes next is spec §5's reconcile,
		// the same one a process start owes — including the part where a lease
		// that is still ours is RESUMED rather than re-promoted, since
		// restoring over a service we were serving seconds ago would destroy
		// data the suspend never touched.
		u.awaitingReconcile = true
	}

	notOurs := v.Lease == decide.Held || v.Lease == decide.Free || v.Lease == decide.Barred
	// `not_holder` is the arbiter REFUSING a release because we are not the
	// holder — spec §7 — which is the most definitive statement it can make
	// that this lease is not ours. It reaches us as Unreachable (the client
	// maps every error but duplicate_host that way, so a bug of ours can never
	// read as a lease state) with Err preserved, and that distinction is the
	// whole point of preserving it: a transport failure is "we never asked",
	// while this is "we asked and were told no". Treating them alike wedged
	// the obligation permanently — a lease that expired during an arbiter
	// outage can never be released, so the op stayed pinned to `release` and
	// the host could never claim anything again.
	refused := v.Err == "not_holder"
	if notOurs || v.Released || refused {
		// The lease is not ours: whatever the torn data was, we are not about
		// to serve it, and whatever release we owed is moot.
		u.orphanEvidence = false
		u.releasePending = false
	}
	if containsAction(d.Actions, decide.Release) {
		u.releasePending = true
		// A release waits for the connector stop named in the same decision.
		// When there is no such stop — an abandoned PROMOTING never started a
		// connector — there is nothing to wait for, and making it wait would
		// strand the lease until its TTL.
		u.stopConfirmed = !containsAction(d.Actions, decide.StopConnector)
	}
	if was == decide.Promoting && d.State.Phase != decide.Promoting {
		// Leaving PROMOTING retires this promotion's hook result: a "done"
		// left lying around would let the NEXT promotion skip its restore and
		// start a service on un-restored data. This is a backstop, not the
		// guarantee — the loop below is what actually holds it today, since
		// decide enters PROMOTING only through rule 6 and rule 6 always names
		// RunPromoteHooks. Kept because the cost is one branch and the failure
		// it guards against is silent data loss if that ever stops being true.
		// (TestASecondPromotionRestoresAgain pins the property itself; it
		// passes with or without this branch, by design.)
		u.hooks, u.hookErr = "", ""
	}
	for _, a := range d.Actions {
		switch a {
		case decide.RunPromoteHooks:
			u.hooks, u.hookErr = hookRunning, ""
		case decide.AbortHooks:
			if wasHooking {
				// A restore was cut off part way through, so the local data is
				// torn in exactly the way spec §5's orphan sweep exists to
				// detect — this is the same evidence, arrived at from the
				// other side, and it must be recorded HERE because the abort
				// itself destroys the trace: hooks.Abort kills the hook
				// container, and a killed container is removed, so the next
				// process (and the next `yours` reading) would find nothing to
				// be suspicious of. Without this, a suspend landing mid-restore
				// re-arms the reconcile and the following `yours` reading
				// adopts a half-restored service.
				u.orphanEvidence = true
			}
			u.hooks, u.hookErr = "", ""
		}
	}
	s.mu.Unlock()

	s.log.Debug("decision", "service", u.key, "phase", string(d.State.Phase),
		"lease", string(v.Lease), "next_op", d.NextOp, "actions", fmt.Sprint(d.Actions),
		"reason", d.Reason)
	if s.dryRun && len(d.Actions) > 0 {
		s.log.Info("dry run: decision", "service", u.key, "phase", string(d.State.Phase),
			"actions", fmt.Sprint(d.Actions), "reason", d.Reason)
	}
	s.notifyTransition(u.key, was, d.State.Phase, wasEvacuating, d.Reason, v.Lease, wall)

	// The one check that never carries a warm start, and the only thing about
	// a lease that can suppress this reconcile. An evacuation names its
	// actions ONCE — the connector stop above all (evacuate.go) — while
	// submit is newest-wins TOTAL replacement (executor.go): a warm start
	// submitted on the next check, while that batch is still being applied,
	// replaces it, and the stop it dropped is never named again. The
	// evacuation then stalls with the connector still serving and the lease
	// still renewing, which needs a human. Before this field existed nothing
	// could submit into that window — decide's rule 19 names no actions at
	// all while evacuating — so this guard is what keeps that true. A
	// database on a host that is leaving is not worth a stalled hand-off, and
	// the first check after the evacuation finishes starts it again.
	if in.Evacuating {
		warm.anyDown = false
	}

	// The copy is built once and appended to locally: appending into extra
	// itself could write into the evacuation tick's own backing array, and
	// appending into d.Actions would edit the decision this check reports.
	//
	// The warm start goes FIRST, ahead of the decision's own actions. On the
	// check a claim is granted those actions include the promote hooks, and a
	// restore racing the database it restores INTO is a promotion that fails
	// for no reason — it recovers through the abandon-and-back-off path, but
	// it never had to happen. Nothing else in a decision depends on running
	// before a container start it does not name.
	actions := d.Actions
	if len(extra) > 0 || warm.anyDown {
		actions = make([]decide.Action, 0, len(d.Actions)+len(extra)+1)
		if warm.anyDown {
			actions = append(actions, startWarmAccessories)
		}
		actions = append(actions, d.Actions...)
		actions = append(actions, extra...)
	}
	// Why any stop in this batch is happening, worked out HERE because this is
	// the only place that has both the decision and the rule context that
	// produced it (stops.go). extra is passed in because an evacuation's
	// connector stop is named by the sequence rather than by decide, and it is
	// the reason an operator is most likely to be looking for.
	u.ex.submit(actions, resolvedContainer, stopIntentFor(was, d, v, in, extra, wall))
}

// notifyTransition is spec §11's other lease-transition webhook, the one
// evacuate.go does not already cover: a promotion or a demotion that decide
// itself drove, rather than an evacuation's deliberate hand-off. Spec §11
// says this is how a human learns a failover happened at all, so it fires on
// exactly the phase change commit just committed — Standby or Promoting into
// Active is a promote (a fresh claim, or spec §5's restart-over-a-live-
// service resume, which also lands here since it goes straight to Active
// with no PROMOTING step of its own); Active or Promoting into Standby is a
// demote, including an abandoned promotion — and never on a check that
// merely holds the phase it already had.
//
// wasEvacuating skips it: see commit's own comment on the field for why an
// evacuating unit's Standby transition is either already reported by
// finishServiceLocked's "evacuate" event or not really a transition commit
// is seeing for the first time.
func (s *Supervisor) notifyTransition(
	key string, was, now decide.Phase, wasEvacuating bool, reason string, lease decide.LeaseState, wall float64,
) {
	if wasEvacuating {
		return
	}
	var kind string
	switch {
	case (was == decide.Standby || was == decide.Promoting) && now == decide.Active:
		kind = "promote"
	case (was == decide.Active || was == decide.Promoting) && now == decide.Standby:
		kind = "demote"
	default:
		return
	}
	s.log.Info(kind, "service", key, "reason", reason, "lease", string(lease), "wall", wall)
	s.notify(kind, map[string]any{
		"host":    s.cfg.Host,
		"service": key,
		"reason":  reason,
		"lease":   string(lease),
		"wall":    wall,
	})
}

// readings measures this check's inputs for one service (spec §4: three
// questions, measured fresh every check). Alongside decide.Inputs it returns
// this check's resolved service-container name — see resolveContainer and
// prepared.resolvedContainer in RunOnce — which is not part of decide's pure
// Inputs (decide has no business knowing container names) but is what
// commit must carry to the executor for STOP_SERVICE/START_SERVICE, and what
// this function itself uses for the Inspect call below and for a
// {container} placeholder in svc.Health.URL.
//
// The third return is this check's warm-accessory reading, which is not part
// of decide.Inputs either — for a stronger reason than the container name:
// warm accessories are reconciled by the supervisor and by nothing else
// (startWarmAccessories).
func (s *Supervisor) readings(
	u *unit, host hostFacts, now float64, suspended bool,
) (decide.Inputs, string, warmReading) {
	svc := u.svc

	resolvedContainer, resolveErr := s.resolveContainer(svc)

	var serviceUp, connectorUp *bool
	var svcState dockerapi.ContainerState
	svcErr := resolveErr
	if resolveErr == nil && resolvedContainer != "" {
		svcState, svcErr = s.dk.Inspect(resolvedContainer)
		if svcErr == nil {
			up := svcState.Running
			serviceUp = &up
		}
	}
	// An unresolved prefix (resolvedContainer == "", resolveErr == nil) skips
	// Inspect entirely rather than calling Inspect(""): svcState stays its
	// zero value (Exists:false) and svcErr stays nil, which is exactly the
	// shape Inspect itself returns for an exact name that doesn't exist — the
	// service container doesn't exist either way, and both read as
	// ineligible below through the identical svcErr==nil/svcState.Exists
	// check.
	connState, connErr := s.dk.Inspect(svc.Connector.Container)
	if connErr == nil {
		up := connState.Running
		connectorUp = &up
	}

	// Accessories are reconciled exactly like the connector, so they are
	// measured exactly like it: one Inspect per configured accessory, fresh
	// every check, exact names always (Validate refuses patterns). One
	// inspect failure poisons the whole reading — a count we could not finish
	// is decide's nil, and justifies no accessory action this check — and it
	// fails accessoriesExist too, since existence is exactly what the failed
	// call could not prove. An accessory that provably does not exist reads
	// as ineligible below: a host that cannot replicate must not win a
	// promotion.
	accessoriesKnown, accessoriesExist := true, true
	accessoriesRunning := 0
	for _, name := range svc.Accessories {
		accState, accErr := s.dk.Inspect(name)
		if accErr != nil {
			accessoriesKnown, accessoriesExist = false, false
			continue
		}
		if !accState.Exists {
			accessoriesExist = false
		}
		if accState.Running {
			accessoriesRunning++
		}
	}
	var accessoriesUp *int
	if accessoriesKnown {
		accessoriesUp = &accessoriesRunning
	}

	// Warm accessories are measured in a loop of their own, and reported in a
	// return value of their own, because they answer a different question:
	// not "should this host be replicating" but "is this host's database up",
	// which has the same answer on both hosts. The counts stay out of
	// decide.Inputs entirely (startWarmAccessories' comment says why); the one
	// place the two kinds meet is existence, which is eligibility's business —
	// a host whose database container is not there cannot serve this service,
	// so it must not win a promotion, exactly as a missing shipper must not.
	//
	// Its own known/unknown poisoning, kept separate for the same reason:
	// folding a warm Inspect failure into accessoriesKnown would nil decide's
	// ACCESSORY count, which is to say it would let a warm reading change what
	// decide does about the shippers.
	warm := warmReading{known: true}
	for _, name := range svc.WarmAccessories {
		warmState, warmErr := s.dk.Inspect(name)
		if warmErr != nil {
			warm.known = false
			accessoriesExist = false
			continue
		}
		if !warmState.Exists {
			accessoriesExist = false
			continue
		}
		if warmState.Running {
			warm.up++
			continue
		}
		// EXISTS and is not running: the one shape this reconcile can act on.
		// A container that does not exist is deliberately not "down" here —
		// starting it would fail on every check forever, alerting each time,
		// and absence is already reported where it belongs, through
		// eligibility above.
		warm.anyDown = true
	}
	if !warm.known {
		// A count we could not finish justifies no warm action this check, the
		// same nil-means-nothing contract decide holds for the accessories.
		warm.anyDown = false
	}

	s.mu.Lock()
	phase, hookState, evacuating := u.state.Phase, u.hooks, u.evacuating
	s.mu.Unlock()

	// Service health: the container runs AND the health URL answers as
	// configured. A container that is not running cannot be healthy, and
	// probing it would only cost a request to prove it. {container} in the
	// URL substitutes this check's resolved name — the exact configured name
	// when there is no prefix pattern, so this is unconditional and costs
	// nothing extra for a service that never uses the placeholder at all.
	healthy := host.ok && serviceUp != nil && *serviceUp
	if healthy {
		healthURL := strings.ReplaceAll(svc.Health.URL, "{container}", resolvedContainer)
		healthy = s.probe.Probe(healthURL, svc.Health.Host, svc.Health.TimeoutSeconds, svc.Health.Expect)
	}
	// Connector readiness folds into ACTIVE health only (spec §4): a standby
	// has no connector to be ready, so probing one there would measure the
	// peer's edge registration, not ours.
	//
	// And only when the connector CONTAINER is actually running. The signal
	// spec §4 wants here is narrow and specific — "a connector container that
	// is *up* but cannot register with the edge" — so a container that is
	// down is out of scope by construction: it trivially cannot answer, and
	// counting that as unhealthiness would demote the host for a condition
	// decide's rule 12 exists to FIX. Getting this wrong makes rule 12
	// unreachable whenever ready_url is configured: the crash-between-stop-
	// and-release heal never fires, and a resumed lease (adoption, or the
	// resume after a suspend) demotes on its first check and takes the full
	// restore path instead of resuming. A running container that cannot
	// register is the genuine failure, and it still demotes.
	connectorRunning := connectorUp != nil && *connectorUp
	if healthy && phase == decide.Active && svc.Connector.ReadyURL != "" && connectorRunning {
		// No Host override here: health.host (its own comment says why it
		// exists) is scoped to the health probe only — A6 explicitly leaves
		// ready_url out of scope, so this call keeps using the URL's own
		// host, exactly as before this field existed.
		healthy = s.probe.Probe(svc.Connector.ReadyURL, "", svc.Health.TimeoutSeconds, []int{200})
	}

	// Eligibility: containers for what conch starts by name — the service,
	// the connector, and every accessory — images for what it creates
	// (spec §4). Deliberately not "the service is running" — requiring that
	// would make failover impossible.
	eligible := host.ok &&
		svcErr == nil && svcState.Exists &&
		connErr == nil && connState.Exists &&
		accessoriesExist &&
		s.hookImagesPresent(svc)

	return decide.Inputs{
		NowMono:          now,
		Suspended:        suspended,
		Healthy:          healthy,
		Eligible:         eligible,
		ConnectorUp:      connectorUp,
		ServiceUp:        serviceUp,
		AccessoriesUp:    accessoriesUp,
		AccessoriesTotal: len(svc.Accessories),
		HookStatus:       hookState,
		Evacuating:       evacuating,
	}, resolvedContainer, warm
}

// warmReading is one check's measurement of one service's warm accessories.
// It is a supervisor type rather than fields on decide.Inputs on purpose: no
// decision in internal/decide is a function of it, and putting it there would
// invite one.
type warmReading struct {
	// known is false when any warm Inspect failed this check. "Docker could
	// not say" is not "it is down", and it justifies no action at all.
	known bool
	// up is how many warm accessories are running, for Status. Meaningful
	// only when known — a check that could not finish its count leaves the
	// previous one in place rather than reporting a partial one as the truth.
	up int
	// anyDown means at least one warm accessory EXISTS and is not running,
	// which is the only condition that starts anything. Never true when the
	// reading is incomplete, and never true for a container that is merely
	// absent (that is eligibility's business, not this reconcile's).
	anyDown bool
}

// resolveContainer answers, for this check, which concrete container svc's
// Container config should be treated as. An exact name (config.Service.
// ContainerPrefix's ordinary case) is returned unchanged with no daemon call
// at all — every existing call-log assertion for a config that doesn't use a
// prefix pattern stays byte-identical, and there is nothing to resolve.
//
// A trailing-* pattern is resolved fresh, every check, against whichever
// container currently matches it (dockerapi.Client.ResolveByPrefix — Kamal
// versions a service container's name on every deploy, so what "app-web-*"
// answers to today may not be what it answers to on the next check). No
// match returns ("", nil): the service container does not exist right now,
// which readings above treats exactly like Inspect finding nothing for an
// exact name — ineligible, not an error. A transport failure resolving the
// prefix is still a real error, and propagates like Inspect's own would.
func (s *Supervisor) resolveContainer(svc config.Service) (string, error) {
	prefix, isPrefix := svc.ContainerPrefix()
	if !isPrefix {
		return svc.Container, nil
	}
	name, running, err := s.dk.ResolveByPrefix(prefix)
	if err == nil {
		s.reportPrefixOverlap(svc.Key(), prefix, running)
	}
	return name, err
}

// reportPrefixOverlap is the blue/green overlap window made visible: more
// than one LIVE container matches this service's prefix at once, ordinarily
// brief (Kamal starting the new version before retiring the old one), during
// which resolution can pick differently from one check to the next — so the
// executor's stop/start actions can act on different names across two checks
// of the SAME promotion. Selection is untouched here; this only makes the
// ambiguity audible instead of silent.
//
// Two things keep it from being noise on a healthy fleet, both learned from
// one. RUNNING containers only: Kamal leaves every previous version's
// container behind, exited, so a host a few deploys old matches half a dozen
// names in the ordinary steady state and none of them is an overlap
// (dockerapi.ResolveByPrefix's own comment). And once per STREAK, like
// logArbiterUnreachable: a deploy that stalls with both versions up would
// otherwise re-alert every interval for as long as it lasts. The latch clears
// as soon as the count falls back to one, so the next overlap is reported
// again. Loop-goroutine-only, no lock — see the field's own comment.
func (s *Supervisor) reportPrefixOverlap(key, prefix string, running int) {
	if running <= 1 {
		delete(s.overlapLogged, key)
		return
	}
	if s.overlapLogged[key] {
		return
	}
	s.overlapLogged[key] = true
	s.log.Warn("more than one container for this prefix is running: resolution can flip mid-deploy, check docker ps on this host",
		"service", key, "prefix", prefix, "running", running)
	s.notify("alert", map[string]any{
		"service": key,
		"host":    s.cfg.Host,
		"prefix":  prefix,
		"running": running,
		"reason":  fmt.Sprint(running) + " containers matching this prefix are running; resolution can flip mid-deploy",
	})
}

// hookImagesPresent checks every hook image, promote and evacuate alike, as
// spec §4 says ("every hook image is present"). Evacuate images gate
// eligibility too because a host that cannot hand a service over later has no
// business taking it now — at the cost, stated plainly, that a missing
// evacuate image blocks a failover the promote hooks could have served.
func (s *Supervisor) hookImagesPresent(svc config.Service) bool {
	for _, set := range [][]config.HookSpec{svc.Hooks.Promote, svc.Hooks.Evacuate} {
		for _, h := range set {
			if !s.dk.ImagePresent(h.Image) {
				return false
			}
		}
	}
	return true
}

// hostFacts is spec §4's host health: measured once per check and shared by
// every service's decision, so three services cost one docker ping and one
// disk read, not three of each. Called only from RunOnce, which is why the
// unmeasurable-disk latch below needs no lock (see the field's own comment).
type hostFacts struct {
	ok       bool
	dockerOK bool
	diskOK   bool
	freeGB   float64
}

func (s *Supervisor) hostFacts() hostFacts {
	f := hostFacts{dockerOK: s.dk.Healthy(), diskOK: true}
	if s.cfg.Disk.Path != "" && s.cfg.Disk.MinFreeGB > 0 {
		free, err := s.disk(s.cfg.Disk.Path)
		switch {
		case err != nil:
			// A floor conch cannot measure is a floor it cannot clear. The
			// same default-deny as ImagePresent and decide's lease reading:
			// "couldn't ask" is never "yes".
			//
			// Reported once per STREAK, like logArbiterUnreachable and
			// reportPrefixOverlap. The overwhelmingly common cause is a
			// disk.path that does not exist on this host — a typo, or a bind
			// that was never made — which is unmeasurable on EVERY check for
			// as long as it lives, so alerting per check is one webhook every
			// interval forever about a condition that has not changed since
			// the first one. The latch clears the moment the floor measures
			// again, so a path that breaks a second time is news again.
			//
			// Eligibility is deliberately NOT latched: every check still
			// fails the floor. The alert is the notification, and it can be
			// said once; the ineligibility is the safety property, and it
			// holds continuously.
			f.diskOK = false
			if !s.diskUnmeasurable {
				s.diskUnmeasurable = true
				s.log.Warn("the disk floor could not be measured: this host stays ineligible until it can be, check disk.path exists in this container",
					"path", s.cfg.Disk.Path, "error", err)
				s.notify("alert", map[string]any{
					"host":   s.cfg.Host,
					"path":   s.cfg.Disk.Path,
					"error":  err.Error(),
					"reason": "the disk floor could not be measured",
				})
			}
		default:
			s.diskUnmeasurable = false
			f.freeGB = free
			f.diskOK = free >= s.cfg.Disk.MinFreeGB
		}
	}
	f.ok = f.dockerOK && f.diskOK
	return f
}

// detectSuspend compares this check's wall delta to its monotonic delta.
// Monotonic clocks do not advance through a VM pause, so a divergence beyond
// slop means unknown time has passed and every held lease is unproven
// (spec §5). Loop-goroutine-only state; no lock needed.
func (s *Supervisor) detectSuspend(now, wall float64) bool {
	suspended := false
	if s.haveAnchor && (wall-s.lastWall)-(now-s.lastMono) > s.cfg.Slop() {
		suspended = true
		s.log.Warn("unknown time has passed: treating every held lease as unproven",
			"wall_delta", wall-s.lastWall, "mono_delta", now-s.lastMono)
	}
	s.lastMono, s.lastWall, s.haveAnchor = now, wall, true
	return suspended
}

// checkFingerprints reports a peer supervising a different set of services
// (spec §11) and does nothing else — a config drift is a human problem, and
// acting on it would mean one host's config deciding another's fate. Reported
// once per changed value rather than once per check.
func (s *Supervisor) checkFingerprints(reply arbiter.Reply) {
	for host, fp := range reply.Fingerprints {
		if host == s.cfg.Host || fp == "" || fp == s.fingerprint {
			continue
		}
		if s.reportedFP[host] == fp {
			continue
		}
		s.reportedFP[host] = fp
		s.log.Warn("a peer is supervising a different set of services: compare the services list in both hosts' configs",
			"peer", host, "peer_fingerprint", fp, "our_fingerprint", s.fingerprint)
		s.notify("fingerprint_mismatch", map[string]any{
			"peer":             host,
			"peer_fingerprint": fp,
			"our_fingerprint":  s.fingerprint,
			"host":             s.cfg.Host,
		})
	}
}

// logArbiterUnreachable is this check's only chance to say WHY the arbiter
// looked unreachable, and this is what was wrong in production: a real
// failover's losing host produced exactly one log line, "arbiter
// unreachable...check this url", for a not_holder refusal from a perfectly
// reachable arbiter — sending the operator toward a healthy url and away
// from the actual event, a lost lease. Verdict.Err carries the arbiter's own
// opinion (see arbiter.Client's toVerdict) and every non-empty value here
// means the SAME thing at the transport layer — the request round-tripped,
// the arbiter answered — so it must never be worded as if the arbiter could
// not be reached. Two Err values are a REFUSAL, the arbiter's most
// definitive kind of answer (not_holder: spec §7, "we asked and were told
// no"; invalid_op: our own malformed request, still a real answer); every
// other non-empty value is the arbiter's error state for a reason this
// client does not specifically recognise, which keeps the older
// unreachable-flavoured wording because reachability, there, is genuinely
// less certain.
//
// A transport failure (a dead socket, a non-200, a body that won't parse, or
// a response that silently drops a requested key) carries NO Err by
// construction — see Verdict.Err's own doc comment and allUnreachable — so
// it produces neither wording above. That is exactly the silence that made
// forensics unable to pin the outage window's start, which is why this
// function also runs a THIRD, deliberately extended check for it below.
//
// Each of the three checks classifies services independently and every one
// collects EVERY affected key, not just the first: an earlier version broke
// out of its loop on the first unreachable-with-Err match, which silently
// dropped every other affected service's name from the one line meant to
// name them.
//
// The refusal and error checks log once per STREAK, not once per check —
// arbRefusedLogged and arbErrLogged (loop-goroutine-only, no lock needed:
// RunOnce is not for concurrent use) latch the first time each fires and
// clear the moment a check's batch does not hit that condition, so the next
// streak logs again. A host stuck refusing or erroring would otherwise fill
// the log with one warning every interval forever.
func (s *Supervisor) logArbiterUnreachable(now float64, reply arbiter.Reply) {
	var refusedKeys, refusedErrs, errKeys, errErrs, transportKeys []string
	for _, k := range s.order {
		v, ok := reply.Services[k]
		if !ok || v.Lease != decide.Unreachable {
			continue
		}
		switch {
		case v.Err == "":
			transportKeys = append(transportKeys, k)
		case v.Err == "not_holder" || v.Err == "invalid_op":
			refusedKeys = append(refusedKeys, k)
			refusedErrs = appendUnique(refusedErrs, v.Err)
		default:
			errKeys = append(errKeys, k)
			errErrs = appendUnique(errErrs, v.Err)
		}
	}

	if len(refusedKeys) == 0 {
		s.arbRefusedLogged = false
	} else if !s.arbRefusedLogged {
		s.arbRefusedLogged = true
		s.log.Warn("arbiter refused "+strings.Join(refusedErrs, ", ")+" for "+strings.Join(refusedKeys, ", ")+
			": the arbiter is reachable; this host's view of the lease is stale",
			"services", strings.Join(refusedKeys, ","), "err", strings.Join(refusedErrs, ","))
	}

	if len(errKeys) == 0 {
		s.arbErrLogged = false
	} else if !s.arbErrLogged {
		s.arbErrLogged = true
		s.log.Warn("arbiter unreachable: no lease can change hands until it answers, check this url",
			"services", strings.Join(errKeys, ","), "err", strings.Join(errErrs, ","), "url", s.cfg.Arbiter.URL)
	}

	s.logArbiterTransportSilence(now, transportKeys)
}

// appendUnique appends v to list unless it is already present. The Err
// values a single batch collects into one wording are few enough (at most a
// handful of distinct arbiter error reasons) that a linear scan costs
// nothing next to an HTTP round trip, and it keeps first-seen order, which
// is s.order's own alphabetical order — deterministic, not map iteration.
func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

// logArbiterTransportSilence is the deliberate extension beyond truthful
// per-check wording: a transport failure carries no Err (see
// logArbiterUnreachable's own comment), so unlike a refusal or an arbiter
// error it produces NOTHING for this check to say — the forensics gap the
// RCA could not close, because nothing in the log let anyone pin where a
// real outage's silent stretch began.
//
// This does not narrate every check the way the two Err-bearing categories
// do; a network blip lasting one or two checks is not an outage, and saying
// so every interval forever is the exact noise those categories' own
// latches exist to avoid. Instead it counts the STREAK and only speaks once
// it reaches demote_after consecutive checks — the same threshold decide
// already uses to call a lease unproven — naming every affected service and
// how long the streak has run so far. arbTransportSince is pinned at the
// streak's first check, not the check that crosses the threshold, so the
// one Info logged on recovery reports the OUTAGE's total duration: an
// operator reading only that line can compute the window's start as
// (this record's time) - duration_seconds, which is the boundary the RCA
// says forensics never had.
func (s *Supervisor) logArbiterTransportSilence(now float64, keys []string) {
	if len(keys) == 0 {
		if s.arbTransportWarned {
			s.log.Info("arbiter transport silence ended: the arbiter is answering again",
				"services", strings.Join(s.arbTransportKeys, ","), "duration_seconds", now-s.arbTransportSince)
		}
		s.arbTransportStreak, s.arbTransportWarned, s.arbTransportSince, s.arbTransportKeys = 0, false, 0, nil
		return
	}

	s.arbTransportKeys = keys
	if s.arbTransportStreak == 0 {
		s.arbTransportSince = now
	}
	s.arbTransportStreak++
	if s.arbTransportStreak >= s.cfg.Timing.DemoteAfter && !s.arbTransportWarned {
		s.arbTransportWarned = true
		s.log.Warn("arbiter unreachable with no error to name: pure transport silence, likely the outage's start, check network/DNS/TLS to the arbiter url",
			"services", strings.Join(keys, ","), "streak", s.arbTransportStreak, "url", s.cfg.Arbiter.URL)
	}
}

// claimsSuppressed is spec §9's "an evacuating host claims nothing": while
// ANY evacuation is in flight, no service on this host may claim a lease,
// however free it reads. Two hosts evacuating in parallel must not adopt each
// other's released services onto machines that are about to go away. Task 12
// sets unit.evacuating; until then this is always false.
func (s *Supervisor) claimsSuppressed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range s.units {
		if u.evacuating {
			return true
		}
	}
	return false
}

// policy is the timing model for one service: four knobs and one derived,
// per-service stop deadline (spec §6).
func (s *Supervisor) policy(svc config.Service) decide.Policy {
	return decide.Policy{
		DemoteAfter:  s.cfg.Timing.DemoteAfter,
		PromoteAfter: s.cfg.Timing.PromoteAfter,
		Interval:     s.cfg.Timing.Interval,
		StopDeadline: s.cfg.StopDeadline(svc),
		// Anything that is not the strict mode is the default, freeze: an
		// unset or unrecognised value must not silently make the arbiter's
		// availability the fleet's.
		FreezeOnLoss:   s.cfg.Arbiter.OnLoss != "stop",
		BackoffSeconds: 3 * float64(s.cfg.Timing.PromoteAfter) * s.cfg.Timing.Interval,
	}
}

func containsAction(actions []decide.Action, want decide.Action) bool {
	for _, a := range actions {
		if a == want {
			return true
		}
	}
	return false
}

// noteWarmStartFailed records that a warm accessory's start failed and reports
// whether this is the FIRST failure of the current streak — the one that
// narrates. Every later failure of the same streak is the same news.
func (s *Supervisor) noteWarmStartFailed(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.warmStartFailed[name] {
		return false
	}
	if s.warmStartFailed == nil {
		s.warmStartFailed = map[string]bool{}
	}
	s.warmStartFailed[name] = true
	return true
}

// noteWarmStartOK ends a failure streak: the container started, so the next
// failure is a new one and says so.
func (s *Supervisor) noteWarmStartOK(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.warmStartFailed, name)
}

// noteConnectorStopped is the executor telling the loop that the stop
// escalation for this service's connector has run to completion. It is the
// precondition on the release obligation, and it is deliberately about the
// escalation having FINISHED rather than about the container being provably
// gone: against a wedged daemon the container can survive both stop and kill
// (spec §9's documented gap, already alerted on by then), and withholding the
// release in that case would strand the lease until its TTL while the peer
// waits — without stopping the connector that is the actual problem.
func (s *Supervisor) noteConnectorStopped(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.units[key]; ok {
		u.stopConfirmed = true
	}
}

// notify is the one door to the webhook layer, and it is shut in a dry run:
// spec §10 says a dry run executes none of the actions it describes, and a
// webhook is an action — it pages a human, it can drive an operator's own
// automation. The event is logged at the same level of detail instead.
func (s *Supervisor) notify(kind string, payload map[string]any) {
	if s.dryRun {
		// The kind at Info, the payload at Debug: a dry run must show that the
		// notification would have gone out, and an operator who wants to see
		// what was in it can ask for it (--log-level debug).
		s.log.Info("dry run: would notify", "kind", kind)
		s.log.Debug("dry run: notification payload", "kind", kind, "payload", fmt.Sprint(payload))
		return
	}
	s.note.Event(kind, payload)
}

// setHookStatus is how an executor's hook goroutine reports back.
func (s *Supervisor) setHookStatus(key, status, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.units[key]; ok {
		u.hooks, u.hookErr = status, detail
	}
}

// phaseOf is the executor's guard against acting on a decision the loop has
// already superseded.
func (s *Supervisor) phaseOf(key string) decide.Phase {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.units[key]; ok {
		return u.state.Phase
	}
	return decide.Standby
}

// statusPeersKey is the one key in Status's output that is not a lease key.
// The map is flat — one entry per service, keyed by lease key — so peer
// presence has to live at that same level, and a service leasing "peers"
// would collide with it. config.Validate refuses that name outright
// (statusReservedKey), which is why this can be an unconditional assignment
// rather than a merge nobody could read the result of.
const statusPeersKey = "peers"

// Status is the admin surface's snapshot (spec §10): every service's state,
// lease, health and last decision, plus the peer presence from the latest
// batch, copied out under the lock so a reader never sees the loop mid-write.
//
// The peers map is the arbiter's own answer, verbatim — host to seconds since
// that host last checked in — and it is here because spec §9's evacuation
// pre-flight refuses on exactly this data: without it, a refusal ("no peer has
// checked in within Ns") could only be diagnosed by guessing what the
// supervisor had seen. It is always present, empty rather than absent before
// the first batch lands, so "nobody is there" never has to be told apart from
// "conch has not asked yet".
func (s *Supervisor) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.units)+1)
	peers := make(map[string]float64, len(s.lastPeers))
	for host, lastSeen := range s.lastPeers {
		peers[host] = lastSeen
	}
	out[statusPeersKey] = peers
	if s.updateVerdict != nil {
		out["updates"] = map[string]any{
			"cloudflared": map[string]any{
				"running": s.updateVerdict.Running,
				"latest":  s.updateVerdict.Latest,
				// ok=false is the fleet's whole reason for the check: there is
				// a newer cloudflared release than the fleet pins.
				"ok": s.updateVerdict.OK,
			},
		}
	}
	for key, u := range s.units {
		entry := map[string]any{
			"phase":        string(u.state.Phase),
			"lease":        string(u.lastLease),
			"healthy":      u.healthy,
			"reason":       u.reason,
			"next_op":      u.nextOp,
			"hooks":        u.hooks,
			"ok_streak":    u.state.OKStreak,
			"bad_streak":   u.state.BadStreak,
			"evacuating":   u.evacuating,
			"reconciling":  u.awaitingReconcile,
			"service":      u.svc.Name,
			"release_owed": u.releasePending,
		}
		if u.hookErr != "" {
			entry["hook_error"] = u.hookErr
		}
		// Why conch last stopped each container of this service's, newest
		// first — the answer to "is this container down because something
		// broke, or because conch fenced it on purpose" (stops.go). Omitted
		// entirely when there is nothing to say, like hook_error above: this
		// process has stopped nothing for this service, which a row of zeroes
		// would say less clearly. `wall` is the check that ORDERED the stop,
		// which can precede the container's actual exit by its whole stop
		// timeout.
		if len(u.lastStops) > 0 {
			rows := make([]map[string]any, 0, len(u.lastStops))
			for _, r := range u.lastStops {
				rows = append(rows, map[string]any{
					"container": r.container,
					"reason":    string(r.reason),
					"wall":      r.wall,
				})
			}
			entry["last_stops"] = rows
		}
		// Only for a service that configures accessories: an empty 0/0 row on
		// every other service would read like something is missing.
		//
		// Warm accessories are counted in the same row — they are accessories,
		// and an operator counting containers wants them all — with their own
		// pair of keys whenever any are configured. The split is what makes
		// the row readable on a standby: 1/2 there is the HEALTHY reading, and
		// only warm_running against warm_total says whether the database an
		// operator is about to deploy against is up.
		if total := len(u.svc.Accessories) + len(u.svc.WarmAccessories); total > 0 {
			accessories := map[string]any{
				"running": u.accessoriesUp + u.warmUp,
				"total":   total,
			}
			if warm := len(u.svc.WarmAccessories); warm > 0 {
				accessories["warm_running"] = u.warmUp
				accessories["warm_total"] = warm
			}
			entry["accessories"] = accessories
		}
		out[key] = entry
	}
	return out
}

// FleetView is the local `conch fleet` answer: this host's own view, merged
// with the arbiter's fleet snapshot when one is reachable. It is deliberately
// not the arbiter's FleetReply: local mode exists to show BOTH what this host
// supervises and what the arbiter believes, so a lease no host's intent list
// accounts for — the post-rename drift the naming work creates — is visible
// rather than hidden behind a merge that trusts one side.
type FleetView struct {
	// Host is this host's configured name, and SupervisedHere its service
	// keys: the local truth, which does not depend on the arbiter being up.
	Host           string   `json:"host"`
	SupervisedHere []string `json:"supervised_here"`
	// Peers is the presence from the most recent batch, host -> seconds since
	// last seen. It is what the arbiter last told us, so it is stale by up to
	// one interval, and empty before the first batch.
	Peers map[string]float64 `json:"peers"`
	// Arbiter is the arbiter's own snapshot, nil when this backend has no
	// fleet endpoint (dynamo, D8) or the arbiter could not be reached. A nil
	// here is never a failed command: the local view stands on its own, with
	// Note explaining what is missing.
	Arbiter *arbiter.FleetReply `json:"arbiter,omitempty"`
	// Unsupervised names every lease the arbiter reports that no host's stated
	// intent accounts for — including this host's own config. It is the drift
	// probe, computed only when Arbiter is non-nil.
	Unsupervised []string `json:"unsupervised,omitempty"`
	// Note says why Arbiter is nil, in the operator's words, when it is.
	Note string `json:"note,omitempty"`
}

// Fleet assembles this host's local view for `conch fleet --socket`. It takes
// the same snapshot Status does, briefly, then makes the one arbiter read
// OUTSIDE the lock: the whole point of local mode is that it works while the
// arbiter is down, and a network call under s.mu would let a slow or dead
// arbiter stall the supervision loop. A missing capability or an unreachable
// arbiter is a note, not an error — the local view is still an answer.
func (s *Supervisor) Fleet(ctx context.Context) FleetView {
	view := FleetView{
		Host:           s.cfg.Host,
		SupervisedHere: append([]string(nil), s.order...),
	}
	s.mu.Lock()
	view.Peers = make(map[string]float64, len(s.lastPeers))
	for host, lastSeen := range s.lastPeers {
		view.Peers[host] = lastSeen
	}
	s.mu.Unlock()

	if s.fleet == nil {
		view.Note = "this backend has no fleet view"
		return view
	}
	reply, err := s.fleet.Fleet(ctx)
	if err != nil {
		view.Note = "the arbiter is unreachable: showing this host's view only"
		return view
	}
	view.Arbiter = &reply
	view.Unsupervised = unsupervisedLeases(view, reply)
	return view
}

// unsupervisedLeases is the drift probe: a lease the arbiter reports that no
// host's stated intent accounts for. A key is supervised if any host's last
// batch listed it in hosts[].services, or — covering a host whose presence has
// lapsed and so is omitted from the render — if it is in THIS host's config.
// A key none of those name is a lease with no owner to serve it, which is
// exactly what a service rename leaves behind.
func unsupervisedLeases(view FleetView, reply arbiter.FleetReply) []string {
	supervised := make(map[string]bool, len(view.SupervisedHere))
	for _, key := range view.SupervisedHere {
		supervised[key] = true
	}
	for _, host := range reply.Hosts {
		for _, key := range host.Services {
			supervised[key] = true
		}
	}
	var out []string
	for key := range reply.Services {
		if !supervised[key] {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// Evacuate and Reclaim, spec §9 and §10's admin verbs, live in evacuate.go
// with the rest of the sequence they start.

// waitIdle blocks until every executor has finished the work it has been
// given. It exists for tests: the whole suite is condvar- and channel-gated
// rather than timed, and this is the gate that replaces a sleep between a
// check and the assertions about what it did. The loop itself never calls it
// — that would be the thing rule 2 exists to forbid.
func (s *Supervisor) waitIdle() {
	for _, key := range s.order {
		s.units[key].ex.waitIdle()
	}
	s.sweeper.waitIdle()
}
