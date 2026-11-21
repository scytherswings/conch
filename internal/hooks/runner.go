// Package hooks runs the operator-supplied one-off containers conch executes
// at promotion and evacuation (spec §8): restore-and-verify before a service
// starts, drain-and-sync before it stops. It knows nothing about promotion
// itself — Decide (Task 7) names the RunPromoteHooks/AbortHooks actions and
// the supervisor (Task 11) calls Run and Abort — this package only knows how
// to run ordered chains of containers safely: deterministic names, no
// AutoRemove, a failed container's logs kept for reading, and a timeout conch
// itself enforces rather than trusting Docker to (spec §8 rule 3).
//
// One chain per service, any number of services at once: a host evacuating
// with `--all` drains every service simultaneously, so an abort is always
// scoped to the service it names (see `chain`).
package hooks

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/dockerapi"
)

// logTailLines bounds how much of a failed hook's output Result.Detail
// carries — enough for an operator to see why it failed, not an unbounded
// dump of a runaway container's stdout.
const logTailLines = 200

// HookLabel and ServiceLabel are stamped on every hook container. HookLabel
// is what KillOrphans filters startup cleanup on (spec §8 rule 4);
// ServiceLabel is for a human running `docker ps` to see at a glance which
// service a container belongs to — this package itself no longer relies on
// reading it back to map orphans to services (see KillOrphans's doc comment
// for why).
//
// Both are exported because a hook container carries BOTH of them, and the
// supervisor's conch.service label sweep (internal/supervisor/sweep.go) must
// therefore skip anything carrying HookLabel or it would fight this package
// over its own containers. That exclusion is only correct while both sides
// spell the labels identically, so they share one definition rather than two
// string literals that can drift apart silently.
const (
	HookLabel    = "conch.hook"
	ServiceLabel = "conch.service"
)

// chain is ONE service's hook chain. Everything about an abort is per-service
// because concurrent chains are routine, not exotic: every service has its own
// executor (Task 11), two services can promote at once, and `conch evacuate
// --all` drains an entire host's services simultaneously by design.
//
// A single shared record of "the chain in flight" is wrong three different
// ways the moment two overlap, and all three are silent:
//
//   - Abort(A) kills whichever container was published last, which may be B's,
//     while A's hook runs on untouched. An evacuation whose drain was never
//     actually cancelled reports success and releases the lease believing its
//     final sync finished — the data-loss path.
//   - Abort(B) sets one shared flag, so A's chain stops too.
//   - Run(B) clears that flag on entry, un-aborting A: an abort that had
//     already landed is lost and A's remaining hooks run.
//
// Concurrent Run calls for the SAME service still clobber each other, and
// deliberately so: that is a caller bug, and the executor's own `hooking`
// guard is what prevents it (executor.go).
type chain struct {
	// active is "Run is inside this service's chain right now", and it is what
	// makes Abort a documented no-op when nothing is running: without it, an
	// abort for an idle service would sit in the map and poison a chain that
	// started later for reasons that had nothing to do with it.
	active bool
	// inFlight is the container name of whichever hook of THIS chain is
	// running, "" in the gap between two hooks.
	inFlight string
	// aborted is sticky for the rest of this service's Run call once set: an
	// abort can land in the gap between two hooks, after one has cleared
	// inFlight and before the next has published its own, and killing the one
	// container that happened to be up is not enough to stop a chain.
	aborted bool
}

// docker is the narrow slice of dockerapi.Client this package needs, defined
// here rather than in dockerapi so a test can supply a recording fake
// without dockerapi growing test-only seams.
//
// This differs from the task brief's sketch of the interface in one way:
// Start is added. The brief omitted it, but spec §8 rule 3 is explicit that
// the sequence is "remove-if-exists -> create -> start -> wait -> read logs
// -> remove", and dockerapi.Client.Create deliberately only creates a
// container without starting it (see dockerapi's own doc comment) — a
// created-but-never-started container has nothing to Wait for; the Engine
// API's /wait blocks until a container *stops*, and one that was never
// started never does. Without calling Start, every hook would hang until
// its timeout on every run. dockerapi.Client already has a matching
// Start(name string) error method (built in Task 9 for the service
// container's own lifecycle), so this is a correction to the narrow
// interface sketch, not a change to dockerapi itself.
type docker interface {
	Create(dockerapi.RunSpec) error
	// The started bool is dockerapi's already-running 304, discarded here:
	// a hook container is freshly created one line above the call, so
	// "already running" is not a state this sequence can reach.
	Start(name string) (started bool, err error)
	Wait(name string, timeoutSeconds float64) (int, error)
	Logs(name string, tail int) string
	Remove(name string) error
	Kill(name string) error
	ListByLabel(key, value string) []string
}

var _ docker = (*dockerapi.Client)(nil)

// Result is the outcome of one Run: every hook in the chain succeeded, or
// the first one that didn't, by name, with enough of its output for an
// operator to see why.
type Result struct {
	OK         bool
	FailedHook string
	Detail     string
}

// Runner executes hook chains, at most one per service and any number of
// services at once. chains is the state Run publishes for Abort — called from
// the supervisor's loop or its deadline-watching goroutine, never Run's own —
// to read and act on without racing Run's writes. Entries are keyed by the
// exact service name Run was called with, so Abort tells "my" apart from
// "my-blog" by map lookup rather than by a name prefix, which is genuinely
// ambiguous (see KillOrphans's doc comment). Entries are never deleted: they
// are bounded by the host's configured service list, and a Run that reset its
// own entry on entry is what keeps a stale flag from meaning anything.
//
// The shape of one entry, and the three silent failures that made it
// per-service rather than global, are documented on `chain` above. The
// stickiness of `aborted` is the one property to keep in mind while reading
// Run: killing the one container that happens to be up is not enough, because
// an abort can land in the gap between two hooks, and a restore that sails
// into its next step is a container writing under a service the supervisor has
// already abandoned — exactly what spec §8 rule 4's orphan cleanup exists to
// recover from after the fact.
type Runner struct {
	d   docker
	clk clock.Clock
	log *slog.Logger

	mu     sync.Mutex
	chains map[string]*chain
}

// NewRunner builds a Runner. log carries this package's own start/finish
// narration (Run, below), scoped deliberately narrow: FAILURE DETAIL is the
// executor's to log (executor.go:373 promote, :409 evacuate), because it
// already holds the service-level context a plain hook-in-isolation log line
// would have to duplicate. What only THIS package can say is that a hook
// started and how long it ran, which is also why clk — long threaded through
// for spec §12 but unread until now — finally has a reader.
func NewRunner(d docker, clk clock.Clock, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{d: d, clk: clk, log: log, chains: map[string]*chain{}}
}

// chainLocked returns this service's record, creating it on first touch.
// Callers hold r.mu.
func (r *Runner) chainLocked(service string) *chain {
	c := r.chains[service]
	if c == nil {
		c = &chain{}
		r.chains[service] = c
	}
	return c
}

// Run executes specs in order for service, stopping at the first failure.
// Each hook's container is named conch-hook-<service>-<hookname> and
// carries labels conch.hook=true and conch.service=service; the name format
// is load-bearing — KillOrphans's known-services matching (below) and, per
// spec §8 rule 4, the supervisor's crash-mid-restore evidence both depend on
// it staying exactly this shape.
//
// Sequence per hook (spec §8 rule 3): remove-if-exists (a container left
// under this exact name by a previous failed run of THIS hook must not
// collide with today's create), create, start, wait bounded by
// spec.Timeout, read logs, and only on a confirmed exit 0, remove. A failed
// hook's container is deliberately left behind on every other path — its
// logs are the one thing an operator needs to see why, and the next run's
// remove-if-exists step is what eventually cleans it up. NEVER AutoRemove.
func (r *Runner) Run(service string, specs []config.HookSpec) Result {
	r.mu.Lock()
	c := r.chainLocked(service)
	// This chain's own state, and only this chain's: clearing a flag another
	// service's abort had just set is one of the three failures `chain`
	// documents.
	c.active, c.aborted, c.inFlight = true, false, ""
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		c.active, c.inFlight = false, ""
		r.mu.Unlock()
	}()

	for _, spec := range specs {
		name := "conch-hook-" + service + "-" + spec.Name

		// The abort check and the inFlight publish must happen as one
		// atomic step. An earlier version of this method did them as two
		// separate critical sections (check, unlock, then lock again to
		// publish) — that left a window in which Abort could acquire the
		// lock in between: it would see aborted still false (so it had
		// nothing new to signal beyond setting the flag) and read the OLD
		// inFlight value (this hook not yet published, so nothing to kill),
		// while Run — having already read aborted as false on the other
		// side of that window — would go on to start this hook anyway. One
		// full hook would run uncancelled past the abort that was meant to
		// stop it, only caught (if at all) by the NEXT iteration's check.
		//
		// Holding mu across both the read and the write closes that window
		// by construction, not by luck: Abort's own critical section (below)
		// also holds mu for its check-and-read, so by mutual exclusion the
		// two can never interleave — whichever one acquires the lock first
		// runs to completion before the other's effects become visible.
		// Either Abort's flag is already set when Run checks it here (Run
		// stops, never publishes, nothing to kill because nothing started),
		// or it isn't yet (Run publishes inFlight before Abort can act, so
		// Abort's subsequent Kill has a real name to target). There is no
		// third case left for a test to observe — see the report for the
		// concurrency tests this is verified against instead.
		r.mu.Lock()
		if c.aborted {
			r.mu.Unlock()
			return Result{OK: false, FailedHook: spec.Name, Detail: "aborted"}
		}
		c.inFlight = name
		r.mu.Unlock()

		// Start is logged only once the abort race above has already been
		// lost by Abort: a hook that never actually begins (the aborted
		// branch above) gets no start/finish pair, because it never ran.
		begin := r.clk.Mono()
		r.log.Info("hook start", "service", service, "hook", spec.Name, "image", spec.Image)

		if err := r.d.Remove(name); err != nil {
			r.clearInFlight(c)
			r.logHookFinish(service, spec.Name, begin, "failed")
			return Result{OK: false, FailedHook: spec.Name,
				Detail: fmt.Sprintf("remove-if-exists %s: %v", name, err)}
		}

		if err := r.d.Create(dockerapi.RunSpec{
			Name:    name,
			Image:   spec.Image,
			Argv:    spec.Argv,
			Binds:   spec.Binds,
			EnvFile: spec.EnvFile,
			Network: spec.Network,
			Labels:  map[string]string{HookLabel: "true", ServiceLabel: service},
		}); err != nil {
			r.clearInFlight(c)
			r.logHookFinish(service, spec.Name, begin, "failed")
			return Result{OK: false, FailedHook: spec.Name,
				Detail: fmt.Sprintf("create %s: %v", name, err)}
		}

		if _, err := r.d.Start(name); err != nil {
			r.clearInFlight(c)
			r.logHookFinish(service, spec.Name, begin, "failed")
			return Result{OK: false, FailedHook: spec.Name,
				Detail: fmt.Sprintf("start %s: %v", name, err)}
		}

		// Wait's own deadline (spec.Timeout) firing is the mechanism spec §8
		// rule 5 requires: a hanging hook is contained and bounded, never
		// left running past its budget. An error here means the timeout, not
		// a transport problem (dockerapi.Client.Wait's doc comment); Kill —
		// deliberately not Remove, so whatever the hook wrote before being
		// cut off survives for the operator, exactly like an exit-code
		// failure below.
		exitCode, err := r.d.Wait(name, spec.Timeout)
		if err != nil {
			r.d.Kill(name)
			r.clearInFlight(c)
			r.logHookFinish(service, spec.Name, begin, "failed")
			return Result{OK: false, FailedHook: spec.Name,
				Detail: fmt.Sprintf("hook %q hit its timeout after %.0fs", spec.Name, spec.Timeout)}
		}

		logs := r.d.Logs(name, logTailLines)

		if exitCode != 0 {
			r.clearInFlight(c)
			r.logHookFinish(service, spec.Name, begin, "failed")
			return Result{OK: false, FailedHook: spec.Name, Detail: logs}
		}

		// Confirmed success: only now is it safe to clean up. A failure to
		// remove here is not a hook failure — best-effort, matching Remove's
		// own "404 is success" contract in the real client — the next run's
		// remove-if-exists step is the backstop either way.
		_ = r.d.Remove(name)
		r.clearInFlight(c)
		r.logHookFinish(service, spec.Name, begin, "ok")
	}
	return Result{OK: true}
}

// Abort ends the hook chain currently running for service, if any — the
// supervisor's response to a deadline reached mid-hooks (decide's
// AbortHooks action, spec §8 rule 5: a hook in flight must not stall the
// loop or stop lease renewal). It marks the chain aborted (so Run stops
// before starting the next hook even if none is running at this exact
// instant) and, if a container is up right now, kills it too. It only
// touches state under mu and never blocks on Run itself, so it is safe to
// call from the goroutine watching the deadline while Run is blocked inside
// Wait on a different one.
//
// It touches ONE service's chain and nothing else — the container it kills is
// that service's own in-flight hook, and the flag it sets is that service's
// own — which matters because other services' chains are routinely running at
// the same time (`chain`, above).
//
// service is looked up exactly, never as a name prefix: "my" must not match a
// hook running for "my-blog" just because "conch-hook-my-" is a textual prefix
// of "conch-hook-my-blog-...". This is the same ambiguity KillOrphans resolves
// by taking the known-services list; here the exact service Run was called
// with is already the map key, so there is no parsing to get wrong.
func (r *Runner) Abort(service string) {
	r.mu.Lock()
	c := r.chainLocked(service)
	name := ""
	if c.active {
		c.aborted = true
		name = c.inFlight
	}
	r.mu.Unlock()

	if name != "" {
		r.d.Kill(name)
	}
}

// KillOrphans kills and removes every container carrying label
// conch.hook=true, then maps the survivors back to services by matching
// each container's name against the known services list. Called at startup,
// before any lease is read (spec §8 rule 4): a restore container that
// outlived a crashed conch must never keep writing under a service the
// restarted conch is about to serve.
//
// Deviation from the task brief's sketch of this method (KillOrphans()
// []string, no arguments, returning container names): a name of the form
// conch-hook-<service>-<hookname> cannot be parsed back into its service
// unambiguously, because both service and hook names may contain hyphens —
// "conch-hook-my-blog-restore" is either service "my-blog" hook "restore"
// or service "my" hook "blog-restore", and no fixed delimiter resolves that
// from the string alone. The docker interface this package consumes has no
// per-container label read (dockerapi has no such method, and this task
// does not add one to it — see the docker interface's doc comment for the
// same reasoning applied to Start). So KillOrphans instead takes the
// caller's own list of known service names and, for each orphaned
// container, matches the longest "conch-hook-<service>-" prefix among them
// — which resolves the "my" vs. "my-blog" case correctly whenever both are
// configured, since the longer, more specific name always wins over a
// shorter one that happens to be a prefix of it. The return value is
// therefore the list of SERVICE names (not container names) that had at
// least one orphan: exactly the crash-mid-restore evidence spec §5 and
// Task 11 need, in the shape Task 11 actually consumes it in (a set of
// services to treat with suspicion), just reached by a different route than
// the brief sketched.
func (r *Runner) KillOrphans(services []string) []string {
	names := r.d.ListByLabel(HookLabel, "true")

	orphaned := make(map[string]bool, len(names))
	for _, name := range names {
		r.d.Kill(name)
		r.d.Remove(name)

		best := ""
		for _, svc := range services {
			prefix := "conch-hook-" + svc + "-"
			if strings.HasPrefix(name, prefix) && len(svc) > len(best) {
				best = svc
			}
		}
		if best != "" {
			orphaned[best] = true
		}
	}

	result := make([]string, 0, len(orphaned))
	for _, svc := range services {
		if orphaned[svc] {
			result = append(result, svc)
		}
	}
	return result
}

func (r *Runner) clearInFlight(c *chain) {
	r.mu.Lock()
	c.inFlight = ""
	r.mu.Unlock()
}

// logHookFinish narrates one hook's outcome and how long it ran, in seconds
// since begin (this hook's own clock.Mono() reading, not the chain's).
// outcome is coarse ("ok"/"failed") deliberately: WHY a hook failed is the
// executor's to say (executor.go:373 promote, :409 evacuate), already
// carrying the service-level context that would otherwise be duplicated
// here.
func (r *Runner) logHookFinish(service, hook string, begin float64, outcome string) {
	r.log.Info("hook finish", "service", service, "hook", hook,
		"outcome", outcome, "duration_seconds", r.clk.Mono()-begin)
}
