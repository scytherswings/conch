package supervisor

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/decide"
)

// The conch.service label sweep (spec §11, sweep.go). Every test here is
// about the same one-sentence contract: a RUNNING container whose
// conch.service label names a service this config does not have is stopped,
// and everything else on the host is left exactly as it was.

// sweepStops is every stop the sweep could have issued for name, read off the
// recording fake's call log.
func sweepStops(t *testing.T, h *harness, name string) int {
	t.Helper()
	n := 0
	for _, c := range h.dk.calls() {
		if c == "stop "+name {
			n++
		}
	}
	return n
}

func unsupervisedWarns(handler *recordingLogHandler) []logRecord {
	return handler.of("stopping a connector this config does not account for")
}

func unsupervisedAlerts(h *harness) []event {
	var out []event
	for _, e := range h.nt.of("alert") {
		if reason, _ := e.payload["reason"].(string); strings.HasPrefix(reason, "unsupervised connector stopped") {
			out = append(out, e)
		}
	}
	return out
}

func TestAnUnlabeledContainerIsNeverSwept(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	// A running container with no conch.service label at all: somebody else's
	// container on a shared host, and none of conch's business.
	h.dk.setRunning("someone-elses-nginx", true, true)
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(2)

	if n := sweepStops(t, h, "someone-elses-nginx"); n != 0 {
		t.Fatalf("an unlabeled container was stopped %d times; the sweep acts on the label and nothing else", n)
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("an unlabeled container produced %d warnings, want 0", got)
	}
}

func TestALabelNamingAConfiguredServiceIsLeftToThatServicesReconcile(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	// Deliberately not the configured connector container itself: this pins
	// the rule the sweep actually applies — the LABEL names a configured
	// service, so the sweep has nothing to say about the container, whatever
	// it is called. Stopping it, if it should be stopped, is that service's
	// own per-check reconcile's business, which knows about leases and phases
	// where the sweep knows only about a label.
	h.dk.setRunning("cf-web-old", true, true)
	h.dk.setLabeled("cf-web-old", "web")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(2)

	if n := sweepStops(t, h, "cf-web-old"); n != 0 {
		t.Fatalf("a container labeled for a CONFIGURED service was stopped %d times by the sweep", n)
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a configured label produced %d warnings, want 0", got)
	}
}

// The deploy templates label connectors with the service NAME (spec §11),
// while everything else in this package identifies a service by its lease
// key. A service that sets lease_key must therefore be accounted for under
// both, or the sweep would stop the live, correctly labeled connector of a
// service it is supervising right now.
func TestALabelNamingAServicesNameIsAccountedForWhenTheLeaseKeyDiffers(t *testing.T) {
	handler := &recordingLogHandler{}
	cfg := testConfig("web")
	cfg.Services[0].LeaseKey = "web-prod"
	h := newRig(t, cfg)
	// STRAY containers on purpose: neither name appears anywhere in the
	// config, so the only thing that can save them is the label value being
	// accounted for. A container the config names is shielded by
	// namedByConfig whatever its label says, which would make this test pass
	// for the wrong reason and pin nothing about the accounted set.
	h.dk.setRunning("cf-web-old", true, true)
	h.dk.setLabeled("cf-web-old", "web") // the NAME, as the template writes it
	h.dk.setRunning("cf-web-older", true, true)
	h.dk.setLabeled("cf-web-older", "web-prod") // and the lease key
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(1)

	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a service with a lease_key had %d containers reported unsupervised, want 0: %+v",
			got, unsupervisedWarns(handler))
	}
	if n := sweepStops(t, h, "cf-web-old"); n != 0 {
		t.Fatalf("the service NAME must be accounted for; a container labeled with it was swept %d times", n)
	}
	if n := sweepStops(t, h, "cf-web-older"); n != 0 {
		t.Fatalf("the lease key must be accounted for too; it was swept %d times", n)
	}
}

func TestARunningConnectorForAServiceThisConfigLacksIsStopped(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	// "blog" was removed from this host's config; its connector is still up.
	h.dk.setRunning("cf-blog", true, true)
	h.dk.setLabeled("cf-blog", "blog")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(1)

	if n := sweepStops(t, h, "cf-blog"); n != 1 {
		t.Fatalf("the unsupervised connector was stopped %d times, want 1: %v", n, h.dk.calls())
	}
	if h.dk.running("cf-blog") {
		t.Fatal("the unsupervised connector is still running after the sweep")
	}

	warns := unsupervisedWarns(handler)
	if len(warns) != 1 {
		t.Fatalf("got %d warnings, want exactly 1: %+v", len(warns), warns)
	}
	if warns[0].attrs["container"] != "cf-blog" || warns[0].attrs["label"] != "blog" {
		t.Fatalf("the warning must name the container and the label value: %+v", warns[0].attrs)
	}

	alerts := unsupervisedAlerts(h)
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want exactly 1: %+v", len(alerts), alerts)
	}
	if alerts[0].payload["container"] != "cf-blog" || alerts[0].payload["label"] != "blog" {
		t.Fatalf("the alert must name the container and the label value: %+v", alerts[0].payload)
	}
	// The sweep names no service, so it must not claim one: "service" in an
	// alert payload is a configured lease key everywhere else in this package.
	if _, ok := alerts[0].payload["service"]; ok {
		t.Fatalf("the alert must not carry a service key: %+v", alerts[0].payload)
	}
}

// The telling is once per streak; the stopping is every check, because a stop
// that did not take must be retried like every other reconcile in this loop.
func TestAnUnsupervisedConnectorIsReportedOncePerStreak(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-blog", true, true)
	h.dk.setLabeled("cf-blog", "blog")
	// A daemon that cannot actually stop it, so the condition survives three
	// checks — the shape a latch has to be tested against.
	h.dk.stopStays["cf-blog"] = true
	h.dk.killStays["cf-blog"] = true
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(3)

	if got := len(unsupervisedWarns(handler)); got != 1 {
		t.Fatalf("three checks of the same unsupervised connector produced %d warnings, want 1", got)
	}
	if got := len(unsupervisedAlerts(h)); got != 1 {
		t.Fatalf("three checks produced %d alerts, want 1", got)
	}
	if n := sweepStops(t, h, "cf-blog"); n != 3 {
		t.Fatalf("the stop must be re-issued every check while it is still running: got %d, want 3", n)
	}

	// It finally goes down: the streak ends, and nothing is reported for a
	// container that is no longer running.
	h.dk.setRunning("cf-blog", true, false)
	h.checks(1)
	if got := len(unsupervisedWarns(handler)); got != 1 {
		t.Fatalf("a stopped container re-reported: %d warnings, want still 1", got)
	}

	// And it comes back — `unless-stopped` after a reboot is exactly how — so
	// it is news again.
	h.dk.setRunning("cf-blog", true, true)
	h.checks(1)
	if got := len(unsupervisedWarns(handler)); got != 2 {
		t.Fatalf("a new streak must warn again: got %d warnings, want 2", got)
	}
}

// The case where "stopping is always safe" is false. A connector this config
// supervises, wearing a label value that names no service — an operator wrote
// the container name where the service name belongs — must not be swept: on an
// ACTIVE host decide's rule 12 starts it again on the next check, and the two
// would take turns every interval, silently after the first warning, until the
// connector's readiness failures demoted the host.
func TestAContainerThisConfigNamesIsNeverSweptHoweverItIsLabelled(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.build(func(o *Options) { o.Logger = slog.New(handler) })
	h.driveToActive()

	// The connector is up and serving, and mislabeled: "cloudflared-web" is
	// its container name in someone's Kamal file, not the service name.
	h.dk.setLabeled("cf-web", "cloudflared-web")
	// The service container too, for the same reason.
	h.dk.setLabeled("app-web", "cloudflared-web")
	mark := len(h.dk.calls())

	h.checks(3)

	for _, c := range h.dk.calls()[mark:] {
		if c == "stop cf-web" || c == "stop app-web" {
			t.Fatalf("the sweep stopped a container this config names: %v", h.dk.calls()[mark:])
		}
	}
	if !h.dk.running("cf-web") {
		t.Fatal("the live connector was stopped by the sweep")
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a supervised container was reported unsupervised %d times, want 0", got)
	}
	if got := len(unsupervisedAlerts(h)); got != 0 {
		t.Fatalf("a supervised container produced %d alerts, want 0", got)
	}

	// It is still worth saying, once per streak per container: the label does
	// nothing, and the next rename will not sweep what its author expects.
	warns := handler.of("a container this config supervises carries a conch.service label")
	if len(warns) != 2 {
		t.Fatalf("got %d mislabel warnings, want one per container: %+v", len(warns), warns)
	}
}

// An accessory is config-named exactly like the connector, so it earns the
// same shield: on the host where its service is ACTIVE, decide's rule 12
// would restart a swept accessory on the next check, forever — the same
// stop/start fight the connector exemption exists to prevent.
func TestAnAccessoryThisConfigNamesIsNeverSweptHoweverItIsLabelled(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, accessoryTestConfig("web"))
	h.build(func(o *Options) { o.Logger = slog.New(handler) })
	h.driveToActive()

	// Mislabeled with a value naming no configured service — the worst case,
	// since only namedByConfig can save it from the sweep.
	h.dk.setLabeled("web-litestream", "litestream")
	mark := len(h.dk.calls())

	h.checks(3)

	for _, c := range h.dk.calls()[mark:] {
		if c == "stop web-litestream" {
			t.Fatalf("the sweep stopped a config-named accessory: %v", h.dk.calls()[mark:])
		}
	}
	if !h.dk.running("web-litestream") {
		t.Fatal("the live accessory was stopped by the sweep")
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a supervised accessory was reported unsupervised %d times, want 0", got)
	}
	// The inert label is still said out loud, once per streak.
	warns := handler.of("a container this config supervises carries a conch.service label")
	if len(warns) != 1 {
		t.Fatalf("got %d mislabel warnings, want 1: %+v", len(warns), warns)
	}
}

func TestAStoppedLabeledContainerIsLeftAlone(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-blog", true, false)
	h.dk.setLabeled("cf-blog", "blog")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(2)

	if n := sweepStops(t, h, "cf-blog"); n != 0 {
		t.Fatalf("a container that is not running was stopped %d times; there is nothing unsupervised about it", n)
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a stopped container produced %d warnings, want 0", got)
	}
}

// hooks.Run stamps BOTH conch.hook=true and conch.service=<service> on every
// hook container it creates, so the sweep would find them all. It must not
// touch them: killing a running restore mid-flight tears exactly the data
// spec §8 rule 4's orphan sweep exists to protect, and the hook runner owns
// those containers at their own timeouts.
func TestHookContainersAreExcludedFromTheSweep(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	// The load-bearing case: the label value names a service this config does
	// NOT have (the service was removed while its evacuate hook was running),
	// so the hook label is the only thing keeping this container alive.
	h.dk.setRunning("conch-hook-blog-drain", true, true)
	h.dk.setHookLabeled("conch-hook-blog-drain", "blog")
	// And the ordinary case, for good measure.
	h.dk.setRunning("conch-hook-web-restore", true, true)
	h.dk.setHookLabeled("conch-hook-web-restore", "web")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(2)

	for _, name := range []string{"conch-hook-blog-drain", "conch-hook-web-restore"} {
		if n := sweepStops(t, h, name); n != 0 {
			t.Fatalf("the sweep stopped hook container %s %d times; the hook runner owns those", name, n)
		}
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a hook container produced %d unsupervised warnings, want 0", got)
	}
}

func TestADryRunSweepLogsAndStopsNothing(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-blog", true, true)
	h.dk.setLabeled("cf-blog", "blog")
	h.build(func(o *Options) {
		o.Logger = slog.New(handler)
		o.DryRun = true
	})

	h.checks(1)

	if got := h.dk.mutations(); len(got) != 0 {
		t.Fatalf("a dry run must change nothing on the host: %v", got)
	}
	if !h.dk.running("cf-blog") {
		t.Fatal("the unsupervised connector was stopped in a dry run")
	}
	said := false
	for _, r := range handler.of("dry run: would stop container") {
		if r.attrs["container"] == "cf-blog" {
			said = true
		}
	}
	if !said {
		t.Fatal("a dry run must say which container it would have stopped")
	}
	// The warning still fires — it is a log line, not an action — but the
	// webhook does not: spec §10, a dry run performs no action, and a webhook
	// pages a human.
	if got := len(unsupervisedWarns(handler)); got != 1 {
		t.Fatalf("got %d warnings in a dry run, want 1", got)
	}
	if got := len(unsupervisedAlerts(h)); got != 0 {
		t.Fatalf("a dry run sent %d webhooks, want 0", got)
	}
}

// The sweeper is a pair of hands of its own, not a turn in some service's
// queue: an unsupervised connector that takes its full 35s to drain must not
// delay the service executors' own work.
func TestTheSweepDoesNotShareAServiceExecutor(t *testing.T) {
	h := newHarness(t, testConfig("web"), nil)
	for _, key := range h.sup.order {
		if h.sup.units[key].ex == h.sup.sweeper {
			t.Fatal("the sweeper must not be a configured service's executor")
		}
	}
	if _, ok := h.sup.units[sweepScope]; ok {
		t.Fatal("the sweep's synthetic unit must not be registered as a service")
	}
	if got := h.sup.sweeper.u.svc.Connector.StopTimeout; got != defaultConnectorStopTimeout {
		t.Fatalf("the sweep stops with the default connector stop timeout: got %v, want %v",
			got, float64(defaultConnectorStopTimeout))
	}
}

// An empty label value is a deploy template whose interpolation produced
// nothing. It stops nothing — "" accounts for no service, and stopping on it
// would turn a typo into an outage — but it is inert, and its author believes
// it is protecting them, so it is said once per streak.
func TestAnEmptyLabelValueIsShieldedButSaidOutLoud(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-orphan", true, true)
	h.dk.setLabeled("cf-orphan", "")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(3)

	if n := sweepStops(t, h, "cf-orphan"); n != 0 {
		t.Fatalf("an empty label value was stopped %d times; it names no service and proves nothing", n)
	}
	if got := len(unsupervisedAlerts(h)); got != 0 {
		t.Fatalf("an empty label sent %d webhooks, want 0: it is config hygiene, not an incident", got)
	}
	warns := handler.of("empty conch.service label")
	if len(warns) != 1 {
		t.Fatalf("three checks produced %d empty-label warnings, want exactly 1: %+v", len(warns), warns)
	}
	if warns[0].attrs["container"] != "cf-orphan" {
		t.Fatalf("the warning must name the container: %+v", warns[0].attrs)
	}
}

// A trailing-* container pattern names ONE container per check — whichever
// this check's resolution picked — not every name that happens to start with
// it. A rogue container sitting under the pattern, labeled for a service this
// config does not have, is exactly as unsupervised as one named anything else.
func TestAPrefixPatternShieldsOnlyTheNameItResolvedTo(t *testing.T) {
	handler := &recordingLogHandler{}
	cfg := testConfig("web")
	cfg.Services[0].Container = "app-*" // Kamal's versioned container names
	h := newRig(t, cfg)
	h.dk.setResolution("app-", "app-web-v1")
	h.dk.setRunning("app-web-v1", true, true)
	// The container this check actually resolved to, mislabeled: shielded,
	// because stopping it would fight the reconcile that supervises it.
	h.dk.setLabeled("app-web-v1", "cloudflared-web")
	// And a rogue container under the same pattern, labeled for a service this
	// config has never heard of: nothing supervises it, and the pattern is not
	// an exemption.
	h.dk.setRunning("app-blog-cf", true, true)
	h.dk.setLabeled("app-blog-cf", "blog")
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	h.checks(1)

	if n := sweepStops(t, h, "app-web-v1"); n != 0 {
		t.Fatalf("the resolved container was swept %d times; this check's reconcile owns it", n)
	}
	if n := sweepStops(t, h, "app-blog-cf"); n != 1 {
		t.Fatalf("the rogue container under the pattern was stopped %d times, want 1: %v", n, h.dk.calls())
	}
	alerts := unsupervisedAlerts(h)
	if len(alerts) != 1 || alerts[0].payload["container"] != "app-blog-cf" {
		t.Fatalf("the rogue container must be the one alerted about: %+v", alerts)
	}
	warns := unsupervisedWarns(handler)
	if len(warns) != 1 || warns[0].attrs["container"] != "app-blog-cf" {
		t.Fatalf("only the rogue container is unsupervised: %+v", warns)
	}
	// And the resolved one is reported as what it is: supervised, wearing a
	// label that does nothing.
	mislabeled := handler.of("a container this config supervises carries a conch.service label")
	if len(mislabeled) != 1 || mislabeled[0].attrs["container"] != "app-web-v1" {
		t.Fatalf("the mislabeled resolved container must be reported as mislabeled: %+v", mislabeled)
	}
}

// The verdict about one container can change with nothing on the host
// changing at all: a blue/green deploy moves which container a trailing-*
// pattern resolves to, and the container that was shielded a moment ago —
// warned about as supervised-but-mislabeled — is now unsupervised and gets
// stopped. That transition is the one an operator most needs to hear, so it
// must not be swallowed by the streak latch that was set while the container
// was merely being warned about.
func TestAVerdictThatFlipsToAStopIsAlwaysToldAbout(t *testing.T) {
	handler := &recordingLogHandler{}
	cfg := testConfig("web")
	cfg.Services[0].Container = "app-*"
	h := newRig(t, cfg)
	h.dk.setResolution("app-", "app-web-v1")
	h.dk.setRunning("app-web-v1", true, true)
	h.dk.setLabeled("app-web-v1", "cloudflared-web") // names no configured service
	h.build(func(o *Options) { o.Logger = slog.New(handler) })

	// Check one: app-web-v1 is what the pattern resolves to, so it is
	// shielded and reported as mislabeled — and the latch is now set for it.
	h.checks(1)
	if n := sweepStops(t, h, "app-web-v1"); n != 0 {
		t.Fatalf("the resolved container was swept %d times, want 0", n)
	}
	if got := len(handler.of("a container this config supervises carries a conch.service label")); got != 1 {
		t.Fatalf("got %d mislabel warnings on the first check, want 1", got)
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("got %d unsupervised warnings while it was still the resolved container, want 0", got)
	}

	// The deploy lands: the pattern now resolves to v2, and v1 — still up,
	// still labeled for nothing this config has — is nobody's.
	h.dk.setRunning("app-web-v2", true, true)
	h.dk.setResolution("app-", "app-web-v2")

	h.checks(1)

	if n := sweepStops(t, h, "app-web-v1"); n != 1 {
		t.Fatalf("the container that stopped being resolved was stopped %d times, want 1: %v", n, h.dk.calls())
	}
	warns := unsupervisedWarns(handler)
	if len(warns) != 1 || warns[0].attrs["container"] != "app-web-v1" {
		t.Fatalf("a stop must always be told about, however it was latched before: %+v", warns)
	}
	alerts := unsupervisedAlerts(h)
	if len(alerts) != 1 || alerts[0].payload["container"] != "app-web-v1" {
		t.Fatalf("a stop must always send its webhook, however it was latched before: %+v", alerts)
	}
	if n := sweepStops(t, h, "app-web-v2"); n != 0 {
		t.Fatalf("the newly resolved container was swept %d times, want 0", n)
	}
}

// A config knob would be a way to turn this off, and there is none by design
// (spec §11 states it as reconcile behaviour, not policy). This pins the one
// property that makes always-on safe: the sweep's only verb is stop.
func TestTheSweepOnlyEverStops(t *testing.T) {
	h := newRig(t, testConfig("web"))
	h.dk.setRunning("cf-blog", true, true)
	h.dk.setLabeled("cf-blog", "blog")
	h.dk.setRunning("cf-gone", true, false)
	h.dk.setLabeled("cf-gone", "gone")
	h.build(nil)

	h.checks(2)

	for _, c := range h.dk.calls() {
		if strings.HasSuffix(c, " cf-blog") || strings.HasSuffix(c, " cf-gone") {
			verb := strings.Fields(c)[0]
			if verb != "stop" && verb != "kill" && verb != "inspect" {
				t.Fatalf("the sweep did %q; its only verb is stop (escalating to kill)", c)
			}
		}
	}
}

// A warm accessory is config-named exactly like the connector and the
// shipper, so it earns the same shield — and it needs it more than either:
// the warm reconcile starts it again on the NEXT check, on every host, so a
// sweep that stopped it would fight this loop once per interval forever, on
// both hosts at once, with the service's database as the rope.
func TestAWarmAccessoryThisConfigNamesIsNeverSweptHoweverItIsLabelled(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newRig(t, warmTestConfig("web"))
	h.build(func(o *Options) { o.Logger = slog.New(handler) })
	// A standby, which is where a warm accessory spends most of its life and
	// the one place its running container looks unaccounted-for.
	h.arb.reply = func(int, string, arbiter.Op) arbiter.Verdict {
		return arbiter.Verdict{Lease: decide.Held, Holder: "host-b", Serving: true}
	}
	h.check()

	// Mislabeled with a value naming no configured service — the worst case,
	// since only namedByConfig can save it from the sweep.
	h.dk.setLabeled("web-db", "postgres")
	mark := len(h.dk.calls())

	h.checks(3)

	for _, c := range h.dk.calls()[mark:] {
		if c == "stop web-db" {
			t.Fatalf("the sweep stopped a config-named warm accessory: %v", h.dk.calls()[mark:])
		}
	}
	if !h.dk.running("web-db") {
		t.Fatal("the live warm accessory was stopped by the sweep")
	}
	if got := len(unsupervisedWarns(handler)); got != 0 {
		t.Fatalf("a supervised warm accessory was reported unsupervised %d times, want 0", got)
	}
}
