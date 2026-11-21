package supervisor

import (
	"log/slog"
	"testing"
)

// ---------------------------------------------------------------------------
// Container lifecycle logging (RCA R5b): a dozen container stops/starts with
// zero log output is the exact failure stopContainer/startContainer must no
// longer be capable of. Failures already log (Warn escalation, Error on a
// container that survives stop+kill, Error on a failed start) — these tests
// pin the missing half, the SUCCESSFUL case, and pin that a dry run adds
// nothing beyond its own existing "would stop/start" lines.
// ---------------------------------------------------------------------------

// TestStopContainerLogsInfoOnSuccess pins the plain case: Stop succeeds, the
// first Inspect confirms it, no kill escalation needed.
func TestStopContainerLogsInfoOnSuccess(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, true)
	h.sup.units["web"].ex.stopContainer("cf-web", 5, reasonStandbyFence, 0, true)

	got := handler.of("stopped container")
	if len(got) != 1 {
		t.Fatalf("want exactly one stopped-container Info record, got %d: %+v", len(got), got)
	}
	if got[0].level != slog.LevelInfo {
		t.Fatalf("want Info level, got %v", got[0].level)
	}
	if got[0].attrs["service"] != "web" || got[0].attrs["container"] != "cf-web" {
		t.Fatalf("record missing service/container attrs: %+v", got[0].attrs)
	}
}

// TestStopContainerLogsInfoOnSuccessAfterKillEscalation covers the escalated
// path: Stop's own Inspect still finds the container running (stopStays), so
// Kill runs too — the eventual success still gets exactly one Info record,
// not one per stage, alongside the Warn escalation logs that already exist.
func TestStopContainerLogsInfoOnSuccessAfterKillEscalation(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, true)
	h.dk.mu.Lock()
	h.dk.stopStays["cf-web"] = true // Stop returns nil but the container keeps running
	h.dk.mu.Unlock()

	h.sup.units["web"].ex.stopContainer("cf-web", 5, reasonStandbyFence, 0, true)

	got := handler.of("stopped container")
	if len(got) != 1 {
		t.Fatalf("want exactly one stopped-container Info record even after a kill escalation, got %d: %+v",
			len(got), got)
	}
	if len(handler.of("the container is still running after a stop: killing it")) != 1 {
		t.Fatal("want the existing kill-escalation Warn to still fire untouched")
	}
}

// TestStopContainerLogsNoInfoWhenItSurvivesStopAndKill is the negative case:
// a container that outlives both stop and kill is a failure (Error already
// logs it), and must not also get a success Info record.
func TestStopContainerLogsNoInfoWhenItSurvivesStopAndKill(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, true)
	h.dk.mu.Lock()
	h.dk.stopStays["cf-web"] = true
	h.dk.killStays["cf-web"] = true
	h.dk.mu.Unlock()

	h.sup.units["web"].ex.stopContainer("cf-web", 5, reasonStandbyFence, 0, true)

	if got := handler.of("stopped container"); len(got) != 0 {
		t.Fatalf("a container that survives stop and kill must not log a success Info, got %+v", got)
	}
	if len(handler.of("the container survived stop and kill: check the docker daemon on this host")) != 1 {
		t.Fatal("want the existing Error to still fire untouched")
	}
}

// TestStopContainerLogsNoInfoWhenInspectErrors covers the other negative
// case: an Inspect that itself failed is not evidence the container is
// down — stopContainer's own doc comment says as much — so it must not be
// read as a verified stop and log the success line.
func TestStopContainerLogsNoInfoWhenInspectErrors(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, true)
	h.dk.mu.Lock()
	h.dk.inspectErr["cf-web"] = true
	h.dk.mu.Unlock()

	h.sup.units["web"].ex.stopContainer("cf-web", 5, reasonStandbyFence, 0, true)

	if got := handler.of("stopped container"); len(got) != 0 {
		t.Fatalf("an unverifiable Inspect must never log a success Info, got %+v", got)
	}
}

// TestStartContainerLogsInfoOnSuccess is stopContainer's success test
// mirrored for the start half of the lifecycle.
func TestStartContainerLogsInfoOnSuccess(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.sup.units["web"].ex.startContainer("cf-web")

	got := handler.of("started container")
	if len(got) != 1 {
		t.Fatalf("want exactly one started-container Info record, got %d: %+v", len(got), got)
	}
	if got[0].level != slog.LevelInfo {
		t.Fatalf("want Info level, got %v", got[0].level)
	}
	if got[0].attrs["service"] != "web" || got[0].attrs["container"] != "cf-web" {
		t.Fatalf("record missing service/container attrs: %+v", got[0].attrs)
	}
}

// TestStopContainerSaysSoWhenTheContainerWasAlreadyDown is F1: decide names
// StopConnector and StopService unconditionally, so the escalation is run
// against containers that had already fallen over — and status has always
// declined to credit conch with those (stops.go). The log has to decline the
// same way, or one host tells two stories about one stop. It still narrates:
// conch really did make the docker calls, and silence would be its own kind
// of gap.
func TestStopContainerSaysSoWhenTheContainerWasAlreadyDown(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, false) // it fell over before conch got here
	h.sup.units["web"].ex.stopContainer("cf-web", 5, reasonDemotion, 0, false)

	if got := handler.of("stopped container"); len(got) != 0 {
		t.Fatalf("conch did not stop a container that was already down, got %+v", got)
	}
	got := handler.of("the container was already down")
	if len(got) != 1 {
		t.Fatalf("want exactly one already-down Info record, got %d: %+v", len(got), got)
	}
	if got[0].level != slog.LevelInfo {
		t.Fatalf("want Info level, got %v", got[0].level)
	}
	if got[0].attrs["service"] != "web" || got[0].attrs["container"] != "cf-web" {
		t.Fatalf("record missing service/container attrs: %+v", got[0].attrs)
	}
	// And the other surface agrees, which is the whole point of the fix.
	if got := stopReasonOf(t, h.sup, "web", "cf-web"); got != "" {
		t.Fatalf("no row either, got %q", got)
	}
}

// TestStartContainerLogsNothingWhenItWasAlreadyRunning is F2 at its narrowest:
// docker answers 304 for a container that was already up, and a start that
// changed nothing is not an event. The CALL still happens — reconciling
// towards a state is idempotent by design — so this pins the narration only.
func TestStartContainerLogsNothingWhenItWasAlreadyRunning(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("cf-web", true, true)
	h.sup.units["web"].ex.startContainer("cf-web")

	if got := handler.of("started container"); len(got) != 0 {
		t.Fatalf("a container that was already running was not started by conch, got %+v", got)
	}
	if len(h.dk.mutations()) != 1 || h.dk.mutations()[0] != "start cf-web" {
		t.Fatalf("the start itself must still be issued, got %v", h.dk.mutations())
	}
}

// TestStartWarmContainerLogsOnlyRealTransitions is the same on the warm path,
// both ways round: the warm start is re-derived on EVERY check that finds any
// one warm accessory down, so this is the site where a false start line would
// repeat forever rather than once.
func TestStartWarmContainerLogsOnlyRealTransitions(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, warmTestConfig("web"), func(o *Options) { o.Logger = slog.New(handler) })
	ex := h.sup.units["web"].ex

	h.dk.setRunning("web-db", true, false)
	ex.startWarmContainer("web-db") // a genuine cold start: one line
	if got := handler.of("started container"); len(got) != 1 {
		t.Fatalf("a cold start is an event and must be said once, got %d: %+v", len(got), got)
	}

	ex.startWarmContainer("web-db") // now up: nothing changed, nothing said
	ex.startWarmContainer("web-db")
	if got := handler.of("started container"); len(got) != 1 {
		t.Fatalf("a running warm accessory started again is not an event, got %d: %+v", len(got), got)
	}
}

// TestOnlyTheDownedWarmSiblingNarratesItsStart is the reported scenario end to
// end, through a real check rather than a direct call. startWarmAccessories
// starts the WHOLE configured list whenever any one member is found down
// (supervisor.go), which is deliberate — but before F2 it meant that a single
// crash-looping accessory made every healthy sibling claim a start, every
// interval, on both hosts, for as long as the crash loop lasted.
func TestOnlyTheDownedWarmSiblingNarratesItsStart(t *testing.T) {
	cfg := warmTestConfig("web")
	cfg.Services[0].WarmAccessories = append(cfg.Services[0].WarmAccessories, "web-garage")
	handler := &recordingLogHandler{}
	h := newHarness(t, cfg, func(o *Options) { o.Logger = slog.New(handler) })

	h.dk.setRunning("web-db", true, true)      // healthy sibling, never stopped
	h.dk.setRunning("web-garage", true, false) // the one that is actually down

	h.check()

	got := handler.of("started container")
	if len(got) != 1 {
		t.Fatalf("exactly one container transitioned, so exactly one start is news, got %d: %+v",
			len(got), got)
	}
	if got[0].attrs["container"] != "web-garage" {
		t.Fatalf("the start must name the container that was actually down, got %+v", got[0].attrs)
	}
	// Non-vacuous: conch really did call start on both, which is the behaviour
	// this test must NOT be read as changing.
	var starts []string
	for _, c := range h.dk.calls() {
		if c == "start web-db" || c == "start web-garage" {
			starts = append(starts, c)
		}
	}
	if len(starts) != 2 {
		t.Fatalf("the whole warm list is still started, got %v", starts)
	}
}

// TestDryRunStopAndStartAddNoNewRecords pins the brief's "dry-run paths
// byte-identical" requirement: the existing "dry run: would stop/start
// container" lines still fire, and NOTHING else does.
func TestDryRunStopAndStartAddNoNewRecords(t *testing.T) {
	handler := &recordingLogHandler{}
	h := newHarness(t, testConfig("web"), func(o *Options) {
		o.Logger = slog.New(handler)
		o.DryRun = true
	})

	ex := h.sup.units["web"].ex
	ex.stopContainer("cf-web", 5, reasonStandbyFence, 0, true)
	ex.startContainer("cf-web")

	if got := handler.of("stopped container"); len(got) != 0 {
		t.Fatalf("a dry run must never log the success line, got %+v", got)
	}
	if got := handler.of("started container"); len(got) != 0 {
		t.Fatalf("a dry run must never log the success line, got %+v", got)
	}
	if len(handler.of("dry run: would stop container")) != 1 {
		t.Fatal("the existing dry-run stop line must still fire")
	}
	if len(handler.of("dry run: would start container")) != 1 {
		t.Fatal("the existing dry-run start line must still fire")
	}
}
