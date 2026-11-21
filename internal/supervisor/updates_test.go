package supervisor

import (
	"context"
	"errors"
	"testing"

	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/updates"
	"log/slog"
)

// fakeUpdatesSource is the version source the update-check tests inject: a
// canned latest tag or error, with a call counter for "was it consulted".
type fakeUpdatesSource struct {
	latest string
	err    error
	calls  int
}

func (f *fakeUpdatesSource) LatestCloudflared(ctx context.Context) (string, error) {
	f.calls++
	return f.latest, f.err
}

// updatesHarness wires a supervisor whose config enables the daily check and
// whose source is the fake, with the connector under test reporting the given
// cloudflared image.
func updatesHarness(t *testing.T, image string, src *fakeUpdatesSource) (*harness, *recordingLogHandler) {
	t.Helper()
	cfg := testConfig("web")
	cfg.Updates = config.UpdatesConfig{Interval: "24h", TimeoutSeconds: 5}
	handler := &recordingLogHandler{}
	h := newHarness(t, cfg, func(o *Options) {
		o.Updates = src
		o.Logger = slog.New(handler)
	})
	h.dk.setRunning("cf-web", true, true)
	if image != "" {
		h.dk.setImage("cf-web", image)
	}
	return h, handler
}

func TestUpdateCheckWarnsOnceOnNewerCloudflared(t *testing.T) {
	src := &fakeUpdatesSource{latest: "2026.10.5"}
	h, handler := updatesHarness(t, "cloudflare/cloudflared:2026.10.4", src)

	for i := 0; i < 3; i++ {
		h.sup.checkCloudflaredUpdates(context.Background())
	}
	if src.calls != 3 {
		t.Fatalf("source consulted %d times, want 3 (once per check)", src.calls)
	}

	u := h.sup.Status()["updates"].(map[string]any)["cloudflared"].(map[string]any)
	if u["running"] != "2026.10.4" || u["latest"] != "2026.10.5" || u["ok"] != false {
		t.Fatalf("verdict = %v, want running 2026.10.4 / latest 2026.10.5 / ok false", u)
	}

	// Said once: one alert and one warn for three identical checks.
	alerts := h.nt.of("alert")
	if len(alerts) != 1 {
		t.Fatalf("alert count = %d, want 1 (a change is news once)", len(alerts))
	}
	if warns := handler.of("a newer cloudflared release is available"); len(warns) != 1 {
		t.Fatalf("warn count = %d, want 1", len(warns))
	}
}

func TestUpdateCheckReAlertsWhenVerdictChanges(t *testing.T) {
	src := &fakeUpdatesSource{latest: "2026.10.5"}
	h, _ := updatesHarness(t, "cloudflare/cloudflared:2026.10.4", src)

	h.sup.checkCloudflaredUpdates(context.Background())
	src.latest = "2026.10.4" // the operator bumps the pin: resolved
	h.sup.checkCloudflaredUpdates(context.Background())
	u := h.sup.Status()["updates"].(map[string]any)["cloudflared"].(map[string]any)
	if u["ok"] != true {
		t.Fatalf("after the bump the verdict should be up to date, got %v", u)
	}
	if len(h.nt.of("alert")) != 1 {
		t.Fatalf("alerts after resolution = %d, want still 1", len(h.nt.of("alert")))
	}

	src.latest = "2026.10.6" // a second, later release: news again
	h.sup.checkCloudflaredUpdates(context.Background())
	if n := len(h.nt.of("alert")); n != 2 {
		t.Fatalf("alerts after a second newer release = %d, want 2", n)
	}
}

func TestUpdateCheckUpToDateIsSilent(t *testing.T) {
	src := &fakeUpdatesSource{latest: "2026.10.4"}
	h, handler := updatesHarness(t, "cloudflare/cloudflared:2026.10.4", src)

	h.sup.checkCloudflaredUpdates(context.Background())

	u := h.sup.Status()["updates"].(map[string]any)["cloudflared"].(map[string]any)
	if u["ok"] != true {
		t.Fatalf("verdict = %v, want ok true", u)
	}
	if len(h.nt.of("alert")) != 0 || len(handler.of("a newer cloudflared release is available")) != 0 {
		t.Fatal("an up-to-date check must not alert or warn")
	}
}

func TestUpdateCheckSourceFailureKeepsLastVerdictAndLogsOnce(t *testing.T) {
	src := &fakeUpdatesSource{latest: "2026.10.5"}
	h, handler := updatesHarness(t, "cloudflare/cloudflared:2026.10.4", src)
	h.sup.checkCloudflaredUpdates(context.Background()) // builds an update-available verdict

	src.err = errors.New("hub down")
	h.sup.checkCloudflaredUpdates(context.Background())
	h.sup.checkCloudflaredUpdates(context.Background())
	if n := len(handler.of("the update check could not reach its version source")); n != 1 {
		t.Fatalf("failure warning count = %d, want 1 per streak", n)
	}
	// The failure does not retract the finding: an un-reachable source is
	// "no new information", not "all clear".
	u := h.sup.Status()["updates"].(map[string]any)["cloudflared"].(map[string]any)
	if u["latest"] != "2026.10.5" || u["ok"] != false {
		t.Fatalf("verdict after a failed source = %v, want the last good answer kept", u)
	}

	src.err = nil
	h.sup.checkCloudflaredUpdates(context.Background())
	if n := len(handler.of("the update check could not reach its version source")); n != 1 {
		t.Fatalf("failure warning count after recovery = %d, want still 1 (the streak closed)", n)
	}
}

func TestUpdateCheckClearsVerdictWhenNoCloudflaredConnector(t *testing.T) {
	src := &fakeUpdatesSource{latest: "2026.10.5"}
	h, _ := updatesHarness(t, "cloudflare/cloudflared:2026.10.4", src)
	h.sup.checkCloudflaredUpdates(context.Background())
	if _, ok := h.sup.Status()["updates"]; !ok {
		t.Fatal("precondition: a verdict should exist before the connector goes away")
	}

	h.dk.setImage("cf-web", "quay.io/someother/connector:2.0")
	h.sup.checkCloudflaredUpdates(context.Background())
	if _, ok := h.sup.Status()["updates"]; ok {
		t.Fatalf("a host with no cloudflared connector must report no update verdict, got %v", h.sup.Status()["updates"])
	}
}

func TestUpdateCheckDisabledSourceIsNoOp(t *testing.T) {
	cfg := testConfig("web")
	cfg.Updates = config.UpdatesConfig{TimeoutSeconds: 5} // no interval: check disabled
	handler := &recordingLogHandler{}
	h := newHarness(t, cfg, func(o *Options) { o.Logger = slog.New(handler) })
	h.dk.setRunning("cf-web", true, true)
	h.dk.setImage("cf-web", "cloudflare/cloudflared:2026.10.4")
	h.sup.checkCloudflaredUpdates(context.Background())
	if _, ok := h.sup.Status()["updates"]; ok {
		t.Fatal("a disabled check must never produce a verdict")
	}
}

func TestCloudflaredTagHelper(t *testing.T) {
	good := []string{
		"cloudflare/cloudflared:2026.10.4",
		"cloudflared:2026.7.3",
		"registry.example.com/cloudflare/cloudflared:2026.10.4",
		"cloudflare/cloudflared:2026.10.4@sha256:abc",
	}
	for _, ref := range good {
		if _, ok := updates.CloudflaredTag(ref); !ok {
			t.Errorf("CloudflaredTag(%q) should parse", ref)
		}
	}
	bad := []string{
		"cloudflare/cloudflared:latest",
		"cloudflare/cloudflared@sha256:abc", // digest-pinned, no tag: nothing to compare
		"ghcr.io/example/widget:2026.10.4",
		"cloudflared:", "",
	}
	for _, ref := range bad {
		if tag, ok := updates.CloudflaredTag(ref); ok {
			t.Errorf("CloudflaredTag(%q) = %v, true; should be false", ref, tag)
		}
	}
}
