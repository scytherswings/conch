package config

import (
	"reflect"
	"strings"
	"testing"
)

// validDynamoConfig is a minimal, otherwise-fault-free dynamo-backend config
// (no services), built as a struct literal per the brief rather than
// round-tripped through YAML — Validate is the unit under test here, not
// Load/decode. The Docker, Updates and Disk blocks carry the values
// defaults() (config.go) would have filled in, because Validate now rejects a
// non-positive docker.timeout: a struct literal that skipped Load must still
// look like a config that went through it.
func validDynamoConfig() Config {
	return Config{
		Host: "host-a",
		Arbiter: ArbiterConfig{
			Backend:         "dynamo",
			OnLoss:          "freeze",
			TimeoutSeconds:  5,
			Table:           "conch-leases",
			Region:          "us-east-1",
			CredentialsFile: "/etc/conch/aws-credentials",
		},
		Timing: TimingConfig{
			TTL:          60,
			Interval:     3,
			DemoteAfter:  3,
			PromoteAfter: 6,
		},
		Docker:  DockerConfig{Socket: "/var/run/docker.sock", TimeoutSeconds: 30},
		Updates: UpdatesConfig{Interval: "24h", TimeoutSeconds: 5},
		Disk:    DiskConfig{MinFreeGB: 10},
	}
}

// validCloudflareConfig is the equivalent baseline for the existing backend.
func validCloudflareConfig() Config {
	return Config{
		Host: "host-a",
		Arbiter: ArbiterConfig{
			Backend:        "cloudflare",
			URL:            "https://conch-lease.example.workers.dev",
			SecretEnv:      "CONCH_AUTH_SECRET",
			OnLoss:         "freeze",
			TimeoutSeconds: 5,
		},
		Timing: TimingConfig{
			TTL:          60,
			Interval:     3,
			DemoteAfter:  3,
			PromoteAfter: 6,
		},
		Docker:  DockerConfig{Socket: "/var/run/docker.sock", TimeoutSeconds: 30},
		Updates: UpdatesConfig{Interval: "24h", TimeoutSeconds: 5},
		Disk:    DiskConfig{MinFreeGB: 10},
	}
}

// dynamoServices builds n distinct, individually-valid services so the
// capacity-guard test's fault is attributable only to volume, not to some
// incidental duplicate key.
func dynamoServices(n int) []Service {
	services := make([]Service, n)
	for i := range services {
		name := "svc" + string(rune('a'+i))
		services[i] = Service{
			Name:      name,
			Container: name + "-web",
			Connector: ConnectorConfig{Container: "cloudflared-" + name},
			Health:    HealthConfig{URL: "http://" + name + "-web:8080/up"},
		}
	}
	return services
}

func TestDynamoBackendRequiresTableRegionCreds(t *testing.T) {
	c := validDynamoConfig()
	c.Arbiter.Table = ""
	c.Arbiter.Region = ""
	c.Arbiter.CredentialsFile = ""
	faults := Validate(c)
	joined := strings.Join(faults, "\n")
	for _, want := range []string{"arbiter.table", "arbiter.region", "arbiter.credentials_file"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing fault about %q in: %v", want, faults)
		}
	}
}

func TestDynamoBackendRejectsCloudflareFields(t *testing.T) {
	c := validDynamoConfig()
	c.Arbiter.URL = "https://conch-lease.example.workers.dev"
	c.Arbiter.SecretEnv = "CONCH_AUTH_SECRET"
	c.Arbiter.SecretFile = "/run/secrets/conch"
	faults := Validate(c)
	joined := strings.Join(faults, "\n")
	for _, want := range []string{"arbiter.url", "arbiter.secret_env", "arbiter.secret_file"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing fault about %q in: %v", want, faults)
		}
	}
}

func TestDynamoTTLBelowTenIsValid(t *testing.T) {
	c := validDynamoConfig()
	c.Timing.TTL = 6
	c.Timing.Interval = 0.5
	faults := Validate(c)
	if len(faults) != 0 {
		t.Fatalf("dynamo has no ttl floor, want valid, got %v", faults)
	}
}

func TestCloudflareTTLFloorStillEnforced(t *testing.T) {
	c := validCloudflareConfig()
	c.Timing.TTL = 6
	faults := Validate(c)
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "timing.ttl") || !strings.Contains(joined, "[10, 300]") {
		t.Fatalf("cloudflare must still enforce the ttl floor, got %v", faults)
	}
}

func TestUnknownBackendFault(t *testing.T) {
	c := validCloudflareConfig()
	c.Arbiter.Backend = "bogus"
	faults := Validate(c)
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "arbiter.backend") || !strings.Contains(joined, "bogus") {
		t.Fatalf("want an arbiter.backend fault, got %v", faults)
	}
}

func TestDynamoCapacityGuardWarnsButNeverRefuses(t *testing.T) {
	// Codebase rule: conch recommends, it does not limit. Exceeding the
	// free tier is a spending decision, not a broken config — so the
	// capacity guard must surface as a warning while Validate stays clean,
	// or an operator who provisioned (and pays for) more capacity is locked
	// out of their own timing by the free-tier default.
	c := validDynamoConfig()
	c.Timing.Interval = 0.05
	c.Services = dynamoServices(10)
	if faults := Validate(c); len(faults) != 0 {
		t.Fatalf("an over-budget config must still be valid, got faults %v", faults)
	}
	warnings := Warnings(c)
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "warning") || !(strings.Contains(joined, "WCU") || strings.Contains(joined, "capacity")) {
		t.Fatalf("want a dynamo capacity warning, got %v", warnings)
	}
}

func TestWarningsSilentForCloudflareBackend(t *testing.T) {
	// The capacity math is dynamo arithmetic; a cloudflare config at any
	// interval must produce no CAPACITY warnings from it. Webhooks are set
	// here so the unrelated webhook warning (tested separately below) can't
	// leak into this assertion.
	c := validCloudflareConfig()
	c.Timing.Interval = 0.05
	c.Webhooks = fullWebhookConfig()
	if w := Warnings(c); len(w) != 0 {
		t.Fatalf("want no warnings for cloudflare backend, got %v", w)
	}
}

// TestDynamoTTLVsHysteresisLawStillApplies pins that the per-service
// ttl-vs-hysteresis threshold (interval*demote_after + stop deadline) still
// fires under the dynamo backend, even though the dynamo branch drops the
// ttlMin floor — the brief calls this out by name as a law that must keep
// working for both backends.
func TestDynamoTTLVsHysteresisLawStillApplies(t *testing.T) {
	c := validDynamoConfig()
	// threshold = interval*demote_after + stop_deadline
	//           = 3*3 + (35 + (5+1)) = 9 + 41 = 50
	c.Services = []Service{{
		Name:      "web",
		Container: "web-web",
		Connector: ConnectorConfig{Container: "cloudflared-web", StopTimeout: 35},
		Health:    HealthConfig{URL: "http://web-web:8080/up"},
	}}
	c.Timing.TTL = 20 // <= 50, and still <= ttlMax so only the hysteresis law fires
	faults := Validate(c)
	if len(faults) != 1 {
		t.Fatalf("want exactly one fault (the hysteresis law), got %v", faults)
	}
	if !strings.Contains(faults[0], "timing.ttl") || !strings.Contains(faults[0], "hysteresis is dead code") {
		t.Fatalf("want the ttl-vs-hysteresis fault, got %v", faults)
	}
}

func TestDynamoCapacityWithinBudgetStaysQuiet(t *testing.T) {
	// Guards against over-tightening: a modest interval and service count
	// must neither fault nor warn. Webhooks are fully configured so this
	// stays a pure test of the capacity guard, not of the webhook warning.
	c := validDynamoConfig()
	c.Services = dynamoServices(5)
	c.Webhooks = fullWebhookConfig()
	if faults := Validate(c); len(faults) != 0 {
		t.Fatalf("want valid, got %v", faults)
	}
	if w := Warnings(c); len(w) != 0 {
		t.Fatalf("want no warnings within budget, got %v", w)
	}
}

// fullWebhookConfig is every WebhookConfig field set to a distinct receiver,
// used to isolate tests of other warning sources from the webhook warning
// added below, and as the "any receiver silences it" / "fully configured"
// baseline.
func fullWebhookConfig() WebhookConfig {
	return WebhookConfig{
		Promote:  "https://hooks.example.com/promote",
		Demote:   "https://hooks.example.com/demote",
		Evacuate: "https://hooks.example.com/evacuate",
		Reclaim:  "https://hooks.example.com/reclaim",
		Alert:    "https://hooks.example.com/alert",
	}
}

// TestWebhookConfigFieldCountIsPinned guards webhookWarnings (validate.go)
// and fullWebhookConfig (above): both hardcode WebhookConfig's five fields
// by name rather than reflecting over the struct, so a field added to
// WebhookConfig without updating both would silently weaken the
// dark-webhooks warning — it would report "configured" for a config whose
// only set field is the new one nobody checks, with no test failure to
// catch it. This test is that catch: it fails the moment the field count
// drifts, pointing at the two places to update.
func TestWebhookConfigFieldCountIsPinned(t *testing.T) {
	got := reflect.TypeOf(WebhookConfig{}).NumField()
	if got != 5 {
		t.Fatalf("WebhookConfig has %d fields, want 5 — update webhookWarnings (validate.go) and fullWebhookConfig (validate_test.go) to cover the new field, then update this pin", got)
	}
}

func TestWebhooksWarnWhenEmptyBothBackends(t *testing.T) {
	// The RCA: production's configs had no webhooks: block at all, and conch
	// neither validated nor warned about it — the one channel meant to carry
	// failover transitions between hosts was silent. This must warn on every
	// backend the same way, since which arbiter backend a host uses says
	// nothing about whether anyone can see its lease transitions.
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"dynamo", validDynamoConfig()},
		{"cloudflare", validCloudflareConfig()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings := Warnings(tc.cfg)
			joined := strings.Join(warnings, "\n")
			if !strings.Contains(joined, "webhooks") || !strings.Contains(joined, "no receivers configured") {
				t.Fatalf("want a dark-webhooks warning, got %v", warnings)
			}
		})
	}
}

func TestWebhooksAnySingleReceiverSilencesWarning(t *testing.T) {
	// Any one field set is enough: the warning is about the channel being
	// entirely dark, not about which events are wired up.
	fields := []struct {
		name string
		set  func(*WebhookConfig)
	}{
		{"promote", func(w *WebhookConfig) { w.Promote = "https://hooks.example.com/promote" }},
		{"demote", func(w *WebhookConfig) { w.Demote = "https://hooks.example.com/demote" }},
		{"evacuate", func(w *WebhookConfig) { w.Evacuate = "https://hooks.example.com/evacuate" }},
		{"reclaim", func(w *WebhookConfig) { w.Reclaim = "https://hooks.example.com/reclaim" }},
		{"alert", func(w *WebhookConfig) { w.Alert = "https://hooks.example.com/alert" }},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			c := validCloudflareConfig()
			f.set(&c.Webhooks)
			warnings := Warnings(c)
			joined := strings.Join(warnings, "\n")
			if strings.Contains(joined, "webhooks") {
				t.Fatalf("one configured receiver (%s) must silence the dark-webhooks warning, got %v", f.name, warnings)
			}
		})
	}
}

func TestDynamoCapacityWarningStillFiresAlongsideWebhookWarning(t *testing.T) {
	// Regression pin for the Warnings() restructure: composing multiple
	// warning sources must not drop the pre-existing dynamo capacity guard.
	// Webhooks are left empty here on purpose, so both warnings fire at
	// once and neither source stomps the other.
	c := validDynamoConfig()
	c.Timing.Interval = 0.05
	c.Services = dynamoServices(10)
	warnings := Warnings(c)
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "warning") || !(strings.Contains(joined, "WCU") || strings.Contains(joined, "capacity")) {
		t.Fatalf("want the dynamo capacity warning to still fire, got %v", warnings)
	}
	if !strings.Contains(joined, "webhooks") {
		t.Fatalf("want the dark-webhooks warning to also fire, got %v", warnings)
	}
	if len(warnings) != 2 {
		t.Fatalf("want exactly the two independent warnings, got %v", warnings)
	}
}

func TestFullyConfiguredProducesNoWarnings(t *testing.T) {
	// A config with webhooks wired up and dynamo usage within budget must be
	// entirely quiet, for both backends.
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"dynamo", func() Config {
			c := validDynamoConfig()
			c.Services = dynamoServices(5)
			c.Webhooks = fullWebhookConfig()
			return c
		}()},
		{"cloudflare", func() Config {
			c := validCloudflareConfig()
			c.Webhooks = fullWebhookConfig()
			return c
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := Warnings(tc.cfg); len(w) != 0 {
				t.Fatalf("want zero warnings, got %v", w)
			}
		})
	}
}
