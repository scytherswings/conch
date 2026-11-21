package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func load(t *testing.T, yamlDoc string) (Config, []string, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "conch.yml")
	if err := os.WriteFile(p, []byte(yamlDoc), 0o644); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

const minimal = `
host: host-a
arbiter: {url: "https://conch-lease.example.workers.dev", secret_env: CONCH_AUTH_SECRET}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
`

func TestMinimalConfigIsValidWithDefaults(t *testing.T) {
	cfg, faults, err := load(t, minimal)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	if cfg.Timing.TTL != 60 || cfg.Timing.Interval != 3 ||
		cfg.Timing.DemoteAfter != 3 || cfg.Timing.PromoteAfter != 6 {
		t.Fatalf("defaults wrong: %+v", cfg.Timing)
	}
	if cfg.Arbiter.OnLoss != "freeze" {
		t.Fatalf("on_loss must default to freeze, got %q", cfg.Arbiter.OnLoss)
	}
	if cfg.Arbiter.Backend != "cloudflare" {
		t.Fatalf("arbiter.backend must default to cloudflare, got %q", cfg.Arbiter.Backend)
	}
	if got := cfg.Services[0].Connector.StopTimeout; got != 35 {
		t.Fatalf("connector stop_timeout default 35, got %v", got)
	}
	if got := cfg.Services[0].Key(); got != "web" {
		t.Fatalf("lease key defaults to name, got %q", got)
	}
	if got := cfg.Services[0].Health.Expect; len(got) != 1 || got[0] != 200 {
		t.Fatalf("health.expect must default to [200], got %v", got)
	}
	if got := cfg.Services[0].Health.TimeoutSeconds; got != 2 {
		t.Fatalf("health.timeout must default to 2, got %v", got)
	}
	if cfg.Docker.Socket != "/var/run/docker.sock" {
		t.Fatalf("docker.socket must default to /var/run/docker.sock, got %q", cfg.Docker.Socket)
	}
	if cfg.Docker.TimeoutSeconds != 30 {
		t.Fatalf("docker.timeout must default to 30, got %v", cfg.Docker.TimeoutSeconds)
	}
	if cfg.Disk.MinFreeGB != 10 {
		t.Fatalf("disk.min_free_gb must default to 10, got %v", cfg.Disk.MinFreeGB)
	}
	if cfg.Arbiter.TimeoutSeconds != 5 {
		t.Fatalf("arbiter.timeout must default to 5, got %v", cfg.Arbiter.TimeoutSeconds)
	}
}

// TestDynamoBackendFieldsRoundTripThroughYAML pins the yaml tags on
// ArbiterConfig's dynamo fields against a typo: this is the one place a
// misspelled `table`/`region`/`endpoint`/`credentials_file` tag (or a
// misspelled `backend`) would go undetected, since every other config test
// either builds Config{} literals directly or never sets these fields at
// all. Later tasks (the dynamo client) rely on these exact names landing
// through the real yaml.v3 decode path, not just compiling.
func TestDynamoBackendFieldsRoundTripThroughYAML(t *testing.T) {
	doc := `
host: host-a
arbiter:
  backend: dynamo
  on_loss: freeze
  table: conch-leases
  region: us-east-1
  endpoint: http://localhost:8000
  credentials_file: /etc/conch/aws-credentials
`
	cfg, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	want := ArbiterConfig{
		Backend:         "dynamo",
		OnLoss:          "freeze",
		TimeoutSeconds:  5, // default: timeout is not set in this doc
		Table:           "conch-leases",
		Region:          "us-east-1",
		Endpoint:        "http://localhost:8000",
		CredentialsFile: "/etc/conch/aws-credentials",
	}
	if cfg.Arbiter != want {
		t.Fatalf("dynamo arbiter fields didn't round-trip through yaml decode:\ngot  %+v\nwant %+v", cfg.Arbiter, want)
	}
}

func TestUnknownKeysAreAFault(t *testing.T) {
	_, _, err := load(t, minimal+"\nttl_seconds: 10\n")
	if err == nil || !strings.Contains(err.Error(), "ttl_seconds") {
		t.Fatalf("unknown keys must fail loudly, got %v", err)
	}
}

func TestUnknownNestedTimingKeyIsAFault(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
timing: {ttl: 60, bogus: 1}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
`
	_, _, err := load(t, doc)
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown nested keys must fail loudly, got %v", err)
	}
}

func TestUnknownNestedServiceKeyIsAFault(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
    bogus: 1
`
	_, _, err := load(t, doc)
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown keys nested in a slice element must fail loudly, got %v", err)
	}
}

func TestUnknownWebhookKeyIsAFault(t *testing.T) {
	doc := minimal + "\nwebhooks: {promote: \"https://ops.example/hook\", bogus: \"https://x\"}\n"
	_, _, err := load(t, doc)
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown webhook keys must fail loudly, got %v", err)
	}
}

func TestEveryFaultIsListedNotJustTheFirst(t *testing.T) {
	bad := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
timing: {ttl: 10, interval: 3, demote_after: 3, promote_after: 2}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web, stop_timeout: 35}
    health: {url: "http://127.0.0.1:8080/up"}
  - name: blog
    lease_key: web
    container: blog-web
    connector: {container: cloudflared-web}
    health: {url: "http://blog-web:8081/up"}
`
	_, faults, err := load(t, bad)
	if err != nil {
		t.Fatal(err)
	}
	wants := []string{
		"ttl",           // 10 cannot exceed interval*demote_after + stop_deadline
		"promote_after", // 2 <= demote_after 3
		"lease_key",     // duplicate "web"
		"connector",     // duplicate connector container
		"127.0.0.1",     // loopback health URL without host_network: true
	}
	joined := strings.Join(faults, "\n")
	for _, w := range wants {
		if !strings.Contains(joined, w) {
			t.Errorf("missing fault about %q in:\n%s", w, joined)
		}
	}
	if len(faults) < len(wants) {
		t.Fatalf("expected at least %d distinct faults, got %d: %v", len(wants), len(faults), faults)
	}
}

func TestLoopbackHealthURLAllowedUnderHostNetwork(t *testing.T) {
	doc := strings.Replace(minimal, "http://web-web:8080/up",
		"http://127.0.0.1:8080/up", 1) + "\nhost_network: true\n"
	_, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("loopback is fine under host networking, got %v %v", err, faults)
	}
}

func TestHealthHostRoundTripsThroughYAML(t *testing.T) {
	doc := strings.Replace(minimal, `health: {url: "http://web-web:8080/up"}`,
		`health: {url: "http://web-web:8080/up", host: "app.internal.example"}`, 1)
	cfg, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	if got := cfg.Services[0].Health.Host; got != "app.internal.example" {
		t.Fatalf("health.host must round-trip through yaml, got %q", got)
	}
}

func TestHealthHostUnsetDefaultsToEmpty(t *testing.T) {
	cfg, faults, err := load(t, minimal)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	if got := cfg.Services[0].Health.Host; got != "" {
		t.Fatalf("an unconfigured health.host must default to empty, got %q", got)
	}
}

func TestHealthHostWithoutHealthURLIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, `health: {url: "http://web-web:8080/up"}`,
		`health: {host: "app.internal.example"}`, 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "health.host") {
		t.Fatalf("a host with no url to attach it to must fault, got %v", faults)
	}
}

func TestTimingComposesPerService(t *testing.T) {
	// stop_deadline(svc) = stop_timeout + slop; slop = arbiter timeout (5) + 1.
	// ttl 60 > 3*3 + (35+6) = 50 -> valid. A 120s stop_timeout faults only if
	// ttl doesn't cover THAT service.
	doc := strings.Replace(minimal, "connector: {container: cloudflared-web}",
		"connector: {container: cloudflared-web, stop_timeout: 120}", 1)
	_, faults, _ := load(t, doc)
	if len(faults) == 0 {
		t.Fatal("ttl 60 cannot cover a 120s connector stop; expected a fault")
	}
}

func TestFingerprintIsStableAcrossServiceOrder(t *testing.T) {
	a, _, _ := load(t, minimal)
	b, _, _ := load(t, strings.Replace(minimal, "name: web", "name: web", 1)) // same doc
	if a.Fingerprint() != b.Fingerprint() || len(a.Fingerprint()) != 16 {
		t.Fatalf("fingerprint unstable or wrong length: %q vs %q", a.Fingerprint(), b.Fingerprint())
	}

	twoServices := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
`
	reversed := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
`
	c, _, err := load(t, twoServices)
	if err != nil {
		t.Fatal(err)
	}
	d, _, err := load(t, reversed)
	if err != nil {
		t.Fatal(err)
	}
	if c.Fingerprint() != d.Fingerprint() {
		t.Fatalf("fingerprint depends on service order: %q vs %q", c.Fingerprint(), d.Fingerprint())
	}
}

func TestHookTimeoutDefaultsWhenOmitted(t *testing.T) {
	doc := minimal + `
    hooks:
      promote:
        - name: restore
          image: alpine
          argv: ["restore"]
        - name: verify
          image: alpine
          argv: ["verify"]
          timeout: 30
`
	cfg, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	hooks := cfg.Services[0].Hooks.Promote
	if got := hooks[0].Timeout; got != 300 {
		t.Fatalf("an omitted hook timeout must default to 300, got %v", got)
	}
	if got := hooks[1].Timeout; got != 30 {
		t.Fatalf("an explicit hook timeout must not be overridden, got %v", got)
	}
}

func TestNegativeHookTimeoutIsAFaultNotDefaulted(t *testing.T) {
	doc := minimal + `
    hooks:
      evacuate:
        - name: final-sync
          image: alpine
          argv: ["sync"]
          timeout: -5
`
	cfg, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Services[0].Hooks.Evacuate[0].Timeout; got != -5 {
		t.Fatalf("a negative timeout must reach Validate unchanged, not be silently defaulted, got %v", got)
	}
	found := false
	for _, f := range faults {
		if strings.Contains(f, "timeout") && strings.Contains(f, "-5") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want a fault reporting the negative timeout, got %v", faults)
	}
}

func TestNonPositiveIntervalIsAFault(t *testing.T) {
	for _, interval := range []string{"0", "-1"} {
		doc := minimal + "\ntiming: {interval: " + interval + "}\n"
		_, faults, err := load(t, doc)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(faults, "\n")
		if !strings.Contains(joined, "timing.interval") || !strings.Contains(joined, "positive") {
			t.Fatalf("interval %s: want a timing.interval fault, got %v", interval, faults)
		}
	}
}

func TestPositiveIntervalStillValid(t *testing.T) {
	// Guards against over-tightening: a small but positive interval must not
	// itself be flagged (only its knock-on effect on the ttl threshold, which
	// this doc keeps clear of).
	doc := minimal + "\ntiming: {interval: 0.5}\n"
	_, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
}

func TestDemoteAfterBelowOneIsAFault(t *testing.T) {
	for _, demoteAfter := range []string{"0", "-2"} {
		doc := minimal + "\ntiming: {demote_after: " + demoteAfter + "}\n"
		_, faults, err := load(t, doc)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(faults, "\n")
		if !strings.Contains(joined, "timing.demote_after") || !strings.Contains(joined, "at least 1") {
			t.Fatalf("demote_after %s: want a timing.demote_after fault, got %v", demoteAfter, faults)
		}
	}
}

func TestPromoteAfterBelowOneIsAFault(t *testing.T) {
	doc := minimal + "\ntiming: {promote_after: 0}\n"
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "zero consecutive good checks") {
		t.Fatalf("promote_after 0: want a timing.promote_after fault, got %v", faults)
	}
}

func TestZeroPromoteAfterOverNegativeDemoteAfterIsCaught(t *testing.T) {
	// The regression this closes: promote_after: 0 > demote_after: -2 used to
	// pass the relative check outright. Both the new positivity faults and
	// the existing relative fault must all fire together.
	doc := minimal + "\ntiming: {promote_after: 0, demote_after: -2}\n"
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	for _, want := range []string{"timing.promote_after", "timing.demote_after"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing fault about %q in: %v", want, faults)
		}
	}
}

func TestDemoteAfterAndPromoteAfterAtTheirFloorStillValid(t *testing.T) {
	// demote_after: 1, promote_after: 2 is the smallest legal pair — guards
	// against over-tightening at the boundary itself.
	doc := minimal + "\ntiming: {demote_after: 1, promote_after: 2}\n"
	_, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
}

func TestTTLOutsideArbiterClampIsAFault(t *testing.T) {
	for _, ttl := range []string{"5", "400"} {
		doc := minimal + "\ntiming: {ttl: " + ttl + "}\n"
		_, faults, err := load(t, doc)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(faults, "\n")
		if !strings.Contains(joined, "timing.ttl") || !strings.Contains(joined, "[10, 300]") {
			t.Fatalf("ttl %s: want a clamp-range fault, got %v", ttl, faults)
		}
	}
}

func TestTTLAtArbiterClampBoundsStillValid(t *testing.T) {
	// ttl: 300 (the upper clamp bound) with otherwise-default timing.
	upper := minimal + "\ntiming: {ttl: 300}\n"
	_, faults, err := load(t, upper)
	if err != nil || len(faults) != 0 {
		t.Fatalf("ttl 300: want valid, got err=%v faults=%v", err, faults)
	}

	// ttl: 10 (the lower clamp bound) needs tighter timing so it also clears
	// the interval*demote_after+stop_deadline threshold, or that unrelated
	// check would fault it first and mask what this test is guarding.
	lower := strings.Replace(minimal, "connector: {container: cloudflared-web}",
		"connector: {container: cloudflared-web, stop_timeout: 1}", 1) +
		"\ntiming: {ttl: 10, interval: 1, demote_after: 1, promote_after: 2}\n"
	_, faults, err = load(t, lower)
	if err != nil || len(faults) != 0 {
		t.Fatalf("ttl 10: want valid, got err=%v faults=%v", err, faults)
	}
}

func TestEmptyContainerIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web", `container: ""`, 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	// "services[0].container:" (with the colon) so this doesn't pass on the
	// connector fault alone — "services[0].container" is a substring of
	// "services[0].connector.container".
	if !strings.Contains(joined, "services[0].container:") || !strings.Contains(joined, "must be set") {
		t.Fatalf("want a services[0].container fault, got %v", faults)
	}
}

func TestEmptyConnectorContainerIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "connector: {container: cloudflared-web}",
		`connector: {container: ""}`, 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services[0].connector.container") || !strings.Contains(joined, "must be set") {
		t.Fatalf("want a services[0].connector.container fault, got %v", faults)
	}
}

// TestTrailingStarContainerIsLegal pins the Kamal-deploy case: a container
// name ending in "*" means prefix match, not an exact name, and by itself
// that is not a fault.
func TestTrailingStarContainerIsLegal(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web", "container: web-web-*", 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if strings.Contains(joined, "container") {
		t.Fatalf("a trailing * must be legal, got %v", faults)
	}
}

// TestBareStarContainerIsAFault: a bare "*" has no literal prefix to match
// on at all, which would resolve against every container on the host.
func TestBareStarContainerIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web", `container: "*"`, 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services[0].container:") || !strings.Contains(joined, "*") {
		t.Fatalf("want a bare-* container fault, got %v", faults)
	}
}

// TestMidStringStarContainerIsAFault: prefix matching only supports a
// trailing "*" — one anywhere else looks like a glob this code doesn't
// implement, and silently treating it as a literal character would be worse
// than refusing it.
func TestMidStringStarContainerIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web", "container: web-*-web", 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services[0].container:") || !strings.Contains(joined, "final character") {
		t.Fatalf("want a mid-string-* container fault, got %v", faults)
	}
}

// TestContainerPrefixReportsPrefixAndLiteral pins the helper the supervisor
// resolves against: a trailing "*" reports the literal text before it, and
// an ordinary exact name reports isPrefix:false so no daemon lookup ever
// happens for it.
func TestContainerPrefixReportsPrefixAndLiteral(t *testing.T) {
	svc := Service{Container: "web-web-*"}
	prefix, isPrefix := svc.ContainerPrefix()
	if !isPrefix || prefix != "web-web-" {
		t.Fatalf("got prefix=%q isPrefix=%v", prefix, isPrefix)
	}

	svc = Service{Container: "web-web"}
	prefix, isPrefix = svc.ContainerPrefix()
	if isPrefix || prefix != "" {
		t.Fatalf("exact name must not be a prefix: got prefix=%q isPrefix=%v", prefix, isPrefix)
	}
}

// TestConnectorContainerWithAsteriskIsAFault: prefix matching is not
// supported for connector containers at all (they're Kamal accessories with
// stable names) — ANY "*" there is a fault, not only a malformed one.
func TestConnectorContainerWithAsteriskIsAFault(t *testing.T) {
	cases := []string{"*", "cloudflared-web-*", "cloud*flared-web", "cloudflared-*-web"}
	for _, container := range cases {
		t.Run(container, func(t *testing.T) {
			doc := strings.Replace(minimal, "connector: {container: cloudflared-web}",
				fmt.Sprintf(`connector: {container: %q}`, container), 1)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].connector.container:") || !strings.Contains(joined, "*") {
				t.Fatalf("want a connector.container * fault, got %v", faults)
			}
		})
	}
}

// TestHealthURLPlaceholderTypoIsAFault: only the EXACT token "{container}"
// substitutes at probe time (supervisor.readings does a literal
// strings.ReplaceAll) — anything placeholder-shaped that isn't exactly that
// would otherwise pass validation and then silently never substitute,
// leaving a health URL that can never resolve to anything real.
func TestHealthURLPlaceholderTypoIsAFault(t *testing.T) {
	cases := []string{
		"http://{Container}:3000/up",
		"http://${container}:3000/up",
		"http://{{container}}:3000/up",
	}
	for _, url := range cases {
		t.Run(url, func(t *testing.T) {
			doc := strings.Replace(minimal, `health: {url: "http://web-web:8080/up"}`,
				fmt.Sprintf(`health: {url: %q}`, url), 1)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].health.url:") {
				t.Fatalf("want a health.url placeholder fault for %q, got %v", url, faults)
			}
		})
	}
}

// TestHealthURLExactPlaceholderIsLegal is the non-regression the fault check
// above must not break: the one placeholder shape that actually substitutes.
func TestHealthURLExactPlaceholderIsLegal(t *testing.T) {
	doc := strings.Replace(minimal, `health: {url: "http://web-web:8080/up"}`,
		`health: {url: "http://{container}:3000/up"}`, 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if strings.Contains(joined, "health.url") {
		t.Fatalf("the exact {container} placeholder must be legal, got %v", faults)
	}
}

// TestPromoteHookArgvPlaceholderTypoIsAFault mirrors
// TestHealthURLPlaceholderTypoIsAFault for a promote hook's Argv: the
// supervisor's executor substitutes ONLY the exact literal "{container}"
// (strings.ReplaceAll, same as health.url), so every near-miss casing would
// otherwise pass validation and then silently never substitute — the hook
// would run with the literal placeholder text as one of its arguments.
func TestPromoteHookArgvPlaceholderTypoIsAFault(t *testing.T) {
	cases := []string{"{Container}", "${container}", "{{container}}"}
	for _, token := range cases {
		t.Run(token, func(t *testing.T) {
			doc := minimal + fmt.Sprintf(`
    hooks:
      promote:
        - name: restore
          image: alpine
          argv: ["restore", "--target", %q]
`, token)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].hooks.promote[0].argv[2]:") {
				t.Fatalf("want a promote-hook argv placeholder fault for %q, got %v", token, faults)
			}
		})
	}
}

// TestPromoteHookArgvExactPlaceholderIsLegal is the non-regression the check
// above must not break: the one placeholder shape that actually substitutes
// (executor.substitutePromoteArgv) must be legal in promote argv.
func TestPromoteHookArgvExactPlaceholderIsLegal(t *testing.T) {
	doc := minimal + `
    hooks:
      promote:
        - name: restore
          image: alpine
          argv: ["restore", "--target", "{container}"]
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if strings.Contains(joined, "hooks.promote[0].argv") {
		t.Fatalf("the exact {container} placeholder must be legal in promote argv, got %v", faults)
	}
}

// TestEvacuateHookArgvPlaceholderIsAFault: evacuate hooks run off the check
// path (executor.startEvacuateHooks via evacuate.go) with no resolved
// container ever in scope — there is no substitution mechanism for them to
// receive, not even for the exact "{container}" token that promote argv
// legitimately uses. Left unrefused, any of these reaches docker as a
// literal argument no container on the host is actually named, which is a
// validation fault, not a silent no-op.
func TestEvacuateHookArgvPlaceholderIsAFault(t *testing.T) {
	cases := []string{"{container}", "{Container}", "${container}", "{{container}}"}
	for _, token := range cases {
		t.Run(token, func(t *testing.T) {
			doc := minimal + fmt.Sprintf(`
    hooks:
      evacuate:
        - name: final-sync
          image: alpine
          argv: ["sync", "--target", %q]
`, token)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].hooks.evacuate[0].argv[2]:") {
				t.Fatalf("want an evacuate-hook argv placeholder fault for %q, got %v", token, faults)
			}
		})
	}
}

// TestEvacuateHookArgvWithNoPlaceholderIsLegal is evacuate's non-regression:
// ordinary argv with no placeholder-shaped token at all must stay legal.
func TestEvacuateHookArgvWithNoPlaceholderIsLegal(t *testing.T) {
	doc := minimal + `
    hooks:
      evacuate:
        - name: final-sync
          image: alpine
          argv: ["sync", "--from", "/data"]
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if strings.Contains(joined, "hooks.evacuate[0].argv") {
		t.Fatalf("placeholder-free evacuate argv must be legal, got %v", faults)
	}
}

// manyServices builds n distinct, individually-valid services so a batch-cap
// test's faults are attributable only to the count, not to some incidental
// duplicate key or loopback URL.
func manyServices(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `
  - name: svc%d
    container: svc%d-web
    connector: {container: cloudflared-svc%d}
    health: {url: "http://svc%d-web:8080/up"}`, i, i, i, i)
	}
	return b.String()
}

func TestTooManyServicesIsAFault(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:` + manyServices(65)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services:") || !strings.Contains(joined, "65") || !strings.Contains(joined, "batch cap") {
		t.Fatalf("want a service-count batch-cap fault, got %v", faults)
	}
}

func TestServiceCountAtBatchCapStillValid(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:` + manyServices(64)
	_, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("64 services: want valid, got err=%v faults=%v", err, faults)
	}
}

func TestShippedExamplesAreValid(t *testing.T) {
	matches, err := filepath.Glob("../../deploy/config.example.*.yml")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("expected 2 shipped example configs, found %d: %v", len(matches), matches)
	}
	var fingerprints []string
	for _, path := range matches {
		cfg, faults, err := Load(path)
		if err != nil || len(faults) != 0 {
			t.Errorf("%s: want valid, got err=%v faults=%v", path, err, faults)
			continue
		}
		fingerprints = append(fingerprints, cfg.Fingerprint())
	}
	// host-a and host-b supervise the same lease keys, so drift detection
	// (spec §11) depends on their fingerprints matching.
	if len(fingerprints) == 2 && fingerprints[0] != fingerprints[1] {
		t.Errorf("host-a and host-b examples supervise the same services but fingerprints differ: %v", fingerprints)
	}
}

// TestPeersIsAReservedLeaseKey pins the one name a service may not lease
// under: `conch status` reports peer presence at the top level, alongside
// the per-service entries, so a service keyed "peers" would overwrite it —
// or be overwritten by it — and an operator would read one as the other.
func TestPeersIsAReservedLeaseKey(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: peers
    container: peers-web
    connector: {container: cloudflared-peers}
    health: {url: "http://peers-web:8080/up"}
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatalf("this config decodes fine; the reserved name is a semantic fault: %v", err)
	}
	if !anyFaultContains(faults, "reserved") {
		t.Fatalf("want a fault about the reserved name, got %v", faults)
	}

	// The lease key is what collides, not the display name: a service named
	// something else that leases "peers" is the same collision.
	doc = strings.Replace(doc, "  - name: peers", "  - name: web\n    lease_key: peers", 1)
	_, faults, err = load(t, doc)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !anyFaultContains(faults, "reserved") {
		t.Fatalf("a lease_key of peers is the same collision, got %v", faults)
	}
}

func anyFaultContains(faults []string, want string) bool {
	for _, f := range faults {
		if strings.Contains(f, want) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// accessories: exact names, one owner per container
// ---------------------------------------------------------------------------

func TestAccessoriesListIsLegalAndDecoded(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web",
		"container: web-web\n    accessories: [web-litestream, web-metrics]", 1)
	cfg, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	got := cfg.Services[0].Accessories
	if len(got) != 2 || got[0] != "web-litestream" || got[1] != "web-metrics" {
		t.Fatalf("accessories decoded wrong: %v", got)
	}
}

func TestEmptyAccessoryNameIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web",
		`container: web-web`+"\n    accessories: [\"\"]", 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services[0].accessories[0]") || !strings.Contains(joined, "empty") {
		t.Fatalf("want an empty-accessory fault, got %v", faults)
	}
}

// TestAccessoryWithAsteriskIsAFault: accessories are hand-created or
// Kamal-booted, never versioned, so a "*" ANYWHERE in the name is a fault —
// the connector.container ruling, not the service container's trailing-*
// prefix support.
func TestAccessoryWithAsteriskIsAFault(t *testing.T) {
	cases := []string{"*", "web-litestream-*", "web-*-litestream"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			doc := strings.Replace(minimal, "container: web-web",
				fmt.Sprintf("container: web-web\n    accessories: [%q]", name), 1)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].accessories[0]") || !strings.Contains(joined, "*") {
				t.Fatalf("want an accessory * fault for %q, got %v", name, faults)
			}
		})
	}
}

func TestDuplicateAccessoryWithinAServiceIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web",
		"container: web-web\n    accessories: [web-litestream, web-litestream]", 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(faults, "\n")
	if !strings.Contains(joined, "services[0].accessories[1]") || !strings.Contains(joined, "twice") {
		t.Fatalf("want a duplicate-accessory fault, got %v", faults)
	}
}

// TestAccessoryCollidingWithAConfigNamedContainerIsAFault pins one owner per
// container: an accessory may not double as any service's container or any
// connector container, its own service's included — those containers already
// have a reconcile, and two reconciles fight.
func TestAccessoryCollidingWithAConfigNamedContainerIsAFault(t *testing.T) {
	cases := []struct{ name, accessory, wantAbout string }{
		{"own service container", "web-web", "service's container"},
		{"own connector container", "cloudflared-web", "connector container"},
		{"another service's container", "blog-web", "service's container"},
		{"another service's connector", "cloudflared-blog", "connector container"},
	}
	base := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
    accessories: [%s]
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
`
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, faults, err := load(t, fmt.Sprintf(base, tc.accessory))
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, "services[0].accessories[0]") || !strings.Contains(joined, tc.wantAbout) {
				t.Fatalf("want a one-owner fault about %q, got %v", tc.wantAbout, faults)
			}
		})
	}
}

func TestAccessorySharedByTwoServicesIsAFaultReportedOnce(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
    accessories: [shared-litestream]
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
    accessories: [shared-litestream]
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range faults {
		if strings.Contains(f, "shared-litestream") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("a shared accessory is one fault, reported once, got %d in %v", n, faults)
	}
}

// ---------------------------------------------------------------------------
// warm accessories: the same exact-name, one-owner-per-container rules, plus
// the one overlap that is worse than a duplicate — a container that is warm
// in one place and lease-following in another, started on every host by one
// reconcile and stopped by the other, once per interval, forever.
// ---------------------------------------------------------------------------

func TestWarmAccessoriesListIsLegalAndDecoded(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web",
		"container: web-web\n    accessories: [web-litestream]\n    warm_accessories: [web-db, web-cache]", 1)
	cfg, faults, err := load(t, doc)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	got := cfg.Services[0].WarmAccessories
	if len(got) != 2 || got[0] != "web-db" || got[1] != "web-cache" {
		t.Fatalf("warm_accessories decoded wrong: %v", got)
	}
	if acc := cfg.Services[0].Accessories; len(acc) != 1 || acc[0] != "web-litestream" {
		t.Fatalf("the two lists must stay separate, got accessories %v", acc)
	}
}

// TestAConfigWithoutWarmAccessoriesDecodesToNothing is half of this feature's
// byte-identical claim (the supervisor's half is in that package): a config
// written before the field existed carries no warm accessory at all, so
// nothing downstream can measure, start or shield one.
func TestAConfigWithoutWarmAccessoriesDecodesToNothing(t *testing.T) {
	cfg, faults, err := load(t, minimal)
	if err != nil || len(faults) != 0 {
		t.Fatalf("want valid, got err=%v faults=%v", err, faults)
	}
	if got := cfg.Services[0].WarmAccessories; got != nil {
		t.Fatalf("an absent warm_accessories must decode to nil, got %v", got)
	}
}

func TestWarmAccessoryNameFaults(t *testing.T) {
	cases := []struct {
		name, list, wantField, wantAbout string
	}{
		{"empty", `[""]`, "services[0].warm_accessories[0]", "empty"},
		{"bare star", `["*"]`, "services[0].warm_accessories[0]", "*"},
		{"trailing star", `["web-db-*"]`, "services[0].warm_accessories[0]", "*"},
		{"listed twice", `[web-db, web-db]`, "services[0].warm_accessories[1]", "twice"},
		{"own service container", `[web-web]`, "services[0].warm_accessories[0]", "service's container"},
		{"own connector", `[cloudflared-web]`, "services[0].warm_accessories[0]", "connector container"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := strings.Replace(minimal, "container: web-web",
				"container: web-web\n    warm_accessories: "+tc.list, 1)
			_, faults, err := load(t, doc)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(faults, "\n")
			if !strings.Contains(joined, tc.wantField) || !strings.Contains(joined, tc.wantAbout) {
				t.Fatalf("want a fault at %s about %q, got %v", tc.wantField, tc.wantAbout, faults)
			}
		})
	}
}

// TestAWarmAccessoryOfOneServiceThatIsARegularAccessoryOfAnotherIsAFault is
// the dangerous case, and the reason this validation exists at all: the warm
// reconcile starts the container on every host, the other service's accessory
// reconcile stops it wherever that service does not serve, and which of them
// won any given interval would depend on executor timing nobody can read off
// the config.
func TestAWarmAccessoryOfOneServiceThatIsARegularAccessoryOfAnotherIsAFault(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
    warm_accessories: [shared-db]
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
    accessories: [shared-db]
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	var about []string
	for _, f := range faults {
		if strings.Contains(f, "shared-db") {
			about = append(about, f)
		}
	}
	if len(about) != 1 {
		t.Fatalf("one container, one fault, reported once: got %d in %v", len(about), faults)
	}
	if !strings.Contains(about[0], "warm") || !strings.Contains(about[0], "one owner per container") {
		t.Fatalf("the fault must name the warm/regular fight, got %q", about[0])
	}
}

// The same fight inside ONE service — a container that is both warm and
// lease-following for the same service — is the same fault: the two
// reconciles are what fight, not the two services.
func TestAContainerThatIsBothWarmAndRegularForOneServiceIsAFault(t *testing.T) {
	doc := strings.Replace(minimal, "container: web-web",
		"container: web-web\n    accessories: [web-db]\n    warm_accessories: [web-db]", 1)
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	if !anyFaultContains(faults, "web-db") {
		t.Fatalf("want a fault about the doubly-owned container, got %v", faults)
	}
}

func TestAWarmAccessorySharedByTwoServicesIsAFaultReportedOnce(t *testing.T) {
	doc := `
host: host-a
arbiter: {url: "https://a.example", secret_env: S}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
    warm_accessories: [shared-db]
  - name: blog
    container: blog-web
    connector: {container: cloudflared-blog}
    health: {url: "http://blog-web:8081/up"}
    warm_accessories: [shared-db]
`
	_, faults, err := load(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range faults {
		if strings.Contains(f, "shared-db") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("a shared warm accessory is one fault, reported once, got %d in %v", n, faults)
	}
}
