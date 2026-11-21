package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/supervisor"
)

// minimalYAML is a config that validates clean, in the same shape as
// internal/config's own `minimal` fixture (Task 6).
const minimalYAML = `
host: host-a
arbiter: {url: "https://conch-lease.example.workers.dev", secret_env: CONCH_AUTH_SECRET}
services:
  - name: web
    container: web-web
    connector: {container: cloudflared-web}
    health: {url: "http://web-web:8080/up"}
`

// multiFaultYAML is Task 6's multi-fault fixture, transcribed verbatim
// (internal/config/config_test.go's TestEveryFaultIsListedNotJustTheFirst):
// a TTL too small for its own composed deadline, promote_after not
// exceeding demote_after, a duplicated lease key, a duplicated connector
// container, and a loopback health URL with no host_network declared — five
// distinct, independent faults, none of which is a YAML decode error, so
// config.Load returns them as faults rather than as err.
const multiFaultYAML = `
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

func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "conch.yml")
	if err := os.WriteFile(p, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// captureOutput redirects os.Stdout/os.Stderr for the duration of f, so a
// test can inspect exactly what a subcommand printed without changing
// run()'s signature to thread writers through every layer (main.go writes
// straight to os.Stdout/os.Stderr, matching every other CLI in the wild).
// The pipes are drained on their own goroutines so a subcommand that writes
// more than a pipe's kernel buffer can never deadlock against this test.
func captureOutput(t *testing.T, f func() int) (stdout, stderr string, code int) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr

	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = outW, errW
	t.Cleanup(func() { os.Stdout, os.Stderr = oldOut, oldErr })

	outCh := make(chan string, 1)
	errCh := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(outR)
		outCh <- string(b)
	}()
	go func() {
		b, _ := io.ReadAll(errR)
		errCh <- string(b)
	}()

	code = f()

	outW.Close()
	errW.Close()
	os.Stdout, os.Stderr = oldOut, oldErr

	return <-outCh, <-errCh, code
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// TestValidateListsEveryFaultAndExits1 pins spec §10's `conch validate`
// contract: every fault listed on stderr, not just the first, and a
// non-zero exit an operator's CI can gate on.
func TestValidateListsEveryFaultAndExits1(t *testing.T) {
	p := writeConfig(t, multiFaultYAML)

	_, stderr, code := captureOutput(t, func() int {
		return run([]string{"validate", "--config", p})
	})

	if code != 1 {
		t.Fatalf("want exit 1, got %d (stderr=%q)", code, stderr)
	}
	lines := nonEmptyLines(stderr)
	if len(lines) < 4 {
		t.Fatalf("want at least 4 distinct fault lines on stderr, got %d:\n%s", len(lines), stderr)
	}
}

// TestValidateCleanExitsZero pins the other half: a clean config exits 0 and
// says so.
func TestValidateCleanExitsZero(t *testing.T) {
	p := writeConfig(t, minimalYAML)

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"validate", "--config", p})
	})

	if code != 0 {
		t.Fatalf("want exit 0, got %d (stderr=%q)", code, stderr)
	}
	if !strings.Contains(stdout, "config is valid") {
		t.Fatalf("want %q on stdout, got %q", "config is valid", stdout)
	}
}

// TestRunRefusesToStartOnFaults pins spec §10's `conch run`: a config with
// faults is refused with exit 2, and — the point of pointing docker.socket
// at a path that provably does not exist — refused BEFORE anything tries to
// reach it. A dial attempt against a missing socket would fail loudly and
// immediately; its absence from stderr is the proof that cmdRun returns on
// the faults check, before dockerapi.New is ever called.
func TestRunRefusesToStartOnFaults(t *testing.T) {
	badSocket := filepath.Join(t.TempDir(), "definitely-does-not-exist.sock")
	doc := multiFaultYAML + "docker: {socket: \"" + badSocket + "\"}\n"
	p := writeConfig(t, doc)

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"run", "--config", p})
	})

	if code != 2 {
		t.Fatalf("want exit 2, got %d (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	for _, needle := range []string{badSocket, "dial", "connection refused", "no such file"} {
		if strings.Contains(stdout, needle) || strings.Contains(stderr, needle) {
			t.Fatalf("output must never mention the docker socket (refusal happens before any dial), "+
				"found %q in stdout=%q stderr=%q", needle, stdout, stderr)
		}
	}
	lines := nonEmptyLines(stderr)
	if len(lines) < 4 {
		t.Fatalf("run must list every fault too, got %d lines:\n%s", len(lines), stderr)
	}
}

// TestNewRunContextCancelsOnSIGTERM is the unit-testable half of `run`'s
// signal handling (the brief's own reasoning for why the rest isn't cheaply
// testable): signal.NotifyContext wires SIGTERM (and SIGINT) to cancel the
// context, and that is the ONLY thing either signal does (spec §9) — nothing
// here stops a container or touches a lease. RunForever returning on ctx
// cancellation with no Docker calls is already pinned in
// internal/supervisor (TestRunForeverReturnsOnContextCancel).
func TestNewRunContextCancelsOnSIGTERM(t *testing.T) {
	ctx, stop := newRunContext()
	defer stop()

	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("newRunContext's context was not cancelled by a self-sent SIGTERM")
	}
}

// TestLogLevelFlagRejectsAnUnknownLevel pins the flag's refusal: a typo is
// caught before conch touches a config, a secret or the Docker socket, and
// the message names the four levels rather than leaving the operator to
// guess which spelling this binary wants.
func TestLogLevelFlagRejectsAnUnknownLevel(t *testing.T) {
	p := writeConfig(t, minimalYAML)

	_, stderr, code := captureOutput(t, func() int {
		return run([]string{"run", "--config", p, "--log-level", "chatty"})
	})

	if code != 2 {
		t.Fatalf("want exit 2, got %d (stderr=%q)", code, stderr)
	}
	for _, want := range []string{"log-level", "debug", "info", "warn", "error"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("the refusal must name %q, got %q", want, stderr)
		}
	}
}

// TestLogLevelSelectsTheHandlerThreshold is the flag's whole point: --log-level
// debug is what shows the per-check decision lines, which are Debug precisely
// so a live fleet is not paying for them every interval. The default is info.
func TestLogLevelSelectsTheHandlerThreshold(t *testing.T) {
	for _, tc := range []struct {
		flag  string
		want  slog.Level
		below slog.Level
	}{
		{"debug", slog.LevelDebug, slog.LevelDebug - 1},
		{"info", slog.LevelInfo, slog.LevelDebug},
		{"warn", slog.LevelWarn, slog.LevelInfo},
		{"error", slog.LevelError, slog.LevelWarn},
	} {
		logger, err := newLogger(tc.flag, false)
		if err != nil {
			t.Fatalf("--log-level %s: %v", tc.flag, err)
		}
		if !logger.Enabled(context.Background(), tc.want) {
			t.Fatalf("--log-level %s must log at %s", tc.flag, tc.want)
		}
		if logger.Enabled(context.Background(), tc.below) {
			t.Fatalf("--log-level %s must not log at %s", tc.flag, tc.below)
		}
	}

	logger, err := newLogger(defaultLogLevel, false)
	if err != nil {
		t.Fatal(err)
	}
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatalf("the default level must not carry per-check decision lines")
	}
	if !logger.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatalf("the default level must still carry lifecycle lines")
	}
}

// TestADryRunIsNeverQuieterThanInfo pins the interaction between the two
// flags: a dry run's whole product is the transcript of what it decided and
// what it would have done, and those lines are Info. A quieter level must
// not be able to turn a dry run into a process that executes nothing and
// says nothing (CONTRIBUTING: "--dry-run must stay honest").
func TestADryRunIsNeverQuieterThanInfo(t *testing.T) {
	for _, level := range []string{"warn", "error"} {
		logger, err := newLogger(level, true)
		if err != nil {
			t.Fatalf("--log-level %s: %v", level, err)
		}
		if !logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Fatalf("--dry-run --log-level %s dropped the dry run's own transcript", level)
		}
	}
	// The floor is a floor, not a level: debug still means debug.
	logger, err := newLogger("debug", true)
	if err != nil {
		t.Fatal(err)
	}
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatalf("--dry-run must not clamp a level the operator raised")
	}
}

// fleetJSON is the frozen GET /fleet shape (Task 1) as the client sees it:
// one live holder, one free key, and two hosts. Every render test reads the
// same fixture so a change in one is a change in all.
const fleetJSON = `{"generatedAt":1795152221297,
  "services":{
    "web":{"holder":"host-a","serving":true,"epoch":12,"expiresIn":8.4,"evacuatedBy":null,"barred":false,"conflict":null},
    "blog":{"holder":null,"serving":false,"epoch":3,"expiresIn":null,"evacuatedBy":null,"barred":false,"conflict":null}},
  "hosts":{
    "host-a":{"lastSeenAgo":1.2,"fingerprint":"ab12","services":["web","blog"]},
    "host-b":{"lastSeenAgo":9.9,"fingerprint":null,"services":[]}}}`

// fleetServer serves fleetJSON on /fleet and 405s anything else, so a client
// that got the path wrong cannot pass by accident.
func fleetServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fleet" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		io.WriteString(w, fleetJSON)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestFleetRemoteRendersServicesThenHosts pins the human form: every service
// as key → holder (or free) → serving → flags, then every host with its
// presence and what it says it supervises. Remote mode reads it with nothing
// but a URL and a secret — no config file, no socket, no supervisor.
func TestFleetRemoteRendersServicesThenHosts(t *testing.T) {
	srv := fleetServer(t)
	t.Setenv("CONCH_FLEET_TEST_SECRET", "s3cr3t")

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--arbiter", srv.URL, "--secret-env", "CONCH_FLEET_TEST_SECRET"})
	})

	if code != 0 {
		t.Fatalf("want exit 0, got %d (stderr=%q)", code, stderr)
	}
	for _, want := range []string{
		"services", "web", "host-a", "serving", "blog", "free", "standby",
		"hosts", "1.2", "ab12", "supervises",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("the human summary is missing %q:\n%s", want, stdout)
		}
	}
	// Services are listed before hosts: the question is "who serves what",
	// and the hosts are supporting detail.
	if strings.Index(stdout, "services") > strings.Index(stdout, "hosts") {
		t.Fatalf("services must come first:\n%s", stdout)
	}
}

// TestFleetRemoteJSONIsMachineReadable pins --json: the reply comes back as
// parseable JSON carrying the same fields, for an agent or a script.
func TestFleetRemoteJSONIsMachineReadable(t *testing.T) {
	srv := fleetServer(t)
	t.Setenv("CONCH_FLEET_TEST_SECRET", "s3cr3t")

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--arbiter", srv.URL, "--secret-env", "CONCH_FLEET_TEST_SECRET", "--json"})
	})
	if code != 0 {
		t.Fatalf("want exit 0, got %d (stderr=%q)", code, stderr)
	}

	var got struct {
		GeneratedAt int64 `json:"generatedAt"`
		Services    map[string]struct {
			Holder  *string `json:"holder"`
			Serving bool    `json:"serving"`
		} `json:"services"`
		Hosts map[string]struct {
			LastSeenAgo float64 `json:"lastSeenAgo"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("--json must emit JSON, got %q: %v", stdout, err)
	}
	if got.GeneratedAt != 1795152221297 {
		t.Fatalf("generatedAt did not survive: %d", got.GeneratedAt)
	}
	if got.Services["web"].Holder == nil || *got.Services["web"].Holder != "host-a" || !got.Services["web"].Serving {
		t.Fatalf("web did not survive: %+v", got.Services["web"])
	}
	if got.Services["blog"].Holder != nil {
		t.Fatalf("a free lease must stay null, got %q", *got.Services["blog"].Holder)
	}
	if got.Hosts["host-b"].LastSeenAgo != 9.9 {
		t.Fatalf("host-b did not survive: %+v", got.Hosts["host-b"])
	}
}

// An arbiter without the route is the upgrade case (and the dynamo backend,
// D8): the operator gets the one message that tells them what to do, and a
// non-zero exit.
func TestFleetTooOldReportsUpgrade(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CONCH_FLEET_TEST_SECRET", "s3cr3t")

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--arbiter", srv.URL, "--secret-env", "CONCH_FLEET_TEST_SECRET"})
	})

	if code == 0 {
		t.Fatalf("an arbiter without /fleet must not exit 0 (stdout=%q)", stdout)
	}
	for _, want := range []string{"/fleet", "upgrade"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("the too-old message must name %q, got %q", want, stderr)
		}
	}
}

// A refused connection is a clear error, a non-zero exit, and NO partial
// table: an empty fleet must never be printed for a read that did not happen.
func TestFleetRefusedConnectionPrintsNoTable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing is listening now: the dial is refused
	t.Setenv("CONCH_FLEET_TEST_SECRET", "s3cr3t")

	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--arbiter", "http://" + addr, "--secret-env", "CONCH_FLEET_TEST_SECRET"})
	})

	if code == 0 {
		t.Fatal("a refused connection must be a non-zero exit")
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("a failed read must print no table, got %q", stdout)
	}
	if !strings.Contains(stderr, "fleet:") {
		t.Fatalf("the failure must name the command, got %q", stderr)
	}
}

// --arbiter is the one required input in remote mode; its absence is a usage
// error (2), caught before any secret is read or any connection made.
func TestFleetRequiresAnArbiterURL(t *testing.T) {
	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet"})
	})
	if code != 2 {
		t.Fatalf("want exit 2, got %d (stdout=%q)", code, stdout)
	}
	if !strings.Contains(stderr, "--arbiter") {
		t.Fatalf("the refusal must name the missing flag, got %q", stderr)
	}
}

// The secret rules are the config's own: with neither secret_env nor
// secret_file, fleet refuses with readSecret's message rather than sending an
// empty credential.
func TestFleetRefusesWithoutASecret(t *testing.T) {
	srv := fleetServer(t)
	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--arbiter", srv.URL})
	})
	if code != 2 {
		t.Fatalf("want exit 2, got %d (stdout=%q)", code, stdout)
	}
	if !strings.Contains(stderr, "secret") {
		t.Fatalf("the refusal must explain the missing secret, got %q", stderr)
	}
}

// TestFleetLocalRendersHostThenArbiter pins the local render: this host's name,
// what it supervises, its peers, then the arbiter's snapshot, then the drift
// probe and any note. It reuses the remote renderer for the arbiter half, so
// the two modes agree on how a fleet reads.
func TestFleetLocalRendersHostThenArbiter(t *testing.T) {
	holder := "host-a"
	view := supervisor.FleetView{
		Host:           "host-a",
		SupervisedHere: []string{"blog", "web"},
		Peers:          map[string]float64{"host-b": 2.5},
		Arbiter: &arbiter.FleetReply{
			GeneratedAt: 1,
			Services:    map[string]arbiter.FleetService{"web": {Holder: &holder, Serving: true, Epoch: 1}},
			Hosts:       map[string]arbiter.FleetHost{"host-a": {LastSeenAgo: 0.4, Services: []string{"web"}}},
		},
		Unsupervised: []string{"orphan"},
		Note:         "the arbiter is unreachable: showing this host's view only",
	}

	var buf strings.Builder
	renderFleetLocal(&buf, view)
	out := buf.String()
	for _, want := range []string{
		"host: host-a", "supervised here", "blog", "web", "peers", "host-b", "2.5",
		"services", "serving", "hosts", "drift", "orphan", "note:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("the local summary is missing %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "host: host-a") > strings.Index(out, "services") {
		t.Fatalf("this host's own view must come first:\n%s", out)
	}
}

// --socket with no supervisor behind it is the one local-mode failure: the
// admin client's own "conch is not answering" message, exit non-zero.
func TestFleetSocketModeWithNoSupervisorFails(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "missing.sock")
	stdout, stderr, code := captureOutput(t, func() int {
		return run([]string{"fleet", "--socket", socket})
	})
	if code == 0 {
		t.Fatalf("a missing supervisor must not exit 0 (stdout=%q)", stdout)
	}
	if !strings.Contains(stderr, "not answering") {
		t.Fatalf("the failure must say conch is not answering, got %q", stderr)
	}
}
