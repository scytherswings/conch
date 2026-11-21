package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
