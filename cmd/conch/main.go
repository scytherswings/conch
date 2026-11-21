// Command conch is the one binary spec §10 describes: the supervisor loop
// (`run`), config linting (`validate`), and the thin admin-socket clients
// (`status`, `evacuate`, `reclaim`) that talk to a running supervisor over
// the unix socket inside the container. Every sequence those verbs start
// belongs to the supervisor (package admin's own doc comment); this file's
// only job is flag parsing and wiring the concrete clients supervisor.New
// cannot invent for itself.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/scytherswings/conch/internal/admin"
	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/dockerapi"
	"github.com/scytherswings/conch/internal/dynarbiter"
	"github.com/scytherswings/conch/internal/hooks"
	"github.com/scytherswings/conch/internal/supervisor"
	"github.com/scytherswings/conch/internal/webhook"
)

// defaultSocket is spec §10's admin surface path: inside the container only,
// reached by `docker exec` / `kamal accessory exec --reuse` — nothing is
// installed on hosts.
const defaultSocket = "/run/conch/admin.sock"

// dryRunBanner is spec §10's wording for a dry run: it logs every decision and
// the actions it would take, executes none, and sends the arbiter only `read`
// lease ops — never renew or release — so a dry run pointed at a live arbiter
// cannot claim a lease out from under the real active host. "Lease ops" is
// load-bearing: every batch still registers the host's presence, so a dry run is
// a no-claim promise, not a no-write one (arbiter/src/lease.js batch()).
const dryRunBanner = "DRY-RUN: decisions logged, nothing executed, arbiter lease ops are reads only (presence still recorded)"

// defaultLogLevel is what a host runs at: lifecycle, and anything degraded or
// broken. The per-check decision lines are Debug, so a fleet that checks every
// three seconds is not paying to say the same thing all day.
const defaultLogLevel = "info"

const usage = `conch: one binary, the supervisor and its admin surface (spec §10)

  conch run      --config /etc/conch/conch.yml [--dry-run] [--log-level info] [--socket ` + defaultSocket + `]
  conch validate --config <path>
  conch status   [--socket ` + defaultSocket + `]
  conch evacuate <service...|--all> [--hold] [--force] [--socket ` + defaultSocket + `]
  conch reclaim  <service> [--socket ` + defaultSocket + `]
`

func main() {
	os.Exit(run(os.Args[1:]))
}

// run is main's entire logic, pulled out so main_test.go can drive it
// directly: exit codes matter here (validate: 0 clean, 1 faults; run:
// refuses on faults with 2; evacuate: 0 confirmed, 1 timeout-or-refused),
// and testing os.Exit itself is not possible.
func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "run":
		return cmdRun(rest)
	case "validate":
		return cmdValidate(rest)
	case "status":
		return cmdStatus(rest)
	case "evacuate":
		return cmdEvacuate(rest)
	case "reclaim":
		return cmdReclaim(rest)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "conch: unknown command %q\n\n%s", sub, usage)
		return 2
	}
}

// cmdValidate is spec §10's `conch validate <config>`: parse and composition
// checks, CI-able. It never stops at the first fault — an operator fixing a
// config one problem at a time deserves the whole list on the first run, not
// one fault per re-run.
func cmdValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to the conch YAML config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "validate: --config is required")
		return 2
	}

	cfg, faults, err := config.Load(*cfgPath)
	if err != nil {
		// A file that can't be read or parsed (including an unknown key,
		// spec §11) is exactly as much a validation failure as a semantic
		// fault is — it just has no faults list to join, since decoding
		// never got far enough to build one.
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	if len(faults) > 0 {
		for _, f := range faults {
			fmt.Fprintln(os.Stderr, f)
		}
		return 1
	}

	// Warnings are advisory by codebase rule (config.Warnings): printed so
	// the operator sees them, never part of the exit code — validate's
	// contract is "can this config work", not "is it free to run".
	for _, w := range config.Warnings(cfg) {
		fmt.Fprintln(os.Stderr, w)
	}
	fmt.Fprintln(os.Stdout, "config is valid")
	return 0
}

// arbiterBackend restates supervisor's unexported arbiterAPI so main can
// hold either concrete client — internal/arbiter's HTTP client or
// internal/dynarbiter's DynamoDB client — in one variable while the switch
// below decides which to build. supervisor.Options.Arbiter accepts either:
// they satisfy arbiterAPI structurally, not by naming it.
type arbiterBackend interface {
	Batch(ttl float64, fingerprint string, ops map[string]arbiter.Op) arbiter.Reply
}

// cmdRun is spec §10's `conch run`: the supervisor loop. Every step through
// readSecret/LoadCreds runs before anything here can reach the Docker
// socket or the arbiter — a config with faults is refused right here, with
// nothing built yet to tear back down.
func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	cfgPath := fs.String("config", "/etc/conch/conch.yml", "path to the conch YAML config")
	dryRun := fs.Bool("dry-run", false, "log every decision and the actions it would take; execute none; arbiter lease ops are reads only")
	socketPath := fs.String("socket", defaultSocket, "unix socket for the admin surface")
	logLevel := fs.String("log-level", defaultLogLevel,
		"debug|info|warn|error; debug adds every per-check decision, one line per service per check")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// Before the config is even read: a typo here is worth catching without
	// having reached a secret, a socket or the arbiter.
	logger, err := newLogger(*logLevel, *dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return 2
	}
	// Set as the default too, not only passed along: internal/webhook takes
	// slog.Default() at construction, and a notifier logging at a level the
	// operator did not ask for is exactly the noise this flag exists to end.
	slog.SetDefault(logger)

	cfg, faults, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return 2
	}
	if len(faults) > 0 {
		fmt.Fprintln(os.Stderr, "run: refusing to start: this config has faults")
		for _, f := range faults {
			fmt.Fprintln(os.Stderr, f)
		}
		return 2
	}
	// Advisory only, by codebase rule: a config that would cost money (or
	// spend free-tier headroom) still runs — conch recommends, it does not
	// limit. The operator who provisioned more capacity on purpose should
	// not have to fight their own supervisor about it.
	for _, w := range config.Warnings(cfg) {
		fmt.Fprintln(os.Stderr, w)
	}

	instance, err := randomInstance()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: generating an instance id: %s\n", err)
		return 2
	}

	if *dryRun {
		logger.Warn(dryRunBanner)
	}

	clk := clock.NewSystem()
	dk := dockerapi.New(cfg.Docker.Socket, cfg.Docker.TimeoutSeconds)

	var arb arbiterBackend
	switch cfg.Arbiter.Backend {
	case "cloudflare":
		secret, err := readSecret(cfg.Arbiter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: %s\n", err)
			return 2
		}
		arb = arbiter.New(cfg.Arbiter.URL, secret, cfg.Host, instance, cfg.Arbiter.TimeoutSeconds, clk)
	case "dynamo":
		id, secret, err := dynarbiter.LoadCreds(cfg.Arbiter.CredentialsFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: %s\n", err)
			return 2
		}
		ddb := dynarbiter.NewDDB(cfg.Arbiter.Region, cfg.Arbiter.Endpoint, id, secret)
		arb = dynarbiter.New(ddb, cfg.Arbiter.Table, cfg.Host, instance, cfg.Arbiter.TimeoutSeconds, clk)
	default:
		// config.Validate already refuses any backend but these two before
		// cmdRun ever reaches here (the faults check above); this default
		// is a second line of defense, not a path a valid config can take.
		fmt.Fprintf(os.Stderr, "run: arbiter.backend %q is neither cloudflare nor dynamo, or conch has no arbiter client to construct\n", cfg.Arbiter.Backend)
		return 2
	}

	hk := hooks.NewRunner(dk, clk, logger)
	notifier := webhook.New(cfg.Webhooks, clk)

	sup, err := supervisor.New(supervisor.Options{
		Config:   cfg,
		Arbiter:  arb,
		Docker:   dk,
		Hooks:    hk,
		Clock:    clk,
		Notify:   notifier,
		DryRun:   *dryRun,
		Instance: instance,
		Logger:   logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return 2
	}

	ctx, stop := newRunContext()
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		notifier.Run(ctx) // best-effort, off every other goroutine's critical path (spec §11)
	}()
	if cfg.Updates.IntervalDuration() > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sup.RunUpdates(ctx) // version check: off-loop, best-effort
		}()
	}

	adminErrCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := admin.Serve(ctx, *socketPath, sup)
		adminErrCh <- err
		if err != nil {
			// A bind failure here (a missing socket directory, most likely)
			// means the whole run must end, not just this one goroutine —
			// a conch with no admin surface has no `evacuate` and no
			// `status`, silently.
			logger.Error("the admin surface could not start: this conch has no status and no evacuate, check the socket path",
				"socket", *socketPath, "error", err)
			stop()
		}
	}()

	sup.RunForever(ctx) // returns only when ctx is cancelled: SIGTERM does nothing to the world (spec §9)
	wg.Wait()

	if adminErr := <-adminErrCh; adminErr != nil {
		return 1 // already logged, loudly, by the goroutine that hit it
	}
	return 0
}

// newLogger builds the one logger every package writes through, at the level
// `--log-level` asked for. An unrecognised name is refused rather than
// silently defaulted: "conch is quieter than I asked for" is a bad thing to
// discover while reading logs for an incident that already happened.
//
// dryRun floors the level at Info, whatever was asked for. CONTRIBUTING makes
// it a constraint that a dry run "logs every decision and the exact actions it
// would take", and those lines are Info — so `--dry-run --log-level warn`
// would otherwise be a dry run that executes nothing AND says nothing, which
// is not a quieter dry run, it is a broken one.
func newLogger(level string, dryRun bool) (*slog.Logger, error) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, fmt.Errorf("--log-level %q: use debug, info, warn or error", level)
	}
	if dryRun && l > slog.LevelInfo {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

// newRunContext is the whole of `run`'s signal handling, pulled out so it can
// be tested without spinning up a real supervisor: SIGTERM and SIGINT both
// just cancel the context, and that is ALL either signal does (spec §9) —
// conch exits, leases untouched, containers untouched. Distinguishing "a
// deploy is restarting conch" from "the host is going down" is not possible
// from a signal alone, and treating SIGTERM as an evacuation trigger would
// turn every conch deploy into a full failover.
func newRunContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}

// readSecret is spec §11's "env then file": SecretEnv is tried first, and an
// unset or empty value falls through to SecretFile rather than failing
// outright, so a host can be configured with both and not care which one is
// actually populated at runtime.
func readSecret(a config.ArbiterConfig) (string, error) {
	if a.SecretEnv != "" {
		if v := os.Getenv(a.SecretEnv); v != "" {
			return v, nil
		}
	}
	if a.SecretFile != "" {
		raw, err := os.ReadFile(a.SecretFile)
		if err != nil {
			return "", fmt.Errorf("reading arbiter.secret_file %s: %w", a.SecretFile, err)
		}
		secret := strings.TrimSpace(string(raw))
		if secret == "" {
			return "", fmt.Errorf(
				"arbiter.secret_file %s is empty: put the fleet's shared arbiter secret there, or set arbiter.secret_env",
				a.SecretFile)
		}
		return secret, nil
	}
	return "", errors.New(
		"no arbiter secret: arbiter.secret_env names an unset or empty environment variable, " +
			"and arbiter.secret_file is not set either")
}

// randomInstance builds the per-process identity spec §7's duplicate-host
// detection needs: two conches sharing a host name are told apart by this,
// not by anything an operator configures.
func randomInstance() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// cmdStatus is spec §10's `conch status`: the running supervisor's JSON
// snapshot, indented for a human reading it at 3am.
func cmdStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	socketPath := fs.String("socket", defaultSocket, "unix socket for the admin surface")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	out, err := (admin.CLI{Socket: *socketPath}).Status()
	if err != nil {
		fmt.Fprintf(os.Stderr, "status: %s\n", err)
		return 1
	}
	fmt.Fprintln(os.Stdout, out)
	return 0
}

// cmdEvacuate is spec §10's `conch evacuate <service...|--all>`: the CLI
// only asks and watches, the supervisor drives every step of the sequence
// (spec §9) — a dropped SSH session cannot leave a host half-evacuated with
// nothing driving it.
func cmdEvacuate(args []string) int {
	fs := flag.NewFlagSet("evacuate", flag.ContinueOnError)
	all := fs.Bool("all", false, "evacuate every service on this host")
	hold := fs.Bool("hold", false, "the bar survives even a peer's claim; cleared only by reclaim")
	force := fs.Bool("force", false, "skip the peer pre-flight check")
	socketPath := fs.String("socket", defaultSocket, "unix socket for the admin surface")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	services := fs.Args()
	if len(services) == 0 && !*all {
		fmt.Fprintln(os.Stderr, "evacuate: name at least one service, or pass --all")
		return 2
	}

	ok, transcript, err := (admin.CLI{Socket: *socketPath}).Evacuate(services, *all, *hold, *force)
	fmt.Fprint(os.Stdout, transcript)
	if err != nil {
		fmt.Fprintf(os.Stderr, "evacuate: %s\n", err)
		return 1
	}
	if !ok {
		return 1
	}
	return 0
}

// cmdReclaim is spec §10's `conch reclaim <service>`: clears this host's own
// evacuation bar for one service and lets it promote again.
func cmdReclaim(args []string) int {
	fs := flag.NewFlagSet("reclaim", flag.ContinueOnError)
	socketPath := fs.String("socket", defaultSocket, "unix socket for the admin surface")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "reclaim: name exactly one service")
		return 2
	}

	if err := (admin.CLI{Socket: *socketPath}).Reclaim(fs.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "reclaim: %s\n", err)
		return 1
	}
	fmt.Fprintln(os.Stdout, "reclaimed")
	return 0
}
