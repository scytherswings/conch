// Package config decodes and validates a host's conch YAML configuration
// (spec §11). Decoding rejects unknown keys everywhere in the schema so a
// misspelled or renamed field fails loudly instead of silently reverting to
// a default.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is one host's full configuration: the arbiter it reports to, the
// timing knobs, and every service it supervises.
type Config struct {
	Host        string        `yaml:"host"`
	Arbiter     ArbiterConfig `yaml:"arbiter"`
	Timing      TimingConfig  `yaml:"timing"`
	Disk        DiskConfig    `yaml:"disk"`
	Webhooks    WebhookConfig `yaml:"webhooks"`
	Docker      DockerConfig  `yaml:"docker"`
	Updates     UpdatesConfig `yaml:"updates"`
	Services    []Service     `yaml:"services"`
	HostNetwork bool          `yaml:"host_network"`
}

// ArbiterConfig is where and how conch reaches the arbiter, and what to do
// if it can't. Backend selects which arbiter client is constructed. Under
// backend: dynamo, Validate refuses any of the cloudflare-only fields (URL,
// SecretEnv, SecretFile) being set — a config carrying both backends' fields
// is confused about which one it targets (spec §5).
type ArbiterConfig struct {
	Backend         string  `yaml:"backend"` // "cloudflare" (default) | "dynamo"
	URL             string  `yaml:"url"`
	SecretEnv       string  `yaml:"secret_env"`
	SecretFile      string  `yaml:"secret_file"`
	OnLoss          string  `yaml:"on_loss"`
	TimeoutSeconds  float64 `yaml:"timeout"`
	Table           string  `yaml:"table"`
	Region          string  `yaml:"region"`
	Endpoint        string  `yaml:"endpoint"` // optional; DynamoDB Local / tests only
	CredentialsFile string  `yaml:"credentials_file"`
}

// TimingConfig is the four knobs everything else derives from (spec §6).
type TimingConfig struct {
	TTL          float64 `yaml:"ttl"`
	Interval     float64 `yaml:"interval"`
	DemoteAfter  int     `yaml:"demote_after"`
	PromoteAfter int     `yaml:"promote_after"`
}

// DiskConfig is the free-space guard that inspects the conch container's
// bound Docker data directory.
type DiskConfig struct {
	Path      string  `yaml:"path"`
	MinFreeGB float64 `yaml:"min_free_gb"`
}

// WebhookConfig is the optional per-event notification targets. All are
// best-effort, off the loop's critical path. Alert covers every non-lease
// notification the supervisor emits — decide's Alert action, an unmeasurable
// disk floor, a stop-escalation to a wedged daemon, and a cross-host
// fingerprint mismatch (spec §11) — since none of those has a lease
// transition of its own to route on.
type WebhookConfig struct {
	Promote  string `yaml:"promote"`
	Demote   string `yaml:"demote"`
	Evacuate string `yaml:"evacuate"`
	Reclaim  string `yaml:"reclaim"`
	Alert    string `yaml:"alert"`
}

// DockerConfig is how conch reaches the Docker socket.
type DockerConfig struct {
	Socket         string  `yaml:"socket"`
	TimeoutSeconds float64 `yaml:"timeout"`
}

// UpdatesConfig is the periodic best-effort check for newer versions of the
// connector images this host supervises — cloudflared above all. The fleet
// deliberately pins cloudflared (its bump is a reviewable one-line commit),
// so "is there a newer release to pin?" is a question with a right answer
// outside the fleet, and this section is the knob for answering it. The
// check is observation and nothing else: it is off the loop's critical path,
// never gatekeeps a decision, and a source that cannot be reached fails
// silently (a version source the host cannot talk to is a network problem,
// not a reason to stop serving).
type UpdatesConfig struct {
	// Interval is a Go duration string (e.g. "24h"). The empty value or "0"
	// disables the check outright (an operator whose hosts cannot reach the
	// source still gets a working supervisor, just no version prompting). A
	// negative value is a validation fault — a negative interval parses fine
	// as a duration and ticks instantly, which is spam software, so Validate
	// refuses it (validate.go).
	Interval string `yaml:"interval"`

	// TimeoutSeconds bounds one version-source fetch. The check is
	// best-effort; after this budget the source is written off for this run
	// and nothing waits on it.
	TimeoutSeconds float64 `yaml:"timeout"`
}

// Service is one supervised unit: a service container plus the connector
// that exposes it, its accessory containers, its health check, its
// promote/evacuate hooks, and its evacuation self-expiry.
type Service struct {
	Name      string          `yaml:"name"`
	LeaseKey  string          `yaml:"lease_key"`
	Container string          `yaml:"container"`
	Connector ConnectorConfig `yaml:"connector"`
	// Accessories are long-lived auxiliary containers (Kamal's word for them)
	// that must run on exactly the host currently serving the service — a
	// replication sidecar above all: two of them on two hosts writing one
	// replica corrupt its generation history, and a standby replicating a
	// stale file is worse than none. conch reconciles them like the
	// connector: started with the service on promotion and on every ACTIVE
	// check that finds one down (adoption included, where hooks never run),
	// stopped wherever the service does not serve. Exact names only —
	// accessories are hand-created or Kamal-booted, never versioned, so
	// Validate refuses any "*" — and one owner per container: a name here may
	// not double as any service's container, connector, or another service's
	// accessory.
	Accessories []string `yaml:"accessories"`
	// WarmAccessories are accessory containers that run on EVERY host,
	// whatever the lease says — a service's own database above all. conch
	// starts one it finds down on any check, in any phase, on either host,
	// adoption included, and no conch path ever stops one.
	//
	// Why that is safe, given that the field above says the opposite about
	// accessories: a regular accessory is a single-writer SHIPPER (a
	// replication sidecar), and two shippers on two hosts writing one replica
	// corrupt its generation history — so a shipper must run on exactly the
	// serving host, and stays here in accessories. A warm accessory is a
	// RESTORE TARGET: the standby's copy is stale, fenced-off local state that
	// promotion overwrites wholesale — a pg_restore, an rclone sync — before
	// anything serves out of it. The "no second writer on the replica"
	// invariant binds the shippers; it says nothing about a database nobody
	// is shipping to and nobody is serving from. What running it warm buys is
	// the incident this field exists to end: a deploy against the standby
	// fails when the database it builds against is not running there.
	//
	// Exact names only and one owner per container, exactly as accessories:
	// Validate refuses any "*", and refuses a container that is warm in one
	// place and lease-following in another — the warm reconcile would start
	// it on every host while the accessory reconcile stopped it wherever that
	// service does not serve, once per interval, forever.
	WarmAccessories []string     `yaml:"warm_accessories"`
	Health          HealthConfig `yaml:"health"`
	Hooks           HookSet      `yaml:"hooks"`
	Evacuation      EvacConfig   `yaml:"evacuation"`
}

// ConnectorConfig is the edge connector (e.g. cloudflared) fronting the
// service container.
type ConnectorConfig struct {
	Container   string  `yaml:"container"`
	ReadyURL    string  `yaml:"ready_url"`
	StopTimeout float64 `yaml:"stop_timeout"`
}

// HealthConfig is the readiness probe conch polls before promoting.
//
// Host, when set, becomes the probe request's Host header (req.Host, not a
// header-map entry — net/http ignores a "Host" key written there). This is
// what lets the check traverse a shared reverse proxy (kamal-proxy and
// similar) addressed by container IP or an internal name, using the public
// vhost the proxy routes on, so the probe sees the same routing decision a
// real user's request would rather than a nearer, proxy-bypassing path
// direct to the app container. Left unset, the request's Host is whatever
// URL's own host is — unchanged from before this field existed.
type HealthConfig struct {
	URL            string  `yaml:"url"`
	Host           string  `yaml:"host"`
	Expect         []int   `yaml:"expect"`
	TimeoutSeconds float64 `yaml:"timeout"`
}

// HookSet is the promote and evacuate hook chains for a service.
type HookSet struct {
	Promote  []HookSpec `yaml:"promote"`
	Evacuate []HookSpec `yaml:"evacuate"`
}

// HookSpec is one containerized hook step.
//
// Argv, for a PROMOTE hook only, may contain the literal token "{container}"
// as one or more of its arguments: the supervisor substitutes it, in every
// position it appears, with this check's resolved service-container name —
// the same value, and the same exact-literal strings.ReplaceAll, that
// HealthConfig.URL's own "{container}" placeholder substitutes into the
// health probe (supervisor.go's readings/resolveContainer). This is what
// lets a promote hook re-register a reverse proxy (kamal-proxy and similar)
// with the freshly resolved app container after a Kamal-style versioned
// deploy, rather than the stale name from before it. Validate refuses every
// near-miss casing ({Container}, ${container}, {{container}} and the rest,
// see validate.go's hookArgvPlaceholderFault) because only the exact literal
// ever substitutes; anything else would silently reach the hook untouched.
//
// EVACUATE hooks get NO such substitution, and "{container}" is refused
// outright in evacuate Argv rather than silently passed through: an
// evacuation runs off this service's own record (evacuate.go), never through
// the check path resolveContainer belongs to, so there is no resolved
// container name in scope to give it. Left unrefused, the literal string
// "{container}" would reach docker as one of the hook's arguments.
type HookSpec struct {
	Name    string   `yaml:"name"`
	Image   string   `yaml:"image"`
	Argv    []string `yaml:"argv"`
	Binds   []string `yaml:"binds"`
	EnvFile string   `yaml:"env_file"`
	Network string   `yaml:"network"`
	Timeout float64  `yaml:"timeout"`
}

// EvacConfig is a service's evacuation self-expiry.
type EvacConfig struct {
	// AutoReclaimAfter is a Go duration string (e.g. "30m"), stored raw and
	// parsed only at validation time — config is data, not a clock read.
	AutoReclaimAfter string `yaml:"auto_reclaim_after"`
}

// Key is the identifier the arbiter tracks: LeaseKey if set, else Name.
func (s Service) Key() string {
	if s.LeaseKey != "" {
		return s.LeaseKey
	}
	return s.Name
}

// ContainerPrefix reports whether Container is written as a trailing-"*"
// prefix pattern (e.g. "web-web-*") rather than one exact name. Deploy tools
// like Kamal append a version suffix to a container's name on every deploy,
// which an exact-match container: can never track — prefix is the literal
// text before the "*", what the daemon-side lookup (dockerapi.ResolveByPrefix)
// matches against each check to find whichever concretely-named container
// currently answers to it. isPrefix is false for an ordinary exact name,
// which needs no such lookup at all, and also false for a bare "*" — Validate
// rejects that as a fault, and reporting it here as a prefix would resolve
// against every container on the host.
func (s Service) ContainerPrefix() (prefix string, isPrefix bool) {
	if strings.HasSuffix(s.Container, "*") && s.Container != "*" {
		return strings.TrimSuffix(s.Container, "*"), true
	}
	return "", false
}

func defaults() Config {
	return Config{
		Timing: TimingConfig{
			TTL:          60,
			Interval:     3,
			DemoteAfter:  3,
			PromoteAfter: 6,
		},
		Arbiter: ArbiterConfig{
			Backend:        "cloudflare",
			OnLoss:         "freeze",
			TimeoutSeconds: 5,
		},
		Docker: DockerConfig{
			Socket:         "/var/run/docker.sock",
			TimeoutSeconds: 30,
		},
		Updates: UpdatesConfig{
			Interval:       "24h",
			TimeoutSeconds: 5,
		},
		Disk: DiskConfig{
			MinFreeGB: 10,
		},
	}
}

// defaultHookTimeout is what a HookSpec with no configured timeout gets.
// Without it, an operator who omits `timeout:` gets the zero value threaded
// straight into dockerapi.Wait(name, 0), which builds an already-expired
// context — every hook would fail instantly, forever, with "timeout after
// 0s" and never actually run (Task 10). A 0 timeout is never a meaningful
// choice (nothing can finish in 0 seconds), so defaulting it is the safe
// direction; a genuinely negative value is a different, actually-invalid
// input and is left alone here so Validate can flag it instead of silently
// swallowing it into this same default (see the Timeout==0 check below).
const defaultHookTimeout = 300

// applyServiceDefaults fills in per-service defaults that yaml.v3 cannot
// apply on its own, since decoding into a zero-value slice element never
// sees the field-level defaults set above.
func applyServiceDefaults(cfg *Config) {
	for i := range cfg.Services {
		svc := &cfg.Services[i]
		if svc.Connector.StopTimeout == 0 {
			svc.Connector.StopTimeout = 35
		}
		if len(svc.Health.Expect) == 0 {
			svc.Health.Expect = []int{200}
		}
		if svc.Health.TimeoutSeconds == 0 {
			svc.Health.TimeoutSeconds = 2
		}
		// Same presence-detection class as the deferred Task 6 finding
		// (progress.md: applyServiceDefaults clobbers explicit zeros with
		// no fault) — an explicit `timeout: 0` is indistinguishable from an
		// omitted one here and gets defaulted too. Deliberately == 0, not
		// <= 0: a negative value must reach Validate unchanged so it can be
		// reported as a fault instead of silently becoming 300.
		for j := range svc.Hooks.Promote {
			if svc.Hooks.Promote[j].Timeout == 0 {
				svc.Hooks.Promote[j].Timeout = defaultHookTimeout
			}
		}
		for j := range svc.Hooks.Evacuate {
			if svc.Hooks.Evacuate[j].Timeout == 0 {
				svc.Hooks.Evacuate[j].Timeout = defaultHookTimeout
			}
		}
	}
}

// Load reads and decodes the YAML file at path, applies defaults, and
// validates it. A non-nil error means the file couldn't be read or parsed
// (including unknown keys); the returned []string carries every semantic
// validation fault found in an otherwise well-formed config, empty when
// valid.
func Load(path string) (Config, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, err
	}
	cfg := defaults()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // unknown keys are a hard error (spec §11)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	applyServiceDefaults(&cfg)
	return cfg, Validate(cfg), nil
}

// Slop is the margin added on top of the arbiter's own timeout to any
// deadline that depends on reaching the arbiter first.
func (c Config) Slop() float64 { return c.Arbiter.TimeoutSeconds + 1 }

// StopDeadline is how long conch allows a service's connector to stop
// before giving up, given the arbiter round-trip it also needs.
func (c Config) StopDeadline(s Service) float64 { return s.Connector.StopTimeout + c.Slop() }

// Fingerprint is a short, order-independent hash of every lease key this
// config supervises, used to detect cross-host config drift (spec §11).
func (c Config) Fingerprint() string {
	keys := make([]string, 0, len(c.Services))
	for _, s := range c.Services {
		keys = append(keys, s.Key())
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(sum[:])[:16]
}

// IntervalDuration parses UpdatesConfig.Interval. A parse error or a zero
// duration both read as "disabled": the check is off unless a config names a
// positive interval. A negative value parses to a negative duration and is a
// validation fault rather than a ticker conch lets run (validate.go).
func (u UpdatesConfig) IntervalDuration() time.Duration {
	d, err := time.ParseDuration(u.Interval)
	if err != nil {
		return 0
	}
	return d
}
