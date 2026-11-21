// Package dockerapi is conch's client for the local Docker Engine API over
// the unix socket (spec §8, §10). It knows two things and nothing else: how
// to reach the daemon (one http.Client dialing a unix socket, every call
// bounded by its own context deadline) and the small slice of the Engine API
// conch needs (inspect/start/stop/kill a long-lived service container,
// create/wait/log/remove a one-off hook container). Everything above this
// layer — remove-if-exists ordering, hook chains, eligibility — belongs to
// Task 10 and the supervisor, not here.
package dockerapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// apiVersion is pinned on every request so a daemon upgrade can never
// silently change a response shape underneath conch.
const apiVersion = "v1.43"

// Client is conch's connection to the local Docker daemon. There is no host
// or port to dial, only a filesystem path, so the URLs built below all use
// the placeholder host "docker" — DialContext ignores it and always dials
// socket.
//
// Timeout is the default per-call budget, used by every method except Stop
// and Wait, which take their own timeout as an argument and compute their
// own context deadline from it instead. That is why there is no
// client-wide http.Client.Timeout: a single fixed value can't be both "short
// enough to bound a hung daemon on an ordinary call" and "long enough to
// cover a slow container's configured stop grace period," so the deadline is
// chosen per call.
type Client struct {
	socket  string
	timeout time.Duration
	http    *http.Client
}

// New builds a Client that dials socket for every request, with
// timeoutSeconds as the default per-call deadline.
func New(socket string, timeoutSeconds float64) *Client {
	var dialer net.Dialer
	return &Client{
		socket:  socket,
		timeout: time.Duration(timeoutSeconds * float64(time.Second)),
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, "unix", socket)
				},
			},
		},
	}
}

// ContainerState is what Inspect distinguishes: a container that has never
// been created (or was removed) is not the same fact as one that exists but
// is stopped. Eligibility checks (spec §8) need to tell those apart, hence
// two separate bools rather than one tri-state string. Image is the
// container's configured image ref (Config.Image — e.g.
// "cloudflare/cloudflared:2026.10.4"), which parity and version checks read;
// it is empty when the container does not exist.
type ContainerState struct {
	Exists  bool
	Running bool
	Image   string
}

// do issues one request against the daemon with a deadline of timeout, and
// returns the response together with the cancel func the caller must defer
// — after it is done reading the body, not before, since cancelling early
// would abort an in-flight body read.
func (c *Client) do(method, path string, body io.Reader, timeout time.Duration) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+apiVersion+path, body)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return resp, cancel, nil
}

// Inspect reports whether name exists and, if so, whether it is running. A
// 404 is not an error — it is the normal, expected answer for "no such
// container" — and maps to the zero ContainerState (Exists:false). Any other
// non-200 is a real failure the caller needs to see, since it means the
// daemon couldn't answer the question at all rather than answering "no."
func (c *Client) Inspect(name string) (ContainerState, error) {
	resp, cancel, err := c.do(http.MethodGet, "/containers/"+name+"/json", nil, c.timeout)
	if err != nil {
		return ContainerState{}, err
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ContainerState{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return ContainerState{}, fmt.Errorf("inspect %s: status %d", name, resp.StatusCode)
	}

	var wire struct {
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
		Config struct {
			Image string `json:"Image"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return ContainerState{}, fmt.Errorf("inspect %s: decode: %w", name, err)
	}
	return ContainerState{Exists: true, Running: wire.State.Running, Image: wire.Config.Image}, nil
}

// Start starts name. A 304 means the container was already running —
// idempotent reconcile treats "already in the desired state" as success, not
// a failure the caller has to special-case.
//
// started reports whether this call is what TRANSITIONED the container:
// true for the daemon's 200/204, false for its 304. It is not a success flag
// — a false with a nil error means "already running", which is the reconcile
// getting what it asked for — and a caller that only wants the desired state
// reached is right to discard it. It exists for the callers that NARRATE:
// "started container" said about a container that was already up is a false
// event in the log, and the 304 is the daemon's own answer to which it was,
// already on the wire and previously thrown away.
func (c *Client) Start(name string) (started bool, err error) {
	resp, cancel, err := c.do(http.MethodPost, "/containers/"+name+"/start", nil, c.timeout)
	if err != nil {
		return false, err
	}
	defer cancel()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return true, nil
	case http.StatusNotModified:
		return false, nil
	default:
		return false, fmt.Errorf("start %s: status %d", name, resp.StatusCode)
	}
}

// Stop stops name, giving it up to timeoutSeconds to exit gracefully before
// the daemon kills it. The call's own context deadline is timeoutSeconds+5,
// not the client's default: the stop itself is entitled to its configured
// grace period, but the HTTP call that waits for it to finish still needs a
// bound, so it gets that grace period plus headroom for the call itself
// rather than being cut short by an unrelated default timeout.
//
// A 304 means the container was already stopped — the same idempotent-
// reconcile reasoning as Start's 304: the reconcile loop asks for a state,
// not for permission to no-op, so "already there" is success.
func (c *Client) Stop(name string, timeoutSeconds int) error {
	path := fmt.Sprintf("/containers/%s/stop?t=%d", name, timeoutSeconds)
	callTimeout := time.Duration(timeoutSeconds+5) * time.Second
	resp, cancel, err := c.do(http.MethodPost, path, nil, callTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotModified:
		return nil
	default:
		return fmt.Errorf("stop %s: status %d", name, resp.StatusCode)
	}
}

// Kill sends SIGKILL to name immediately, no grace period — the supervisor's
// escalation path when Stop's deadline has already passed.
func (c *Client) Kill(name string) error {
	resp, cancel, err := c.do(http.MethodPost, "/containers/"+name+"/kill", nil, c.timeout)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("kill %s: status %d", name, resp.StatusCode)
	}
}

// ImagePresent reports whether ref is present locally. It never pulls (spec
// §8 rule 2: hook images gate eligibility and are never fetched at
// promotion time) and collapses every failure mode — not found, daemon
// unreachable, deadline exceeded — to false, since eligibility only ever
// needs a yes/no and a promotion path must never depend on distinguishing
// "missing" from "couldn't ask."
func (c *Client) ImagePresent(ref string) bool {
	resp, cancel, err := c.do(http.MethodGet, "/images/"+ref+"/json", nil, c.timeout)
	if err != nil {
		return false
	}
	defer cancel()
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Healthy reports whether the daemon itself is reachable and answering.
func (c *Client) Healthy() bool {
	resp, cancel, err := c.do(http.MethodGet, "/_ping", nil, c.timeout)
	if err != nil {
		return false
	}
	defer cancel()
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// RunSpec describes a one-off hook container (Task 10 builds these from
// config.HookSpec; this package only knows how to run one). There is
// deliberately no Env field: secrets travel only through EnvFile, which
// Create reads from disk itself. A RunSpec can therefore never carry a
// secret value inline — not in this struct, not in a log line that dumps
// it, not anywhere a `docker inspect` on the resulting container could read
// it back (spec §8 rule 1: env-file only, never inline).
type RunSpec struct {
	Name    string
	Image   string
	Argv    []string
	Binds   []string
	EnvFile string
	Network string
	Labels  map[string]string
}

// createBody is the Engine API's container-create wire shape, narrowed to
// what conch needs. AutoRemove is deliberately absent, on every path through
// this file, forever: with it set, a failed hook's container — and the
// stderr in its logs — vanishes before conch can read it, which is exactly
// the one thing an operator needs when a hook fails (spec §8 rule 3).
// Remove-if-exists is handled explicitly by the hook runner (Task 10)
// before Create is ever called, never automatically by the daemon here.
type createBody struct {
	Image      string            `json:"Image"`
	Cmd        []string          `json:"Cmd,omitempty"`
	Env        []string          `json:"Env,omitempty"`
	Labels     map[string]string `json:"Labels,omitempty"`
	HostConfig hostConfig        `json:"HostConfig"`
}

type hostConfig struct {
	Binds       []string `json:"Binds,omitempty"`
	NetworkMode string   `json:"NetworkMode,omitempty"`
}

// Create creates (but does not start) the container described by spec.
// EnvFile, if set, is read from disk here and its lines sent as the create
// body's Env — the only path by which a hook container's environment is
// populated.
func (c *Client) Create(spec RunSpec) error {
	env, err := readEnvFile(spec.EnvFile)
	if err != nil {
		return fmt.Errorf("create %s: env file: %w", spec.Name, err)
	}

	payload, err := json.Marshal(createBody{
		Image:  spec.Image,
		Cmd:    spec.Argv,
		Env:    env,
		Labels: spec.Labels,
		HostConfig: hostConfig{
			Binds:       spec.Binds,
			NetworkMode: spec.Network,
		},
	})
	if err != nil {
		return fmt.Errorf("create %s: marshal: %w", spec.Name, err)
	}

	path := "/containers/create?name=" + url.QueryEscape(spec.Name)
	resp, cancel, err := c.do(http.MethodPost, path, bytes.NewReader(payload), c.timeout)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		return nil
	}
	return fmt.Errorf("create %s: status %d", spec.Name, resp.StatusCode)
}

// readEnvFile parses a docker-style env file: one KEY=VALUE per line, blank
// lines and #-comment lines skipped. Parsing happens here, inside Create,
// rather than upstream in config or a hook builder, so a secret value's only
// time in memory as a Go string is the trip from disk into this request
// body — it never passes through RunSpec or anything a caller might log.
func readEnvFile(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		env = append(env, line)
	}
	return env, nil
}

// Wait blocks, from the daemon's point of view, until name exits, bounded by
// timeoutSeconds — the hook's own run budget (config.HookSpec.Timeout via
// Task 10), threaded through per call like Stop's timeout argument rather
// than falling back to the client's constructor default.
//
// Unlike Stop, this deadline is not padded with extra call headroom. Stop
// tells the daemon its own budget (the "?t=" query parameter) and needs a
// few seconds beyond that for the daemon to act and reply; the Wait
// endpoint takes no timeout of its own to coordinate with — timeoutSeconds
// here is the *only* thing enforcing a hook's run budget at all. A context
// deadline firing on this call is therefore not a transport failure being
// misread as a hook result: it is the "hanging hook is contained and
// bounded by its deadline" mechanism spec §8 rule 5 requires, and Task 10
// must treat the resulting error as "the hook timed out," not as "the
// daemon went away."
func (c *Client) Wait(name string, timeoutSeconds float64) (int, error) {
	timeout := time.Duration(timeoutSeconds * float64(time.Second))
	resp, cancel, err := c.do(http.MethodPost, "/containers/"+name+"/wait", nil, timeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("wait %s: status %d", name, resp.StatusCode)
	}

	var wire struct {
		StatusCode int `json:"StatusCode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return 0, fmt.Errorf("wait %s: decode: %w", name, err)
	}
	return wire.StatusCode, nil
}

// Logs fetches up to tailLines of combined stdout+stderr as plain text.
// Failures collapse to "" rather than an error: Logs's one caller (Task 10,
// reading a failed hook's stderr for the operator) needs best-effort text
// to put in a notification, not another error path to handle on top of the
// hook failure it's already reporting.
func (c *Client) Logs(name string, tailLines int) string {
	path := fmt.Sprintf("/containers/%s/logs?stdout=1&stderr=1&tail=%d", name, tailLines)
	resp, cancel, err := c.do(http.MethodGet, path, nil, c.timeout)
	if err != nil {
		return ""
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}
	return demux(resp.Body)
}

// demux strips Docker's multiplexed-stream framing: an 8-byte header per
// frame (1 byte stream type, 3 reserved bytes, a 4-byte big-endian payload
// length) followed by that many bytes of payload, repeated to EOF. This is
// the format the daemon always uses for a non-TTY container's logs, which
// every hook container is (RunSpec has no TTY option), so it is the only
// shape Logs ever has to handle. Any read short of a full frame (a
// truncated body, an early EOF) just stops and returns what was decoded so
// far, matching Logs's best-effort contract.
func demux(r io.Reader) string {
	var out bytes.Buffer
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if _, err := io.CopyN(&out, r, int64(size)); err != nil {
			break
		}
	}
	return out.String()
}

// Remove deletes name, always with ?force=1. A 404 — nothing to remove — is
// success, not a failure: the hook runner's remove-if-exists step (Task 10)
// must not fail just because there was nothing there to begin with.
//
// force=1 is not optional. Every one of this package's callers (remove-if-
// exists before a hook Create, KillOrphans' Kill-then-Remove at startup)
// calls Remove right after asking the container to stop, never after
// confirming it already has — Kill returns as soon as the daemon has sent
// the signal, not once the container has actually exited, so a plain DELETE
// lands in a real race window where the daemon still considers it running
// and answers 409. force=1 tells the daemon to stop it first if necessary,
// closing that window; it is a no-op, not a hazard, against a container
// that's already exited (spec §8's "no AutoRemove" rule is about Docker
// never removing a container on its own initiative — it says nothing about
// how conch's own explicit remove call is allowed to behave once conch has
// decided to remove one).
func (c *Client) Remove(name string) error {
	resp, cancel, err := c.do(http.MethodDelete, "/containers/"+name+"?force=1", nil, c.timeout)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("remove %s: status %d", name, resp.StatusCode)
	}
}

// ResolveByPrefix finds the one container whose name — trimmed of Docker's
// leading "/", the same as ListByLabel — starts with prefix, for a
// service whose deploy tool (Kamal, in particular) appends a version suffix
// to the container name on every deploy, so an exact configured name can
// never match it twice in a row.
//
// Docker's own "name" filter is a SUBSTRING match, not a prefix match, so
// every candidate the daemon returns is re-checked with strings.HasPrefix
// before being trusted: a container named "other-web-web-3cb8" would
// otherwise pass a filter for "web-web-" even though it is not a prefix
// match at all.
//
// No genuine match is not an error — it returns ("", nil). During a Kamal
// deploy's brief window the old and new containers can both legitimately
// exist; between deploys, or before the first one, none may exist yet. Both
// are facts about the world, not failures, exactly like Inspect's 404. A
// transport failure or non-200, in contrast, means the daemon could not
// answer the question at all, and that is still an error.
//
// Multiple genuine matches are resolved by preferring a RUNNING container
// over a stopped one — but never REQUIRING one: on a standby host the
// versioned container can be legitimately stopped, and always returning ""
// in that case would make the service permanently ineligible and unable to
// ever start. Among containers in the same running/not-running state, the
// most recently Created one wins, since that is the one the deploy tool
// just started.
//
// Selection is unaffected by how many candidates there are — the SECOND
// return value, running, is how many of the genuine matches are RUNNING,
// purely for the caller to notice a blue/green overlap window (more than one
// LIVE candidate at once, ordinarily brief, during which resolution can be
// unstable from one check to the next); it never changes which name wins.
//
// Deliberately the running count and not the total: Kamal leaves every
// previous version's container on the host, exited, forever, so a host three
// deploys old genuinely matches six names and only one of them can be
// selected twice in a row. Counting those made the caller's overlap alert
// fire on every check of every prefixed service on a healthy fleet.
func (c *Client) ResolveByPrefix(prefix string) (name string, running int, err error) {
	filters, err := json.Marshal(map[string][]string{"name": {prefix}})
	if err != nil {
		return "", 0, err
	}
	path := "/containers/json?all=1&filters=" + url.QueryEscape(string(filters))
	resp, cancel, err := c.do(http.MethodGet, path, nil, c.timeout)
	if err != nil {
		return "", 0, err
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("resolve %s*: status %d", prefix, resp.StatusCode)
	}

	var wire []struct {
		Names   []string `json:"Names"`
		State   string   `json:"State"`
		Created int64    `json:"Created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return "", 0, fmt.Errorf("resolve %s*: decode: %w", prefix, err)
	}

	var best string
	var bestRunning bool
	var bestCreated int64
	have := false
	for _, container := range wire {
		if len(container.Names) == 0 {
			continue
		}
		name := strings.TrimPrefix(container.Names[0], "/")
		if !strings.HasPrefix(name, prefix) {
			continue // Docker's name filter is substring, not prefix — recheck
		}
		isRunning := container.State == "running"
		if isRunning {
			running++
		}
		switch {
		case !have:
			best, bestRunning, bestCreated, have = name, isRunning, container.Created, true
		case isRunning && !bestRunning:
			best, bestRunning, bestCreated = name, isRunning, container.Created
		case isRunning == bestRunning && container.Created > bestCreated:
			best, bestCreated = name, container.Created
		}
	}
	return best, running, nil
}

// LabeledContainer is one container ListWithLabel found: its name (leading
// "/" trimmed, like every other name this package returns), the value of the
// label that was asked for, whether it is running, and every OTHER label it
// carries.
//
// Labels is here because "does this container carry some other label too" is
// a question the caller cannot answer any other way: Docker's label filter
// ANDs, it cannot negate, so a caller that must EXCLUDE containers carrying a
// second label (the supervisor's conch.service sweep must leave conch.hook
// containers to the hook runner — see internal/supervisor/sweep.go) would
// otherwise need a second listing and a set difference. The daemon already
// sends the whole map in this response, so carrying it costs nothing.
type LabeledContainer struct {
	Name    string
	Value   string
	Running bool
	Labels  map[string]string
}

// ListWithLabel returns every container (running or not) carrying label key
// AT ALL, whatever its value — a label-key-only filter, which is a shape the
// Engine API supports directly ({"label":["conch.service"]}) and the one
// ListByLabel below cannot express.
//
// Every failure — transport, non-200, unparseable body — collapses to nil,
// the same best-effort contract as ListByLabel. The one caller (the
// supervisor's label sweep) only ever STOPS what this returns, so "couldn't
// ask" collapsing to "nothing is labeled" errs towards leaving containers
// alone, which is the safe direction: a sweep that skips a check stops
// nothing that check and reconsiders on the next one.
func (c *Client) ListWithLabel(key string) []LabeledContainer {
	filters, err := json.Marshal(map[string][]string{"label": {key}})
	if err != nil {
		return nil
	}
	path := "/containers/json?all=1&filters=" + url.QueryEscape(string(filters))
	resp, cancel, err := c.do(http.MethodGet, path, nil, c.timeout)
	if err != nil {
		return nil
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var wire []struct {
		Names  []string          `json:"Names"`
		State  string            `json:"State"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return nil
	}
	out := make([]LabeledContainer, 0, len(wire))
	for _, container := range wire {
		if len(container.Names) == 0 {
			continue
		}
		out = append(out, LabeledContainer{
			Name:    strings.TrimPrefix(container.Names[0], "/"),
			Value:   container.Labels[key],
			Running: container.State == "running",
			Labels:  container.Labels,
		})
	}
	return out
}

// ListByLabel returns the name of every container (running or not) carrying
// label key=value, with the leading "/" Docker prefixes every container
// name with trimmed off. Used at startup to find orphaned conch-hook-*
// containers (spec §8 rule 4) before adopting any lease.
func (c *Client) ListByLabel(key, value string) []string {
	filters, err := json.Marshal(map[string][]string{"label": {key + "=" + value}})
	if err != nil {
		return nil
	}
	path := "/containers/json?all=1&filters=" + url.QueryEscape(string(filters))
	resp, cancel, err := c.do(http.MethodGet, path, nil, c.timeout)
	if err != nil {
		return nil
	}
	defer cancel()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var wire []struct {
		Names []string `json:"Names"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return nil
	}
	names := make([]string, 0, len(wire))
	for _, container := range wire {
		if len(container.Names) == 0 {
			continue
		}
		names = append(names, strings.TrimPrefix(container.Names[0], "/"))
	}
	return names
}
