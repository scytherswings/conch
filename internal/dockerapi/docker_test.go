package dockerapi_test

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/dockerapi"
)

// fakeDaemon starts a recording HTTP server over a unix socket, the same
// shape httptest.NewServer gives for TCP. The socket lives under a
// short-prefixed os.MkdirTemp directory rather than t.TempDir(): unix
// socket paths are capped at 104 bytes on macOS, and t.TempDir() embeds the
// full (sub)test name in the path, which overflows that limit for several
// of this file's longer test names.
func fakeDaemon(t *testing.T, handler http.Handler) (socket string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "dockerapi")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	socket = filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return socket
}

func TestStartTreats304AsSuccess(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	started, err := dockerapi.New(socket, 1).Start("web")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Success, but not a transition: this is the one place the daemon's own
	// answer to "was it already running" is pinned, and every caller that
	// narrates a start hangs off it.
	if started {
		t.Fatal("a 304 means the container was already running: started must be false")
	}
}

// TestStartReportsATransitionOn204 is the other half of the 304 mapping: a
// daemon that actually started the container says so, or the bool above
// would be trivially satisfiable by always returning false.
func TestStartReportsATransitionOn204(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	started, err := dockerapi.New(socket, 1).Start("web")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !started {
		t.Fatal("a 204 is the daemon starting the container: started must be true")
	}
}

func TestInspectDistinguishesMissingFromStopped(t *testing.T) {
	var status int
	var body string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	c := dockerapi.New(socket, 1)

	status = http.StatusNotFound
	body = ""
	got, err := c.Inspect("ghost")
	if err != nil {
		t.Fatalf("missing: unexpected error: %v", err)
	}
	if got != (dockerapi.ContainerState{Exists: false}) {
		t.Fatalf("missing: got %+v", got)
	}

	status = http.StatusOK
	body = `{"State":{"Running":false}}`
	got, err = c.Inspect("stopped")
	if err != nil {
		t.Fatalf("stopped: unexpected error: %v", err)
	}
	if got != (dockerapi.ContainerState{Exists: true, Running: false}) {
		t.Fatalf("stopped: got %+v", got)
	}

	status = http.StatusOK
	body = `{"State":{"Running":true},"Config":{"Image":"cloudflare/cloudflared:2026.10.4"}}`
	got, err = c.Inspect("web")
	if err != nil {
		t.Fatalf("running: unexpected error: %v", err)
	}
	if want := (dockerapi.ContainerState{Exists: true, Running: true, Image: "cloudflare/cloudflared:2026.10.4"}); got != want {
		t.Fatalf("running: got %+v, want %+v", got, want)
	}
}

func TestStopSendsTheConfiguredTimeout(t *testing.T) {
	var gotURI string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	if err := dockerapi.New(socket, 1).Stop("web", 35); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if gotURI != "/v1.43/containers/web/stop?t=35" {
		t.Fatalf("request URI: got %q", gotURI)
	}
}

// Stop treats 304 as success for the same reason Start does: the reconcile
// loop asks the daemon to reach a state, not for permission to act, so a
// container that is already stopped is already correct — not a failure the
// supervisor needs to retry or alert on.
func TestStopTreats304AsSuccess(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	if err := dockerapi.New(socket, 1).Stop("web", 35); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// Stop's call must be bounded by its own timeout (configured stop time + 5s
// headroom), never by the client's constructor default — the default here
// is far too short to survive the handler's sleep, so this only passes if
// Stop is actually computing a per-call deadline rather than inheriting one
// fixed at construction.
func TestStopHasHeadroomBeyondTheClientDefault(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	c := dockerapi.New(socket, 0.05) // default timeout far shorter than the sleep above
	if err := c.Stop("web", 1); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestStopHasHeadroomBeyondTheClientDefault (above) only proves the call's
// deadline isn't the client's constructor default — Stop("web", 1) against a
// 300ms sleep would pass whether the call timeout were 1s or 1+5s. This test
// pins the actual "+5" budget: a client default long enough to be a red
// herring, Stop("web", 0) (the daemon-facing budget is zero), and a handler
// that only responds after ~200ms. That only succeeds if the call's own
// deadline is 0+5=5s; if Stop used timeoutSeconds verbatim the context would
// already be expired (or expire immediately) before the handler ever wrote a
// response.
func TestStopBudgetIncludesFiveSecondsOfCallHeadroom(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	c := dockerapi.New(socket, 5) // long default, irrelevant if Stop computes its own deadline
	if err := c.Stop("web", 0); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestEveryCallHasADeadline(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {} // never answers
	}))
	c := dockerapi.New(socket, 0.2)
	start := time.Now()
	_, err := c.Inspect("web")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Inspect must respect its deadline, took %v", elapsed)
	}
	if err == nil {
		t.Fatal("want an error against a server that never answers")
	}
}

func TestCreateReadsEnvFileAndNeverPutsSecretsInTheSpecStruct(t *testing.T) {
	envPath := filepath.Join(t.TempDir(), "hook.env")
	if err := os.WriteFile(envPath, []byte("DB_PASS=x\n# a comment\n\nOTHER=y\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var body map[string]any
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode create body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))

	// RunSpec has no Env field at all — there is no dockerapi.RunSpec{Env: ...}
	// that would compile, which is the actual guarantee; EnvFile is the only
	// way a secret reaches the wire, and only through this call.
	spec := dockerapi.RunSpec{Name: "conch-hook-web-promote", Image: "img", EnvFile: envPath}
	if err := dockerapi.New(socket, 1).Create(spec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	env, ok := body["Env"].([]any)
	if !ok {
		t.Fatalf("Env: got %+v", body["Env"])
	}
	got := make(map[string]bool, len(env))
	for _, v := range env {
		got[v.(string)] = true
	}
	if !got["DB_PASS=x"] || !got["OTHER=y"] {
		t.Fatalf("Env: got %+v", env)
	}
	if len(env) != 2 {
		t.Fatalf("Env: comment/blank lines must be skipped, got %+v", env)
	}
}

// No AutoRemove field, ever, on the wire: with it set, a failed hook's
// container — and the stderr in its logs — would vanish before conch could
// read it, which is exactly the one thing an operator needs when a hook
// fails (spec §8 rule 3).
func TestCreateNeverSendsAutoRemove(t *testing.T) {
	var raw string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
		w.WriteHeader(http.StatusCreated)
	}))
	spec := dockerapi.RunSpec{Name: "conch-hook-web-promote", Image: "img"}
	if err := dockerapi.New(socket, 1).Create(spec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if strings.Contains(raw, "AutoRemove") {
		t.Fatalf("create body must never mention AutoRemove, got %s", raw)
	}
}

func TestCreateSendsExpectedBodyShape(t *testing.T) {
	var gotURI string
	var body map[string]any
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode create body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	spec := dockerapi.RunSpec{
		Name:    "conch-hook-web-promote",
		Image:   "example/hook:latest",
		Argv:    []string{"restore.sh", "--full"},
		Binds:   []string{"/data:/data"},
		Network: "conch-net",
		Labels:  map[string]string{"conch.hook": "true"},
	}
	if err := dockerapi.New(socket, 1).Create(spec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if gotURI != "/v1.43/containers/create?name=conch-hook-web-promote" {
		t.Fatalf("request URI: got %q", gotURI)
	}
	if body["Image"] != "example/hook:latest" {
		t.Fatalf("Image: got %+v", body["Image"])
	}
	cmd, _ := body["Cmd"].([]any)
	if len(cmd) != 2 || cmd[0] != "restore.sh" || cmd[1] != "--full" {
		t.Fatalf("Cmd: got %+v", body["Cmd"])
	}
	labels, _ := body["Labels"].(map[string]any)
	if labels["conch.hook"] != "true" {
		t.Fatalf("Labels: got %+v", body["Labels"])
	}
	hostConfig, ok := body["HostConfig"].(map[string]any)
	if !ok {
		t.Fatalf("HostConfig: got %+v", body["HostConfig"])
	}
	binds, _ := hostConfig["Binds"].([]any)
	if len(binds) != 1 || binds[0] != "/data:/data" {
		t.Fatalf("HostConfig.Binds: got %+v", hostConfig["Binds"])
	}
	if hostConfig["NetworkMode"] != "conch-net" {
		t.Fatalf("HostConfig.NetworkMode: got %+v", hostConfig["NetworkMode"])
	}
}

func TestWaitReturnsExitCode(t *testing.T) {
	var gotURI string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		io.WriteString(w, `{"StatusCode":137}`)
	}))
	exitCode, err := dockerapi.New(socket, 1).Wait("web", 5)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if exitCode != 137 {
		t.Fatalf("exitCode: got %d", exitCode)
	}
	if gotURI != "/v1.43/containers/web/wait" {
		t.Fatalf("request URI: got %q", gotURI)
	}
}

// Wait must bound its call by its own timeoutSeconds argument, not the
// client's constructor default, exactly like Stop.
func TestWaitUsesItsOwnTimeoutNotTheClientDefault(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		io.WriteString(w, `{"StatusCode":0}`)
	}))
	c := dockerapi.New(socket, 0.05)
	exitCode, err := c.Wait("web", 2)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode: got %d", exitCode)
	}
}

// buildFrame constructs one Docker multiplexed-stream frame: an 8-byte
// header (stream type, 3 reserved bytes, big-endian payload length) then
// the payload itself.
func buildFrame(streamType byte, payload string) []byte {
	header := make([]byte, 8)
	header[0] = streamType
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	return append(header, []byte(payload)...)
}

func TestLogsStripsMultiplexedFrames(t *testing.T) {
	var gotURI string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		w.Write(buildFrame(1, "hello "))
		w.Write(buildFrame(2, "world"))
	}))
	got := dockerapi.New(socket, 1).Logs("web", 50)
	if got != "hello world" {
		t.Fatalf("Logs: got %q", got)
	}
	if gotURI != "/v1.43/containers/web/logs?stdout=1&stderr=1&tail=50" {
		t.Fatalf("request URI: got %q", gotURI)
	}
}

// A non-200 must collapse to "", same as a transport failure, not panic or
// return a mangled decode of an error body as if it were log frames.
func TestLogsReturnsEmptyOnNon200(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"message":"no such container"}`)
	}))
	got := dockerapi.New(socket, 1).Logs("ghost", 50)
	if got != "" {
		t.Fatalf("Logs: got %q, want empty on non-200", got)
	}
}

func TestListByLabelFiltersCorrectly(t *testing.T) {
	var gotFilters string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFilters = r.URL.Query().Get("filters")
		if r.URL.Query().Get("all") != "1" {
			t.Errorf("all: got %q", r.URL.Query().Get("all"))
		}
		io.WriteString(w, `[{"Names":["/conch-hook-web-promote"]},{"Names":["/other","/alias"]}]`)
	}))
	names := dockerapi.New(socket, 1).ListByLabel("conch.hook", "true")

	var decoded map[string][]string
	if err := json.Unmarshal([]byte(gotFilters), &decoded); err != nil {
		t.Fatalf("filters query wasn't valid JSON: %v (%s)", err, gotFilters)
	}
	if len(decoded["label"]) != 1 || decoded["label"][0] != "conch.hook=true" {
		t.Fatalf("filters: got %+v", decoded)
	}
	if len(names) != 2 || names[0] != "conch-hook-web-promote" || names[1] != "other" {
		t.Fatalf("names: got %+v", names)
	}
}

func TestListWithLabelFiltersOnTheKeyAlone(t *testing.T) {
	var gotFilters string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFilters = r.URL.Query().Get("filters")
		if r.URL.Query().Get("all") != "1" {
			t.Errorf("all: got %q", r.URL.Query().Get("all"))
		}
		io.WriteString(w, `[
			{"Names":["/cf-web"],"State":"running","Labels":{"conch.service":"web"}},
			{"Names":["/cf-old"],"State":"exited","Labels":{"conch.service":"old"}},
			{"Names":["/conch-hook-web-restore"],"State":"running",
			 "Labels":{"conch.service":"web","conch.hook":"true"}}
		]`)
	}))
	got := dockerapi.New(socket, 1).ListWithLabel("conch.service")

	// A label-key-only filter: the value half must be absent, or the daemon
	// would answer only for containers whose value is literally "".
	var decoded map[string][]string
	if err := json.Unmarshal([]byte(gotFilters), &decoded); err != nil {
		t.Fatalf("filters query wasn't valid JSON: %v (%s)", err, gotFilters)
	}
	if len(decoded["label"]) != 1 || decoded["label"][0] != "conch.service" {
		t.Fatalf("filters: got %+v, want a bare key with no =value", decoded)
	}

	if len(got) != 3 {
		t.Fatalf("got %d containers, want 3: %+v", len(got), got)
	}
	if got[0].Name != "cf-web" || got[0].Value != "web" || !got[0].Running {
		t.Fatalf("first container: got %+v", got[0])
	}
	if got[1].Name != "cf-old" || got[1].Value != "old" || got[1].Running {
		t.Fatalf("a stopped container is reported, not running: got %+v", got[1])
	}
	// The whole label map travels back, which is the only way a caller can
	// exclude a container for carrying a SECOND label (Docker's filter ANDs;
	// it cannot negate).
	if got[2].Labels["conch.hook"] != "true" {
		t.Fatalf("every label must be carried back: got %+v", got[2])
	}
}

func TestListWithLabelCollapsesFailureToNothing(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	if got := dockerapi.New(socket, 1).ListWithLabel("conch.service"); got != nil {
		t.Fatalf("a daemon that cannot answer must read as nothing labeled: got %+v", got)
	}
}

// resolveHandler serves /containers/json?all=1&filters=... the way the real
// daemon would for a name filter: containers is the full, unfiltered set the
// fake "daemon" holds, and the handler itself applies Docker's own
// SUBSTRING semantics for the name filter (never a real prefix match) so
// these tests can pin ResolveByPrefix's own HasPrefix recheck against
// exactly the false positives the real daemon would hand it.
type fakeContainer struct {
	name    string
	state   string // "running" or anything else
	created int64
}

func resolveHandler(t *testing.T, containers []fakeContainer, gotFilters *string) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotURI := r.URL.Path; gotURI != "/v1.43/containers/json" {
			t.Errorf("unexpected path: %s", gotURI)
		}
		if r.URL.Query().Get("all") != "1" {
			t.Errorf("all: got %q", r.URL.Query().Get("all"))
		}
		filters := r.URL.Query().Get("filters")
		if gotFilters != nil {
			*gotFilters = filters
		}
		var decoded map[string][]string
		if err := json.Unmarshal([]byte(filters), &decoded); err != nil {
			t.Fatalf("filters query wasn't valid JSON: %v (%s)", err, filters)
		}
		name := decoded["name"][0]

		type wireContainer struct {
			Names   []string `json:"Names"`
			State   string   `json:"State"`
			Created int64    `json:"Created"`
		}
		var out []wireContainer
		for _, c := range containers {
			// Docker's own name filter is substring, not prefix — matching that
			// here is what makes the false-positive tests below meaningful.
			if strings.Contains(c.name, name) {
				out = append(out, wireContainer{Names: []string{"/" + c.name}, State: c.state, Created: c.created})
			}
		}
		json.NewEncoder(w).Encode(out)
	})
}

func TestResolveByPrefixNoMatchesReturnsEmptyNotError(t *testing.T) {
	socket := fakeDaemon(t, resolveHandler(t, nil, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("want no error on an absent match, got %v", err)
	}
	if name != "" {
		t.Fatalf("want empty name, got %q", name)
	}
	if running != 0 {
		t.Fatalf("want 0 running matches, got %d", running)
	}
}

// TestResolveByPrefixExcludesSubstringFalsePositives pins the recheck this
// method exists to do: Docker's name filter matches "web-web-" as a
// SUBSTRING anywhere in the name, so "other-web-web-3cb8" is a false
// positive the filter alone would happily return. Only a real prefix match
// may be selected, or counted.
func TestResolveByPrefixExcludesSubstringFalsePositives(t *testing.T) {
	containers := []fakeContainer{
		{name: "other-web-web-3cb8", state: "running", created: 100},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if name != "" {
		t.Fatalf("a substring-only match must be excluded, got %q", name)
	}
	if running != 0 {
		t.Fatalf("a substring-only false positive must not count as a genuine match, got %d", running)
	}
}

func TestResolveByPrefixPrefersRunningOverExited(t *testing.T) {
	containers := []fakeContainer{
		{name: "web-web-aaaa", state: "exited", created: 200}, // newer, but not running
		{name: "web-web-bbbb", state: "running", created: 100},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if name != "web-web-bbbb" {
		t.Fatalf("want the running container preferred over the newer exited one, got %q", name)
	}
	if running != 1 {
		t.Fatalf("the exited previous version must not count as running, want 1, got %d", running)
	}
}

func TestResolveByPrefixPrefersNewestAmongEqualState(t *testing.T) {
	containers := []fakeContainer{
		{name: "web-web-old", state: "running", created: 100},
		{name: "web-web-new", state: "running", created: 200},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if name != "web-web-new" {
		t.Fatalf("want the most recently created among equals, got %q", name)
	}
	if running != 2 {
		t.Fatalf("want 2 running matches, got %d", running)
	}
}

// TestResolveByPrefixNeverRequiresRunning pins the failover-safety property:
// on a standby host every matching container may be stopped, and "prefer
// running" must never become "require running" — that would make the
// service permanently ineligible and StartService could never fire.
func TestResolveByPrefixNeverRequiresRunning(t *testing.T) {
	containers := []fakeContainer{
		{name: "web-web-old", state: "exited", created: 100},
		{name: "web-web-new", state: "exited", created: 200},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if name != "web-web-new" {
		t.Fatalf("want the newest exited container when none are running, got %q", name)
	}
	if running != 0 {
		t.Fatalf("nothing is running here, want 0, got %d", running)
	}
}

// TestResolveByPrefixReportsMultipleRunningMatches pins the blue/green
// overlap window this exists to surface: two real, HasPrefix-passing, LIVE
// candidates both count, even though exactly one is ever selected — the
// caller (supervisor) uses the count to alert, never to change selection.
func TestResolveByPrefixReportsMultipleRunningMatches(t *testing.T) {
	containers := []fakeContainer{
		{name: "web-web-old", state: "running", created: 100}, // Kamal is mid-deploy: both exist
		{name: "web-web-new", state: "running", created: 200},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if running != 2 {
		t.Fatalf("want 2 running matches during the overlap window, got %d", running)
	}
	if name != "web-web-new" {
		t.Fatalf("selection must stay unchanged: want the newest running match, got %q", name)
	}
}

// TestResolveByPrefixIgnoresExitedPreviousVersions is the live-fleet case
// the running count exists for: Kamal leaves every previous version's
// container on the host, exited, so a host several deploys old matches many
// names while exactly one is live. That is the ordinary steady state, not an
// overlap window, and the count the supervisor alerts on must say so.
func TestResolveByPrefixIgnoresExitedPreviousVersions(t *testing.T) {
	containers := []fakeContainer{
		{name: "web-web-v1", state: "exited", created: 100},
		{name: "web-web-v2", state: "exited", created: 200},
		{name: "web-web-v3", state: "exited", created: 300},
		{name: "web-web-v4", state: "exited", created: 400},
		{name: "web-web-v5", state: "exited", created: 500},
		{name: "web-web-v6", state: "running", created: 600},
	}
	socket := fakeDaemon(t, resolveHandler(t, containers, nil))
	name, running, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	if name != "web-web-v6" {
		t.Fatalf("want the one running version selected, got %q", name)
	}
	if running != 1 {
		t.Fatalf("five exited previous versions are not an overlap window, want 1, got %d", running)
	}
}

func TestResolveByPrefixSendsNameFilter(t *testing.T) {
	var gotFilters string
	socket := fakeDaemon(t, resolveHandler(t, nil, &gotFilters))
	if _, _, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-"); err != nil {
		t.Fatalf("ResolveByPrefix: %v", err)
	}
	var decoded map[string][]string
	if err := json.Unmarshal([]byte(gotFilters), &decoded); err != nil {
		t.Fatalf("filters query wasn't valid JSON: %v (%s)", err, gotFilters)
	}
	if len(decoded["name"]) != 1 || decoded["name"][0] != "web-web-" {
		t.Fatalf("filters: got %+v", decoded)
	}
}

// A transport error (or a non-200) is a real failure, unlike an absent
// match: the daemon couldn't answer the question at all, and that must not
// be silently folded into "no such container".
func TestResolveByPrefixTransportFailureIsAnError(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	_, _, err := dockerapi.New(socket, 1).ResolveByPrefix("web-web-")
	if err == nil {
		t.Fatal("want an error on a non-200 status")
	}
}

func TestKillHitsKillEndpoint(t *testing.T) {
	var gotURI, gotMethod string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI, gotMethod = r.RequestURI, r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := dockerapi.New(socket, 1).Kill("web"); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if gotMethod != http.MethodPost || gotURI != "/v1.43/containers/web/kill" {
		t.Fatalf("got %s %s", gotMethod, gotURI)
	}
}

func TestImagePresentTrueAndFalse(t *testing.T) {
	var status int
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	c := dockerapi.New(socket, 1)

	status = http.StatusOK
	if !c.ImagePresent("example/hook:latest") {
		t.Fatal("want true when the daemon has the image")
	}
	status = http.StatusNotFound
	if c.ImagePresent("example/hook:latest") {
		t.Fatal("want false when the daemon doesn't have the image")
	}
}

func TestHealthyPings(t *testing.T) {
	var gotURI string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	if !dockerapi.New(socket, 1).Healthy() {
		t.Fatal("want true against a 200")
	}
	if gotURI != "/v1.43/_ping" {
		t.Fatalf("request URI: got %q", gotURI)
	}
}

// Remove-if-exists (Task 10's job, not this package's) must not fail just
// because there was nothing to remove: a 404 here is success.
func TestRemoveTreats404AsSuccess(t *testing.T) {
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	if err := dockerapi.New(socket, 1).Remove("ghost"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

// TestRemoveAlwaysForces pins force=1 on every Remove call: every caller
// (hook remove-if-exists, KillOrphans) calls Remove right after asking a
// container to stop, not after confirming it has, so a plain DELETE can
// land in the daemon's own 409-while-still-exiting window. force=1 closes
// it and is harmless against an already-exited container.
func TestRemoveAlwaysForces(t *testing.T) {
	var gotURI, gotMethod string
	socket := fakeDaemon(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI, gotMethod = r.RequestURI, r.Method
		w.WriteHeader(http.StatusOK)
	}))
	if err := dockerapi.New(socket, 1).Remove("web"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if gotMethod != http.MethodDelete || gotURI != "/v1.43/containers/web?force=1" {
		t.Fatalf("got %s %s", gotMethod, gotURI)
	}
}
