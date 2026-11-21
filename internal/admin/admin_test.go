package admin

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/supervisor"
)

// gateTimeout bounds every wait in this file. Nothing here should take real
// time, so this exists only so a mutation that stops an event happening fails
// loudly instead of hanging until the package timeout.
const gateTimeout = 5 * time.Second

// stub is the supervisor as this package sees it: three verbs. The real
// Supervisor is checked against the same interface at compile time in
// admin.go (`var _ Backend = (*supervisor.Supervisor)(nil)`), which is the
// guarantee that matters — its own fakes are unexported in package supervisor,
// and importing them here would be an import cycle.
type stub struct {
	mu         sync.Mutex
	status     map[string]any
	fleetReply *arbiter.FleetReply
	events     chan supervisor.EvacEvent
	evacErr    error
	reclaimErr error
	evacArgs   []any
	reclaimed  string
}

func newStub() *stub {
	return &stub{
		status: map[string]any{
			"web":   map[string]any{"phase": "ACTIVE", "lease": "yours"},
			"blog":  map[string]any{"phase": "STANDBY", "lease": "held"},
			"peers": map[string]float64{"host-b": 2.5},
		},
		events: make(chan supervisor.EvacEvent, 8),
	}
}

func (s *stub) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *stub) Fleet(ctx context.Context) supervisor.FleetView {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := supervisor.FleetView{
		Host:           "host-a",
		SupervisedHere: []string{"web", "blog"},
		Peers:          map[string]float64{"host-b": 2.5},
	}
	if s.fleetReply != nil {
		view.Arbiter = s.fleetReply
	}
	return view
}

func (s *stub) Evacuate(services []string, all, hold, force bool) (<-chan supervisor.EvacEvent, error) {
	s.mu.Lock()
	s.evacArgs = []any{services, all, hold, force}
	err := s.evacErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.events, nil
}

func (s *stub) Reclaim(service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimed = service
	return s.reclaimErr
}

func (s *stub) args() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evacArgs
}

// serve starts the admin surface on a tempdir socket and returns the client.
// Serve's listener is created inside the goroutine, so the socket is waited
// for rather than assumed.
func serve(t *testing.T, b Backend) (CLI, string) {
	t.Helper()
	socket := tempSocket(t)
	return serveAt(t, b, socket), socket
}

// tempSocket is t.TempDir with a length limit. A unix socket path lives in
// sun_path, which is 104 bytes on darwin and 108 on linux, and t.TempDir's
// path (on darwin, /var/folders/../T/<TestName><digits>/001) blows past it for
// any test with a descriptive name — bind(2) then fails with "invalid
// argument", which reads like anything but "the path is too long".
func tempSocket(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if len(dir) > 60 {
		short, err := os.MkdirTemp("/tmp", "conch")
		if err != nil {
			t.Fatalf("mkdtemp: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(short) })
		dir = short
	}
	return filepath.Join(dir, "s.sock")
}

func serveAt(t *testing.T, b Backend, socket string) CLI {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, socket, b) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(gateTimeout):
			t.Error("Serve never returned after its context was cancelled")
		}
	})
	awaitSocket(t, socket)
	return CLI{Socket: socket}
}

func awaitSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(gateTimeout)
	for time.Now().Before(deadline) {
		// A SOCKET, not merely a path: the stale-socket test starts with an
		// ordinary file sitting on this path, and dialling that gets "socket
		// operation on non-socket" rather than a connection.
		if info, err := os.Stat(socket); err == nil && info.Mode()&fs.ModeSocket != 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the socket %s never appeared", socket)
}

func TestStatusReportsEveryService(t *testing.T) {
	cli, _ := serve(t, newStub())
	out, err := cli.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	for _, want := range []string{"web", "blog", "ACTIVE", "yours"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the status snapshot is missing %q: %s", want, out)
		}
	}
	// Peer presence rides the same snapshot, and this half is what an
	// operator reads when the evacuation pre-flight refuses (spec §9).
	for _, want := range []string{"peers", "host-b", "2.5"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the status snapshot is missing peer presence %q: %s", want, out)
		}
	}
}

// TestFleetReportsTheLocalViewOverTheSocket pins the local read: this host's
// services and peers plus the arbiter's snapshot the supervisor merged in,
// all the way out through the client. The shape is the supervisor's FleetView,
// not the arbiter's bare reply — local mode exists to show both sides.
func TestFleetReportsTheLocalViewOverTheSocket(t *testing.T) {
	b := newStub()
	holder := "host-b"
	b.fleetReply = &arbiter.FleetReply{
		GeneratedAt: 42,
		Services:    map[string]arbiter.FleetService{"web": {Holder: &holder, Serving: true, Epoch: 1}},
		Hosts:       map[string]arbiter.FleetHost{},
	}
	cli, _ := serve(t, b)

	view, err := cli.Fleet()
	if err != nil {
		t.Fatalf("Fleet: %v", err)
	}
	if view.Host != "host-a" {
		t.Fatalf("host: got %q", view.Host)
	}
	if len(view.SupervisedHere) != 2 || view.SupervisedHere[0] != "web" {
		t.Fatalf("supervised_here: got %v", view.SupervisedHere)
	}
	if view.Peers["host-b"] != 2.5 {
		t.Fatalf("peers: got %v", view.Peers)
	}
	if view.Arbiter == nil || view.Arbiter.GeneratedAt != 42 {
		t.Fatalf("the arbiter snapshot did not survive: %+v", view.Arbiter)
	}
}

func TestEvacuateStreamsProgressThenTheVerdict(t *testing.T) {
	b := newStub()
	cli, _ := serve(t, b)

	go func() {
		b.events <- supervisor.EvacEvent{Service: "web", Message: "running the evacuate hooks"}
		b.events <- supervisor.EvacEvent{Service: "web", Message: "handed over to host-b"}
		b.events <- supervisor.EvacEvent{Done: true, OK: true, Message: "every named service was handed over"}
		close(b.events)
	}()

	ok, transcript, err := cli.Evacuate([]string{"web"}, false, true, false)
	if err != nil {
		t.Fatalf("Evacuate: %v", err)
	}
	if !ok {
		t.Fatalf("the final line said ok: %s", transcript)
	}
	for _, want := range []string{"running the evacuate hooks", "handed over to host-b"} {
		if !strings.Contains(transcript, want) {
			t.Fatalf("the transcript dropped %q: %s", want, transcript)
		}
	}
	args := b.args()
	if len(args) != 4 || args[1] != false || args[2] != true || args[3] != false {
		t.Fatalf("the flags did not survive the round trip: %v", args)
	}
}

func TestARefusalIsAVerdictNotATransportError(t *testing.T) {
	b := newStub()
	b.evacErr = errors.New("no peer has checked in: pass --force if you really do want this host down")
	cli, _ := serve(t, b)

	ok, transcript, err := cli.Evacuate(nil, true, false, false)
	if err != nil {
		t.Fatalf("a refusal is an answer, not a transport failure: %v", err)
	}
	if ok {
		t.Fatal("a refused evacuation is not a successful one")
	}
	if !strings.Contains(transcript, "--force") {
		t.Fatalf("the refusal must reach the operator intact: %s", transcript)
	}
}

func TestAStreamThatEndsWithoutAVerdictIsAnError(t *testing.T) {
	b := newStub()
	cli, _ := serve(t, b)
	go func() {
		b.events <- supervisor.EvacEvent{Service: "web", Message: "running the evacuate hooks"}
		close(b.events) // the supervisor went away mid-sequence
	}()
	ok, _, err := cli.Evacuate([]string{"web"}, false, false, false)
	if err == nil {
		t.Fatal("no verdict is not the same as a verdict of false: it must be an error")
	}
	if ok {
		t.Fatal("and it is certainly not a success")
	}
}

func TestReclaimSurfacesTheSupervisorsRefusal(t *testing.T) {
	b := newStub()
	cli, _ := serve(t, b)
	if err := cli.Reclaim("web"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	b.mu.Lock()
	got := b.reclaimed
	b.reclaimErr = errors.New("an evacuation is in flight on this host")
	b.mu.Unlock()
	if got != "web" {
		t.Fatalf("the service name did not survive the round trip, got %q", got)
	}

	err := cli.Reclaim("web")
	if err == nil || !strings.Contains(err.Error(), "evacuation is in flight") {
		t.Fatalf("a refusal must reach the operator, got %v", err)
	}
}

func TestTheSocketIsPrivate(t *testing.T) {
	_, socket := serve(t, newStub())
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("anyone who can open this socket can evacuate the host: mode is %v", perm)
	}
}

// TestServeCreatesTheSocketsParentDirectory pins the fix for a real startup
// failure: the shipped image is scratch-based with no /run at all, so
// binding /run/conch/admin.sock with nothing having created /run/conch first
// fails ENOENT and conch never comes up. Serve must create that directory
// itself rather than assume the image or an operator's volume mount already
// did.
func TestServeCreatesTheSocketsParentDirectory(t *testing.T) {
	dir := t.TempDir()
	if len(dir) > 60 {
		short, err := os.MkdirTemp("/tmp", "conch")
		if err != nil {
			t.Fatalf("mkdtemp: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(short) })
		dir = short
	}
	// "run/conch" does not exist yet — nothing under dir has created it.
	socket := filepath.Join(dir, "run", "conch", "admin.sock")
	cli := serveAt(t, newStub(), socket)
	if _, err := cli.Status(); err != nil {
		t.Fatalf("a missing parent directory must not stop conch from serving: %v", err)
	}
}

func TestServeReplacesAStaleSocket(t *testing.T) {
	socket := tempSocket(t)
	// A conch that was killed rather than stopped leaves the socket file
	// behind, and bind(2) fails on a path that already exists.
	if err := os.WriteFile(socket, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cli := serveAt(t, newStub(), socket)
	if _, err := cli.Status(); err != nil {
		t.Fatalf("a stale socket must not stop the next conch from serving: %v", err)
	}
}

// TestServeRefusesAWorldWritableSocketDirectory: the socket is chmodded 0600
// only after bind, so a directory another local user can write to is a window
// in which they can race conch to the socket path. Serve must refuse to serve
// on one rather than open the window.
func TestServeRefusesAWorldWritableSocketDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	err := Serve(context.Background(), filepath.Join(dir, "s.sock"), newStub())
	if err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("a group/other-writable socket directory must be refused, got %v", err)
	}
}

func TestServeStopsWithItsContextAndTakesTheSocketWithIt(t *testing.T) {
	b := newStub()
	socket := tempSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, socket, b) }()
	awaitSocket(t, socket)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a cancelled context is an ordinary shutdown: %v", err)
		}
	case <-time.After(gateTimeout):
		t.Fatal("Serve outlived its context")
	}
	if _, err := os.Stat(socket); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the socket should be gone, got %v", err)
	}
}

// TestAWatcherThatWalksAwayDoesNotWedgeTheSurface is spec §9's whole reason
// for putting the sequence in the supervisor: a dropped SSH session must not
// leave a host half-evacuated. Here it must also not leave a handler blocked
// on a channel nobody will ever drain.
func TestAWatcherThatWalksAwayDoesNotWedgeTheSurface(t *testing.T) {
	b := newStub()
	cli, _ := serve(t, b)

	ctx, cancel := context.WithCancel(context.Background())
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		_, _, _ = cli.evacuate(ctx, []string{"web"}, false, false, false)
	}()
	b.events <- supervisor.EvacEvent{Service: "web", Message: "running the evacuate hooks"}
	cancel()
	select {
	case <-watching:
	case <-time.After(gateTimeout):
		t.Fatal("the watcher never returned")
	}

	// The surface is still serving, and the evacuation is still the
	// supervisor's: nothing here cancelled it.
	if _, err := cli.Status(); err != nil {
		t.Fatalf("a dropped watcher must not wedge the socket: %v", err)
	}
}
