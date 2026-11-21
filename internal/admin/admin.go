// Package admin is the running supervisor's control surface: HTTP over a unix
// socket inside the container (spec §10). Both halves live here — the server
// the supervisor runs, and the client the CLI subcommands are.
//
// Three properties are the whole design.
//
//   - THE SOCKET IS THE BOUNDARY. Nothing is installed on hosts; the transport
//     from outside is `docker exec` / `kamal accessory exec --reuse` into the
//     container that is already running conch. A fresh container has no socket
//     and no supervisor, which is exactly the failure an operator should get
//     rather than a second, lease-claiming conch.
//
//   - THE VERBS ONLY ASK. Every sequence they start belongs to the supervisor
//     (spec §9): this package never stops a container, never sends an arbiter
//     op, and never decides anything. A dropped SSH session therefore cannot
//     leave a host half-evacuated — the handler goes away, the evacuation does
//     not.
//
//   - A REFUSAL IS AN ANSWER. "No peer has checked in" and "an evacuation is
//     already in flight" are verdicts, not transport failures, and they travel
//     as the stream's final line (or the body's `detail`) rather than as an
//     HTTP error. The client's `error` return is reserved for the cases where
//     the operator genuinely learned nothing: no socket, no response, a
//     truncated stream.
package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/scytherswings/conch/internal/supervisor"
)

// socketMode is 0600 because anyone who can open this socket can evacuate the
// host.
const socketMode fs.FileMode = 0o600

// maxRequestBody bounds a request an operator can only send from inside the
// container: small, and finite.
const maxRequestBody = 64 << 10

// Backend is the running supervisor, as this package needs it.
//
// It is an interface rather than *supervisor.Supervisor (which the brief's
// sketch named) for one reason: package supervisor's fakes — the scripted
// arbiter, the recording docker, the fake clock — are unexported test
// fixtures, so building a real supervisor here would mean either exporting
// them or writing a second set. The compile-time assertion below is the
// guarantee that sketch was actually asking for, and it cannot drift.
type Backend interface {
	Status() map[string]any
	Evacuate(services []string, all, hold, force bool) (<-chan supervisor.EvacEvent, error)
	Reclaim(service string) error
}

var _ Backend = (*supervisor.Supervisor)(nil)

// Serve runs the admin surface on socketPath until ctx is cancelled, and takes
// the socket file with it when it goes.
//
// The socket's parent directory is created first: the shipped image is
// scratch-based with no /run at all, and bind(2) on a path whose directory
// doesn't exist fails with ENOENT — which, unhandled, would mean conch dies
// at startup on every fresh container, before it ever reaches the arbiter or
// the Docker socket. 0700 because anyone who can write into this directory
// can plant something on the socket path before conch gets there.
//
// A stale socket left by a conch that was killed rather than stopped is
// unlinked next: bind(2) fails on a path that already exists, so without this
// a hard kill would cost the next conch its entire admin surface.
func Serve(ctx context.Context, socketPath string, b Backend) error {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return fmt.Errorf("admin: creating the directory for %s: %w", socketPath, err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("admin: removing the stale socket %s: %w", socketPath, err)
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("admin: listening on %s: %w", socketPath, err)
	}
	defer os.Remove(socketPath)

	if err := os.Chmod(socketPath, socketMode); err != nil {
		ln.Close()
		return fmt.Errorf("admin: securing %s: %w", socketPath, err)
	}

	srv := &http.Server{Handler: routes(b)}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
		}
		// Close, not Shutdown: an in-flight /evacuate watcher may legitimately
		// be streaming for minutes, and the sequence it is watching belongs to
		// the supervisor either way. Waiting for it would mean a conch that
		// cannot exit because somebody left a terminal open.
		srv.Close()
	}()

	err = srv.Serve(ln)
	close(stopped)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func routes(b Backend) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Status())
	})

	mux.HandleFunc("POST /evacuate", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Services []string `json:"services"`
			All      bool     `json:"all"`
			Hold     bool     `json:"hold"`
			Force    bool     `json:"force"`
		}
		if err := decode(w, r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "detail": err.Error()})
			return
		}

		// 202 goes out before the verb is even attempted: from here on every
		// answer, refusals included, is a line in the stream. One path for the
		// client to parse, and no header written after the first line.
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusAccepted)
		rc := http.NewResponseController(w)
		rc.Flush()
		enc := json.NewEncoder(w)

		events, err := b.Evacuate(req.Services, req.All, req.Hold, req.Force)
		if err != nil {
			_ = enc.Encode(supervisor.EvacEvent{Done: true, OK: false, Message: err.Error()})
			rc.Flush()
			return
		}
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					return
				}
				if enc.Encode(ev) != nil {
					return // the watcher hung up mid-line
				}
				rc.Flush()
				if ev.Done {
					return
				}
			case <-r.Context().Done():
				// The watcher walked away. The supervisor owns the sequence and
				// carries on without it (spec §9); this handler must not sit on
				// a channel nobody will drain.
				return
			}
		}
	})

	mux.HandleFunc("POST /reclaim", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Service string `json:"service"`
		}
		if err := decode(w, r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "detail": err.Error()})
			return
		}
		if err := b.Reclaim(req.Service); err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	return mux
}

func decode(w http.ResponseWriter, r *http.Request, into any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	dec.DisallowUnknownFields() // the same loudness config decoding gets (spec §11)
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("malformed request: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// ---------------------------------------------------------------------------
// The client half: what `conch status`, `conch evacuate` and `conch reclaim`
// are underneath (Task 14 wires the flags).
// ---------------------------------------------------------------------------

// CLI talks to one running supervisor's socket.
type CLI struct{ Socket string }

// client has no Timeout, deliberately: an evacuation streams for as long as
// the sequence takes, and the supervisor's own 120s budget is the deadline
// that matters. A dial failure against a missing socket is immediate anyway,
// which is the case an operator actually hits (a fresh container, no
// supervisor).
func (c CLI) client() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", c.Socket)
			},
		},
	}
}

// notAnswering is the one failure every verb shares, and the one an operator
// actually hits: there is no supervisor behind this socket. Naming both ways
// out — a conch that is not running, and a --socket pointed somewhere else —
// is the difference between a fixable message and a puzzling one, since the
// wrapped dial error only ever says the file is not there.
func notAnswering(socket string, err error) error {
	return fmt.Errorf(
		"conch is not answering on %s: check that conch run is live in this container, or point --socket at it: %w",
		socket, err)
}

// Status returns the supervisor's snapshot, indented for a human at 3am.
func (c CLI) Status() (string, error) {
	resp, err := c.client().Get("http://conch/status")
	if err != nil {
		return "", notAnswering(c.Socket, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("the supervisor answered %s to a status request: %s",
			resp.Status, strings.TrimSpace(string(raw)))
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return string(raw), nil // valid or not, the operator gets what conch said
	}
	return pretty.String(), nil
}

// Evacuate requests an evacuation and follows it to its verdict. ok is the
// supervisor's answer; transcript is every progress line, in order, ready to
// print; err means the operator learned nothing — no socket, no response, or a
// stream that ended without a verdict.
func (c CLI) Evacuate(services []string, all, hold, force bool) (bool, string, error) {
	return c.evacuate(context.Background(), services, all, hold, force)
}

func (c CLI) evacuate(ctx context.Context, services []string, all, hold, force bool) (bool, string, error) {
	body, err := json.Marshal(map[string]any{
		"services": services, "all": all, "hold": hold, "force": force,
	})
	if err != nil {
		return false, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://conch/evacuate", bytes.NewReader(body))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client().Do(req)
	if err != nil {
		return false, "", notAnswering(c.Socket, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return false, "", fmt.Errorf("the supervisor answered %s to an evacuate request: %s",
			resp.Status, strings.TrimSpace(string(raw)))
	}

	var transcript strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev supervisor.EvacEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return false, transcript.String(), fmt.Errorf("evacuate: unreadable progress line %q: %w", line, err)
		}
		if ev.Service != "" {
			transcript.WriteString(ev.Service + ": ")
		}
		transcript.WriteString(ev.Message + "\n")
		if ev.Done {
			return ev.OK, transcript.String(), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, transcript.String(), fmt.Errorf(
			"the evacuation's progress stream broke, and it is still running without a watcher: check conch status: %w", err)
	}
	// No verdict is not a verdict of false: the sequence may well be running
	// still, and only the supervisor knows. Say so.
	return false, transcript.String(), errors.New(
		"evacuate: the stream ended without a verdict — check `conch status` for what actually happened")
}

// Reclaim clears this host's own evacuation bar for one service. The
// supervisor's refusals come back as errors: there is nothing else for this
// verb to report.
func (c CLI) Reclaim(service string) error {
	body, _ := json.Marshal(map[string]any{"service": service})
	resp, err := c.client().Post("http://conch/reclaim", "application/json", bytes.NewReader(body))
	if err != nil {
		return notAnswering(c.Socket, err)
	}
	defer resp.Body.Close()

	var out struct {
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return fmt.Errorf("reclaim: %s: unreadable answer: %w", resp.Status, err)
	}
	if !out.OK {
		if out.Detail == "" {
			out.Detail = resp.Status
		}
		return errors.New(out.Detail)
	}
	return nil
}
