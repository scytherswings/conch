// Package arbiter is the client of the arbiter's single /batch endpoint
// (spec §7, Appendix A). It owns exactly one hard rule that the rest of the
// system leans on: an HTTP failure of any kind — refused connection, timeout,
// non-200, a body that won't parse, a key the response silently drops — comes
// back as decide.Unreachable for every key the caller asked about, built from
// the request's own op map, never read off the response. Decide (internal
// core) already treats Unreachable as "prove nothing, hold no lease"; this
// package's only job is to never let a network problem read as anything
// stronger than that.
package arbiter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/decide"
)

// Op is one service's request within a batch (Appendix A). Zero-value fields
// are omitted on the wire: Serving/Override/Evacuate/Hold/BarTTL only matter
// to "renew" and "release", and sending them false/0 for "read" would be
// noise the arbiter has to ignore rather than an absence it can rely on.
type Op struct {
	Op       string  `json:"op"`
	Serving  bool    `json:"serving,omitempty"`
	Override bool    `json:"override,omitempty"`
	Evacuate bool    `json:"evacuate,omitempty"`
	Hold     bool    `json:"hold,omitempty"`
	BarTTL   float64 `json:"bar_ttl,omitempty"`
}

// Verdict is one service's answer, normalised out of the arbiter's
// discriminated-union wire shape into one struct where only the fields that
// matter for Lease are meaningful (spec §7's five real states plus the two
// answers that are not the arbiter's opinion at all: Unreachable and
// Conflict — see decide.LeaseState).
type Verdict struct {
	Lease       decide.LeaseState
	TTL         float64 // granted, when yours
	Holder      string
	Serving     bool
	Epoch       int
	EvacuatedBy string
	Released    bool
	Err         string // set when the arbiter itself returned state:"error" (Lease is Conflict for duplicate_host, Unreachable otherwise); never set on a transport failure
}

// Reply is one batch round-trip. SentAt is the anchor spec §6 measures every
// lease deadline against: it is the clock reading at the moment the request
// left, not when the response arrived, so response latency can only shorten
// a believed lease, never extend it.
type Reply struct {
	Services     map[string]Verdict
	Peers        map[string]float64 // host -> last_seen seconds
	Fingerprints map[string]string
	SentAt       float64
}

// FleetReply is the whole fleet as GET /fleet renders it: every service the
// arbiter knows of, and every host still inside its presence window. It is a
// pure read — fetching it claims no lease and records no presence — so unlike
// a batch it speaks for no host, and can be asked from anywhere that holds the
// fleet secret (D5).
type FleetReply struct {
	GeneratedAt int64                   `json:"generatedAt"`
	Services    map[string]FleetService `json:"services"`
	Hosts       map[string]FleetHost    `json:"hosts"`
}

// FleetService is one key's inventory row. A nil Holder is a free lease, not
// an empty host name: the arbiter renders a never-claimed key and a lapsed one
// the same way, and this client must not invent a holder for either. A nil
// ExpiresIn is "no deadline to report" (a free lease, or a lease whose expiry
// the arbiter chose not to state), never a deadline of zero.
type FleetService struct {
	Holder      *string  `json:"holder"`
	Serving     bool     `json:"serving"`
	Epoch       int      `json:"epoch"`
	ExpiresIn   *float64 `json:"expiresIn"`
	EvacuatedBy *string  `json:"evacuatedBy"`
	Barred      bool     `json:"barred"`
	Conflict    *string  `json:"conflict"`
}

// FleetHost is one host's inventory row: how long since it last checked in,
// the image fingerprint it last reported, and the valid service keys it last
// sent. Services is the host's stated intent — what it says it supervises —
// and is not a claim about who holds a lease.
type FleetHost struct {
	LastSeenAgo float64  `json:"lastSeenAgo"`
	Fingerprint *string  `json:"fingerprint"`
	Services    []string `json:"services"`
}

// ErrNoFleet is the arbiter's 404 for a route it does not serve: the fleet
// endpoint is newer than this deployment, or the backend never offers it
// (D8). It is a distinct error so a caller can say "upgrade the arbiter"
// rather than report an indistinguishable transport failure.
var ErrNoFleet = errors.New("the arbiter does not expose /fleet; upgrade it first")

// Client is one host's connection to the arbiter: one URL, one auth secret,
// one identity (host, instance) it stamps on every request, and one
// http.Client whose Timeout is the sole deadline enforcement — no per-call
// context needed because every call shares the same budget.
type Client struct {
	url      string
	secret   string
	host     string
	instance string
	http     *http.Client
	clk      clock.Clock
}

// New builds a Client. timeout is seconds, converted once into the
// underlying http.Client's Timeout, which covers dial, TLS, request write
// and response read as a single budget — enough on its own to bound
// TestDeadlineAgainstAServerThatNeverAnswers without a context.
//
// url is normalised into the arbiter's one real endpoint here, once, rather
// than trusted verbatim: spec §11 documents the config value as the BASE url
// (e.g. "https://conch-lease.example.workers.dev"), and the worker serves
// only /batch (and /health), 404ing everything else. Posting the bare base
// url as configured therefore reads as permanently unreachable — that was a
// production bug. New is forgiving of both config styles: a bare base url
// (trailing slash or not) gets "/batch" appended, and a url that already
// ends in "/batch" is left alone, so an operator who copies the full
// endpoint into config does not end up with "/batch/batch".
func New(url, secret, host, instance string, timeout float64, clk clock.Clock) *Client {
	return &Client{
		url:      batchURL(url),
		secret:   secret,
		host:     host,
		instance: instance,
		http:     &http.Client{Timeout: time.Duration(timeout * float64(time.Second))},
		clk:      clk,
	}
}

// batchURL normalises a configured arbiter url into the one path the worker
// actually serves batches on. See New's doc comment for why.
func batchURL(url string) string {
	trimmed := strings.TrimRight(url, "/")
	if strings.HasSuffix(trimmed, "/batch") {
		return trimmed
	}
	return trimmed + "/batch"
}

// fleetURL normalises an arbiter url into the path GET /fleet lives on. New
// stores its client's url already ending in "/batch", so a Client built by New
// and then asked for Fleet would GET the batch path — a write path, and a 405
// to a GET — which this client would misreport as an arbiter too old. Stripping
// it is the whole reason this derivation exists. A url that already ends in
// "/fleet" is left alone; anything else gets "/fleet" appended, matching
// batchURL's tolerance of both config styles.
func fleetURL(url string) string {
	trimmed := strings.TrimRight(url, "/")
	switch {
	case strings.HasSuffix(trimmed, "/batch"):
		return strings.TrimSuffix(trimmed, "/batch") + "/fleet"
	case strings.HasSuffix(trimmed, "/fleet"):
		return trimmed
	default:
		return trimmed + "/fleet"
	}
}

// NewFleet builds a read-only Client for GET /fleet. It carries no host and no
// instance: a fleet read speaks for no host and claims nothing, so there is no
// identity to stamp on it. url is normalised once, here, exactly as New
// normalises a base url; timeout is seconds, bounded by the same http.Client
// Timeout a batch uses.
func NewFleet(url, secret string, timeout float64, clk clock.Clock) *Client {
	return &Client{
		url:    fleetURL(url),
		secret: secret,
		http:   &http.Client{Timeout: time.Duration(timeout * float64(time.Second))},
		clk:    clk,
	}
}

// Fleet performs one GET /fleet and decodes the inventory. Unlike Batch, which
// answers per requested key and so can fail closed key by key, a fleet read
// answers or it does not: any transport failure, a non-200, or a body that
// will not parse is an error, and no partial list is ever fabricated from a
// response the arbiter did not actually send. A 404 is ErrNoFleet, the one
// failure worth its own name.
func (c *Client) Fleet(ctx context.Context) (FleetReply, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fleetURL(c.url), nil)
	if err != nil {
		return FleetReply{}, err
	}
	req.Header.Set("X-Conch-Auth", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return FleetReply{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return FleetReply{}, ErrNoFleet
	}
	if resp.StatusCode != http.StatusOK {
		return FleetReply{}, fmt.Errorf("the arbiter answered %s to a fleet request", resp.Status)
	}

	var reply FleetReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return FleetReply{}, fmt.Errorf("the arbiter's fleet reply did not parse: %w", err)
	}
	return reply, nil
}

// requestBody is Appendix A's request shape.
type requestBody struct {
	Host        string        `json:"host"`
	Instance    string        `json:"instance"`
	TTL         float64       `json:"ttl"`
	Fingerprint string        `json:"fingerprint"`
	Services    map[string]Op `json:"services"`
}

// wireVerdict is Appendix A's response verdict shape: one struct wide enough
// for all five states plus error, decoded once and then narrowed by state in
// toVerdict. Every field is optional on the wire, hence omitempty going out
// were this ever re-encoded, and no non-zero-value requirement coming in.
type wireVerdict struct {
	State       string  `json:"state"`
	TTL         float64 `json:"ttl"`
	Holder      string  `json:"holder"`
	Serving     bool    `json:"serving"`
	Epoch       int     `json:"epoch"`
	EvacuatedBy string  `json:"evacuated_by"`
	Released    bool    `json:"released"`
	Error       string  `json:"error"`
}

type wirePeer struct {
	LastSeen float64 `json:"last_seen"`
}

type wireResponse struct {
	Services     map[string]wireVerdict `json:"services"`
	Peers        map[string]wirePeer    `json:"peers"`
	Fingerprints map[string]string      `json:"fingerprints"`
}

// Batch sends one request covering every key in ops and returns one Verdict
// per key in ops — never more, never fewer. Any failure between building the
// request and decoding a well-formed response (transport error, non-200,
// unparseable body, or a response that simply omits a requested key) falls
// back to allUnreachable, which is built by ranging over ops, never over
// whatever the response did or didn't contain.
func (c *Client) Batch(ttl float64, fingerprint string, ops map[string]Op) Reply {
	payload, err := json.Marshal(requestBody{
		Host:        c.host,
		Instance:    c.instance,
		TTL:         ttl,
		Fingerprint: fingerprint,
		Services:    ops,
	})
	if err != nil {
		return Reply{Services: allUnreachable(ops), SentAt: c.clk.Mono()}
	}

	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return Reply{Services: allUnreachable(ops), SentAt: c.clk.Mono()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Conch-Auth", c.secret)

	// Captured on the line before the request is written (spec §5
	// anchoring): everything after this point is latency the deadline must
	// not be charged for.
	sentAt := c.clk.Mono()
	resp, err := c.http.Do(req)
	if err != nil {
		return Reply{Services: allUnreachable(ops), SentAt: sentAt}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Reply{Services: allUnreachable(ops), SentAt: sentAt}
	}

	var wire wireResponse
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return Reply{Services: allUnreachable(ops), SentAt: sentAt}
	}

	services := make(map[string]Verdict, len(ops))
	for key := range ops {
		wv, present := wire.Services[key]
		if !present {
			services[key] = Verdict{Lease: decide.Unreachable}
			continue
		}
		services[key] = toVerdict(wv)
	}

	peers := make(map[string]float64, len(wire.Peers))
	for host, p := range wire.Peers {
		peers[host] = p.LastSeen
	}

	return Reply{
		Services:     services,
		Peers:        peers,
		Fingerprints: wire.Fingerprints,
		SentAt:       sentAt,
	}
}

// allUnreachable builds a reply covering every requested key, and only ever
// the requested keys: the shape a transport failure must produce, and the
// shape a decode failure must fall back to, since the response cannot be
// trusted to say which keys it meant to answer.
func allUnreachable(ops map[string]Op) map[string]Verdict {
	out := make(map[string]Verdict, len(ops))
	for key := range ops {
		out[key] = Verdict{Lease: decide.Unreachable}
	}
	return out
}

// toVerdict narrows one wire verdict by its state tag. An error state maps
// to Conflict only for duplicate_host — the one error that means another
// live process is answering to this host name. Every other error
// (invalid_op, not_holder) is our own bug, not a peer's claim, so it maps to
// Unreachable like any other reading we cannot act on, with Err preserved
// for logging. An unrecognised state string (a protocol change) gets the
// same fail-closed treatment for the same reason.
func toVerdict(wv wireVerdict) Verdict {
	switch wv.State {
	case "yours":
		return Verdict{Lease: decide.Yours, TTL: wv.TTL, Epoch: wv.Epoch}
	case "held":
		return Verdict{Lease: decide.Held, Holder: wv.Holder, Serving: wv.Serving, Epoch: wv.Epoch}
	case "free":
		return Verdict{Lease: decide.Free, Epoch: wv.Epoch, EvacuatedBy: wv.EvacuatedBy, Released: wv.Released}
	case "barred":
		return Verdict{Lease: decide.Barred, EvacuatedBy: wv.EvacuatedBy, Epoch: wv.Epoch}
	case "error":
		if wv.Error == "duplicate_host" {
			return Verdict{Lease: decide.Conflict, Err: wv.Error}
		}
		return Verdict{Lease: decide.Unreachable, Err: wv.Error}
	default:
		return Verdict{Lease: decide.Unreachable, Err: wv.Error}
	}
}
