package arbiter_test

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scytherswings/conch/internal/arbiter"
	"github.com/scytherswings/conch/internal/clock"
	"github.com/scytherswings/conch/internal/decide"
)

func TestTransportFailureMarksEveryRequestedKeyUnreachable(t *testing.T) {
	c := arbiter.New("http://192.0.2.1:1", "s", "host-a", "i1", 0.05, clock.NewFake())
	r := c.Batch(60, "fp", map[string]arbiter.Op{
		"web": {Op: "renew"}, "blog": {Op: "read"}, "store": {Op: "read"},
	})
	if len(r.Services) != 3 {
		t.Fatalf("want all 3 requested keys, got %d", len(r.Services))
	}
	for k, v := range r.Services {
		if v.Lease != decide.Unreachable {
			t.Fatalf("%s: want unreachable, got %s", k, v.Lease)
		}
	}
}

func TestA401IsUnreachableNotHeld(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}})
	if r.Services["web"].Lease != decide.Unreachable {
		t.Fatal("a rotated secret must never read as another host holding the lease")
	}
}

func TestVerdictMapping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"services":{
          "web":{"state":"yours","ttl":60,"epoch":3},
          "blog":{"state":"held","holder":"host-b","serving":true,"epoch":7},
          "store":{"state":"barred","evacuated_by":"host-a","epoch":2},
          "shop":{"state":"error","error":"duplicate_host"},
          "shed":{"state":"yours","epoch":4}},
          "peers":{"host-b":{"last_seen":2.1}},"fingerprints":{"host-b":"abc"}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{
			"web": {Op: "renew"}, "blog": {Op: "read"},
			"store": {Op: "renew"}, "shop": {Op: "renew"}, "shed": {Op: "read"}})
	if r.Services["web"].Lease != decide.Yours || r.Services["web"].TTL != 60 {
		t.Fatalf("web: got %+v", r.Services["web"])
	}
	if r.Services["blog"].Lease != decide.Held || !r.Services["blog"].Serving {
		t.Fatalf("blog: got %+v", r.Services["blog"])
	}
	if r.Services["store"].Lease != decide.Barred || r.Services["store"].EvacuatedBy != "host-a" {
		t.Fatalf("store: got %+v", r.Services["store"])
	}
	if r.Services["shop"].Lease != decide.Conflict {
		t.Fatalf("shop: got %+v", r.Services["shop"])
	}
	// A "read" on a lease this host already holds comes back as
	// lease.js's view() shape: yours, but no ttl at all (not ttl:0 — the
	// field is absent). decide.Decide only arms the expiry when
	// LeaseTTL > 0 (Task 7), so this ttl-less Yours must pass through as
	// TTL 0, never fabricate one.
	if r.Services["shed"].Lease != decide.Yours || r.Services["shed"].TTL != 0 {
		t.Fatalf("shed: got %+v", r.Services["shed"])
	}
	if r.Peers["host-b"] != 2.1 {
		t.Fatalf("peers: got %+v", r.Peers)
	}
	if r.Fingerprints["host-b"] != "abc" {
		t.Fatalf("fingerprints: got %+v", r.Fingerprints)
	}
}

// The config carries the arbiter's BASE url (spec §11:
// "url: https://conch-lease.example.workers.dev"); the worker serves only
// /batch (and /health) and 404s everything else. A client that posted to the
// bare base URL verbatim would read every live deployment as permanently
// unreachable — exactly the production bug this pins.
func TestBatchPostsToTheBatchPathNotTheBareBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/batch" {
			w.WriteHeader(404)
			return
		}
		io.WriteString(w, `{"services":{"web":{"state":"yours","ttl":60,"epoch":1}},"peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}})
	if r.Services["web"].Lease != decide.Yours {
		t.Fatalf("a bare base url must reach /batch, got %+v", r.Services["web"])
	}
}

// A config url with a trailing slash and one that already ends in /batch
// (both real-world styles, per the doc comment on New) must both land on
// /batch, never /batch/batch and never a stray double slash.
func TestBatchURLNormalizationAcceptsBothConfigStyles(t *testing.T) {
	var gotPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		if r.URL.Path != "/batch" {
			w.WriteHeader(404)
			return
		}
		io.WriteString(w, `{"services":{"web":{"state":"yours","ttl":60,"epoch":1}},"peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()

	for _, url := range []string{srv.URL + "/", srv.URL + "/batch"} {
		r := arbiter.New(url, "s", "host-a", "i1", 1, clock.NewFake()).
			Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}})
		if r.Services["web"].Lease != decide.Yours {
			t.Fatalf("url %q: want it to reach /batch, got %+v", url, r.Services["web"])
		}
	}
	for _, p := range gotPaths {
		if p != "/batch" {
			t.Fatalf("want every request to hit exactly /batch, got path %q (all: %v)", p, gotPaths)
		}
	}
}

func TestDeadlineAgainstAServerThatNeverAnswers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0") // accepts, never reads or writes
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c
		}
	}()
	start := time.Now()
	r := arbiter.New("http://"+ln.Addr().String(), "s", "host-a", "i1", 0.2, clock.NewSystem()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("call must respect its deadline, took %v", elapsed)
	}
	if r.Services["web"].Lease != decide.Unreachable {
		t.Fatalf("web: got %+v", r.Services["web"])
	}
}

func TestSentAtIsRecordedBeforeTheRequest(t *testing.T) {
	clk := clock.NewFake()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clk.Advance(5) // 5 "seconds" of network/server latency
		io.WriteString(w, `{"services":{"web":{"state":"yours","ttl":60,"epoch":1}},"peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clk).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}})
	if r.SentAt != 0 { // fake started at 0; advance happened DURING the request
		t.Fatalf("SentAt must be captured before send, got %v", r.SentAt)
	}
}

// A 200 with a requested key silently dropped from the response must never
// read as free: the arbiter's own contract is "poisons only itself" per key,
// but a transport-adjacent bug (a proxy stripping a field, a truncated body
// that still parses) that drops a key entirely must fail closed like any
// other missing answer, not grant an implicit free lease.
func TestMissingRequestedKeyInResponseIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"services":{"web":{"state":"yours","ttl":60,"epoch":1}},"peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}, "blog": {Op: "read"}})
	if r.Services["web"].Lease != decide.Yours {
		t.Fatalf("web: got %+v", r.Services["web"])
	}
	if r.Services["blog"].Lease != decide.Unreachable {
		t.Fatalf("blog: a dropped entry must never read as free, got %+v", r.Services["blog"])
	}
}

// Only duplicate_host means another live process; invalid_op and not_holder
// are our own bug (a malformed op, releasing a lease we don't hold) and must
// not be mistaken for a peer's claim on the name.
func TestOtherErrorValuesAreUnreachableNotConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"services":{
          "web":{"state":"error","error":"invalid_op"},
          "blog":{"state":"error","error":"not_holder"}},
          "peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "renew"}, "blog": {Op: "release"}})
	if r.Services["web"].Lease != decide.Unreachable || r.Services["web"].Err != "invalid_op" {
		t.Fatalf("web: got %+v", r.Services["web"])
	}
	if r.Services["blog"].Lease != decide.Unreachable || r.Services["blog"].Err != "not_holder" {
		t.Fatalf("blog: got %+v", r.Services["blog"])
	}
}

func TestFreeVerdictCarriesReleasedAndEvacuatedBy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"services":{
          "web":{"state":"free","epoch":5,"released":true,"evacuated_by":"host-c"}},
          "peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	r := arbiter.New(srv.URL, "s", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp", map[string]arbiter.Op{"web": {Op: "release", Evacuate: true}})
	v := r.Services["web"]
	if v.Lease != decide.Free || !v.Released || v.EvacuatedBy != "host-c" || v.Epoch != 5 {
		t.Fatalf("web: got %+v", v)
	}
}

// Appendix A's wire shape uses omitempty everywhere op fields are false/zero,
// and the client must carry the caller's own identity and auth on every call.
func TestRequestBodyIsMinimalAndCarriesIdentity(t *testing.T) {
	var gotAuth string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("X-Conch-Auth")
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		io.WriteString(w, `{"services":{"web":{"state":"yours","ttl":60,"epoch":1}},"peers":{},"fingerprints":{}}`)
	}))
	defer srv.Close()
	arbiter.New(srv.URL, "topsecret", "host-a", "i1", 1, clock.NewFake()).
		Batch(60, "fp1", map[string]arbiter.Op{"web": {Op: "renew"}})

	if gotAuth != "topsecret" {
		t.Fatalf("X-Conch-Auth: got %q", gotAuth)
	}
	if body["host"] != "host-a" || body["instance"] != "i1" || body["fingerprint"] != "fp1" {
		t.Fatalf("identity fields: got %+v", body)
	}
	if body["ttl"] != 60.0 {
		t.Fatalf("ttl: got %+v", body["ttl"])
	}
	services, ok := body["services"].(map[string]any)
	if !ok {
		t.Fatalf("services: got %+v", body["services"])
	}
	web, ok := services["web"].(map[string]any)
	if !ok {
		t.Fatalf("services.web: got %+v", services["web"])
	}
	if web["op"] != "renew" {
		t.Fatalf("services.web.op: got %+v", web["op"])
	}
	for _, field := range []string{"serving", "override", "evacuate", "hold", "bar_ttl"} {
		if _, present := web[field]; present {
			t.Fatalf("services.web.%s: must be omitted when zero, got %+v", field, web[field])
		}
	}
}
