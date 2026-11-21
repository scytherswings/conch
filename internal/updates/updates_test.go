package updates

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseTag(t *testing.T) {
	cases := []struct {
		in   string
		want string // canonical String() when parseable, "" when not
	}{
		{"2026.10.4", "2026.10.4"},
		{"2026.7.3", "2026.7.3"},
		{"2026.10.04", "2026.10.4"},  // zero-padded month/patch normalise
		{" 2026.10.4 ", "2026.10.4"}, // surrounding space tolerated
		{"v2026.10.4", ""},           // v-prefix is not the fleet's tag shape
		{"2026.10.4.1", ""},          // four-part, never used
		{"2026.10.4-rc1", ""},        // prerelease suffix
		{"latest", ""},               // not a version at all
		{"2026.10", ""},
		{"", ""},
	}
	for _, c := range cases {
		tag, ok := ParseTag(c.in)
		got := ""
		if ok {
			got = tag.String()
		}
		if got != c.want {
			t.Errorf("ParseTag(%q) = %q, %v; want %q", c.in, got, ok, c.want)
		}
	}
}

func TestTagOrdering(t *testing.T) {
	older, _ := ParseTag("2026.7.3")
	newer1, _ := ParseTag("2026.10.4")
	newer2, _ := ParseTag("2027.1.0")
	if !older.Before(newer1) || !newer1.Before(newer2) {
		t.Fatalf("expected 2026.7.3 < 2026.10.4 < 2027.1.0")
	}
	if !newer2.Before(Tag{Year: 2027, Month: 1, Patch: 1}) {
		t.Fatalf("expected patch to break the tie")
	}
	// Same tag is not "before" itself or its equal.
	if same, _ := ParseTag("2026.10.4"); same.Before(same) {
		t.Fatalf("a tag must not be before itself")
	}
}

func TestDockerHubPicksTheNewestStableTag(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("content-type", "application/json")
		// Deliberately NOT in version order (ordering=last_updated is not a
		// version order), and with non-stable tags to filter.
		w.Write([]byte(`{"results":[
			{"name":"2026.10.4"},{"name":"2026.7.3"},{"name":"latest"},
			{"name":"2026.10.3"},{"name":"2026.10.4-rc1"},{"name":"2027.1.0"}
		]}`))
	}))
	defer srv.Close()

	d := NewDockerHub(2 * time.Second)
	d.endpoint = srv.URL

	got, err := d.LatestCloudflared(context.Background())
	if err != nil {
		t.Fatalf("LatestCloudflared: %v", err)
	}
	if got != "2027.1.0" {
		t.Fatalf("latest stable = %q, want 2027.1.0", got)
	}
	if gotPath != "/tags" {
		t.Fatalf("requested %q, want /tags", gotPath)
	}
}

func TestDockerHubNonOKIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	d := NewDockerHub(time.Second)
	d.endpoint = srv.URL

	if _, err := d.LatestCloudflared(context.Background()); err == nil {
		t.Fatal("expected an error for a 429, got nil")
	}
}

func TestDockerHubNoStableTagsIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Write([]byte(`{"results":[{"name":"latest"},{"name":"dev"}]}`))
	}))
	defer srv.Close()

	d := NewDockerHub(time.Second)
	d.endpoint = srv.URL

	if _, err := d.LatestCloudflared(context.Background()); err == nil {
		t.Fatal("expected an error when no stable tag exists, got nil")
	}
}
