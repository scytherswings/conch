// Package updates is conch's best-effort answer to "is there a newer release
// of the software we pin?" — cloudflared above all, since the fleet
// deliberately pins cloudflared (a bump is a reviewable one-line commit) and
// the only lossy part of that discipline is noticing there IS a bump to
// review. It is a narrow, injected source: the supervisor consumes the
// interface, the one concrete source hits Docker Hub, and the failure mode
// of everything here is a logged skip — a version source a host cannot reach
// is a network problem, never a reason to touch serving (config.go's
// UpdatesConfig comment says the same).
package updates

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Source is one place conch can learn the newest stable release of a
// component it supervises. Implementations must be fast and best-effort:
// the supervisor bounds every call with the configured timeout and treats
// any error as "no opinion this run".
type Source interface {
	// LatestCloudflared returns the newest stable cloudflared release tag
	// (e.g. "2026.10.4"), or an error if the source could not be read.
	LatestCloudflared(ctx context.Context) (string, error)
}

// stableTag matches the fleet's stable cloudflared shape: a four-digit year,
// a month, a patch — 2026.10.4, 2026.7.3 — nothing else. Prerelease shapes
// and four-part tags the fleet has never used are outside the comparison.
var stableTag = regexp.MustCompile(`^(\d{4})\.(\d{1,2})\.(\d{1,2})$`)

// Tag is one parsed cloudflared release tag, ordered numerically.
type Tag struct {
	Year, Month, Patch int
}

// ParseTag parses a `YYYY.M.P` tag. ParseTag reports false for anything that
// is not exactly that shape (a prerelease suffix, a "v" prefix, a four-part
// tag): an unparseable tag is not a version conch can compare.
func ParseTag(s string) (Tag, bool) {
	m := stableTag.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return Tag{}, false
	}
	y, err1 := strconv.Atoi(m[1])
	mo, err2 := strconv.Atoi(m[2])
	p, err3 := strconv.Atoi(m[3])
	if err1 != nil || err2 != nil || err3 != nil {
		return Tag{}, false
	}
	return Tag{Year: y, Month: mo, Patch: p}, true
}

// Before reports whether t is strictly older than other.
func (t Tag) Before(other Tag) bool {
	if t.Year != other.Year {
		return t.Year < other.Year
	}
	if t.Month != other.Month {
		return t.Month < other.Month
	}
	return t.Patch < other.Patch
}

// String reconstructs the canonical `YYYY.M.P` form.
func (t Tag) String() string {
	return fmt.Sprintf("%d.%d.%d", t.Year, t.Month, t.Patch)
}

// CloudflaredTag extracts the stable cloudflared tag from a Docker image ref
// as a host would inspect it: "cloudflare/cloudflared:2026.10.4", with an
// optional digest suffix ("...:2026.10.4@sha256:..."), a registry prefix, or
// a bare "cloudflared:..." name. Anything that is not a cloudflared image
// with a parseable stable tag reports false — a digest-pinned ref with no
// tag, a floating ":latest", a different connector's image.
func CloudflaredTag(ref string) (Tag, bool) {
	if i := strings.IndexByte(ref, '@'); i >= 0 {
		ref = ref[:i] // a digest tells us nothing about the tag; drop it
	}
	repo, tag, ok := strings.Cut(ref, ":")
	if !ok {
		return Tag{}, false
	}
	if repo != "cloudflared" && !strings.HasSuffix(repo, "/cloudflared") {
		return Tag{}, false
	}
	return ParseTag(tag)
}

// DockerHub reads cloudflare/cloudflared's tags from Docker Hub's CRUD API.
// It is the default Source; the endpoint field is what tests override.
type DockerHub struct {
	// endpoint is the repository tags URL (a trailing "/tags" is ignored),
	// defaulting to Docker Hub's public API.
	endpoint string
	client   *http.Client
}

// NewDockerHub builds a DockerHub source whose one fetch is bounded by
// timeout (the whole check's budget, not a per-tag one).
func NewDockerHub(timeout time.Duration) *DockerHub {
	return &DockerHub{
		endpoint: "https://hub.docker.com/v2/repositories/cloudflare/cloudflared",
		client:   &http.Client{Timeout: timeout},
	}
}

var _ Source = (*DockerHub)(nil)

// LatestCloudflared fetches the tag list and returns the newest stable tag
// in it. Only stable `YYYY.M.P` tags are considered — the fleet's shape —
// and the newest is the numerically largest, not merely the newest-sorted
// from the API (Docker Hub can paginate by last_updated, which is not a
// version order). An absent or unparsable body is an error: "the source
// could not be read" is the honest shape for a check that has no opinion.
func (d *DockerHub) LatestCloudflared(ctx context.Context) (string, error) {
	endpoint := d.endpoint
	if strings.HasSuffix(endpoint, "/") {
		endpoint = strings.TrimRight(endpoint, "/")
	}
	if strings.HasSuffix(endpoint, "/tags") {
		endpoint = strings.TrimSuffix(endpoint, "/tags")
	}
	// page_size is generous because the one thing this fetch is FOR is the
	// newest tags; a weekly shipper's latest stable is always on page one.
	url := endpoint + "/tags?page_size=100&ordering=last_updated"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("docker hub %s: status %d", url, resp.StatusCode)
	}

	var body struct {
		Results []struct {
			Name string `json:"name"`
		} `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return "", err
	}

	best := Tag{}
	found := false
	for _, r := range body.Results {
		t, ok := ParseTag(r.Name)
		if !ok {
			continue
		}
		if !found || best.Before(t) {
			best, found = t, true
		}
	}
	if !found {
		return "", fmt.Errorf("docker hub %s: no stable tags in the response", url)
	}
	return best.String(), nil
}
