package supervisor

import (
	"context"
	"io"
	"net/http"
	"syscall"
	"time"
)

// probeBodyLimit is how much of a health response is read before the
// connection is returned to the pool. Draining lets keep-alive reuse the
// connection; the cap stops a service that answers 200 with a gigabyte from
// costing anything.
const probeBodyLimit = 4 << 10

// httpProber is spec §4's HTTP health check. Two things about it are
// load-bearing:
//
//   - Redirects are never followed. A 301 to a working page must not mask a
//     sick service, so a redirect is an ANSWER, compared against the
//     configured expect list like any other status, never a hop to take.
//     http.ErrUseLastResponse is what makes the redirect itself the result.
//
//   - The deadline is per call, from the service's own health.timeout, on a
//     shared Transport — the same shape as dockerapi's client. A fixed
//     client-wide Timeout could not serve services with different budgets.
type httpProber struct {
	client *http.Client
}

func newHTTPProber() *httpProber {
	return &httpProber{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Probe reports whether url answered with one of the expected statuses. An
// unconfigured URL is not a failure: a service with no health check is one
// conch judges by its container alone, not one that is permanently sick. Any
// transport failure — refused, timed out, TLS, DNS — is unhealthy, since the
// question being asked is "can this be served", and "couldn't ask" is never
// yes.
//
// host, when non-empty, overrides the request's Host header by setting
// req.Host directly — the one mechanism that actually works. net/http's
// Transport always writes the wire Host line from req.Host (or, if that is
// empty, from the request URL's host) and ignores any "Host" entry placed in
// req.Header, so a header-map assignment would silently do nothing. This is
// what lets the probe traverse a reverse proxy addressed by an internal
// name or IP while presenting the public vhost the proxy actually routes
// on — RCA background: conch reported healthy for 39 hours because its
// probe went straight to the app container and never saw what a real
// request through the proxy would.
func (p *httpProber) Probe(url, host string, timeoutSeconds float64, expect []int) bool {
	if url == "" {
		return true
	}
	if len(expect) == 0 {
		expect = []int{http.StatusOK}
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 2
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	if host != "" {
		req.Host = host
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, probeBodyLimit))

	for _, want := range expect {
		if resp.StatusCode == want {
			return true
		}
	}
	return false
}

// diskFreeGB is spec §4's disk headroom, read through the conch container's
// bound docker data directory (spec §11's namespace honesty: this path
// resolves INSIDE the container). Bavail, not Bfree: the reserved blocks
// root can still write into are not headroom a docker daemon can use.
func diskFreeGB(path string) (float64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, err
	}
	return float64(fs.Bavail) * float64(fs.Bsize) / (1 << 30), nil
}
