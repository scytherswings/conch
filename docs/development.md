# Development

This page is the practical guide to building and testing conch. The
[contributing guide](../CONTRIBUTING.md) explains the constraints that matter
when you change the code; read both before opening a pull request.

## Prerequisites

- **Go 1.26+** — the supervisor and CLI.
- **Node 22+** and npm — the Cloudflare Worker and its tests.
- **Docker** — only for the image build and for running the supervisor against a
  real daemon. The test suites do not need Docker or network access.
- **Wrangler** — installed by `npm ci` in `arbiter/`; no global install needed.

## The short version

```sh
make test        # Go suite + arbiter suite
make lint        # gofmt check + go vet
make validate    # validate every shipped example config
make build       # build ./conch
make docker      # build the image as conch:dev
make clean       # remove ./conch
```

`make help` lists the same targets. The raw commands are below if you prefer
not to use the Makefile.

## Layout

| Path | What |
|---|---|
| `cmd/conch/` | The one binary: `run`, `validate`, `status`, `evacuate`, `reclaim`. |
| `internal/decide/` | The pure decision function. No I/O, no clock, no network. |
| `internal/supervisor/` | The action layer that carries out decisions. |
| `internal/config/` | YAML parsing and validation. |
| `internal/arbiter/` | HTTP client for the Cloudflare Worker backend. |
| `internal/dynarbiter/` | SigV4 client for the DynamoDB backend. |
| `internal/dockerapi/` | The Docker Engine API over its unix socket. |
| `arbiter/` | The Cloudflare Worker and its Durable Object. |
| `deploy/` | Kamal accessory templates, example host configs, and the runbook. |
| `examples/` | Copyable configs and a minimal compose file. |
| `docs/` | Public documentation. `docs/private/` is ignored. |

## Build

```sh
go build -o conch ./cmd/conch
```

The binary is static and has no runtime dependencies. The Dockerfile builds the
same binary into a `scratch` image with CA certificates. To build the image:

```sh
docker build -t conch:dev .
```

## Test

```sh
go test ./...
```

The Go suite runs in seconds. It does not use the network, Docker, or sleeps;
time is injected through the `Clock` interface. A test that sleeps is a defect,
not a slow test.

```sh
cd arbiter && npm ci && npm test
```

The arbiter suite runs in the Workers vitest pool (miniflare). It does not need
a Cloudflare account. `make test-arbiter` runs `npm ci` for you if
`arbiter/node_modules` is missing.

## Format and vet

```sh
gofmt -w .
go vet ./...
```

`make lint` fails if any Go file needs formatting or if `go vet` reports a
problem. The arbiter is JavaScript and is not covered by `gofmt`.

## Validate the examples

```sh
go run ./cmd/conch validate --config deploy/config.example.host-a.yml
go run ./cmd/conch validate --config deploy/config.example.host-b.yml
go run ./cmd/conch validate --config examples/config.minimal.yml
```

`conch validate` exits `1` and lists every fault, not just the first. A config
that passes `validate` is the only config conch will run. CI validates the
shipped examples on every change.

## The constraints that matter

The full list is in [CONTRIBUTING.md](../CONTRIBUTING.md). The short version:

- **`internal/decide` stays pure.** It is a function of plain data. Everything
  effectful is an action it returns. A test parses its imports and fails on
  anything outside `fmt` and `strings`.
- **Time is injected.** No `time.Now` or `time.Sleep` in the decision or
  supervisor paths; use the `Clock` interface.
- **Every outbound call has a hard deadline.** A hung supervisor is a missing
  timeout on the arbiter client or the Docker socket client.
- **`--dry-run` does not perform actions.** It logs every decision, executes none, and sends
  the arbiter only `read` operations. A test enforces that.
- **Services are configuration, not code.** A code path keyed on a particular
  application's name is going the wrong way.
- **No real infrastructure in the repo.** Use generic placeholders. Keep
  hostnames, addresses, account IDs, and credentials out of commits.

## Private notes

Public documentation lives in `docs/` and is tracked. Private design records,
plans, runbooks, burn-in notes, and decisions live in `docs/private/`, which is
ignored by git. Do not put real hostnames or credential locations in the public
tree, even in comments.

## CI and releases

CI runs the Go suite, the arbiter suite, the example validation, a Docker build,
and a secret scan on every push and pull request. Dependabot keeps the Go,
npm, GitHub Actions, and Docker dependencies current.

A tag matching `v*` triggers the release workflow: it runs the tests, builds
static binaries for Linux and macOS, publishes them with a checksum file, and
pushes the container image to GitHub Container Registry. The workflow is in
[`.github/workflows/release.yml`](../.github/workflows/release.yml).

## Troubleshooting the local setup

- **`go: cannot find main module`** — run commands from the repository root.
  `go.mod` lives at the root, not inside `conch/`.
- **`npm test` fails in `arbiter/`** — run `npm ci` first. The Worker tests use
  the installed `@cloudflare/vitest-pool-workers`; a stale `node_modules` is the
  usual cause.
- **`make validate` fails** — read the full output. The validator reports every
  fault at once, and the first one is not always the most important.
- **The Docker build fails on `go mod download`** — make sure `go.sum` is
  committed and current. `go mod tidy` should not be needed for a normal build.
