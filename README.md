# conch

[![CI](https://github.com/scytherswings/conch/actions/workflows/ci.yml/badge.svg)](https://github.com/scytherswings/conch/actions/workflows/ci.yml)
[![Go](https://img.shields.io/github/go-mod/go-version/scytherswings/conch)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A centralized lock for Cloudflare Tunnel connectors.** Two hosts run the same
services; conch decides which one may serve each service, and moves it when that
host stops being able to.

Cloudflare Tunnel routes traffic on *connector* liveness, not *service* health,
and picks randomly among live connectors. Run connectors at two hosts, let one
host's service die, and Cloudflare keeps sending traffic there — visitors get
502s. conch makes connector liveness track service health: if a service is
unhealthy here, conch stops its connector here and the edge routes to the other
host. A Cloudflare Worker holds one lease per service so two connectors for the
same service are never live at once.

```
client → Cloudflare edge → tunnel(svc) → [ the ONE live connector ] → proxy → svc
                                                  ▲
                                   conch (one per host) ──── POST /batch ──── arbiter
                                                                        (Worker + 1 DO)
```

## Quick start

You need **two** hosts running your services in Docker, a Cloudflare account
(free), and one tunnel per service. conch is designed and tested for two hosts;
do not run it as a single-host tool.

```sh
# 1. Deploy the arbiter (Cloudflare Worker + one Durable Object)
cd arbiter && npm ci && npx wrangler deploy
npx wrangler secret put CONCH_AUTH_SECRET   # same long random string on every host

# 2. Build conch
cd .. && go build -o conch ./cmd/conch
# or: go install github.com/scytherswings/conch/cmd/conch@latest
# or: docker build -t <your-registry>/conch:latest .

# 3. Copy a config, set `host:`, `arbiter.url:`, and your services
cp deploy/config.example.host-a.yml ./conch.yml
./conch validate --config ./conch.yml

# 4. Dry-run first: logs every decision, executes none, reads only
CONCH_AUTH_SECRET=... ./conch run --config ./conch.yml --dry-run --socket ./admin.sock

# 5. In another shell, ask what it thinks
./conch status --socket ./admin.sock
```

Every service should report `"phase": "STANDBY"` and `"next_op": "read"` in the
dry run — nothing claimed, nothing touched. That is the smoke test. Once conch
has actually stopped something, the service also carries `last_stops`: one row
per container, newest first, saying which rule asked for the stop and when. A
row means conch asked for the stop and the stop was confirmed — the thing
`docker ps` cannot tell you.

Connectors must be `restart: unless-stopped`, never `always`: `always`
resurrects connectors conch deliberately stopped, which is the forbidden
two-connectors outcome delivered by one YAML line.

## What this is not

- **Not high availability.** Medium availability, best-effort: a bounded outage
  on failover, and writes inside the replication-lag window are lost on
  *unplanned* failover. That is the trade for free.
- **Not a load balancer.** conch decides who *may* serve, never who gets a
  request.
- **Not active/active.** Exactly one host serves each service.
- **Not a database failover tool.** It orchestrates restores from replication you
  already run; it takes no backups.
- **Not a replacement for quorum.** Designed and tested for two hosts. At
  three-plus, use real quorum tooling.

Read the [known gaps](docs/known-gaps.md) before trusting conch with anything
you cannot lose. The first one is real: a dead or stuck conch with a healthy
connector can still mean both hosts serve.

## Documentation

- [Quick start](docs/quickstart.md) — the full walkthrough.
- [Configuration](docs/configuration.md) — every field and default.
- [Deployment](docs/deployment.md) — Kamal accessory, binds, secret rotation.
- [Operations](docs/operations.md) — status, evacuate, reclaim, upgrades.
- [Architecture](docs/architecture.md) — the lease model and invariants.
- [Failover timing](docs/failover-timing.md) — the default timeline and how to measure yours.
- [Known gaps](docs/known-gaps.md) — the honest limitations.
- [Arbiter backends](docs/backends.md) — Cloudflare Worker or DynamoDB.
- [Security](docs/security.md) — trust boundaries and hardening.
- [Troubleshooting](docs/troubleshooting.md) — symptoms and first checks.
- [Development](docs/development.md) — build, test, and constraints.
- [Examples](examples/README.md) — copyable configs and a compose file.

## Development

```sh
make test          # Go + arbiter suites
make build         # ./conch
make lint          # gofmt + go vet
make validate      # validate the shipped example configs
make docker        # build the image
```

CI runs the same checks on every push and pull request. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR — its constraints are
load-bearing, not style. Everyone follows the [Code of Conduct](CODE_OF_CONDUCT.md);
report a vulnerability privately via [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
