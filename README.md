# conch

[![CI](https://github.com/scytherswings/conch/actions/workflows/ci.yml/badge.svg)](https://github.com/scytherswings/conch/actions/workflows/ci.yml)
[![Go](https://img.shields.io/github/go-mod/go-version/scytherswings/conch)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> *"He can hold it when he's speaking."* — whoever holds the conch has the floor,
> and everyone else waits.

The name is the design: exactly one host holds the floor for each service, and the
other waits. conch keeps one Cloudflare Tunnel connector live per service across
two hosts, and fails over when the active host goes unhealthy. It is built for
[Kamal](https://kamal-deploy.org) deployments that run the same containers on two
hosts.

## Why this exists

Kamal deploys the same containers to two hosts. Cloudflare Tunnel exposes them.
Cloudflare routes traffic to any live connector, and it does not check whether the
service behind that connector is healthy. If both hosts run connectors for the
same service and one host's service dies, Cloudflare can keep sending traffic to
the dead host. Visitors get 502s.

Cloudflare's paid Load Balancing product fixes this. conch is the open-source
alternative: connector liveness follows service health. When a service is
unhealthy on a host, conch stops that host's connector and the edge routes to the
other host.

Two connectors for one service must never be live at once. That is the failure
conch exists to prevent. A small arbiter (a Cloudflare Worker with one Durable
Object, or a DynamoDB table) holds one lease per service. Each host checks in
every few seconds. A host may run a connector only while it holds the lease. When
the lease expires, the other host can claim it.

```
client -> Cloudflare edge -> tunnel(svc) -> the one live connector -> proxy -> svc
                                                ^
                                 conch (one per host) -- POST /batch --> arbiter
                                                              (Worker + 1 DO)
```

## Quick start

You need two hosts running your services in Docker, a Cloudflare account, and one
tunnel per service. conch is designed for two hosts. Do not run it as a
single-host tool.

The normal path is Kamal. The full runbook is in
[`deploy/README.md`](deploy/README.md).

1. Deploy the arbiter. It is a Cloudflare Worker with one Durable Object.

   ```sh
   cd arbiter
   npm ci
   npx wrangler deploy
   npx wrangler secret put CONCH_AUTH_SECRET
   ```

   Use the same secret on every host. Wrangler prints the Worker's URL.

2. Add the conch accessory to your Kamal `config/deploy.yml`. Start from
   [`deploy/accessory-quick-start.yml`](deploy/accessory-quick-start.yml) or
   [`deploy/conch-accessory.template.yml`](deploy/conch-accessory.template.yml).
   Copy the matching host config from `deploy/` and set `host:`, the arbiter URL,
   and your real services.

3. Put `CONCH_AUTH_SECRET` and `TUNNEL_TOKEN` in `.kamal/secrets`.

4. Validate the config. The pre-deploy hook in
   [`deploy/hooks/pre-deploy-validate`](deploy/hooks/pre-deploy-validate) runs it
   for you. Run it by hand with `conch validate --config ./conch.yml`; it lists
   every fault, not just the first.

5. Boot the accessory on both hosts:

   ```sh
   kamal accessory boot conch
   ```

6. Check `conch status`. Every service should be `STANDBY` with `next_op: read`.
   That means conch has not claimed anything.

Before booting for real, run the binary with `--dry-run` on a host. It logs every
decision, performs no actions, and sends the arbiter only reads. See
[`docs/quickstart.md`](docs/quickstart.md) for the manual path and the dry run.

If you are not using Kamal, build with `go build -o conch ./cmd/conch` or
`go install github.com/scytherswings/conch/cmd/conch@latest`, and run it with the
same config. The Dockerfile builds a scratch image.

## What conch is not

- **Medium availability, best-effort.** It is not high availability. At the
  defaults, the lease timeline is about 78 seconds: a 60-second lease plus 6 good
  checks at 3 seconds each. Cloudflare's own dead-connector detection adds to the
  real outage. Requests fail during that window. On an unplanned failover, writes
  that have not reached the standby are lost, because replication is asynchronous.
  Whether that is good enough depends on your application.
- **Not a load balancer.** It decides which host may serve, never which request
  goes where.
- **Not active/active.** One host serves each service.
- **Not a database failover tool.** It runs the restore commands you configure.
  It never promotes a database primary and takes no backups.
- **Not a quorum system.** It is built and tested for two hosts. For three or
  more, use real quorum tooling.

Read [`docs/known-gaps.md`](docs/known-gaps.md) before you trust conch with
anything you cannot lose. The first gap is real: a dead or stuck conch with a
healthy connector can still mean both hosts serve.

## Documentation

- [Quick start](docs/quickstart.md)
- [Configuration](docs/configuration.md)
- [Deployment](docs/deployment.md)
- [Operations](docs/operations.md)
- [Architecture](docs/architecture.md)
- [Failover timing](docs/failover-timing.md)
- [Known gaps](docs/known-gaps.md)
- [Arbiter backends](docs/backends.md)
- [Security](docs/security.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Development](docs/development.md)
- [Examples](examples/README.md)

## Development

```sh
make test        # Go and arbiter tests
make build       # build ./conch
make lint        # gofmt and go vet
make validate    # validate the example configs
make docker      # build the image
```

CI runs the tests, vet, gofmt, example validation, a Docker build, and a secret
scan. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the constraints. Security
reports go through [`SECURITY.md`](SECURITY.md).

## License

MIT. See [`LICENSE`](LICENSE).
