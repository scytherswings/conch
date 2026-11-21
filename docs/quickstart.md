# Quick start

This is the shortest path to a working two-host setup. It assumes you already
have two hosts running your services in Docker and one Cloudflare Tunnel per
service. If you want the Kamal accessory path instead of hand-built containers,
read [Deployment](deployment.md) after step 1.

## What you need

- **Two hosts** running the same services in Docker. conch is designed and
  tested for two; do not run it as a single-host tool.
- **A Cloudflare account** (the free tier is enough) and one tunnel per service.
- **A shared secret** — a long random string, the same value on every host. The
  arbiter uses it to authenticate every request.
- **Go 1.26+** if you build from source, or the published container image.
- **Kamal** only if you want the shipped deployment path. The supervisor itself
  is a single static binary and does not need Kamal.

## 1. Deploy the arbiter

The arbiter is a Cloudflare Worker with one Durable Object. It holds one lease
per service.

```sh
cd arbiter
npm install
npx wrangler deploy
npx wrangler secret put CONCH_AUTH_SECRET
```

`wrangler deploy` prints the Worker URL. It looks like
`https://conch-lease.<your-subdomain>.workers.dev`. Write it down; every host's
config points at it. The secret you set here must be the same value on every
host.

> The Worker name is in `arbiter/wrangler.toml`. If you change it, the URL
> changes. A dedicated Cloudflare account is recommended: the free tier's
> request meter is shared with everything else in the account.

## 2. Build conch

From the repository root:

```sh
go build -o conch ./cmd/conch
```

Or install the CLI directly:

```sh
go install github.com/scytherswings/conch/cmd/conch@latest
```

Or build the container image:

```sh
docker build -t <your-registry>/conch:latest .
```

The image is a `scratch` image with one static binary and CA certificates.
That is deliberate: a failover supervisor has to work when the host is already
unhealthy.

## 3. Write one config per host

Start from the shipped examples and change only what is yours:

```sh
cp deploy/config.example.host-a.yml ./conch.yml
```

Set:

- `host:` — a stable logical name for this host (`host-a`, `host-b`). It is a
  distributed-systems identifier, not a hostname. Do not rename it in place.
- `arbiter.url:` — the Worker URL from step 1.
- `arbiter.secret_env:` — the environment variable that holds the shared
  secret. The default name is `CONCH_AUTH_SECRET`.
- `services:` — your real service names, container names, connector names and
  health URLs.

Validate before you run anything:

```sh
./conch validate --config ./conch.yml
```

It exits `1` and lists **every** fault, not just the first. A config that
passes `validate` is the only config conch will run.

## 4. Dry-run before you touch traffic

A dry run logs every decision and performs none of the actions. It sends the
arbiter only `read` operations, so it cannot claim a lease from a live host.

```sh
CONCH_AUTH_SECRET=... ./conch run --config ./conch.yml --dry-run --socket ./admin.sock
```

In another shell, ask it what it thinks:

```sh
./conch status --socket ./admin.sock
```

Every service should report `"phase": "STANDBY"` and `"next_op": "read"` —
nothing claimed, nothing touched. That is the smoke test. If a service is
already `ACTIVE`, something else is holding the lease; stop and find out what
before you continue.

## 5. Run it for real

Start the supervisor on both hosts with the same config shape and the same
secret. The first host to win a lease becomes `ACTIVE`; the other stays
`STANDBY`. That is the normal steady state.

```sh
CONCH_AUTH_SECRET=... ./conch run --config ./conch.yml
```

If you are using Kamal, the accessory templates in `deploy/` wrap the same
binary with the right binds. See [Deployment](deployment.md).

## What "working" looks like

- One host is `ACTIVE` for each service; the other is `STANDBY`.
- The active host's connector is running; the standby host's connector is
  stopped.
- `conch status` shows a lease and a `next_op` for each service.
- If conch has stopped a container on purpose, the service also carries
  `last_stops`: one row per container, newest first, saying which rule asked
  for the stop and when. A row means conch asked for the stop and the stop was
  confirmed. That is the thing `docker ps` cannot tell you, because a container
  conch fenced on purpose and one that fell over look identical from outside.

## Next

- Read [Configuration](configuration.md) before you change timing or add
  services.
- Read [Known gaps](known-gaps.md) before you trust it with stateful data.
- Read [Deployment](deployment.md) for the Kamal accessory path, the two binds,
  and secret rotation.
- Read [Operations](operations.md) for evacuate, reclaim, inventory and
  upgrades.
