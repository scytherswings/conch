# Deployment

conch runs as one container per host. It needs two things from the host:

1. **The Docker socket**, read-write. conch starts and stops containers through
   the same daemon that runs them.
2. **The Docker data directory**, read-only. The free-space guard's `disk.path`
   resolves inside the conch container, so the host's `/var/lib/docker` must be
   bound at the same path. Without it, the guard sees a missing path and warns
   on every check.

The shipped image is a `scratch` image with one static binary and CA
certificates. It has no shell. Use the admin socket for commands, not
`docker exec ... sh`.

The full deployment runbook is
[`deploy/README.md`](../deploy/README.md). This page is the map.

## Two paths

### Path 1: Kamal accessory (recommended if you already use Kamal)

The templates in `deploy/` are Kamal accessories. There are two shapes:

- [`deploy/accessory-quick-start.yml`](../deploy/accessory-quick-start.yml) —
  one service, one host, host networking. This is the shortest path to a
  working connector-readiness check. It requires the app's health port to be
  published on the host loopback.
- [`deploy/conch-accessory.template.yml`](../deploy/conch-accessory.template.yml)
  — multiple services, per host, Docker-network DNS. This is the steady-state
  shape. It does not use host networking.

Copy the relevant accessory block into your Kamal `config/deploy.yml`, mount the
config file, and set the secret. The runbook covers the exact steps, the
`--reuse` caveat, and the pre-deploy validate hook.

### Path 2: Plain Docker

If you do not use Kamal, run the same image with the same two binds:

```sh
docker run -d \
  --name conch \
  --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /var/lib/docker:/var/lib/docker:ro \
  -v /etc/conch:/etc/conch:ro \
  -e CONCH_AUTH_SECRET \
  <your-registry>/conch:latest \
  run --config /etc/conch/conch.yml --socket /run/conch/admin.sock
```

The admin socket lives inside the container. To run admin commands from the
host, use `docker exec conch conch status` (the binary is at `/conch` in the
image) or mount a host directory at `/run/conch` and point the host-side CLI at
it. Do not publish the admin socket on a network.

## Build the image

```sh
docker build -t <your-registry>/conch:latest .
```

The `Dockerfile` is at the repository root. It builds a static binary and copies
it into a `scratch` image. Pin the image by digest in production.

## The secret

Every host sends the same shared secret to the arbiter in the `X-Conch-Auth`
header. The arbiter compares it constant-time. The secret is the fence: anyone
who has it can claim leases.

- Store it in your secret manager, not in the repo.
- Give it to conch through `arbiter.secret_env` or `arbiter.secret_file`.
- On the Worker side, set it with `wrangler secret put CONCH_AUTH_SECRET`.
- Rotate with the two-secret window: **add-next → roll hosts → retire-old**.
  Never flip a single secret across a live fleet; every host still on the old
  value would start getting 401 at the stop deadline. The full procedure is in
  [`deploy/README.md`](../deploy/README.md#rotation).

## Validate before you deploy

Install the pre-deploy hook so a bad config never reaches a host:

```sh
cp deploy/hooks/pre-deploy-validate .kamal/hooks/pre-deploy
chmod +x .kamal/hooks/pre-deploy
```

The hook runs `conch validate` against the mounted config using the image you
are about to deploy. A config with any fault aborts the deploy. `conch validate`
lists every fault, not just the first.

## Two hosts, one lease per service

conch is designed and tested for two hosts. Do not run it as a single-host tool.
Each host runs its own conch container and its own copy of the config. The only
difference between the two configs is `host:`.

Both hosts supervise the same service names and lease keys. The arbiter decides
which host holds each lease. The holder runs the connector; the other host
stops its connector and waits.

## Connector restart policy

The connector must be `restart: unless-stopped`, never `always`. `always`
resurrects a connector conch deliberately stopped, which produces two live
connectors from one YAML line. This is the single most important deployment
detail on this page.

## Identity fields are immutable in place

`host`, a service's `name`, and its `lease_key` are distributed-systems
identifiers. Renaming one in a live config orphans the lease: the arbiter starts
tracking a new key while the old connector may still be serving. Evacuate first,
then edit. The procedure is in
[`deploy/README.md`](../deploy/README.md#identity-fields-are-immutable-in-place).

## Planned maintenance

Never just reboot a host that is serving. Evacuate first:

```sh
kamal accessory exec --reuse conch-host-a "conch evacuate --all"
# wait for exit 0, then reboot
```

`--reuse` is load-bearing: without it, Kamal may start a new container instead
of talking to the running one. Flags go before service names. The runbook covers
the full maintenance sequence, including `--hold` when you are tearing a host
down.

## The replication-sidecar warning

If you run a replication shipper (Litestream, a WAL shipper, anything with a
single writer), it must follow the lease. Put it under `accessories`, not
`warm_accessories`. A shipper on the standby corrupts the replica's generation.
A database that a promotion restores into is the opposite: put it under
`warm_accessories` so it runs on both hosts. See
[Configuration](configuration.md#accessories-and-warm_accessories).

## Next

- [Operations](operations.md) for day-2 commands.
- [Known gaps](known-gaps.md) before you trust it with stateful data.
- [Troubleshooting](troubleshooting.md) when something looks wrong.
