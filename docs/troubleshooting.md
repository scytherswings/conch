# Troubleshooting

Start with the config and the supervisor's own view. Most problems are one of:
the config does not validate, the secret is wrong, the health check cannot reach
the service, or the Docker socket is not mounted.

## First checks

```sh
conch validate --config /path/to/conch.yml
conch status --socket /run/conch/admin.sock
```

`validate` exits `1` and lists **every** fault, not just the first. Fix all of
them before you run. `status` prints the supervisor's view as JSON: each
service's phase, lease, peer map, and `last_stops`.

## A service is stuck in `STANDBY`

The standby is healthy if it is not serving. Check in this order:

1. **Is the arbiter reachable?** A 401 means the secret is wrong or a rotation
   is half-done. A 500 from the Worker usually means `CONCH_AUTH_SECRET` is not
   set. A timeout means the URL or the network is wrong.
2. **Is the health check green?** The health URL resolves **inside the conch
   container**, not on the host. If it uses `127.0.0.1`, the conch container must
   use host networking (`host_network: true`). If it uses a Docker DNS name, the
   conch container must be on the same Docker network.
3. **Is the peer holding the lease?** If the peer is `ACTIVE`, the standby is
   doing its job. If the peer is dead but the lease has not expired, wait for
   the `ttl`. If the peer is frozen, the lease may be held by a corpse until the
   TTL expires.
4. **Is the connector healthy?** A connector that is up but not ready can block
   promotion. Check the connector's `ready_url` and its own logs.

## A service is `ACTIVE` but the connector is down

- Check the connector container. Is it running? Is it restarting?
- Check the `restart` policy. It must be `unless-stopped`, never `always`.
  `always` resurrects a connector conch deliberately stopped.
- Check the Docker daemon. A wedged daemon can leave a connector in any state.
- Check `last_stops`. A row means conch asked for the stop and the stop was
  confirmed. A container that fell over on its own looks different.

## Both hosts are `ACTIVE`

That is the forbidden outcome. Stop one host's connector manually first. Then
find out why the lease was granted twice:

- Was the arbiter reachable from both hosts? An asymmetric partition can let a
  standby promote while a frozen host serves. See [Known gaps](known-gaps.md).
- Was a second conch started on the same host with the same `host:` value? The
  arbiter rejects duplicate holders, but a duplicate `host:` value is still a
  config error.
- Was the secret rotated on one host only? A host that cannot authenticate may
  behave differently.
- Was the lease stolen by a manual `wrangler` change or a DynamoDB edit?

Do not restart both hosts and hope. Read the logs from both sides.

## The arbiter returns `401`

The secret does not match. Check:

- The host's `CONCH_AUTH_SECRET` value.
- The Worker's `CONCH_AUTH_SECRET` (and `CONCH_AUTH_SECRET_NEXT` during
  rotation).
- The `arbiter.secret_env` / `secret_file` path. A file with the wrong mode or
  a missing newline can still be read; a missing file is a config fault.

The rotation order is **add-next → roll hosts → retire-old**. A single-secret
flip is a self-inflicted outage.

## The arbiter returns `500`

The Worker is missing its secret. Set it:

```sh
cd arbiter
npx wrangler secret put CONCH_AUTH_SECRET
```

If the Worker is up but the Durable Object is wedged, check the Worker logs with
`npx wrangler tail`. The `arbiter/README.md` describes the protocol.

## The arbiter returns `too_many_services`

The Worker caps a batch at 64 services. A single host config that supervises
more than that is rejected. Split the fleet or move to the DynamoDB backend if
you need a larger single batch.

## The health check fails, but the service is up

The probe runs from the conch container. Check:

- The URL's host resolves from **inside** the conch container.
- The port is reachable on the Docker network.
- The `Host` header is what the service expects. Use `health.host` when the
  service is behind a shared reverse proxy.
- The status code is in `health.expect`.
- The timeout is long enough. The default is a short deadline; a slow service
  will fail.
- If the URL is loopback, `host_network: true` is set and the connector is on
  the host network.

## The disk warning fires

The `disk` guard checks the path inside the conch container. It warns when free
space is below `min_free_gb`. A full disk is a leading cause of wedged Docker
daemons. Free space; do not ignore it. The guard recommends; it does not limit.

## A hook fails

Hook failures are retried with backoff. Check the hook container's logs. A
restore hook that takes longer than the check interval can delay promotion; the
failover time is the timeline plus the restore time. A hook that fails
permanently leaves the service unable to promote; the fix is the hook, not the
lease.

## The supervisor will not start

- Run `conch validate` first. It prints every fault.
- Check the secret. A missing secret is a startup failure, not a runtime one.
- Check the Docker socket. The container must mount it read-write.
- Check the config path. The default is `/etc/conch/conch.yml`; the container
  must have it mounted.
- Check the admin socket directory. The default is `/run/conch/admin.sock`; the
  process must be able to create it.

## Debugging safely

`--dry-run` logs every decision and performs none of the actions. It sends the
arbiter only reads, so it cannot claim a lease from a live host. Use it to see
what conch would do without touching traffic.

```sh
CONCH_AUTH_SECRET=... conch run --config ./conch.yml --dry-run --socket ./admin.sock
```

Do not run a second real supervisor on the same host with the same config. One
conch per host.

## The `fingerprints` differ between hosts

The peer map reports a fingerprint per host. A mismatch means the hosts disagree
about the config. Fix the config drift before you rely on failover. The
`updates` check also compares connector image versions; a mismatch is a warning,
not a lease failure.

## Where to look next

- [Operations](operations.md) for the commands.
- [Known gaps](known-gaps.md) for the failure modes that are not bugs.
- [Architecture](architecture.md) for the lease model.
- [`arbiter/README.md`](../arbiter/README.md) for the wire protocol.
- [`deploy/README.md`](../deploy/README.md) for the deployment runbook.
