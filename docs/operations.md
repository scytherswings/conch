# Operations

Day-2 commands and the checks that matter. The full deployment runbook is
[`deploy/README.md`](../deploy/README.md); this page is the quick reference.

## The admin socket

The supervisor listens on a Unix socket inside its container. The CLI talks to
that socket; it does not read the config or call the arbiter directly.

```sh
# Inside the container (the binary is at /conch in the scratch image)
docker exec conch /conch status

# Through Kamal, with the EXISTING container
kamal accessory exec --reuse conch-host-a "conch status"
```

`--reuse` is load-bearing. Without it, Kamal may start a new container instead
of talking to the running one. Flags go before service names.

## `conch status`

Prints the supervisor's view of every service as JSON. The fields that matter:

| Field | Meaning |
|---|---|
| `phase` | `ACTIVE`, `STANDBY`, `EVACUATING`, `SUSPENDED`, or the transitional phase. |
| `next_op` | The next arbiter operation conch intends: `read`, `claim`, `renew`, `release`. |
| `lease` | The lease state the arbiter last reported. |
| `peers` | The peer hosts the arbiter has seen recently. |
| `last_stops` | One row per container conch stopped, newest first: the rule that asked for the stop and the wall clock of the check that ordered it. A row means conch asked for the stop and the stop was confirmed. |
| `accessories` | The lease-following accessory counts. |
| `warm_accessories` | The warm accessory counts. |

A healthy standby reads `"phase": "STANDBY"` and `"next_op": "read"` — nothing
claimed, nothing touched. That is the smoke test.

`last_stops` is the thing `docker ps` cannot tell you: a container conch fenced
on purpose and one that fell over look identical from outside. A row means conch
asked for that stop and the stop was confirmed.

## `conch evacuate`

Moves a service off this host deliberately. It drains the service, stops the
connector, releases the lease, and leaves a bar so nothing walks back in while
you work.

```sh
conch evacuate web                 # one service
conch evacuate --all               # every service on this host
conch evacuate --hold --all        # leave the bar in place; do not auto-reclaim
```

Flags go before service names. `--hold` is what you want when you are tearing a
host down: without it, a service with `evacuation.auto_reclaim_after` set can
reclaim itself while you are still working.

`conch evacuate` exits non-zero on timeout or refusal. Do not reboot a host
until it exits zero.

## `conch reclaim`

Clears an evacuation bar and lets the host compete for the lease again. Use it
when you are done with maintenance and the host is healthy.

```sh
conch reclaim web
conch reclaim --all
```

## Planned maintenance

The safe sequence for a host that is serving:

1. `conch evacuate --all` on the host.
2. Wait for exit 0. The connectors are stopped and the leases are released.
3. Do the maintenance.
4. Reboot or redeploy.
5. `conch reclaim --all` when the host is healthy again.

Never just reboot a serving host. The lease will expire, the peer will promote,
and the rebooted host's connector may briefly come back live before reconcile
stops it. That is the boot race described in [Known gaps](known-gaps.md).

## Upgrades

The supervisor is stateless; the lease lives in the arbiter. To upgrade:

1. Build and pin the new image by digest.
2. Evacuate the host you are upgrading, or accept the failover window.
3. Replace the conch container. The connector stays stopped while the host is
   evacuated.
4. Confirm `conch status` is healthy.
5. `conch reclaim --all`.
6. Repeat on the other host.

The two hosts should run the same image. The `updates` check warns when the
connector images differ; keep the supervisor images in step too.

## Fleet inventory

[`deploy/inventory`](../deploy/inventory) reads `conch status` out of each
host's conch container and prints one block per host. Pass SSH targets on the
command line; keep real hostnames out of the repo.

```sh
deploy/inventory you@host-a you@host-b
deploy/inventory host-a:conch-host-a        # explicit conch container
```

An unreachable host is reported as unreachable, not omitted. A host that is
genuinely down shows up as down.

## Security audit

[`deploy/security-audit`](../deploy/security-audit) is a read-only posture and
patch-drift check. It prints one block per host and exits non-zero if any check
fails, so it can run from cron or a scheduled agent.

```sh
deploy/security-audit you@host-a you@host-b
```

It checks OS patch state, the hardening services, and the expected Ruby version.
Keep real hostnames out of the repo; pass them on the command line.

## The label sweep

Every check, conch stops any **running** container carrying a `conch.service`
label that this host's config no longer names. That is how a renamed or removed
service's connector cannot keep serving unsupervised. It only ever stops;
removing the stopped container is still yours. See the rename procedure in
[`deploy/README.md`](../deploy/README.md#identity-fields-are-immutable-in-place).

## Logs

The supervisor logs to stdout. Use your container runtime's log driver. The
`--log-level` flag controls verbosity. A dry run logs every decision and
performs none.

## Secret rotation

The arbiter accepts two secrets during rotation. The order is **add-next → roll
hosts → retire-old**. A single-secret flip across a live fleet is a
self-inflicted outage. The full procedure is in
[`deploy/README.md`](../deploy/README.md#rotation).

## When something looks wrong

Start with [Troubleshooting](troubleshooting.md). The short version:

- A service stuck in `STANDBY` with a healthy peer: check the arbiter URL, the
  secret, and the health check.
- A service `ACTIVE` but the connector is down: check the connector's
  `restart` policy and the Docker daemon.
- Both hosts `ACTIVE`: stop and read [Known gaps](known-gaps.md) first. That is
  the outcome conch is built to prevent.
- The disk warning fires: free space. A full disk wedges Docker in ways that
  make every other symptom worse.
