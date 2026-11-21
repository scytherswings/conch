---
name: conch-fleet
description: Read a conch fleet's live inventory - which services conch manages, which host holds each lease, whether it is serving, and which services each host supervises. Use when asked where services run, which host is active, what conch is managing, whether a service depends on conch, or anything about a conch supervisor, its leases, or the arbiter.
license: MIT
compatibility: Needs network access to the conch arbiter and its shared secret. Uses curl (and the conch binary if installed). Read-only.
---

# conch fleet inventory

conch is a failover supervisor: exactly one host holds the lease for each
service and serves it; the other waits. This skill reads the fleet's live state
from the arbiter's read-only `GET /fleet`, so you can answer "where does this
run?" and "which host is active?" without ssh, a config file, or a checkout.

## Quick start

```sh
CONCH_ARBITER=https://conch-lease.example.workers.dev \
CONCH_AUTH_SECRET=... \
  scripts/fleet.sh
```

Or, if the `conch` binary is installed, the same thing with nicer output:

```sh
conch fleet --arbiter https://conch-lease.example.workers.dev --json
```

Nothing here writes. The read claims, renews, and releases nothing, so it is
safe to point at a production arbiter.

## Reading the output

`services` — one entry per service conch manages:

- `holder`: the host holding the lease, or `null` if free.
- `serving`: true while that host is actually serving traffic.
- `epoch`: bumps on each new hold; useful to spot a handover.
- `expiresIn`: seconds until the lease lapses (the holder renews every few).
- `barred`: fenced by an evacuation. `conflict`: a duplicate-host window.

`hosts` — one entry per host seen in the last hour:

- `lastSeenAgo`: seconds since the host last checked in.
- `services`: the services that host supervises (its intent).

## Answering common questions

- **Which host is active for X?** `services.X.holder`, and `serving` says whether
  traffic is actually flowing there.
- **What does conch manage?** Every key in `services`; every name in each host's
  `services` list.
- **Does service Y depend on conch?** It appears in `services`, or in some
  host's `services` list.
- **Is anything wrong?** A service with `holder: null` that some host still lists
  under `services` is drift — nobody supervises it. A `conflict` means two
  supervisors share one host name.

A service absent from every host is not supervised; it may be served
active-active with no conch involved.

## Reference

See the conch docs for the endpoint's shape and its authentication:
`docs/security.md` (the read surface) and `deploy/README.md` (fleet inventory).
