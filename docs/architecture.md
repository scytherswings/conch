# Architecture

conch is a failover supervisor for two hosts running the same services behind
Cloudflare Tunnel. It makes connector liveness track service health, so the edge
routes around a host that cannot serve.

## The problem

Cloudflare Tunnel routes traffic on **connector** liveness, not **service**
health, and picks randomly among live connectors. Run connectors at two hosts,
let one host's service die, and Cloudflare keeps sending traffic there. Visitors
get 502s. Cloudflare sells the fix (Load Balancing); conch is the free
workaround.

## The trick

Make connector liveness track service health:

- If a service is unhealthy here, stop its connector here.
- The edge then routes to the other host.
- Connector-level failover becomes service-level failover.

That needs an arbiter, because two connectors for one service must never be live
at once. That is the one outcome that corrupts data.

```
 client → Cloudflare edge → tunnel(svc) → [ the ONE live connector ] → proxy → svc
                                                    ▲
                                     conch (one per host) ──── POST /batch ──── arbiter
                                                                          (Worker + 1 DO)
```

## The components

### The arbiter

The arbiter holds one lease per service. The default is a Cloudflare Worker with
a single Durable Object. A second backend talks to DynamoDB directly. The whole
fleet's lease state lives in one place, so there is one serialization point for
every claim.

Each lease records the holder, the instance, the epoch, the expiry, whether the
holder is serving, and whether the service is evacuated. The arbiter also tracks
the last check-in from each host.

The protocol is one `POST /batch` per host per check. The request carries the
host's ops for every service; the response carries the verdicts and the peer
map. The Go client and the Worker share the same wire shape. See
[`arbiter/README.md`](../arbiter/README.md) for the protocol and the Durable
Object details.

### The supervisor

One conch per host. It checks every `interval`, reads the arbiter's verdicts,
and reconciles the local Docker state:

- The lease holder runs the service container and its connector.
- The standby stops its connector and leaves the service container alone.
- Accessories follow the lease; warm accessories run everywhere.
- Health checks gate promotion. Hooks run on promotion and evacuation.

The supervisor is a single static Go binary. It talks to Docker over the Unix
socket and to the arbiter over HTTPS. It has no database of its own; the arbiter
is the only shared state.

### The decision function

`internal/decide` is pure. It takes plain data and returns actions. Everything
effectful — Docker calls, HTTP calls, hooks — is an action it returns. That is
what makes `--dry-run` mean "do not perform the actions" rather than "audit
every call site". The action layer sits behind narrow interfaces with recording
fakes, so tests assert exactly which Docker operations happened, in what order.

## The safety invariants

These are the load-bearing rules. They are enforced by validation, by the
decision function, or by the deployment shape.

1. **One holder per service.** The arbiter serializes claims. Two hosts cannot
   both hold the same lease.
2. **Quick to stop, slow to take over.** `promote_after > demote_after` is
   enforced. A flapping link cannot ping-pong traffic.
3. **The stop deadline is derived per service.** It is the connector's
   `stop_timeout` plus slop. One slow-draining connector taxes only itself.
4. **Exact names, one owner per container.** Accessories and warm accessories
   are exact names, never `*`. No container may be owned by two services. Two
   reconciles fighting over one container would start and stop it forever.
5. **`restart: unless-stopped`, never `always`.** `always` resurrects a
   connector conch deliberately stopped. That is the forbidden two-connectors
   outcome delivered by one YAML line.
6. **The secret is the fence.** Every host authenticates with the shared secret.
   A compromised secret can claim leases. Treat it as a credential.
7. **The decision function is pure.** No I/O in the policy. Time is injected.
   Every outbound call has a hard deadline.

## Failure model

The arbiter is the single point of coordination. Its failure is the interesting
case.

- **`on_loss: freeze` (default).** If the arbiter is unreachable, conch freezes
  the fleet in place: whatever is serving keeps serving on an expired lease and
  conch yells. This is safe against every symmetric arbiter failure, because
  promotion requires a successful grant. The residual risk is an asymmetric
  partition: the standby still reaches the arbiter, sees a free lease, and
  promotes while the frozen host serves.
- **`on_loss: stop`.** If the arbiter is unreachable, conch stops serving. This
  removes the asymmetric-partition risk and makes the arbiter's availability the
  fleet's.

Neither closes every failure. The [known gaps](known-gaps.md) page is the honest
list.

## The label sweep

Every check, conch stops any running container carrying a `conch.service` label
that this host's config no longer names. That is how a renamed or removed
service's connector cannot keep serving unsupervised. It only ever stops;
removing the stopped container is yours. The label belongs on the connector, not
the app container.

## What this is not

- **Not high availability.** Medium availability, best-effort: a bounded outage
  on failover, and writes inside the replication-lag window are lost on
  unplanned failover. That is the trade for free.
- **Not a load balancer.** conch decides who may serve, never who gets a
  request. If it ever distributes requests, it has gone wrong.
- **Not active/active.** Exactly one host serves each service.
- **Not a database failover tool.** It never promotes a database primary and
  never destroys local data. It orchestrates restores from replication you
  already run; it takes no backups.
- **Not a replacement for quorum.** Two or more hosts, designed and tested for
  two. At three-plus, use real quorum tooling: Patroni, etcd, a stretched Nomad
  region.

## Why Go, and why so few dependencies

A failover supervisor has to work when the host is already unhealthy. A single
static binary in a `scratch` image is the smallest surface that can drive Docker
and speak HTTPS. Beyond a YAML parser and the optional DynamoDB SDK, there are no
dependencies outside the Go standard library. The scarcity is deliberate: this
component can stop every service's connector, and every dependency is code that
can take the fleet down.

## Next

- [Failover timing](failover-timing.md) for the default timeline.
- [Known gaps](known-gaps.md) for the honest limitations.
- [Arbiter backends](backends.md) for Cloudflare vs DynamoDB.
