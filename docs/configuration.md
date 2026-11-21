# Configuration

One YAML file per host. The file is read once at startup. Unknown keys are
rejected, so a typo fails loudly instead of silently falling back to a default.
`conch validate --config <file>` prints every semantic fault and exits `1` when
any exist; `conch run` refuses to start on a config that does not validate.

The shipped examples are the best starting point:

- [`deploy/config.example.host-a.yml`](../deploy/config.example.host-a.yml)
- [`deploy/config.example.host-b.yml`](../deploy/config.example.host-b.yml)

They are deliberately generic (`host-a`, `host-b`, `example.com`,
`conch-lease.example.workers.dev`). Replace every placeholder.

## The shape

```yaml
host: host-a
host_network: false

arbiter:
  backend: cloudflare          # cloudflare (default) | dynamo
  url: https://conch-lease.example.workers.dev
  secret_env: CONCH_AUTH_SECRET
  on_loss: freeze              # freeze (default) | stop
  timeout: 5                   # seconds; hard deadline on every arbiter call

timing:
  ttl: 60
  interval: 3
  demote_after: 3
  promote_after: 6

disk:
  path: /var/lib/docker
  min_free_gb: 10

docker:
  socket: /var/run/docker.sock
  timeout: 30

updates:
  interval: 24h                # Go duration; "0" or "" disables
  timeout: 5

webhooks:
  promote: https://ops.example/hooks/promote
  demote: https://ops.example/hooks/demote
  evacuate: https://ops.example/hooks/evacuate
  reclaim: https://ops.example/hooks/reclaim
  alert: https://ops.example/hooks/alert

services:
  - name: web
    lease_key: web
    container: web-web
    connector:
      container: cloudflared-web
      stop_timeout: 35
      ready_url: http://127.0.0.1:2000/ready
    accessories: [web-litestream]
    warm_accessories: [web-postgres]
    health:
      url: http://web-web:8080/up
      expect: [200]
      timeout: 2
    hooks:
      promote:
        - name: restore
          image: litestream/litestream
          argv: ["restore", "-if-replica-exists", "/data/db"]
          binds: ["web-data:/data"]
          env_file: /secrets/web-restore.env
          timeout: 300
      evacuate:
        - name: final-replicate
          image: litestream/litestream
          argv: ["replicate", "-once"]
          binds: ["web-data:/data"]
          timeout: 120
    evacuation:
      auto_reclaim_after: 30m
```

## Top-level fields

| Field | Required | Default | Meaning |
|---|---|---|---|
| `host` | yes | — | Stable logical name for this host. It is the lease holder identity. Do not rename it in place. |
| `host_network` | no | `false` | Set `true` when the conch container runs with host networking. Loopback health URLs are rejected unless this is `true`. |
| `arbiter` | yes | — | Where leases live. See below. |
| `timing` | yes | see below | The four knobs that govern failover. |
| `disk` | no | `min_free_gb: 10` | Free-space guard. |
| `docker` | no | `/var/run/docker.sock`, `30s` | Docker API socket and per-call deadline. |
| `updates` | no | `24h`, `5s` | Periodic connector-image update check. |
| `webhooks` | no | — | Optional per-event JSON POSTs. |
| `services` | yes | — | One or more supervised services. |

## `arbiter`

The arbiter is the single source of truth for who may serve each service.

### Cloudflare Worker + Durable Object (default)

```yaml
arbiter:
  backend: cloudflare
  url: https://conch-lease.example.workers.dev
  secret_env: CONCH_AUTH_SECRET      # or secret_file: /run/secrets/conch
  on_loss: freeze                    # freeze (default) | stop
  timeout: 5
```

| Field | Default | Meaning |
|---|---|---|
| `backend` | `cloudflare` | `cloudflare` or `dynamo`. |
| `url` | — | The Worker URL. Required for the Cloudflare backend. |
| `secret_env` | — | Name of the environment variable holding the shared secret. |
| `secret_file` | — | Path to a file holding the shared secret. Use one or the other. |
| `on_loss` | `freeze` | What to do when the arbiter is unreachable. `freeze` keeps serving on an expired lease and alerts. `stop` stops serving and makes the arbiter's availability the fleet's. |
| `timeout` | `5` | Seconds. A hard deadline on every arbiter call. |

The Worker accepts two secrets during rotation: `CONCH_AUTH_SECRET` and
`CONCH_AUTH_SECRET_NEXT`. The rotation order is **add-next → roll hosts →
retire-old**. See [Deployment](deployment.md#secret-rotation).

### DynamoDB

```yaml
arbiter:
  backend: dynamo
  table: conch
  region: us-west-2
  credentials_file: /etc/conch/hooks/aws.env
  timeout: 0.35
```

| Field | Meaning |
|---|---|
| `table` | The DynamoDB table. One table, one region, one item for the whole fleet. |
| `region` | AWS region. |
| `credentials_file` | A file containing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, mode `0600`. |
| `endpoint` | Optional. DynamoDB Local or tests only. |

Do not mix backend fields. A config carrying both backends' fields is rejected.
The DynamoDB table and IAM user are created by
[`deploy/dynamo/bootstrap.sh`](../deploy/dynamo/bootstrap.sh).

## `timing`

Four knobs. Everything else is derived.

| Knob | Default | Question it answers |
|---|---|---|
| `ttl` | `60` | How long does a lease grant last? |
| `interval` | `3` | How often does conch check? |
| `demote_after` | `3` | Consecutive bad checks before giving up a lease. |
| `promote_after` | `6` | Consecutive good checks before claiming. |

The safety rules are enforced by validation:

- **`promote_after > demote_after`.** Quick to stop serving, slow to take over.
  A flapping link cannot ping-pong traffic.
- **`ttl` must be longer than the worst-case demotion path.** The law is
  `ttl > interval × demote_after + stop_timeout + arbiter.timeout + 1`, where
  `stop_timeout` is the service's connector stop timeout. The `+1` is slop.
- **The Cloudflare backend clamps `ttl` to `[10, 300]`.** The DynamoDB backend
  has no such clamp; its only floor is the law above.
- **The stop deadline is derived per service** from its `stop_timeout` plus
  slop. One slow-draining connector taxes only itself.

The defaults are a good starting point for a two-host fleet. If you shorten
them, re-run `conch validate` and read the [timing](failover-timing.md) page.

## `disk`

```yaml
disk:
  path: /var/lib/docker
  min_free_gb: 10
```

The path resolves **inside the conch container**, not on the host. Bind the
host's Docker data directory read-only at the same path (the deploy templates
bind `/var/lib/docker` read-only). The guard warns when free space falls below
`min_free_gb`; it does not refuse to serve. A full disk is a leading cause of
wedged Docker daemons, so treat the warning as an incident.

## `docker`

```yaml
docker:
  socket: /var/run/docker.sock
  timeout: 30
```

The Docker API socket and the per-call deadline. The default socket is correct
for the shipped container. The timeout is a hard deadline on every Docker call.

## `updates`

```yaml
updates:
  interval: 24h
  timeout: 5
```

Periodic check for newer connector images. It compares the local connector
image with the peer's and warns when they differ. `interval` is a Go duration;
`"0"` or `""` disables it. A negative interval is a validation fault. The check
is observation only: it never changes traffic.

## `webhooks`

```yaml
webhooks:
  promote: https://ops.example/hooks/promote
  demote: https://ops.example/hooks/demote
  evacuate: https://ops.example/hooks/evacuate
  reclaim: https://ops.example/hooks/reclaim
  alert: https://ops.example/hooks/alert
```

All optional. Each event is a JSON `POST`. The payload identifies the event,
the service, the host, and the decision that caused it. `alert` covers degraded
states that are not a lease transition. Webhooks are off the critical loop: a
failed POST never blocks a decision. See `internal/webhook` for the exact
payload shape.

## `services`

Each service is one supervised unit: an application container, its connector,
optional accessories, a health check, and optional hooks.

### Identity fields

`host`, a service's `name`, and its `lease_key` are load-bearing distributed
systems identifiers. Renaming any of them in a live config orphans the lease.
To rename a service or remove one, evacuate first and follow the procedure in
[Deployment](../deploy/README.md#identity-fields-are-immutable-in-place).

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Unique service name. Also the default lease key. |
| `lease_key` | no | Stable lease identity. Defaults to `name`. Leave it equal to `name` unless you have a reason. |
| `container` | yes | The application container conch starts and stops. Exact name, or a trailing `*` prefix for Kamal-versioned names. |

### `container` and Kamal-versioned names

Kamal appends a version suffix to app container names on every deploy. Use a
trailing `*` prefix so conch resolves the concrete container each check:

```yaml
services:
  - name: web
    container: "web-web-*"
    connector: { container: cloudflared-web }
    health: { url: "http://{container}:3000/up" }
```

The connector is always an exact name. It is a Kamal accessory, so its name is
stable. The `{container}` placeholder is substituted with the resolved app
container name in the health URL and in promote-hook `argv`. Only the exact
token `{container}` is substituted; near-miss casings are validation faults.

### `connector`

```yaml
connector:
  container: cloudflared-web
  stop_timeout: 35
  ready_url: http://127.0.0.1:2000/ready
```

| Field | Default | Meaning |
|---|---|---|
| `container` | — | Exact connector container name. Required. |
| `stop_timeout` | `35` | Seconds. Must be longer than the connector's own grace period. cloudflared's default is 30s. |
| `ready_url` | — | Optional readiness URL for the connector. The connector must be ready before the service is considered promoted. |

The connector must be `restart: unless-stopped`, never `always`. `always`
resurrects a connector conch deliberately stopped, which produces two live
connectors from one YAML line.

### `accessories` and `warm_accessories`

These are exact container names. No wildcards. No container may be owned by two
services.

- **`accessories`** follow the lease. They run where the service serves. Use
  them for replication shippers: a Litestream sidecar, a WAL shipper, anything
  that must not run on both hosts at once. A shipper on the standby corrupts the
  replica's generation.
- **`warm_accessories`** run on **every** host, whatever the lease says. Use
  them for restore targets: a database that a promotion restores into. A warm
  database on the standby is a target, not a serving copy. Do not put a shipper
  here.

If a container is a shipper, it belongs under `accessories`. If it is a restore
target, it belongs under `warm_accessories`. Getting this backwards is the
fastest way to corrupt a replica.

### `health`

```yaml
health:
  url: http://web-web:8080/up
  expect: [200]
  timeout: 2
  host: app.internal.example       # optional Host header
```

| Field | Default | Meaning |
|---|---|---|
| `url` | — | Required. The readiness probe. Resolves inside the conch container. |
| `expect` | `[200]` | Accepted HTTP status codes. |
| `timeout` | `2` | Seconds. |
| `host` | — | Optional `Host` header. Use it when the probe must traverse a shared reverse proxy. |

The URL is resolved from inside the conch container. For a container on the
same Docker network, use its service name. For a loopback URL, set
`host_network: true` and run the conch container with host networking. The
`{container}` placeholder is allowed in the URL.

### `hooks`

Hooks are one-shot containers run on promotion or evacuation. They are how you
restore a database, run a migration, or register a freshly restored container
with a reverse proxy.

```yaml
hooks:
  promote:
    - name: restore
      image: litestream/litestream
      argv: ["restore", "-if-replica-exists", "/data/db"]
      binds: ["web-data:/data"]
      env_file: /secrets/web-restore.env
      network: web-net
      timeout: 300
  evacuate:
    - name: final-replicate
      image: litestream/litestream
      argv: ["replicate", "-once"]
      binds: ["web-data:/data"]
      timeout: 120
```

| Field | Meaning |
|---|---|
| `name` | Step name. Shown in logs and status. |
| `image` | Container image. |
| `argv` | Command and arguments. |
| `binds` | Docker volume binds. |
| `env_file` | Environment file path, resolved inside the conch container. |
| `network` | Docker network. |
| `timeout` | Seconds. Default `300`. |

Promote hooks run before the connector starts. Evacuate hooks run before the
connector stops. The `{container}` placeholder is substituted in promote-hook
`argv`; it is **not** substituted in evacuate hooks. A failed hook fails the
promotion or evacuation; conch backs off and retries on the next check.

### `evacuation`

```yaml
evacuation:
  auto_reclaim_after: 30m
```

A Go duration. When set, the evacuation bar self-expires after that duration, so
a forgotten evacuation does not last forever. The default is empty: never
auto-reclaim. Use `conch evacuate --hold` when you are tearing a host down and
do not want an automatic reclaim in the middle of surgery.

## Validation rules worth knowing

- `host` must be set.
- At least one service is required.
- Service names and lease keys must be unique.
- No container may be owned by two services.
- A connector container may not double as a service container or an accessory.
- A container may not be both an accessory and a warm accessory.
- `promote_after` must be greater than `demote_after`.
- The `ttl` law above must hold for every service.
- The Cloudflare backend rejects a `ttl` outside `[10, 300]`.
- The DynamoDB backend rejects any Cloudflare-only field, and vice versa.
- The arbiter accepts at most 64 services in one batch. Keep the fleet at or
  below that; a larger config is rejected by the arbiter.
- Loopback health URLs require `host_network: true`.
- A trailing `*` is allowed only on the service `container`, not on connectors,
  accessories, or warm accessories.

## Common mistakes

- **`restart: always` on a connector.** It resurrects a connector conch stopped
  on purpose. Use `unless-stopped`.
- **A database under `accessories`.** It follows the lease and disappears from
  the standby. Use `warm_accessories` for restore targets.
- **A shipper under `warm_accessories`.** It runs on both hosts and corrupts the
  replica. Use `accessories`.
- **`disk.path` without the read-only bind.** The path resolves inside the
  container. If it is not bound, the guard sees a missing path and warns.
- **Renaming `host`, `name`, or `lease_key` in place.** Evacuate first. See
  [Deployment](../deploy/README.md#identity-fields-are-immutable-in-place).
- **Using the same secret on a host you do not control.** The arbiter secret is
  the fence. Treat it as a credential.
