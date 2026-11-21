# Arbiter backends

The arbiter holds the leases. There are two backends, selected by
`arbiter.backend`.

## Cloudflare Worker + Durable Object (default)

The default. A Cloudflare Worker with one Durable Object holds every lease. It
is the smallest deployment: one Worker, one DO, one secret. The free tier is
enough for a two-host fleet.

```yaml
arbiter:
  backend: cloudflare          # default; can be omitted
  url: https://conch-lease.example.workers.dev
  secret_env: CONCH_AUTH_SECRET
  on_loss: freeze
```

Two ceilings come with the free tier:

- A combined ~100k/day request meter. Every host checks in every interval; two
  hosts at 3s is ~57k requests/day. A third host or a tighter interval spends
  the meter faster.
- A `ttl` floor clamped to `[10, 300]`. Validation enforces the floor.

The Worker accepts two secrets during rotation: `CONCH_AUTH_SECRET` and
`CONCH_AUTH_SECRET_NEXT`. See [Deployment](deployment.md#secret-rotation).

Read [`arbiter/README.md`](../arbiter/README.md) for the wire protocol and the
Durable Object details.

## DynamoDB

A second backend talks to DynamoDB directly. There is no Worker and no middle
tier; conch signs requests with SigV4. It exists for two ceilings the Worker/DO
free tier imposes and DynamoDB's always-free tier does not: the request meter and
the `ttl` floor.

```yaml
arbiter:
  backend: dynamo
  table: conch
  region: us-west-2
  credentials_file: /etc/conch/hooks/aws.env   # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, mode 0600
  timeout: 0.35
```

One table, one region, no sharding. The whole fleet's lease state lives in one
DynamoDB item. The `credentials_file` must be mode `0600` and must not be in
version control.

Bootstrap the table and IAM user with one idempotent script:

```sh
deploy/dynamo/bootstrap.sh
```

The script prints the access key and secret once. Put them in the credentials
file on each host. The cutover playbook for moving a live fleet between backends
is in [`deploy/README.md`](../deploy/README.md). The one invariant: **the
standby's conch stops, in full, before either host's backend changes.** Split
backend operation is not safe.

### Capacity guard

DynamoDB bills the whole fleet's lease state as one item. The capacity guard
warns when the config's request rate would exceed the always-free tier. It
recommends; it does not limit. If you provisioned capacity, ignore it. If you
are on the free tier, do not.

### Timing

The DynamoDB backend's only `ttl` floor is the existing law. The reference
config uses much tighter timing:

```yaml
timing:
  interval: 0.5
  ttl: 6
  demote_after: 2
  promote_after: 3
```

Read [Failover timing](failover-timing.md) before changing it. Tighter timing is
a different risk profile, not a free lunch.

## Choosing

| | Cloudflare Worker + DO | DynamoDB |
|---|---|---|
| Middle tier | One Worker | None |
| Free tier | ~100k requests/day, `ttl` 10–300 | Always-free tier, no request meter |
| Secret | Worker secret | AWS credentials file |
| Cutover | Redeploy the Worker | Bootstrap table and IAM, change config |
| Best for | The default two-host fleet | Fleets that need tighter timing or more requests |

If you are unsure, use the default. The DynamoDB backend is a deliberate
alternative, not a required upgrade.
