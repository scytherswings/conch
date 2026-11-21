# conch arbiter

The lock. A Cloudflare Worker in front of **one** Durable Object holding a table of
`lease_key → {holder, instance, epoch, expires_at, serving, evacuated_by}`, plus each host's last
check-in time. No build step, no framework, two source files.

Every lease lives in one object on purpose: three free allowances force it (Workers requests and DO
requests at 100k/day each, and DO compute duration at 13k GB-s/day, where one always-awake object
already reads as ~85% of the allowance under the pessimistic interpretation). Per-service objects
would multiply both. See [Architecture](../docs/architecture.md) and
[Arbiter backends](../docs/backends.md).

## The protocol

One route matters: **`POST /batch`** — one request per host per check, carrying every service and
its op (`renew`, `read`, `release`); one response carrying a verdict per service, plus `peers` and
`fingerprints`. `GET /health` answers `{"ok": true}` and needs no auth.

The wire format, the three ops, the evacuation bar, lazy expiry and the duplicate-host rule are
the arbiter's contract — there is no schema validator, and the Go client in
`../internal/arbiter/client.go` is the other half of it. Read them side by side before
changing either. `ttl` is silently clamped to `[TTL_MIN, TTL_MAX]` = `[10, 300]` (`src/lease.js`);
`conch validate` rejects a `timing.ttl` outside that range so a host never runs hysteresis math
against a ttl the arbiter didn't actually grant.

Properties the tests pin, because each one's absence is a specific outage: the batch is atomic (one
`Date.now()`, at most one storage write); expiry is lazy, so the read that discovers an expired
lease is the cleanup; a malformed entry poisons only itself; `release` is refused unless the
requester is the current holder; and an instance-id flip-flop within one TTL refuses **both** hosts
with `duplicate_host` rather than letting two supervisors sharing a `host:` value both read "yours".

Storage is bounded, so no authenticated caller can grow the one object every lease depends on
without limit: at most `MAX_LEASES` service records may exist (a `renew` that would create a brand
new key past the ceiling is refused `at_capacity` — the client fails closed on it, holding
nothing), a host reports at most `MAX_HOST_IMAGES` images, a `release` of a never-claimed key mints
no record, and hosts silent past an hour are forgotten. The ceiling is far above any real fleet;
it only refuses pathological keys.

## Deploy

```sh
npm install
npm run deploy          # wrangler deploy
npm test                # vitest, via the Workers pool (miniflare)
```

`wrangler.toml` names the Worker `conch-lease` with `workers_dev = true`, binds the `LEASE` Durable
Object, and declares the SQLite migration for it. Nothing else is configured.

## Secrets

Two accepted secrets, both Worker secrets, compared constant-time against the `X-Conch-Auth` header:

```sh
npx wrangler secret put CONCH_AUTH_SECRET        # required; the Worker 500s without it
npx wrangler secret put CONCH_AUTH_SECRET_NEXT   # optional; only exists for rotation
```

**Rotation order is add-next → roll hosts → retire-old**, and it exists because a single-secret flip
cannot be simultaneous across hosts: every host still on the old value would start reading 401 the
instant the Worker was updated, which is a self-inflicted fleet outage at the stop deadlines. The
full procedure, with the Kamal side, is in [`../deploy/README.md`](../deploy/README.md#rotation).

## Two operational notes

**The free-plan meters are account-wide.** Any other Worker on the same account spends conch's
budget, and unauthenticated junk sent to a guessable `workers.dev` URL costs a Worker request each
time even though it is rejected. Run the arbiter in a **dedicated Cloudflare account**, or at least
behind a non-guessable name; the $5 Workers Paid plan removes the ceiling entirely. If a meter does
exhaust, the Worker errors — and under the default `arbiter.on_loss: freeze` the fleet freezes in
place and yells until the daily reset rather than taking itself down.

**`compatibility_date` is ahead of the test runtime.** `wrangler.toml` pins a date newer than the
Workers runtime bundled with the installed `@cloudflare/vitest-pool-workers`, so every `npm test`
run prints a warning and **falls back to an older date**. The suite therefore does not exercise the
compatibility date that gets deployed. It is harmless for this code today — nothing here depends on
dated behaviour — but if a test ever disagrees with production, this is the first thing to check.
