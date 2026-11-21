# Deploying conch

This is the deployment runbook: how to get conch running on your hosts via
Kamal, how to set up and rotate its arbiter secret, and how to run planned
maintenance without a failover. It says nothing about what conch is or why
it exists — that's the project README. It only covers deployment.

Every example here uses generic names (`web`, `blog`, `host-a`, `host-b`,
`example.com`) — copy and rename, never paste your own hostnames or tokens
into a commit.

## Install

conch is a Kamal accessory: one container per host, driven by a YAML config
file you write and a shared secret it uses to talk to the arbiter (the
Cloudflare Worker in `arbiter/`). Two starting points, depending on how many
services you're running:

### Path 1 — one service, quick start

Use [`accessory-quick-start.yml`](./accessory-quick-start.yml) if you want the connector
readiness check working from the start, with the fewest moving parts to get it there:
host networking on both conch and cloudflared makes `127.0.0.1` a legal health URL and puts
cloudflared's metrics port directly in reach. It is **not** the more turnkey option for a
typical app fronted by kamal-proxy, though — it requires that app to also publish its health
port on the host loopback, which is extra plumbing most Kamal apps don't otherwise need. If your
app already answers health checks over the ordinary docker network (the common case), skip
straight to Path 2, below — it needs no changes to how your app is deployed at all.

Steps:

1. Use the published image (`ghcr.io/scytherswings/conch:latest`) or build your
   own: `docker build -t <your-registry>/conch:latest .` — see
   [`Dockerfile`](../Dockerfile). It's a scratch image; nothing but the
   binary and CA certs.
2. Copy [`config.example.host-a.yml`](./config.example.host-a.yml) to your Kamal project as
   `config/conch.yml`, add `host_network: true`, and change every `http://web-web:8080/up`
   style URL to `http://127.0.0.1:<port>/up` — the comment block at the top of
   `accessory-quick-start.yml` shows the resulting shape.
3. Paste the `accessories:` block from `accessory-quick-start.yml` into your Kamal
   `config/deploy.yml`, filling in your own image tag.
4. Add `CONCH_AUTH_SECRET` and `TUNNEL_TOKEN` to `.kamal/secrets` (see **Secrets**, below).
5. Install the pre-deploy hook (see **Validate before you deploy**, below).
6. `kamal accessory boot conch` and `kamal accessory boot cloudflared-web` (one name per
   invocation; `NAME=all` boots every accessory if you'd rather do both, plus anything else
   defined, at once).
7. Repeat steps 2–6 for your second host, changing `host:` in both the config and the
   accessory block, and renaming both accessory keys (`conch` → `conch-host-b`,
   `cloudflared-web` → `cloudflared-web-host-b`) — a deploy.yml's `accessories:` map has one
   entry per key, so a second unrenamed `conch:` block silently overwrites the first instead
   of adding a second host. conch is designed and tested for two hosts — don't run
   it as a single-host tool. This gets you two hosts on host networking with the readiness
   check wired up — [`conch-accessory.template.yml`](./conch-accessory.template.yml) (Path 2)
   is a different steady-state shape (docker-network DNS, no readiness check, room for more
   than one service), not this one scaled up; move to it deliberately, not by accretion, if
   you want that shape instead.

### Path 2 — multiple services, per host

Use [`conch-accessory.template.yml`](./conch-accessory.template.yml) once you have more
than one service, or once Path 1 is working and you want the steady-state shape. It mounts
[`config.example.host-a.yml`](./config.example.host-a.yml) and
[`config.example.host-b.yml`](./config.example.host-b.yml) directly — copy those two files
into your Kamal project, edit the `services:` list to match your real services, and paste the
`accessories:` block into `config/deploy.yml`. This path uses docker-network DNS names
instead of host networking (`http://web-web:8080/up`, resolved by Docker's own DNS on the
network Kamal already puts every accessory and deployed service on) — no `network: host`
anywhere, and no loopback addresses to get wrong.

### Kamal-deployed app containers (versioned names)

Kamal names an app's own container after the image it's running, and that name changes on
every deploy (`web-web-3cb82b9b...`, a different suffix on the next one) — the ordinary
`container:` field is an exact match, so a Kamal-deployed app container can never stay eligible
past its first redeploy. Write `container:` with a trailing `*` instead, and use `{container}`
in `health.url`, and conch resolves the live container fresh every check:

```yaml
services:
  - name: web
    container: "myapp-web-*"                 # prefix match, not exact
    connector: { container: cloudflared-web }
    health: { url: "http://{container}:3000/up" }  # substitutes the resolved name
```

`connector.container` stays an exact name always — cloudflared (or whatever fronts the
service) is a Kamal *accessory*, not something Kamal versions on deploy, so its name is
stable. Only the app container itself needs the prefix.

### Service accessories (replication sidecars)

A stateful service usually runs a sidecar container that must live on **exactly** the host
currently serving it — a litestream continuous replicator above all: two replicators on two
hosts writing one bucket path corrupt the replica's generation history, and a standby
replicating a stale file is worse than none. List such containers under the service's
`accessories:` key and conch reconciles them exactly like the connector — including on
**adoption** (a conch restart or fast reboot that finds the lease still its own), where
hooks never run and a hook-started sidecar would stay silently down:

```yaml
services:
  - name: web
    container: web-web
    connector: { container: cloudflared-web }
    accessories: [web-litestream]      # hand-created or Kamal-booted; exact names only
```

What conch does with them:

- **On promotion**: started after the service container and before the connector —
  replication is up before traffic arrives.
- **On every ACTIVE check**: any configured accessory found stopped is started again,
  the same heal a crashed connector gets. This is what closes the adoption gap.
- **Wherever the service does not serve** (steady standby, demotion, refusal, suspend,
  the stop-mode deadline): any accessory found running is stopped, with a 30s grace so a
  replicator can flush its last segments on SIGTERM — Docker's 10s default is too tight.
- **Eligibility**: every configured accessory container must exist on the host, or the
  host cannot claim the service — a host that cannot replicate must not win a promotion.
- **`conch status`** reports `"accessories": {"running": N, "total": M}` per service.

Rules: exact container names only — accessories are hand-created or Kamal-booted, never
versioned, so `*` patterns are a validation fault — and **one owner per container**: an
accessory name may not double as any service's `container:`, any `connector.container`, or
another service's accessory. Two reconciles fighting over one container would start and
stop it once per interval, forever. Like connectors, accessory containers should be
`restart: unless-stopped`, never `always`, so a reboot cannot resurrect a replicator conch
deliberately stopped on the standby.

**Don't run a Kamal deploy of the app while a promotion or failover of that app is in
flight.** conch resolves the prefix fresh every check, and during blue/green overlap (old and
new containers briefly both present) resolution can pick differently between the check that
stops a container and the later check that starts one — deliberately avoid stacking the two.

Both paths need two binds on the conch accessory, and both templates already carry them:

- `/var/run/docker.sock:/var/run/docker.sock` (read-write) — conch drives Docker directly:
  starting and stopping the service and connector containers, and running its hook containers.
- `/var/lib/docker:/var/lib/docker:ro` — the config's `disk.path` resolves *inside* the conch
  container (the namespace-honesty rule: paths in the config resolve inside the conch container),
  and the shipped image is a scratch image with nothing at that path unless it's bound in. Skip this bind and the free-space check fails
  every check, forever, on both hosts — host health reads unhealthy permanently, and nothing can
  ever promote. This is easy to miss because nothing about it looks urgent until the first
  failover never happens.

### Warm accessories (databases and other restore targets)

A service's own database is the opposite case, and it gets the opposite rule. List it under
`warm_accessories:` and conch runs it on **every** host, whatever the lease says — started
wherever it is found stopped, in any phase, on both hosts, adoption included, and **never
stopped by conch anywhere**:

```yaml
services:
  - name: web
    container: web-web
    connector: { container: cloudflared-web }
    accessories:      [web-litestream]   # follows the lease: runs only where the service serves
    warm_accessories: [web-postgres]     # runs on every host, whatever the lease says
```

Why that is safe when the sidecar rule above says the exact opposite: a regular accessory is
a single-**writer** shipper, and two shippers on two hosts writing one replica corrupt its
generation history. A warm accessory is a **restore target** — the standby's copy is stale,
fenced-off local state that promotion overwrites wholesale (a `pg_restore`, an `rclone sync`)
before anything serves out of it. The "no second writer on the replica" invariant binds the
shippers; it says nothing about a database nobody is shipping to and nobody is serving from.

What it buys is the failure this field exists to end: **a deploy against the standby fails
when the database it builds against is not running there.** A stopped database on the standby
is not a quiet cost — it breaks every deploy, asset build and migration dry-run aimed at that
host, and the breakage only shows up when someone needs the standby most.

- **Eligibility**: like a regular accessory, every warm accessory container must exist on the
  host, or that host cannot claim the service. A host missing its database must not promote.
- **`conch status`** counts them in the same row, with their own pair of keys:
  `"accessories": {"running": 1, "total": 2, "warm_running": 1, "warm_total": 1}`. On a
  standby that is the *healthy* reading — the shipper is down because it should be, and only
  `warm_running` against `warm_total` says whether the database is up.
- **Same rules otherwise**: exact container names only (a `*` is a validation fault), and one
  owner per container. A container that is warm in one place and lease-following in another is
  refused outright — one reconcile would start it on every host while the other stopped it
  wherever that service does not serve, once per interval, forever.

### Fleet inventory

"Which sites depend on conch?" is not answered by a list in this repo, and it
shouldn't be — that list would go stale the first time the fleet changes (a
renamed or removed service, a site moved to active-active), and the real
answers live a repo or two away anyway. The running fleet answers for itself:
[`deploy/inventory`](./inventory) reads `conch status` out of each host's conch
container and prints what to depend on:

```sh
deploy/inventory you@host-a you@host-b              # auto-finds each host's conch container
deploy/inventory host-a:conch-host-a                # or name it explicitly
```

For every host it prints the services that host supervises with phase and
lease, the peer map, and any `last_stops` evidence. A service supervised on any
host is a service that depends on conch; a site absent from every host's
status is not supervised and may well be served active-active with no conch
involvement. A host that is unreachable shows up as **UNREACHABLE** rather than
vanishing from the output — a box that is genuinely down is a finding, not an
absence. Keep hostnames out of the repo (CONTRIBUTING) and pass them on the
command line, exactly as everything else in this runbook does.

### Validate before you deploy

Install [`hooks/pre-deploy-validate`](./hooks/pre-deploy-validate) as `.kamal/hooks/pre-deploy`
in your Kamal project — Kamal only recognizes that exact filename, with no extension.
Copy it in:

```sh
cp deploy/hooks/pre-deploy-validate .kamal/hooks/pre-deploy
chmod +x .kamal/hooks/pre-deploy
```

(Or symlink it instead, if this conch checkout lives inside your Kamal project — e.g.
`ln -s ../../vendor/conch/deploy/hooks/pre-deploy-validate .kamal/hooks/pre-deploy` — so
future changes here stay in sync automatically. The relative path has to match your own
layout; the copy above always works regardless of where this repo sits relative to your
Kamal project.)

It runs `conch validate` against your mounted config, using the same image the deploy is about
to ship, and a non-zero exit aborts the deploy before anything reaches a host. Set `CONCH_IMAGE`
in your shell (or your CI environment) to the image:tag you're deploying — the hook refuses to
run without it rather than silently validating against the wrong image. `conch validate` also
runs standalone, any time: `conch validate --config path/to/conch.yml`, exit 0 clean, exit 1 with
every fault listed (not just the first one).

## Secrets

conch authenticates to the arbiter with a shared secret, sent as the `X-Conch-Auth` header on
every batch request. The config's `arbiter.secret_env` (or `arbiter.secret_file`) names where to
find it; the Kamal side is an ordinary Kamal secret, resolved from `.kamal/secrets`:

```
# .kamal/secrets
CONCH_AUTH_SECRET=$(op read "op://vault/conch-lease/CONCH_AUTH_SECRET")
TUNNEL_TOKEN_WEB=$(op read "op://vault/tunnels/web/token")
TUNNEL_TOKEN_BLOG=$(op read "op://vault/tunnels/blog/token")
```

(Any secret-fetching command works here — `.kamal/secrets` just needs to end up with the right
environment variables set when Kamal reads it. Never commit the resolved values.)

The arbiter itself holds the accepted secret(s) as Cloudflare Worker secrets, set with
`wrangler`, from `arbiter/`:

```sh
wrangler secret put CONCH_AUTH_SECRET
```

### Rotation

The arbiter accepts two secrets at once — `CONCH_AUTH_SECRET` and, when set,
`CONCH_AUTH_SECRET_NEXT` — for exactly this reason: a naive single-secret flip is a
self-inflicted fleet outage, because it can't be simultaneous across hosts and every host on the
old secret starts reading as unauthorized the instant the arbiter is updated. Rotate as
add-next → roll-hosts → retire-old, and there is never a window where any host gets refused:

1. **add-next** — generate a new secret value and set it as the arbiter's second accepted
   secret. Nothing on any host has changed yet; both the old and the new value now authenticate.

   ```sh
   wrangler secret put CONCH_AUTH_SECRET_NEXT
   ```

2. **roll hosts** — update `.kamal/secrets` so `CONCH_AUTH_SECRET` resolves to the new value,
   then redeploy the conch accessory on one host at a time. Each host's conch is its own
   accessory, already pinned to that one host (`conch-host-a`, `conch-host-b` in
   `conch-accessory.template.yml`), so rebooting by name already scopes to one host:

   ```sh
   kamal accessory reboot conch-host-a
   # confirm it's healthy (kamal accessory exec --reuse conch-host-a "conch status"), then:
   kamal accessory reboot conch-host-b
   ```

   A host that has rolled sends the new value, matched by `CONCH_AUTH_SECRET_NEXT`. A host that
   hasn't rolled yet still sends the old value, matched by `CONCH_AUTH_SECRET`, unchanged. Either
   order, either state, every host is authenticated throughout.

3. **retire-old** — once every host is confirmed on the new secret, promote it to primary and
   drop the old one:

   ```sh
   wrangler secret put CONCH_AUTH_SECRET        # same new value again, now as primary
   wrangler secret delete CONCH_AUTH_SECRET_NEXT
   ```

   From this point the old secret value no longer authenticates anything.

## Planned maintenance

**Never just reboot a host with services on it.** SIGTERM does nothing to the world by design
— conch exits with leases and containers untouched, because there is no way for a
signal alone to tell "Kamal is redeploying conch" apart from "the host is going down," and
treating SIGTERM as a move-everything-off trigger would turn every conch deploy into a full
failover. A bare `reboot` on a host serving something is the ungraceful case: it works, but it's
the failure path, not the maintenance path, and it costs whatever's in the replication-lag window.

Planned maintenance is, run against the accessory for the specific host you're taking down
(`conch-host-a`, in these examples — substitute whichever one is actually pinned to that host;
`kamal accessory exec [NAME]` runs on every host that accessory NAME spans, so naming the
single-host-pinned accessory, not a shared multi-host one, is what keeps this targeted):

```sh
kamal accessory exec --reuse conch-host-a "conch evacuate --all"
# wait for it to exit 0, then, on that host:
reboot
```

Naming the host-pinned accessory scopes the *container* work to that one host,
but Kamal still opens channels to every server in the project's `servers:`
list, so if a peer host is down or unreachable the command fails up front
(`Network is unreachable`) before touching anything. When a host is down, add
`--hosts <ip>` to keep the command to the boxes that can answer:

```sh
kamal accessory reboot conch-host-a --hosts 203.0.113.10
```

`conch evacuate <service...|--all>` is the deliberate move. The supervisor owns
the entire sequence — evacuate hooks (drain, final replication sync) run, the connector stops,
the lease releases with a bar so nothing walks back in mid-surgery, and only then does the
command return — the CLI itself just asks and watches, so a dropped SSH session mid-evacuation
can't leave a host half-evacuated with nothing driving it. It exits non-zero on timeout or
refusal, in which case **don't reboot**: read what it printed, fix whatever's wrong (usually
"no peer has checked in recently" — evacuating into a void would strand the service barred with
nowhere to go, and it refuses by default; `--force` overrides for "I really do want it down
anyway"), and retry.

### The `--reuse` caveat

`kamal accessory exec` by default starts a **fresh** container to run the command in — no admin
socket, no running supervisor, nothing to evacuate. `--reuse` is what runs the command inside the
container that's actually serving:

```sh
kamal accessory exec --reuse conch-host-a "conch status"
kamal accessory exec --reuse conch-host-a "conch evacuate web"
kamal accessory exec --reuse conch-host-a "conch reclaim web"
```

Forgetting `--reuse` doesn't fail loudly — it starts a second, throwaway conch process with
nothing to talk to over the socket, which is a confusing way to learn this. `docker exec conch
conch status` reaches the same place more directly if you're already on the host — the binary
lives at `/conch` (the entrypoint), and its container is named `conch` when the accessory sets
`service: conch`, as both templates do. A project that overrides `service:` names its container
after that (`conch-host-a` under service `conch` may become `<app>-conch-a` under service
`<app>`), so the safe spelling that works under any name is `docker exec <name> /conch
status` — or let [`deploy/inventory`](./inventory) find the name for you.

## The replication-sidecar warning

**conch orchestrates restores; it never performs backups.** Continuous replication — a
Litestream sidecar, `pg_receivewal`, whatever your service uses — is part of the service's own
deployment and runs the whole time the service serves, not something conch starts.

The trap: Kamal starts your app's containers on every host at every deploy, including standbys.
A replication sidecar deployed "alongside the app," the ordinary Kamal way, runs on the standby
too — and now two writers are pushing into one replication target, which corrupts exactly the
copy a failover would need to restore from. This is silent until the moment you actually fail
over, at which point it's too late to notice.

Ship the sidecar so it follows the service container conch itself manages — same start/stop
fate, not a separate always-on Kamal role — or make the sidecar refuse to run at all against a
database whose service isn't serving locally right now. Either is sufficient; deploying it
unconditionally alongside the app is not.

## Identity fields are immutable in place

`host`, a service's `name`, and its `lease_key` are load-bearing distributed-systems
identifiers, not labels. Renaming any of them in a live config and redeploying
orphans the lease: the arbiter now tracks a different key than the one the running connector
is serving under, and the "new" identity starts claiming a "new," empty lease from scratch —
while the old connector, unreferenced by anything in the new config, may still be sitting there
serving. That's dual-serve with every indicator green, and it's exactly the outcome the whole
lock design exists to prevent.

**Renames and service removals are done by evacuating first**, never by editing the identity
field in place:

1. Find which host currently holds the service (`conch status` on either host shows it — or
   just try both; evacuating a host that doesn't hold the lease is a safe no-op, "not held
   here: nothing to evacuate"), then evacuate it there, with `--hold` so nothing reclaims the
   old identity mid-edit:

   Flags before the service name — conch's flag parser (Go's stdlib `flag`) stops reading
   flags at the first positional argument, so `conch evacuate web --hold` silently treats
   `--hold` as a second service name, not a flag:

   ```sh
   kamal accessory exec --reuse conch-host-a "conch evacuate --hold web"
   ```

   Confirm it exits 0 — this releases the lease and confirms the connector is stopped.

2. Edit the identity fields (`name`, `lease_key`, `container`, `connector.container`, or
   `host`) in the config on both hosts, and update the matching Kamal accessory block(s) to
   reference the new container name(s).

3. Deploy the new config to both hosts (`kamal accessory reboot conch-host-a` and
   `conch-host-b`, or a full deploy if the accessory image changed too).

4. **Clean up the old containers.** conch now automates the connector half of this: the
   `conch.service` label sweep (`internal/supervisor/sweep.go`) runs every check and
   stops any *running* container whose `conch.service` label names a service this host's config
   no longer has, warning once and sending one alert webhook as it does. So an old connector
   that carries the label goes down on its own within an interval of the new config being live,
   on every host, without anyone typing anything. A container the config still names is never
   stopped by it, whatever its label says — if that label names nothing, conch warns that it is
   inert and leaves the container to the service's own reconcile.

   Two halves of the job it does not do. It only ever **stops** — the container and its Kamal
   accessory are still there — so removing them is still yours:

   ```sh
   docker stop cloudflared-web-old && docker rm cloudflared-web-old
   # or, if it was its own Kamal accessory:
   kamal accessory remove <old-accessory-name>
   ```

   And it acts on the label alone, so it touches **only** labeled connectors. An old *app*
   container left behind by a rename is Kamal's to remove (`kamal app remove` / a deploy of the
   new name), never conch's — conch never removes a container, and never stops one it was not
   asked to supervise or told about by a label.

5. Bring the new identity up through the ordinary front door — it's not a reclaim (`conch
   reclaim` only clears a bar on a lease_key that already exists; a renamed identity is a brand
   new key to the arbiter, with no bar and no history). Once step 3's deploy is live on both
   hosts, they see a free, unbarred lease under the new key and promote through the normal
   `promote_after` green-check path.

If you're only removing a service, stop after step 1 (evacuate, don't reclaim) and delete its
block from the config on both hosts before step 3 — then step 4 still applies to whatever
connector container it left behind.
