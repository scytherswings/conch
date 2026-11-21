# conch documentation

Start with the [main README](../README.md) for the one-paragraph pitch and the
quickest path to a running supervisor. These pages are the long version.

## Read in this order

1. **[Quick start](quickstart.md)** — deploy the arbiter, build conch, validate a
   config, and run a dry run against your own services.
2. **[Configuration](configuration.md)** — every field, every default, and the
   safety rules `conch validate` enforces.
3. **[Deployment](deployment.md)** — the Kamal accessory paths, the two Docker
   binds, secret rotation, and the pre-deploy hook.
4. **[Operations](operations.md)** — day-2 commands: status, evacuate, reclaim,
   fleet inventory, upgrades, and planned maintenance.
5. **[Architecture](architecture.md)** — the lease model, the pure decision
   function, and the invariants that keep two hosts from serving at once.
6. **[Failover timing](failover-timing.md)** — the default timeline and how to
   measure your own.
7. **[Known gaps](known-gaps.md)** — the limitations. Read this before you trust
   conch with anything you cannot lose.
8. **[Arbiter backends](backends.md)** — Cloudflare Worker + Durable Object, or
   DynamoDB.
9. **[Security](security.md)** — trust boundaries and operator hardening.
10. **[Troubleshooting](troubleshooting.md)** — symptoms and first checks.
11. **[Development](development.md)** — build, test, and the load-bearing
    constraints for contributors.

## Examples

The [examples](../examples/README.md) directory collects copyable configs and
deployment templates. The shipped `deploy/` directory is the canonical source
for the Kamal accessory templates and the host example configs.

## The short version

- conch is designed and tested for **two hosts**. It is not a single-host tool,
  and it is not a load balancer.
- Exactly one host may serve each service. The arbiter holds one lease per
  service; the lease holder runs the connector, the other host waits.
- A dead or stuck conch with a healthy connector can still mean both hosts
  serve. That is the first [known gap](known-gaps.md), and no amount of
  configuration closes it completely.
- `promote_after > demote_after` is enforced. Quick to stop serving, slow to
  take over.
- The decision function is pure. Everything effectful is an action it returns,
  which is what makes `--dry-run` mean "do not perform the actions" rather than
  "audit every call site".
