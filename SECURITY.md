# Security

conch is a failover supervisor; it holds leases that gate who may serve each
service. A compromise of the things it *trusts* (its config, its secrets, the
arbiter) can deliberately misroute or dual-serve traffic. Please read this
before reporting.

## Supported versions

Only the latest `main` and the current tagged release receive security fixes.
Backports to older tags are made on a best-effort basis, since the fix usually
touches the lease/decide core that differs a lot between releases.

## Reporting a vulnerability

**Do not open a public issue.** Cloudflare Tunnel failover tooling touches the
data-path between real users and their apps, and the details of a working
exploit are worth keeping private for a coordinated window.

Report privately to the maintainers:

- Use [GitHub private vulnerability
  reporting](https://github.com/scytherswings/conch/security/advisories/new)
  (preferred);
- or contact the maintainer on GitHub at [@scytherswings](https://github.com/scytherswings).

Please include: the affected component (`conch` supervisor, `arbiter` Worker,
`deploy` tooling), the conch and arbiter versions, the minimal reproduction, and
why you believe it is a security issue rather than an availability or
correctness bug in a normally-configured deployment.

## What counts as a vulnerability here

These are the trust boundaries, and a break in one is a bug:

- **The config → supervisor trust.** Anything reachable from the config file
  (hook `image:`/`argv:`, hook `binds:`, `env_file:` paths) runs with the
  privileges of the conch container and the Docker socket it carries. A change
  that widens what a hostile or untrusted config can execute is reportable.
- **The arbiter auth secret.** conch authenticates to the arbiter with the
  `X-Conch-Auth` header. A way to recover, forge, or bypass that secret, or to
  make the arbiter accept requests without it, is reportable.
- **The lease invariants.** Two connectors for one service live at once is the
  one outcome that corrupts data. Anything that lets two hosts both serve a
  lease they should not — including the failure modes the README's *Known
  gaps* section already discloses — sits at the core.
- **Leaked handling of secrets or local state** in hooks, logs, or the Docker
  socket client.

## What is *not* a vulnerability

These are disclosed, documented behaviours. The [known gaps](docs/known-gaps.md)
page calls them out by name. They are the cost of the design, not defects:

- A **dead or stuck conch with a healthy connector** allows brief dual-serve
  until the lease expires and reconcile stops it.
- **Arbiter unavailability** under the default `on_loss: freeze` holds the
  fleet in place rather than stopping it.
- A **wedged Docker daemon** that cannot actually stop a container.

## Security hardening notes for operators

- Run conch with the minimal container privileges the shipped Dockerfile gives
  it (a `scratch` image running one binary), and never share its
  `/var/run/docker.sock` bind with untrusted workloads.
- Keep `arbiter.secret_env` / `arbiter.secret_file` and the DynamoDB
  `credentials_file` mode `0600` and out of version control. Rotate
  `CONCH_AUTH_SECRET` per the *Rotation* section in `deploy/README.md`.
- Hooks run as root inside the conch container by design (they need the Docker
  socket); review every `image:`, `argv:`, `binds:` and `env_file:` in your
  config as the boundary it is.
- Pin the conch image (`<registry>/conch:<sha256>`) in your Kamal accessory so
  a redeploy cannot silently pick up a newer, untested build.