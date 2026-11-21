# Security

conch is a failover supervisor. It holds leases that gate who may serve each
service. A compromise of the things it trusts can misroute or dual-serve
traffic. This page is the operator hardening guide; the vulnerability reporting
policy is in [SECURITY.md](../SECURITY.md).

## Trust boundaries

Treat these as code:

1. **The config.** Hooks can run arbitrary containers with the Docker socket.
   `binds`, `env_file`, and `argv` are all under the config's control. A hostile
   config is game over. Review config changes like code changes.
2. **The shared secret.** The arbiter authenticates every request with the
   `X-Conch-Auth` header. Anyone with the secret can claim leases. Treat it as a
   credential.
3. **The Docker socket.** The conch container mounts the host's Docker socket
   read-write. A compromise of conch is effectively root on the host. Pin the
   image by digest and limit who can change the config.
4. **The arbiter.** A compromised Worker or DynamoDB table can lie about leases.
   Run the arbiter in a dedicated Cloudflare account, or with least-privilege
   DynamoDB credentials.

## The secret

- Store it in a secret manager or an environment variable. The config names the
  source: `arbiter.secret_env` or `arbiter.secret_file`.
- If you use `secret_file`, make it mode `0600` and keep it out of version
  control. The DynamoDB `credentials_file` is the same rule.
- Rotate with the two-secret window: **add-next → roll hosts → retire-old**.
  A single-secret flip across a live fleet is a self-inflicted outage. See
  [Deployment](deployment.md#secret-rotation).
- Never put the secret in the config file, a webhook URL, a log line, or a
  container image.

## The arbiter

- Run the Worker in a **dedicated Cloudflare account**. The free-tier request
  meters are account-wide; other Workers spend the same budget.
- Use a **non-guessable Worker name**. The URL is not a secret, but an
  unauthenticated request still costs a Worker request. The secret is the
  authentication.
- The Worker compares secrets constant-time. Keep both rotation secrets set only
  during the rotation window.
- If you use DynamoDB, give the IAM user least privilege on the one table. The
  bootstrap script creates the user and prints the credentials once.

## The Docker socket

The conch container drives Docker. That means:

- It can start, stop, and kill containers on the host.
- It can run hook containers with the privileges you give them.
- It can mount host paths through `binds`.

Run it on a host you control. Do not expose the Docker socket to untrusted
tenants. Do not run untrusted configs. If you need to separate duties, run conch
on a dedicated host or use a Docker socket proxy with a narrow API allowlist.

## The image

- Pin the image by digest in production:
  `registry.example/conch@sha256:...`.
- The image is `scratch` with one static binary. There is no shell. Admin
  commands go through the Unix socket, not `docker exec ... sh`.
- The release workflow publishes the image from a tagged commit. Read the
  [release notes](https://github.com/scytherswings/conch/releases) before you
  upgrade.

## Webhooks

Webhook URLs are outbound and may contain credentials. Treat the config as
sensitive. The payload is JSON and identifies the service and the event. conch
does not send the arbiter secret to webhooks. Webhooks are best-effort and off
the critical loop.

## Logs

The supervisor logs to stdout. It does not log the shared secret. It does log
service names, phases, and decisions. Review your log retention policy; a
failover log can reveal your service topology.

## Reporting a vulnerability

Do not open a public issue. Follow [SECURITY.md](../SECURITY.md). Include the
component, the version, and a minimal reproduction. The policy lists what counts
as a vulnerability and what is a disclosed design limit.
