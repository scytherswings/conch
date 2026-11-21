# Examples

Copyable starting points. The canonical deployment templates live in
[`deploy/`](../deploy/); these examples are the smallest useful versions.

| File | What it shows |
|---|---|
| [`config.minimal.yml`](config.minimal.yml) | One stateless service, one connector, the four timing knobs. |
| [`docker-compose.yml`](docker-compose.yml) | A single-host illustration: the service, its connector, and conch. |

## Try the minimal config

```sh
cp examples/config.minimal.yml ./conch.yml
# edit host:, arbiter.url:, and the container/health names
./conch validate --config ./conch.yml
```

`conch validate` exits `1` and lists every fault, not just the first. A config
that passes is the only config conch will run.

## Try the compose stack

The compose file is a single-host illustration. conch is designed for two hosts:
run the same stack on each host with the same service and lease key, changing
only `host:` in `conch.yml`.

```sh
cd examples
cp config.minimal.yml conch.yml
# edit conch.yml: host:, arbiter.url:, and the container/health names
CONCH_AUTH_SECRET=... TUNNEL_TOKEN=... docker compose up -d
```

The `conch` container mounts the Docker socket because it starts and stops the
connector. That socket is the privilege; the container does not need to be
privileged. The `conch` image defaults to the published image; build locally
with `docker build -t conch:dev .` and set `CONCH_IMAGE=conch:dev` to use it.

## Where to go next

- [Quick start](../docs/quickstart.md) — the full two-host walkthrough.
- [Configuration](../docs/configuration.md) — every field and default.
- [Deployment](../docs/deployment.md) — the Kamal accessory path, binds, and
  secret rotation.
- [Known gaps](../docs/known-gaps.md) — read this before trusting conch with
  stateful data.
