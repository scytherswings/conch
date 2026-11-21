# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-21

### Added

- The `conch` supervisor: one static Go binary that reconciles Docker
  containers against a lease held by an arbiter.
- The arbiter: a Cloudflare Worker with one Durable Object, one lease per
  service, atomic batch operations, and constant-time secret authentication.
- An optional DynamoDB arbiter backend for deployments that need more than the
  Worker free tier's request meter or a `ttl` below the Worker's clamp.
- The CLI: `run`, `validate`, `status`, `evacuate`, and `reclaim`.
- Dry-run mode that logs every decision, performs no actions, and sends the
  arbiter only reads.
- Kamal accessory templates, example host configs, the pre-deploy validate
  hook, and the DynamoDB bootstrap script.
- Public documentation: quick start, configuration, deployment, operations,
  architecture, failover timing, known gaps, backends, security,
  troubleshooting, and development.
- Copyable examples: a minimal config and a single-host compose file.
- CI for the Go and Workers suites, example validation, the Docker build, and a
  secret scan. Dependabot covers Go, npm, GitHub Actions, and Docker.
- A release workflow that builds static binaries and publishes the container
  image to GitHub Container Registry.

### Security

- The repository contains no real hostnames, addresses, or credentials. The
  example configs use generic placeholders.
- The arbiter authenticates every request with a shared secret and supports
  add-next → rotate hosts → remove-previous secret rotation.

[Unreleased]: https://github.com/scytherswings/conch/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/scytherswings/conch/releases/tag/v0.1.0
