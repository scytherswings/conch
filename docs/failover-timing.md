# Failover timing

Four knobs govern failover. Everything else is derived. A fifth knob that could
quietly widen the window in which two hosts both serve is exactly the knob that
must not exist.

| Knob | Question it answers | Default |
|---|---|---|
| `ttl` | How long does a lease grant last? | 60s |
| `interval` | How often does conch check? | 3s |
| `demote_after` | Consecutive bad checks before giving up a lease | 3 |
| `promote_after` | Consecutive good checks before claiming | 6 |

The safety rules:

- **`promote_after > demote_after`.** Quick to stop serving, slow to take over.
  A flapping link cannot ping-pong traffic. Validation enforces it.
- **`ttl > interval × demote_after + stop_timeout + arbiter.timeout + 1`.** The
  lease must outlive the demotion path. Validation enforces it per service.
- **The stop deadline is derived per service** from its `stop_timeout` plus
  slop. One slow-draining connector taxes only itself.
- **The Cloudflare backend clamps `ttl` to `[10, 300]`.** The DynamoDB backend
  does not. Validation enforces the backend's floor.

## The default timeline

At defaults, host-a's uplink dying at t=0 just after a renew:

- **t=60:** the lease expires and reads free.
- **t≈78:** host-b claims after 6 × 3s of green checks and is serving a few
  seconds later.
- **host-a's side:** under `on_loss: stop`, it stops its connector at t≈19.
  Under the default `on_loss: freeze`, it stops the moment connectivity returns
  and its renew is refused.

Two honesty notes on t≈78:

- **Ungraceful death.** On power loss or a kernel panic, the edge takes time to
  notice dead QUIC connections and keeps routing a share of requests to the
  corpse. The user-visible error window is the timeline **plus** Cloudflare's
  dead-connector detection. Measure it on your own fleet; do not take a number
  on faith.
- **Stateful services.** The promote hooks are the real floor. Failover time is
  this timeline **plus** restore duration. Measure your restore and treat that
  as the number.

## The DynamoDB backend

The DynamoDB backend's only `ttl` floor is the law above. The reference config
uses much tighter timing because DynamoDB's always-free tier does not have the
Worker/DO request meter:

```yaml
timing:
  interval: 0.5
  ttl: 6
  demote_after: 2
  promote_after: 3
```

Detection plus claim lands in roughly 6–7 seconds. That is a different risk
profile, not a free lunch: the tighter the timing, the less room there is for a
slow health check or a slow restore. Read [Arbiter backends](backends.md) before
changing it.

## Measure your own

The defaults are a starting point, not a guarantee. Before you adopt conch for
anything you cannot lose:

1. Measure the edge's dead-connector detection on your own tunnel.
2. Measure your restore time, if the service is stateful.
3. Run a planned failover and time it end to end.
4. Decide whether the user-visible window is acceptable.

If it is not, the honest answer may be a paid load balancer or a real quorum
system, not a tighter `ttl`.
