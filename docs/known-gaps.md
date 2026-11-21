# Known gaps

Read this before you trust conch with anything you cannot lose.

Conch is not high availability. It narrows the window in which two hosts both
serve; it does not close it. The remaining gap is the cost of not using a paid
load balancer.

## 1. A dead or stuck conch with a healthy connector means both hosts serve

The lease expires, the peer promotes, and Cloudflare splits requests between the
two live connectors per request. Both databases take interleaved writes until a
human intervenes.

What narrows it:

- Reconcile on every check.
- Deadlines by construction.
- `restart: unless-stopped` on the connector.

What does not close it: a conch process that is alive enough to hold the lease
but not alive enough to reconcile. A dead process is detectable; a stuck one is
not.

## 2. A wedged Docker daemon can leave a connector serving

The stop runs through the same daemon whose failure (full disk, hung I/O) often
triggered the demotion. conch escalates: `stop`, verify, `kill`, verify, then
sends an alert. A daemon that cannot kill a container leaves it up.

What narrows it: the disk warning and the escalation ladder. What does not close
it: a daemon that cannot act.

## 3. The boot race

`unless-stopped` auto-starts a connector that was running at shutdown. If the
peer took the lease during the reboot, the booting host's connector is briefly
live until reconcile stops it, seconds later.

What narrows it: the short interval and the first reconcile. What does not close
it: the gap between container start and the first check.

## 4. `arbiter.on_loss: freeze` trades one risk for another

The default freezes the fleet in place when the arbiter is unreachable: whatever
is serving keeps serving on an expired lease and conch yells. That is safe
against every **symmetric** arbiter failure, because promotion requires a
successful grant.

The residual risk is an **asymmetric** partition: the standby still reaches the
arbiter, sees a free lease, and promotes while the frozen host serves.

`on_loss: stop` removes that risk and makes the arbiter's availability the
fleet's. Choose deliberately; neither is free.

## 5. A service broken identically on both hosts alternates slowly

Neither host can serve it. Each keeps taking a turn failing. On a stateful
service, that means restore hooks re-running each time. Hysteresis and the
hook-failure backoff bound the churn; they do not fix the service. The fix is a
human.

## 6. The label sweep stops connectors; the rest of a rename is yours

Every check, conch stops any running container carrying a `conch.service` label
that this host's config no longer names. That is how a renamed or removed
service's connector cannot keep serving unsupervised. It only ever **stops**;
removing the stopped container is still yours. See the rename procedure in
[`deploy/README.md`](../deploy/README.md#identity-fields-are-immutable-in-place).

## When not to use conch

- **You need high availability.** Use a real load balancer and a real quorum
  system. conch is an open-source alternative, not a replacement for either.
- **You have one host.** conch is designed and tested for two. A single-host
  deployment is a supervised container, not a failover system.
- **You need active/active.** Exactly one host serves each service. That is the
  point.
- **You cannot tolerate a bounded outage on failover.** Measure the window first.
  If it is unacceptable, the answer may be a paid service.
- **You need database failover.** conch orchestrates restores from replication
  you already run. It never promotes a database primary and takes no backups.
