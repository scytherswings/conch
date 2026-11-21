import { SELF, reset } from "cloudflare:test";
import { describe, it, expect, vi, afterEach } from "vitest";

async function send(body) {
  const res = await SELF.fetch("https://arbiter.test/batch", {
    method: "POST",
    headers: { "content-type": "application/json", "X-Conch-Auth": "test-secret" },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}
const req = (host, instance, services, ttl = 60) =>
  ({ host, instance, ttl, services, fingerprint: "fp-" + host });

// Any test that fakes the clock must restore it here, not at the end of its own
// body — an assertion failure above an inline vi.useRealTimers() would otherwise
// leak fake time into every later test in the file.
//
// reset() gives each `it` fresh Durable Object storage; see lease.spec.js.
afterEach(async () => {
  vi.useRealTimers();
  await reset();
});

describe("duplicate host detection (spec §7)", () => {
  it("a single instance change is a normal conch restart", async () => {
    await send(req("host-a", "old1", { web: { op: "renew" } }));
    const r = await send(req("host-a", "new2", { web: { op: "renew" } }));
    expect(r.body.services.web.state).toBe("yours");
  });

  it("a flip-flop within one TTL refuses BOTH with duplicate_host", async () => {
    await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    await send(req("host-a", "bbb2", { web: { op: "renew" } })); // change 1
    const flip = await send(req("host-a", "aaa1", { web: { op: "renew" } })); // A again!
    expect(flip.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });
    const other = await send(req("host-a", "bbb2", { web: { op: "renew" } }));
    expect(other.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });
  });

  it("after the conflict window the lease can be claimed again", async () => {
    await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    await send(req("host-a", "bbb2", { web: { op: "renew" } }));
    await send(req("host-a", "aaa1", { web: { op: "renew" } })); // conflict window opens, ttl=60
    vi.setSystemTime(Date.now() + 61_000); // conflict window == ttl
    const r = await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    expect(r.body.services.web.state).toBe("yours");
  });

  it("three genuinely sequential restarts (A -> B -> C) inside one TTL are not a conflict", async () => {
    // The guard is "does this instance match the ONE we already flipped away
    // from", not "has the instance changed again" — three distinct instances
    // in a row must never trip it, even packed inside a single TTL.
    const base = new Date("2026-01-01T00:00:00Z").getTime();
    vi.setSystemTime(base);
    await send(req("host-a", "aaa", { web: { op: "renew" } }, 60));
    vi.setSystemTime(base + 10_000);
    const b = await send(req("host-a", "bbb", { web: { op: "renew" } }, 60));
    expect(b.body.services.web.state).toBe("yours");
    vi.setSystemTime(base + 20_000);
    const c = await send(req("host-a", "ccc", { web: { op: "renew" } }, 60));
    expect(c.body.services.web.state).toBe("yours");
  });

  it("during an open conflict window, a different host claims once the lease itself expires", async () => {
    // The conflict window (opened at detection) outlives the underlying
    // lease's own expiresAt (set at the prior grant) — there's a real gap
    // where the lease has lapsed but the window hasn't. A genuinely
    // different host name must be able to claim in that gap: that's
    // failover working, not the conflict reopening.
    const base = new Date("2026-01-01T00:00:00Z").getTime();
    vi.setSystemTime(base);
    await send(req("host-a", "aaa1", { web: { op: "renew" } }, 60)); // expiresAt = base+60s
    vi.setSystemTime(base + 30_000);
    await send(req("host-a", "bbb2", { web: { op: "renew" } }, 60)); // changedAt=+30s, expiresAt=+90s
    vi.setSystemTime(base + 40_000);
    const flip = await send(req("host-a", "aaa1", { web: { op: "renew" } }, 60)); // conflictUntil=+100s
    expect(flip.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });

    vi.setSystemTime(base + 95_000); // past expiresAt (+90s), conflictUntil (+100s) still open
    const other = await send(req("host-b", "c1", { web: { op: "renew" } }));
    expect(other.body.services.web.state).toBe("yours");
  });

  it("during that same open conflict window, the conflicted host is still refused", async () => {
    // The discriminating pair for the test above: at the identical timestamp
    // where a DIFFERENT host is granted the now-expired lease, the
    // CONFLICTED host name (either flip-flopping instance) is still refused
    // — proving the refusal tracks the conflicted host, not raw holdership.
    const base = new Date("2026-01-01T00:00:00Z").getTime();
    vi.setSystemTime(base);
    await send(req("host-a", "aaa1", { web: { op: "renew" } }, 60));
    vi.setSystemTime(base + 30_000);
    await send(req("host-a", "bbb2", { web: { op: "renew" } }, 60));
    vi.setSystemTime(base + 40_000);
    await send(req("host-a", "aaa1", { web: { op: "renew" } }, 60)); // conflictUntil=+100s

    vi.setSystemTime(base + 95_000); // past expiresAt (+90s), conflictUntil (+100s) still open
    const a = await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    expect(a.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });
    const b = await send(req("host-a", "bbb2", { web: { op: "renew" } }));
    expect(b.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });
  });

  it("a prior instance reclaims once the flip-flop window has closed, even under continuous renewal", async () => {
    // Isolates the `now - instanceChangedAt < windowMs` bound in renewOne
    // from every other guard that could mask its absence: the lease is kept
    // continuously renewed (never past its own expiresAt, so expireIfDue
    // never nulls the holder and no conflictUntil/natural-expiry path gets
    // involved) and the flip is back to the *same* prevInstance a plain
    // instance-history check would still recognize. Only the elapsed-time
    // bound distinguishes "yours" from "duplicate_host" here.
    const base = new Date("2026-01-01T00:00:00Z").getTime();
    vi.setSystemTime(base);
    await send(req("host-a", "aaa", { web: { op: "renew" } }, 60)); // fresh hold, expiresAt=+60s

    vi.setSystemTime(base + 10_000);
    const flip1 = await send(req("host-a", "bbb", { web: { op: "renew" } }, 60));
    // prevInstance=aaa, instanceChangedAt=+10s, expiresAt=+70s
    expect(flip1.body.services.web.state).toBe("yours");

    // Keep renewing as bbb (same instance -> no flip-flop check at all) well
    // past where the window would close (+70s), so the lease is never
    // allowed to lapse.
    vi.setSystemTime(base + 40_000);
    const keepAlive = await send(req("host-a", "bbb", { web: { op: "renew" } }, 60)); // expiresAt=+100s
    expect(keepAlive.body.services.web.state).toBe("yours");

    // +75s is 65s after the +10s instance change: strictly past the 60s
    // window (closed at +70s), while the lease itself is still held
    // (expiresAt=+100s) thanks to the keep-alive above.
    vi.setSystemTime(base + 75_000);
    const back = await send(req("host-a", "aaa", { web: { op: "renew" } }, 60));
    expect(back.body.services.web.state).toBe("yours");
  });

  it("a read by the conflicted holder during the window still returns the normal view, not an error", async () => {
    // Reads never mutate holdership, and the flip-flop refusal is about
    // renewal specifically (spec §7's "refuses both" is describing the two
    // competing renew calls). A read mid-conflict reports whatever the lease
    // actually looks like right now — here, still validly held by host-a
    // (whichever instance renewed last) — not a duplicate_host error.
    await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    await send(req("host-a", "bbb2", { web: { op: "renew" } }));
    const flip = await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    expect(flip.body.services.web).toMatchObject({ state: "error", error: "duplicate_host" });

    const read = await send(req("host-a", "aaa1", { web: { op: "read" } }));
    expect(read.body.services.web.state).toBe("yours");
  });
});
