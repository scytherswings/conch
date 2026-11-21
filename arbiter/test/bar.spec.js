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

describe("evacuation bar", () => {
  it("an evacuating release bars the releaser and only the releaser", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true } }));
    const a = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(a.body.services.web).toMatchObject({ state: "barred", evacuated_by: "host-a" });
    const b = await send(req("host-b", "b1", { web: { op: "renew" } }));
    expect(b.body.services.web.state).toBe("yours");
  });

  it("a peer's claim clears an ordinary bar", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true } }));
    await send(req("host-b", "b1", { web: { op: "renew" } })); // peer claims, clearing the bar
    await send(req("host-b", "b1", { web: { op: "release" } })); // peer goes away plainly
    const a = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(a.body.services.web.state).toBe("yours"); // bar was cleared by b's claim
  });

  it("a hold bar survives the peer's claim and the peer's later explicit release", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true, hold: true } }));
    await send(req("host-b", "b1", { web: { op: "renew" } }));   // peer claims over the hold
    await send(req("host-b", "b1", { web: { op: "release" } })); // peer goes away
    const a = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(a.body.services.web.state).toBe("barred"); // still barred: hold held
  });

  it("a hold bar survives the peer's lease timing out, not just an explicit release", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true, hold: true } }));
    await send(req("host-b", "b1", { web: { op: "renew" } }, 60)); // peer claims over the hold
    vi.setSystemTime(Date.now() + 61_000); // b's lease lapses on its own; nobody releases
    const a = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(a.body.services.web.state).toBe("barred"); // lease expiry alone doesn't clear a hold
  });

  it("override clears the sender's own bar (even a hold) and claims in one op", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true, hold: true } }));
    const barred = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(barred.body.services.web.state).toBe("barred");
    const a = await send(req("host-a", "a1", { web: { op: "renew", override: true } }));
    expect(a.body.services.web.state).toBe("yours");
  });

  it("bar_ttl lazily expires the bar", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true, bar_ttl: 30 } }));
    const before = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(before.body.services.web.state).toBe("barred");
    vi.setSystemTime(Date.now() + 31_000);
    const after = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(after.body.services.web.state).toBe("yours");
  });

  it("bar_ttl lazily expires a hold bar too", async () => {
    // The interface note says bar_ttl expires "either kind" — exercise the
    // hold variant separately from the plain-bar test above.
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1",
      { web: { op: "release", evacuate: true, hold: true, bar_ttl: 30 } }));
    const before = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(before.body.services.web.state).toBe("barred");
    vi.setSystemTime(Date.now() + 31_000);
    const after = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(after.body.services.web.state).toBe("yours");
  });

  it("a read shows free-with-bar to everyone else", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true } }));
    const b = await send(req("host-b", "b1", { web: { op: "read" } }));
    expect(b.body.services.web).toMatchObject({ state: "free", evacuated_by: "host-a" });
  });

  it("a read by the barred host itself shows barred, not free", async () => {
    // view()'s own barred branch (as opposed to renewOne's early return):
    // op:"read" must resolve the same "barred" verdict for the barred host.
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true } }));
    const a = await send(req("host-a", "a1", { web: { op: "read" } }));
    expect(a.body.services.web).toMatchObject({ state: "barred", evacuated_by: "host-a" });
  });

  it("the barred host's own read shows the peer's holdership, not barred, while the peer holds", async () => {
    // Holdership outranks bar display: once a peer claims over a hold bar, the
    // barred host's own view must reflect who actually holds the lease now,
    // not the historical bar (spec §7 edge case).
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    await send(req("host-a", "a1", { web: { op: "release", evacuate: true, hold: true } }));
    await send(req("host-b", "b1", { web: { op: "renew" } })); // peer claims over the hold
    const a = await send(req("host-a", "a1", { web: { op: "read" } }));
    expect(a.body.services.web).toMatchObject({ state: "held", holder: "host-b" });
  });
});
