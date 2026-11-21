import { SELF, env, runInDurableObject, reset } from "cloudflare:test";
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

/** GET /fleet with the secret; returns { status, body }. */
async function fleet(headers = { "X-Conch-Auth": "test-secret" }) {
  const res = await SELF.fetch("https://arbiter.test/fleet", { headers });
  return { status: res.status, body: await res.json() };
}

const stub = () => env.LEASE.get(env.LEASE.idFromName("conch"));

// Any test that fakes the clock must restore it here, not at the end of its own
// body — an assertion failure above an inline vi.useRealTimers() would otherwise
// leak fake time into every later test in the file.
//
// reset() gives each `it` fresh Durable Object storage; see lease.spec.js.
afterEach(async () => {
  vi.useRealTimers();
  await reset();
});

describe("GET /fleet inventory", () => {
  it("records a host's valid service keys, dropping an oversized one from both", async () => {
    const longKey = "k".repeat(129);
    const { body: batch } = await send(req("host-a", "a1", {
      web: { op: "renew" }, blog: { op: "read" }, [longKey]: { op: "renew" },
    }));
    // The op loop already drops the key: it has no response to hang (D2).
    expect(batch.services[longKey]).toBeUndefined();

    const { body } = await fleet();
    expect(body.hosts["host-a"].services.sort()).toEqual(["blog", "web"]);
    expect(body.services[longKey]).toBeUndefined();
  });

  it("a batch with no services yields an empty hosts[].services, not a crash", async () => {
    await send(req("host-a", "a1", {}));
    const { body } = await fleet();
    expect(body.hosts["host-a"].services).toEqual([]);
    expect(body.hosts["host-a"].fingerprint).toBe("fp-host-a");
  });

  it("a /fleet read performs no storage write (D3)", async () => {
    await send(req("host-a", "a1", { web: { op: "renew", serving: true } }));

    // leaseKeys() would read the same in-memory state whether or not save() ran,
    // so it cannot prove the read is non-mutating. The storage.put spy can.
    const { body, calls } = await runInDurableObject(stub(), async (instance, state) => {
      const spy = vi.spyOn(state.storage, "put");
      const res = await instance.fetch(new Request("https://lease.invalid/fleet"));
      const body = await res.json();
      return { body, calls: spy.mock.calls.length };
    });

    expect(calls).toBe(0);
    expect(body).toMatchObject({
      services: { web: { holder: "host-a", serving: true, epoch: 1 } },
    });
    expect(typeof body.generatedAt).toBe("number");
  });

  it("a never-claimed key is absent from services (a read creates no record)", async () => {
    await send(req("host-a", "a1", { ghost: { op: "read" } }));
    const { body } = await fleet();
    expect(body.services.ghost).toBeUndefined();
    const keys = await runInDurableObject(stub(), (instance) => Object.keys(instance.leases));
    expect(keys).not.toContain("ghost");
  });

  it("a lapsed lease renders holder null, but stored state is untouched", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }, 60));
    vi.setSystemTime(Date.now() + 61_000);

    const { body } = await fleet();
    expect(body.services.web.holder).toBeNull();
    expect(body.services.web.serving).toBe(false);
    expect(body.services.web.expiresIn).toBeNull();

    // The render expired a copy; the stored holder persists until the next batch.
    const stored = await runInDurableObject(stub(), (instance) => instance.leases.web.holder);
    expect(stored).toBe("host-a");
  });

  it("omits a host silent past PRESENCE_TTL_MS from hosts", async () => {
    await send(req("host-a", "a1", {}));
    await send(req("host-b", "b1", {}));
    vi.setSystemTime(Date.now() + 3_600_001);

    const { body } = await fleet();
    expect(body.hosts["host-b"]).toBeUndefined();
  });

  it("surfaces a duplicate-host window as services[key].conflict", async () => {
    await send(req("host-a", "aaa1", { web: { op: "renew" } }));
    await send(req("host-a", "bbb2", { web: { op: "renew" } }));
    await send(req("host-a", "aaa1", { web: { op: "renew" } })); // flip-flop opens the window

    const { body } = await fleet();
    expect(body.services.web.conflict).toBe("host-a");
  });

  it("refuses an unauthenticated read with 401 and writes no presence", async () => {
    const res = await SELF.fetch("https://arbiter.test/fleet");
    expect(res.status).toBe(401);
    // Auth precedes the DO call, so nothing was registered; a later authed read
    // still sees an empty fleet.
    const { body } = await fleet();
    expect(body).toMatchObject({ services: {}, hosts: {} });
  });

  it("serves the snapshot with the secret", async () => {
    const { status, body } = await fleet();
    expect(status).toBe(200);
    expect(body).toMatchObject({ services: {}, hosts: {} });
    expect(typeof body.generatedAt).toBe("number");
  });

  it("rejects a non-GET method with 405", async () => {
    const res = await SELF.fetch("https://arbiter.test/fleet", {
      method: "POST",
      headers: { "X-Conch-Auth": "test-secret" },
    });
    expect(res.status).toBe(405);
    expect((await res.json()).error).toBe("method_not_allowed");
  });
});
