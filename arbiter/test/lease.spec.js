import { SELF, env, runInDurableObject, reset } from "cloudflare:test";
import { describe, it, expect, vi, afterEach } from "vitest";
import { MAX_LEASES, MAX_HOST_IMAGES } from "../src/lease.js";

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

/** Reach into the singleton DO's live instance state (white-box, for storage-growth checks). */
async function leaseKeys() {
  const stub = env.LEASE.get(env.LEASE.idFromName("conch"));
  return runInDurableObject(stub, (instance) => Object.keys(instance.leases));
}

/** Pre-fill the live instance's lease table through the DO's own blank-record
 *  factory, so a cap test does not have to make MAX_LEASES real round-trips. */
async function seedLeases(n) {
  const stub = env.LEASE.get(env.LEASE.idFromName("conch"));
  await runInDurableObject(stub, (instance) => {
    for (let i = 0; i < n; i++) instance.lease(`seed-${i}`);
  });
}

// Any test that fakes the clock must restore it here, not at the end of its own
// body — an assertion failure above an inline vi.useRealTimers() would otherwise
// leak fake time into every later test in the file.
//
// reset() gives each `it` fresh Durable Object storage, the per-test isolation
// @cloudflare/vitest-pool-workers' old `isolatedStorage: true` provided (the
// new @cloudflare/vitest-plugin leaves it to the test).
afterEach(async () => {
  vi.useRealTimers();
  await reset();
});

describe("lease basics", () => {
  it("a claim on a free lease is granted and epoch starts at 1", async () => {
    const { body } = await send(req("host-a", "a1", { web: { op: "renew", serving: false } }));
    expect(body.services.web).toMatchObject({ state: "yours", ttl: 60, epoch: 1 });
  });

  it("renewal by the holder extends without bumping the epoch", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    const { body } = await send(req("host-a", "a1", { web: { op: "renew", serving: true } }));
    expect(body.services.web).toMatchObject({ state: "yours", epoch: 1 });
  });

  it("a renew from another host is refused with holder and serving", async () => {
    await send(req("host-a", "a1", { web: { op: "renew", serving: true } }));
    const { body } = await send(req("host-b", "b1", { web: { op: "renew" } }));
    expect(body.services.web).toMatchObject({ state: "held", holder: "host-a", serving: true });
  });

  it("read never mutates holdership", async () => {
    const r1 = await send(req("host-b", "b1", { web: { op: "read" } }));
    expect(r1.body.services.web.state).toBe("free");
    const r2 = await send(req("host-b", "b1", { web: { op: "read" } }));
    expect(r2.body.services.web.state).toBe("free"); // still free: read claimed nothing
  });

  it("an expired lease reads free, and the next claimant bumps the epoch", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }, 60));
    // Advance past expiry: vitest-pool-workers supports fake timers via vi.setSystemTime.
    vi.setSystemTime(Date.now() + 61_000);
    const read = await send(req("host-b", "b1", { web: { op: "read" } }));
    expect(read.body.services.web.state).toBe("free");
    const claim = await send(req("host-b", "b1", { web: { op: "renew" } }));
    expect(claim.body.services.web).toMatchObject({ state: "yours", epoch: 2 });
  });

  it("release by the holder frees; release by a non-holder errors and changes nothing", async () => {
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    const bad = await send(req("host-b", "b1", { web: { op: "release" } }));
    expect(bad.body.services.web).toMatchObject({ state: "error", error: "not_holder" });
    const still = await send(req("host-b", "b1", { web: { op: "read" } }));
    expect(still.body.services.web.state).toBe("held");
    const ok = await send(req("host-a", "a1", { web: { op: "release" } }));
    expect(ok.body.services.web).toMatchObject({ state: "free", released: true });
  });

  it("release with evacuate returns a free-shaped verdict carrying evacuated_by", async () => {
    // Appendix A / spec §7: the releaser's own response is always state:"free"
    // (plus evacuated_by + released:true), never "barred" — "barred" is what
    // THIS host sees on its NEXT read/renew of the same key.
    await send(req("host-a", "a1", { web: { op: "renew" } }));
    const { body } = await send(req("host-a", "a1", { web: { op: "release", evacuate: true } }));
    expect(body.services.web).toEqual({
      state: "free", evacuated_by: "host-a", epoch: 1, released: true,
    });
  });

  it("a malformed entry poisons only itself; siblings still process", async () => {
    const { body } = await send(req("host-a", "a1", {
      web: { op: "renew" }, blog: { op: "explode" },
    }));
    expect(body.services.web.state).toBe("yours");
    expect(body.services.blog).toMatchObject({ state: "error", error: "invalid_op" });
  });

  it("a non-object entry value still gets an error under its own key, not silence", async () => {
    const { body } = await send(req("host-a", "a1", {
      web: { op: "renew" }, blog: "oops",
    }));
    expect(body.services.web.state).toBe("yours");
    expect(body.services.blog).toMatchObject({ state: "error", error: "invalid_op" });
  });

  it("a mixed grant/refusal response is the normal mid-failover shape", async () => {
    await send(req("host-b", "b1", { blog: { op: "renew" } }));
    const { body } = await send(req("host-a", "a1", {
      web: { op: "renew" }, blog: { op: "renew" },
    }));
    expect(body.services.web.state).toBe("yours");
    expect(body.services.blog.state).toBe("held");
  });

  it("an entry with an invalid op does not persist a lease record for its key", async () => {
    await send(req("host-a", "a1", { ghost: { op: "explode" } }));
    expect(await leaseKeys()).not.toContain("ghost");
  });

  it("a read of a never-held key does not persist a lease record for its key", async () => {
    await send(req("host-a", "a1", { ghost: { op: "read" } }));
    expect(await leaseKeys()).not.toContain("ghost");
  });
});

describe("presence: peers and fingerprints", () => {
  it("reports a peer's last_seen in seconds, excludes self, and echoes its fingerprint", async () => {
    await send(req("host-a", "a1", {}));
    await send(req("host-b", "b1", {}));
    vi.setSystemTime(Date.now() + 5_000);
    const { body } = await send(req("host-a", "a1", {}));

    expect(body.peers["host-a"]).toBeUndefined(); // never itself
    expect(body.peers["host-b"].last_seen).toBeCloseTo(5, 0); // seconds, not ms
    expect(body.fingerprints["host-b"]).toBe("fp-host-b");
  });

  it("forgets a host that has been silent for more than an hour", async () => {
    await send(req("host-a", "a1", {}));
    await send(req("host-b", "b1", {}));
    vi.setSystemTime(Date.now() + 3_600_001);
    const { body } = await send(req("host-a", "a1", {}));

    expect(body.peers["host-b"]).toBeUndefined();
    expect(body.fingerprints["host-b"]).toBeUndefined();
  });
});

describe("storage bounds (F2)", () => {
  it("a release of a never-claimed key does not persist a lease record", async () => {
    const { body } = await send(req("host-a", "a1", { ghost: { op: "release" } }));
    expect(body.services.ghost).toMatchObject({ state: "error", error: "not_holder" });
    expect(await leaseKeys()).not.toContain("ghost");
  });

  it("caps the images a host reports at MAX_HOST_IMAGES", async () => {
    const images = {};
    for (let i = 0; i < MAX_HOST_IMAGES + 5; i++) images[`svc${i}`] = `img-${i}`;
    await send({ ...req("host-a", "a1", {}), images });

    const stub = env.LEASE.get(env.LEASE.idFromName("conch"));
    const stored = await runInDurableObject(stub,
      (instance) => Object.keys(instance.hosts["host-a"].images));
    expect(stored).toHaveLength(MAX_HOST_IMAGES);
  });

  it("refuses a renew that would create a key past the lease ceiling, and creates nothing", async () => {
    await seedLeases(MAX_LEASES);
    const { body } = await send(req("host-a", "a1", { fresh: { op: "renew" } }));
    expect(body.services.fresh).toMatchObject({ state: "error", error: "at_capacity" });
    const keys = await leaseKeys();
    expect(keys).not.toContain("fresh");
    expect(keys).toHaveLength(MAX_LEASES);
  });

  it("still renews an existing key when the table is at the ceiling", async () => {
    // The cap bounds creation, not a live lease: at the ceiling a host must keep
    // renewing what it already holds. Seed one below the ceiling so the claim
    // itself is the record that fills it, then renew it again.
    await seedLeases(MAX_LEASES - 1);
    const claim = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(claim.body.services.web.state).toBe("yours");
    const again = await send(req("host-a", "a1", { web: { op: "renew" } }));
    expect(again.body.services.web.state).toBe("yours");
  });
});
