import { SELF, reset } from "cloudflare:test";
import { describe, it, expect, afterEach } from "vitest";
import worker from "../src/index.js";

const batch = (body, headers = {}) =>
  SELF.fetch("https://arbiter.test/batch", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });

// reset() gives each `it` fresh Durable Object storage; see lease.spec.js.
afterEach(async () => {
  await reset();
});

describe("worker surface", () => {
  it("health needs no auth", async () => {
    const res = await SELF.fetch("https://arbiter.test/health");
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true });
  });

  it("batch without the secret is 401", async () => {
    const res = await batch({ host: "host-a", instance: "aa", ttl: 60, services: {} });
    expect(res.status).toBe(401);
  });

  it("accepts the current secret", async () => {
    const res = await batch(
      { host: "host-a", instance: "aa", ttl: 60, services: {} },
      { "X-Conch-Auth": "test-secret" });
    expect(res.status).toBe(200);
  });

  it("accepts the NEXT secret during rotation", async () => {
    // CONCH_AUTH_SECRET_NEXT is bound in vitest.config.js via miniflare bindings.
    const res = await batch(
      { host: "host-a", instance: "aa", ttl: 60, services: {} },
      { "X-Conch-Auth": "next-secret" });
    expect(res.status).toBe(200);
  });

  it("rejects oversized batches before the DO", async () => {
    const services = {};
    for (let i = 0; i < 65; i++) services[`svc${i}`] = { op: "read" };
    const res = await batch({ host: "host-a", instance: "aa", ttl: 60, services },
      { "X-Conch-Auth": "test-secret" });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe("too_many_services");
  });

  it("unknown paths are 404", async () => {
    const res = await SELF.fetch("https://arbiter.test/nope");
    expect(res.status).toBe(404);
  });

  it("non-POST to /batch is 405", async () => {
    const res = await SELF.fetch("https://arbiter.test/batch", {
      method: "GET",
      headers: { "X-Conch-Auth": "test-secret" },
    });
    expect(res.status).toBe(405);
    expect((await res.json()).error).toBe("method_not_allowed");
  });

  it("invalid JSON body is 400", async () => {
    const res = await SELF.fetch("https://arbiter.test/batch", {
      method: "POST",
      headers: { "content-type": "application/json", "X-Conch-Auth": "test-secret" },
      body: "{ not valid json",
    });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe("invalid_json");
  });

  it("non-object body is 400", async () => {
    const res = await batch([], { "X-Conch-Auth": "test-secret" });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe("invalid_body");
  });

  it("services as array is 400", async () => {
    const res = await batch(
      { host: "host-a", instance: "aa", ttl: 60, services: [] },
      { "X-Conch-Auth": "test-secret" });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe("invalid_body");
  });

  it("missing CONCH_AUTH_SECRET is 500", async () => {
    const req = new Request("https://arbiter.test/batch", {
      method: "POST",
      headers: { "content-type": "application/json", "X-Conch-Auth": "test-secret" },
      body: JSON.stringify({ host: "host-a", instance: "aa", ttl: 60, services: {} }),
    });
    const res = await worker.fetch(req, {});
    expect(res.status).toBe(500);
    const body = await res.json();
    expect(body.error).toBe("server_misconfigured");
    // No oracle: an unauthenticated caller learns only that it is misconfigured,
    // never which secret is unset (the log says that).
    expect(body.detail).toBeUndefined();
  });
});
