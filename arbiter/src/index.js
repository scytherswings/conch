/**
 * conch arbiter — a centralized lock for tunnel connectors.
 * One Worker route that matters (/batch), one Durable Object behind it.
 * See docs/specs/2026-08-11-conch-rewrite-design.md §7.
 */
export { LeaseDO } from "./lease.js";

const SINGLETON = "conch"; // every lease lives in ONE Durable Object (free-tier duration meter)
const MAX_SERVICES = 64;

const json = (body, status = 200) =>
  new Response(JSON.stringify(body) + "\n", {
    status, headers: { "content-type": "application/json; charset=utf-8" },
  });

/** Constant-time comparison so auth failures leak no timing signal. */
function secretsMatch(given, expected) {
  if (typeof given !== "string" || typeof expected !== "string" || !expected) return false;
  const enc = new TextEncoder();
  const a = enc.encode(given), b = enc.encode(expected);
  if (a.byteLength !== b.byteLength) return false;
  return crypto.subtle.timingSafeEqual(a, b);
}

/** Two secrets, so rotation is add-next -> roll hosts -> retire-old (spec §7). */
function authorized(request, env) {
  const given = request.headers.get("X-Conch-Auth") ?? "";
  return secretsMatch(given, env.CONCH_AUTH_SECRET)
      || secretsMatch(given, env.CONCH_AUTH_SECRET_NEXT);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") return json({ ok: true, service: "conch-arbiter" });
    if (url.pathname !== "/batch") return json({ error: "not_found" }, 404);
    if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    if (!env.CONCH_AUTH_SECRET)
      return json({ error: "server_misconfigured", detail: "CONCH_AUTH_SECRET unset" }, 500);
    if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);

    let body;
    try { body = await request.json(); } catch { return json({ error: "invalid_json" }, 400); }
    if (body === null || typeof body !== "object" || Array.isArray(body)
        || body.services === null || typeof body.services !== "object"
        || Array.isArray(body.services))
      return json({ error: "invalid_body" }, 400);
    if (Object.keys(body.services).length > MAX_SERVICES)
      return json({ error: "too_many_services", max: MAX_SERVICES }, 400);

    const stub = env.LEASE.get(env.LEASE.idFromName(SINGLETON));
    const res = await stub.fetch("https://lease.invalid/batch", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return json(await res.json(), res.status);
  },
};
