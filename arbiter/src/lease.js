/**
 * One Durable Object holds every lease (spec §3: three free-tier meters force it).
 * Expiry is lazy (spec §7): the read that discovers an expired lease is the cleanup.
 * One Date.now() and at most one storage write per batch: the response is atomic.
 */
const STORAGE_KEY = "state";
const TTL_MIN = 10, TTL_MAX = 300, TTL_DEFAULT = 60;
const PRESENCE_TTL_MS = 3_600_000; // forget hosts silent for an hour

// Hard ceilings on what one batch may persist (F2). The whole fleet lives in
// this one object, so no authenticated caller — bug or brute force — may grow
// it without bound. A renew may create a record only while the table has room;
// a host reports at most as many images as the Worker lets a batch carry
// services (MAX_SERVICES, index.js), which is also the most services a host can
// legitimately have.
export const MAX_LEASES = 1024;
export const MAX_HOST_IMAGES = 64;

const VALID_OPS = new Set(["renew", "read", "release"]);

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));
const ttlSeconds = (v) => Number.isFinite(Number(v))
  ? clamp(Math.round(Number(v)), TTL_MIN, TTL_MAX) : TTL_DEFAULT;

function blankLease() {
  return { holder: null, instance: null, epoch: 0, expiresAt: null, serving: false,
           evacuatedBy: null, barExpiresAt: null, barHold: false,
           prevInstance: null, instanceChangedAt: null,
           conflictUntil: null, conflictHost: null };
}

export class LeaseDO {
  constructor(state) {
    this.state = state;
    this.state.blockConcurrencyWhile(async () => {
      const stored = await this.state.storage.get(STORAGE_KEY);
      this.leases = stored?.leases ?? {};
      this.hosts = stored?.hosts ?? {};
    });
  }

  async save() {
    await this.state.storage.put(STORAGE_KEY, { leases: this.leases, hosts: this.hosts });
  }

  lease(key) { return this.leases[key] ?? (this.leases[key] = blankLease()); }

  /** Lazy expiry for one key against the batch's single `now`. */
  expireIfDue(now, l) {
    if (l.holder !== null && l.expiresAt !== null && now >= l.expiresAt) {
      l.holder = null; l.instance = null; l.expiresAt = null; l.serving = false;
    }
    if (l.evacuatedBy !== null && l.barExpiresAt !== null && now >= l.barExpiresAt) {
      l.evacuatedBy = null; l.barExpiresAt = null; l.barHold = false;
    }
    if (l.conflictUntil !== null && now >= l.conflictUntil) {
      l.conflictUntil = null; l.conflictHost = null;
    }
  }

  view(l, forHost) {
    if (l.holder === forHost) return { state: "yours", epoch: l.epoch };
    if (l.holder !== null)
      return { state: "held", holder: l.holder, serving: l.serving, epoch: l.epoch };
    if (l.evacuatedBy === forHost)
      return { state: "barred", evacuated_by: l.evacuatedBy, epoch: l.epoch };
    const out = { state: "free", epoch: l.epoch };
    if (l.evacuatedBy !== null) out.evacuated_by = l.evacuatedBy;
    return out;
  }

  renewOne(now, key, req, entry) {
    const l = this.lease(key);
    const wantsOverride = entry.override === true;
    // Bar check: barred unless this renew is the barred host overriding.
    if (l.holder === null && l.evacuatedBy === req.host && !wantsOverride) {
      return { state: "barred", evacuated_by: l.evacuatedBy, epoch: l.epoch };
    }
    if (l.holder !== null && l.holder !== req.host) {
      return { state: "held", holder: l.holder, serving: l.serving, epoch: l.epoch };
    }

    // Duplicate-host detection (spec §7): two supervisors sharing one host
    // name are indistinguishable by name. A single instance change on a
    // held lease is a normal conch restart; a flip-flop (A -> B -> A)
    // within one TTL means two live processes fighting over the name.
    // `conflictHost` (not just `l.holder`) records who was conflicted:
    // once detected, EVERY renew claiming that host name is refused until
    // the window closes, even after the underlying lease's own expiresAt
    // lapses and expireIfDue nulls l.holder out from under it. A different
    // host name is untouched by any of this — if the lease itself expires
    // mid-window, a real peer taking over is failover working, not the
    // conflict reopening, so that must fall through to the grant below.
    if (l.conflictUntil !== null && now < l.conflictUntil && req.host === l.conflictHost) {
      return { state: "error", error: "duplicate_host" };
    }
    if (l.holder === req.host && l.instance !== null && req.instance
        && l.instance !== req.instance) {
      const windowMs = ttlSeconds(req.ttl) * 1000;
      if (l.prevInstance === req.instance
          && l.instanceChangedAt !== null && now - l.instanceChangedAt < windowMs) {
        l.conflictUntil = now + windowMs;
        l.conflictHost = req.host;
        return { state: "error", error: "duplicate_host" };
      }
      l.prevInstance = l.instance;
      l.instanceChangedAt = now;
    }

    if (l.holder === null) {
      l.epoch += 1;
      // A fresh hold starts with no flip-flop history: the concept only
      // makes sense across a continuously-held run of the same host name.
      l.prevInstance = null; l.instanceChangedAt = null;
      if (l.evacuatedBy !== null) {
        const ownOverride = l.evacuatedBy === req.host && wantsOverride;
        const peerClaim = l.evacuatedBy !== req.host && !l.barHold;
        if (ownOverride || peerClaim) {
          l.evacuatedBy = null; l.barExpiresAt = null; l.barHold = false;
        }
        // A hold bar under a peer's claim stays recorded: the lease is granted
        // to the peer, but the bar outlives it (spec §7) until the barred
        // host's own override renew, or bar_ttl, clears it.
      }
    }
    l.holder = req.host;
    l.instance = req.instance ?? null;
    l.expiresAt = now + ttlSeconds(req.ttl) * 1000;
    l.serving = entry.serving === true;
    return { state: "yours", ttl: ttlSeconds(req.ttl), epoch: l.epoch };
  }

  releaseOne(now, key, req, entry) {
    const l = this.lease(key);
    if (l.holder === null || l.holder !== req.host) {
      return { state: "error", error: "not_holder" };
    }
    l.holder = null; l.instance = null; l.expiresAt = null; l.serving = false;
    if (entry.evacuate === true) {
      l.evacuatedBy = req.host;
      l.barHold = entry.hold === true;
      l.barExpiresAt = Number.isFinite(Number(entry.bar_ttl))
        ? now + Math.round(Number(entry.bar_ttl)) * 1000 : null;
    }
    // Always free-shaped (plan Appendix A / spec §7): this is the releaser's
    // own confirmation that the release happened, not a status check on
    // themselves. view() would say "barred" here because evacuatedBy now
    // equals req.host — that's the right answer for THEIR NEXT read/renew,
    // not for this response.
    const out = { state: "free", epoch: l.epoch, released: true };
    if (l.evacuatedBy !== null) out.evacuated_by = l.evacuatedBy;
    return out;
  }

  batch(now, body) {
    const host = body.host, instance = body.instance;
    if (typeof host !== "string" || !host || host.length > 128)
      return { error: "invalid_host", status: 400 };

    // Presence: the arbiter remembers each host's last check-in (spec §7),
    // its config fingerprint, and — for the fleet's image-parity check — the
    // container image each service resolves to on that host right now. Images
    // are bounded and optional: a host that sends none (an older conch, or a
    // service it cannot resolve) simply reports none, which its peers read
    // as "no opinion", never as a mismatch.
    const images = {};
    if (body.images && typeof body.images === "object") {
      let n = 0;
      for (const [key, img] of Object.entries(body.images)) {
        if (n >= MAX_HOST_IMAGES) break; // F2: a host cannot report images without limit
        if (!key || key.length > 128) continue;
        if (typeof img === "string" && img) { images[key] = img.slice(0, 512); n++; }
      }
    }
    this.hosts[host] = { lastSeen: now, fingerprint: body.fingerprint ?? null, images };
    for (const [h, rec] of Object.entries(this.hosts)) {
      if (now - rec.lastSeen > PRESENCE_TTL_MS) delete this.hosts[h];
    }

    const services = {};
    for (const [key, entry] of Object.entries(body.services ?? {})) {
      // An unusable key has nowhere to hang a response, so it is dropped
      // silently. Everything else — including a malformed entry value —
      // still gets an error under its own key: "poisons only itself" means
      // the sibling keeps processing, not that the bad key vanishes.
      if (!key || key.length > 128) continue;
      if (entry === null || typeof entry !== "object" || !VALID_OPS.has(entry.op)) {
        services[key] = { state: "error", error: "invalid_op" };
        continue;
      }

      const req = { host, instance, ttl: body.ttl };
      if (entry.op === "read") {
        // Non-creating lookup: a read must not persist a blank record for a
        // key nobody has ever claimed (unbounded storage growth otherwise).
        const l = this.leases[key];
        if (l === undefined) { services[key] = { state: "free", epoch: 0 }; continue; }
        this.expireIfDue(now, l);
        services[key] = this.view(l, host);
        continue;
      }

      // A release is a statement about a lease the sender claims to hold, so it
      // needs no record of its own: a key nobody has ever claimed is refused
      // (not_holder) without minting one. Creating the blank record here would
      // let any caller grow the table for free (F2), and releaseOne's verdict
      // is identical either way.
      if (entry.op === "release") {
        const l = this.leases[key];
        if (l === undefined) { services[key] = { state: "error", error: "not_holder" }; continue; }
        this.expireIfDue(now, l);
        services[key] = this.releaseOne(now, key, req, entry);
        continue;
      }

      // renew legitimately creates the record on first touch, but only while the
      // table has room. Past the ceiling a brand-new key is refused rather than
      // allowed to grow the one object every lease depends on without limit.
      // The Go client fails closed on an unknown error state (client.go's
      // toVerdict), so a host simply holds nothing for that key.
      if (this.leases[key] === undefined && Object.keys(this.leases).length >= MAX_LEASES) {
        services[key] = { state: "error", error: "at_capacity" };
        continue;
      }
      const l = this.lease(key);
      this.expireIfDue(now, l);
      services[key] = this.renewOne(now, key, req, entry);
    }

    const peers = {}, fingerprints = {}, hostImages = {};
    for (const [h, rec] of Object.entries(this.hosts)) {
      if (h === host) continue;
      peers[h] = { last_seen: Math.max(0, (now - rec.lastSeen) / 1000) };
      if (rec.fingerprint) fingerprints[h] = rec.fingerprint;
      // Images exist on the wire only for hosts that sent any: an absent
      // key and an empty object both mean "that peer reports nothing", which
      // a new conch reads as no-opinion parity rather than mismatch (the
      // older-conch compatibility case; client.go's PeerImages is nil then).
      if (rec.images && Object.keys(rec.images).length > 0) hostImages[h] = rec.images;
    }
    return { services, peers, fingerprints, hostImages };
  }

  async fetch(request) {
    const body = await request.json();
    const now = Date.now();
    const result = this.batch(now, body);
    if (result.status) {
      const { status, ...rest } = result;
      return new Response(JSON.stringify(rest), {
        status, headers: { "content-type": "application/json" } });
    }
    await this.save(); // exactly one write per batch (presence always changes)
    return new Response(JSON.stringify(result), {
      headers: { "content-type": "application/json" } });
  }
}
