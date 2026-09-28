/* -------------------------------------------------------
   Resilience helpers for when MongoDB writes fail (e.g. the Atlas
   cluster hits its storage cap and blocks all writes). Pure/stateless
   with respect to email — no dependency on alertEngine.js so it can be
   unit-tested standalone.

   In-memory dedup guard
   ----------------------
   The real cooldown/dedup lives in MongoDB (alert_history, current_state,
   cod_alert_history). When a write to one of those silently fails, the
   next incoming event re-reads stale data and can decide to fire again —
   this guard is a short-lived, in-process safety net layered on top,
   armed the moment a fire is *decided* (before any Mongo write is even
   attempted), so a burst of events during an active outage can't slip
   through just because the DB-side record never landed. It resets on
   process restart; it is not a replacement for the persistent cooldown.
--------------------------------------------------------*/

const recentFireGuard = new Map(); // key -> expiresAt (ms epoch)

function isGuardActive(key) {
  const expiresAt = recentFireGuard.get(key);
  if (expiresAt == null) return false;
  if (Date.now() >= expiresAt) {
    recentFireGuard.delete(key);
    return false;
  }
  return true;
}

function armGuard(key, durationMs) {
  if (!(durationMs > 0)) return;
  recentFireGuard.set(key, Date.now() + durationMs);
}

/* -------------------------------------------------------
   TTL indexes for this repo's own Mongo collections. Not today's
   storage cause, but keeps this system's footprint on a shared cluster
   from becoming a future version of the same problem. Idempotent —
   safe to call every time a fresh client connects.
--------------------------------------------------------*/

const DAY_MS = 24 * 60 * 60 * 1000;

async function ensureMongoIndexes(db) {
  const specs = [
    { collection: "alert_history", field: "triggered_at", expireAfterSeconds: (90 * DAY_MS) / 1000 },
    { collection: "push_notifications", field: "created_at", expireAfterSeconds: (30 * DAY_MS) / 1000 },
    { collection: "cod_alert_history", field: "triggered_at", expireAfterSeconds: (60 * DAY_MS) / 1000 },
  ];

  for (const { collection, field, expireAfterSeconds } of specs) {
    try {
      await db.collection(collection).createIndex({ [field]: 1 }, { expireAfterSeconds, background: true });
    } catch (err) {
      console.warn(`⚠️ Failed to ensure TTL index on ${collection}.${field}:`, err.message);
    }
  }
}

module.exports = { isGuardActive, armGuard, ensureMongoIndexes };
