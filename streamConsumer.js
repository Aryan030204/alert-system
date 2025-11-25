const { Redis } = require("@upstash/redis");
const { processIncomingEvent } = require("./alertEngine.js");

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

async function startQueueWorker() {
  console.log("⚡ Upstash Queue Worker Running...");

  while (true) {
    try {
      // Use lpop directly instead of eval for better REST API compatibility
      const raw = await redis.lpop("summary_updates_queue");

      if (raw) {
        console.log("📥 Received:", raw);
        // Parse if it's a string, otherwise use as-is
        let event = raw;
        if (typeof raw === "string") {
          try {
            event = JSON.parse(raw);
          } catch (e) {
            console.error("❌ Failed to parse event:", e);
            continue;
          }
        }
        await processIncomingEvent(event);
      } else {
        // No job → wait a bit before polling again
        await new Promise((r) => setTimeout(r, 1000));
      }
    } catch (err) {
      console.error("Queue Error:", err);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

module.exports = { startQueueWorker };
