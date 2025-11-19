const { Redis } = require("@upstash/redis");
const { processIncomingEvent } = require("./alertEngine.js");

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// Lua script: POP from queue if exists, else return nil
const POP_LUA = `
  local key = KEYS[1]
  local item = redis.call("LPOP", key)
  return item
`;

async function startQueueWorker() {
  console.log("⚡ Upstash Queue Worker Running...");

  while (true) {
    try {
      const raw = await redis.eval(POP_LUA, ["summary_updates_queue"], []);

      if (raw) {
        console.log("📥 Received:", raw);
        const evt = JSON.stringify(raw);
        await processIncomingEvent(raw);
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

module.exports =  { startQueueWorker };
