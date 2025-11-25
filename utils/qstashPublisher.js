const { Client } = require("@upstash/qstash");

const qstashClient = new Client({
  token: process.env.QSTASH_TOKEN,
  baseUrl: process.env.QSTASH_URL || "https://qstash.upstash.io",
});

async function enqueueEvent(event) {
  const topic = process.env.QSTASH_TOPIC;
  if (!topic) {
    throw new Error("Missing QSTASH_TOPIC env var");
  }

  return qstashClient.publishJSON({
    topic,
    body: event,
    retries: 5,
  });
}

module.exports = { enqueueEvent };
