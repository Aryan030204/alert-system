// Simple test publisher for local /qstash/events endpoint.
// Usage:
//   node sendTestEvent.js                      -> sends the built-in sample to http://localhost:5000
//   node sendTestEvent.js --url http://host:port
//   node sendTestEvent.js --json '{"brand_id":3,...}'
// Requires Node 18+ (global fetch) and a QSTASH signing key in .env or env var QSTASH_CURRENT_SIGNING_KEY
require("dotenv").config();
const crypto = require("crypto");

const DEFAULT_URL = "http://localhost:3000/qstash/events";

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { url: DEFAULT_URL, json: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--url" && args[i + 1]) {
      out.url = args[++i];
    } else if (a === "--json" && args[i + 1]) {
      out.json = args[++i];
    }
  }
  return out;
}

function buildSignature(bodyString, signingKey) {
  // Upstash QStash uses a Stripe-like signature format: t={ts},v1={hmac}
  // HMAC is computed over `${ts}.${bodyString}` using SHA256.
  // This mirrors the verification step in the server Receiver.
  const ts = Math.floor(Date.now() / 1000).toString();
  const payload = `${ts}.${bodyString}`;
  const hmac = crypto.createHmac("sha256", signingKey || "");
  hmac.update(payload);
  const sig = hmac.digest("hex");
  return `t=${ts},v1=${sig}`;
}

async function main() {
  const { url, json } = parseArgs();

  const sampleEvent = {
    brand_id: 3,
    brand: "tmc",
    total_sales: 1283735,
    total_orders: 2245,
    aov: 605.8334828021219,
    total_sessions: 9000000, // inflate sessions so total_sales / total_sessions * 100 drops CVR
    total_atc_sessions: 13837,
    gross_sales: 173874.20956420898,
  };

  const eventObj = json ? JSON.parse(json) : sampleEvent;
  const bodyString = JSON.stringify(eventObj);

  const signingKey = process.env.QSTASH_CURRENT_SIGNING_KEY;
  if (!signingKey) {
    console.warn(
      "Warning: QSTASH_CURRENT_SIGNING_KEY not found in env — the server will reject the request unless verification is disabled."
    );
  }

  const signatureHeader = buildSignature(bodyString, signingKey);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Upstash-Signature": signatureHeader,
      },
      body: bodyString,
    });

    const text = await res.text();
    console.log("Status:", res.status);
    console.log("Response body:", text);
  } catch (err) {
    console.error("Request failed:", err.message);
  }
}

main();