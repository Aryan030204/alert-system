require("dotenv").config({ path: "./.env" });
const { Receiver } = require("@upstash/qstash");

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { url: "http://localhost:5000/qstash/events", json: null, perf: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--url" && args[i + 1]) {
      out.url = args[++i];
    } else if (a === "--json" && args[i + 1]) {
      out.json = args[++i];
    } else if (a === "--perf" && args[i + 1]) {
      out.perf = Number(args[++i]);
    }
  }
  return out;
}

async function main() {
  const { url, json, perf } = parseArgs();

  const sampleEventPerformance = {
    brand_key: "PTS",
    brand_name: "SkincarePersonalTouch",
    performance: 75,
    cls: 0.021,
    fcp: 10.69,
    inp: 0,
    lcp: 21.81,
    ttfb: 0.03,
    date: "2025-12-24",
    time: "19:30:00"
  };

  const eventObj = json ? JSON.parse(json) : sampleEventPerformance;
  if (perf != null && !json) eventObj.performance = perf;
  
  const bodyString = JSON.stringify(eventObj);

  const signingKey = process.env.QSTASH_CURRENT_SIGNING_KEY;
  if (!signingKey) {
    console.error("Missing QSTASH_CURRENT_SIGNING_KEY in .env");
    process.exit(1);
  }

  const receiver = new Receiver({
    currentSigningKey: signingKey,
    nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY || "",
  });

  const signature = "dummy-signature";

  console.log("Sending event to:", url);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Upstash-Signature": signature,
      },
      body: bodyString,
    });

    console.log("Status:", res.status);
    const body = await res.text();
    console.log("Response body:", body);
  } catch (err) {
    console.error("Request failed:", err.message);
  }
}

main();
