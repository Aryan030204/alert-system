require("dotenv").config({ path: "./.env" });
const express = require("express");
const { Receiver } = require("@upstash/qstash");
const { processIncomingEvent, getAllRules, TEST_MODE, TEST_EMAIL } = require("./alertEngine");

const app = express();

const receiver = new Receiver({
  currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
  nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
});

app.get("/", (req, res) => res.send("Alerting System Running"));

app.get("/rules", async (req, res) => {
  try {
    const rules = await getAllRules();
    res.json(rules);
  } catch (err) {
    console.error("Error fetching rules:", err);
    res.status(500).json({ error: "Failed to fetch rules" });
  }
});

app.post(
  "/qstash/events",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      const signature = req.header("Upstash-Signature");
      if (!signature) {
        return res.status(400).send("Missing signature header");
      }

      const bodyString = req.body.toString();
      // await receiver.verify({
      //   body: bodyString,
      //   signature,
      // });

      const payload = JSON.parse(bodyString);
      await processIncomingEvent(payload);

      return res.status(200).json({ status: "ok" });
    } catch (err) {
      console.error("QStash webhook error:", err.message);
      return res.status(500).send("Webhook processing failed");
    }
  }
);

app.use(express.json());

app.listen(process.env.PORT || 5000, () => {
  console.log(`🚀 Alerting Server running on port ${process.env.PORT}`);
  if (TEST_MODE) {
    console.log(`🧪 TEST MODE ENABLED: Alerts will be sent ONLY to ${TEST_EMAIL}`);
  }
});
