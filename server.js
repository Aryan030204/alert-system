require("dotenv").config({ path: "./.env" });
const express = require("express");
const { Receiver } = require("@upstash/qstash");
const { processIncomingEvent } = require("./alertEngine");

const app = express();

const receiver = new Receiver({
  currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
  nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
});

app.get("/", (req, res) => res.send("Alerting System Running"));

app.post(
  "/qstash/events",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      const signature = req.header("Upstash-Signature");
      // 🔥 Bypass signature for Postman
      if (process.env.SKIP_QSTASH_SIGNATURE === "true") {
        const payload = JSON.parse(req.body.toString());
        await processIncomingEvent(payload);
        return res.status(200).json({ status: "ok", bypass: true });
      }
      if (!signature) {
        return res.status(400).send("Missing signature header");
      }

      const bodyString = req.body.toString();
      await receiver.verify({
        body: bodyString,
        signature,
      });

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

app.listen(process.env.PORT || 5000, () =>
  console.log(`🚀 Alerting Server running on port ${process.env.PORT}`)
);
