require("dotenv").config({ path: "./.env" });
const express = require("express");
const { startQueueWorker } = require("./streamConsumer");

const app = express();
app.use(express.json());

app.get("/", (req, res) => res.send("Alerting System Running"));

startQueueWorker();

app.listen(process.env.PORT || 5000, () =>
  console.log(`🚀 Alerting Server running on port ${process.env.PORT}`)
);
