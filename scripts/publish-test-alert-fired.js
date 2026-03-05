require("dotenv").config({ path: "./.env" });
const crypto = require("crypto");
const {
  resolveTenantIdFromBrandId,
} = require("../utils/alertFiredEventNormalizer");
const { rabbitmqPublisher } = require("../utils/rabbitmqPublisher");

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {
    brandId: 3,
    alertId: 987,
    alertType: "cvr_drop",
    metricName: "cvr",
    thresholdType: "below_pct",
    thresholdValue: -15,
    observedValue: -18.3,
    alertName: "CVR drop > 15%",
    severity: "high",
  };

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--brandId" && args[i + 1]) out.brandId = Number(args[++i]);
    else if (a === "--alertId" && args[i + 1]) out.alertId = Number(args[++i]);
    else if (a === "--alertType" && args[i + 1]) out.alertType = String(args[++i]);
    else if (a === "--metricName" && args[i + 1]) out.metricName = String(args[++i]);
    else if (a === "--thresholdType" && args[i + 1]) out.thresholdType = String(args[++i]);
    else if (a === "--thresholdValue" && args[i + 1]) out.thresholdValue = Number(args[++i]);
    else if (a === "--observedValue" && args[i + 1]) out.observedValue = Number(args[++i]);
    else if (a === "--alertName" && args[i + 1]) out.alertName = String(args[++i]);
    else if (a === "--severity" && args[i + 1]) out.severity = String(args[++i]);
  }

  return out;
}

function buildTestEvent(input) {
  const now = new Date();
  const firedAt = now.toISOString();
  const todayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())).toISOString();
  const yesterdayStart = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1)
  ).toISOString();
  const twoDaysAgoStart = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 2)
  ).toISOString();

  const tenantId = resolveTenantIdFromBrandId(input.brandId);
  const idempotencyKey = `alert-fired:${input.alertId}:${firedAt}`;

  return {
    eventId: typeof crypto.randomUUID === "function" ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"),
    eventType: "alert.fired",
    occurredAt: firedAt,
    source: "alerts-pipeline",
    idempotencyKey,
    tenantId,
    brandId: input.brandId,
    alertId: input.alertId,
    schemaVersion: "1",
    payload: {
      alertName: input.alertName,
      alertType: input.alertType,
      scope: "single",
      status: "triggered",
      severity: input.severity,
      metricName: input.metricName,
      thresholdType: input.thresholdType,
      thresholdValue: input.thresholdValue,
      observedValue: input.observedValue,
      firedAt,
      window: {
        start: yesterdayStart,
        end: todayStart,
      },
      baselineWindow: {
        start: twoDaysAgoStart,
        end: yesterdayStart,
      },
    },
  };
}

async function main() {
  const input = parseArgs();
  const event = buildTestEvent(input);

  console.log("Publishing test alert.fired event:", {
    eventType: event.eventType,
    tenantId: event.tenantId,
    brandId: event.brandId,
    alertId: event.alertId,
    idempotencyKey: event.idempotencyKey,
    exchange: process.env.RABBITMQ_EXCHANGE || "alerts.events",
    publishDisabled: process.env.RABBITMQ_PUBLISH_DISABLED,
  });

  try {
    await rabbitmqPublisher.publishAlertFiredEvent(event);
    console.log("✅ alert.fired test event published");
  } finally {
    await rabbitmqPublisher.close();
  }
}

main().catch((err) => {
  console.error("❌ Failed to publish test alert.fired event:", err.message);
  process.exit(1);
});
