const crypto = require("crypto");

const EVENT_TYPE = "alert.fired";
const EVENT_SOURCE = "alerts-pipeline";
const SCHEMA_VERSION = "1";

function toIsoString(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function toNumberIfNumeric(value) {
  if (value === null || value === undefined || value === "") return value;
  const num = Number(value);
  return Number.isNaN(num) ? value : num;
}

function getFirstDefined(obj, keys) {
  if (!obj) return undefined;
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) return obj[key];
  }
  return undefined;
}

function parseBrandsConfig(raw = process.env.BRANDS_CONFIG) {
  if (!raw) {
    throw new Error("BRANDS_CONFIG env var is required to resolve tenantId for alert.fired events");
  }

  let parsed;
  try {
    parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch (err) {
    throw new Error(`Invalid BRANDS_CONFIG JSON: ${err.message}`);
  }

  if (!Array.isArray(parsed)) {
    throw new Error("BRANDS_CONFIG must be a JSON array");
  }

  return parsed;
}

function resolveTenantIdFromBrandId(brandId, brandsConfig) {
  const normalizedBrandId = Number(brandId);
  if (!Number.isFinite(normalizedBrandId)) {
    throw new Error(`Invalid brandId for tenant mapping: ${brandId}`);
  }

  const config = brandsConfig || parseBrandsConfig();
  const match = config.find((item) => Number(item.brandId) === normalizedBrandId);
  if (!match || !match.key) {
    throw new Error(`Missing tenant mapping for brandId ${normalizedBrandId} in BRANDS_CONFIG`);
  }

  return String(match.key);
}

function extractWindow(input, prefix) {
  const direct = input?.[prefix];
  if (direct && direct.start && direct.end) {
    const start = toIsoString(direct.start);
    const end = toIsoString(direct.end);
    if (start && end) return { start, end };
  }

  const start = getFirstDefined(input, [`${prefix}Start`, `${prefix}_start`]);
  const end = getFirstDefined(input, [`${prefix}End`, `${prefix}_end`]);
  const startIso = toIsoString(start);
  const endIso = toIsoString(end);
  if (startIso && endIso) return { start: startIso, end: endIso };

  return undefined;
}

function extractCorrelationFields(event) {
  return {
    traceId: getFirstDefined(event, ["traceId", "trace_id"]),
    correlationId: getFirstDefined(event, [
      "correlationId",
      "correlation_id",
      "requestId",
      "request_id",
    ]),
  };
}

function createEventId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString("hex");
}

function buildEvaluationWindowMarker({ event, alertHour }) {
  if (event?.window?.end || event?.window_end || event?.windowEnd) {
    return (
      toIsoString(event.window?.end) ||
      toIsoString(event.window_end) ||
      toIsoString(event.windowEnd)
    );
  }

  const eventDate = getFirstDefined(event, ["date", "event_date"]);
  if (!eventDate) return null;

  const explicitTime = getFirstDefined(event, ["time", "event_time"]);
  if (explicitTime) {
    return `${eventDate}T${String(explicitTime)}`;
  }

  if (typeof alertHour === "number" && alertHour >= 0 && alertHour <= 23) {
    return `${eventDate}T${String(alertHour).padStart(2, "0")}:00:00`;
  }

  return String(eventDate);
}

function hashDeterministicParts(parts) {
  return crypto.createHash("sha256").update(JSON.stringify(parts)).digest("hex").slice(0, 20);
}

function buildAlertFiredIdempotencyKey({
  alertId,
  incidentId,
  firedAt,
  firedAtStable = false,
  evaluationWindowEnd,
  thresholdType,
  thresholdValue,
  event,
  alertHour,
}) {
  const normalizedAlertId = Number(alertId);
  if (!Number.isFinite(normalizedAlertId)) {
    throw new Error(`Invalid alertId for idempotency key: ${alertId}`);
  }

  const resolvedIncidentId = incidentId || getFirstDefined(event, ["incidentId", "incident_id"]);
  if (resolvedIncidentId) {
    return `alert-fired:${normalizedAlertId}:${resolvedIncidentId}`;
  }

  if (firedAtStable) {
    const firedAtIso = toIsoString(firedAt);
    if (firedAtIso) {
      return `alert-fired:${normalizedAlertId}:${firedAtIso}`;
    }
  }

  const windowMarker = evaluationWindowEnd || buildEvaluationWindowMarker({ event, alertHour });
  if (windowMarker) {
    return `alert-fired:${normalizedAlertId}:${windowMarker}:${thresholdType}:${thresholdValue}`;
  }

  const fallbackHash = hashDeterministicParts({
    alertId: normalizedAlertId,
    thresholdType,
    thresholdValue,
    alertHour,
    eventDate: getFirstDefined(event, ["date", "event_date"]),
    eventTime: getFirstDefined(event, ["time", "event_time"]),
    eventBrandId: getFirstDefined(event, ["brand_id", "brandId"]),
    eventKeys: Object.keys(event || {}).sort(),
  });
  return `alert-fired:${normalizedAlertId}:${fallbackHash}`;
}

function validateRequiredFields(envelope) {
  const requiredTopLevel = [
    "eventId",
    "eventType",
    "occurredAt",
    "source",
    "idempotencyKey",
    "tenantId",
    "brandId",
    "alertId",
    "payload",
  ];

  for (const key of requiredTopLevel) {
    if (envelope[key] === undefined || envelope[key] === null || envelope[key] === "") {
      throw new Error(`alert.fired envelope missing required field: ${key}`);
    }
  }

  const requiredPayload = [
    "alertName",
    "alertType",
    "scope",
    "status",
    "severity",
    "metricName",
    "thresholdType",
    "thresholdValue",
    "observedValue",
    "firedAt",
  ];

  for (const key of requiredPayload) {
    if (envelope.payload[key] === undefined || envelope.payload[key] === null || envelope.payload[key] === "") {
      throw new Error(`alert.fired payload missing required field: ${key}`);
    }
  }
}

function normalizeAlertFiredEvent(input, options = {}) {
  const { brandsConfig } = options;
  const rule = input?.rule || {};
  const event = input?.event || {};
  const firedAt = input?.firedAt || new Date();
  const firedAtIso = toIsoString(firedAt);

  if (!firedAtIso) {
    throw new Error("Invalid firedAt for alert.fired event");
  }

  const alertId = Number(rule.id ?? rule.alert_id);
  const brandId = Number(rule.brand_id ?? event.brand_id ?? event.brandId);
  if (!Number.isFinite(alertId)) {
    throw new Error(`Invalid alertId in fired alert input: ${rule.id ?? rule.alert_id}`);
  }
  if (!Number.isFinite(brandId)) {
    throw new Error(`Invalid brandId in fired alert input: ${rule.brand_id ?? event.brand_id ?? event.brandId}`);
  }

  const tenantId = resolveTenantIdFromBrandId(brandId, brandsConfig);
  const alertType = String(rule.alert_type || rule.alertType || rule.metric_name || "").trim();
  if (!alertType) {
    throw new Error(`Missing alertType for alertId ${alertId}; expected rule.alert_type or rule.metric_name`);
  }

  const severity = String(
    rule.severity || (String(input.newState || "").toUpperCase() === "CRITICAL" ? "critical" : "medium")
  ).toLowerCase();

  const metricName = String(rule.metric_name || rule.metricName || alertType);
  const thresholdType = String(rule.threshold_type || rule.thresholdType || "unknown");
  const thresholdValue = toNumberIfNumeric(rule.threshold_value ?? rule.thresholdValue);
  const observedValue = toNumberIfNumeric(input.metricValue ?? input.observedValue ?? input.triggerValue);

  const payload = {
    alertName: String(rule.name || rule.alert_name || `${metricName} alert`),
    alertType,
    // TODO: Make scope dynamic when upstream alerts API/pipeline supports multi/global scopes.
    scope: "single",
    status: "triggered",
    severity,
    metricName,
    thresholdType,
    thresholdValue,
    observedValue,
    firedAt: firedAtIso,
  };

  const window = input.window || extractWindow(input, "window") || extractWindow(event, "window");
  if (window) payload.window = window;

  const baselineWindow =
    input.baselineWindow ||
    extractWindow(input, "baselineWindow") ||
    extractWindow(event, "baselineWindow") ||
    extractWindow(event, "baseline_window");
  if (baselineWindow) payload.baselineWindow = baselineWindow;

  const context = {
    previousState: input.previousState,
    newState: input.newState,
    historicValue: toNumberIfNumeric(input.avgHistoric),
    dropPercent: toNumberIfNumeric(input.dropPercent),
    alertHour: typeof input.alertHour === "number" ? input.alertHour : undefined,
    metricType: rule.metric_type,
  };
  const compactContext = Object.fromEntries(
    Object.entries(context).filter(([, v]) => v !== undefined && v !== null && v !== "")
  );
  if (Object.keys(compactContext).length) {
    payload.context = compactContext;
  }

  const correlation = extractCorrelationFields(event);
  const envelope = {
    eventId: createEventId(),
    eventType: EVENT_TYPE,
    occurredAt: firedAtIso,
    source: EVENT_SOURCE,
    idempotencyKey: buildAlertFiredIdempotencyKey({
      alertId,
      incidentId: input.incidentId,
      firedAt,
      firedAtStable: Boolean(input.firedAtStable),
      evaluationWindowEnd: input.evaluationWindowEnd || payload.window?.end,
      thresholdType,
      thresholdValue,
      event,
      alertHour: input.alertHour,
    }),
    tenantId,
    brandId,
    alertId,
    schemaVersion: SCHEMA_VERSION,
    payload,
  };

  if (correlation.traceId) envelope.traceId = String(correlation.traceId);
  if (correlation.correlationId) envelope.correlationId = String(correlation.correlationId);

  validateRequiredFields(envelope);
  return envelope;
}

module.exports = {
  normalizeAlertFiredEvent,
  buildAlertFiredIdempotencyKey,
  resolveTenantIdFromBrandId,
  parseBrandsConfig,
};
