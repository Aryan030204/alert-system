const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");

/* -------------------------------------------------------
   Fetch brand DB name
--------------------------------------------------------*/
async function getDbName(brandId) {
  const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
    brandId,
  ]);
  return rows.length ? rows[0].db_name : null;
}

/* -------------------------------------------------------
   Compute window averages:
   - Today's window avg → hours 0..H-1
   - History window avg → previous X days same window
--------------------------------------------------------*/
async function getWindowAverages(brandId, metricName, hour, lookbackDays) {
  try {
    const dbName = await getDbName(brandId);
    if (!dbName) return { todayAvg: null, historyAvg: null };

    const windowEnd = hour; // hours 0..hour-1

    const columnMap = {
      total_orders: "number_of_orders",
      total_atc_sessions: "number_of_atc_sessions",
      total_sessions: "number_of_sessions",
      total_sales: "total_sales",
    };

    let col = columnMap[metricName];

    /* ------------------ Derived Metrics ------------------ */

    // AOV
    if (metricName === "aov") {
      const [today] = await pool.query(
        `
        SELECT AVG(total_sales / NULLIF(number_of_orders, 0)) AS avg_val
        FROM ${dbName}.hour_wise_sales
        WHERE date = CURDATE() AND hour < ?
          AND number_of_orders > 0
        `,
        [windowEnd]
      );

      const [hist] = await pool.query(
        `
        SELECT AVG(total_sales / NULLIF(number_of_orders, 0)) AS avg_val
        FROM ${dbName}.hour_wise_sales
        WHERE date >= CURDATE() - INTERVAL ? DAY
          AND date < CURDATE()
          AND hour < ?
          AND number_of_orders > 0
        `,
        [lookbackDays, windowEnd]
      );

      return {
        todayAvg: today[0].avg_val ? Number(today[0].avg_val) : null,
        historyAvg: hist[0].avg_val ? Number(hist[0].avg_val) : null,
      };
    }

    // Conversion Rate
    if (metricName === "conversion_rate") {
      const [today] = await pool.query(
        `
        SELECT AVG(number_of_orders / NULLIF(number_of_sessions, 0)) * 100 AS avg_val
        FROM ${dbName}.hour_wise_sales
        WHERE date = CURDATE() AND hour < ?
          AND number_of_sessions > 0
        `,
        [windowEnd]
      );

      const [hist] = await pool.query(
        `
        SELECT AVG(number_of_orders / NULLIF(number_of_sessions, 0)) * 100 AS avg_val
        FROM ${dbName}.hour_wise_sales
        WHERE date >= CURDATE() - INTERVAL ? DAY
          AND date < CURDATE()
          AND hour < ?
          AND number_of_sessions > 0
        `,
        [lookbackDays, windowEnd]
      );

      return {
        todayAvg: today[0].avg_val ? Number(today[0].avg_val) : null,
        historyAvg: hist[0].avg_val ? Number(hist[0].avg_val) : null,
      };
    }

    /* ------------------ Base Metrics ------------------ */

    if (!col) return { todayAvg: null, historyAvg: null };

    const [today] = await pool.query(
      `
      SELECT AVG(${col}) AS avg_val
      FROM ${dbName}.hour_wise_sales
      WHERE date = CURDATE() AND hour < ?
      `,
      [windowEnd]
    );

    const [hist] = await pool.query(
      `
      SELECT AVG(${col}) AS avg_val
      FROM ${dbName}.hour_wise_sales
      WHERE date >= CURDATE() - INTERVAL ? DAY
        AND date < CURDATE()
        AND hour < ?
      `,
      [lookbackDays, windowEnd]
    );

    return {
      todayAvg: today[0].avg_val ? Number(today[0].avg_val) : null,
      historyAvg: hist[0].avg_val ? Number(hist[0].avg_val) : null,
    };
  } catch (err) {
    console.error("🔥 window avg error:", err.message);
    return { todayAvg: null, historyAvg: null };
  }
}

/* -------------------------------------------------------
   Load active alerts
--------------------------------------------------------*/
async function loadRules(brandId) {
  const [rows] = await pool.query(
    "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
    [brandId]
  );
  return rows;
}

/* -------------------------------------------------------
   JSON Parsing for channel_config
--------------------------------------------------------*/
function parseConfig(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;

  try {
    return JSON.parse(raw);
  } catch {
    try {
      return JSON.parse(
        raw
          .replace(/'/g, '"')
          .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":')
      );
    } catch {
      return null;
    }
  }
}

/* -------------------------------------------------------
   Compute a metric from event or formula
--------------------------------------------------------*/
async function computeMetric(rule, event) {
  try {
    if (rule.metric_type === "base") return event[rule.metric_name];

    if (rule.metric_type === "derived") return evaluate(rule.formula, event);
  } catch (err) {
    console.error("❌ metric compute error", err.message);
  }
  return null;
}

/* -------------------------------------------------------
   Cooldown
--------------------------------------------------------*/
async function checkCooldown(alertId, minutes) {
  const [rows] = await pool.query(
    `
    SELECT triggered_at
    FROM alert_history
    WHERE alert_id = ?
    ORDER BY triggered_at DESC
    LIMIT 1
    `,
    [alertId]
  );
  if (!rows.length) return false;

  const diff = (Date.now() - new Date(rows[0].triggered_at)) / 60000;
  return diff < minutes;
}

/* -------------------------------------------------------
   Email sender
--------------------------------------------------------*/
async function sendEmail(cfg, subject, html) {
  try {
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.ALERT_EMAIL_USER,
        pass: process.env.ALERT_EMAIL_PASS,
      },
    });

    await transporter.sendMail({
      from: `"Alerting System" <${
        cfg.smtp_user || process.env.ALERT_EMAIL_USER
      }>`,
      to: cfg.to.join(","),
      subject,
      html,
    });

    console.log("📧 Email sent!");
  } catch (err) {
    console.error("❌ email error:", err.message);
  }
}

/* -------------------------------------------------------
   Trigger an alert
--------------------------------------------------------*/
async function triggerAlert(rule, event, metricValue, histAvg, dropPercent) {
  console.log("\n🚨 ALERT TRIGGERED:", rule.name);

  const [channels] = await pool.query(
    "SELECT * FROM alert_channels WHERE alert_id = ?",
    [rule.id]
  );

  for (const ch of channels) {
    if (ch.channel_type !== "email") continue;

    const cfg = parseConfig(ch.channel_config);
    if (!cfg?.to?.length) continue;

    await sendEmail(
      cfg,
      rule.name,
      `
      <h1>${rule.name}</h1>
      <p>Brand: ${event.brand}</p>
      <p>Current Value: ${metricValue}</p>
      <p>Historical Avg: ${histAvg}</p>
      <p>Drop %: ${dropPercent?.toFixed(2)}</p>
    `
    );
  }

  await pool.query(
    "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
    [rule.id, rule.brand_id, JSON.stringify(event)]
  );
}

/* -------------------------------------------------------
   Compare with threshold
--------------------------------------------------------*/
function evaluateThreshold(rule, metricValue, histAvg, dropPercent) {
  const threshold = Number(rule.threshold_value);

  if (rule.threshold_type === "percentage_drop") {
    return dropPercent >= threshold;
  }

  if (rule.threshold_type === "percentage_rise") {
    return -dropPercent >= threshold;
  }

  return metricValue < threshold;
}

/* -------------------------------------------------------
   Main processor
--------------------------------------------------------*/
async function processIncomingEvent(event) {
  if (typeof event === "string") event = JSON.parse(event);

  const rules = await loadRules(event.brand_id);
  const hour = new Date().getHours();

  for (const rule of rules) {
    const metricValueRaw = await computeMetric(rule, event);
    if (metricValueRaw == null) continue;

    const lookback = rule.lookback_days || 7;

    const { todayAvg, historyAvg } = await getWindowAverages(
      event.brand_id,
      rule.metric_name,
      hour,
      lookback
    );

    const metricValue = todayAvg ?? metricValueRaw;
    const histAvg = historyAvg;

    let dropPercent = null;
    if (histAvg && metricValue && histAvg > 0)
      dropPercent = ((histAvg - metricValue) / histAvg) * 100;

    const shouldTrigger = evaluateThreshold(
      rule,
      metricValue,
      histAvg,
      dropPercent
    );

    if (!shouldTrigger) continue;
    if (await checkCooldown(rule.id, rule.cooldown_minutes)) continue;

    await triggerAlert(rule, event, metricValue, histAvg, dropPercent);
  }
}

module.exports = { processIncomingEvent };
