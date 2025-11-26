const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");

/* -------------------------------------------------------
   7-Day Rolling Average Lookup (from brand DB)
--------------------------------------------------------*/
async function get7DayAvgForMetric(brandId, metricName, hour) {
  try {
    const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
      brandId,
    ]);
    if (!rows.length) return null;

    const dbName = rows[0].db_name;

    if (metricName === "aov") {
      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(total_sales / NULLIF(number_of_orders, 0)) AS avg_val
        FROM ${dbName}.hour_wise_sales
        WHERE hour = ?
          AND date >= CURDATE() - INTERVAL 7 DAY
          AND date < CURDATE()
          AND number_of_orders > 0
        `,
        [hour]
      );

      const raw = avgRows[0]?.avg_val;
      if (raw == null) return null;

      const num = Number(raw);
      return Number.isNaN(num) ? null : Number(num.toFixed(2));
    }

    if (metricName === "conversion_rate") {
      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(number_of_orders / NULLIF(number_of_atc_sessions, 0))*100 AS avg_val,
          COUNT(*) AS row_count
        FROM ${dbName}.hour_wise_sales
        WHERE hour = ?
          AND date >= CURDATE() - INTERVAL 7 DAY
          AND date < CURDATE()
          AND number_of_atc_sessions > 0
        `,
        [hour]
      );

      const raw = avgRows[0]?.avg_val;
      const rowCount = avgRows[0]?.row_count ?? 0;

      if (raw == null) return null;

      const num = Number(raw);
      if (Number.isNaN(num)) return null;

      const rounded = Number(num.toFixed(4));

      console.log(
        `✓ conversion_rate 7-day avg for brand ${brandId}, hour ${hour}: ${rounded} from ${rowCount} rows`
      );

      return rounded;
    }

    const columnMap = {
      total_orders: "number_of_orders",
      total_atc_sessions: "number_of_atc_sessions",
      total_sales: "total_sales",
      total_sessions: "number_of_sessions",
    };

    const col = columnMap[metricName];
    if (!col) {
      console.warn(`⚠️ No column mapping for metric: ${metricName}`);
      return null;
    }

    const [avgRows] = await pool.query(
      `
      SELECT AVG(${col}) AS avg_val
      FROM ${dbName}.hour_wise_sales
      WHERE hour = ?
        AND date >= CURDATE() - INTERVAL 7 DAY
        AND date < CURDATE()
      `,
      [hour]
    );

    const raw = avgRows[0]?.avg_val;
    if (raw == null) return null;

    const num = Number(raw);
    if (Number.isNaN(num)) return null;

    console.log(
      `7days average of ${metricName} of brandId ${brandId} is:`,
      num
    );

    return Number(num.toFixed(2));
  } catch (err) {
    console.error(`🔥 7-day avg error for ${metricName}:`, err.message);
    return null;
  }
}

/* -------------------------------------------------------
   Load active alerts for a brand
--------------------------------------------------------*/
async function loadRulesForBrand(brandId) {
  const [rules] = await pool.query(
    "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
    [brandId]
  );
  return rules;
}

/* -------------------------------------------------------
   Parse JSON configs (robust)
--------------------------------------------------------*/
function parseChannelConfig(raw) {
  if (!raw) return null;

  if (typeof raw === "object") return raw;

  if (typeof raw !== "string") {
    console.warn("⚠ channel_config is not string or object:", raw);
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    try {
      const fixed = raw
        .trim()
        .replace(/'/g, '"')
        .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":');
      return JSON.parse(fixed);
    } catch {
      console.warn("⚠ Invalid JSON in channel_config:", raw);
      return null;
    }
  }
}

/* -------------------------------------------------------
   Compute metric
--------------------------------------------------------*/
async function computeMetric(rule, event) {
  try {
    if (rule.metric_type === "base") {
      return event[rule.metric_name];
    }
    if (rule.metric_type === "derived") {
      return evaluate(rule.formula, event);
    }
  } catch (err) {
    console.error("❌ Metric computation error:", err.message);
  }
  return null;
}

function normalizeEventKeys(event) {
  if (!event || typeof event !== "object") return event;

  const normalized = {};

  for (const [key, value] of Object.entries(event)) {
    normalized[key] = value;

    const snake = key.replace(/[A-Z]/g, (char) => `_${char.toLowerCase()}`);
    if (snake !== key && normalized[snake] === undefined) {
      normalized[snake] = value;
    }
  }

  return normalized;
}

/* -------------------------------------------------------
   Cooldown protection
--------------------------------------------------------*/
async function checkCooldown(alertId, cooldownMinutes) {
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

  const minutes = (Date.now() - new Date(rows[0].triggered_at)) / 60000;
  return minutes < cooldownMinutes;
}

/* -------------------------------------------------------
   PREMIUM EMAIL TEMPLATE
--------------------------------------------------------*/
function generateEmailHTML(event, rule, metricValue, avg7, dropPercent) {
  const metricLabel = rule.metric_name.replace(/_/g, " ").toUpperCase();

  const hasAvg = typeof avg7 === "number" && !Number.isNaN(avg7);
  const hasDrop = typeof dropPercent === "number" && !Number.isNaN(dropPercent);

  const formatValue = (val) => {
    if (typeof val === "number") {
      if (val % 1 === 0) return val.toString();
      return val.toFixed(2);
    }
    return val;
  };

  let metricRows = `
    <tr>
      <td style="padding:10px 0; color:#6b7280; font-size:15px;">Current Value</td>
      <td style="padding:10px 0; text-align:right; font-weight:bold; color:#dc2626; font-size:15px;">
        ${formatValue(metricValue)}
      </td>
    </tr>
 `;

  let thresholdDisplay = "";
  if (rule.threshold_type === "percentage_drop") {
    thresholdDisplay = `${rule.threshold_value}% drop`;
  } else if (rule.threshold_type === "percentage_rise") {
    thresholdDisplay = `${rule.threshold_value}% rise`;
  } else {
    thresholdDisplay = formatValue(rule.threshold_value);
  }

  metricRows += `
    <tr>
      <td style="padding:10px 0; color:#6b7280; font-size:15px;">Alert Threshold</td>
      <td style="padding:10px 0; text-align:right; font-weight:bold; font-size:15px;">
        ${thresholdDisplay}
      </td>
    </tr>
  `;

  if (hasAvg) {
    metricRows += `
      <tr>
        <td style="padding:10px 0; color:#6b7280; font-size:15px;">7-Day Average (Same Hour)</td>
        <td style="padding:10px 0; text-align:right; font-weight:bold; font-size:15px;">
          ${formatValue(avg7)}
        </td>
      </tr>
    `;
  }

  if (hasDrop) {
    const dropColor = dropPercent > 0 ? "#e11d48" : "#10b981";
    const dropLabel = dropPercent > 0 ? "Drop" : "Increase";
    metricRows += `
      <tr>
        <td style="padding:10px 0; color:#6b7280; font-size:15px;">Percentage ${dropLabel}</td>
        <td style="padding:10px 0; text-align:right; font-weight:bold; color:${dropColor}; font-size:15px;">
          ${Math.abs(dropPercent).toFixed(2)}%
        </td>
      </tr>
    `;
  }

  return `
  <html>
  <body style="margin:0; padding:0; background:#f4f6fb; font-family:Arial, sans-serif;">
    <div style="max-width:620px; margin:30px auto; background:#ffffff;
      border-radius:12px; overflow:hidden; box-shadow:0 6px 25px rgba(0,0,0,0.08);">

      <div style="background:#4f46e5; padding:26px 32px; color:#ffffff;">
        <h2 style="margin:0; font-size:24px; font-weight:600;">
          ⚠️ Insight alert for ${event.brand}
        </h2>
        <p style="margin:6px 0 0; font-size:14px; opacity:0.9;">
          One of your key activity signals moved more than usual.
        </p>
      </div>

      <div style="padding:30px; line-height:1.6; color:#374151;">
        <p style="font-size:16px;">
          We noticed a change in <strong>${metricLabel}</strong> that may need attention.
        </p>

        <div style="background:#f9fafb; border-radius:10px; padding:20px;
          border:1px solid #e5e7eb; margin-bottom:22px;">
          
          <h3 style="margin:0 0 16px; font-size:18px; font-weight:600; color:#111827;">Alert Details</h3>

          <table style="width:100%; border-collapse:collapse;">
            ${metricRows}
          </table>
        </div>
        
        <div style="background:#fef3c7; border-left:4px solid #f59e0b; padding:12px 16px; margin-bottom:20px; border-radius:6px;">
          <p style="margin:0; font-size:14px; color:#92400e;">
            <strong>Metric:</strong> ${metricLabel}<br>
            <strong>Threshold Type:</strong> ${rule.threshold_type.replace(
              /_/g,
              " "
            )}<br>
            <strong>Severity:</strong> ${rule.severity.toUpperCase()}
          </p>
        </div>

        <p style="font-size:15px; color:#4b5563;">
          This may be temporary, but it’s worth a quick look to ensure everything is running smoothly.
        </p>
      </div>

      <div style="background:#f3f4f6; padding:14px; text-align:center%;">
        <span style="font-size:12px; color:#6b7280;">
          You’re receiving this to stay ahead of store activity trends.<br>
          © ${new Date().getFullYear()} Datum Inc.
        </span>
      </div>
    </div>
  </body>
  </html>
  `;
}

/* -------------------------------------------------------
   Send Email
--------------------------------------------------------*/
async function sendEmail(cfg, subject, html) {
  try {
    if (!cfg || !cfg.to || !Array.isArray(cfg.to) || cfg.to.length === 0) {
      console.error("❌ Invalid email configuration: missing 'to' array", cfg);
      return;
    }

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
    console.error("❌ Email send failed:", err.message);
  }
}

/* -------------------------------------------------------
   Fire Alert
--------------------------------------------------------*/
async function triggerAlert(rule, event, metricValue, avg7, dropPercent) {
  console.log("\n" + "=".repeat(60));
  console.log("🚨 ALERT TRIGGERED");
  console.log("=".repeat(60));
  console.log(`Alert Name: ${rule.name}`);
  console.log(`Brand: ${event.brand} (ID: ${event.brand_id})`);
  console.log(`Metric: ${rule.metric_name} (${rule.metric_type})`);
  console.log(`Current Value: ${metricValue}`);
  console.log(`Threshold Type: ${rule.threshold_type}`);
  console.log(`Threshold Value: ${rule.threshold_value}`);

  if (avg7 !== null && typeof avg7 === "number" && !Number.isNaN(avg7)) {
    console.log(`7-Day Average (same hour): ${avg7.toFixed(2)}`);
  } else {
    console.log("7-Day Average (same hour): N/A");
  }

  if (
    dropPercent !== null &&
    typeof dropPercent === "number" &&
    !Number.isNaN(dropPercent)
  ) {
    console.log(`Drop Percentage: ${dropPercent.toFixed(2)}%`);
  } else {
    console.log("Drop Percentage: N/A");
  }

  console.log(`Severity: ${rule.severity}`);
  console.log(`Cooldown: ${rule.cooldown_minutes} minutes`);
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log("=".repeat(60) + "\n");

  const emailHTML = generateEmailHTML(
    event,
    rule,
    metricValue,
    avg7,
    dropPercent
  );

  const [channels] = await pool.query(
    "SELECT * FROM alert_channels WHERE alert_id = ?",
    [rule.id]
  );

  console.log("Channels fetched for alert", rule.id, channels);

  for (const ch of channels) {
    if (ch.channel_type !== "email") continue;

    console.log("channel_config RAW:", ch.channel_config);

    const cfg = parseChannelConfig(ch.channel_config);

    if (!cfg) {
      console.log(`the cfg is null or invalid for alert ${rule.id}`);
      continue;
    }

    if (!cfg.to || !Array.isArray(cfg.to) || cfg.to.length === 0) {
      console.log(`❌ cfg.to invalid for alert ${rule.id}`, cfg);
      continue;
    }

    console.log(`📧 Sending email to: ${cfg.to.join(", ")}`);
    await sendEmail(cfg, rule.name, emailHTML);
  }

  await pool.query(
    "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
    [rule.id, rule.brand_id, JSON.stringify(event)]
  );

  console.log(`✅ Alert history recorded for alert ID: ${rule.id}\n`);
}

/* -------------------------------------------------------
   Threshold Evaluation
--------------------------------------------------------*/
async function evaluateThreshold(rule, metricValue, avg7, dropPercent) {
  const threshold = Number(rule.threshold_value);

  if (rule.threshold_type === "percentage_drop") {
    if (avg7 == null || dropPercent == null || Number.isNaN(dropPercent)) {
      return false;
    }
    return dropPercent >= threshold;
  }

  if (rule.threshold_type === "percentage_rise") {
    if (avg7 == null || dropPercent == null || Number.isNaN(dropPercent)) {
      return false;
    }
    return -dropPercent >= threshold;
  }

  return metricValue < threshold;
}

/* -------------------------------------------------------
   Main Controller
--------------------------------------------------------*/
async function processIncomingEvent(event) {
  if (typeof event === "string") event = JSON.parse(event);

  event = normalizeEventKeys(event);

  console.log("📥 Event Received:", event);

  const rules = await loadRulesForBrand(event.brand_id);
  const hour = new Date().getHours();

  const metricsNeedingAvg = [
    "total_orders",
    "total_atc_sessions",
    "total_sessions",
    "total_sales",
    "aov",
    "conversion_rate",
  ];

  for (const rule of rules) {
    const metricValue = await computeMetric(rule, event);
    if (metricValue == null) continue;

    let avg7 = null;
    let dropPercent = null;

    if (
      metricsNeedingAvg.includes(rule.metric_name) ||
      rule.threshold_type === "percentage_drop" ||
      rule.threshold_type === "percentage_rise"
    ) {
      avg7 = await get7DayAvgForMetric(event.brand_id, rule.metric_name, hour);

      if (
        avg7 !== null &&
        typeof avg7 === "number" &&
        !Number.isNaN(avg7) &&
        avg7 > 0
      ) {
        dropPercent = ((avg7 - metricValue) / avg7) * 100;
      }
    }

    const shouldTrigger = await evaluateThreshold(
      rule,
      metricValue,
      avg7,
      dropPercent
    );

    console.log("shouldTrigger for rule", rule.id, "=", shouldTrigger);

    if (!shouldTrigger) continue;

    const cooldown = await checkCooldown(rule.id, rule.cooldown_minutes);
    if (cooldown) continue;

    await triggerAlert(rule, event, metricValue, avg7, dropPercent);
  }
}

module.exports = { processIncomingEvent };
