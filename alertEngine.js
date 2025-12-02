const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");

/* -------------------------------------------------------
   Historical Average Lookup (using lookback_days)
--------------------------------------------------------*/
async function getHistoricalAvgForMetric(
  brandId,
  metricName,
  hourCutoff,
  lookbackDays
) {
  try {
    if (hourCutoff <= 0) return null;

    const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
      brandId,
    ]);
    if (!rows.length) return null;

    const dbName = rows[0].db_name;
    const days = Number(lookbackDays) > 0 ? Number(lookbackDays) : 7;

    // AOV AVG
    if (metricName === "aov") {
      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(daily_aov) AS avg_val,
          COUNT(*) AS day_count
        FROM (
          SELECT 
            date,
            SUM(total_sales) / NULLIF(SUM(number_of_orders), 0) AS daily_aov
          FROM ${dbName}.hour_wise_sales
          WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
            AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
            AND hour < ?
          GROUP BY date
          HAVING SUM(number_of_orders) > 0
        ) AS t;
        `,
        [days, hourCutoff]
      );

      const raw = avgRows[0]?.avg_val;
      const dayCount = avgRows[0]?.day_count ?? 0;
      if (!raw || dayCount === 0) return null;

      const rounded = Number(Number(raw).toFixed(2));
      console.log(
        `✓ AOV historical avg for brand ${brandId} (last ${days} days): ${rounded}`
      );
      return rounded;
    }

    // CVR AVG
    if (metricName === "conversion_rate") {
      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(daily_cvr) AS avg_val,
          COUNT(*) AS day_count
        FROM (
          SELECT 
            date,
            (SUM(number_of_orders) / NULLIF(SUM(number_of_sessions), 0)) * 100 AS daily_cvr
          FROM ${dbName}.hour_wise_sales
          WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
            AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
            AND hour < ?
          GROUP BY date
          HAVING SUM(number_of_sessions) > 0
        ) AS t;
        `,
        [days, hourCutoff]
      );

      const raw = avgRows[0]?.avg_val;
      const dayCount = avgRows[0]?.day_count ?? 0;
      if (!raw || dayCount === 0) return null;

      const rounded = Number(Number(raw).toFixed(4));
      console.log(
        `✓ conversion_rate historical avg for brand ${brandId} = ${rounded}`
      );
      return rounded;
    }

    // BASE metrics
    const map = {
      total_orders: "number_of_orders",
      total_atc_sessions: "number_of_atc_sessions",
      total_sales: "total_sales",
      total_sessions: "number_of_sessions",
    };

    const col = map[metricName];
    if (!col) return null;

    const [avgRows] = await pool.query(
      `
      SELECT 
        AVG(daily_val) AS avg_val,
        COUNT(*) AS day_count
      FROM (
        SELECT 
          date,
          SUM(${col}) AS daily_val
        FROM ${dbName}.hour_wise_sales
        WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
          AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
          AND hour < ?
        GROUP BY date
      ) AS t;
      `,
      [days, hourCutoff]
    );

    const raw = avgRows[0]?.avg_val;
    const dayCount = avgRows[0]?.day_count ?? 0;
    if (!raw || dayCount === 0) return null;

    const rounded = Number(Number(raw).toFixed(2));
    console.log(`✓ ${metricName} historical avg = ${rounded}`);
    return rounded;
  } catch (err) {
    console.error(`🔥 historical avg error for ${metricName}:`, err.message);
    return null;
  }
}

/* -------------------------------------------------------
   Load Active Alerts
--------------------------------------------------------*/
async function loadRulesForBrand(brandId) {
  const [rules] = await pool.query(
    "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
    [brandId]
  );
  return rules;
}

/* -------------------------------------------------------
   Parse channel_config
--------------------------------------------------------*/
function parseChannelConfig(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;

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
   Compute Metric
--------------------------------------------------------*/
async function computeMetric(rule, event) {
  try {
    if (rule.metric_type === "base") return event[rule.metric_name];
    if (rule.metric_type === "derived") return evaluate(rule.formula, event);
  } catch (err) {
    console.error("❌ Metric computation error:", err.message);
  }
  return null;
}

/* -------------------------------------------------------
   Normalize keys
--------------------------------------------------------*/
function normalizeEventKeys(event) {
  if (!event) return event;
  const normalized = {};

  for (const [k, v] of Object.entries(event)) {
    normalized[k] = v;
    const snake = k.replace(/[A-Z]/g, (ch) => `_${ch.toLowerCase()}`);
    if (snake !== k) normalized[snake] = v;
  }
  return normalized;
}

/* -------------------------------------------------------
   Cooldown
--------------------------------------------------------*/
async function checkCooldown(alertId, minutesCfg) {
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

  const mins = (Date.now() - new Date(rows[0].triggered_at)) / 60000;
  return mins < minutesCfg;
}

/* -------------------------------------------------------
   Email HTML
--------------------------------------------------------*/
function generateEmailHTML(
  event,
  rule,
  metricValue,
  avgHistoric,
  dropPercent,
  alertHour
) {
  const brandName = String(event.brand || "").toUpperCase();
  const metricLabel = rule.metric_name.replace(/_/g, " ").toUpperCase();

  const hasAvg = typeof avgHistoric === "number" && !Number.isNaN(avgHistoric);
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
        <td style="padding:10px 0; color:#6b7280; font-size:15px;">Historical Avg (${
          rule.lookback_days
        } days)</td>
        <td style="padding:10px 0; text-align:right; font-weight:bold; font-size:15px;">
          ${formatValue(avgHistoric)}
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
          ⚠️ Insight alert for ${brandName}
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
            ${
              typeof alertHour === "number"
                ? `<br><strong>Hour:</strong> ${alertHour} (data up to hour ${Math.max(
                    0,
                    alertHour
                  )}h)`
                : ""
            }
          </p>
        </div>

        <!-- ⭐ ADDED THIS BLOCK -->
        <p style="font-size:15px; color:#4b5563; margin-top:20px;">
  Take a look at the latest activity on your dashboard for possible causes: 
  <a href="https://datum.trytechit.co/" style="color:#4f46e5; text-decoration:underline;">
    https://datum.trytechit.co/
  </a>
</p>
      </div>

      <div style="background:#f3f4f6; padding:14px; text-align:center;">
        <span style="font-size:12px; color:#6b7280;">
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
   Trigger Alert
--------------------------------------------------------*/
async function triggerAlert(
  rule,
  event,
  metricValue,
  avgHistoric,
  dropPercent,
  alertHour
) {
  const emailHTML = generateEmailHTML(
    event,
    rule,
    metricValue,
    avgHistoric,
    dropPercent,
    alertHour
  );

  const [channels] = await pool.query(
    "SELECT * FROM alert_channels WHERE alert_id = ?",
    [rule.id]
  );

  for (const ch of channels) {
    if (ch.channel_type !== "email") continue;

    const cfg = parseChannelConfig(ch.channel_config);
    if (!cfg) continue;

    const metricDisplayName = rule.metric_name.replace(/_/g, " ");
    const subjectMetricName =
      metricDisplayName.charAt(0).toUpperCase() + metricDisplayName.slice(1);

    const dropVal =
      dropPercent && !Number.isNaN(dropPercent)
        ? Math.abs(dropPercent).toFixed(2)
        : "0.00";

    const endHour = alertHour || 0;

    const subject = `${subjectMetricName} Alert | ${dropVal}% Drop | ${event.brand.toUpperCase()} | 0 - ${endHour} Hours`;

    await sendEmail(cfg, subject, emailHTML);
  }

  await pool.query(
    "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
    [rule.id, rule.brand_id, JSON.stringify(event)]
  );
}

async function evaluateThreshold(rule, metricValue, avgHistoric, dropPercent) {
  const threshold = Number(rule.threshold_value);
  if (rule.threshold_type === "percentage_drop") {
    if (
      avgHistoric == null ||
      dropPercent == null ||
      Number.isNaN(dropPercent)
    ) {
      return false;
    }
    return dropPercent >= threshold;
  }
  if (rule.threshold_type === "percentage_rise") {
    if (
      avgHistoric == null ||
      dropPercent == null ||
      Number.isNaN(dropPercent)
    ) {
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

  // ⭐ GET CURRENT IST HOUR — REQUIRED FOR QUIET HOURS CHECK
  const currentISTHour = Number(
    new Intl.DateTimeFormat("en-US", {
      timeZone: "Asia/Kolkata",
      hour: "2-digit",
      hour12: false,
    }).format(new Date())
  );

  console.log("Current IST Hour:", currentISTHour);

  // Determine hourCutoff (existing logic, untouched)
  const istHour = currentISTHour;
  const hourCutoff =
    typeof event.hour === "number" && event.hour >= 0 && event.hour <= 23
      ? event.hour
      : istHour;

  const metricsNeedingAvg = [
    "total_orders",
    "total_atc_sessions",
    "total_sessions",
    "total_sales",
    "aov",
    "conversion_rate",
  ];

  for (const rule of rules) {
    /* ----------------------------
       QUIET HOURS CHECK (NEW)
    -----------------------------*/
    if (
      typeof rule.quiet_hours_start === "number" &&
      typeof rule.quiet_hours_end === "number"
    ) {
      const qs = rule.quiet_hours_start;
      const qe = rule.quiet_hours_end;

      const inQuiet =
        qs < qe
          ? currentISTHour >= qs && currentISTHour < qe
          : currentISTHour >= qs || currentISTHour < qe;

      if (inQuiet) {
        console.log(
          `⏳ Quiet hours active for rule ${rule.id}: Skipped. (${qs}:00 → ${qe}:00 IST)`
        );
        continue;
      }
    }
    /* ----------------------------*/

    const metricValue = await computeMetric(rule, event);
    if (metricValue == null) continue;

    let avgHistoric = null;
    let dropPercent = null;

    const needsHistorical =
      metricsNeedingAvg.includes(rule.metric_name) ||
      rule.threshold_type.includes("percentage");

    if (needsHistorical) {
      const lookbackDays = rule.lookback_days || 7;

      avgHistoric = await getHistoricalAvgForMetric(
        event.brand_id,
        rule.metric_name,
        hourCutoff,
        lookbackDays
      );

      if (avgHistoric > 0) {
        dropPercent = ((avgHistoric - metricValue) / avgHistoric) * 100;
      }
    }

    const shouldTrigger = await evaluateThreshold(
      rule,
      metricValue,
      avgHistoric,
      dropPercent
    );

    console.log("shouldTrigger:", shouldTrigger);
    if (!shouldTrigger) continue;

    const cooldown = await checkCooldown(rule.id, rule.cooldown_minutes);
    if (cooldown) {
      console.log(
        `Skipped trigger for rule ${rule.id} (${rule.name}) due to cooldown: ${rule.cooldown_minutes} minutes configured.`
      );
      continue;
    }

    await triggerAlert(
      rule,
      event,
      metricValue,
      avgHistoric,
      dropPercent,
      hourCutoff
    );
  }
}

module.exports = { processIncomingEvent };
