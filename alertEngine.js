const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");
const { MongoClient } = require("mongodb");
let mongoClient = null;

/* -------------------------------------------------------
   Historical Average Lookup (using lookback_days)
   - Sessions data: hourly_sessions_summary_shopify (Shopify)
   - Sales/Orders data: hour_wise_sales (legacy)
--------------------------------------------------------*/
async function getHistoricalAvgForMetric(
  brandId,
  metricName,
  hourCutoff,
  lookbackDays
) {
  try {
    console.log(`\n📊 ═══════════════════════════════════════════════════════`);
    console.log(`📊 HISTORICAL AVG LOOKUP: ${metricName}`);
    console.log(`📊 ═══════════════════════════════════════════════════════`);
    console.log(`📊 Brand ID: ${brandId}`);
    console.log(`📊 Hour Cutoff: ${hourCutoff} (data from hour 0 to ${hourCutoff - 1})`);
    console.log(`📊 Lookback Days: ${lookbackDays}`);

    if (hourCutoff <= 0) {
      console.log(`📊 ❌ Skipped: hourCutoff <= 0`);
      return null;
    }

    const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
      brandId,
    ]);
    if (!rows.length) {
      console.log(`📊 ❌ Skipped: Brand not found`);
      return null;
    }

    const dbName = rows[0].db_name;
    const days = Number(lookbackDays) > 0 ? Number(lookbackDays) : 7;

    console.log(`📊 Database: ${dbName}`);
    console.log(`📊 Date Range: Last ${days} days (excluding today)`);

    // AOV AVG (from hour_wise_sales - sales data)
    if (metricName === "aov") {
      console.log(`📊 Source Table: ${dbName}.hour_wise_sales`);
      console.log(`📊 Formula: SUM(total_sales) / SUM(number_of_orders)`);

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
      console.log(`📊 Days with data: ${dayCount}`);

      if (!raw || dayCount === 0) {
        console.log(`📊 ❌ No historical data found`);
        return null;
      }

      const rounded = Number(Number(raw).toFixed(2));
      console.log(`📊 ✅ Historical AVG: ${rounded}`);
      console.log(`📊 ═══════════════════════════════════════════════════════\n`);
      return rounded;
    }

    // CVR AVG (sessions from Shopify, orders from hour_wise_sales)
    if (metricName === "conversion_rate") {
      console.log(`📊 Source Tables: ${dbName}.hourly_sessions_summary_shopify + ${dbName}.hour_wise_sales`);
      console.log(`📊 Formula: (SUM(orders) / SUM(sessions)) × 100`);

      // First get daily breakdown for detailed logging
      const [dailyBreakdown] = await pool.query(
        `
        SELECT 
          s.date,
          SUM(s.number_of_sessions) AS total_sessions,
          SUM(h.number_of_orders) AS total_orders,
          (SUM(h.number_of_orders) / NULLIF(SUM(s.number_of_sessions), 0)) * 100 AS daily_cvr
        FROM ${dbName}.hourly_sessions_summary_shopify s
        LEFT JOIN ${dbName}.hour_wise_sales h 
          ON s.date = h.date AND s.hour = h.hour
        WHERE s.date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
          AND s.date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
          AND s.hour < ?
        GROUP BY s.date
        HAVING SUM(s.number_of_sessions) > 0
        ORDER BY s.date;
        `,
        [days, hourCutoff]
      );

      console.log(`📊 Daily Breakdown (hour 0 to ${hourCutoff - 1}):`);
      dailyBreakdown.forEach(row => {
        console.log(`📊   ${row.date}: Sessions=${row.total_sessions}, Orders=${row.total_orders}, CVR=${Number(row.daily_cvr).toFixed(4)}%`);
      });

      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(daily_cvr) AS avg_val,
          COUNT(*) AS day_count
        FROM (
          SELECT 
            s.date,
            (SUM(h.number_of_orders) / NULLIF(SUM(s.number_of_sessions), 0)) * 100 AS daily_cvr
          FROM ${dbName}.hourly_sessions_summary_shopify s
          LEFT JOIN ${dbName}.hour_wise_sales h 
            ON s.date = h.date AND s.hour = h.hour
          WHERE s.date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
            AND s.date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
            AND s.hour < ?
          GROUP BY s.date
          HAVING SUM(s.number_of_sessions) > 0
        ) AS t;
        `,
        [days, hourCutoff]
      );

      const raw = avgRows[0]?.avg_val;
      const dayCount = avgRows[0]?.day_count ?? 0;
      console.log(`📊 Days with data: ${dayCount}`);

      if (!raw || dayCount === 0) {
        console.log(`📊 ❌ No historical data found`);
        return null;
      }

      const rounded = Number(Number(raw).toFixed(4));
      console.log(`📊 ✅ Historical AVG: ${rounded}%`);
      console.log(`📊 ═══════════════════════════════════════════════════════\n`);
      return rounded;
    }

    // SESSION metrics (from Shopify table)
    const sessionMetrics = {
      total_sessions: "number_of_sessions",
      total_atc_sessions: "number_of_atc_sessions",
    };

    if (sessionMetrics[metricName]) {
      const col = sessionMetrics[metricName];
      console.log(`📊 Source Table: ${dbName}.hourly_sessions_summary_shopify`);
      console.log(`📊 Column: ${col}`);

      // Get daily breakdown for logging
      const [dailyBreakdown] = await pool.query(
        `
        SELECT 
          date,
          SUM(${col}) AS daily_val
        FROM ${dbName}.hourly_sessions_summary_shopify
        WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
          AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
          AND hour < ?
        GROUP BY date
        ORDER BY date;
        `,
        [days, hourCutoff]
      );

      console.log(`📊 Daily Breakdown (hour 0 to ${hourCutoff - 1}):`);
      dailyBreakdown.forEach(row => {
        console.log(`📊   ${row.date}: ${col}=${row.daily_val}`);
      });

      const [avgRows] = await pool.query(
        `
        SELECT 
          AVG(daily_val) AS avg_val,
          COUNT(*) AS day_count
        FROM (
          SELECT 
            date,
            SUM(${col}) AS daily_val
          FROM ${dbName}.hourly_sessions_summary_shopify
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
      console.log(`📊 Days with data: ${dayCount}`);

      if (!raw || dayCount === 0) {
        console.log(`📊 ❌ No historical data found`);
        return null;
      }

      const rounded = Number(Number(raw).toFixed(2));
      console.log(`📊 ✅ Historical AVG: ${rounded}`);
      console.log(`📊 ═══════════════════════════════════════════════════════\n`);
      return rounded;
    }

    // SALES/ORDER metrics (from legacy hour_wise_sales table)
    const salesMetrics = {
      total_orders: "number_of_orders",
      total_sales: "total_sales",
    };

    const col = salesMetrics[metricName];
    if (!col) {
      console.log(`📊 ❌ Unknown metric: ${metricName}`);
      return null;
    }

    console.log(`📊 Source Table: ${dbName}.hour_wise_sales`);
    console.log(`📊 Column: ${col}`);

    // Get daily breakdown for logging
    const [dailyBreakdown] = await pool.query(
      `
      SELECT 
        date,
        SUM(${col}) AS daily_val
      FROM ${dbName}.hour_wise_sales
      WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
        AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
        AND hour < ?
      GROUP BY date
      ORDER BY date;
      `,
      [days, hourCutoff]
    );

    console.log(`📊 Daily Breakdown (hour 0 to ${hourCutoff - 1}):`);
    dailyBreakdown.forEach(row => {
      console.log(`📊   ${row.date}: ${col}=${row.daily_val}`);
    });

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
    console.log(`📊 Days with data: ${dayCount}`);
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
  try {
    if (!mongoClient) {
      if (!process.env.MONGO_URI) {
        throw new Error("MONGO_URI not set");
      }
      mongoClient = new MongoClient(process.env.MONGO_URI);
      await mongoClient.connect();
      console.log("✅ Connected to MongoDB");
    }

    // Assuming the URI points to the correct DB, or using default
    const db = mongoClient.db();

    // Ensure brand_id is Number to match the document structure (e.g. brand_id: 4)
    const rules = await db.collection("alerts").find({
      brand_id: Number(brandId),
      is_active: 1
    }).toArray();

    return rules;
  } catch (err) {
    console.error("🔥 Error loading rules from MongoDB:", err.message);
    return [];
  }
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

async function getAllRules() {
  try {
    if (!mongoClient) {
      if (!process.env.MONGO_URI) {
        throw new Error("MONGO_URI not set");
      }
      mongoClient = new MongoClient(process.env.MONGO_URI);
      await mongoClient.connect();
    }
    const db = mongoClient.db();
    const rules = await db.collection("alerts").find({}).toArray();
    return rules;
  } catch (err) {
    console.error("🔥 Error fetching all rules:", err.message);
    return [];
  }
}

/* -------------------------------------------------------
   Compute Metric
--------------------------------------------------------*/
async function computeMetric(rule, event) {
  try {
    if (rule.metric_type === "base") {
      const val = event[rule.metric_name];
      if (val === undefined) {
        // Not a failure, just not in this specific event (e.g., perf event vs sales event)
        return null;
      }
      return val;
    }
    if (rule.metric_type === "derived") {
      const val = evaluate(rule.formula, event);
      if (typeof val === "number") {
        if (Number.isNaN(val) || !Number.isFinite(val)) return 0;
      }
      return val;
    }
  } catch (err) {
    if (err.message.includes("Undefined symbol")) {
      const missing = err.message.split("symbol ")[1];
      console.log(`ℹ Skipping rule ${rule.id}: metric '${missing}' not in event data`);
    } else {
      console.error("❌ Metric computation error:", err.message);
    }
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
async function checkCooldown(alertId, cooldownMinutes) {
  try {
    const db = mongoClient.db();
    const rows = await db.collection("alert_history")
      .find({ alert_id: alertId })
      .sort({ triggered_at: -1 })
      .limit(1)
      .toArray();


    if (!rows.length) return false;

    // --- Convert triggered_at (UTC from DB) → IST ---
    const triggeredUTC = new Date(rows[0].triggered_at);
    const triggeredIST = new Date(
      triggeredUTC.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    );

    // --- Convert NOW → IST ---
    const nowIST = new Date(
      new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    );

    const minutes = (nowIST - triggeredIST) / 60000;

    return minutes < cooldownMinutes;
  } catch (err) {
    console.error("🔥 Error checking cooldown:", err.message);
    return false;
  }
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
  } else if (rule.threshold_type === "less_than") {
    thresholdDisplay = `less than ${formatValue(rule.threshold_value)}`;
  } else if (rule.threshold_type === "greater_than") {
    thresholdDisplay = `greater than ${formatValue(rule.threshold_value)}`;
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
    const historicalLabel =
      rule.metric_name === "performance"
        ? "Prior Value"
        : `Historical Avg (${rule.lookback_days} days)`;
    metricRows += `
      <tr>
        <td style="padding:10px 0; color:#6b7280; font-size:15px;">${historicalLabel}</td>
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
            ${typeof alertHour === "number"
      ? `<br><strong>Hour:</strong> ${alertHour} (data up to hour ${Math.max(
        0,
        alertHour
      )}h)`
      : ""
    }
          </p>
        </div>

        <!-- ⭐ Dashboard link -->
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
    <div style="background:#f3f4f6; padding:14px; text-align:center;">
        <span style="font-size:12px; color:#6b7280;">
          © ${new Date().getFullYear()} Datum Inc.
        </span>
      </div>
    </div>
  </body>
  </html>
  `;

  if (require('./alertEngine').TEST_MODE_FLAG) {
    return `
      <div style="background-color: #fff3cd; color: #856404; padding: 10px; text-align: center; font-weight: bold; border: 1px solid #ffeeba;">
        🚧 THIS IS A TEST ALERT SENT FOR TESTING THE ALERT SYSTEM 🚧
      </div>
     ` + html;
  }
  return html;
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
      from: `"Alerting System" <${cfg.smtp_user || process.env.ALERT_EMAIL_USER
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
// 🧪 TEST MODE: Set to true to send all alerts to single test email
const TEST_MODE = process.env.TEST_MODE;
const TEST_EMAIL = process.env.TEST_EMAIL;

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

  // 🧪 TEST MODE: Override all channels to single test email
  if (TEST_MODE) {
    const metricDisplayName = rule.metric_name.replace(/_/g, " ");
    const subjectMetricName =
      metricDisplayName.charAt(0).toUpperCase() + metricDisplayName.slice(1);
    const dropVal =
      dropPercent && !Number.isNaN(dropPercent)
        ? Math.abs(dropPercent).toFixed(2)
        : "0.00";
    const endHour = alertHour || 0;
    const subject = `[TEST ALERT] ${subjectMetricName} Alert | ${dropVal}% Drop | ${event.brand.toUpperCase()} | 0 - ${endHour} Hours`;

    console.log(`🧪 TEST MODE: Sending to ${TEST_EMAIL} only`);

    // Inject test banner into HTML
    const testHtml = `
      <div style="background-color: #fff3cd; color: #856404; padding: 10px; text-align: center; font-weight: bold; border: 1px solid #ffeeba; font-family: Arial, sans-serif; margin-bottom: 20px;">
        🚧 THIS IS A TEST ALERT SENT FOR TESTING THE ALERT SYSTEM 🚧
      </div>
      ${emailHTML}
    `;

    await sendEmail({ to: [TEST_EMAIL] }, subject, testHtml);

    try {
      const db = mongoClient.db();
      await db.collection("alert_history").insertOne({
        alert_id: rule.id,
        brand_id: rule.brand_id,
        payload: event,
        triggered_at: new Date()
      });
    } catch (err) {
      console.error("🔥 Error saving test alert history to MongoDB:", err.message);
    }
    return;
  }

  let channels = [];
  if (rule.have_recipients === 1) {
    [channels] = await pool.query(
      "SELECT * FROM alert_channels WHERE alert_id = ?",
      [rule.id]
    );
  } else {
    [channels] = await pool.query(
      "SELECT * FROM brands_alert_channel WHERE brand_id = ? AND is_active = 1",
      [rule.brand_id]
    );
  }

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

  }


  try {
    const db = mongoClient.db();
    await db.collection("alert_history").insertOne({
      alert_id: rule.id,
      brand_id: rule.brand_id,
      payload: event,
      triggered_at: new Date()
    });
  } catch (err) {
    console.error("🔥 Error saving alert history to MongoDB:", err.message);
  }
}

/* -------------------------------------------------------
   Normal Threshold Evaluation
--------------------------------------------------------*/
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
  if (rule.threshold_type === "less_than") {
    const isBelow = metricValue < threshold;
    const isWorsening = avgHistoric == null || metricValue < avgHistoric;
    return isBelow && isWorsening;
  }
  if (rule.threshold_type === "greater_than") {
    const isAbove = metricValue > threshold;
    const isWorsening = avgHistoric == null || metricValue > avgHistoric;
    return isAbove && isWorsening;
  }
  // Fallback for legacy 'absolute' type
  return metricValue < threshold;
}

/* -------------------------------------------------------
   Main Controller
--------------------------------------------------------*/
async function processIncomingEvent(event) {
  if (typeof event === "string") event = JSON.parse(event);
  event = normalizeEventKeys(event);

  console.log("📥 Event Received:", event);

  let brandId = event.brand_id;

  // Resolve brand_id from brand_key if missing
  if (!brandId && event.brand_key) {
    const [brands] = await pool.query(
      "SELECT id FROM brands WHERE name = ?",
      [event.brand_key]
    );
    if (brands.length > 0) {
      brandId = brands[0].id;
      event.brand_id = brandId;
      if (!event.brand) event.brand = event.brand_key;
    } else {
      console.warn("⚠ brand_key not found:", event.brand_key);
      return;
    }
  }

  if (!brandId) {
    console.error("❌ missing brand_id and brand_key");
    return;
  }

  const rules = await loadRulesForBrand(brandId);

  // current IST hour
  const currentISTHour = Number(
    new Intl.DateTimeFormat("en-US", {
      timeZone: "Asia/Kolkata",
      hour: "2-digit",
      hour12: false,
    }).format(new Date())
  );

  console.log("Current IST Hour:", currentISTHour);

  // Determine hourCutoff (existing logic)
  const istHour = currentISTHour;
  const hourCutoff =
    typeof event.hour === "number" && event.hour >= 0 && event.hour <= 23
      ? event.hour
      : istHour;

  console.log(`\n🔧 ═══════════════════════════════════════════════════════`);
  console.log(`🔧 PROCESSING CONFIG`);
  console.log(`🔧 ═══════════════════════════════════════════════════════`);
  console.log(`🔧 Brand ID: ${brandId}`);
  console.log(`🔧 Brand Name: ${event.brand || event.brand_key}`);
  console.log(`🔧 Hour from event: ${event.hour ?? 'NOT PROVIDED'}`);
  console.log(`🔧 Hour Cutoff (used): ${hourCutoff}`);
  console.log(`🔧 Active Rules Found: ${rules.length}`);
  console.log(`🔧 ═══════════════════════════════════════════════════════\n`);

  const metricsNeedingAvg = [
    "total_orders",
    "total_atc_sessions",
    "total_sessions",
    "total_sales",
    "aov",
    "conversion_rate",
  ];

  for (const rule of rules) {
    console.log(`\n🎯 ═══════════════════════════════════════════════════════`);
    console.log(`🎯 EVALUATING RULE: ${rule.id} - ${rule.name}`);
    console.log(`🎯 ═══════════════════════════════════════════════════════`);
    console.log(`🎯 Metric: ${rule.metric_name}`);
    console.log(`🎯 Type: ${rule.metric_type}`);
    console.log(`🎯 Threshold Type: ${rule.threshold_type}`);
    console.log(`🎯 Threshold Value: ${rule.threshold_value}`);
    console.log(`🎯 Lookback Days: ${rule.lookback_days || 7}`);
    console.log(`🎯 Cooldown: ${rule.cooldown_minutes} minutes`);

    const metricValue = await computeMetric(rule, event);

    if (metricValue == null) {
      console.log(`🎯 ❌ Metric value is NULL - skipping rule`);
      continue;
    }

    console.log(`🎯 Current Metric Value: ${metricValue}`);

    let avgHistoric = null;
    let dropPercent = null;

    const isAbsoluteCondition = ["less_than", "greater_than", "absolute"].includes(rule.threshold_type);

    if (rule.threshold_type.includes("percentage") || (rule.metric_name === "performance" && isAbsoluteCondition)) {
      if (rule.metric_name === "performance") {
        // --- Daily Baseline Reset Logic ---
        let history = [];
        try {
          const db = mongoClient.db();
          history = await db.collection("alert_history")
            .find({ alert_id: rule.id })
            .sort({ triggered_at: -1 })
            .limit(1)
            .toArray();
        } catch (err) {
          console.error("🔥 Error fetching performance history:", err.message);
        }

        // Get Today in IST
        const nowIST = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
        const todayStr = nowIST.toISOString().split("T")[0];

        // Default to threshold value as the prior baseline
        avgHistoric = Number(rule.threshold_value);

        if (history.length > 0) {
          const lastIST = new Date(
            new Date(history[0].triggered_at).toLocaleString("en-US", {
              timeZone: "Asia/Kolkata",
            })
          );
          const lastDateStr = lastIST.toISOString().split("T")[0];

          // Only use history if it happened TODAY in IST
          if (lastDateStr === todayStr) {
            try {
              const raw = history[0].payload;
              const prevPayload = typeof raw === "string" ? JSON.parse(raw) : raw;
              const prevValue = prevPayload.performance;
              if (typeof prevValue === "number" && prevValue > 0) {
                avgHistoric = prevValue;
              }
            } catch (e) {
              console.error("🔥 Performance history error:", e.message);
            }
          }
        }

        if (avgHistoric != null) {
          dropPercent = ((avgHistoric - metricValue) / avgHistoric) * 100;
          console.log(`✓ performance = ${metricValue}`);
          console.log(`✓ performance prior value for rule ${rule.id} = ${avgHistoric}`);
          if (dropPercent != null) {
            console.log(
              `✓ performance drop percent for rule ${rule.id} = ${dropPercent.toFixed(2)}%`
            );
          }
        }
      } else {
        const lookbackDays = rule.lookback_days || 7;

        avgHistoric = await getHistoricalAvgForMetric(
          brandId,
          rule.metric_name,
          hourCutoff,
          lookbackDays
        );

        if (avgHistoric > 0) {
          dropPercent = ((avgHistoric - metricValue) / avgHistoric) * 100;

          console.log(`\n📉 ═══════════════════════════════════════════════════════`);
          console.log(`📉 DROP/RISE CALCULATION`);
          console.log(`📉 ═══════════════════════════════════════════════════════`);
          console.log(`📉 Historical AVG: ${avgHistoric}`);
          console.log(`📉 Current Value: ${metricValue}`);
          console.log(`📉 Formula: ((${avgHistoric} - ${metricValue}) / ${avgHistoric}) × 100`);
          console.log(`📉 Result: ${dropPercent.toFixed(4)}%`);
          console.log(`📉 ${dropPercent > 0 ? '⬇️ DROP' : '⬆️ RISE'}: ${Math.abs(dropPercent).toFixed(2)}%`);
          console.log(`📉 ═══════════════════════════════════════════════════════\n`);
        } else {
          console.log(`📉 ⚠️ No historical avg - cannot calculate drop percentage`);
        }
      }
    }

    // 1️⃣ Normal threshold check (must pass first)
    console.log(`\n⚖️ ═══════════════════════════════════════════════════════`);
    console.log(`⚖️ THRESHOLD EVALUATION`);
    console.log(`⚖️ ═══════════════════════════════════════════════════════`);
    console.log(`⚖️ Threshold Type: ${rule.threshold_type}`);
    console.log(`⚖️ Threshold Value: ${rule.threshold_value}`);
    console.log(`⚖️ Current Metric: ${metricValue}`);
    console.log(`⚖️ Historical AVG: ${avgHistoric ?? 'N/A'}`);
    console.log(`⚖️ Drop/Rise %: ${dropPercent?.toFixed(2) ?? 'N/A'}%`);

    const shouldTriggerNormal = await evaluateThreshold(
      rule,
      metricValue,
      avgHistoric,
      dropPercent
    );

    console.log(`⚖️ Threshold Check: ${rule.threshold_type} >= ${rule.threshold_value}?`);
    console.log(`⚖️ Result: ${shouldTriggerNormal ? '✅ PASSED (alert should fire)' : '❌ FAILED (no alert)'}`);
    console.log(`⚖️ ═══════════════════════════════════════════════════════\n`);

    if (!shouldTriggerNormal) {
      // If it doesn't pass normal threshold, we don't even consider quiet/critical.
      continue;
    }

    // 2️⃣ Quiet hours + critical override
    let inQuiet = false;

    if (
      typeof rule.quiet_hours_start === "number" &&
      typeof rule.quiet_hours_end === "number"
    ) {
      const qs = rule.quiet_hours_start;
      const qe = rule.quiet_hours_end;

      inQuiet =
        qs < qe
          ? currentISTHour >= qs && currentISTHour < qe
          : currentISTHour >= qs || currentISTHour < qe;

      if (inQuiet) {
        const crit = Number(rule.critical_threshold);
        const hasCrit =
          !Number.isNaN(crit) &&
          crit > 0 &&
          rule.threshold_type === "percentage_drop";
        const hasDrop =
          typeof dropPercent === "number" && !Number.isNaN(dropPercent);

        if (hasCrit && hasDrop && dropPercent >= crit) {
          console.log(
            `⚠️ Critical override for rule ${rule.id
            }: drop=${dropPercent.toFixed(
              2
            )}% >= critical=${crit}% — alert will fire even in quiet hours.`
          );
          // allow to proceed
        } else {
          console.log(
            `⏳ Quiet hours active for rule ${rule.id}: Skipped. (${qs}:00 → ${qe}:00 IST) ` +
            `drop=${hasDrop ? dropPercent.toFixed(2) : "N/A"}%, critical=${hasCrit ? crit : "N/A"
            }`
          );
          continue;
        }
      }
    }

    // 3️⃣ Cooldown check (unchanged)
    console.log(`\n⏰ Checking cooldown for rule ${rule.id}...`);
    const cooldown = await checkCooldown(rule.id, rule.cooldown_minutes);
    if (cooldown) {
      console.log(
        `⏰ ❌ Skipped: Rule ${rule.id} (${rule.name}) is in cooldown (${rule.cooldown_minutes} minutes)`
      );
      continue;
    }
    console.log(`⏰ ✅ Cooldown cleared - proceeding to fire alert`);

    // 4️⃣ Finally fire the alert
    console.log(`\n🚨 ═══════════════════════════════════════════════════════`);
    console.log(`🚨 FIRING ALERT!`);
    console.log(`🚨 ═══════════════════════════════════════════════════════`);
    console.log(`🚨 Rule: ${rule.id} - ${rule.name}`);
    console.log(`🚨 Brand: ${event.brand}`);
    console.log(`🚨 Metric: ${rule.metric_name}`);
    console.log(`🚨 Current Value: ${metricValue}`);
    console.log(`🚨 Historical AVG: ${avgHistoric}`);
    console.log(`🚨 Drop/Rise: ${dropPercent?.toFixed(2)}%`);
    console.log(`🚨 ═══════════════════════════════════════════════════════\n`);

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

module.exports = { processIncomingEvent, getAllRules, TEST_MODE, TEST_EMAIL };
