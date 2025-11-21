const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");

/* -------------------------------------------------------
   Load rules for a brand
--------------------------------------------------------*/
async function loadRulesForBrand(brandId) {
  const [rules] = await pool.query(
    "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
    [brandId]
  );
  return rules;
}

/* -------------------------------------------------------
   Safely parse JSON channel configs
--------------------------------------------------------*/
function parseChannelConfig(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;

  try {
    return JSON.parse(raw);
  } catch {
    console.warn("⚠ Invalid JSON in channel_config:", raw);
    return null;
  }
}

/* -------------------------------------------------------
   Compute metric value (base / derived)
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

/* -------------------------------------------------------
   Cooldown check
--------------------------------------------------------*/
async function checkCooldown(alertId, cooldownMinutes) {
  const [[row]] = await pool.query(
    `SELECT triggered_at 
     FROM alert_history 
     WHERE alert_id = ? 
     ORDER BY triggered_at DESC LIMIT 1`,
    [alertId]
  );

  if (!row) return false;

  const minutesSince = (Date.now() - new Date(row.triggered_at)) / 60000;
  return minutesSince < cooldownMinutes;
}

/* -------------------------------------------------------
   Build Human-friendly Email
--------------------------------------------------------*/
function generateEmailHTML(event, rule, metricValue) {
  return `
  <html>
  <body style="margin:0; padding:0; background:#f0f3f9; font-family:Arial, sans-serif;">

    <div style="max-width:620px; margin:35px auto; background:#ffffff; border-radius:14px; overflow:hidden; box-shadow:0 6px 25px rgba(0,0,0,0.09);">

      <!-- Header -->
      <div style="
        background: linear-gradient(135deg, #3b82f6, #6366f1);
        padding:28px 32px;
        color:#ffffff;
      ">
        <h2 style="margin:0; font-size:26px; font-weight:600;">
          ⚠️ A Quick Heads-Up About ${event.brand}
        </h2>
        <p style="margin:8px 0 0; opacity:0.9; font-size:15px;">
          Something important caught our attention today.
        </p>
      </div>

      <!-- Body -->
      <div style="padding:32px; line-height:1.65; color:#374151;">

        <p style="font-size:16px; margin:0 0 20px;">
          Hi there,
          <br><br>
          We noticed an unusual pattern today that might need your attention.
          Everything is working fine overall, but one of the activity indicators for your store
          showed a sudden shift that stands out from the usual flow.
        </p>

        <!-- Highlight Section -->
        <div style="
          background:#f9fafb;
          border-left:5px solid #3b82f6;
          padding:18px 22px;
          border-radius:10px;
          margin-bottom:25px;
        ">
          <p style="margin:0; font-size:15px;">
            The area we are keeping an eye on:  
            <strong style="color:#111827;">${rule.metric_name.toUpperCase()}</strong>
          </p>
          <p style="margin:8px 0 0; font-size:15px;">
            Current status looks a little different than usual:
            <strong style="color:#dc2626;">${metricValue}</strong>
          </p>
        </div>

        <!-- Explanation -->
        <p style="font-size:16px; margin:0 0 20px;">
          This does not necessarily mean something is wrong, customers' behavior can vary
          throughout the day. But we believe it’s worth giving it a quick glance so you stay ahead of things.
        </p>

        <!-- Quick Tips -->
        <div style="
          background:#eef2ff;
          padding:18px 22px;
          border-radius:10px;
          margin-bottom:20px;
        ">
          <h3 style="margin-top:0; font-size:17px; color:#4338ca;">What you can do</h3>
          <ul style="margin:0; padding-left:20px; font-size:15px; color:#4b5563;">
            <li>Take a quick look at your dashboard for today's performance trends.</li>
            <li>Notice if this pattern aligns with any ongoing campaigns or changes.</li>
            <li>If this continues, you might want to check product, traffic or offer performance.</li>
          </ul>
        </div>

        <!-- Brand Mention -->
        <p style="font-size:15px; color:#6b7280; margin-top:20px;">
          We’ll keep monitoring things for ${event.brand}.  
          If anything else stands out, we’ll notify you right away.
        </p>

      </div>

      <!-- Footer -->
      <div style="
        background:#f3f4f6;
        padding:15px 20px;
        text-align:center;
        font-size:13px;
        color:#6b7280;
      ">
        This message was sent to help you stay informed and in control.<br>
        © ${new Date().getFullYear()} Datum Inc. All rights reserved.
      </div>

    </div>

  </body>
  </html>`;
}

/* -------------------------------------------------------
   Send email
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
      from: `"Alerting System" <${cfg.smtp_user}>`,
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
   Trigger alert
--------------------------------------------------------*/
async function triggerAlert(rule, event, metricValue) {
  console.log(`🚨 Triggering alert: ${rule.name}`);

  const emailHTML = generateEmailHTML(event, rule, metricValue);

  const [channels] = await pool.query(
    "SELECT * FROM alert_channels WHERE alert_id = ?",
    [rule.id]
  );

  for (const ch of channels) {
    const cfg = parseChannelConfig(ch.channel_config);
    if (!cfg) continue;
    await sendEmail(cfg, rule.name, emailHTML);
  }

  await pool.query(
    "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
    [rule.id, rule.brand_id, JSON.stringify(event)]
  );
}

/* -------------------------------------------------------
   Evaluate threshold (now supports < and > rules)
--------------------------------------------------------*/
async function evaluateThreshold(rule, metricValue) {
  const threshold = Number(rule.threshold_value);

  switch (rule.threshold_type) {
    case "absolute":
      // Trigger if value is BELOW threshold (classic low alert)
      return metricValue < threshold;

    case "percentage_drop":
      // Example meaning: value is lower than X% of normal
      return metricValue <= threshold;

    case "percentage_rise":
      // Example meaning: value is higher than X% of normal
      return metricValue >= threshold;

    default:
      console.warn("⚠ Unknown threshold type:", rule.threshold_type);
      return false;
  }
}

/* -------------------------------------------------------
   Main Event Processing
--------------------------------------------------------*/
async function processIncomingEvent(event) {
  if (typeof event === "string") {
    event = JSON.parse(event);
  }

  console.log("📥 Incoming event:", event);

  const rules = await loadRulesForBrand(event.brand_id);

  for (const rule of rules) {
    const metricValue = await computeMetric(rule, event);
    if (metricValue == null) continue;

    const shouldTrigger = await evaluateThreshold(rule, metricValue);
    if (!shouldTrigger) continue;

    const cooldown = await checkCooldown(rule.id, rule.cooldown_minutes);
    if (cooldown) continue;

    await triggerAlert(rule, event, metricValue);
  }
}

module.exports = { processIncomingEvent };
