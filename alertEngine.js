// OLD LOGIC

// const pool = require("./db");
// const { evaluate } = require("mathjs");
// const nodemailer = require("nodemailer");

// /* -------------------------------------------------------
//    Historical Average Lookup (using lookback_days)
// --------------------------------------------------------*/
// async function getHistoricalAvgForMetric(
//   brandId,
//   metricName,
//   hourCutoff,
//   lookbackDays
// ) {
//   try {
//     if (hourCutoff <= 0) return null;

//     const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
//       brandId,
//     ]);
//     if (!rows.length) return null;

//     const dbName = rows[0].db_name;
//     const days = Number(lookbackDays) > 0 ? Number(lookbackDays) : 7;

//     // AOV AVG
//     if (metricName === "aov") {
//       const [avgRows] = await pool.query(
//         `
//         SELECT 
//           AVG(daily_aov) AS avg_val,
//           COUNT(*) AS day_count
//         FROM (
//           SELECT 
//             date,
//             SUM(total_sales) / NULLIF(SUM(number_of_orders), 0) AS daily_aov
//           FROM ${dbName}.hour_wise_sales
//           WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
//             AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
//             AND hour < ?
//           GROUP BY date
//           HAVING SUM(number_of_orders) > 0
//         ) AS t;
//         `,
//         [days, hourCutoff]
//       );

//       const raw = avgRows[0]?.avg_val;
//       const dayCount = avgRows[0]?.day_count ?? 0;
//       if (!raw || dayCount === 0) return null;

//       const rounded = Number(Number(raw).toFixed(2));
//       console.log(
//         `✓ AOV historical avg for brand ${brandId} (last ${days} days): ${rounded}`
//       );
//       return rounded;
//     }

//     // CVR AVG
//     if (metricName === "conversion_rate") {
//       const [avgRows] = await pool.query(
//         `
//         SELECT 
//           AVG(daily_cvr) AS avg_val,
//           COUNT(*) AS day_count
//         FROM (
//           SELECT 
//             date,
//             (SUM(number_of_orders) / NULLIF(SUM(number_of_sessions), 0)) * 100 AS daily_cvr
//           FROM ${dbName}.hour_wise_sales
//           WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
//             AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
//             AND hour < ?
//           GROUP BY date
//           HAVING SUM(number_of_sessions) > 0
//         ) AS t;
//         `,
//         [days, hourCutoff]
//       );

//       const raw = avgRows[0]?.avg_val;
//       const dayCount = avgRows[0]?.day_count ?? 0;
//       if (!raw || dayCount === 0) return null;

//       const rounded = Number(Number(raw).toFixed(4));
//       console.log(
//         `✓ conversion_rate historical avg for brand ${brandId} = ${rounded}`
//       );
//       return rounded;
//     }

//     // BASE metrics
//     const map = {
//       total_orders: "number_of_orders",
//       total_atc_sessions: "number_of_atc_sessions",
//       total_sales: "total_sales",
//       total_sessions: "number_of_sessions",
//     };

//     const col = map[metricName];
//     if (!col) return null;

//     const [avgRows] = await pool.query(
//       `
//       SELECT 
//         AVG(daily_val) AS avg_val,
//         COUNT(*) AS day_count
//       FROM (
//         SELECT 
//           date,
//           SUM(${col}) AS daily_val
//         FROM ${dbName}.hour_wise_sales
//         WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
//           AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
//           AND hour < ?
//         GROUP BY date
//       ) AS t;
//       `,
//       [days, hourCutoff]
//     );

//     const raw = avgRows[0]?.avg_val;
//     const dayCount = avgRows[0]?.day_count ?? 0;
//     if (!raw || dayCount === 0) return null;

//     const rounded = Number(Number(raw).toFixed(2));
//     console.log(`✓ ${metricName} historical avg = ${rounded}`);
//     return rounded;
//   } catch (err) {
//     console.error(`🔥 historical avg error for ${metricName}:`, err.message);
//     return null;
//   }
// }

// /* -------------------------------------------------------
//    Load Active Alerts
// --------------------------------------------------------*/
// async function loadRulesForBrand(brandId) {
//   const [rules] = await pool.query(
//     "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
//     [brandId]
//   );
//   return rules;
// }

// /* -------------------------------------------------------
//    Parse channel_config
// --------------------------------------------------------*/
// function parseChannelConfig(raw) {
//   if (!raw) return null;
//   if (typeof raw === "object") return raw;

//   try {
//     return JSON.parse(raw);
//   } catch {
//     try {
//       const fixed = raw
//         .trim()
//         .replace(/'/g, '"')
//         .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":');
//       return JSON.parse(fixed);
//     } catch {
//       console.warn("⚠ Invalid JSON in channel_config:", raw);
//       return null;
//     }
//   }
// }

// /* -------------------------------------------------------
//    Compute Metric
// --------------------------------------------------------*/
// async function computeMetric(rule, event) {
//   try {
//     if (rule.metric_type === "base") return event[rule.metric_name];
//     if (rule.metric_type === "derived") return evaluate(rule.formula, event);
//   } catch (err) {
//     console.error("❌ Metric computation error:", err.message);
//   }
//   return null;
// }

// /* -------------------------------------------------------
//    Normalize keys
// --------------------------------------------------------*/
// function normalizeEventKeys(event) {
//   if (!event) return event;
//   const normalized = {};

//   for (const [k, v] of Object.entries(event)) {
//     normalized[k] = v;
//     const snake = k.replace(/[A-Z]/g, (ch) => `_${ch.toLowerCase()}`);
//     if (snake !== k) normalized[snake] = v;
//   }
//   return normalized;
// }

// /* -------------------------------------------------------
//    Cooldown
// --------------------------------------------------------*/
// async function checkCooldown(alertId, cooldownMinutes) {
//   const [rows] = await pool.query(
//     `
//       SELECT triggered_at 
//       FROM alert_history 
//       WHERE alert_id = ?
//       ORDER BY triggered_at DESC 
//       LIMIT 1
//     `,
//     [alertId]
//   );

//   if (!rows.length) return false;

//   // --- Convert triggered_at (UTC from DB) → IST ---
//   const triggeredUTC = new Date(rows[0].triggered_at);
//   const triggeredIST = new Date(
//     triggeredUTC.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
//   );

//   // --- Convert NOW → IST ---
//   const nowIST = new Date(
//     new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
//   );

//   const minutes = (nowIST - triggeredIST) / 60000;

//   return minutes < cooldownMinutes;
// }

// /* -------------------------------------------------------
//    Email HTML
// --------------------------------------------------------*/
// function generateEmailHTML(
//   event,
//   rule,
//   metricValue,
//   avgHistoric,
//   dropPercent,
//   alertHour
// ) {
//   const brandName = String(event.brand || "").toUpperCase();
//   const metricLabel = rule.metric_name.replace(/_/g, " ").toUpperCase();

//   const hasAvg = typeof avgHistoric === "number" && !Number.isNaN(avgHistoric);
//   const hasDrop = typeof dropPercent === "number" && !Number.isNaN(dropPercent);

//   const formatValue = (val) => {
//     if (typeof val === "number") {
//       if (val % 1 === 0) return val.toString();
//       return val.toFixed(2);
//     }
//     return val;
//   };

//   let metricRows = `
//     <tr>
//       <td style="padding:10px 0; color:#6b7280; font-size:15px;">Current Value</td>
//       <td style="padding:10px 0; text-align:right; font-weight:bold; color:#dc2626; font-size:15px;">
//         ${formatValue(metricValue)}
//       </td>
//     </tr>
//   `;

//   let thresholdDisplay = "";
//   if (rule.threshold_type === "percentage_drop") {
//     thresholdDisplay = `${rule.threshold_value}% drop`;
//   } else if (rule.threshold_type === "percentage_rise") {
//     thresholdDisplay = `${rule.threshold_value}% rise`;
//   } else {
//     thresholdDisplay = formatValue(rule.threshold_value);
//   }

//   metricRows += `
//     <tr>
//       <td style="padding:10px 0; color:#6b7280; font-size:15px;">Alert Threshold</td>
//       <td style="padding:10px 0; text-align:right; font-weight:bold; font-size:15px;">
//         ${thresholdDisplay}
//       </td>
//     </tr>
//   `;

//   if (hasAvg) {
//     metricRows += `
//       <tr>
//         <td style="padding:10px 0; color:#6b7280; font-size:15px;">Historical Avg (${
//           rule.lookback_days
//         } days)</td>
//         <td style="padding:10px 0; text-align:right; font-weight:bold; font-size:15px;">
//           ${formatValue(avgHistoric)}
//         </td>
//       </tr>
//     `;
//   }

//   if (hasDrop) {
//     const dropColor = dropPercent > 0 ? "#e11d48" : "#10b981";
//     const dropLabel = dropPercent > 0 ? "Drop" : "Increase";
//     metricRows += `
//       <tr>
//         <td style="padding:10px 0; color:#6b7280; font-size:15px;">Percentage ${dropLabel}</td>
//         <td style="padding:10px 0; text-align:right; font-weight:bold; color:${dropColor}; font-size:15px;">
//           ${Math.abs(dropPercent).toFixed(2)}%
//         </td>
//       </tr>
//     `;
//   }

//   return `
//   <html>
//   <body style="margin:0; padding:0; background:#f4f6fb; font-family:Arial, sans-serif;">
//     <div style="max-width:620px; margin:30px auto; background:#ffffff;
//       border-radius:12px; overflow:hidden; box-shadow:0 6px 25px rgba(0,0,0,0.08);">

//       <div style="background:#4f46e5; padding:26px 32px; color:#ffffff;">
//         <h2 style="margin:0; font-size:24px; font-weight:600;">
//           ⚠️ Insight alert for ${brandName}
//         </h2>
//         <p style="margin:6px 0 0; font-size:14px; opacity:0.9;">
//           One of your key activity signals moved more than usual.
//         </p>
//       </div>

//       <div style="padding:30px; line-height:1.6; color:#374151;">
//         <p style="font-size:16px;">
//           We noticed a change in <strong>${metricLabel}</strong> that may need attention.
//         </p>

//         <div style="background:#f9fafb; border-radius:10px; padding:20px;
//           border:1px solid #e5e7eb; margin-bottom:22px;">
          
//           <h3 style="margin:0 0 16px; font-size:18px; font-weight:600; color:#111827;">Alert Details</h3>

//           <table style="width:100%; border-collapse:collapse;">
//             ${metricRows}
//           </table>
//         </div>
        
//         <div style="background:#fef3c7; border-left:4px solid #f59e0b; padding:12px 16px; margin-bottom:20px; border-radius:6px;">
//           <p style="margin:0; font-size:14px; color:#92400e;">
//             <strong>Metric:</strong> ${metricLabel}<br>
//             <strong>Threshold Type:</strong> ${rule.threshold_type.replace(
//               /_/g,
//               " "
//             )}<br>
//             <strong>Severity:</strong> ${rule.severity.toUpperCase()}
//             ${
//               typeof alertHour === "number"
//                 ? `<br><strong>Hour:</strong> ${alertHour} (data up to hour ${Math.max(
//                     0,
//                     alertHour
//                   )}h)`
//                 : ""
//             }
//           </p>
//         </div>

//         <!-- ⭐ Dashboard link -->
//         <p style="font-size:15px; color:#4b5563; margin-top:20px;">
//           Take a look at the latest activity on your dashboard for possible causes: 
//           <a href="https://datum.trytechit.co/" style="color:#4f46e5; text-decoration:underline;">
//             https://datum.trytechit.co/
//           </a>
//         </p>
//       </div>

//       <div style="background:#f3f4f6; padding:14px; text-align:center;">
//         <span style="font-size:12px; color:#6b7280;">
//           © ${new Date().getFullYear()} Datum Inc.
//         </span>
//       </div>
//     </div>
//   </body>
//   </html>
//   `;
// }

// /* -------------------------------------------------------
//    Send Email
// --------------------------------------------------------*/
// async function sendEmail(cfg, subject, html) {
//   try {
//     if (!cfg || !cfg.to || !Array.isArray(cfg.to) || cfg.to.length === 0) {
//       console.error("❌ Invalid email configuration: missing 'to' array", cfg);
//       return;
//     }

//     const transporter = nodemailer.createTransport({
//       service: "gmail",
//       auth: {
//         user: process.env.ALERT_EMAIL_USER,
//         pass: process.env.ALERT_EMAIL_PASS,
//       },
//     });

//     await transporter.sendMail({
//       from: `"Alerting System" <${
//         cfg.smtp_user || process.env.ALERT_EMAIL_USER
//       }>`,
//       to: cfg.to.join(","),
//       subject,
//       html,
//     });

//     console.log("📧 Email sent!");
//   } catch (err) {
//     console.error("❌ Email send failed:", err.message);
//   }
// }

// /* -------------------------------------------------------
//    Trigger Alert
// --------------------------------------------------------*/
// async function triggerAlert(
//   rule,
//   event,
//   metricValue,
//   avgHistoric,
//   dropPercent,
//   alertHour
// ) {
//   const emailHTML = generateEmailHTML(
//     event,
//     rule,
//     metricValue,
//     avgHistoric,
//     dropPercent,
//     alertHour
//   );

//   const [channels] = await pool.query(
//     "SELECT * FROM alert_channels WHERE alert_id = ?",
//     [rule.id]
//   );

//   for (const ch of channels) {
//     if (ch.channel_type !== "email") continue;

//     const cfg = parseChannelConfig(ch.channel_config);
//     if (!cfg) continue;

//     const metricDisplayName = rule.metric_name.replace(/_/g, " ");
//     const subjectMetricName =
//       metricDisplayName.charAt(0).toUpperCase() + metricDisplayName.slice(1);

//     const dropVal =
//       dropPercent && !Number.isNaN(dropPercent)
//         ? Math.abs(dropPercent).toFixed(2)
//         : "0.00";

//     const endHour = alertHour || 0;

//     const subject = `${subjectMetricName} Alert | ${dropVal}% Drop | ${event.brand.toUpperCase()} | 0 - ${endHour} Hours`;

//     await sendEmail(cfg, subject, emailHTML);
//   }

//   await pool.query(
//     "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
//     [rule.id, rule.brand_id, JSON.stringify(event)]
//   );
// }

// /* -------------------------------------------------------
//    Normal Threshold Evaluation
// --------------------------------------------------------*/
// async function evaluateThreshold(rule, metricValue, avgHistoric, dropPercent) {
//   const threshold = Number(rule.threshold_value);
//   if (rule.threshold_type === "percentage_drop") {
//     if (
//       avgHistoric == null ||
//       dropPercent == null ||
//       Number.isNaN(dropPercent)
//     ) {
//       return false;
//     }
//     return dropPercent >= threshold;
//   }
//   if (rule.threshold_type === "percentage_rise") {
//     if (
//       avgHistoric == null ||
//       dropPercent == null ||
//       Number.isNaN(dropPercent)
//     ) {
//       return false;
//     }
//     return -dropPercent >= threshold;
//   }
//   return metricValue < threshold;
// }

// /* -------------------------------------------------------
//    Main Controller
// --------------------------------------------------------*/
// async function processIncomingEvent(event) {
//   if (typeof event === "string") event = JSON.parse(event);
//   event = normalizeEventKeys(event);

//   console.log("📥 Event Received:", event);

//   const rules = await loadRulesForBrand(event.brand_id);

//   // current IST hour
//   const currentISTHour = Number(
//     new Intl.DateTimeFormat("en-US", {
//       timeZone: "Asia/Kolkata",
//       hour: "2-digit",
//       hour12: false,
//     }).format(new Date())
//   );

//   console.log("Current IST Hour:", currentISTHour);

//   // Determine hourCutoff (existing logic)
//   const istHour = currentISTHour;
//   const hourCutoff =
//     typeof event.hour === "number" && event.hour >= 0 && event.hour <= 23
//       ? event.hour
//       : istHour;

//   const metricsNeedingAvg = [
//     "total_orders",
//     "total_atc_sessions",
//     "total_sessions",
//     "total_sales",
//     "aov",
//     "conversion_rate",
//   ];

//   for (const rule of rules) {
//     rule.quiet_hours_start = Number(rule.quiet_hours_start);
//     rule.quiet_hours_end = Number(rule.quiet_hours_end);
//     rule.critical_threshold = Number(rule.critical_threshold);

//     const metricValue = await computeMetric(rule, event);
//     if (metricValue == null) continue;

//     let avgHistoric = null;
//     let dropPercent = null;

//     const needsHistorical =
//       metricsNeedingAvg.includes(rule.metric_name) ||
//       rule.threshold_type.includes("percentage");

//     if (needsHistorical) {
//       const lookbackDays = rule.lookback_days || 7;

//       avgHistoric = await getHistoricalAvgForMetric(
//         event.brand_id,
//         rule.metric_name,
//         hourCutoff,
//         lookbackDays
//       );

//       if (avgHistoric > 0) {
//         dropPercent = ((avgHistoric - metricValue) / avgHistoric) * 100;
//       }
//     }

//     // 1️⃣ Normal threshold check (must pass first)
//     const shouldTriggerNormal = await evaluateThreshold(
//       rule,
//       metricValue,
//       avgHistoric,
//       dropPercent
//     );

//     console.log(
//       `Rule ${rule.id} normal threshold result:`,
//       shouldTriggerNormal
//     );

//     if (!shouldTriggerNormal) {
//       // If it doesn't pass normal threshold, we don't even consider quiet/critical.
//       continue;
//     }

//     // 2️⃣ Quiet hours + critical override
//     let inQuiet = false;

//     if (
//       typeof rule.quiet_hours_start === "number" &&
//       typeof rule.quiet_hours_end === "number"
//     ) {
//       const qs = rule.quiet_hours_start;
//       const qe = rule.quiet_hours_end;

//       inQuiet =
//         qs < qe
//           ? currentISTHour >= qs && currentISTHour < qe
//           : currentISTHour >= qs || currentISTHour < qe;

//       if (inQuiet) {
//         const crit = Number(rule.critical_threshold);
//         const hasCrit =
//           !Number.isNaN(crit) &&
//           crit > 0 &&
//           rule.threshold_type === "percentage_drop";
//         const hasDrop =
//           typeof dropPercent === "number" && !Number.isNaN(dropPercent);

//         if (hasCrit && hasDrop && dropPercent >= crit) {
//           console.log(
//             `⚠️ Critical override for rule ${
//               rule.id
//             }: drop=${dropPercent.toFixed(
//               2
//             )}% >= critical=${crit}% — alert will fire even in quiet hours.`
//           );
//           // allow to proceed
//         } else {
//           console.log(
//             `⏳ Quiet hours active for rule ${rule.id}: Skipped. (${qs}:00 → ${qe}:00 IST) ` +
//               `drop=${hasDrop ? dropPercent.toFixed(2) : "N/A"}%, critical=${
//                 hasCrit ? crit : "N/A"
//               }`
//           );
//           continue;
//         }
//       }
//     }

//     // 3️⃣ Cooldown check (unchanged)
//     const cooldown = await checkCooldown(rule.id, rule.cooldown_minutes);
//     if (cooldown) {
//       console.log(
//         `Skipped trigger for rule ${rule.id} (${rule.name}) due to cooldown: ${rule.cooldown_minutes} minutes configured.`
//       );
//       continue;
//     }

//     // 4️⃣ Finally fire the alert
//     await triggerAlert(
//       rule,
//       event,
//       metricValue,
//       avgHistoric,
//       dropPercent,
//       hourCutoff
//     );
//   }
// }

// module.exports = { processIncomingEvent };




// NEW LOGIC
const pool = require("./db");
const { evaluate } = require("mathjs");
const nodemailer = require("nodemailer");

function log(stage, ...args) {
  console.log(`[AlertEngine:${stage}]`, ...args);
}

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
  `;
}

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
    log("historicalAvg:start", {
      brandId,
      metricName,
      hourCutoff,
      lookbackDays,
    });
    if (hourCutoff <= 0) return null;

    const [rows] = await pool.query("SELECT db_name FROM brands WHERE id = ?", [
      brandId,
    ]);
    if (!rows.length) return null;

    const dbName = rows[0].db_name;
    const days = Number(lookbackDays) > 0 ? Number(lookbackDays) : 7;

    if (metricName === "aov") {
      const [avgRows] = await pool.query(
        `
        SELECT AVG(daily_aov) AS avg_val, COUNT(*) AS day_count
        FROM (
          SELECT date,
            SUM(total_sales) / NULLIF(SUM(number_of_orders), 0) AS daily_aov
          FROM ${dbName}.hour_wise_sales
          WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
            AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
            AND hour < ?
          GROUP BY date
          HAVING SUM(number_of_orders) > 0
        ) t
        `,
        [days, hourCutoff]
      );

      if (!avgRows[0]?.avg_val) return null;
      const val = Number(Number(avgRows[0].avg_val).toFixed(2));
      log("historicalAvg:done", { metricName, avg: val });
      return val;
    }

    if (metricName === "conversion_rate") {
      const [avgRows] = await pool.query(
        `
        SELECT AVG(daily_cvr) AS avg_val, COUNT(*) AS day_count
        FROM (
          SELECT date,
            (SUM(number_of_orders) / NULLIF(SUM(number_of_sessions), 0)) * 100 AS daily_cvr
          FROM ${dbName}.hour_wise_sales
          WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
            AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
            AND hour < ?
          GROUP BY date
          HAVING SUM(number_of_sessions) > 0
        ) t
        `,
        [days, hourCutoff]
      );

      if (!avgRows[0]?.avg_val) return null;
      const val = Number(Number(avgRows[0].avg_val).toFixed(4));
      log("historicalAvg:done", { metricName, avg: val });
      return val;
    }

    const colMap = {
      total_orders: "number_of_orders",
      total_atc_sessions: "number_of_atc_sessions",
      total_sales: "total_sales",
      total_sessions: "number_of_sessions",
    };

    const col = colMap[metricName];
    if (!col) return null;

    const [avgRows] = await pool.query(
      `
      SELECT AVG(daily_val) AS avg_val
      FROM (
        SELECT date, SUM(${col}) AS daily_val
        FROM ${dbName}.hour_wise_sales
        WHERE date >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')) - INTERVAL ? DAY
          AND date < DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
          AND hour < ?
        GROUP BY date
      ) t
      `,
      [days, hourCutoff]
    );

    if (!avgRows[0]?.avg_val) return null;
    const val = Number(Number(avgRows[0].avg_val).toFixed(2));
    log("historicalAvg:done", { metricName, avg: val });
    return val;
  } catch (err) {
    console.error("🔥 historical avg error:", err.message);
    return null;
  }
}

/* -------------------------------------------------------
   Load Active Alerts
--------------------------------------------------------*/
async function loadRulesForBrand(brandId) {
  log("rules:load:start", { brandId });
  const [rules] = await pool.query(
    "SELECT * FROM alerts WHERE brand_id = ? AND is_active = 1",
    [brandId]
  );
  log("rules:load:done", { count: rules.length });
  return rules;
}

/* -------------------------------------------------------
   Parse JSON
--------------------------------------------------------*/
function parseChannelConfig(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    console.warn("⚠️ channel_config parse failed", raw);
    return null;
  }
}

/* -------------------------------------------------------
   Compute Metric
--------------------------------------------------------*/
async function computeMetric(rule, event) {
  try {
    if (rule.metric_type === "base") {
      const val = event[rule.metric_name];
      log("metric:base", { ruleId: rule.id, metric: rule.metric_name, val });
      return val;
    }
    if (rule.metric_type === "derived") {
      const val = evaluate(rule.formula, event);
      log("metric:derived", {
        ruleId: rule.id,
        metric: rule.metric_name,
        val,
      });
      return val;
    }
  } catch (err) {
    console.error("❌ Metric computation error", err.message);
    return null;
  }
}

/* -------------------------------------------------------
   Normalize Event
--------------------------------------------------------*/
function normalizeEventKeys(event) {
  const out = {};
  for (const [k, v] of Object.entries(event)) {
    out[k] = v;
    out[k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`)] = v;
  }
  log("event:normalized", {
    originalKeys: Object.keys(event),
    normalizedKeys: Object.keys(out),
  });
  return out;
}

/* -------------------------------------------------------
   Cooldown
--------------------------------------------------------*/
async function checkCooldown(alertId, cooldownMinutes) {
  log("cooldown:check:start", { alertId, cooldownMinutes });
  const [rows] = await pool.query(
    "SELECT triggered_at FROM alert_history WHERE alert_id=? ORDER BY triggered_at DESC LIMIT 1",
    [alertId]
  );
  if (!rows.length) return false;

  const triggeredIST = new Date(
    new Date(rows[0].triggered_at).toLocaleString("en-US", {
      timeZone: "Asia/Kolkata",
    })
  );
  const nowIST = new Date(
    new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
  );

  const minutesSince = (nowIST - triggeredIST) / 60000;
  const inCooldown = minutesSince < cooldownMinutes;
  log("cooldown:check:result", {
    alertId,
    minutesSince,
    inCooldown,
  });
  return inCooldown;
}

/* -------------------------------------------------------
   Email Sender
--------------------------------------------------------*/
async function sendEmail(cfg, subject, html) {
  try {
    log("email:send:start", { to: cfg.to, subject });
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.ALERT_EMAIL_USER,
        pass: process.env.ALERT_EMAIL_PASS,
      },
    });

    // await transporter.sendMail({
    //   from: `"Alerting System" <${cfg.smtp_user || process.env.ALERT_EMAIL_USER}>`,
    //   to: cfg.to.join(","),
    //   subject,
    //   html,
    // });

    await transporter.sendMail({
      from: `"Alerting System" <${cfg.smtp_user || process.env.ALERT_EMAIL_USER}>`,
      to: "aryan.arora@trytechit.co",
      subject,
      html,
    });
    log("email:send:done", { to: cfg.to, subject });
  } catch (err) {
    console.error("❌ Email send failed:", err);
    throw err;
  }
}

/* -------------------------------------------------------
   Trigger Alert (UPDATED)
--------------------------------------------------------*/
async function triggerAlert(
  rule,
  event,
  metricValue,
  avgHistoric,
  dropPercent,
  alertHour
) {
  log("alert:trigger:start", {
    ruleId: rule.id,
    brandId: event.brand_id,
    metricValue,
    avgHistoric,
    dropPercent,
    alertHour,
  });
  const emailHTML = generateEmailHTML(
    event,
    rule,
    metricValue,
    avgHistoric,
    dropPercent,
    alertHour
  );

  /* 🔥 NEW: Prefer brand-level channels */
  const [brandChannels] = await pool.query(
    `
    SELECT * FROM brands_alert_channel
    WHERE brand_id = ? AND is_active = 1
    `,
    [event.brand_id]
  );

  let channels = brandChannels;
  log("alert:channels:brand", { count: brandChannels.length });

  if (!channels.length) {
    const [alertChannels] = await pool.query(
      "SELECT * FROM alert_channels WHERE alert_id = ?",
      [rule.id]
    );
    channels = alertChannels;
    log("alert:channels:fallback", { count: channels.length });
  }

  for (const ch of channels) {
    if (ch.channel_type !== "email") continue;

    const cfg = parseChannelConfig(ch.channel_config);
    if (!cfg || !cfg.to) {
      log("alert:channel:skip", { reason: "invalid config", channelId: ch.id });
      continue;
    }

    log("alert:channel:email", {
      channelId: ch.id,
      recipients: cfg.to,
    });

    const metricName =
      rule.metric_name.replace(/_/g, " ").charAt(0).toUpperCase() +
      rule.metric_name.replace(/_/g, " ").slice(1);

    const dropVal =
      typeof dropPercent === "number"
        ? Math.abs(dropPercent).toFixed(2)
        : "0.00";

    const subject = `${metricName} Alert | ${dropVal}% Drop | ${event.brand.toUpperCase()} | 0 - ${alertHour} Hours`;

    log("alert:email:subject", { subject });
    await sendEmail(cfg, subject, emailHTML);
  }

  await pool.query(
    "INSERT INTO alert_history (alert_id, brand_id, payload) VALUES (?, ?, ?)",
    [rule.id, rule.brand_id, JSON.stringify(event)]
  );
  log("alert:trigger:recorded", { ruleId: rule.id });
}

/* -------------------------------------------------------
   Threshold Evaluation
--------------------------------------------------------*/
async function evaluateThreshold(rule, metricValue, avgHistoric, dropPercent) {
  const threshold = Number(rule.threshold_value);
  let result = false;
  if (rule.threshold_type === "percentage_drop")
    result = dropPercent >= threshold;
  else if (rule.threshold_type === "percentage_rise")
    result = -dropPercent >= threshold;
  else result = metricValue < threshold;
  log("threshold:evaluate", {
    ruleId: rule.id,
    type: rule.threshold_type,
    metricValue,
    avgHistoric,
    dropPercent,
    threshold,
    result,
  });
  return result;
}

/* -------------------------------------------------------
   Main Controller
--------------------------------------------------------*/
async function processIncomingEvent(event) {
  if (typeof event === "string") event = JSON.parse(event);
  log("event:received", event);
  event = normalizeEventKeys(event);

  let brandId = event.brand_id;

  // Resolve brand_id from brand_key if missing
  if (!brandId && event.brand_key) {
    log("brand:lookup", { brand_key: event.brand_key });
    const [brands] = await pool.query(
      "SELECT id FROM brands WHERE name = ?",
      [event.brand_key]
    );
    if (brands.length > 0) {
      brandId = brands[0].id;
      event.brand_id = brandId; // Add to event for later use
      log("brand:resolved", { brand_key: event.brand_key, brandId });
      // Ensure brand is present for alert subjects
      if (!event.brand) event.brand = event.brand_key;
    } else {
      log("brand:error", { reason: "brand_key not found", brand_key: event.brand_key });
      return; // Stop if brand unknown
    }
  }

  if (!brandId) {
    log("event:error", { reason: "missing brand_id and brand_key" });
    return;
  }

  const rules = await loadRulesForBrand(brandId);
  log("rules:count", { count: rules.length });

  const currentISTHour = Number(
    new Intl.DateTimeFormat("en-US", {
      timeZone: "Asia/Kolkata",
      hour: "2-digit",
      hour12: false,
    }).format(new Date())
  );

  const hourCutoff =
    typeof event.hour === "number" ? event.hour : currentISTHour;
  log("hours", { currentISTHour, hourCutoff });

  for (const rule of rules) {
    log("rule:start", { id: rule.id, name: rule.name, metric: rule.metric_name });
    const metricValue = await computeMetric(rule, event);
    if (metricValue == null) {
      log("rule:skip", { ruleId: rule.id, reason: "metric null" });
      continue;
    }

    let avgHistoric = null;
    let dropPercent = null;

    if (rule.threshold_type.includes("percentage") || rule.metric_name === "performance") {
      // Calculate drop relative to previous alert for performance
      if (rule.metric_name === "performance") {
        log("rule:performance:lookup", { ruleId: rule.id });
        const [history] = await pool.query(
          "SELECT payload FROM alert_history WHERE alert_id = ? ORDER BY triggered_at DESC LIMIT 1",
          [rule.id]
        );
        log("rule:performance:history", { count: history.length, ruleId: rule.id });

        if (history.length > 0) {
          try {
            const raw = history[0].payload;
            const prevPayload = typeof raw === "string" ? JSON.parse(raw) : raw;
            const prevValue = prevPayload.performance;
            if (typeof prevValue === "number" && prevValue > 0) {
              avgHistoric = prevValue;
              dropPercent = ((prevValue - metricValue) / prevValue) * 100;
              log("rule:performance:drop", { ruleId: rule.id, prevValue, currentValue: metricValue, dropPercent });
            }
          } catch (e) {
            log("rule:performance:error", { ruleId: rule.id, error: e.message });
          }
        } else {
          log("rule:performance:first", { ruleId: rule.id, reason: "no history" });
        }
      } else {
        avgHistoric = await getHistoricalAvgForMetric(
          brandId,
          rule.metric_name,
          hourCutoff,
          rule.lookback_days || 7
        );
        if (avgHistoric > 0)
          dropPercent = ((avgHistoric - metricValue) / avgHistoric) * 100;
        log("rule:historical", { ruleId: rule.id, avgHistoric, dropPercent });
      }
    }

    if (
      !(await evaluateThreshold(rule, metricValue, avgHistoric, dropPercent))
    )
      continue;

    if (await checkCooldown(rule.id, rule.cooldown_minutes)) {
      log("rule:skip", { ruleId: rule.id, reason: "cooldown" });
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
    log("rule:completed", { ruleId: rule.id });
  }
}

module.exports = { processIncomingEvent };
