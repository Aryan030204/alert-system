/* -------------------------------------------------------
   Discount monitor email builder.
   Turns the structured `brand_results` from discount_monitor/run.py
   (--json-output) into one readable HTML digest + an alert-style subject.
   Inline-styled tables only, for email-client compatibility.
--------------------------------------------------------*/

const { renderKpiCards } = require("./kpiCards");

const C = {
  red: "#dc2626",
  amber: "#f59e0b",
  green: "#10b981",
  ink: "#111827",
  text: "#374151",
  muted: "#6b7280",
  border: "#e5e7eb",
  card: "#f9fafb",
  indigo: "#4f46e5",
  amberSoft: "#fef3c7",
};

const ALERT_META = {
  DISCOUNT_RATE_SPIKE: { name: "Discount Rate Spike", tier: "ALERT", priority: 1 },
  USAGE_RATE_SPIKE: { name: "Discount Usage Spike", tier: "ALERT", priority: 2 },
  NEW_CODE: { name: "New Discount Code", tier: "ALERT", priority: 3 },
  DISCOUNT_OUTPACING_SALES: { name: "Discount Outpacing Sales", tier: "WATCH", priority: 4 },
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const isNum = (x) => typeof x === "number" && Number.isFinite(x);

function fmtAmount(x) {
  if (!isNum(x)) return "—";
  const sign = x < 0 ? "-" : "";
  const v = Math.abs(x);
  if (v >= 1e7) return `${sign}Rs ${(v / 1e7).toFixed(2)}Cr`;
  if (v >= 1e5) return `${sign}Rs ${(v / 1e5).toFixed(2)}L`;
  return `${sign}Rs ${Math.round(v).toLocaleString("en-IN")}`;
}

const fmtInt = (x) => (isNum(x) ? Math.round(x).toLocaleString("en-IN") : "—");
const fmtPct = (x, digits = 1) => (isNum(x) ? `${x.toFixed(digits)}%` : "—");
const fmtRate = (x) => (isNum(x) ? `${x.toFixed(Math.abs(x) < 1 ? 2 : 1)}%` : "—");
const fmtDelta = (x) => (!isNum(x) ? "new" : `${x >= 0 ? "+" : ""}${x.toFixed(0)}%`);

// upIsBad: discount metrics rising is a concern; sales rising is good.
function deltaColor(delta, upIsBad) {
  if (!isNum(delta) || Math.abs(delta) < 0.5) return C.muted;
  const up = delta > 0;
  return up === upIsBad ? C.red : C.green;
}

/* ---------- alert helpers ---------- */

function alertDelta(type, snapshot) {
  if (type === "DISCOUNT_RATE_SPIKE") return snapshot.discount_rate_delta;
  if (type === "USAGE_RATE_SPIKE") return snapshot.usage_rate_delta;
  if (type === "DISCOUNT_OUTPACING_SALES") return snapshot.discount_amount_delta;
  return null; // NEW_CODE has no baseline, so no delta
}

// Transition framing, mirrors alertEngine.js's TRANSITION_COLOR/LABEL for
// the COD digest -- keep the three colors in sync if either changes.
function transitionColor(newState) {
  if (newState === "CRITICAL") return C.red;
  if (newState === "NORMAL") return C.green;
  return C.amber;
}
function transitionLabel(newState) {
  if (newState === "CRITICAL") return "ESCALATED";
  if (newState === "NORMAL") return "RESOLVED";
  return "NEW";
}

function sortedAlerts(brandResult) {
  return [...(brandResult.transitions || [])]
    .filter((t) => ALERT_META[t.alertType])
    .sort((a, b) => ALERT_META[a.alertType].priority - ALERT_META[b.alertType].priority);
}

function alertExplanation(type, br) {
  const s = br.snapshot || {};
  if (type === "DISCOUNT_RATE_SPIKE") {
    return `Discount rate is ${fmtRate(s.discount_rate_current)} vs ${fmtRate(s.discount_rate_baseline)} baseline (${fmtDelta(s.discount_rate_delta)}) — more of each sale is being given away.`;
  }
  if (type === "USAGE_RATE_SPIKE") {
    return `${fmtRate(s.usage_rate_current)} of orders used a discount vs ${fmtRate(s.usage_rate_baseline)} baseline (${fmtDelta(s.usage_rate_delta)}) — more orders are using a code, even if the amount looks flat.`;
  }
  if (type === "DISCOUNT_OUTPACING_SALES") {
    return `Discount amount is ${fmtDelta(s.discount_amount_delta)} vs baseline while total sales are ${fmtDelta(s.sales_delta)} — discounts are growing faster than revenue.`;
  }
  if (type === "NEW_CODE") {
    const nc = (br.new_codes || [])[0];
    return nc
      ? `${nc.code} already makes up ${fmtPct(nc.orders_share_pct, 0)} of today's discounted orders and wasn't used in the last 7 days.`
      : "A discount code with no recent history is suddenly being used.";
  }
  return "";
}

/* ---------- subject ---------- */

// Lead brand = highest-priority transition across brands, ties broken by
// larger delta. CRITICAL transitions always outrank TRIGGERED/NORMAL ones
// regardless of alert-type priority, so an escalation never gets buried
// behind a routine new-breach in the subject line.
function pickLead(brandEntries) {
  let lead = null;
  for (const br of brandEntries) {
    const top = sortedAlerts(br)[0];
    if (!top) continue;
    const delta = alertDelta(top.alertType, br.snapshot || {});
    const severityRank = top.newState === "CRITICAL" ? 0 : top.newState === "TRIGGERED" ? 1 : 2;
    const candidate = {
      brand: br.brand,
      type: top.alertType,
      newState: top.newState,
      delta,
      priority: ALERT_META[top.alertType].priority,
      severityRank,
    };
    const better =
      !lead ||
      candidate.severityRank < lead.severityRank ||
      (candidate.severityRank === lead.severityRank && candidate.priority < lead.priority) ||
      (candidate.severityRank === lead.severityRank &&
        candidate.priority === lead.priority &&
        (delta ?? -Infinity) > (lead.delta ?? -Infinity));
    if (better) lead = candidate;
  }
  return lead;
}

function buildDiscountSubject(brandEntries, istHour) {
  const lead = pickLead(brandEntries);
  if (!lead) return `Discount Monitor | ${brandEntries.length} changed | 0-${istHour}h`;

  const others = brandEntries.length - 1;
  const brandLabel = `${String(lead.brand).toUpperCase()}${others > 0 ? ` +${others} more` : ""}`;
  const deltaPart = isNum(lead.delta)
    ? ` | ${Math.abs(lead.delta).toFixed(2)}% ${lead.delta < 0 ? "Drop" : "Rise"}`
    : "";
  const prefix =
    lead.newState === "CRITICAL" ? "ESCALATED: " : lead.newState === "NORMAL" ? "RESOLVED: " : "";
  return `${prefix}${brandLabel} | ${ALERT_META[lead.type].name}${deltaPart} | 0-${istHour}h`;
}

/* ---------- section renderers ---------- */

function chip(text, bg, color = "#ffffff") {
  return `<span style="display:inline-block; background:${bg}; color:${color}; font-size:11px; font-weight:700; letter-spacing:0.04em; padding:3px 9px; border-radius:999px; line-height:1.4; max-width:100%; word-break:break-word;">${escapeHtml(text)}</span>`;
}

function sectionTitle(text) {
  return `<h4 style="margin:18px 0 8px; font-size:11px; font-weight:700; letter-spacing:0.06em; text-transform:uppercase; color:${C.muted};">${escapeHtml(text)}</h4>`;
}

function renderTriggers(br) {
  const rows = sortedAlerts(br)
    .map((t) => {
      const meta = ALERT_META[t.alertType];
      const color = transitionColor(t.newState);
      return `
        <div style="padding:9px 0; border-bottom:1px solid #f3f4f6;">
          ${chip(meta.name, color)} ${chip(transitionLabel(t.newState), color)}
          <div style="font-size:13px; line-height:1.5; color:${C.text}; padding-top:6px;">${escapeHtml(alertExplanation(t.alertType, br))}</div>
        </div>`;
    })
    .join("");
  return `${sectionTitle("What changed")}${rows}`;
}

function th(text, align = "right") {
  return `<th style="padding:6px 4px; text-align:${align}; font-size:10px; font-weight:700; letter-spacing:0.05em; text-transform:uppercase; color:${C.muted}; border-bottom:2px solid ${C.border};">${escapeHtml(text)}</th>`;
}

function td(html, { align = "right", bold = false, color = C.text, bg = "" } = {}) {
  return `<td style="padding:8px 4px; text-align:${align}; font-size:13px; font-weight:${bold ? 700 : 400}; color:${color}; border-bottom:1px solid #f3f4f6; ${bg ? `background:${bg};` : ""}">${html}</td>`;
}

function renderComparison(br) {
  const s = br.snapshot || {};
  const cur = s.current || {};
  const base = s.baseline || {};
  const rows = [
    ["Discount amount", fmtAmount(cur.discount_amount), fmtAmount(base.discount_amount), s.discount_amount_delta, true],
    ["Total sales", fmtAmount(cur.gross_sales), fmtAmount(base.gross_sales), s.sales_delta, false],
    ["Discount rate", fmtRate(s.discount_rate_current), fmtRate(s.discount_rate_baseline), s.discount_rate_delta, true],
    ["Discount usage", fmtRate(s.usage_rate_current), fmtRate(s.usage_rate_baseline), s.usage_rate_delta, true],
  ]
    .map(
      ([label, now, before, delta, upIsBad]) => `
        <tr>
          ${td(escapeHtml(label), { align: "left", color: C.ink })}
          ${td(escapeHtml(now), { bold: true, color: C.ink })}
          ${td(escapeHtml(before), { color: C.muted })}
          ${td(escapeHtml(fmtDelta(delta)), { bold: true, color: deltaColor(delta, upIsBad) })}
        </tr>`,
    )
    .join("");
  return `
    ${sectionTitle("Today vs 7-day baseline")}
    <table role="presentation" style="width:100%; border-collapse:collapse;">
      <tr>${th("Metric", "left")}${th("Today")}${th("Baseline")}${th("Change")}</tr>
      ${rows}
    </table>`;
}

function renderCodes(br) {
  let out = "";
  const tc = br.top_code;
  if (tc) {
    const split =
      Math.abs((tc.amount_share_pct ?? 0) - (tc.orders_share_pct ?? 0)) >= 15
        ? ` · ${fmtPct(tc.amount_share_pct, 0)} of discount amount`
        : "";
    out += `${sectionTitle("Top discount code")}
      <div style="background:${C.card}; border:1px solid ${C.border}; border-radius:10px; padding:10px 12px; font-size:13px; color:${C.text};">
        ${chip(tc.code, C.indigo)}&nbsp; ${fmtPct(tc.orders_share_pct, 0)} of discounted orders${split}
      </div>`;
  }
  const newCodes = (br.new_codes || []).slice(0, 3);
  if (newCodes.length) {
    const lines = newCodes
      .map(
        (nc) =>
          `<div style="padding:4px 0; font-size:13px; color:${C.text};">${chip(nc.code, C.red)}&nbsp; ${fmtPct(nc.orders_share_pct, 0)} of today's discounted orders · none in the last 7 days</div>`,
      )
      .join("");
    out += `${sectionTitle("New discount codes")}<div style="background:${C.card}; border:1px solid ${C.border}; border-radius:10px; padding:8px 12px;">${lines}</div>`;
  }
  return out;
}

function renderUtm(br) {
  const rows = (br.utm_rows || []).slice(0, 5);
  if (!rows.length) return "";
  const body = rows
    .map((r) => {
      const bg = r.flagged ? C.amberSoft : "";
      const flag = r.flagged ? ` ${chip("FLAGGED", C.amber)}` : "";
      return `
        <tr>
          ${td(`${escapeHtml(r.source)}${flag}`, { align: "left", color: C.ink, bold: !!r.flagged, bg })}
          ${td(escapeHtml(fmtPct(r.baseline_share)), { color: C.muted, bg })}
          ${td(escapeHtml(fmtPct(r.current_share)), { bold: true, color: C.ink, bg })}
          ${td(escapeHtml(fmtDelta(r.delta_pct)), { bold: true, color: deltaColor(r.delta_pct, true), bg })}
        </tr>`;
    })
    .join("");
  return `
    ${sectionTitle("Discount by UTM source")}
    <table role="presentation" style="width:100%; border-collapse:collapse;">
      <tr>${th("Source", "left")}${th("Baseline")}${th("Today")}${th("Change")}</tr>
      ${body}
    </table>`;
}

function renderDrill(br) {
  const cards = (br.flagged_sources || [])
    .filter((fs) => fs.drill)
    .map((fs) => {
      const d = fs.drill;
      const codes = (d.codes || [])
        .map((c) => {
          const label = c.diverges
            ? `${c.code} · ${fmtPct(c.amount_share_pct, 0)} amt (${fmtPct(c.orders_share_pct, 0)} orders)`
            : `${c.code} · ${fmtPct(c.amount_share_pct, 0)}`;
          return `<span style="display:inline-block; margin:4px 6px 0 0;">${chip(label, "#eef2ff", C.indigo)}</span>`;
        })
        .join("");
      return `
        <div style="background:${C.card}; border:1px solid ${C.border}; border-radius:10px; padding:12px; margin-bottom:10px;">
          <div style="font-size:14px; font-weight:700; color:${C.ink};">${escapeHtml(fs.source)} &rarr; ${escapeHtml(d.campaign)}</div>
          <div style="font-size:13px; color:${C.text}; padding-top:4px;">
            ${fmtPct(d.baseline_share)} &rarr; <strong>${fmtPct(d.current_share)}</strong> of ${escapeHtml(fs.source)}'s discount
            <strong style="color:${deltaColor(d.delta_pct, true)};">(${escapeHtml(fmtDelta(d.delta_pct))})</strong>
          </div>
          ${codes ? `<div style="padding-top:4px;">${codes}</div>` : ""}
        </div>`;
    })
    .join("");
  return cards ? `${sectionTitle("Campaign drill-down")}${cards}` : "";
}

// Brand-level badge = worst transition present: any CRITICAL outranks any
// TRIGGERED, which outranks an all-RESOLVED brand (nothing left breaching).
function brandBadge(br) {
  const states = (br.transitions || []).map((t) => t.newState);
  if (states.includes("CRITICAL")) return { label: "CRITICAL", color: C.red };
  if (states.includes("TRIGGERED")) return { label: "NEW", color: C.amber };
  if (states.length) return { label: "RESOLVED", color: C.green };
  return { label: "WATCH", color: C.amber };
}

function renderBrandSection(br) {
  const badge = brandBadge(br);
  return `
    <div style="border:1px solid ${C.border}; border-radius:12px; padding:14px; margin-bottom:16px;">
      <table role="presentation" style="width:100%; border-collapse:collapse;"><tr>
        <td style="font-size:20px; font-weight:800; color:${C.ink};">${escapeHtml(String(br.brand).toUpperCase())}</td>
        <td style="text-align:right;">${chip(badge.label, badge.color)}</td>
      </tr></table>
      ${renderKpiCards(br.kpis)}
      ${renderTriggers(br)}
      ${renderComparison(br)}
      ${renderCodes(br)}
      ${renderUtm(br)}
      ${renderDrill(br)}
    </div>`;
}

/* ---------- public builder ---------- */

function buildDiscountMonitorEmail(body, { istHour = 0 } = {}) {
  // body.brand_results here is the already transition-filtered list from
  // alertEngine.js's evaluateDiscountTransitions() -- only brands with at
  // least one NORMAL<->TRIGGERED<->CRITICAL transition this run, each
  // carrying a `transitions` array (not the raw alerts_fired/tier fields).
  const brandEntries = Array.isArray(body.brand_results) ? body.brand_results : [];
  const runDate = body.run_date || "";

  // Older payloads without structured data: keep the plain digest readable.
  if (!brandEntries.length) {
    const subject = `Discount Monitor | 0 changed | 0-${istHour}h`;
    const html = `<html><body style="font-family:Arial,sans-serif;"><pre style="white-space:pre-wrap;">${escapeHtml(body.digest_text || "No digest available.")}</pre></body></html>`;
    return { subject, html };
  }

  const hasCritical = brandEntries.some((br) => (br.transitions || []).some((t) => t.newState === "CRITICAL"));
  const allResolved = brandEntries.every((br) => (br.transitions || []).every((t) => t.newState === "NORMAL"));
  const bannerColor = hasCritical ? C.red : allResolved ? C.green : C.amber;
  const n = brandEntries.length;
  const normal = Array.isArray(body.normal_brands) ? body.normal_brands : [];
  const skipped = Array.isArray(body.skipped_brands) ? body.skipped_brands : [];

  const footerLines = [
    normal.length ? `Normal: ${normal.join(", ")}` : "",
    skipped.length ? `Skipped (low traffic / error): ${skipped.join(", ")}` : "",
  ].filter(Boolean);

  const html = `
  <html>
  <body style="margin:0; padding:0; background:#f4f6fb; font-family:Arial, sans-serif;">
    <div style="max-width:680px; margin:12px auto; background:#ffffff; border-radius:12px; overflow:hidden; box-shadow:0 6px 25px rgba(0,0,0,0.08);">

      <div style="background:${bannerColor}; padding:18px 18px; color:#ffffff;">
        <h2 style="margin:0; font-size:20px; font-weight:600; line-height:1.3;">🏷️ Discount Monitor — ${n} brand${n === 1 ? "" : "s"} changed state</h2>
        <p style="margin:6px 0 0; font-size:12px; opacity:0.92; line-height:1.5;">
          ${escapeHtml(runDate)} · ${Number(body.brand_count || 0)} brands checked · ${Number(body.normal_count || normal.length)} normal
        </p>
      </div>

      <div style="padding:16px 12px; line-height:1.5; color:${C.text};">
        ${brandEntries.map(renderBrandSection).join("")}
        ${
          footerLines.length
            ? `<p style="margin:0 0 16px; font-size:13px; color:${C.muted};">${footerLines.map(escapeHtml).join("<br>")}</p>`
            : ""
        }
        <p style="font-size:13px; color:#4b5563; margin:8px 0 0;">
          Take a look at the latest activity on your dashboard for possible causes:
          <a href="https://datum.trytechit.co/" style="color:${C.indigo}; text-decoration:underline;">https://datum.trytechit.co/</a>
        </p>
      </div>

      <div style="background:#f3f4f6; padding:14px; text-align:center;">
        <span style="font-size:12px; color:${C.muted};">© ${new Date().getFullYear()} Datum Inc.</span>
      </div>
    </div>
  </body>
  </html>`;

  return { subject: buildDiscountSubject(brandEntries, istHour), html };
}

module.exports = { buildDiscountMonitorEmail, buildDiscountSubject };
