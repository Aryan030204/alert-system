/* -------------------------------------------------------
   Shared KPI cards (Total Sales, Sessions, ATC Sessions, CVR, AOV)
   with change vs the 7-day average. Used by the discount and COD
   monitor emails. Inline-styled tables for email-client compatibility.
--------------------------------------------------------*/

const C = {
  red: "#dc2626",
  green: "#10b981",
  ink: "#111827",
  muted: "#6b7280",
  border: "#e5e7eb",
  card: "#f9fafb",
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

// All five KPIs are "up is good".
function deltaColor(delta) {
  if (!isNum(delta) || Math.abs(delta) < 0.5) return C.muted;
  return delta > 0 ? C.green : C.red;
}

function sectionTitle(text) {
  return `<h4 style="margin:18px 0 8px; font-size:11px; font-weight:700; letter-spacing:0.06em; text-transform:uppercase; color:${C.muted};">${escapeHtml(text)}</h4>`;
}

function card(label, value, delta) {
  return `
    <td style="background:${C.card}; border:1px solid ${C.border}; border-radius:10px; padding:10px 4px; text-align:center;">
      <div style="font-size:9px; font-weight:700; letter-spacing:0.04em; text-transform:uppercase; color:${C.muted}; padding-bottom:5px;">${escapeHtml(label)}</div>
      <div style="font-size:15px; font-weight:700; color:${C.ink}; white-space:nowrap;">${escapeHtml(value)}</div>
      <div style="font-size:11px; font-weight:700; padding-top:4px; color:${isNum(delta) ? deltaColor(delta) : C.muted};">${isNum(delta) ? `${delta >= 0 ? "▲" : "▼"} ${Math.abs(delta).toFixed(0)}%` : "—"}</div>
    </td>`;
}

const SPACER = `<td style="width:8px;"></td>`;

/**
 * Five cards in a 3 + 2 grid (equal columns): stays readable on phones,
 * where a single row of five collapses into unreadable slivers.
 * @param kpis  { total_sales, sessions, atc_rate, cvr, aov, deltas: {...} } or null
 * @param title section heading shown above the cards
 */
function renderKpiCards(kpis, title = "Today so far (IST) · vs 7-day avg") {
  if (!kpis) {
    return `${sectionTitle(title)}<div style="font-size:13px; color:${C.muted}; background:${C.card}; border:1px dashed ${C.border}; border-radius:10px; padding:12px 14px;">KPI data unavailable for this run.</div>`;
  }
  const deltas = kpis.deltas || {};
  const atcRate = isNum(kpis.atc_rate)
    ? kpis.atc_rate
    : kpis.sessions
      ? (kpis.atc_sessions / kpis.sessions) * 100
      : null;

  const c1 = card("Total Sales", fmtAmount(kpis.total_sales), deltas.total_sales);
  const c2 = card("Sessions", fmtInt(kpis.sessions), deltas.sessions);
  const c3 = card("ATC Rate", fmtPct(atcRate, 2), deltas.atc_rate);
  const c4 = card("CVR", fmtPct(kpis.cvr, 2), deltas.cvr);
  const c5 = card("AOV", fmtAmount(kpis.aov), deltas.aov);

  return `
    ${sectionTitle(title)}
    <table role="presentation" style="width:100%; table-layout:fixed; border-collapse:separate; border-spacing:0;">
      <tr>${c1}${SPACER}${c2}${SPACER}${c3}</tr>
      <tr><td colspan="5" style="height:8px; line-height:8px; font-size:8px;">&nbsp;</td></tr>
      <tr>${c4}${SPACER}${c5}${SPACER}<td></td></tr>
    </table>`;
}

module.exports = { renderKpiCards };
