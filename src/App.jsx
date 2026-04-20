import { useState, useCallback, useRef, useEffect } from "react";
import XLSX from "xlsx-js-style";

// ─── Pod config ───────────────────────────────────────────────────────────────
const PODS = [
  { id: "zain-nia",        bdr: "Zain",  aes: ["Nia"] },
  { id: "dani-mike-gavin", bdr: "Dani",  aes: ["Mike", "Gavin"] },
  { id: "jacob-bobby",     bdr: "Jacob", aes: ["Bobby"] },
];

// FurtherAI palette — dark green theme
const THEME = {
  bg:        "#0B1F14",        // dark forest green
  surface:   "#0F2B1C",        // slightly lighter green
  border:    "#1A3D2A",        // muted green border
  borderHi:  "#245535",        // highlighted border
  text:      "#F0F0F0",        // white text
  muted:     "#7A9B8A",        // muted green-gray
  accent:    "#4AFFC4",        // keep teal-green accent
  accentDim: "#1A3D33",        // dimmed accent
  serif:     "'DM Serif Display', serif", // for headings
};

const POD_COLORS = {
  "zain-nia":        { bg: "#0D2137", border: "#1A4A7A", text: "#60A5FA", label: "Zain / Nia" },
  "dani-mike-gavin": { bg: "#0D2818", border: "#1A5C2E", text: "#4ADE80", label: "Dani / Mike & Gavin" },
  "jacob-bobby":     { bg: "#2A2200", border: "#5C4A00", text: "#FBBF24", label: "Jacob / Bobby" },
};

const OWNER_PALETTE = [
  { bg: "#1F0D2E", border: "#4A1E70", text: "#C084FC" },
  { bg: "#2E0D1A", border: "#701E3A", text: "#FB7185" },
  { bg: "#1A1A00", border: "#4A4A00", text: "#FACC15" },
  { bg: "#001A2E", border: "#004A70", text: "#38BDF8" },
  { bg: "#001A0D", border: "#004A1E", text: "#34D399" },
];

const UNASSIGNED_COLOR = { bg: "#111118", border: "#2A2A3A", text: "#6B6B8A" };

let palIdx = 0;
const palCache = {};

function ownerColor(name) {
  if (!name) return UNASSIGNED_COLOR;
  const pid = podForOwner(name);
  if (pid) return POD_COLORS[pid];
  if (!palCache[name]) { palCache[name] = OWNER_PALETTE[palIdx % OWNER_PALETTE.length]; palIdx++; }
  return palCache[name];
}

function podForOwner(name) {
  if (!name) return null;
  const l = name.toLowerCase();
  for (const p of PODS) {
    if (l.includes(p.bdr.toLowerCase())) return p.id;
    if (p.aes.some(ae => l.includes(ae.toLowerCase()))) return p.id;
  }
  return null;
}

function normalize(s) {
  return (s || "").toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|co\.?|group|holdings|insurance|services|the)\b/g, "")
    .replace(/[^a-z0-9]/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeDomain(d) {
  if (!d) return "";
  return String(d).trim().toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
}

// True if a === b, or the shorter is a whole-word prefix of the longer (space boundary).
// Requires shorter ≥ 6 chars to reject generic short substrings.
function tokenPrefixMatch(a, b) {
  if (!a || !b) return false;
  if (a === b) return true;
  const [shorter, longer] = a.length < b.length ? [a, b] : [b, a];
  if (shorter.length < 6) return false;
  return longer.startsWith(shorter + " ");
}

// Detect hallucinated AI text masquerading as company names
const HALLUCINATION_PATTERNS = [
  /\b(checking|reviewing|contacting|visiting|looking up|searching)\b/i,
  /\b(conference|attendee|organizer|registration|directory|roster)\b/i,
  /\b(linkedin|professional profile|professional database|professional networking)\b/i,
  /\b(company directories|company rosters|company employee|employment records)\b/i,
  /\b(official .*(list|app|website|materials|program))/i,
  /\b(access to the|confirmed employment|recent company)/i,
  /^current professional/i,
];
function isHallucinatedCompany(value) {
  if (!value || value.length < 3) return false;
  const v = value.trim();
  // Real company names are rarely this long
  if (v.length > 80) return true;
  return HALLUCINATION_PATTERNS.some(p => p.test(v));
}

// ─── Known Insurance Companies (pre-classification override) ─────────────────
// Catches major companies that the Claude API frequently misclassifies
const KNOWN_COMPANIES = [
  // Insurance Carriers
  { pattern: /\bcna\b/i, type: "Insurance Carrier" },
  { pattern: /\bbeazley\b/i, type: "Insurance Carrier" },
  { pattern: /\baxa\s*xl\b/i, type: "Insurance Carrier" },
  { pattern: /\bhanover\b.*\b(insurance|group)\b/i, type: "Insurance Carrier" },
  { pattern: /\bintact\b.*\b(financial|insurance)\b/i, type: "Insurance Carrier" },
  { pattern: /\bfederated\s*mutual\b/i, type: "Insurance Carrier" },
  { pattern: /\binigo\b/i, type: "Insurance Carrier" },
  { pattern: /\bvantage\s*risk\b/i, type: "Insurance Carrier" },
  { pattern: /\bqbe\b/i, type: "Insurance Carrier" },
  { pattern: /\ballied\s*world\b/i, type: "Insurance Carrier" },
  { pattern: /\bfalvey\b.*insurance/i, type: "Insurance Carrier" },
  { pattern: /\bcanopius\b/i, type: "Insurance Carrier" },
  { pattern: /\bliberty\s*mutual\b/i, type: "Insurance Carrier" },
  { pattern: /\btravelers\b.*\b(insurance|companies)?\b/i, type: "Insurance Carrier" },
  { pattern: /\bchubb\b/i, type: "Insurance Carrier" },
  { pattern: /\bzurich\b.*\b(insurance|resilience|north\s*america)?\b/i, type: "Insurance Carrier" },
  { pattern: /\bhartford\b.*\b(financial|insurance)?\b/i, type: "Insurance Carrier" },
  { pattern: /\bnationwide\b.*\b(mutual|insurance)\b/i, type: "Insurance Carrier" },
  { pattern: /\bprogressive\b.*insurance/i, type: "Insurance Carrier" },
  { pattern: /\bmarkel\b/i, type: "Insurance Carrier" },
  { pattern: /\baxis\b.*\b(capital|insurance)\b/i, type: "Insurance Carrier" },
  { pattern: /\bhiscox\b/i, type: "Insurance Carrier" },
  { pattern: /\ballianz\b/i, type: "Insurance Carrier" },
  { pattern: /\baig\b/i, type: "Insurance Carrier" },
  { pattern: /\btokio\s*marine\b/i, type: "Insurance Carrier" },
  { pattern: /\bsompo\b/i, type: "Insurance Carrier" },
  { pattern: /\bw\.?r\.?\s*berkley\b/i, type: "Insurance Carrier" },
  { pattern: /\bsiriuspoint\b/i, type: "Insurance Carrier" },
  { pattern: /\bscor\b/i, type: "Insurance Carrier" },
  { pattern: /\beverest\b.*\b(re|group)\b/i, type: "Insurance Carrier" },
  { pattern: /\blancashire\b/i, type: "Insurance Carrier" },
  { pattern: /\bfidelis\b/i, type: "Insurance Carrier" },
  { pattern: /\berie\s*insurance\b/i, type: "Insurance Carrier" },
  { pattern: /\bselective\s*insurance\b/i, type: "Insurance Carrier" },
  { pattern: /\bgreat\s*american\b.*insurance/i, type: "Insurance Carrier" },
  { pattern: /\busaig\b/i, type: "Insurance Carrier" },
  { pattern: /\bargo\b.*group/i, type: "Insurance Carrier" },
  { pattern: /\baspen\b.*\b(insurance|re)\b/i, type: "Insurance Carrier" },
  { pattern: /\bemperio?n\b/i, type: "Insurance Carrier" },
  { pattern: /\bsigma7\b/i, type: "Insurance Carrier" },
  // Insurance Brokers
  { pattern: /\bgallagher\b(?!.*bassett)/i, type: "Insurance Broker" },
  { pattern: /\bgallagher\s*bassett\b/i, type: "TPA / Claims Admin" },
  { pattern: /\balliant\b.*insurance/i, type: "Insurance Broker" },
  { pattern: /\bnfp\b/i, type: "Insurance Broker" },
  { pattern: /\brisk\s*placement\s*services\b/i, type: "Insurance Broker" },
  { pattern: /\blockton\b/i, type: "Insurance Broker" },
  { pattern: /\bbrown\s*&\s*brown\b/i, type: "Insurance Broker" },
  { pattern: /\bhub\s*international\b/i, type: "Insurance Broker" },
  { pattern: /\busi\b.*\b(insurance|services)\b/i, type: "Insurance Broker" },
  { pattern: /\bacrisure\b/i, type: "Insurance Broker" },
  { pattern: /\bassuredpartners\b/i, type: "Insurance Broker" },
  { pattern: /\bmcgriff\b/i, type: "Insurance Broker" },
  { pattern: /\balera\s*group\b/i, type: "Insurance Broker" },
  { pattern: /\bhigginbotham\b/i, type: "Insurance Broker" },
  { pattern: /\bholmes\s*murphy\b/i, type: "Insurance Broker" },
  // MGA / Specialty Underwriter
  { pattern: /\bambridge\b/i, type: "MGA / Specialty Underwriter" },
  { pattern: /\bryan\s*specialty\b/i, type: "MGA / Specialty Underwriter" },
  { pattern: /\bcrc\s*group\b/i, type: "MGA / Specialty Underwriter" },
  { pattern: /\bamwins\b/i, type: "MGA / Specialty Underwriter" },
  { pattern: /\bburns\s*&\s*wilcox\b/i, type: "MGA / Specialty Underwriter" },
  { pattern: /\bworldwide\s*facilities\b/i, type: "MGA / Specialty Underwriter" },
  // TPA / Claims Admin
  { pattern: /\bmclarens\b/i, type: "TPA / Claims Admin" },
  { pattern: /\bsedgwick\b/i, type: "TPA / Claims Admin" },
  { pattern: /\bcrawford\b.*\b(company|claims)\b/i, type: "TPA / Claims Admin" },
  { pattern: /\bbroadspire\b/i, type: "TPA / Claims Admin" },
  // Reinsurers
  { pattern: /\bmunich\s*re\b/i, type: "Reinsurer" },
  { pattern: /\bswiss\s*re\b/i, type: "Reinsurer" },
  { pattern: /\bgen\s*re\b/i, type: "Reinsurer" },
  { pattern: /\brennaissancere\b/i, type: "Reinsurer" },
  { pattern: /\bpartnerre\b/i, type: "Reinsurer" },
  { pattern: /\btransre\b/i, type: "Reinsurer" },
  { pattern: /\bhannover\s*re\b/i, type: "Reinsurer" },
];

function classifyKnownCompany(companyName) {
  if (!companyName) return null;
  for (const { pattern, type } of KNOWN_COMPANIES) {
    if (pattern.test(companyName)) return type;
  }
  return null;
}

// ─── ICP Classification ──────────────────────────────────────────────────────
const COMPANY_TYPES = [
  "Insurance Carrier", "MGA / Specialty Underwriter", "Insurance Broker",
  "TPA / Claims Admin", "Risk Consulting / Advisory", "InsurTech / Technology",
  "Reinsurer", "Captive / Risk Finance", "Corporate / End-User", "Academic / Association",
];

function detectSeniority(title) {
  const t = (title || "").toLowerCase();
  // Check VP/AVP BEFORE president to avoid "Vice President" matching C-Suite
  if (/\b(evp|svp)\b/.test(t) || /executive vice president/i.test(t) || /senior vice president/i.test(t)) return "EVP / SVP";
  if (/vice president/i.test(t) || /\b(vp|avp)\b/.test(t) || /\bassistant vice president\b/.test(t)) return "VP / AVP";
  // Now safe to check president (won't match "vice president" since that already returned)
  if (/\b(ceo|cfo|cto|cio|coo|cro|ciso|cmo|cco)\b/.test(t) || /\bchief\s/.test(t) || /\bpresident\b/.test(t) || /\bfounder\b/.test(t)) return "C-Suite / Founder";
  if (/\b(director|head of|managing director|partner|principal|practice leader)\b/.test(t)) return "Director / Head";
  if (/\b(manager|counsel|attorney|treasurer|controller|supervisor|risk manager|general manager)\b/.test(t)) return "Manager / Counsel";
  if (/\b(analyst|specialist|coordinator|associate|adjuster|underwriter|account executive|producer|professor|instructor|student|lecturer|editor)\b/.test(t)) return "Analyst / Specialist";
  return "Other";
}

const ICP_PRIORITY_TYPES = new Set(["Insurance Carrier", "MGA / Specialty Underwriter", "Reinsurer"]);
const ICP_WARM_TYPES = new Set(["Insurance Broker", "TPA / Claims Admin", "InsurTech / Technology", "Risk Consulting / Advisory"]);
const ICP_SENIOR = new Set(["C-Suite / Founder", "EVP / SVP", "VP / AVP", "Director / Head"]);
const ICP_FOR_PRIORITY = new Set(["Insurance Carrier", "MGA / Specialty Underwriter", "Insurance Broker", "Reinsurer"]);

// Map HubSpot `category` property (set by hubspot-cleanup) → Conference Buddy Company Type.
const HS_CATEGORY_TO_TYPE = {
  "Carrier": "Insurance Carrier",
  "MGA/MGU": "MGA / Specialty Underwriter",
  "Brokerage": "Insurance Broker",
  "Reinsurer": "Reinsurer",
  "Insurtech": "InsurTech / Technology",
};
// Reverse map — for writing Conference Buddy classifications back to HubSpot `category`.
const TYPE_TO_HS_CATEGORY = Object.fromEntries(
  Object.entries(HS_CATEGORY_TO_TYPE).map(([hsCat, cbType]) => [cbType, hsCat])
);
// Case-insensitive lookup helper for HubSpot `category` values
function hsCategoryToType(hsCategory) {
  const v = (hsCategory || "").trim().toLowerCase();
  if (!v) return "";
  for (const [src, dst] of Object.entries(HS_CATEGORY_TO_TYPE)) {
    if (src.toLowerCase() === v) return dst;
  }
  return "";
}

// Push Conference Buddy classifications back to HubSpot as `category`.
// updates: { hsId: hubspotCategory, ... }. Returns { ok, fail }.
async function writebackHubspotCategories(updates, hsKey) {
  const ids = Object.keys(updates);
  if (!ids.length || !hsKey) return { ok: 0, fail: 0 };
  let ok = 0, fail = 0;
  for (let b = 0; b < ids.length; b += 100) {
    const chunk = ids.slice(b, b + 100);
    const payload = {
      inputs: chunk.map(id => ({ id, properties: { category: updates[id] } })),
    };
    try {
      const res = await fetch("/api/hubspot/crm/v3/objects/companies/batch/update", {
        method: "POST",
        headers: { Authorization: `Bearer ${hsKey}`, "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) ok += chunk.length;
      else fail += chunk.length;
    } catch {
      fail += chunk.length;
    }
  }
  return { ok, fail };
}

function computeICP(companyType, seniority, title, hsTier = "") {
  if ((hsTier || "").toLowerCase() === "disqualified") {
    return { icpFit: "None", priority: "4 - Do Not Contact", reason: "HubSpot: Disqualified company", angle: "" };
  }
  const out = _computeICPRaw(companyType, seniority, title);
  if (!ICP_FOR_PRIORITY.has(companyType) && (out.priority === "1 - Priority" || out.priority === "2 - Warm")) {
    if (out.icpFit === "High" || out.icpFit === "Medium") out.icpFit = "Low";
    out.reason = `${out.reason} (capped: non-ICP Company Type)`;
    out.priority = "3 - Okay";
  }
  return out;
}

function _computeICPRaw(companyType, seniority, title) {
  const t = (title || "").toLowerCase();

  // Tier 1 — Priority: right person at right company
  if (ICP_PRIORITY_TYPES.has(companyType) && ICP_SENIOR.has(seniority)) {
    let angle = "Show end-to-end insurance ops AI platform — 30x faster submissions";
    if (/claim/i.test(t)) angle = "Show claims FNOL & document intake automation";
    else if (/underwrit/i.test(t)) angle = "Show submission automation & underwriting triage use case";
    else if (/distribut|marketing|growth|brand/i.test(t)) angle = "Discuss how FurtherAI accelerates bind cycle & distribution ops";
    else if (/risk|compliance|audit|erm/i.test(t)) angle = "Frame around underwriting audit & compliance automation";
    else if (/digital|innovat|ai\b|tech/i.test(t)) angle = "Lead with operational efficiency & AI adoption ROI (646% case study)";
    return { icpFit: "High", priority: "1 - Priority", reason: `${companyType} — senior decision maker`, angle };
  }

  // Tier 2 — Warm: mid person at right company, OR right person at good company
  if (ICP_PRIORITY_TYPES.has(companyType) && seniority === "Manager / Counsel") {
    let angle = "Frame around underwriting audit & compliance automation";
    if (/underwrit/i.test(t)) angle = "Show submission automation & underwriting triage use case";
    else if (/claim/i.test(t)) angle = "Show claims FNOL & document intake automation";
    return { icpFit: "Medium", priority: "2 - Warm", reason: `${companyType} — mid-level (${seniority})`, angle };
  }
  if (ICP_WARM_TYPES.has(companyType) && ICP_SENIOR.has(seniority)) {
    const map = {
      "Insurance Broker":        { angle: "Show how FurtherAI speeds up submission placement & quote readiness", reason: `Broker — ${seniority}` },
      "TPA / Claims Admin":      { angle: "Focus on claims document structuring & FNOL automation", reason: "TPA — document/claims processing overlap" },
      "InsurTech / Technology":   { angle: "Explore integration or co-sell with existing platforms", reason: "InsurTech — partner/channel potential" },
      "Risk Consulting / Advisory": { angle: "Explore referral/reseller partnership for carrier clients", reason: "Consultant — influencer/partner potential" },
    };
    const m = map[companyType] || { angle: "", reason: companyType };
    return { icpFit: "Medium", priority: "2 - Warm", ...m };
  }

  // Tier 3 — Okay: wrong person at right company, mid person at good company, right person at other company
  if (ICP_PRIORITY_TYPES.has(companyType)) {
    let angle = "Frame around underwriting audit & compliance automation";
    if (/underwrit/i.test(t)) angle = "Show submission automation & underwriting triage use case";
    else if (/claim/i.test(t)) angle = "Show claims FNOL & document intake automation";
    return { icpFit: "Low", priority: "3 - Okay", reason: `${companyType} — ${seniority || "low seniority"}`, angle };
  }
  if (ICP_WARM_TYPES.has(companyType) && seniority === "Manager / Counsel") {
    const angleMap = {
      "Insurance Broker": "Show how FurtherAI speeds up submission placement & quote readiness",
      "TPA / Claims Admin": "Focus on claims document structuring & FNOL automation",
    };
    return { icpFit: "Low", priority: "3 - Okay", reason: `${companyType} — ${seniority}`, angle: angleMap[companyType] || "" };
  }
  if (!ICP_PRIORITY_TYPES.has(companyType) && !ICP_WARM_TYPES.has(companyType) && ICP_SENIOR.has(seniority)) {
    let angle = "Introduce FurtherAI's insurance ops AI platform — explore relevance to their business";
    if (/risk|insurance|compliance|audit/i.test(t)) angle = "Frame around underwriting audit & compliance automation — show how FurtherAI handles insurance document workflows";
    else if (/claim/i.test(t)) angle = "Show claims FNOL & document intake automation";
    else if (/digital|innovat|ai\b|tech/i.test(t)) angle = "Lead with operational efficiency & AI adoption ROI (646% case study)";
    else if (companyType === "Corporate / End-User") angle = "Discuss how FurtherAI automates insurance document processing — relevant if they manage risk programs or buy coverage";
    else if (companyType === "Academic / Association") angle = "Explore partnership or speaking opportunities — FurtherAI's AI-driven insurance ops resonates with industry education";
    else if (companyType === "Captive / Risk Finance") angle = "Show how FurtherAI streamlines policy and claims document workflows for captive programs";
    return { icpFit: "Low", priority: "3 - Okay", reason: `${companyType || "Unknown"} — senior but outside core ICP`, angle };
  }

  // Tier 4 — Do Not Contact: low fit on both dimensions
  let reason = `${companyType || "Unknown"} — outside ICP`;
  if (companyType === "Academic / Association") reason = "Academic / Association — no fit";
  else if (companyType === "Captive / Risk Finance") reason = "Captive / Risk Finance — no fit";
  else if (ICP_WARM_TYPES.has(companyType)) reason = `${companyType} — low seniority`;
  return { icpFit: "None", priority: "4 - Do Not Contact", reason, angle: "" };
}

// ─── Logo SVG ─────────────────────────────────────────────────────────────────
function FurtherLogo() {
  return (
    <svg width="28" height="28" viewBox="0 0 28 28" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect width="28" height="28" rx="6" fill={THEME.accent} />
      <path d="M7 8h14v3H10v3h9v3H10v5H7V8z" fill="#0B1F14" />
    </svg>
  );
}

// ─── Step bar ─────────────────────────────────────────────────────────────────
const STEPS = ["Upload", "Map columns", "Enriching", "Download"];

function StepBar({ current }) {
  return (
    <div style={{ display: "flex", alignItems: "center", marginBottom: 40, gap: 0 }}>
      {STEPS.map((label, i) => (
        <div key={label} style={{ display: "flex", alignItems: "center", flex: i < STEPS.length - 1 ? 1 : "none" }}>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
            <div style={{
              width: 30, height: 30, borderRadius: "50%",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 11, fontWeight: 700, letterSpacing: "0.02em",
              background: i < current ? THEME.accent : i === current ? "transparent" : "transparent",
              border: i < current ? `2px solid ${THEME.accent}` : i === current ? `2px solid ${THEME.accent}` : `2px solid ${THEME.border}`,
              color: i < current ? THEME.bg : i === current ? THEME.accent : THEME.muted,
              transition: "all 0.25s",
            }}>
              {i < current ? "✓" : i + 1}
            </div>
            <span style={{
              fontSize: 10, fontWeight: i === current ? 700 : 400,
              color: i === current ? THEME.accent : i < current ? THEME.text : THEME.muted,
              whiteSpace: "nowrap", letterSpacing: "0.05em", textTransform: "uppercase",
            }}>{label}</span>
          </div>
          {i < STEPS.length - 1 && (
            <div style={{
              flex: 1, height: 1,
              background: i < current ? THEME.accent : THEME.border,
              margin: "0 6px", marginBottom: 22, transition: "background 0.3s",
            }} />
          )}
        </div>
      ))}
    </div>
  );
}

// ─── Badge ────────────────────────────────────────────────────────────────────
function Badge({ ownerName, unassignedBdr }) {
  const label = unassignedBdr ? `Unassigned — ${unassignedBdr}` : (ownerName || "Unassigned");
  const c = unassignedBdr ? ownerColor(unassignedBdr) : (ownerName ? ownerColor(ownerName) : UNASSIGNED_COLOR);
  return (
    <span style={{
      background: c.bg, color: c.text, border: `1px solid ${c.border}`,
      fontSize: 11, fontWeight: 600, padding: "2px 9px", borderRadius: 4,
      whiteSpace: "nowrap", letterSpacing: "0.02em",
    }}>{label}</span>
  );
}

// ─── Input / Select ───────────────────────────────────────────────────────────
const inputStyle = {
  width: "100%", padding: "9px 12px", borderRadius: 8,
  border: `1px solid ${THEME.border}`, background: THEME.bg,
  fontSize: 13, color: THEME.text, outline: "none", boxSizing: "border-box",
  fontFamily: "inherit",
};

// ─── Checkpoint helpers ──────────────────────────────────────────────────────
const STORAGE_KEY = "conference_enricher_progress";

function saveCheckpoint(data) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      timestamp: Date.now(),
      headers: data.headers,
      colMap: data.colMap,
      rawRows: data.rawRows,
      results: data.results,
      processedIndex: data.processedIndex,
      hsMap: data.hsMap,
      hsIdMap: data.hsIdMap,
    }));
  } catch (_) {} // localStorage might be full
}

function loadCheckpoint() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const data = JSON.parse(raw);
    // Expire after 4 hours
    if (Date.now() - data.timestamp > 4 * 60 * 60 * 1000) {
      localStorage.removeItem(STORAGE_KEY);
      return null;
    }
    return data;
  } catch (_) { return null; }
}

function clearCheckpoint() {
  localStorage.removeItem(STORAGE_KEY);
}

// ─── Main ─────────────────────────────────────────────────────────────────────
export default function App() {
  const [step, setStep] = useState(0);
  const [rawRows, setRawRows] = useState([]);
  const [headers, setHeaders] = useState([]);
  const [colMap, setColMap] = useState({ company: "", name: "", title: "", email: "" });
  const [hsKey, setHsKey] = useState(import.meta.env.VITE_HUBSPOT_TOKEN || "");
  const [apKey, setApKey] = useState(import.meta.env.VITE_APOLLO_API_KEY || "");
  const [enriched, setEnriched] = useState([]);
  const [prog, setProg] = useState({ done: 0, total: 0, msg: "" });
  const [stats, setStats] = useState(null);
  const [errs, setErrs] = useState([]);
  const [dragOver, setDragOver] = useState(false);
  const [autoDownload, setAutoDownload] = useState(false);
  const [checkpoint, setCheckpoint] = useState(null);
  const [filteredCount, setFilteredCount] = useState(0);
  const [sourceFileName, setSourceFileName] = useState("");
  const fileRef = useRef();

  // Check for saved progress on mount
  useEffect(() => {
    const cp = loadCheckpoint();
    if (cp) setCheckpoint(cp);
  }, []);

  // ── File upload ──────────────────────────────────────────────────────────────
  const handleFile = useCallback((file) => {
    setSourceFileName(file.name || "");
    const reader = new FileReader();
    reader.onload = (e) => {
      const wb = XLSX.read(e.target.result, { type: "array" });
      const ws = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "" });
      if (rows.length < 2) return;
      const hdrs = rows[0].map(String);
      setHeaders(hdrs);
      // Auto-detect company column first for filtering
      const auto = { company: "", name: "", title: "", email: "" };
      hdrs.forEach(h => {
        const l = h.toLowerCase();
        if (!auto.company && /company|org|account.?name|employer/.test(l)) auto.company = h;
        if (!auto.name && /^name$|^full.?name$|^contact.?name$|^attendee.?name$/.test(l)) auto.name = h;
        if (!auto.title && /title|job|role|position/.test(l)) auto.title = h;
        if (!auto.email && /email|e-mail/.test(l)) auto.email = h;
      });
      const compIdx = auto.company ? hdrs.indexOf(auto.company) : -1;
      const allParsed = rows.slice(1).map(r => { const o = {}; hdrs.forEach((h, i) => { o[h] = String(r[i] ?? ""); }); return o; });

      // Fix rows with shifted columns (e.g., Salutation|FirstName|LastName|Title|Company...
      // mapped as Name|Company|JobTitle|Email|Phone...)
      const SALUTATIONS = /^(mr\.?|mrs\.?|ms\.?|miss|dr\.?|prof\.?)$/i;
      let fixedCount = 0;
      if (auto.name && auto.company && auto.title) {
        allParsed.forEach(r => {
          const nameVal = (r[auto.name] || "").trim();
          const compVal = (r[auto.company] || "").trim();
          const titleVal = (r[auto.title] || "").trim();
          // Detect: Name is a salutation or single first-name that matches Company
          const isSalutation = SALUTATIONS.test(nameVal);
          const isFirstNameShift = nameVal && compVal && nameVal === compVal && titleVal && !titleVal.includes(" ") && titleVal.length < 30;
          if (isSalutation || isFirstNameShift) {
            // Columns are: Salutation, FirstName, LastName, Title, Company, ...
            // Mapped as: Name, Company, JobTitle, Email, Phone, ...
            const firstName = isSalutation ? compVal : nameVal;
            const lastName = titleVal;
            const realTitle = auto.email ? (r[auto.email] || "") : "";
            // Find the real company — it's in the Phone column position (5th col)
            const phoneCol = hdrs[4] || "";
            const realCompany = phoneCol ? (r[phoneCol] || "") : "";
            // Reconstruct
            r[auto.name] = `${firstName} ${lastName}`.trim();
            r[auto.company] = realCompany || compVal;
            r[auto.title] = realTitle;
            if (auto.email) r[auto.email] = ""; // was actually the title, clear it
            fixedCount++;
          }
        });
      }

      // Filter out rows with hallucinated AI text in the company column
      const clean = compIdx >= 0
        ? allParsed.filter(r => !isHallucinatedCompany(r[auto.company]))
        : allParsed;
      setFilteredCount(allParsed.length - clean.length);
      if (fixedCount > 0) console.log(`Fixed ${fixedCount} rows with shifted columns`);
      setRawRows(clean);
      setColMap(auto);
      setStep(1);
    };
    reader.readAsArrayBuffer(file);
  }, []);

  const onDrop = useCallback((e) => {
    e.preventDefault();
    setDragOver(false);
    const f = e.dataTransfer?.files?.[0] || e.target?.files?.[0];
    if (f) handleFile(f);
  }, [handleFile]);

  // ── Enrichment pipeline ──────────────────────────────────────────────────────
  async function runEnrichment() {
    const errors = [];
    const hsMap = {};
    const hsIdMap = {};
    const hsDomainMap = {};

    const enrichStartTime = Date.now();

    if (hsKey) {
      try {
        setProg({ done: 0, total: rawRows.length, msg: "Fetching HubSpot owners...", startTime: enrichStartTime });
        const ownerRes = await fetch("/api/hubspot/crm/v3/owners?limit=100", {
          headers: { Authorization: `Bearer ${hsKey}` }
        });
        const ownerMap = {};
        if (ownerRes.ok) {
          const od = await ownerRes.json();
          (od.results || []).forEach(o => { ownerMap[o.id] = `${o.firstName || ""} ${o.lastName || ""}`.trim(); });
        }

        setProg(p => ({ ...p, msg: "Fetching HubSpot companies..." }));
        let after = null, fetched = 0;
        while (true) {
          const url = `/api/hubspot/crm/v3/objects/companies?limit=100&properties=name,domain,hubspot_owner_id,parent_company_id,lifecyclestage,hs_lead_status,icp_tier,category${after ? `&after=${after}` : ""}`;
          const res = await fetch(url, { headers: { Authorization: `Bearer ${hsKey}` } });
          if (!res.ok) { errors.push(`HubSpot ${res.status}: ${res.statusText}`); break; }
          const d = await res.json();
          (d.results || []).forEach(c => {
            const norm = normalize(c.properties?.name);
            const entry = {
              ownerName: ownerMap[c.properties?.hubspot_owner_id] || null,
              id: c.id,
              parentId: c.properties?.parent_company_id || null,
              lifecycle: c.properties?.lifecyclestage || "",
              leadStatus: c.properties?.hs_lead_status || "",
              icpTier: (c.properties?.icp_tier || "").toLowerCase(),
              category: (c.properties?.category || "").trim(),
              domain: normalizeDomain(c.properties?.domain || ""),
            };
            if (norm) hsMap[norm] = entry;
            hsIdMap[c.id] = norm;
            if (entry.domain) hsDomainMap[entry.domain] = entry;
          });
          fetched += (d.results || []).length;
          if (d.paging?.next?.after) after = d.paging.next.after; else break;
          if (fetched > 20000) break;
        }
      } catch (err) { errors.push(`HubSpot: ${err.message}`); }
    }

    function lookup(name, domain) {
      const norm = normalize(name);
      // 1. Exact normalized-name match (most specific — preserves subsidiary granularity)
      if (norm && hsMap[norm]) return hsMap[norm];
      // 2. Domain match (deterministic — good fallback when name varies)
      if (domain) {
        const nd = normalizeDomain(domain);
        if (nd && hsDomainMap[nd]) return hsDomainMap[nd];
      }
      if (!norm) return null;
      // 3. Whole-token prefix match (rejects generic substring overlaps)
      for (const [k, v] of Object.entries(hsMap)) {
        if (tokenPrefixMatch(norm, k)) return v;
      }
      return null;
    }

    function resolveEntry(name, domain) {
      const entry = lookup(name, domain);
      if (!entry) return null;
      // Fill missing ownerName / icpTier / category from parent chain
      const filled = { ...entry };
      let cursor = entry;
      let hops = 0;
      while (hops < 3 && cursor && cursor.parentId &&
             (!filled.ownerName || !filled.icpTier || !filled.category)) {
        const pNorm = hsIdMap[cursor.parentId];
        const parent = pNorm ? hsMap[pNorm] : null;
        if (!parent) break;
        if (!filled.ownerName && parent.ownerName) filled.ownerName = parent.ownerName;
        if (!filled.icpTier && parent.icpTier) filled.icpTier = parent.icpTier;
        if (!filled.category && parent.category) filled.category = parent.category;
        cursor = parent;
        hops++;
      }
      return filled;
    }

    function formatRelationship(entry) {
      if (!entry) return "";
      const lc = (entry.lifecycle || "").toLowerCase();
      const ls = (entry.leadStatus || "").toLowerCase();
      if (lc === "customer") return "Customer";
      if (lc === "opportunity") return "Opportunity";
      if (ls === "in_progress" || ls === "in progress") return "In Progress";
      if (ls === "open") return "Open Lead";
      if (lc === "lead") return "Lead";
      if (lc === "subscriber") return "Subscriber";
      if (lc === "marketingqualifiedlead") return "MQL";
      if (lc === "salesqualifiedlead") return "SQL";
      if (lc === "evangelist") return "Evangelist";
      if (lc || ls) return (lc || ls).replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
      return entry.ownerName ? "Known" : "";
    }

    const total = rawRows.length;
    const results = new Array(total);
    const bdrNames = PODS.map(p => p.bdr);
    let unassignedIdx = 0;
    const fnCol = headers.find(h => /^first.?name$/i.test(h));
    const lnCol = headers.find(h => /^last.?name$/i.test(h));

    // Domain cache: company name → domain (learned from successful Apollo matches)
    const domainCache = {};

    // After finding a person via search, enrich them to get full contact info
    async function enrichPerson(person) {
      if (!person?.id) return person;
      try {
        await new Promise(r => setTimeout(r, 300));
        const res = await fetch("/api/apollo/v1/people/match", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
          body: JSON.stringify({ id: person.id, reveal_personal_emails: true }),
        });
        if (res.ok) {
          const d = await res.json();
          if (d.person) return d.person;
        }
        return person;
      } catch {
        return person;
      }
    }

    function extractPerson(p) {
      return {
        _email: p.email || "",
        _phone: p.phone_numbers?.[0]?.sanitized_number || p.organization?.phone || "",
        _linkedin: p.linkedin_url || "",
        _apollo_title: p.title || "",
        _apollo_company: p.organization?.name || "",
      };
    }

    function getNameParts(row) {
      let firstName = "", lastName = "";
      if (fnCol && lnCol) {
        firstName = row[fnCol] || "";
        lastName = row[lnCol] || "";
      } else if (colMap.name && row[colMap.name]) {
        const parts = row[colMap.name].trim().split(" ");
        firstName = parts[0] || "";
        lastName = parts.slice(1).join(" ") || "";
      }
      return { firstName, lastName };
    }

    // ── Process a single row (Apollo enrichment) ──
    async function processRow(i) {
      const row = rawRows[i];
      const companyRaw = row[colMap.company] || "";
      const emailCandidate = colMap.email ? row[colMap.email] : "";
      const emailRaw = emailCandidate.includes("@") ? emailCandidate : "";

      const compNorm = normalize(companyRaw);
      let hsEntry = hsKey && companyRaw ? resolveEntry(companyRaw, domainCache[compNorm]) : null;
      let ownerName = hsEntry?.ownerName || null;
      let apolloData = {};
      const { firstName, lastName } = getNameParts(row);

      const titleRaw = (colMap.title ? row[colMap.title] : "") || "";

      if (apKey) {
        try {
          let person = null;

          // ── Attempt 1: People Match (needs name or email) ──
          if (firstName || emailRaw) {
            const matchBody = { reveal_personal_emails: true };
            if (emailRaw) matchBody.email = emailRaw;
            if (companyRaw) matchBody.organization_name = companyRaw;
            if (domainCache[compNorm]) matchBody.domain = domainCache[compNorm];
            if (firstName) matchBody.first_name = firstName;
            if (lastName) matchBody.last_name = lastName;

            let res = await fetch("/api/apollo/v1/people/match", {
              method: "POST",
              headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
              body: JSON.stringify(matchBody),
            });
            if (res.ok) {
              const d = await res.json();
              person = d.person;
            }

            // ── Attempt 2: People Search by name + company (with delay) ──
            if (!person && firstName && companyRaw) {
              await new Promise(r => setTimeout(r, 300));
              const searchBody = {
                q_keywords: `${firstName} ${lastName}`.trim(),
                person_titles: [],
                q_organization_name: companyRaw,
                page: 1, per_page: 1,
              };
              if (domainCache[compNorm]) searchBody.organization_domains = [domainCache[compNorm]];
              res = await fetch("/api/apollo/api/v1/mixed_people/api_search", {
                method: "POST",
                headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
                body: JSON.stringify(searchBody),
              });
              if (res.ok) {
                const d = await res.json();
                const candidates = d.people || [];
                const fn = firstName.toLowerCase(), ln = lastName.toLowerCase();
                person = candidates.find(c =>
                  (c.first_name || "").toLowerCase() === fn &&
                  (c.last_name || "").toLowerCase() === ln
                ) || candidates[0] || null;
              }
            }
          }

          // ── Attempt 3: Title + Company search (no name needed) ──
          // ── Attempt 3: Title + Company search (with delay) ──
          if (!person && titleRaw && companyRaw) {
            await new Promise(r => setTimeout(r, 300));
            const searchBody = {
              person_titles: [titleRaw],
              q_organization_name: companyRaw,
              page: 1, per_page: 3,
            };
            if (domainCache[compNorm]) searchBody.organization_domains = [domainCache[compNorm]];
            const res = await fetch("/api/apollo/api/v1/mixed_people/api_search", {
              method: "POST",
              headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
              body: JSON.stringify(searchBody),
            });
            if (res.ok) {
              const d = await res.json();
              const candidates = d.people || [];
              // Pick the best match — prefer exact title match
              const tLower = titleRaw.toLowerCase();
              person = candidates.find(c => (c.title || "").toLowerCase() === tLower) || candidates[0] || null;
              // Store discovered name back into the row
              if (person) {
                row._discoveredName = `${person.first_name || ""} ${person.last_name || ""}`.trim();
              }
            }
          }

          if (person) {
            // Enrich the person to get full contact info (email, phone, last name)
            person = await enrichPerson(person);
            row._discoveredName = `${person.first_name || ""} ${person.last_name || ""}`.trim();
            apolloData = extractPerson(person);
            // Cache the domain for this company
            if (person.organization?.primary_domain && compNorm) {
              domainCache[compNorm] = person.organization.primary_domain;
            }
            if (hsKey) {
              // Only refine if the initial match wasn't an exact name hit (exact name is most specific).
              const rawNorm = normalize(companyRaw);
              const hadExact = !!(rawNorm && hsMap[rawNorm]);
              if (!hadExact) {
                const apolloDomain = domainCache[compNorm] || "";
                const apolloCompany = apolloData._apollo_company || companyRaw;
                if (apolloDomain || apolloCompany) {
                  const refined = resolveEntry(apolloCompany, apolloDomain);
                  if (refined && (!hsEntry || refined.id !== hsEntry.id)) {
                    hsEntry = refined;
                    ownerName = refined.ownerName || null;
                  }
                }
              }
            }
          }
        } catch (_) {}
      }

      return { row, apolloData, ownerName, hsEntry };
    }

    // ── Run Apollo sequentially to avoid rate limits ──
    const CONCURRENCY = 1;
    let done = 0;
    for (let batch = 0; batch < total; batch += CONCURRENCY) {
      const end = Math.min(batch + CONCURRENCY, total);
      const promises = [];
      for (let i = batch; i < end; i++) {
        promises.push(processRow(i).then(r => ({ idx: i, ...r })));
      }
      const batchResults = await Promise.all(promises);
      // Rate limit: wait 500ms between rows to avoid Apollo 429s
      await new Promise(r => setTimeout(r, 500));

      for (const { idx, row, apolloData, ownerName, hsEntry } of batchResults) {
        const podId = ownerName ? podForOwner(ownerName) : null;
        const unassignedBdr = !ownerName ? bdrNames[unassignedIdx++ % bdrNames.length] : null;
        let who = "";
        if (podId) {
          const pod = PODS.find(p => p.id === podId);
          who = pod ? pod.bdr : ownerName;
        } else if (ownerName) {
          who = ownerName;
        } else if (unassignedBdr) {
          who = unassignedBdr;
        }
        results[idx] = {
          ...row, ...apolloData,
          _ownerName: ownerName || "",
          _podId: podId || "",
          _unassignedBdr: unassignedBdr || "",
          _who: who,
          _relationship: formatRelationship(hsEntry),
          _hsIcpTier: hsEntry?.icpTier || "",
          _hsCategory: hsEntry?.category || "",
          _hsId: hsEntry?.id || "",
        };
      }

      done = end;
      setProg({ done, total, msg: `Processing ${done.toLocaleString()} of ${total.toLocaleString()}...`, startTime: enrichStartTime });

      // Checkpoint every 25 rows
      if (done % 25 === 0 || done === total) {
        saveCheckpoint({
          headers, colMap, rawRows, results: results.filter(Boolean),
          processedIndex: done,
          hsMap, hsIdMap, hsDomainMap,
        });
      }
    }

    // ── Claude Person Research (for rows with no name — title+company only lists) ──
    const claudeKey = import.meta.env.VITE_ANTHROPIC_API_KEY;
    const noNameRows = results.filter(r => !r._discoveredName && !getNameParts(r).firstName && (colMap.title ? r[colMap.title] : r._apollo_title));
    if (claudeKey && noNameRows.length > 0) {
      setProg({ done: 0, total: noNameRows.length, msg: `Researching ${noNameRows.length} unidentified people...`, startTime: enrichStartTime });
      const RBATCH = 20;
      let researched = 0;
      for (let b = 0; b < noNameRows.length; b += RBATCH) {
        const batch = noNameRows.slice(b, b + RBATCH);
        const lines = batch.map((r, i) =>
          `${i + 1}. "${colMap.title ? r[colMap.title] : (r._apollo_title || "")}" at "${r[colMap.company] || r._apollo_company || ""}"`
        ).join("\n");

        try {
          const res = await fetch("/api/anthropic/v1/messages", {
            method: "POST",
            headers: { "x-api-key": claudeKey, "anthropic-version": "2023-06-01", "content-type": "application/json" },
            body: JSON.stringify({
              model: "claude-haiku-4-5-20251001",
              max_tokens: 4096,
              messages: [{ role: "user", content: `For each numbered entry, identify the most likely person who holds this role at this company as of 2025. Return ONLY their first name and last name. If you cannot identify the person, return "Unknown Unknown".\n\nFormat:\n1. FirstName LastName\n\n${lines}` }],
            }),
          });
          if (res.ok) {
            const d = await res.json();
            const text = d.content?.[0]?.text || "";
            const names = [];
            text.split("\n").forEach(line => {
              const m = line.match(/^(\d+)\.\s*(.+)/);
              if (m) {
                const idx = parseInt(m[1]) - 1;
                const name = m[2].trim();
                if (idx >= 0 && idx < batch.length && !name.toLowerCase().includes("unknown")) {
                  const parts = name.split(/\s+/);
                  names[idx] = { first: parts[0], last: parts.slice(1).join(" ") };
                }
              }
            });

            // Enrich found names through Apollo
            for (let i = 0; i < batch.length; i++) {
              if (names[i]) {
                batch[i]._discoveredName = `${names[i].first} ${names[i].last}`;
                await new Promise(r => setTimeout(r, 500));
                let md = null;
                try {
                  const matchRes = await fetch("/api/apollo/v1/people/match", {
                    method: "POST",
                    headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
                    body: JSON.stringify({ first_name: names[i].first, last_name: names[i].last, organization_name: batch[i][colMap.company] || "", reveal_personal_emails: true }),
                  });
                  if (matchRes.ok) md = await matchRes.json();
                } catch {
                  md = null;
                }
                if (md?.person) {
                  batch[i]._discoveredName = `${md.person.first_name || names[i].first} ${md.person.last_name || names[i].last}`;
                  if (md.person.email) batch[i]._email = md.person.email;
                  if (md.person.phone_numbers?.[0]?.sanitized_number) batch[i]._phone = md.person.phone_numbers[0].sanitized_number;
                  if (md.person.linkedin_url) batch[i]._linkedin = md.person.linkedin_url;
                }
                researched++;
              }
            }
          } else { break; }
        } catch { break; }
        setProg({ done: Math.min(b + RBATCH, noNameRows.length), total: noNameRows.length, msg: `Researched ${researched} people...`, startTime: enrichStartTime });
      }
    }

    // ── ICP Classification (Company Type via Claude, then Seniority + ICP locally) ──
    const compTypeCol = headers.find(h => /company.?type/i.test(h));
    // Also check for "Membership" column (e.g. TMPAA lists use this for company type)
    const membershipCol = headers.find(h => /^membership$/i.test(h));
    if (membershipCol && !compTypeCol) {
      const MEMBERSHIP_MAP = {
        "carrier": "Insurance Carrier",
        "program administrator": "MGA / Specialty Underwriter",
        "reinsurer": "Reinsurer",
        "broker": "Insurance Broker",
        "tpa": "TPA / Claims Admin",
        "insurtech": "InsurTech / Technology",
        "other": "",
      };
      results.forEach(r => {
        const mem = (r[membershipCol] || "").toLowerCase().trim();
        const mapped = MEMBERSHIP_MAP[mem];
        if (mapped) r._companyType = mapped;
      });
    }
    const needsCompType = !compTypeCol || results.every(r => !r[compTypeCol]);

    // HubSpot category override (from cleanup-authored `category` property) — runs regardless of Claude availability
    results.forEach(r => {
      if (!r._companyType && r._hsCategory) {
        const mapped = hsCategoryToType(r._hsCategory);
        if (mapped) r._companyType = mapped;
      }
    });

    if (claudeKey && needsCompType) {
      // Pre-classify known companies before hitting the API
      results.forEach(r => {
        const known = classifyKnownCompany(r[colMap.company] || "");
        if (known) r._companyType = known;
      });

      setProg({ done: 0, total, msg: "Classifying companies (ICP)...", startTime: enrichStartTime });
      const BATCH = 30;
      for (let b = 0; b < results.length; b += BATCH) {
        const batch = results.slice(b, b + BATCH);
        // Skip rows already classified by known-company lookup
        const needsClassification = batch.filter(r => !r._companyType);
        if (needsClassification.length === 0) {
          setProg({ done: b + batch.length, total, msg: `Classifying ${Math.min(b + batch.length, total)} of ${total}...`, startTime: enrichStartTime });
          continue;
        }
        const lines = needsClassification.map((r, i) =>
          `${i + 1}. Company: "${r[colMap.company] || ""}" | Title: "${colMap.title ? r[colMap.title] : (r._apollo_title || "")}"`
        ).join("\n");

        try {
          const res = await fetch("/api/anthropic/v1/messages", {
            method: "POST",
            headers: {
              "x-api-key": claudeKey,
              "anthropic-version": "2023-06-01",
              "content-type": "application/json",
            },
            body: JSON.stringify({
              model: "claude-haiku-4-5-20251001",
              max_tokens: 4096,
              messages: [{
                role: "user",
                content: `You are classifying companies for an insurance industry CRM. For each numbered entry, classify the Company Type as EXACTLY one of these values:\n- Insurance Carrier\n- MGA / Specialty Underwriter\n- Insurance Broker\n- TPA / Claims Admin\n- Risk Consulting / Advisory\n- InsurTech / Technology\n- Reinsurer\n- Captive / Risk Finance\n- Corporate / End-User\n- Academic / Association\n\nRules:\n- Insurance companies that write policies = Insurance Carrier\n- Managing General Agents, wholesalers, program administrators that underwrite = MGA / Specialty Underwriter\n- Brokerages that place risk = Insurance Broker\n- Third-party administrators, claims servicers = TPA / Claims Admin\n- Law firms, consultancies, risk advisors = Risk Consulting / Advisory\n- Software/tech vendors serving insurance = InsurTech / Technology\n- Reinsurance companies = Reinsurer\n- Non-insurance companies (manufacturers, retailers, hospitals, etc.) = Corporate / End-User\n- Universities, associations, foundations = Academic / Association\n\nReply with ONLY numbered lines:\n1. Company Type\n2. Company Type\n\nCompanies:\n${lines}`
              }],
            }),
          });
          if (res.ok) {
            const d = await res.json();
            const text = d.content?.[0]?.text || "";
            text.split("\n").forEach(line => {
              const m = line.match(/^(\d+)\.\s*(.+)/);
              if (m) {
                const idx = parseInt(m[1]) - 1;
                if (idx >= 0 && idx < needsClassification.length) {
                  const ct = m[2].trim();
                  needsClassification[idx]._companyType = COMPANY_TYPES.find(t => ct.toLowerCase().includes(t.toLowerCase())) || ct;
                }
              }
            });
          } else {
            const errBody = await res.text().catch(() => "");
            errors.push(`Claude ${res.status}: ${errBody.slice(0, 100)}`);
            break;
          }
        } catch (err) {
          errors.push(`Claude: ${err.message}`);
          break;
        }
        setProg({ done: b + batch.length, total, msg: `Classifying ${Math.min(b + batch.length, total)} of ${total}...`, startTime: enrichStartTime });
      }
    }

    // ── Writeback: fill missing HubSpot `category` for Tier 1/2 companies ──
    if (hsKey) {
      const writeback = {};
      for (const r of results) {
        const hsCat = (r._hsCategory || "").trim();
        const tier = (r._hsIcpTier || "").toLowerCase();
        const target = TYPE_TO_HS_CATEGORY[r._companyType];
        // Safe because TYPE_TO_HS_CATEGORY only maps the 5 insurance buckets — non-insurance
        // classifications can't produce a writeback target. Skip only Disqualified companies.
        if (r._hsId && !hsCat && tier !== "disqualified" && target) {
          writeback[r._hsId] = target;
        }
      }
      const count = Object.keys(writeback).length;
      if (count) {
        setProg(p => ({ ...p, msg: `HubSpot writeback: ${count} companies...` }));
        const { ok, fail } = await writebackHubspotCategories(writeback, hsKey);
        if (fail) errors.push(`HubSpot writeback: ${fail} failed`);
        // Reflect writes locally so subsequent resume runs see the category
        for (const [id, cat] of Object.entries(writeback)) {
          for (const r of results) if (r._hsId === id) r._hsCategory = cat;
        }
        console.log(`HubSpot writeback: ${ok} updated, ${fail} failed`);
      }
    }

    // ── Compute Seniority + ICP for every row ──
    setProg({ done: total, total, msg: "Computing ICP...", startTime: enrichStartTime });
    results.forEach(r => {
      const title = (colMap.title ? r[colMap.title] : "") || r._apollo_title || "";
      const ct = r._companyType || (compTypeCol ? r[compTypeCol] : "") || "Corporate / End-User";
      r._companyType = ct;
      r._seniority = detectSeniority(title);
      const icp = computeICP(ct, r._seniority, title, r._hsIcpTier || "");
      r._icpFit = icp.icpFit;
      r._outreachPriority = icp.priority;
      r._icpReason = icp.reason;
      r._outreachAngle = icp.angle;

      // Customers → always Do Not Contact (already have the relationship)
      if (r._relationship === "Customer") {
        r._outreachPriority = "4 - Do Not Contact";
        r._icpFit = "None";
        r._icpReason = "Existing customer — do not contact";
        r._outreachAngle = "";
      }
    });

    setProg({ done: total, total, msg: "Sorting...", startTime: enrichStartTime });

    // Sort: Assigned first (grouped by person: Zain, Dani, Jacob, then other owners alpha),
    // then unassigned (grouped by BDR: Zain, Dani, Jacob).
    // Within each person group, sort by outreach priority.
    const podOrder = PODS.map(p => p.id);
    const prioVal = (r) => r._outreachPriority === "1 - Priority" ? 0 : r._outreachPriority === "2 - Warm" ? 1 : r._outreachPriority === "3 - Okay" ? 2 : 3;
    const whoOrder = (r) => {
      // Pod BDRs get 0-2, other named owners get 10+, no-owner HubSpot gets 50, unassigned gets 100+
      if (r._unassignedBdr) return 100 + bdrNames.indexOf(r._unassignedBdr);
      if (r._podId) return podOrder.indexOf(r._podId);
      if (r._ownerName) return 10; // individual owners cluster together
      return 50; // known in HS but no owner
    };
    results.sort((a, b) => {
      // Primary: assigned vs unassigned
      const wa = whoOrder(a), wb = whoOrder(b);
      const assignedA = wa < 100, assignedB = wb < 100;
      if (assignedA !== assignedB) return assignedA ? -1 : 1;
      // Secondary: group by person/owner
      if (wa !== wb) return wa - wb;
      // Within same owner group, sort by individual owner name
      if (wa === 10) {
        const cmp = (a._ownerName || "").localeCompare(b._ownerName || "");
        if (cmp !== 0) return cmp;
      }
      // Tertiary: outreach priority
      const pa = prioVal(a), pb = prioVal(b);
      if (pa !== pb) return pa - pb;
      return 0;
    });

    const s = { pods: {}, owners: {}, unassigned: 0 };
    PODS.forEach(p => { s.pods[p.id] = 0; });
    results.forEach(r => {
      if (r._unassignedBdr) s.unassigned++;
      else if (r._podId) s.pods[r._podId]++;
      else if (r._ownerName) s.owners[r._ownerName] = (s.owners[r._ownerName] || 0) + 1;
    });

    // Final checkpoint after sort
    saveCheckpoint({
      headers, colMap, rawRows, results,
      processedIndex: total,
      hsMap, hsIdMap, hsDomainMap,
    });

    setStats(s);
    setEnriched(results);
    setErrs(errors);
    setProg({ done: total, total, msg: "Complete — downloading...", startTime: enrichStartTime });
    setAutoDownload(true);
    setStep(3);
  }

  // ── Resume enrichment from checkpoint ────────────────────────────────────────
  async function resumeEnrichment(cp) {
    const errors = [];
    const hsMap = { ...cp.hsMap };
    const hsIdMap = { ...cp.hsIdMap };
    const hsDomainMap = { ...(cp.hsDomainMap || {}) };
    const resumeStartTime = Date.now();

    function lookup(name, domain) {
      if (domain) {
        const nd = normalizeDomain(domain);
        if (nd && hsDomainMap[nd]) return hsDomainMap[nd];
      }
      const norm = normalize(name);
      if (!norm) return null;
      if (hsMap[norm]) return hsMap[norm];
      for (const [k, v] of Object.entries(hsMap)) {
        if (tokenPrefixMatch(norm, k)) return v;
      }
      return null;
    }

    function resolveEntry(name, domain) {
      const entry = lookup(name, domain);
      if (!entry) return null;
      const filled = { ...entry };
      let cursor = entry;
      let hops = 0;
      while (hops < 3 && cursor && cursor.parentId &&
             (!filled.ownerName || !filled.icpTier || !filled.category)) {
        const pNorm = hsIdMap[cursor.parentId];
        const parent = pNorm ? hsMap[pNorm] : null;
        if (!parent) break;
        if (!filled.ownerName && parent.ownerName) filled.ownerName = parent.ownerName;
        if (!filled.icpTier && parent.icpTier) filled.icpTier = parent.icpTier;
        if (!filled.category && parent.category) filled.category = parent.category;
        cursor = parent;
        hops++;
      }
      return filled;
    }

    function formatRelationship(entry) {
      if (!entry) return "";
      const lc = (entry.lifecycle || "").toLowerCase();
      const ls = (entry.leadStatus || "").toLowerCase();
      if (lc === "customer") return "Customer";
      if (lc === "opportunity") return "Opportunity";
      if (ls === "in_progress" || ls === "in progress") return "In Progress";
      if (ls === "open") return "Open Lead";
      if (lc === "lead") return "Lead";
      if (lc === "marketingqualifiedlead") return "MQL";
      if (lc === "salesqualifiedlead") return "SQL";
      if (lc || ls) return (lc || ls).replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
      return entry.ownerName ? "Known" : "";
    }

    const total = cp.rawRows.length;
    const results = [...cp.results];
    const bdrNames = PODS.map(p => p.bdr);
    let unassignedIdx = cp.results.filter(r => r._unassignedBdr).length;

    for (let i = cp.processedIndex; i < total; i++) {
      const row = cp.rawRows[i];
      const companyRaw = row[cp.colMap.company] || "";
      const emailCandidate = cp.colMap.email ? row[cp.colMap.email] : "";
      const emailRaw = emailCandidate.includes("@") ? emailCandidate : "";
      setProg({ done: i, total, msg: `Processing ${i + 1} of ${total.toLocaleString()}...`, startTime: resumeStartTime });

      const compNorm = normalize(companyRaw);

      // Domain cache for resume (shared across rows)
      if (!resumeEnrichment._domainCache) resumeEnrichment._domainCache = {};
      const domainCache = resumeEnrichment._domainCache;

      let hsEntry = hsKey && companyRaw ? resolveEntry(companyRaw, domainCache[compNorm]) : null;
      let ownerName = hsEntry?.ownerName || null;
      let apolloData = {};

      let firstName = "", lastName = "";
      const fnCol = cp.headers.find(h => /^first.?name$/i.test(h));
      const lnCol = cp.headers.find(h => /^last.?name$/i.test(h));
      if (fnCol && lnCol) {
        firstName = row[fnCol] || "";
        lastName = row[lnCol] || "";
      } else if (cp.colMap.name && row[cp.colMap.name]) {
        const parts = row[cp.colMap.name].trim().split(" ");
        firstName = parts[0] || "";
        lastName = parts.slice(1).join(" ") || "";
      }

      if (apKey && (firstName || emailRaw)) {
        try {
          // Attempt 1: People Match
          const matchBody = { reveal_personal_emails: true };
          if (emailRaw) matchBody.email = emailRaw;
          if (companyRaw) matchBody.organization_name = companyRaw;
          if (domainCache[compNorm]) matchBody.domain = domainCache[compNorm];
          if (firstName) matchBody.first_name = firstName;
          if (lastName) matchBody.last_name = lastName;

          let res = await fetch("/api/apollo/v1/people/match", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
            body: JSON.stringify(matchBody),
          });
          let person = null;
          if (res.ok) { const d = await res.json(); person = d.person; }

          // Attempt 2: People Search fallback
          if (!person && firstName && companyRaw) {
            const searchBody = {
              q_keywords: `${firstName} ${lastName}`.trim(),
              q_organization_name: companyRaw,
              page: 1, per_page: 1,
            };
            if (domainCache[compNorm]) searchBody.organization_domains = [domainCache[compNorm]];
            res = await fetch("/api/apollo/api/v1/mixed_people/api_search", {
              method: "POST",
              headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
              body: JSON.stringify(searchBody),
            });
            if (res.ok) {
              const d = await res.json();
              const candidates = d.people || [];
              const fn = firstName.toLowerCase(), ln = lastName.toLowerCase();
              person = candidates.find(c =>
                (c.first_name || "").toLowerCase() === fn &&
                (c.last_name || "").toLowerCase() === ln
              ) || candidates[0] || null;
            }
          }

          if (person) {
            apolloData = {
              _email: person.email || "",
              _phone: person.phone_numbers?.[0]?.sanitized_number || person.organization?.phone || "",
              _linkedin: person.linkedin_url || "",
              _apollo_title: person.title || "",
              _apollo_company: person.organization?.name || "",
            };
            if (person.organization?.primary_domain && compNorm) {
              domainCache[compNorm] = person.organization.primary_domain;
            }
            if (hsKey) {
              const apolloDomain = domainCache[compNorm] || "";
              const nd = normalizeDomain(apolloDomain);
              if (nd && hsDomainMap[nd]) {
                const domainEntry = resolveEntry(apolloData._apollo_company || companyRaw, apolloDomain);
                if (domainEntry) { hsEntry = domainEntry; ownerName = domainEntry.ownerName || null; }
              } else if (!hsEntry && apolloData._apollo_company) {
                const fallback = resolveEntry(apolloData._apollo_company, apolloDomain);
                if (fallback) { hsEntry = fallback; ownerName = fallback.ownerName || null; }
              }
            }
          }
          await new Promise(r => setTimeout(r, 100));
        } catch (_) {}
      }

      const podId = ownerName ? podForOwner(ownerName) : null;
      const unassignedBdr = !ownerName ? bdrNames[unassignedIdx++ % bdrNames.length] : null;

      let who = "";
      if (podId) {
        const pod = PODS.find(p => p.id === podId);
        who = pod ? pod.bdr : ownerName;
      } else if (ownerName) {
        who = ownerName;
      } else if (unassignedBdr) {
        who = unassignedBdr;
      }

      results.push({
        ...row, ...apolloData,
        _ownerName: ownerName || "",
        _podId: podId || "",
        _unassignedBdr: unassignedBdr || "",
        _who: who,
        _relationship: formatRelationship(hsEntry),
        _hsIcpTier: hsEntry?.icpTier || "",
        _hsCategory: hsEntry?.category || "",
        _hsId: hsEntry?.id || "",
      });

      // Checkpoint every 25 rows
      if ((i + 1) % 25 === 0 || i === total - 1) {
        saveCheckpoint({
          headers: cp.headers, colMap: cp.colMap, rawRows: cp.rawRows, results,
          processedIndex: i + 1,
          hsMap, hsIdMap, hsDomainMap,
        });
      }
    }

    // ICP Classification (Company Type via Claude)
    const compTypeCol = cp.headers.find(h => /company.?type/i.test(h));
    const membershipCol = cp.headers.find(h => /^membership$/i.test(h));
    if (membershipCol && !compTypeCol) {
      const MEMBERSHIP_MAP = {
        "carrier": "Insurance Carrier",
        "program administrator": "MGA / Specialty Underwriter",
        "reinsurer": "Reinsurer",
        "broker": "Insurance Broker",
        "tpa": "TPA / Claims Admin",
        "insurtech": "InsurTech / Technology",
        "other": "",
      };
      results.forEach(r => {
        const mem = (r[membershipCol] || "").toLowerCase().trim();
        const mapped = MEMBERSHIP_MAP[mem];
        if (mapped) r._companyType = mapped;
      });
    }
    const needsCompType = !compTypeCol || results.every(r => !r[compTypeCol]);

    // HubSpot category override (from cleanup-authored `category` property) — runs regardless of Claude availability
    results.forEach(r => {
      if (!r._companyType && r._hsCategory) {
        const mapped = hsCategoryToType(r._hsCategory);
        if (mapped) r._companyType = mapped;
      }
    });

    const claudeKey = import.meta.env.VITE_ANTHROPIC_API_KEY;
    if (claudeKey && needsCompType) {
      // Pre-classify known companies before hitting the API
      results.forEach(r => {
        const known = classifyKnownCompany(r[cp.colMap.company] || "");
        if (known) r._companyType = known;
      });

      setProg({ done: 0, total, msg: "Classifying companies (ICP)...", startTime: resumeStartTime });
      const BATCH = 30;
      for (let b = 0; b < results.length; b += BATCH) {
        const batch = results.slice(b, b + BATCH);
        const needsClassification = batch.filter(r => !r._companyType);
        if (needsClassification.length === 0) {
          setProg({ done: b + batch.length, total, msg: `Classifying ${Math.min(b + batch.length, total)} of ${total}...`, startTime: resumeStartTime });
          continue;
        }
        const lines = needsClassification.map((r, idx) =>
          `${idx + 1}. Company: "${r[cp.colMap.company] || ""}" | Title: "${cp.colMap.title ? r[cp.colMap.title] : (r._apollo_title || "")}"`
        ).join("\n");

        try {
          const res = await fetch("/api/anthropic/v1/messages", {
            method: "POST",
            headers: { "x-api-key": claudeKey, "anthropic-version": "2023-06-01", "content-type": "application/json" },
            body: JSON.stringify({
              model: "claude-haiku-4-5-20251001",
              max_tokens: 4096,
              messages: [{ role: "user", content: `You are classifying companies for an insurance industry CRM. For each numbered entry, classify the Company Type as EXACTLY one of these values:\n- Insurance Carrier\n- MGA / Specialty Underwriter\n- Insurance Broker\n- TPA / Claims Admin\n- Risk Consulting / Advisory\n- InsurTech / Technology\n- Reinsurer\n- Captive / Risk Finance\n- Corporate / End-User\n- Academic / Association\n\nRules:\n- Insurance companies that write policies = Insurance Carrier\n- Managing General Agents, wholesalers, program administrators that underwrite = MGA / Specialty Underwriter\n- Brokerages that place risk = Insurance Broker\n- Third-party administrators, claims servicers = TPA / Claims Admin\n- Law firms, consultancies, risk advisors = Risk Consulting / Advisory\n- Software/tech vendors serving insurance = InsurTech / Technology\n- Reinsurance companies = Reinsurer\n- Non-insurance companies = Corporate / End-User\n- Universities, associations, foundations = Academic / Association\n\nReply with ONLY numbered lines:\n1. Company Type\n\nCompanies:\n${lines}` }],
            }),
          });
          if (res.ok) {
            const d = await res.json();
            const text = d.content?.[0]?.text || "";
            text.split("\n").forEach(line => {
              const m = line.match(/^(\d+)\.\s*(.+)/);
              if (m) {
                const idx = parseInt(m[1]) - 1;
                if (idx >= 0 && idx < needsClassification.length) {
                  const ct = m[2].trim();
                  needsClassification[idx]._companyType = COMPANY_TYPES.find(t => ct.toLowerCase().includes(t.toLowerCase())) || ct;
                }
              }
            });
          } else {
            const errBody = await res.text().catch(() => "");
            errors.push(`Claude ${res.status}: ${errBody.slice(0, 100)}`);
            break;
          }
        } catch (err) { errors.push(`Claude: ${err.message}`); break; }
        setProg({ done: b + batch.length, total, msg: `Classifying ${Math.min(b + batch.length, total)} of ${total}...`, startTime: resumeStartTime });
      }
    }

    // Writeback: fill missing HubSpot `category` for Tier 1/2 companies
    if (hsKey) {
      const writeback = {};
      for (const r of results) {
        const hsCat = (r._hsCategory || "").trim();
        const tier = (r._hsIcpTier || "").toLowerCase();
        const target = TYPE_TO_HS_CATEGORY[r._companyType];
        // Safe because TYPE_TO_HS_CATEGORY only maps the 5 insurance buckets — non-insurance
        // classifications can't produce a writeback target. Skip only Disqualified companies.
        if (r._hsId && !hsCat && tier !== "disqualified" && target) {
          writeback[r._hsId] = target;
        }
      }
      const count = Object.keys(writeback).length;
      if (count) {
        setProg(p => ({ ...p, msg: `HubSpot writeback: ${count} companies...` }));
        const { ok, fail } = await writebackHubspotCategories(writeback, hsKey);
        if (fail) errors.push(`HubSpot writeback: ${fail} failed`);
        for (const [id, cat] of Object.entries(writeback)) {
          for (const r of results) if (r._hsId === id) r._hsCategory = cat;
        }
        console.log(`HubSpot writeback: ${ok} updated, ${fail} failed`);
      }
    }

    // Compute ICP for every row
    results.forEach(r => {
      const title = (cp.colMap.title ? r[cp.colMap.title] : "") || r._apollo_title || "";
      const ct = r._companyType || (compTypeCol ? r[compTypeCol] : "") || "Corporate / End-User";
      r._companyType = ct;
      r._seniority = detectSeniority(title);
      const icp = computeICP(ct, r._seniority, title, r._hsIcpTier || "");
      r._icpFit = icp.icpFit;
      r._outreachPriority = icp.priority;
      r._icpReason = icp.reason;
      r._outreachAngle = icp.angle;

      // Customers → always Do Not Contact (already have the relationship)
      if (r._relationship === "Customer") {
        r._outreachPriority = "4 - Do Not Contact";
        r._icpFit = "None";
        r._icpReason = "Existing customer — do not contact";
        r._outreachAngle = "";
      }
    });

    setProg({ done: total, total, msg: "Sorting...", startTime: resumeStartTime });

    const podOrder = PODS.map(p => p.id);
    const prioVal = (r) => r._outreachPriority === "1 - Priority" ? 0 : r._outreachPriority === "2 - Warm" ? 1 : r._outreachPriority === "3 - Okay" ? 2 : 3;
    const whoOrder = (r) => {
      if (r._unassignedBdr) return 100 + bdrNames.indexOf(r._unassignedBdr);
      if (r._podId) return podOrder.indexOf(r._podId);
      if (r._ownerName) return 10;
      return 50;
    };
    results.sort((a, b) => {
      const wa = whoOrder(a), wb = whoOrder(b);
      const assignedA = wa < 100, assignedB = wb < 100;
      if (assignedA !== assignedB) return assignedA ? -1 : 1;
      if (wa !== wb) return wa - wb;
      if (wa === 10) {
        const cmp = (a._ownerName || "").localeCompare(b._ownerName || "");
        if (cmp !== 0) return cmp;
      }
      const pa = prioVal(a), pb = prioVal(b);
      if (pa !== pb) return pa - pb;
      return 0;
    });

    const s = { pods: {}, owners: {}, unassigned: 0 };
    PODS.forEach(p => { s.pods[p.id] = 0; });
    results.forEach(r => {
      if (r._unassignedBdr) s.unassigned++;
      else if (r._podId) s.pods[r._podId]++;
      else if (r._ownerName) s.owners[r._ownerName] = (s.owners[r._ownerName] || 0) + 1;
    });

    // Final checkpoint
    saveCheckpoint({
      headers: cp.headers, colMap: cp.colMap, rawRows: cp.rawRows, results,
      processedIndex: total,
      hsMap, hsIdMap, hsDomainMap,
    });

    setStats(s);
    setEnriched(results);
    setErrs(errors);
    setProg({ done: total, total, msg: "Complete — downloading...", startTime: resumeStartTime });
    setAutoDownload(true);
    setStep(3);
  }

  // ── Helper: find original column by pattern ─────────────────────────────────
  function findCol(patterns) {
    for (const p of patterns) {
      const found = headers.find(h => p.test(h.toLowerCase()));
      if (found) return found;
    }
    return null;
  }

  function getFullName(row) {
    // 1. Try explicit first/last name columns
    const fn = findCol([/^first.?name$/]);
    const ln = findCol([/^last.?name$/]);
    if (fn && ln) {
      const full = `${row[fn] || ""} ${row[ln] || ""}`.trim();
      if (full) return full;
    }
    // 2. Try mapped name column (but never return the company name)
    if (colMap.name) {
      const name = row[colMap.name] || "";
      const company = colMap.company ? (row[colMap.company] || "") : "";
      if (name && name !== company) return name;
    }
    // 3. Use Apollo-discovered name
    if (row._discoveredName) return row._discoveredName;
    // 4. Nothing found — return empty, NOT the company
    return "";
  }

  // ── Download XLSX ────────────────────────────────────────────────────────────
  function splitName(fullName) {
    const n = (fullName || "").trim();
    if (!n) return { first: "", last: "" };
    const parts = n.split(/\s+/);
    if (parts.length === 1) return { first: parts[0], last: "" };
    return { first: parts[0], last: parts.slice(1).join(" ") };
  }

  function downloadXLSX() {
    const phoneCol = findCol([/phone/]);
    const isEmail = (v) => typeof v === "string" && v.includes("@");
    const bdrNames = PODS.map(p => p.bdr);

    // ── Split into 4 groups ──
    const isHighPrio = (r) => r._outreachPriority === "1 - Priority" || r._outreachPriority === "2 - Warm";

    // Sheet 1 "Prime": High prio + pod-assigned (owner is in a pod)
    const prime = enriched.filter(r => isHighPrio(r) && r._podId);

    // Sheet 2 "Round Robin": High prio + NOT pod-assigned (unassigned OR non-pod owner like Aman/Logan)
    // Re-assign non-pod owners to BDRs via round-robin
    let rrIdx = 0;
    const roundRobin = enriched.filter(r => isHighPrio(r) && !r._podId).map(r => {
      const assignedBdr = bdrNames[rrIdx++ % bdrNames.length];
      return { ...r, _rrBdr: assignedBdr, _origOwner: r._ownerName || "" };
    });

    // Sheet 3 "Okay": Tier 3
    const okay = enriched.filter(r => r._outreachPriority === "3 - Okay");

    // Sheet 4 "Do Not Contact": Tier 4
    const doNotContact = enriched.filter(r => r._outreachPriority === "4 - Do Not Contact");

    // ── Column definitions ──
    const PRIME_COLS = [
      { label: "First Name",         get: (row) => splitName(getFullName(row)).first },
      { label: "Last Name",          get: (row) => splitName(getFullName(row)).last },
      { label: "Company",            get: (row) => colMap.company ? (row[colMap.company] || "") : "" },
      { label: "Job Title",          get: (row) => (colMap.title ? row[colMap.title] : "") || row._apollo_title || "" },
      { label: "Email",              get: (row) => { const m = colMap.email ? row[colMap.email] : ""; const a = row._email || ""; return isEmail(m) ? m : isEmail(a) ? a : ""; }},
      { label: "Phone",              get: (row) => (phoneCol ? row[phoneCol] : "") || row._phone || "" },
      { label: "Company Type",       get: (row) => row._companyType || "" },
      { label: "Seniority",          get: (row) => row._seniority || "" },
      { label: "Outreach Priority",  get: (row) => row._outreachPriority || "" },
      { label: "ICP Fit",            get: (row) => row._icpFit || "" },
      { label: "ICP Reason",         get: (row) => row._icpReason || "" },
      { label: "Outreach Angle",     get: (row) => row._outreachAngle || "" },
      { label: "HubSpot Status",     get: (row) => row._relationship || "" },
      { label: "Who",                get: (row) => row._who || "" },
    ];

    const RR_COLS = [
      { label: "First Name",         get: (row) => splitName(getFullName(row)).first },
      { label: "Last Name",          get: (row) => splitName(getFullName(row)).last },
      { label: "Company",            get: (row) => colMap.company ? (row[colMap.company] || "") : "" },
      { label: "Job Title",          get: (row) => (colMap.title ? row[colMap.title] : "") || row._apollo_title || "" },
      { label: "Email",              get: (row) => { const m = colMap.email ? row[colMap.email] : ""; const a = row._email || ""; return isEmail(m) ? m : isEmail(a) ? a : ""; }},
      { label: "Phone",              get: (row) => (phoneCol ? row[phoneCol] : "") || row._phone || "" },
      { label: "Company Type",       get: (row) => row._companyType || "" },
      { label: "Seniority",          get: (row) => row._seniority || "" },
      { label: "Outreach Priority",  get: (row) => row._outreachPriority || "" },
      { label: "ICP Fit",            get: (row) => row._icpFit || "" },
      { label: "Outreach Angle",     get: (row) => row._outreachAngle || "" },
      { label: "HubSpot Status",     get: (row) => row._relationship || "" },
      { label: "HubSpot Owner",      get: (row) => row._origOwner || "" },
      { label: "Assigned To",        get: (row) => row._rrBdr || "" },
    ];

    const LOW_COLS = [
      { label: "First Name",         get: (row) => splitName(getFullName(row)).first },
      { label: "Last Name",          get: (row) => splitName(getFullName(row)).last },
      { label: "Company",            get: (row) => colMap.company ? (row[colMap.company] || "") : "" },
      { label: "Job Title",          get: (row) => (colMap.title ? row[colMap.title] : "") || row._apollo_title || "" },
      { label: "Email",              get: (row) => { const m = colMap.email ? row[colMap.email] : ""; const a = row._email || ""; return isEmail(m) ? m : isEmail(a) ? a : ""; }},
      { label: "Phone",              get: (row) => (phoneCol ? row[phoneCol] : "") || row._phone || "" },
      { label: "Company Type",       get: (row) => row._companyType || "" },
      { label: "HubSpot Status",     get: (row) => row._relationship || "" },
      { label: "Who",                get: (row) => row._who || "" },
    ];

    // ── Sort helpers ──
    const podOrder = PODS.map(p => p.id);
    const prioVal = (r) => r._outreachPriority === "1 - Priority" ? 0 : r._outreachPriority === "2 - Warm" ? 1 : r._outreachPriority === "3 - Okay" ? 2 : 3;

    // Prime: Zain pod → Dani pod → Jacob pod, then by priority
    prime.sort((a, b) => {
      const pa = podOrder.indexOf(a._podId), pb = podOrder.indexOf(b._podId);
      if (pa !== pb) return pa - pb;
      return prioVal(a) - prioVal(b);
    });

    // Round Robin: Zain → Dani → Jacob, then by priority
    roundRobin.sort((a, b) => {
      const ba = bdrNames.indexOf(a._rrBdr), bb = bdrNames.indexOf(b._rrBdr);
      if (ba !== bb) return ba - bb;
      return prioVal(a) - prioVal(b);
    });

    // Okay: Zain → Dani → Jacob → others, then alpha
    okay.sort((a, b) => {
      const wa = a._who || "", wb = b._who || "";
      const oa = bdrNames.indexOf(wa) >= 0 ? bdrNames.indexOf(wa) : (wa ? 10 : 50);
      const ob = bdrNames.indexOf(wb) >= 0 ? bdrNames.indexOf(wb) : (wb ? 10 : 50);
      if (oa !== ob) return oa - ob;
      if (oa === 10) { const c = wa.localeCompare(wb); if (c !== 0) return c; }
      return 0;
    });

    // Do Not Contact: alpha by company
    doNotContact.sort((a, b) => {
      const ca = (colMap.company ? a[colMap.company] : "") || "", cb = (colMap.company ? b[colMap.company] : "") || "";
      return ca.localeCompare(cb);
    });

    // ── Build sheets ──
    const xlsxColors = {
      "zain-nia":        { fill: "DBEAFE", text: "1E40AF" },
      "dani-mike-gavin": { fill: "DCFCE7", text: "166534" },
      "jacob-bobby":     { fill: "FEF9C3", text: "854D0E" },
    };
    const prioColors = { "1 - Priority": { fill: "DCFCE7", text: "166534" }, "2 - Warm": { fill: "FEF9C3", text: "854D0E" }, "3 - Okay": { fill: "DBEAFE", text: "1E40AF" }, "4 - Do Not Contact": { fill: "FEE2E2", text: "991B1B" } };
    const fitColors = { "High": { fill: "DCFCE7", text: "166534" }, "Medium": { fill: "FEF9C3", text: "854D0E" }, "Low": { fill: "DBEAFE", text: "1E40AF" }, "None": { fill: "FEE2E2", text: "991B1B" } };
    const statusColors = { "Customer": { fill: "DBEAFE", text: "1E40AF" }, "Opportunity": { fill: "F3E8FF", text: "6B21A8" }, "In Progress": { fill: "FEF9C3", text: "854D0E" }, "Lead": { fill: "FFEDD5", text: "9A3412" }, "SQL": { fill: "F3E8FF", text: "6B21A8" }, "MQL": { fill: "FFE4E6", text: "9F1239" } };

    // Single sheet with section banners
    const ALL_COLS = [...RR_COLS]; // superset columns (includes HubSpot Owner + Assigned To)
    const numCols = ALL_COLS.length;
    const headerRow = ALL_COLS.map(c => c.label);
    const emptyRow = Array(numCols).fill("");
    const banner = (text) => [text, ...Array(numCols - 1).fill("")];

    const wsData = [headerRow];
    wsData.push(banner(`PRIME — High Priority, Pod-Assigned (${prime.length})`));
    prime.forEach(r => wsData.push(ALL_COLS.map(c => {
      if (c.label === "Assigned To") return r._who || "";
      if (c.label === "HubSpot Owner") return r._ownerName || "";
      return c.get(r);
    })));
    wsData.push(emptyRow);
    wsData.push(banner(`ROUND ROBIN — High Priority, Not Pod-Owned (${roundRobin.length}) — Redistributed to Zain / Dani / Jacob`));
    roundRobin.forEach(r => wsData.push(ALL_COLS.map(c => c.get(r))));
    wsData.push(emptyRow);
    wsData.push(banner(`OKAY — Worth a Touch, Lower Priority (${okay.length})`));
    okay.forEach(r => wsData.push(ALL_COLS.map(c => {
      if (c.label === "Assigned To") return r._who || "";
      if (c.label === "HubSpot Owner") return r._ownerName || "";
      return c.get(r);
    })));
    wsData.push(emptyRow);
    wsData.push(banner(`DO NOT CONTACT — Outside ICP (${doNotContact.length})`));
    doNotContact.forEach(r => wsData.push(ALL_COLS.map(c => {
      if (c.label === "Assigned To") return r._who || "";
      if (c.label === "HubSpot Owner") return r._ownerName || "";
      return c.get(r);
    })));

    const ws = XLSX.utils.aoa_to_sheet(wsData);

    // Header style
    headerRow.forEach((_, ci) => {
      const addr = XLSX.utils.encode_cell({ r: 0, c: ci });
      if (!ws[addr]) ws[addr] = { v: headerRow[ci] };
      ws[addr].s = { font: { bold: true, color: { rgb: "FFFFFF" }, sz: 11 }, fill: { patternType: "solid", fgColor: { rgb: "1A1A2E" } }, alignment: { horizontal: "center", vertical: "center" }, border: { bottom: { style: "medium", color: { rgb: "4AFFC4" } } } };
    });

    // Banner + data styling
    const bannerStyle = { font: { bold: true, color: { rgb: "FFFFFF" }, sz: 13 }, fill: { patternType: "solid", fgColor: { rgb: "0B1F14" } }, alignment: { vertical: "center" } };
    if (!ws["!merges"]) ws["!merges"] = [];
    const assignedCI = ALL_COLS.findIndex(c => c.label === "Assigned To");
    const prioCI2 = ALL_COLS.findIndex(c => c.label === "Outreach Priority");
    const fitCI2 = ALL_COLS.findIndex(c => c.label === "ICP Fit");
    const statusCI2 = ALL_COLS.findIndex(c => c.label === "HubSpot Status");

    for (let ri = 1; ri < wsData.length; ri++) {
      const first = (wsData[ri][0] || "").toString();
      const isBanner = first.startsWith("PRIME") || first.startsWith("ROUND ROBIN") || first.startsWith("OKAY") || first.startsWith("DO NOT CONTACT");
      if (isBanner) {
        ws["!merges"].push({ s: { r: ri, c: 0 }, e: { r: ri, c: numCols - 1 } });
        for (let ci = 0; ci < numCols; ci++) { const a = XLSX.utils.encode_cell({ r: ri, c: ci }); if (!ws[a]) ws[a] = { v: "" }; ws[a].s = bannerStyle; }
        continue;
      }
      if (wsData[ri].every(v => !v)) continue;

      const who = wsData[ri][assignedCI] || "";
      const pid = podForOwner(who);
      const wc = pid ? xlsxColors[pid] : { fill: "F1F5F9", text: "475569" };
      const pr = wsData[ri][prioCI2] || "", ft = wsData[ri][fitCI2] || "", st = wsData[ri][statusCI2] || "";
      for (let ci = 0; ci < numCols; ci++) {
        const addr = XLSX.utils.encode_cell({ r: ri, c: ci });
        if (!ws[addr]) ws[addr] = { v: "" };
        let f = "FFFFFF", t = "1A1A2E", b = false;
        if (ci === assignedCI) { f = wc.fill; t = wc.text; b = true; }
        else if (ci === prioCI2 && prioColors[pr]) { f = prioColors[pr].fill; t = prioColors[pr].text; b = true; }
        else if (ci === fitCI2 && fitColors[ft]) { f = fitColors[ft].fill; t = fitColors[ft].text; }
        else if (ci === statusCI2 && statusColors[st]) { f = statusColors[st].fill; t = statusColors[st].text; }
        ws[addr].s = { fill: { patternType: "solid", fgColor: { rgb: f } }, font: { sz: 10, bold: b, color: { rgb: t } }, alignment: { vertical: "center" } };
      }
    }

    ws["!cols"] = ALL_COLS.map(c => ({ wch: Math.min(Math.max(c.label.length + 4, 16), 50) }));
    ws["!rows"] = wsData.map((row, i) => {
      if (i === 0) return { hpt: 22 };
      const first = (row[0] || "").toString();
      if (first.startsWith("PRIME") || first.startsWith("ROUND") || first.startsWith("OKAY") || first.startsWith("DO NOT")) return { hpt: 30 };
      if (row.every(v => !v)) return { hpt: 8 };
      return { hpt: 16 };
    });

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Conference Enriched");

    const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array", cellStyles: true });
    const blob = new Blob([wbout], { type: "application/octet-stream" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    // Name: extract conference name from source file, uppercase + _CONFBUDDY
    const confName = (sourceFileName || "conference")
      .replace(/\.(csv|xlsx|xls)$/i, "")
      .replace(/[\s_-]+/g, "_")
      .replace(/[^a-zA-Z0-9_]/g, "")
      .toUpperCase();
    a.download = `${confName}_CONFBUDDY.xlsx`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    clearCheckpoint();
  }

  // Auto-download when enrichment finishes
  useEffect(() => {
    if (autoDownload && enriched.length > 0 && step === 3) {
      downloadXLSX();
      setAutoDownload(false);
    }
  }, [autoDownload, enriched, step]);

  const pct = prog.total ? Math.round((prog.done / prog.total) * 100) : 0;

  // ── Render ───────────────────────────────────────────────────────────────────
  return (
    <div style={{ minHeight: "100vh", background: THEME.bg, color: THEME.text, fontFamily: "'Inter', 'Helvetica Neue', sans-serif", paddingBottom: 80 }}>

      {/* Header */}
      <div style={{ borderBottom: `1px solid ${THEME.border}`, background: THEME.surface, padding: "0" }}>
        <div style={{ maxWidth: 760, margin: "0 auto", padding: "14px 28px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <FurtherLogo />
            <div>
              <span style={{ fontSize: 15, fontWeight: 700, color: THEME.text, letterSpacing: "-0.01em", fontFamily: THEME.serif }}>FurtherAI</span>
              <span style={{ fontSize: 13, color: THEME.muted, marginLeft: 8 }}>/ Conference List Enricher</span>
            </div>
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            {PODS.map(p => {
              const c = POD_COLORS[p.id];
              return (
                <div key={p.id} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 20, background: c.bg, color: c.text, border: `1px solid ${c.border}`, fontWeight: 600 }}>
                  {p.bdr} → {p.aes.join(", ")}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Body */}
      <div style={{ maxWidth: 760, margin: "0 auto", padding: "40px 28px 0" }}>
        <StepBar current={step} />

        {/* ── Step 0: Upload ── */}
        {step === 0 && (<>
          {checkpoint && (
            <div style={{ background: THEME.surface, border: `1px solid ${THEME.accent}44`, borderRadius: 12, padding: "16px 20px", marginBottom: 20, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 700, color: THEME.text, fontFamily: THEME.serif }}>Resume previous session?</div>
                <div style={{ fontSize: 12, color: THEME.muted, marginTop: 4 }}>{checkpoint.results.length} of {checkpoint.rawRows.length} rows processed</div>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => {
                  setHeaders(checkpoint.headers);
                  setColMap(checkpoint.colMap);
                  setRawRows(checkpoint.rawRows);
                  setEnriched(checkpoint.results);
                  if (checkpoint.processedIndex >= checkpoint.rawRows.length) {
                    const s = { pods: {}, owners: {}, unassigned: 0 };
                    PODS.forEach(p => { s.pods[p.id] = 0; });
                    checkpoint.results.forEach(r => {
                      if (r._unassignedBdr) s.unassigned++;
                      else if (r._podId) s.pods[r._podId]++;
                      else if (r._ownerName) s.owners[r._ownerName] = (s.owners[r._ownerName] || 0) + 1;
                    });
                    setStats(s);
                    setStep(3);
                    setAutoDownload(true);
                  } else {
                    setStep(2);
                    resumeEnrichment(checkpoint);
                  }
                }} style={{ padding: "8px 18px", borderRadius: 8, border: "none", background: THEME.accent, color: THEME.bg, fontSize: 13, fontWeight: 700, cursor: "pointer" }}>
                  Resume
                </button>
                <button onClick={() => { clearCheckpoint(); setCheckpoint(null); }} style={{ padding: "8px 18px", borderRadius: 8, border: `1px solid ${THEME.border}`, background: "transparent", color: THEME.muted, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>
                  Start fresh
                </button>
              </div>
            </div>
          )}
          <div
            onDrop={onDrop}
            onDragOver={e => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onClick={() => fileRef.current.click()}
            style={{
              border: `2px dashed ${dragOver ? THEME.accent : THEME.borderHi}`,
              borderRadius: 16, padding: "72px 40px", textAlign: "center", cursor: "pointer",
              background: dragOver ? THEME.accentDim : THEME.surface,
              transition: "all 0.2s",
            }}
          >
            <div style={{ width: 52, height: 52, borderRadius: 12, background: THEME.accentDim, border: `1px solid ${THEME.accent}33`, display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 20px", fontSize: 24 }}>
              📋
            </div>
            <div style={{ fontSize: 18, fontWeight: 700, color: THEME.text, marginBottom: 8, letterSpacing: "-0.02em", fontFamily: THEME.serif }}>
              Drop your attendee file here
            </div>
            <div style={{ fontSize: 13, color: THEME.muted, marginBottom: 20 }}>
              CSV or XLSX — any column layout supported
            </div>
            <div style={{ display: "inline-block", padding: "8px 20px", borderRadius: 8, border: `1px solid ${THEME.accent}`, color: THEME.accent, fontSize: 13, fontWeight: 600 }}>
              Browse files
            </div>
            <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }} onChange={onDrop} />
          </div>
        </>)}

        {/* ── Step 1: Column map ── */}
        {step === 1 && (
          <div style={{ background: THEME.surface, borderRadius: 16, border: `1px solid ${THEME.border}`, padding: 28 }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: THEME.text, marginBottom: 4, letterSpacing: "-0.02em", fontFamily: THEME.serif }}>Map your columns</div>
            <div style={{ fontSize: 13, color: THEME.muted, marginBottom: filteredCount ? 8 : 24 }}>{rawRows.length.toLocaleString()} rows detected. We auto-mapped what we could — fix anything that's off.</div>
            {filteredCount > 0 && (
              <div style={{ fontSize: 12, color: "#FBBF24", background: "#2A220010", border: "1px solid #5C4A00", borderRadius: 8, padding: "8px 12px", marginBottom: 24 }}>
                Filtered out {filteredCount} rows with invalid company data (AI-hallucinated text detected).
              </div>
            )}

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 22 }}>
              {[
                { key: "company", label: "Company / Org", req: true },
                { key: "name",    label: "Contact name" },
                { key: "title",   label: "Job title" },
                { key: "email",   label: "Email" },
              ].map(({ key, label, req }) => (
                <div key={key}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: THEME.muted, display: "block", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>
                    {label}{req && <span style={{ color: THEME.accent }}> *</span>}
                  </label>
                  <select value={colMap[key]} onChange={e => setColMap(m => ({ ...m, [key]: e.target.value }))} style={{ ...inputStyle }}>
                    <option value="">— not mapped —</option>
                    {headers.map(h => <option key={h} value={h}>{h}</option>)}
                  </select>
                </div>
              ))}
            </div>

            {rawRows[0] && (
              <div style={{ background: THEME.bg, border: `1px solid ${THEME.border}`, borderRadius: 8, padding: "10px 14px", marginBottom: 24, fontSize: 12, color: THEME.muted }}>
                <span style={{ fontWeight: 700, color: THEME.text }}>Row 1 preview: </span>
                {colMap.name && <><span style={{ color: THEME.accent }}>{rawRows[0][colMap.name]}</span> · </>}
                {colMap.title && <>{rawRows[0][colMap.title]} · </>}
                {colMap.company && <>{rawRows[0][colMap.company]} · </>}
                {colMap.email && <>{rawRows[0][colMap.email]}</>}
              </div>
            )}

            <button onClick={() => { setStep(2); runEnrichment(); }} disabled={!colMap.company} style={{
              padding: "10px 24px", borderRadius: 8, border: "none", cursor: colMap.company ? "pointer" : "not-allowed",
              background: colMap.company ? THEME.accent : THEME.border,
              color: colMap.company ? THEME.bg : THEME.muted,
              fontSize: 13, fontWeight: 700, letterSpacing: "0.02em",
            }}>
              Run enrichment →
            </button>
          </div>
        )}

        {/* ── Step 2: Progress ── */}
        {step === 2 && (
          <div style={{ background: THEME.surface, borderRadius: 16, border: `1px solid ${THEME.border}`, padding: 48, textAlign: "center" }}>
            <div style={{ width: 48, height: 48, borderRadius: "50%", border: `3px solid ${THEME.accentDim}`, borderTop: `3px solid ${THEME.accent}`, margin: "0 auto 24px", animation: "spin 1s linear infinite" }} />
            <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
            <div style={{ fontSize: 15, fontWeight: 700, color: THEME.text, marginBottom: 20, letterSpacing: "-0.01em", fontFamily: THEME.serif }}>{prog.msg}</div>
            <div style={{ background: THEME.bg, borderRadius: 999, height: 4, overflow: "hidden", maxWidth: 380, margin: "0 auto 12px", border: `1px solid ${THEME.border}` }}>
              <div style={{ background: THEME.accent, height: "100%", width: `${pct}%`, borderRadius: 999, transition: "width 0.4s ease" }} />
            </div>
            <div style={{ fontSize: 13, color: THEME.muted }}>{prog.done.toLocaleString()} / {prog.total.toLocaleString()} rows</div>
            <div style={{ fontSize: 12, color: THEME.muted, marginTop: 4 }}>
              {prog.done > 5 && prog.total > prog.done && prog.startTime && (() => {
                const elapsed = (Date.now() - prog.startTime) / 1000;
                const rate = prog.done / elapsed;
                const remaining = Math.round((prog.total - prog.done) / rate / 60);
                return `~${remaining} min remaining`;
              })()}
            </div>
          </div>
        )}

        {/* ── Step 3: Results ── */}
        {step === 3 && stats && (
          <div>
            {/* Stat cards */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 10, marginBottom: 28 }}>
              {PODS.map(p => {
                const c = POD_COLORS[p.id]; const count = stats.pods[p.id] || 0;
                return (
                  <div key={p.id} style={{ background: c.bg, border: `1px solid ${c.border}`, borderRadius: 12, padding: "14px 16px" }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: c.text, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>{c.label}</div>
                    <div style={{ fontSize: 28, fontWeight: 800, color: c.text, lineHeight: 1, letterSpacing: "-0.03em" }}>{count.toLocaleString()}</div>
                    <div style={{ fontSize: 11, color: c.text, opacity: 0.55, marginTop: 4 }}>{enriched.length ? Math.round(count / enriched.length * 100) : 0}% of total</div>
                  </div>
                );
              })}
              {Object.entries(stats.owners).map(([owner, count]) => {
                const c = ownerColor(owner);
                return (
                  <div key={owner} style={{ background: c.bg, border: `1px solid ${c.border}`, borderRadius: 12, padding: "14px 16px" }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: c.text, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>{owner}</div>
                    <div style={{ fontSize: 28, fontWeight: 800, color: c.text, lineHeight: 1, letterSpacing: "-0.03em" }}>{count.toLocaleString()}</div>
                    <div style={{ fontSize: 11, color: c.text, opacity: 0.55, marginTop: 4 }}>{enriched.length ? Math.round(count / enriched.length * 100) : 0}% of total</div>
                  </div>
                );
              })}
              <div style={{ background: UNASSIGNED_COLOR.bg, border: `1px solid ${UNASSIGNED_COLOR.border}`, borderRadius: 12, padding: "14px 16px" }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: UNASSIGNED_COLOR.text, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>Unassigned</div>
                <div style={{ fontSize: 28, fontWeight: 800, color: UNASSIGNED_COLOR.text, lineHeight: 1, letterSpacing: "-0.03em" }}>{stats.unassigned.toLocaleString()}</div>
                <div style={{ fontSize: 11, color: UNASSIGNED_COLOR.text, opacity: 0.6, marginTop: 4 }}>split 3 ways ↓ bottom</div>
              </div>
            </div>

            {errs.length > 0 && (
              <div style={{ background: "#1A0505", border: "1px solid #5C1A1A", borderRadius: 8, padding: "10px 14px", marginBottom: 20, fontSize: 12, color: "#FCA5A5" }}>
                {errs.map((e, i) => <div key={i}>⚠ {e}</div>)}
              </div>
            )}

            {/* Preview table */}
            <div style={{ background: THEME.surface, border: `1px solid ${THEME.border}`, borderRadius: 12, overflow: "hidden", marginBottom: 28 }}>
              <div style={{ padding: "12px 18px", borderBottom: `1px solid ${THEME.border}`, fontSize: 12, fontWeight: 700, color: THEME.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>
                Preview — first 15 rows
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ background: THEME.bg }}>
                      {["#", "Name", "Company", "Job Title", "Email", "Who"].map(h => (
                        <th key={h} style={{ padding: "8px 14px", textAlign: "left", fontWeight: 700, color: THEME.muted, borderBottom: `1px solid ${THEME.border}`, whiteSpace: "nowrap", textTransform: "uppercase", fontSize: 10, letterSpacing: "0.07em" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {enriched.slice(0, 15).map((row, i) => {
                      const c = row._unassignedBdr ? ownerColor(row._unassignedBdr)
                        : row._podId ? POD_COLORS[row._podId]
                        : row._ownerName ? ownerColor(row._ownerName)
                        : UNASSIGNED_COLOR;
                      return (
                        <tr key={i} style={{ background: c.bg, borderBottom: `1px solid ${THEME.border}` }}>
                          <td style={{ padding: "8px 14px", color: c.text, opacity: 0.4 }}>{i + 1}</td>
                          <td style={{ padding: "8px 14px", color: c.text, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{getFullName(row) || "—"}</td>
                          <td style={{ padding: "8px 14px", color: c.text, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{colMap.company ? row[colMap.company] : "—"}</td>
                          <td style={{ padding: "8px 14px", color: c.text, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{(colMap.title ? row[colMap.title] : "") || row._apollo_title || "—"}</td>
                          <td style={{ padding: "8px 14px", color: c.text, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{
                            (() => { const m = colMap.email ? row[colMap.email] : ""; const a = row._email || ""; return (m && m.includes("@") ? m : a && a.includes("@") ? a : "") || "—"; })()
                          }</td>
                          <td style={{ padding: "8px 14px" }}><Badge ownerName={row._ownerName} unassignedBdr={row._unassignedBdr} /></td>
                        </tr>
                      );
                    })}
                    {enriched.length > 15 && (
                      <tr style={{ background: THEME.bg }}>
                        <td colSpan={6} style={{ padding: "10px 14px", fontSize: 11, color: THEME.muted, textAlign: "center" }}>
                          +{(enriched.length - 15).toLocaleString()} more rows in the download
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
              <button onClick={downloadXLSX} style={{
                padding: "12px 28px", borderRadius: 10, border: "none",
                background: THEME.accent, color: THEME.bg,
                fontSize: 14, fontWeight: 700, cursor: "pointer", letterSpacing: "-0.01em",
              }}>
                Download XLSX
              </button>
              <button onClick={() => {
                clearCheckpoint(); setCheckpoint(null);
                setStep(0); setRawRows([]); setEnriched([]); setStats(null);
                setErrs([]); setProg({ done: 0, total: 0, msg: "" }); setAutoDownload(false);
                palIdx = 0; Object.keys(palCache).forEach(k => delete palCache[k]);
              }} style={{
                padding: "12px 20px", borderRadius: 10, border: `1px solid ${THEME.border}`,
                background: "transparent", fontSize: 13, cursor: "pointer", color: THEME.muted, fontWeight: 600,
              }}>
                Start over
              </button>
              <span style={{ fontSize: 12, color: THEME.muted, marginLeft: 4 }}>
                {enriched.length.toLocaleString()} rows processed
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
