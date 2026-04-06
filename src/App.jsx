import { useState, useCallback, useRef, useEffect } from "react";
import XLSX from "xlsx-js-style";

// ─── Pod config ───────────────────────────────────────────────────────────────
const PODS = [
  { id: "zain-nia",        bdr: "Zain",  aes: ["Nia"] },
  { id: "dani-mike-gavin", bdr: "Dani",  aes: ["Mike", "Gavin"] },
  { id: "jacob-bobby",     bdr: "Jacob", aes: ["Bobby"] },
];

// FurtherAI palette — dark theme
const THEME = {
  bg:        "#0C0C10",
  surface:   "#13131A",
  border:    "#1E1E2E",
  borderHi:  "#2A2A3E",
  text:      "#F0F0F8",
  muted:     "#6B6B8A",
  accent:    "#4AFFC4",   // teal-green from FurtherAI
  accentDim: "#1A3D33",
};

const POD_COLORS = {
  "zain-nia":        { bg: "#0D2137", border: "#1A4A7A", text: "#60A5FA", label: "Zain / Nia" },
  "dani-mike-gavin": { bg: "#0D2818", border: "#1A5C2E", text: "#4ADE80", label: "Dani / Mike & Gavin" },
  "jacob-bobby":     { bg: "#2A1A00", border: "#5C3A00", text: "#FBBF24", label: "Jacob / Bobby" },
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

// ─── Logo SVG ─────────────────────────────────────────────────────────────────
function FurtherLogo() {
  return (
    <svg width="28" height="28" viewBox="0 0 28 28" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect width="28" height="28" rx="6" fill={THEME.accent} />
      <path d="M7 8h14v3H10v3h9v3H10v5H7V8z" fill="#0C0C10" />
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
  const fileRef = useRef();

  // ── File upload ──────────────────────────────────────────────────────────────
  const handleFile = useCallback((file) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const wb = XLSX.read(e.target.result, { type: "array" });
      const ws = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "" });
      if (rows.length < 2) return;
      const hdrs = rows[0].map(String);
      setHeaders(hdrs);
      setRawRows(rows.slice(1).map(r => { const o = {}; hdrs.forEach((h, i) => { o[h] = String(r[i] ?? ""); }); return o; }));
      const auto = { company: "", name: "", title: "", email: "" };
      hdrs.forEach(h => {
        const l = h.toLowerCase();
        if (!auto.company && /company|org|account|employer/.test(l)) auto.company = h;
        if (!auto.name && /^name$|full.?name|contact.?name/.test(l)) auto.name = h;
        if (!auto.title && /title|job|role|position/.test(l)) auto.title = h;
        if (!auto.email && /email|e-mail/.test(l)) auto.email = h;
      });
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

    if (hsKey) {
      try {
        setProg({ done: 0, total: rawRows.length, msg: "Fetching HubSpot owners..." });
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
          const url = `/api/hubspot/crm/v3/objects/companies?limit=100&properties=name,hubspot_owner_id,parent_company_id${after ? `&after=${after}` : ""}`;
          const res = await fetch(url, { headers: { Authorization: `Bearer ${hsKey}` } });
          if (!res.ok) { errors.push(`HubSpot ${res.status}: ${res.statusText}`); break; }
          const d = await res.json();
          (d.results || []).forEach(c => {
            const norm = normalize(c.properties?.name);
            if (!norm) return;
            const entry = {
              ownerName: ownerMap[c.properties?.hubspot_owner_id] || null,
              id: c.id,
              parentId: c.properties?.parent_company_id || null,
            };
            hsMap[norm] = entry;
            hsIdMap[c.id] = norm;
          });
          fetched += (d.results || []).length;
          if (d.paging?.next?.after) after = d.paging.next.after; else break;
          if (fetched > 20000) break;
        }
      } catch (err) { errors.push(`HubSpot: ${err.message}`); }
    }

    function lookup(name) {
      const norm = normalize(name);
      if (hsMap[norm]) return hsMap[norm];
      for (const [k, v] of Object.entries(hsMap)) {
        if (k.length > 3 && (norm.includes(k) || k.includes(norm))) return v;
      }
      return null;
    }

    function resolveOwner(name) {
      let entry = lookup(name);
      if (!entry) return null;
      let hops = 0;
      while (entry && !entry.ownerName && entry.parentId && hops < 3) {
        const pNorm = hsIdMap[entry.parentId];
        entry = pNorm ? hsMap[pNorm] : null;
        hops++;
      }
      return entry?.ownerName || null;
    }

    const total = rawRows.length;
    const results = [];
    const bdrNames = PODS.map(p => p.bdr);
    let unassignedIdx = 0;

    for (let i = 0; i < total; i++) {
      const row = rawRows[i];
      const companyRaw = row[colMap.company] || "";
      const emailCandidate = colMap.email ? row[colMap.email] : "";
      const emailRaw = emailCandidate.includes("@") ? emailCandidate : "";
      setProg({ done: i, total, msg: `Processing ${i + 1} of ${total.toLocaleString()}...` });

      let ownerName = hsKey && companyRaw ? resolveOwner(companyRaw) : null;
      let apolloData = {};

      if (apKey) {
        try {
          const body = { reveal_personal_emails: true };
          if (emailRaw) body.email = emailRaw;
          if (companyRaw) body.organization_name = companyRaw;
          const fnCol = headers.find(h => /^first.?name$/i.test(h));
          const lnCol = headers.find(h => /^last.?name$/i.test(h));
          if (fnCol && lnCol) {
            if (row[fnCol]) body.first_name = row[fnCol];
            if (row[lnCol]) body.last_name = row[lnCol];
          } else if (colMap.name && row[colMap.name]) {
            const parts = row[colMap.name].trim().split(" ");
            if (parts[0]) body.first_name = parts[0];
            if (parts[1]) body.last_name = parts.slice(1).join(" ");
          }
          const res = await fetch("/api/apollo/v1/people/match", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Api-Key": apKey },
            body: JSON.stringify(body),
          });
          if (res.ok) {
            const d = await res.json();
            const p = d.person;
            if (p) {
              console.log(`Apollo [${body.first_name} ${body.last_name}]: email=${p.email}, org_phone=${p.organization?.phone}, phones=${JSON.stringify(p.phone_numbers?.slice(0,2))}`);
              apolloData = {
                _email: p.email || "",
                _phone: p.phone_numbers?.[0]?.sanitized_number || p.organization?.phone || "",
                _linkedin: p.linkedin_url || "",
                _apollo_title: p.title || "",
                _seniority: p.seniority || "",
                _city: p.city || "",
                _apollo_company: p.organization?.name || "",
              };
              if (!ownerName && apolloData._apollo_company && hsKey) {
                ownerName = resolveOwner(apolloData._apollo_company);
              }
            }
          }
          await new Promise(r => setTimeout(r, 200));
        } catch (_) {}
      }

      const podId = ownerName ? podForOwner(ownerName) : null;
      const unassignedBdr = !ownerName ? bdrNames[unassignedIdx++ % bdrNames.length] : null;

      // Build "Who" value
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
      });
    }

    // ── Classification for Membership + Programs/Business Category ──
    const memberCol = headers.find(h => /membership|member.?type/i.test(h));
    const needsMembership = !memberCol || results.every(r => !r[memberCol]);
    const progCol = headers.find(h => /program|business.?cat|category/i.test(h));
    const needsProg = !progCol || results.every(r => !r[progCol]);

    const claudeKey = import.meta.env.VITE_ANTHROPIC_API_KEY;
    if (claudeKey && (needsMembership || needsProg)) {
      setProg({ done: 0, total, msg: "Classifying with Claude..." });
      const BATCH = 30;
      for (let b = 0; b < results.length; b += BATCH) {
        const batch = results.slice(b, b + BATCH);
        const lines = batch.map((r, i) =>
          `${i + 1}. Company: "${r[colMap.company] || ""}" | Title: "${colMap.title ? r[colMap.title] : (r._apollo_title || "")}"`
        ).join("\n");

        const classifyFields = [];
        if (needsMembership) classifyFields.push('"Membership" (one of: Carrier, Program Administrator, Other)');
        if (needsProg) classifyFields.push('"Category" (one of: Carrier, Program Administrator, MGA, Reinsurer, Broker, Technology/Vendor, Consultant, Other)');

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
                content: `You are classifying insurance industry companies. For each numbered company below, provide ${classifyFields.join(" and ")}.\n\nReply with ONLY numbered lines in this exact format:\n1. Membership | Category\n2. Membership | Category\n\nCompanies:\n${lines}`
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
                if (idx >= 0 && idx < batch.length) {
                  const parts = m[2].split("|").map(s => s.trim());
                  if (needsMembership && parts[0]) batch[idx]._membership = parts[0];
                  if (needsProg && parts[needsMembership ? 1 : 0]) batch[idx]._bizCategory = parts[needsMembership ? 1 : 0];
                }
              }
            });
          } else {
            // Claude API failed — log and continue
            const errBody = await res.text().catch(() => "");
            errors.push(`Claude ${res.status}: ${errBody.slice(0, 100)}`);
            break; // Don't retry if auth fails
          }
        } catch (err) {
          errors.push(`Claude: ${err.message}`);
          break;
        }
        setProg({ done: b + batch.length, total, msg: `Classifying ${Math.min(b + batch.length, total)} of ${total}...` });
      }
    }

    setProg({ done: total, total, msg: "Sorting..." });

    const podOrder = PODS.map(p => p.id);
    results.sort((a, b) => {
      const sa = a._unassignedBdr ? 999 : a._podId ? podOrder.indexOf(a._podId) : 100;
      const sb = b._unassignedBdr ? 999 : b._podId ? podOrder.indexOf(b._podId) : 100;
      if (sa !== sb) return sa - sb;
      if (sa === 999) return bdrNames.indexOf(a._unassignedBdr) - bdrNames.indexOf(b._unassignedBdr);
      if (sa === 100) return (a._ownerName || "").localeCompare(b._ownerName || "");
      return 0;
    });

    const s = { pods: {}, owners: {}, unassigned: 0 };
    PODS.forEach(p => { s.pods[p.id] = 0; });
    results.forEach(r => {
      if (r._unassignedBdr) s.unassigned++;
      else if (r._podId) s.pods[r._podId]++;
      else if (r._ownerName) s.owners[r._ownerName] = (s.owners[r._ownerName] || 0) + 1;
    });

    setStats(s);
    setEnriched(results);
    setErrs(errors);
    setProg({ done: total, total, msg: "Complete — downloading..." });
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
    const fn = findCol([/^first.?name$/]);
    const ln = findCol([/^last.?name$/]);
    if (fn && ln) return `${row[fn] || ""} ${row[ln] || ""}`.trim();
    if (colMap.name) return row[colMap.name] || "";
    return "";
  }

  // ── Download XLSX ────────────────────────────────────────────────────────────
  function downloadXLSX() {
    // Fixed output columns
    const phoneCol = findCol([/phone/]);
    const memberCol = findCol([/membership/, /member.?type/, /type/]);
    const progCol = findCol([/program/, /business.?cat/, /category/]);
    const isEmail = (v) => typeof v === "string" && v.includes("@");
    const OUTPUT_COLS = [
      { label: "Name",                        get: (row) => getFullName(row) },
      { label: "Company",                    get: (row) => colMap.company ? (row[colMap.company] || "") : "" },
      { label: "Job Title",                  get: (row) => (colMap.title ? row[colMap.title] : "") || row._apollo_title || "" },
      { label: "Email",                      get: (row) => {
        const mapped = colMap.email ? row[colMap.email] : "";
        const apollo = row._email || "";
        if (isEmail(mapped)) return mapped;
        if (isEmail(apollo)) return apollo;
        return "";
      }},
      { label: "Phone",                      get: (row) => (phoneCol ? row[phoneCol] : "") || row._phone || "" },
      { label: "Membership",                 get: (row) => (memberCol ? row[memberCol] : "") || row._membership || "" },
      { label: "Programs/Business Category", get: (row) => (progCol ? row[progCol] : "") || row._bizCategory || "" },
      { label: "Who",                        get: (row) => row._who || "" },
    ];

    // Split assigned vs unassigned, insert 2-row gap
    const assigned = enriched.filter(r => !r._unassignedBdr);
    const unassigned = enriched.filter(r => r._unassignedBdr);
    const gapRows = unassigned.length > 0 ? [null, null] : []; // 2 empty rows
    const allRows = [...assigned, ...gapRows, ...unassigned];

    const headerRow = OUTPUT_COLS.map(c => c.label);
    const wsData = [headerRow, ...allRows.map(row =>
      row ? OUTPUT_COLS.map(c => c.get(row)) : OUTPUT_COLS.map(() => "")
    )];
    const ws = XLSX.utils.aoa_to_sheet(wsData);

    // Header styling
    headerRow.forEach((_, ci) => {
      const addr = XLSX.utils.encode_cell({ r: 0, c: ci });
      if (!ws[addr]) ws[addr] = { v: headerRow[ci] };
      ws[addr].s = {
        font: { bold: true, color: { rgb: "FFFFFF" }, sz: 11 },
        fill: { patternType: "solid", fgColor: { rgb: "1A1A2E" } },
        alignment: { horizontal: "center", vertical: "center" },
        border: { bottom: { style: "medium", color: { rgb: "4AFFC4" } } },
      };
    });

    // Row colors — light pastels for Excel readability
    const xlsxColors = {
      "zain-nia":        { fill: "DBEAFE", text: "1E40AF" },
      "dani-mike-gavin": { fill: "DCFCE7", text: "166534" },
      "jacob-bobby":     { fill: "FEF9C3", text: "854D0E" },
    };
    const ownerXlsxCache = {};
    const ownerXlsxPalette = [
      { fill: "F3E8FF", text: "6B21A8" },
      { fill: "FFE4E6", text: "9F1239" },
      { fill: "FFEDD5", text: "9A3412" },
      { fill: "E0F2FE", text: "075985" },
      { fill: "FDF4FF", text: "86198F" },
      { fill: "F0FDF4", text: "166534" },
    ];
    let ownerXlsxIdx = 0;

    function getRowColor(row) {
      if (!row) return null;
      if (row._unassignedBdr) {
        const pid = podForOwner(row._unassignedBdr);
        return pid ? xlsxColors[pid] : { fill: "F1F5F9", text: "475569" };
      } else if (row._podId && xlsxColors[row._podId]) {
        return xlsxColors[row._podId];
      } else if (row._ownerName) {
        if (!ownerXlsxCache[row._ownerName]) {
          ownerXlsxCache[row._ownerName] = ownerXlsxPalette[ownerXlsxIdx++ % ownerXlsxPalette.length];
        }
        return ownerXlsxCache[row._ownerName];
      }
      return { fill: "F1F5F9", text: "475569" };
    }

    const whoColIdx = OUTPUT_COLS.length - 1; // "Who" is the last column

    allRows.forEach((row, ri) => {
      const color = getRowColor(row);
      if (!color) return; // gap row — leave blank
      OUTPUT_COLS.forEach((_, ci) => {
        const addr = XLSX.utils.encode_cell({ r: ri + 1, c: ci });
        if (!ws[addr]) ws[addr] = { v: "" };
        const isWho = ci === whoColIdx;
        ws[addr].s = {
          fill: { patternType: "solid", fgColor: { rgb: isWho ? color.fill : "FFFFFF" } },
          font: { sz: 10, bold: isWho, color: isWho ? { rgb: color.text } : { rgb: "1A1A2E" } },
          alignment: { vertical: "center" },
        };
      });
    });

    ws["!cols"] = OUTPUT_COLS.map(c => ({ wch: Math.min(Math.max(c.label.length + 4, 16), 50) }));
    ws["!rows"] = [{ hpt: 22 }, ...allRows.map(r => ({ hpt: r ? 16 : 10 }))];

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Enriched Attendees");
    // Browser-safe download (writeFile uses Node fs which fails in browser)
    const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array", cellStyles: true });
    const blob = new Blob([wbout], { type: "application/octet-stream" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "conference_enriched.xlsx";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
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
              <span style={{ fontSize: 15, fontWeight: 700, color: THEME.text, letterSpacing: "-0.01em" }}>FurtherAI</span>
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
        {step === 0 && (
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
            <div style={{ fontSize: 18, fontWeight: 700, color: THEME.text, marginBottom: 8, letterSpacing: "-0.02em" }}>
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
        )}

        {/* ── Step 1: Column map ── */}
        {step === 1 && (
          <div style={{ background: THEME.surface, borderRadius: 16, border: `1px solid ${THEME.border}`, padding: 28 }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: THEME.text, marginBottom: 4, letterSpacing: "-0.02em" }}>Map your columns</div>
            <div style={{ fontSize: 13, color: THEME.muted, marginBottom: 24 }}>{rawRows.length.toLocaleString()} rows detected. We auto-mapped what we could — fix anything that's off.</div>

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
            <div style={{ fontSize: 15, fontWeight: 700, color: THEME.text, marginBottom: 20, letterSpacing: "-0.01em" }}>{prog.msg}</div>
            <div style={{ background: THEME.bg, borderRadius: 999, height: 4, overflow: "hidden", maxWidth: 380, margin: "0 auto 12px", border: `1px solid ${THEME.border}` }}>
              <div style={{ background: THEME.accent, height: "100%", width: `${pct}%`, borderRadius: 999, transition: "width 0.4s ease" }} />
            </div>
            <div style={{ fontSize: 13, color: THEME.muted }}>{prog.done.toLocaleString()} / {prog.total.toLocaleString()} rows</div>
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
