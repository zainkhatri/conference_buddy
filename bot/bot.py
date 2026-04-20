import os, re, csv, json, io, warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv
load_dotenv()

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import gspread
import requests as req

# ─── Config ───
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
SHEETS_TOKEN_PATH = os.environ["GOOGLE_SHEETS_TOKEN"]
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
# FurtherAI outbound pipeline — push enriched contacts so automated email can draft + send
FURTHERAI_PIPELINE_URL = os.environ.get("FURTHERAI_PIPELINE_URL", "https://autoemail-nine.vercel.app/api/schedule?conference_import=1")
FURTHERAI_CRON_SECRET = os.environ.get("FURTHERAI_CRON_SECRET", "")

app = App(token=SLACK_BOT_TOKEN)
import time

# ─── Company name normalization (matches the app) ───
def normalize_company(s):
    s = (s or "").lower()
    s = re.sub(r'\b(llc|inc|corp|ltd|co\.?|group|holdings|insurance|services|the)\b', '', s)
    s = re.sub(r'[^a-z0-9]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def normalize_domain(d):
    if not d: return ""
    d = d.strip().lower()
    d = re.sub(r'^https?://', '', d)
    d = re.sub(r'^www\.', '', d)
    return d.split('/', 1)[0]

def _token_prefix_match(a, b):
    """True if a == b, or the shorter is a whole-word prefix of the longer (space boundary).
    Requires the shorter to be at least 6 chars to reject generic short names."""
    if not a or not b: return False
    if a == b: return True
    shorter, longer = (a, b) if len(a) < len(b) else (b, a)
    if len(shorter) < 6: return False
    return longer.startswith(shorter + " ")

# ─── HubSpot Enrichment ───
def fetch_hubspot_data():
    if not HUBSPOT_TOKEN:
        return {}, {}, {}
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    # Fetch owners
    owner_map = {}
    r = req.get("https://api.hubapi.com/crm/v3/owners?limit=100", headers=headers)
    if r.ok:
        for o in r.json().get("results", []):
            owner_map[o["id"]] = f"{o.get('firstName','')} {o.get('lastName','')}".strip()

    # Fetch all companies
    hs_map = {}  # normalized name → entry
    hs_id_map = {}  # company id → entry
    hs_domain_map = {}  # normalized domain → entry
    after = None
    while True:
        url = f"https://api.hubapi.com/crm/v3/objects/companies?limit=100&properties=name,domain,hubspot_owner_id,parent_company_id,lifecyclestage,hs_lead_status,icp_tier,category"
        if after: url += f"&after={after}"
        r = req.get(url, headers=headers)
        if not r.ok: break
        data = r.json()
        for co in data.get("results", []):
            props = co.get("properties", {})
            name = props.get("name", "")
            entry = {
                "ownerName": owner_map.get(props.get("hubspot_owner_id", ""), ""),
                "id": co["id"],
                "parentId": props.get("parent_company_id", ""),
                "lifecycle": (props.get("lifecyclestage", "") or "").lower(),
                "leadStatus": (props.get("hs_lead_status", "") or "").lower(),
                "icpTier": (props.get("icp_tier", "") or "").lower(),
                "category": (props.get("category", "") or "").strip(),
                "domain": normalize_domain(props.get("domain", "")),
            }
            norm = normalize_company(name)
            if norm: hs_map[norm] = entry
            hs_id_map[co["id"]] = entry
            if entry["domain"]: hs_domain_map[entry["domain"]] = entry
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after: break

    return hs_map, hs_id_map, hs_domain_map

def lookup_hubspot(company, hs_map, hs_id_map, hs_domain_map=None, domain=""):
    norm = normalize_company(company)
    # 1. Exact normalized-name match (most specific — preserves subsidiary granularity)
    if norm and norm in hs_map: return hs_map[norm]
    # 2. Domain match (deterministic — good fallback when name varies)
    if domain and hs_domain_map:
        nd = normalize_domain(domain)
        if nd and nd in hs_domain_map:
            return hs_domain_map[nd]
    if not norm: return None
    # 3. Whole-token prefix match (rejects generic substring overlaps)
    for key, entry in hs_map.items():
        if _token_prefix_match(norm, key):
            return entry
    return None

def resolve_parent(entry, hs_id_map, depth=0):
    if depth > 3 or not entry: return entry
    if entry.get("ownerName"): return entry
    pid = entry.get("parentId")
    if pid and pid in hs_id_map:
        return resolve_parent(hs_id_map[pid], hs_id_map, depth+1)
    return entry

def resolve_with_inheritance(entry, hs_id_map, depth=0):
    """Return an entry with missing ownerName/icpTier/category filled from the parent chain.
    Child's own non-empty values always win."""
    if not entry or depth > 3: return entry
    filled = dict(entry)
    if filled.get("ownerName") and filled.get("icpTier") and filled.get("category"):
        return filled
    pid = filled.get("parentId")
    if pid and pid in hs_id_map:
        parent = resolve_with_inheritance(hs_id_map[pid], hs_id_map, depth + 1)
        for k in ("ownerName", "icpTier", "category"):
            if not filled.get(k) and parent.get(k):
                filled[k] = parent[k]
    return filled

def writeback_hubspot_categories(updates, log_fn=None):
    """PATCH HubSpot companies in batch with a new `category`.
    updates: dict of {company_id: hubspot_category_value}. Returns (ok_count, fail_count)."""
    if not updates or not HUBSPOT_TOKEN:
        return 0, 0
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    ok, fail = 0, 0
    items = list(updates.items())
    # HubSpot batch endpoint accepts up to 100 inputs per request
    for b in range(0, len(items), 100):
        chunk = items[b:b+100]
        payload = {"inputs": [{"id": cid, "properties": {"category": cat}} for cid, cat in chunk]}
        try:
            r = req.post("https://api.hubapi.com/crm/v3/objects/companies/batch/update",
                         headers=headers, json=payload, timeout=30)
            if r.ok:
                ok += len(chunk)
                # Refresh cache entries so in-memory map reflects the write
                for cid, cat in chunk:
                    if cid in _hs_cache["id_map"]:
                        _hs_cache["id_map"][cid]["category"] = cat
            else:
                fail += len(chunk)
                if log_fn: log_fn(f"HubSpot writeback {r.status_code}: {r.text[:200]}")
        except Exception as e:
            fail += len(chunk)
            if log_fn: log_fn(f"HubSpot writeback error: {e}")
    return ok, fail

def format_relationship(entry):
    if not entry: return ""
    lc = entry.get("lifecycle", "")
    ls = entry.get("leadStatus", "")
    if lc == "customer": return "Customer"
    if lc == "opportunity": return "Opportunity"
    if ls in ("in_progress", "in progress"): return "In Progress"
    if ls == "open": return "Open Lead"
    if lc == "lead": return "Lead"
    if lc == "subscriber": return "Subscriber"
    if lc == "marketingqualifiedlead": return "MQL"
    if lc == "salesqualifiedlead": return "SQL"
    if lc == "evangelist": return "Evangelist"
    if lc or ls: return (lc or ls).replace("_", " ").title()
    return "Known" if entry.get("ownerName") else ""

# ─── HubSpot Cache (fetch once at startup, refresh every 30 min) ───
_hs_cache = {"map": {}, "id_map": {}, "domain_map": {}, "ts": 0}
def get_hubspot_cached():
    if time.time() - _hs_cache["ts"] > 1800:  # 30 min
        m, im, dm = fetch_hubspot_data()
        _hs_cache["map"] = m
        _hs_cache["id_map"] = im
        _hs_cache["domain_map"] = dm
        _hs_cache["ts"] = time.time()
    return _hs_cache["map"], _hs_cache["id_map"], _hs_cache["domain_map"]

# ─── Apollo Enrichment ───
def apollo_match(row, domain_cache):
    if not APOLLO_API_KEY: return
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    email = row.get("Email", "").strip()
    first = row.get("First Name", "").strip()
    last = row.get("Last Name", "").strip()
    company = row.get("Company", "").strip()
    norm_co = normalize_company(company)

    body = {"reveal_personal_emails": True}
    if email and "@" in email: body["email"] = email
    if first: body["first_name"] = first
    if last: body["last_name"] = last
    if company: body["organization_name"] = company
    if norm_co in domain_cache: body["domain"] = domain_cache[norm_co]

    if len(body) <= 1: return  # only reveal_personal_emails

    try:
        r = req.post("https://api.apollo.io/v1/people/match", headers=headers, json=body, timeout=10)
        if r.ok:
            person = r.json().get("person")
            if person:
                if person.get("email"): row["_email"] = person["email"]
                if person.get("phone_numbers") and person["phone_numbers"]:
                    row["_phone"] = person["phone_numbers"][0].get("sanitized_number", "")
                if person.get("linkedin_url"): row["_linkedin"] = person["linkedin_url"]
                if person.get("title"): row["_apollo_title"] = person["title"]
                org = person.get("organization", {})
                if org.get("primary_domain") and norm_co:
                    domain_cache[norm_co] = org["primary_domain"]
                # Employment history → role_started + tenure_months (for personalization hook)
                emp = person.get("employment_history") or []
                # Current role = first entry where end_date is null/missing
                current = next((e for e in emp if not e.get("end_date")), emp[0] if emp else None)
                if current and current.get("start_date"):
                    sd = current["start_date"]  # "YYYY-MM" or "YYYY-MM-DD"
                    row["_role_started"] = sd
                    # Compute tenure in months from start_date to today
                    try:
                        from datetime import datetime
                        parts = sd.split("-")
                        y, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 1
                        now = datetime.utcnow()
                        months = (now.year - y) * 12 + (now.month - m)
                        row["_tenure_months"] = max(0, months)
                    except Exception:
                        pass
    except: pass
    time.sleep(0.1)

# ─── Apollo: company news enrichment (cached per domain) ───
def apollo_org_news(domain, cache):
    """Return a short list of recent news headlines for a company by domain.
    Uses Apollo's news_articles/search endpoint. Cache by domain to save credits."""
    if not APOLLO_API_KEY or not domain: return []
    if domain in cache: return cache[domain]
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY, "Cache-Control": "no-cache"}
    try:
        r = req.post(
            "https://api.apollo.io/v1/news_articles/search",
            headers=headers,
            json={"q_organization_domains": domain, "page": 1, "per_page": 5},
            timeout=10,
        )
        items = []
        if r.ok:
            for n in (r.json().get("news_articles") or [])[:3]:
                title = (n.get("title") or "").strip()
                pub = (n.get("publication_date") or "")[:10]
                if title:
                    items.append(f"{title} ({pub})" if pub else title)
        cache[domain] = items
        time.sleep(0.1)
        return items
    except Exception:
        cache[domain] = []
        return []

# ─── Claude: single-sentence personalization hook ───
def claude_generate_hook(row, news_items):
    """Generate one short personalization sentence for a contact given Apollo signals.
    Returns empty string if no meaningful signal or if model output is a refusal."""
    if not ANTHROPIC_API_KEY: return ""
    tenure = row.get("_tenure_months")
    title = row.get("Job Title", "") or row.get("_apollo_title", "")
    company = row.get("Company", "")

    recent_role = tenure is not None and tenure <= 12
    has_news = bool(news_items)
    if not recent_role and not has_news: return ""

    ctx = []
    if title and company: ctx.append(f"Role: {title} at {company}")
    if recent_role:
        ctx.append(f"SIGNAL: Started this role {tenure} months ago. This IS the hook — under 12 months is significant.")
    if has_news:
        ctx.append("Recent company news:\n- " + "\n- ".join(news_items[:3]))

    prompt = (
        "Write ONE sentence for the opening of a cold email. Reference the signal.\n\n"
        "RULES:\n"
        "- Output ONLY the sentence. No preamble, no apology, no explanation.\n"
        "- Under 20 words. Conversational, not salesy.\n"
        "- No em-dashes or semicolons. No specific dates (say 'three months in', not '2025-11-01').\n"
        "- Do NOT narrate generic tenure like 'X years at Y'.\n\n"
        "GOOD EXAMPLES:\n"
        "- Stepping into the CIO seat at Westfield in your first few months is a lot on your plate.\n"
        "- Three months into the Chief Underwriting role is about when the ops gaps start to scream.\n"
        "- Congrats on the Series B — the growth is going to push submission volume hard.\n\n"
        "BAD (do not do this):\n"
        "- I don't have enough information...\n"
        "- Without concrete details I cannot...\n\n"
        "SIGNALS:\n"
        + "\n".join(ctx)
        + "\n\nOutput the sentence only:"
    )

    try:
        r = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 100, "messages": [{"role": "user", "content": prompt}]},
            timeout=15,
        )
        if not r.ok: return ""
        text = (r.json().get("content", [{}])[0].get("text", "") or "").strip()
        text = re.sub(r'^["\']|["\']$', '', text).strip()
        lower = text.lower()
        refusal_markers = [
            "i don't", "i do not", "i cannot", "i can't", "i'm unable",
            "i have no", "i lack", "without concrete", "without verified",
            "without specific", "returning empty", "cannot write", "cannot create",
            "cannot provide", "not enough information", "insufficient information",
            "i need more", "based on the information", "here is", "here's",
        ]
        if any(m in lower for m in refusal_markers): return ""
        if len(text) < 12 or len(text.split()) < 4 or len(text.split()) > 35: return ""
        return text
    except Exception:
        return ""
    return ""

# ─── Push to FurtherAI outbound pipeline ───
def push_to_furtherai(data, conference_name):
    """POST enriched contacts to FurtherAI /api/schedule?conference_import=1.
    Returns (ok, response_json)."""
    if not FURTHERAI_CRON_SECRET:
        return False, {"error": "FURTHERAI_CRON_SECRET not configured"}
    INSURANCE_TYPES = {"Insurance Carrier", "MGA / Specialty Underwriter", "Insurance Broker",
                        "TPA / Claims Admin", "Reinsurer", "Captive / Risk Finance"}
    contacts = []
    for r in data:
        email = (r.get("_email") or r.get("Email") or "").strip().lower()
        if not email or "@" not in email: continue
        if r.get("Outreach Priority") == "4 - Do Not Contact": continue  # DNC
        if r.get("Company Type", "") not in INSURANCE_TYPES: continue  # non-insurance filter
        sig = {}
        if r.get("_suggested_hook"): sig["suggested_hook"] = r["_suggested_hook"]
        if r.get("_role_started"): sig["role_started"] = r["_role_started"]
        if r.get("_tenure_months") is not None: sig["tenure_months"] = r["_tenure_months"]
        if r.get("_recent_news"): sig["recent_news"] = r["_recent_news"]
        contacts.append({
            "email": email,
            "first_name": r.get("First Name", ""),
            "last_name": r.get("Last Name", ""),
            "company": r.get("Company", ""),
            "title": r.get("Job Title", "") or r.get("_apollo_title", ""),
            "apollo_signals": sig,
            "outreach_angle": r.get("Outreach Angle", ""),
            "priority": r.get("Outreach Priority", ""),
            "pod_owner": r.get("Who", ""),
        })
    if not contacts:
        return False, {"error": "no contacts with email + not-DNC"}
    try:
        r = req.post(
            FURTHERAI_PIPELINE_URL,
            headers={"Authorization": f"Bearer {FURTHERAI_CRON_SECRET}", "Content-Type": "application/json"},
            json={"conference_name": conference_name, "contacts": contacts},
            timeout=90,
        )
        return r.ok, (r.json() if r.ok else {"error": r.text[:500]})
    except Exception as e:
        return False, {"error": str(e)[:200]}

# ─── Claude Classification ───
COMPANY_TYPES = ["Insurance Carrier", "MGA / Specialty Underwriter", "Insurance Broker",
    "TPA / Claims Admin", "Risk Consulting / Advisory", "InsurTech / Technology",
    "Reinsurer", "Captive / Risk Finance", "Corporate / End-User", "Academic / Association"]

def claude_classify_batch(batch, data):
    if not ANTHROPIC_API_KEY: return
    lines = "\n".join(f'{i+1}. Company: "{r.get("Company","")}" | Title: "{r.get("Job Title","")}"' for i, r in enumerate(batch))
    prompt = f'You are classifying companies for an insurance industry CRM. For each numbered entry, classify the Company Type as EXACTLY one of these values:\n- Insurance Carrier\n- MGA / Specialty Underwriter\n- Insurance Broker\n- TPA / Claims Admin\n- Risk Consulting / Advisory\n- InsurTech / Technology\n- Reinsurer\n- Captive / Risk Finance\n- Corporate / End-User\n- Academic / Association\n\nRules:\n- Insurance companies that write policies = Insurance Carrier\n- Managing General Agents, wholesalers, program administrators that underwrite = MGA / Specialty Underwriter\n- Brokerages that place risk = Insurance Broker\n- Third-party administrators, claims servicers = TPA / Claims Admin\n- Law firms, consultancies, risk advisors = Risk Consulting / Advisory\n- Software/tech vendors serving insurance = InsurTech / Technology\n- Reinsurance companies = Reinsurer\n- Non-insurance companies = Corporate / End-User\n- Universities, associations, foundations = Academic / Association\n\nReply with ONLY numbered lines:\n1. Company Type\n\nCompanies:\n{lines}'

    try:
        r = req.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 4096, "messages": [{"role": "user", "content": prompt}]},
            timeout=30)
        if r.ok:
            text = r.json().get("content", [{}])[0].get("text", "")
            for line in text.split("\n"):
                m = re.match(r'^(\d+)\.\s*(.+)', line)
                if m:
                    idx = int(m.group(1)) - 1
                    if 0 <= idx < len(batch):
                        ct = m.group(2).strip()
                        matched = next((t for t in COMPANY_TYPES if t.lower() in ct.lower()), ct)
                        batch[idx]["Company Type"] = matched
    except: pass

# ─── Known Insurance Companies ───
KNOWN_COMPANIES = [
    (re.compile(r'\bcna\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bbeazley\b', re.I), "Insurance Carrier"),
    (re.compile(r'\baxa\s*xl\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bhanover\b.*\b(insurance|group)\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bintact\b.*\b(financial|insurance)\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bfederated\s*mutual\b', re.I), "Insurance Carrier"),
    (re.compile(r'\binigo\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bvantage\s*risk\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bqbe\b', re.I), "Insurance Carrier"),
    (re.compile(r'\ballied\s*world\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bfalvey\b.*insurance', re.I), "Insurance Carrier"),
    (re.compile(r'\bcanopius\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bliberty\s*mutual\b', re.I), "Insurance Carrier"),
    (re.compile(r'\btravelers\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bchubb\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bzurich\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bnationwide\b.*\b(mutual|insurance)\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bmarkel\b', re.I), "Insurance Carrier"),
    (re.compile(r'\baxis\b.*\b(capital|insurance)\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bhiscox\b', re.I), "Insurance Carrier"),
    (re.compile(r'\ballianz\b', re.I), "Insurance Carrier"),
    (re.compile(r'\baig\b', re.I), "Insurance Carrier"),
    (re.compile(r'\btokio\s*marine\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bsompo\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bw\.?r\.?\s*berkley\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bsiriuspoint\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bscor\b', re.I), "Insurance Carrier"),
    (re.compile(r'\beverest\b.*\b(re|group)\b', re.I), "Insurance Carrier"),
    (re.compile(r'\blancashire\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bfidelis\b', re.I), "Insurance Carrier"),
    (re.compile(r'\berie\s*insurance\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bselective\s*insurance\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bgreat\s*american\b.*insurance', re.I), "Insurance Carrier"),
    (re.compile(r'\busaig\b', re.I), "Insurance Carrier"),
    (re.compile(r'\bargo\b.*group', re.I), "Insurance Carrier"),
    (re.compile(r'\baspen\b.*\b(insurance|re)\b', re.I), "Insurance Carrier"),
    # Brokers
    (re.compile(r'\bgallagher\b(?!.*bassett)', re.I), "Insurance Broker"),
    (re.compile(r'\bgallagher\s*bassett\b', re.I), "TPA / Claims Admin"),
    (re.compile(r'\balliant\b.*insurance', re.I), "Insurance Broker"),
    (re.compile(r'\bnfp\b', re.I), "Insurance Broker"),
    (re.compile(r'\brisk\s*placement\s*services\b', re.I), "Insurance Broker"),
    (re.compile(r'\blockton\b', re.I), "Insurance Broker"),
    (re.compile(r'\bbrown\s*&\s*brown\b', re.I), "Insurance Broker"),
    (re.compile(r'\bhub\s*international\b', re.I), "Insurance Broker"),
    (re.compile(r'\busi\b.*\b(insurance|services)\b', re.I), "Insurance Broker"),
    (re.compile(r'\bacrisure\b', re.I), "Insurance Broker"),
    (re.compile(r'\bassuredpartners\b', re.I), "Insurance Broker"),
    (re.compile(r'\bmcgriff\b', re.I), "Insurance Broker"),
    (re.compile(r'\balera\s*group\b', re.I), "Insurance Broker"),
    (re.compile(r'\bhigginbotham\b', re.I), "Insurance Broker"),
    (re.compile(r'\bholmes\s*murphy\b', re.I), "Insurance Broker"),
    # MGA
    (re.compile(r'\bambridge\b', re.I), "MGA / Specialty Underwriter"),
    (re.compile(r'\bryan\s*specialty\b', re.I), "MGA / Specialty Underwriter"),
    (re.compile(r'\bcrc\s*group\b', re.I), "MGA / Specialty Underwriter"),
    (re.compile(r'\bamwins\b', re.I), "MGA / Specialty Underwriter"),
    (re.compile(r'\bburns\s*&\s*wilcox\b', re.I), "MGA / Specialty Underwriter"),
    (re.compile(r'\bworldwide\s*facilities\b', re.I), "MGA / Specialty Underwriter"),
    # TPA
    (re.compile(r'\bmclarens\b', re.I), "TPA / Claims Admin"),
    (re.compile(r'\bsedgwick\b', re.I), "TPA / Claims Admin"),
    (re.compile(r'\bcrawford\b.*\b(company|claims)\b', re.I), "TPA / Claims Admin"),
    (re.compile(r'\bbroadspire\b', re.I), "TPA / Claims Admin"),
    # Reinsurers
    (re.compile(r'\bmunich\s*re\b', re.I), "Reinsurer"),
    (re.compile(r'\bswiss\s*re\b', re.I), "Reinsurer"),
    (re.compile(r'\bgen\s*re\b', re.I), "Reinsurer"),
    (re.compile(r'\brennaissancere\b', re.I), "Reinsurer"),
    (re.compile(r'\bpartnerre\b', re.I), "Reinsurer"),
    (re.compile(r'\btransre\b', re.I), "Reinsurer"),
    (re.compile(r'\bhannover\s*re\b', re.I), "Reinsurer"),
]

def classify_known(company):
    if not company: return None
    for pat, ctype in KNOWN_COMPANIES:
        if pat.search(company): return ctype
    return None

# ─── ICP Logic ───
CORE = {"Insurance Carrier", "MGA / Specialty Underwriter", "Reinsurer"}
WARM = {"Insurance Broker", "TPA / Claims Admin", "InsurTech / Technology", "Risk Consulting / Advisory"}
SENIOR = {"C-Suite / Founder", "EVP / SVP", "VP / AVP", "Director / Head"}
# Company Types that may carry Priority 1 or 2. Everything else caps at 3.
# Carrier / MGA / Broker / Reinsurer are ICP buyers for FurtherAI.
ICP_FOR_PRIORITY = {"Insurance Carrier", "MGA / Specialty Underwriter", "Insurance Broker", "Reinsurer"}

# Map HubSpot `category` property (set by hubspot-cleanup) → Conference Buddy Company Type.
HS_CATEGORY_TO_TYPE = {
    "Carrier": "Insurance Carrier",
    "MGA/MGU": "MGA / Specialty Underwriter",
    "Brokerage": "Insurance Broker",
    "Reinsurer": "Reinsurer",
    "Insurtech": "InsurTech / Technology",
}
# Reverse map — for writing Conference Buddy classifications back to HubSpot's `category` property.
TYPE_TO_HS_CATEGORY = {v: k for k, v in HS_CATEGORY_TO_TYPE.items()}

def detect_seniority(title):
    t = (title or "").lower()
    if re.search(r'\b(evp|svp)\b', t) or "executive vice president" in t or "senior vice president" in t: return "EVP / SVP"
    if re.search(r'vice president', t) or re.search(r'\b(vp|avp)\b', t): return "VP / AVP"
    if re.search(r'\b(ceo|cfo|cto|cio|coo|cro|ciso|cmo|cco)\b', t) or re.search(r'\bchief\s', t) or re.search(r'\bpresident\b', t) or re.search(r'\bfounder\b', t): return "C-Suite / Founder"
    if re.search(r'\b(director|head of|managing director|partner|principal|practice leader)\b', t): return "Director / Head"
    if re.search(r'\b(manager|counsel|attorney|treasurer|controller|supervisor|risk manager|general manager)\b', t): return "Manager / Counsel"
    if re.search(r'\b(analyst|specialist|coordinator|associate|adjuster|underwriter|account executive|producer)\b', t): return "Analyst / Specialist"
    return "Other"

def compute_icp(company_type, seniority, title, status, hs_tier=""):
    # HubSpot-level hard override: company was already classified Disqualified by the CRM cleanup.
    if (hs_tier or "").lower() == "disqualified":
        return "None", "4 - Do Not Contact", "HubSpot: Disqualified company", ""

    fit, prio, reason, angle = _compute_icp_raw(company_type, seniority, title, status)
    # ICP filter: non-ICP Company Types cannot hold Priority 1 or 2.
    if company_type not in ICP_FOR_PRIORITY and prio in ("1 - Priority", "2 - Warm"):
        if fit in ("High", "Medium"):
            fit = "Low"
        reason = f"{reason} (capped: non-ICP Company Type)"
        prio = "3 - Okay"
    return fit, prio, reason, angle

def _compute_icp_raw(company_type, seniority, title, status):
    t = (title or "").lower()
    if status == "Customer":
        return "None", "4 - Do Not Contact", "Existing customer — do not contact", ""

    # Tier 1
    if company_type in CORE and seniority in SENIOR:
        angle = "Show end-to-end insurance ops AI platform — 30x faster submissions"
        if re.search(r'claim', t): angle = "Show claims FNOL & document intake automation"
        elif re.search(r'underwrit', t): angle = "Show submission automation & underwriting triage use case"
        elif re.search(r'distribut|marketing|growth|brand', t): angle = "Discuss how FurtherAI accelerates bind cycle & distribution ops"
        elif re.search(r'risk|compliance|audit|erm', t): angle = "Frame around underwriting audit & compliance automation"
        elif re.search(r'digital|innovat|ai\b|tech', t): angle = "Lead with operational efficiency & AI adoption ROI (646% case study)"
        return "High", "1 - Priority", f"{company_type} — senior decision maker", angle

    # Tier 2
    if company_type in CORE and seniority == "Manager / Counsel":
        angle = "Frame around underwriting audit & compliance automation"
        if re.search(r'underwrit', t): angle = "Show submission automation & underwriting triage use case"
        elif re.search(r'claim', t): angle = "Show claims FNOL & document intake automation"
        return "Medium", "2 - Warm", f"{company_type} — mid-level ({seniority})", angle
    if company_type in WARM and seniority in SENIOR:
        amap = {
            "Insurance Broker": ("Show how FurtherAI speeds up submission placement & quote readiness", f"Broker — {seniority}"),
            "TPA / Claims Admin": ("Focus on claims document structuring & FNOL automation", "TPA — document/claims processing overlap"),
            "InsurTech / Technology": ("Explore integration or co-sell with existing platforms", "InsurTech — partner/channel potential"),
            "Risk Consulting / Advisory": ("Explore referral/reseller partnership for carrier clients", "Consultant — influencer/partner potential"),
        }
        angle, reason = amap.get(company_type, ("", company_type))
        return "Medium", "2 - Warm", reason, angle

    # Tier 3
    if company_type in CORE:
        angle = "Frame around underwriting audit & compliance automation"
        if re.search(r'underwrit', t): angle = "Show submission automation & underwriting triage use case"
        elif re.search(r'claim', t): angle = "Show claims FNOL & document intake automation"
        return "Low", "3 - Okay", f"{company_type} — {seniority or 'low seniority'}", angle
    if company_type in WARM and seniority == "Manager / Counsel":
        amap = {"Insurance Broker": "Show how FurtherAI speeds up submission placement & quote readiness",
                "TPA / Claims Admin": "Focus on claims document structuring & FNOL automation"}
        return "Low", "3 - Okay", f"{company_type} — {seniority}", amap.get(company_type, "")
    if company_type not in CORE and company_type not in WARM and seniority in SENIOR:
        angle = "Introduce FurtherAI's insurance ops AI platform — explore relevance to their business"
        if re.search(r'risk|insurance|compliance|audit', t): angle = "Frame around underwriting audit & compliance automation"
        elif re.search(r'claim', t): angle = "Show claims FNOL & document intake automation"
        elif re.search(r'digital|innovat|ai\b|tech', t): angle = "Lead with operational efficiency & AI adoption ROI (646% case study)"
        elif company_type == "Corporate / End-User": angle = "Discuss how FurtherAI automates insurance document processing — relevant if they manage risk programs"
        elif company_type == "Academic / Association": angle = "Explore partnership or speaking opportunities — FurtherAI's AI-driven insurance ops"
        return "Low", "3 - Okay", f"{company_type or 'Unknown'} — senior but outside core ICP", angle

    # Tier 4
    reason = f"{company_type or 'Unknown'} — outside ICP"
    if company_type == "Academic / Association": reason = "Academic / Association — no fit"
    elif company_type == "Captive / Risk Finance": reason = "Captive / Risk Finance — no fit"
    elif company_type in WARM: reason = f"{company_type} — low seniority"
    return "None", "4 - Do Not Contact", reason, ""

# ─── Google Sheets Push ───
PODS = [
    {"bdr":"Zain","aes":["Nia"],"bg":{"red":0.859,"green":0.890,"blue":0.996},"tx":{"red":0.118,"green":0.251,"blue":0.686}},
    {"bdr":"Dani","aes":["Mike","Gavin"],"bg":{"red":0.863,"green":0.988,"blue":0.906},"tx":{"red":0.086,"green":0.396,"blue":0.204}},
    {"bdr":"Jacob","aes":["Bobby"],"bg":{"red":0.996,"green":0.976,"blue":0.765},"tx":{"red":0.522,"green":0.302,"blue":0.055}},
]
BDR_SET = {p["bdr"].lower() for p in PODS}
# All pod member names (BDR + AEs) that map to a pod BDR
POD_MEMBER_MAP = {}
for p in PODS:
    POD_MEMBER_MAP[p["bdr"].lower()] = p["bdr"]
    for ae in p.get("aes", []):
        POD_MEMBER_MAP[ae.lower()] = p["bdr"]

def bdr_color(name):
    for p in PODS:
        if p["bdr"].lower() == (name or "").strip().lower(): return p
    return None

C = {
    "hdr_bg":{"red":0.1,"green":0.12,"blue":0.18},"hdr_tx":{"red":1,"green":1,"blue":1},
    "ban_bg":{"red":0.043,"green":0.122,"blue":0.078},"ban_tx":{"red":1,"green":1,"blue":1},
    "p_bg":{"red":0.863,"green":0.988,"blue":0.906},"p_tx":{"red":0.086,"green":0.396,"blue":0.204},
    "w_bg":{"red":0.996,"green":0.976,"blue":0.765},"w_tx":{"red":0.522,"green":0.302,"blue":0.055},
    "o_bg":{"red":0.859,"green":0.890,"blue":0.996},"o_tx":{"red":0.118,"green":0.251,"blue":0.686},
    "d_bg":{"red":0.996,"green":0.886,"blue":0.886},"d_tx":{"red":0.6,"green":0.106,"blue":0.106},
    "cust_bg":{"red":0.859,"green":0.890,"blue":0.996},"cust_tx":{"red":0.118,"green":0.251,"blue":0.686},
    "opp_bg":{"red":0.953,"green":0.910,"blue":1.0},"opp_tx":{"red":0.420,"green":0.129,"blue":0.659},
    "lead_bg":{"red":1.0,"green":0.929,"blue":0.835},"lead_tx":{"red":0.604,"green":0.204,"blue":0.071},
    "new_bg":{"red":0.945,"green":0.961,"blue":0.973},"new_tx":{"red":0.392,"green":0.455,"blue":0.545},
    "nohs_bg":{"red":0.945,"green":0.961,"blue":0.973},"nohs_tx":{"red":0.392,"green":0.455,"blue":0.545},
}
PRIO_C = {"1 - Priority":("p_bg","p_tx"),"2 - Warm":("w_bg","w_tx"),"3 - Okay":("o_bg","o_tx"),"4 - Do Not Contact":("d_bg","d_tx")}
FIT_C = {"High":("p_bg","p_tx"),"Medium":("w_bg","w_tx"),"Low":("o_bg","o_tx"),"None":("d_bg","d_tx")}
STATUS_C = {"Customer":("cust_bg","cust_tx"),"Opportunity":("opp_bg","opp_tx"),"In Progress":("w_bg","w_tx"),
    "Lead":("lead_bg","lead_tx"),"Open Lead":("lead_bg","lead_tx"),"SQL":("opp_bg","opp_tx"),
    "MQL":("d_bg","d_tx"),"New":("new_bg","new_tx"),"Known":("nohs_bg","nohs_tx"),
    "Subscriber":("nohs_bg","nohs_tx"),"Not in HubSpot":("nohs_bg","nohs_tx")}
COL_W = {"First Name":90,"Last Name":100,"Company":180,"Job Title":200,"Email":200,"Phone":110,
    "Company Type":150,"Seniority":120,"Outreach Priority":120,"ICP Fit":70,"ICP Reason":180,
    "Outreach Angle":220,"HubSpot Status":100,"HubSpot Owner":110,"Assigned To":80,"Who":80}

def get_gsheets_client():
    with open(SHEETS_TOKEN_PATH) as f:
        tok = json.load(f)
    creds = Credentials(token=tok['token'], refresh_token=tok['refresh_token'],
        token_uri=tok['token_uri'], client_id=tok['client_id'],
        client_secret=tok['client_secret'], scopes=tok['scopes'])
    if creds.expired:
        creds.refresh(Request())
        # Save refreshed token
        tok['token'] = creds.token
        with open(SHEETS_TOKEN_PATH, 'w') as f:
            json.dump(tok, f)
    return gspread.authorize(creds), creds

def push_to_sheets(data, headers, sheet_name):
    gc, creds = get_gsheets_client()
    who_key = "Assigned To" if "Assigned To" in headers else ("Who" if "Who" in headers else None)

    pod_alloc, round_robin, dnc = [], [], []
    for r in data:
        prio = r.get("Outreach Priority",""); status = r.get("HubSpot Status","").strip()
        if prio == "4 - Do Not Contact" or status == "Customer": dnc.append(r)
        elif r.get("_is_pod_owned"): pod_alloc.append(r)
        else: round_robin.append(r)

    tier_ord = {"1 - Priority":0,"2 - Warm":1,"3 - Okay":2,"4 - Do Not Contact":3}
    def sk(r):
        w = (r.get(who_key,"") if who_key else "").strip().lower()
        wr = 0 if w=="zain" else 1 if w=="dani" else 2 if w=="jacob" else (10 if w else 50)
        return (wr, tier_ord.get(r.get("Outreach Priority",""), 99))
    pod_alloc.sort(key=sk); round_robin.sort(key=sk)

    ncols = len(headers)
    final = [headers]
    banners = set()

    banners.add(len(final))
    final.append([f"POD ALLOCATION ({len(pod_alloc)})"] + [""]*(ncols-1))
    for r in pod_alloc: final.append([r.get(h,"") for h in headers])
    final.append([""]*ncols)
    banners.add(len(final))
    final.append([f"ROUND ROBIN ({len(round_robin)})"] + [""]*(ncols-1))
    for r in round_robin: final.append([r.get(h,"") for h in headers])
    final.append([""]*ncols)
    banners.add(len(final))
    final.append([f"DO NOT CONTACT ({len(dnc)})"] + [""]*(ncols-1))
    for r in dnc: final.append([r.get(h,"") for h in headers])

    nrows = len(final)
    sh = gc.create(sheet_name)
    ws = sh.sheet1
    ws.update_title("Conference Enriched")
    if ws.row_count < nrows + 5: ws.resize(rows=nrows+5, cols=ncols)
    ws.update(final, value_input_option='RAW')

    sid = ws.id
    pi = headers.index("Outreach Priority") if "Outreach Priority" in headers else -1
    fi = headers.index("ICP Fit") if "ICP Fit" in headers else -1
    wi = headers.index(who_key) if who_key and who_key in headers else -1
    si = headers.index("HubSpot Status") if "HubSpot Status" in headers else -1
    oi = headers.index("HubSpot Owner") if "HubSpot Owner" in headers else -1

    reqs = []
    reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":0,"endRowIndex":1,"startColumnIndex":0,"endColumnIndex":ncols},"cell":{"userEnteredFormat":{"backgroundColor":C["hdr_bg"],"textFormat":{"foregroundColor":C["hdr_tx"],"bold":True,"fontSize":10},"horizontalAlignment":"CENTER"}},"fields":"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"}})
    for bi in sorted(banners):
        reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":bi,"endRowIndex":bi+1,"startColumnIndex":0,"endColumnIndex":ncols},"cell":{"userEnteredFormat":{"backgroundColor":C["ban_bg"],"textFormat":{"foregroundColor":C["ban_tx"],"bold":True,"fontSize":12}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})
        reqs.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":bi,"endRowIndex":bi+1,"startColumnIndex":0,"endColumnIndex":ncols},"mergeType":"MERGE_ALL"}})

    for ri in range(nrows):
        if ri == 0 or ri in banners: continue
        row = final[ri]
        if all(not c for c in row): continue
        if pi>=0 and row[pi] in PRIO_C:
            bg,tx=PRIO_C[row[pi]]; reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":ri,"endRowIndex":ri+1,"startColumnIndex":pi,"endColumnIndex":pi+1},"cell":{"userEnteredFormat":{"backgroundColor":C[bg],"textFormat":{"foregroundColor":C[tx],"bold":True}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})
        if fi>=0 and row[fi] in FIT_C:
            bg,tx=FIT_C[row[fi]]; reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":ri,"endRowIndex":ri+1,"startColumnIndex":fi,"endColumnIndex":fi+1},"cell":{"userEnteredFormat":{"backgroundColor":C[bg],"textFormat":{"foregroundColor":C[tx]}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})
        if wi>=0:
            pod=bdr_color(row[wi])
            if pod: reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":ri,"endRowIndex":ri+1,"startColumnIndex":wi,"endColumnIndex":wi+1},"cell":{"userEnteredFormat":{"backgroundColor":pod["bg"],"textFormat":{"foregroundColor":pod["tx"],"bold":True}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})
        if si>=0 and row[si] in STATUS_C:
            bg,tx=STATUS_C[row[si]]; reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":ri,"endRowIndex":ri+1,"startColumnIndex":si,"endColumnIndex":si+1},"cell":{"userEnteredFormat":{"backgroundColor":C[bg],"textFormat":{"foregroundColor":C[tx],"bold":True}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})
        if oi>=0 and row[oi]=="Not in HubSpot":
            reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":ri,"endRowIndex":ri+1,"startColumnIndex":oi,"endColumnIndex":oi+1},"cell":{"userEnteredFormat":{"backgroundColor":C["nohs_bg"],"textFormat":{"foregroundColor":C["nohs_tx"]}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}})

    reqs.append({"updateSheetProperties":{"properties":{"sheetId":sid,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}})
    for ci,h in enumerate(headers):
        reqs.append({"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":ci,"endIndex":ci+1},"properties":{"pixelSize":COL_W.get(h,120)},"fields":"pixelSize"}})

    for i in range(0, len(reqs), 500):
        sh.batch_update({"requests": reqs[i:i+500]})

    # Move to shared Drive folder
    file_id = sh.id
    drive_headers = {"Authorization": f"Bearer {creds.token}"}
    r = req.get(f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=parents&supportsAllDrives=true", headers=drive_headers)
    old_parents = ",".join(r.json().get("parents", []))
    req.patch(f"https://www.googleapis.com/drive/v3/files/{file_id}?addParents={DRIVE_FOLDER_ID}&removeParents={old_parents}&supportsAllDrives=true", headers=drive_headers)

    return sh.url, len(pod_alloc), len(round_robin), len(dnc)

# ─── Process CSV (full pipeline — same as localhost app) ───
def process_csv(csv_text, filename, progress_fn=None):
    reader = csv.DictReader(io.StringIO(csv_text))
    orig_headers = list(reader.fieldnames)
    data = list(reader)
    total = len(data)

    def log(msg):
        if progress_fn: progress_fn(msg)

    # ── Step 1: HubSpot lookup (cached) ──
    log(f"Loading HubSpot data...")
    hs_map, hs_id_map, hs_domain_map = get_hubspot_cached()
    log(f"HubSpot: {len(hs_map)} companies ({len(hs_domain_map)} with domain)")

    # ── Step 2: Apollo enrichment (skip rows that already have email + phone) ──
    domain_cache = {}
    email_col = next((h for h in orig_headers if "email" in h.lower()), None)
    phone_col = next((h for h in orig_headers if "phone" in h.lower()), None)
    if APOLLO_API_KEY:
        needs_apollo = [r for r in data if not (email_col and r.get(email_col, "").strip() and "@" in r.get(email_col, "") and phone_col and r.get(phone_col, "").strip())]
        log(f"Apollo: enriching {len(needs_apollo)}/{total} contacts (skipping {total - len(needs_apollo)} with email+phone)...")
        for i, r in enumerate(needs_apollo):
            apollo_match(r, domain_cache)
            if (i+1) % 50 == 0: log(f"Apollo: {i+1}/{len(needs_apollo)}")
        log(f"Apollo enrichment complete")

    # ── Step 3: HubSpot match per row ──
    log("Matching contacts to HubSpot...")
    bdr_names = [p["bdr"] for p in PODS]
    unassigned_idx = 0
    for r in data:
        company = r.get("Company", "")
        norm_co = normalize_company(company)
        domain = domain_cache.get(norm_co, "") if APOLLO_API_KEY else ""
        hs_entry = lookup_hubspot(company, hs_map, hs_id_map, hs_domain_map, domain)
        if hs_entry:
            hs_entry = resolve_with_inheritance(hs_entry, hs_id_map)
        r["HubSpot Status"] = format_relationship(hs_entry)
        r["HubSpot Owner"] = hs_entry.get("ownerName", "") if hs_entry else ""
        r["_hs_icp_tier"] = hs_entry.get("icpTier", "") if hs_entry else ""
        r["_hs_category"] = hs_entry.get("category", "") if hs_entry else ""
        r["_hs_id"] = hs_entry.get("id", "") if hs_entry else ""

        # Pod assignment from HubSpot owner
        owner = r["HubSpot Owner"]
        pod_bdr = None
        is_pod_owned = False
        if owner:
            # Check each word in owner name against pod members
            for word in owner.lower().split():
                if word in POD_MEMBER_MAP:
                    pod_bdr = POD_MEMBER_MAP[word]
                    is_pod_owned = True
                    break

        # If no pod from HubSpot, check existing "Who" column
        who_key = "Who" if "Who" in orig_headers else ("Assigned To" if "Assigned To" in orig_headers else None)
        existing_who = r.get(who_key, "").strip() if who_key else ""
        if not pod_bdr:
            for word in existing_who.lower().split():
                if word in POD_MEMBER_MAP:
                    pod_bdr = POD_MEMBER_MAP[word]
                    is_pod_owned = True
                    break

        # Non-pod HubSpot owners (Aman, Logan, Kush, etc.) → round robin
        if not pod_bdr:
            pod_bdr = bdr_names[unassigned_idx % len(bdr_names)]
            unassigned_idx += 1
            is_pod_owned = False

        r["Who"] = pod_bdr
        r["_is_pod_owned"] = is_pod_owned

        # Fill email from Apollo if original is empty
        orig_email_col = next((h for h in orig_headers if "email" in h.lower()), None)
        if orig_email_col and not r.get(orig_email_col, "").strip() and r.get("_email"):
            r[orig_email_col] = r["_email"]
        # Fill phone
        orig_phone_col = next((h for h in orig_headers if "phone" in h.lower()), None)
        if orig_phone_col and not r.get(orig_phone_col, "").strip() and r.get("_phone"):
            r[orig_phone_col] = r["_phone"]

    # ── Step 4: Company Type classification ──
    # First: map Membership column
    MEMBERSHIP_MAP = {
        "carrier": "Insurance Carrier",
        "program administrator": "MGA / Specialty Underwriter",
        "reinsurer": "Reinsurer",
        "broker": "Insurance Broker",
        "tpa": "TPA / Claims Admin",
        "insurtech": "InsurTech / Technology",
    }
    membership_col = next((h for h in orig_headers if h.lower().strip() == "membership"), None)
    comp_type_col = next((h for h in orig_headers if re.match(r'company.?type', h, re.I)), None)

    for r in data:
        # Membership mapping
        if membership_col and not comp_type_col:
            mem = (r.get(membership_col, "") or "").lower().strip()
            mapped = MEMBERSHIP_MAP.get(mem)
            if mapped: r["Company Type"] = mapped
        # Known company override
        known = classify_known(r.get("Company", ""))
        if known: r["Company Type"] = known
        # HubSpot category override (from cleanup-authored `category` property)
        if not r.get("Company Type", "").strip():
            hs_cat = (r.get("_hs_category", "") or "").strip()
            # Case-insensitive lookup against HS_CATEGORY_TO_TYPE
            for src_cat, dst_type in HS_CATEGORY_TO_TYPE.items():
                if hs_cat.lower() == src_cat.lower():
                    r["Company Type"] = dst_type
                    break

    # Claude classification for rows still missing company type
    needs_claude = [r for r in data if not r.get("Company Type", "").strip()]
    if needs_claude and ANTHROPIC_API_KEY:
        log(f"Classifying {len(needs_claude)} companies via Claude...")
        BATCH = 30
        for b in range(0, len(needs_claude), BATCH):
            batch = needs_claude[b:b+BATCH]
            claude_classify_batch(batch, data)
            if b + BATCH < len(needs_claude): log(f"Claude: {min(b+BATCH, len(needs_claude))}/{len(needs_claude)}")
        log("Claude classification complete")

    # Default remaining
    for r in data:
        if not r.get("Company Type", "").strip():
            r["Company Type"] = "Corporate / End-User"

    # ── Step 4b: Write classifications back to HubSpot (close the loop) ──
    # Safe because TYPE_TO_HS_CATEGORY only maps the 5 insurance buckets — non-insurance
    # Claude classifications (Corporate, TPA, Academic, etc.) can't produce a writeback target.
    # Skip only Disqualified companies (cleanup already marked them not-insurance).
    writeback = {}
    for r in data:
        hs_id = r.get("_hs_id", "")
        hs_cat = (r.get("_hs_category", "") or "").strip()
        hs_tier = (r.get("_hs_icp_tier", "") or "").lower()
        ct = r.get("Company Type", "")
        hs_target = TYPE_TO_HS_CATEGORY.get(ct)
        if hs_id and not hs_cat and hs_tier != "disqualified" and hs_target:
            writeback[hs_id] = hs_target
    if writeback:
        log(f"HubSpot writeback: {len(writeback)} companies missing category")
        ok, fail = writeback_hubspot_categories(writeback, log)
        log(f"HubSpot writeback: {ok} updated, {fail} failed")

    # ── Step 5: Seniority + ICP ──
    log("Computing ICP tiers...")
    for r in data:
        title = r.get("Job Title", "") or r.get("_apollo_title", "")
        ct = r["Company Type"]
        seniority = r.get("Seniority", "") or detect_seniority(title)
        r["Seniority"] = seniority
        status = r.get("HubSpot Status", "").strip()

        icp_fit, prio, reason, angle = compute_icp(ct, seniority, title, status, r.get("_hs_icp_tier", ""))
        r["ICP Fit"] = icp_fit
        r["Outreach Priority"] = prio
        r["ICP Reason"] = reason
        r["Outreach Angle"] = angle

        # Customers → DNC
        if status == "Customer":
            r["Outreach Priority"] = "4 - Do Not Contact"
            r["ICP Fit"] = "None"
            r["ICP Reason"] = "Existing customer — do not contact"
            r["Outreach Angle"] = ""

    # Fill blanks
    for r in data:
        if not r.get("HubSpot Status", "").strip(): r["HubSpot Status"] = "New"
        if not r.get("HubSpot Owner", "").strip(): r["HubSpot Owner"] = "Not in HubSpot"
        if not r.get("Outreach Angle", "").strip() and r.get("Outreach Priority") != "4 - Do Not Contact":
            r["Outreach Angle"] = "Introduce FurtherAI's insurance ops AI platform — explore relevance to their business"

    # ── Step 5.5: Company news pass (one call per unique domain, cached) ──
    # Only for contacts not flagged DNC and that have a domain from Apollo
    news_cache = {}
    needs_news = [r for r in data if r.get("Outreach Priority") != "4 - Do Not Contact"]
    if APOLLO_API_KEY and needs_news:
        log(f"Apollo news: pulling for {len(set(domain_cache.values()))} unique domains...")
        for r in needs_news:
            norm_co = normalize_company(r.get("Company", ""))
            domain = domain_cache.get(norm_co, "")
            if not domain: continue
            items = apollo_org_news(domain, news_cache)
            if items: r["_recent_news"] = items

    # ── Step 5.6: Suggested hook generation (one Claude Haiku call per contact with a signal) ──
    if ANTHROPIC_API_KEY:
        needs_hook = [r for r in data if r.get("Outreach Priority") != "4 - Do Not Contact"
                      and (r.get("_role_started") or r.get("_recent_news"))]
        log(f"Generating hooks for {len(needs_hook)} contacts...")
        for i, r in enumerate(needs_hook):
            hook = claude_generate_hook(r, r.get("_recent_news", []))
            if hook: r["_suggested_hook"] = hook
            if (i + 1) % 25 == 0: log(f"Hooks: {i+1}/{len(needs_hook)}")

    # Surface hook + tenure + news to visible columns for BDRs
    for r in data:
        if r.get("_suggested_hook"): r["Suggested Hook"] = r["_suggested_hook"]
        if r.get("_role_started"): r["Role Started"] = r["_role_started"]
        if r.get("_tenure_months") is not None: r["Tenure (months)"] = str(r["_tenure_months"])
        if r.get("_recent_news"): r["Recent Company News"] = " | ".join(r["_recent_news"][:2])

    # ── Build output headers ──
    icp_cols = ["Company Type", "Seniority", "Outreach Priority", "ICP Fit", "ICP Reason", "Outreach Angle",
                "Role Started", "Tenure (months)", "Recent Company News", "Suggested Hook",
                "HubSpot Status", "HubSpot Owner", "Who"]
    headers = list(orig_headers)
    for col in icp_cols:
        if col not in headers:
            headers.append(col)

    # Clean up filename → sheet title
    name = filename.replace(".csv", "").replace(".xlsx", "").replace(".xls", "")
    # Remove common suffixes like "- sheetname", "attendee-list", etc.
    name = re.sub(r'\s*-\s*[a-z0-9-]+$', '', name, flags=re.I)
    name = name.replace("_", " ").replace("-", " ").strip()
    # Title case, collapse spaces
    name = re.sub(r'\s+', ' ', name).strip()
    if not name: name = "Conference"
    sheet_name = f"{name} — Conference Enriched"

    log("Pushing to Google Sheets...")
    url, pod, rr, dnc = push_to_sheets(data, headers, sheet_name)

    # ── Step 6: Push to FurtherAI outbound pipeline (opt-in via env) ──
    pipeline_result = None
    if FURTHERAI_CRON_SECRET:
        log("Pushing enriched contacts to FurtherAI pipeline...")
        ok, resp = push_to_furtherai(data, name)
        if ok:
            pipeline_result = resp
            log(f"Pipeline: imported={resp.get('imported', 0)} skipped_customer={resp.get('skipped_customer', 0)} skipped_no_signal={resp.get('skipped_no_signal', 0)}")
        else:
            log(f"Pipeline push failed: {resp.get('error', 'unknown')[:120]}")

    return url, len(data), pod, rr, dnc, pipeline_result

# ─── Slack Event: File Shared ───
@app.event("file_shared")
def handle_file_shared(event, client, say):
    file_id = event.get("file_id")
    if not file_id: return

    file_info = client.files_info(file=file_id)["file"]
    filename = file_info.get("name", "")
    channel = event.get("channel_id", "")

    if not filename.lower().endswith(".csv"):
        return

    # Download file first to count rows for time estimate
    try:
        url = file_info["url_private"]
        resp = req.get(url, headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"})
        csv_text = resp.text
        row_count = csv_text.count('\n') - 1

        # Estimate time: ~0.1s per Apollo row that needs enrichment, ~1s per 30 Claude rows, ~5s for Sheets
        reader = csv.DictReader(io.StringIO(csv_text))
        hdrs = reader.fieldnames or []
        email_col = next((h for h in hdrs if "email" in h.lower()), None)
        phone_col = next((h for h in hdrs if "phone" in h.lower()), None)
        sample = list(reader)
        needs_apollo = sum(1 for r in sample if not (email_col and r.get(email_col, "").strip() and "@" in r.get(email_col, "") and phone_col and r.get(phone_col, "").strip()))
        membership_col = next((h for h in hdrs if h.lower().strip() == "membership"), None)
        comp_type_col = next((h for h in hdrs if re.match(r'company.?type', h, re.I)), None)
        needs_claude = 0 if (membership_col or comp_type_col) else row_count

        apollo_secs = needs_apollo * 0.15
        claude_secs = (needs_claude / 30) * 2
        est_secs = int(apollo_secs + claude_secs + 10)  # 10s for HubSpot match + Sheets push
        est_min = est_secs // 60
        est_str = f"{est_min}m {est_secs % 60}s" if est_min > 0 else f"{est_secs}s"

        msg = say(channel=channel, text=f":hourglass_flowing_sand: Processing *{filename}* — {row_count} contacts\nEstimated time: *{est_str}*")
        ts = msg.get("ts") if isinstance(msg, dict) else None
    except Exception as e:
        say(channel=channel, text=f":x: Error reading *{filename}*: {str(e)[:200]}")
        return

    start_time = time.time()
    def progress(status):
        try:
            elapsed = int(time.time() - start_time)
            if ts: client.chat_update(channel=channel, ts=ts, text=f":hourglass_flowing_sand: *{filename}* — {row_count} contacts (est: {est_str})\n{status} [{elapsed}s elapsed]")
        except: pass

    try:
        # Process — full pipeline (Apollo + HubSpot + Claude + ICP + FurtherAI push if enabled)
        sheet_url, total, pod, rr, dnc, pipeline_result = process_csv(csv_text, filename, progress_fn=progress)

        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f":white_check_mark: *{filename}* processed!"}},
            {"type": "section", "text": {"type": "mrkdwn", "text":
                f"*{total}* contacts\n"
                f":green_circle: Pod Allocation: *{pod}*\n"
                f":large_yellow_circle: Round Robin: *{rr}*\n"
                f":red_circle: Do Not Contact: *{dnc}*"
            }},
            {"type": "section", "text": {"type": "mrkdwn", "text": f":bar_chart: <{sheet_url}|Open in Google Sheets>"}},
        ]
        if pipeline_result:
            imp = pipeline_result.get("imported", 0)
            skc = pipeline_result.get("skipped_customer", 0)
            ske = pipeline_result.get("skipped_existing", 0)
            sks = pipeline_result.get("skipped_no_signal", 0)
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                f":mailbox_with_mail: *FurtherAI pipeline*: imported {imp} · skipped {skc} customer, {ske} existing, {sks} no-signal (sequences paused for review)"
            }})
        say(channel=channel, blocks=blocks)
    except Exception as e:
        say(channel=channel, text=f":x: Error processing *{filename}*: {str(e)[:200]}")

# ─── Slack Event: Message (catch-all for app_mention) ───
@app.event("message")
def handle_message(event):
    pass  # Required to avoid unhandled event warnings

# ─── Start ───
if __name__ == "__main__":
    print("Conference Buddy bot starting...")
    print("Pre-fetching HubSpot data...")
    m, im, dm = get_hubspot_cached()
    print(f"HubSpot loaded: {len(m)} companies ({len(dm)} with domain)")
    print("Ready!")
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
