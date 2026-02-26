# app.py — FIILTHY backend API (Flask) aligned to Supabase "projects" schema
#
# Tables expected in Supabase:
# - projects (id, owner_id, name, url, niche, keywords, locations, created_at)
# - leads (id, project_id, title, content, source, url, deep_link, status, score, intent, reasons, analysis, why_this_is_a_lead, last_analyzed_at, created_at)
# - site_audits (id, project_id, target_url, fetched_at, summary, positioning, issues, quick_wins, suggested_copy)
# - usage_counters (optional, used by billing.py)
#
# Env vars required:
#   SUPABASE_URL
#   SUPABASE_KEY  (SERVICE_ROLE recommended on server)
# Optional:
#   FIILTHY_API_KEY (if set, clients must send X-API-Key)
# SERP providers (pick ONE):
#   SERPAPI_KEY
#   SERPER_API_KEY
#
# Run:
#   python app.py

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request

try:
    from supabase import create_client
except Exception:
    create_client = None

# Existing logic modules
from logic.query_builder import build_queries, CompanyProfile
from logic.scoring import score_lead
from logic.cache import filter_queries, mark_query_run
from logic.billing import can_use, consume
from logic.notify import maybe_alert_hot_lead
from logic.referrals import ensure_referral_code, attribute_referral

# New AI modules
from logic.site_audit import audit_site
from logic.lead_ai import analyze_lead

# -----------------------------
# Config
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

FIILTHY_API_KEY = os.getenv("FIILTHY_API_KEY", "").strip()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()         # serpapi.com
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()   # serper.dev

HTTP_TIMEOUT = int(os.getenv("FIILTHY_HTTP_TIMEOUT", "20"))
MAX_SERP_QUERIES_PER_SCAN = int(os.getenv("MAX_SERP_QUERIES_PER_SCAN", "20"))
SERP_RESULTS_PER_QUERY = int(os.getenv("SERP_RESULTS_PER_QUERY", "10"))
SERP_SLEEP_SECONDS = float(os.getenv("SERP_SLEEP_SECONDS", "0.2"))

app = Flask(__name__)

# -----------------------------
# Supabase
# -----------------------------
_supabase = None

def get_supabase():
    global _supabase
    if _supabase is not None:
        return _supabase
    if not SUPABASE_URL or not SUPABASE_KEY or create_client is None:
        raise RuntimeError("Supabase not configured. Set SUPABASE_URL and SUPABASE_KEY.")
    _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase

# -----------------------------
# Security (simple API key)
# -----------------------------
def require_api_key():
    if not FIILTHY_API_KEY:
        return
    key = request.headers.get("X-API-Key", "").strip()
    if key != FIILTHY_API_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

@app.before_request
def _auth_middleware():
    if request.path in ["/health", "/"]:
        return
    resp = require_api_key()
    if resp:
        return resp

# -----------------------------
# Utilities
# -----------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clean_list(val) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str):
        return [x.strip() for x in val.split(",") if x.strip()]
    return [str(val).strip()] if str(val).strip() else []

# -----------------------------
# SERP Fetchers
# -----------------------------
def fetch_serp_results(query: str, per_query: int = SERP_RESULTS_PER_QUERY) -> List[Dict[str, str]]:
    """
    Returns list of {title, url, snippet, source}
    Uses SERPAPI (preferred) or SERPER.
    """
    per_query = max(1, min(100, int(per_query)))

    # 1) SerpApi
    if SERPAPI_KEY:
        params = {
            "engine": "google",
            "q": query,
            "num": per_query,
            "api_key": SERPAPI_KEY,
            "hl": "en",
            "gl": "ca",
        }
        r = requests.get("https://serpapi.com/search.json", params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        out = []
        for res in (data.get("organic_results") or [])[:per_query]:
            title = (res.get("title") or "").strip()
            url = (res.get("link") or "").strip()
            snippet = (res.get("snippet") or "").strip()
            if not title or not url:
                continue
            out.append({"title": title, "url": url, "snippet": snippet, "source": "serp/google"})
        return out

    # 2) Serper.dev
    if SERPER_API_KEY:
        headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
        payload = {"q": query, "num": per_query}
        r = requests.post("https://google.serper.dev/search", headers=headers, json=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        out = []
        for res in (data.get("organic") or [])[:per_query]:
            title = (res.get("title") or "").strip()
            url = (res.get("link") or "").strip()
            snippet = (res.get("snippet") or "").strip()
            if not title or not url:
                continue
            out.append({"title": title, "url": url, "snippet": snippet, "source": "serp/google"})
        return out

    return []

# -----------------------------
# DB helpers
# -----------------------------
def upsert_project(sb, payload: dict) -> dict:
    """
    Creates/updates a project row.
    Required: owner_id (or user_id), name, url
    Optional: niche, keywords, locations
    """
    owner_id = (payload.get("owner_id") or payload.get("user_id") or "").strip()
    name = (payload.get("name") or "").strip()
    url = (payload.get("url") or "").strip()

    if not owner_id:
        raise ValueError("owner_id (or user_id) is required")
    if not name:
        raise ValueError("name is required")
    if not url:
        raise ValueError("url is required")

    row = {
        "owner_id": owner_id,
        "name": name,
        "url": url,
        "niche": (payload.get("niche") or "").strip() or None,
        "keywords": _clean_list(payload.get("keywords")),
        "locations": _clean_list(payload.get("locations")),
        "created_at": now_iso(),
    }

    # assumes unique(url) in projects, adjust if your constraint differs
    resp = sb.table("projects").upsert(row, on_conflict="url").execute()
    data = resp.data or []
    return data[0] if data else row

def insert_lead(sb, row: dict) -> Optional[dict]:
    """
    Upsert a lead on url. Assumes leads.url is UNIQUE.
    """
    if not row.get("url"):
        return None
    resp = sb.table("leads").upsert(row, on_conflict="url").execute()
    data = resp.data or []
    return data[0] if data else row

def list_leads(sb, project_id: Optional[str] = None, limit: int = 50) -> List[dict]:
    q = sb.table("leads").select("*").order("created_at", desc=True).limit(limit)
    if project_id:
        q = q.eq("project_id", project_id)
    resp = q.execute()
    return resp.data or []

# -----------------------------
# Core scan logic
# -----------------------------
def run_project_scan(
    sb,
    *,
    user_id: str,
    user_email: str,
    project: dict,
    max_queries: int = MAX_SERP_QUERIES_PER_SCAN,
    per_query: int = SERP_RESULTS_PER_QUERY,
) -> Dict[str, Any]:
    project_id = project.get("id")

    profile = CompanyProfile(
        url=project.get("url") or "",
        name=project.get("name"),
        niche=project.get("niche"),
        keywords=tuple(project.get("keywords") or ()),
        locations=tuple(project.get("locations") or ()),
    )

    queries = build_queries(profile, max_queries=max_queries)[:max_queries]
    queries = filter_queries(sb, queries)

    if queries:
        if not can_use(sb, user_id, "serp_queries", amount=len(queries)):
            return {"ok": False, "error": "SERP quota exceeded", "queries_attempted": len(queries), "inserted": 0}
        consume(sb, user_id, "serp_queries", amount=len(queries), meta={"project_id": project_id})

    inserted = 0
    errors: List[str] = []

    for q in queries:
        try:
            results = fetch_serp_results(q, per_query=per_query)
            mark_query_run(sb, q)

            for res in results:
                title = (res.get("title") or "").strip()
                url = (res.get("url") or "").strip()
                snippet = (res.get("snippet") or "").strip()
                source = res.get("source") or "serp/google"

                if not url or not title:
                    continue

                score, intent, reasons = score_lead(
                    title,
                    snippet,
                    url=url,
                    source=source,
                    created_at_iso=None,
                )

                lead_row = {
                    "project_id": project_id,
                    "title": title[:500],
                    "content": snippet[:4000],
                    "source": source,
                    "url": url,
                    "deep_link": url,     # click → exact lead page
                    "status": "new",
                    "score": score,
                    "intent": intent,
                    "reasons": reasons,
                    "created_at": now_iso(),
                }

                saved = insert_lead(sb, lead_row)
                if saved:
                    inserted += 1

                    if can_use(sb, user_id, "notifications", amount=1):
                        sent = maybe_alert_hot_lead(
                            sb,
                            user_id=user_id,
                            user_email=user_email,
                            lead_id=str(saved.get("id") or url),
                            lead_title=title,
                            lead_url=url,
                            lead_source=source,
                            lead_score=score,
                            lead_snippet=snippet,
                            channel="email",
                        )
                        if sent:
                            consume(sb, user_id, "notifications", amount=1, meta={"project_id": project_id})

            time.sleep(SERP_SLEEP_SECONDS)

        except Exception as e:
            errors.append(f"{q[:80]}... -> {str(e)}")

    return {"ok": True, "project_id": project_id, "queries_ran": len(queries), "inserted": inserted, "errors": errors[:10]}

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def index():
    return jsonify({"ok": True, "service": "fiilthy-backend"})

@app.get("/health")
def health():
    ok = True
    err = None
    try:
        get_supabase()
    except Exception as e:
        ok = False
        err = str(e)
    return jsonify({"ok": ok, "supabase": ok, "error": err})

# --- Referral endpoints ---
@app.post("/users/<user_id>/referral-code")
def api_referral_code(user_id):
    sb = get_supabase()
    code = ensure_referral_code(sb, user_id)
    return jsonify({"ok": True, "referral_code": code})

@app.post("/users/<user_id>/apply-referral")
def api_apply_referral(user_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}
    code = (body.get("referral_code") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "referral_code required"}), 400
    ok = attribute_referral(sb, user_id, code)
    return jsonify({"ok": True, "attributed": ok})

# --- Projects ---
@app.post("/projects")
def api_create_project():
    """
    Body must include:
      owner_id (or user_id), user_email, name, url
    Optional:
      niche, keywords[], locations[]
    """
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_email = (body.get("user_email") or "").strip()
    if not user_email:
        return jsonify({"ok": False, "error": "user_email required"}), 400

    try:
        project = upsert_project(sb, body)
        return jsonify({"ok": True, "project": project})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.get("/projects")
def api_list_projects():
    """
    Query params: owner_id=... (or user_id)
    """
    sb = get_supabase()
    owner_id = (request.args.get("owner_id") or request.args.get("user_id") or "").strip()
    if not owner_id:
        return jsonify({"ok": False, "error": "owner_id required"}), 400

    resp = sb.table("projects").select("*").eq("owner_id", owner_id).order("created_at", desc=True).execute()
    return jsonify({"ok": True, "projects": resp.data or []})

# --- Scan ---
@app.post("/projects/<project_id>/scan")
def api_scan_project(project_id):
    """
    Body must include:
      user_id, user_email
    Optional:
      max_queries, per_query
    """
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    if not can_use(sb, user_id, "scans", amount=1):
        return jsonify({"ok": False, "error": "Scan quota exceeded"}), 402
    consume(sb, user_id, "scans", amount=1, meta={"project_id": project_id})

    resp = sb.table("projects").select("*").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    project = rows[0]

    max_q = int(body.get("max_queries") or MAX_SERP_QUERIES_PER_SCAN)
    per_q = int(body.get("per_query") or SERP_RESULTS_PER_QUERY)

    if not SERPAPI_KEY and not SERPER_API_KEY:
        return jsonify({"ok": False, "error": "No SERP provider configured. Set SERPAPI_KEY or SERPER_API_KEY."}), 500

    result = run_project_scan(
        sb,
        user_id=user_id,
        user_email=user_email,
        project=project,
        max_queries=max_q,
        per_query=per_q,
    )
    return jsonify(result)

# --- Leads list ---
@app.get("/leads")
def api_list_leads():
    """
    Query params:
      project_id (optional)
      limit (optional)
    """
    sb = get_supabase()
    project_id = (request.args.get("project_id") or "").strip() or None
    limit = int(request.args.get("limit") or "50")
    limit = max(1, min(200, limit))
    leads = list_leads(sb, project_id=project_id, limit=limit)
    return jsonify({"ok": True, "leads": leads})

# --- Site Audit (pain points) ---
@app.post("/projects/<project_id>/audit")
def api_audit_project(project_id):
    """
    Body must include:
      user_id, user_email
    Optional:
      target_url (defaults to project.url)
    """
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    resp = sb.table("projects").select("*").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    project = rows[0]

    target_url = (body.get("target_url") or project.get("url") or "").strip()
    if not target_url:
        return jsonify({"ok": False, "error": "Project has no url"}), 400

    audit = audit_site(target_url)

    sb.table("site_audits").insert({
        "project_id": project_id,
        "target_url": target_url,
        "summary": audit.get("summary"),
        "positioning": audit.get("positioning"),
        "issues": audit.get("issues"),
        "quick_wins": audit.get("quick_wins"),
        "suggested_copy": audit.get("suggested_copy"),
    }).execute()

    return jsonify({"ok": True, "project_id": project_id, "audit": audit})

# --- Lead AI analysis + human drafts ---
@app.post("/leads/<lead_id>/analyze")
def api_analyze_lead(lead_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    # fetch lead
    resp = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    lead = rows[0]
    project_id = lead.get("project_id")
    if not project_id:
        return jsonify({"ok": False, "error": "Lead missing project_id"}), 400

    # latest audit positioning
    audit_resp = (
        sb.table("site_audits")
        .select("*")
        .eq("project_id", project_id)
        .order("fetched_at", desc=True)
        .limit(1)
        .execute()
    )
    audits = audit_resp.data or []
    positioning = audits[0].get("positioning") if audits else {}

    analysis = analyze_lead(lead, positioning)

    sb.table("leads").update({
        "score": analysis.get("score", lead.get("score", 0)),
        "why_this_is_a_lead": analysis.get("why_this_is_a_lead", ""),
        "analysis": analysis,
        "status": "drafted",
        "last_analyzed_at": now_iso(),
    }).eq("id", lead_id).execute()

    return jsonify({"ok": True, "lead_id": lead_id, "analysis": analysis})

# --- Usage ---
@app.get("/usage")
def api_usage():
    sb = get_supabase()
    user_id = (request.args.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    from logic.billing import current_period_start
    start_iso = current_period_start().isoformat()

    resp = (
        sb.table("usage_counters")
        .select("*")
        .eq("user_id", user_id)
        .eq("period_start", start_iso)
        .limit(1)
        .execute()
    )
    row = (resp.data or [])
    return jsonify({"ok": True, "period_start": start_iso, "usage": row[0] if row else {}})

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True 