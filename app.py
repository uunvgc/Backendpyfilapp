# app.py
# FIILTHY backend API (Flask) — plugs into your /logic modules + Supabase.
#
# What this gives you:
# - Create companies (URL + keywords/niche/locations)
# - Run scans (HN/Reddit optional + SERP “everywhere”)
# - Insert leads into Supabase (dedupe via upsert on url)
# - Scoring + reasons (logic/scoring.py)
# - Query builder (logic/query_builder.py)
# - Query + URL caching (logic/cache.py)
# - Billing quotas (logic/billing.py)
# - Notifications (logic/notify.py)
# - Referrals (logic/referrals.py)
#
# Env vars required:
#   SUPABASE_URL
#   SUPABASE_KEY   (SERVICE_ROLE key recommended on server)
#
# Optional SERP provider env vars (pick ONE):
#   SERPAPI_KEY                 (SerpApi)
#   SERPER_API_KEY              (Serper.dev)
#
# Security / access:
#   FIILTHY_API_KEY             (recommended; clients must send X-API-Key header)
#
# Email alerts (optional, for logic/notify.py):
#   SMTP_HOST SMTP_PORT SMTP_USER SMTP_PASS SMTP_FROM
#   ALERT_SCORE_THRESHOLD (default 75)
#
# Run:
#   python app.py
#
# Notes:
# - This file does NOT do Supabase auth/JWT verification. It uses X-API-Key.
#   For launch, you can add Supabase JWT verification later.

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

try:
    from supabase import create_client
except Exception:
    create_client = None

# logic modules you created
from logic.query_builder import build_queries, CompanyProfile
from logic.scoring import score_lead
from logic.cache import filter_queries, mark_query_run
from logic.billing import can_use, consume
from logic.notify import maybe_alert_hot_lead
from logic.referrals import ensure_referral_code, attribute_referral


# -----------------------------
# Config
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

FIILTHY_API_KEY = os.getenv("FIILTHY_API_KEY", "").strip()

# SERP providers
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()         # serpapi.com
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()   # serper.dev

HTTP_TIMEOUT = int(os.getenv("FIILTHY_HTTP_TIMEOUT", "20"))
MAX_SERP_QUERIES_PER_SCAN = int(os.getenv("MAX_SERP_QUERIES_PER_SCAN", "20"))
SERP_RESULTS_PER_QUERY = int(os.getenv("SERP_RESULTS_PER_QUERY", "10"))

# Optional: slow down to avoid bursts
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
        return  # not enforced if you didn't set it (not recommended)
    key = request.headers.get("X-API-Key", "").strip()
    if key != FIILTHY_API_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401


@app.before_request
def _auth_middleware():
    # Allow health without key
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
        # allow comma-separated
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

    # No provider configured
    return []


# -----------------------------
# DB helpers
# -----------------------------
def upsert_company(sb, user_id: str, payload: dict) -> dict:
    """
    Creates/updates a company row.
    """
    url = (payload.get("url") or "").strip()
    if not url:
        raise ValueError("url is required")

    row = {
        "user_id": user_id,
        "url": url,
        "name": (payload.get("name") or "").strip() or None,
        "niche": (payload.get("niche") or "").strip() or None,
        "keywords": _clean_list(payload.get("keywords")),
        "locations": _clean_list(payload.get("locations")),
        "created_at": now_iso(),
    }

    # If you have a unique constraint on (user_id, url) you can use on_conflict="user_id,url"
    # If not, simplest is insert and let duplicates exist — but not recommended.
    # We'll try upsert on url (assuming url is unique in your companies table).
    resp = sb.table("companies").upsert(row, on_conflict="url").execute()
    data = (resp.data or [])
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


def list_leads(sb, company_id: Optional[str] = None, limit: int = 50) -> List[dict]:
    q = sb.table("leads").select("*").order("created_at", desc=True).limit(limit)
    if company_id:
        q = q.eq("company_id", company_id)
    resp = q.execute()
    return resp.data or []


# -----------------------------
# Core scan logic
# -----------------------------
def run_company_scan(
    sb,
    *,
    user_id: str,
    user_email: str,
    company: dict,
    max_queries: int = MAX_SERP_QUERIES_PER_SCAN,
    per_query: int = SERP_RESULTS_PER_QUERY,
) -> Dict[str, Any]:
    """
    Build queries -> cache filter -> enforce billing -> fetch SERP -> score -> insert -> notify.
    """
    company_id = company.get("id")
    profile = CompanyProfile(
        url=company.get("url") or "",
        name=company.get("name"),
        niche=company.get("niche"),
        keywords=tuple(company.get("keywords") or ()),
        locations=tuple(company.get("locations") or ()),
    )

    queries = build_queries(profile, max_queries=max_queries)
    queries = queries[:max_queries]

    # cache-filter queries (24h cooldown)
    queries = filter_queries(sb, queries)

    # Billing: SERP queries quota
    if queries:
        if not can_use(sb, user_id, "serp_queries", amount=len(queries)):
            return {
                "ok": False,
                "error": "SERP quota exceeded for this user/plan",
                "queries_attempted": len(queries),
                "inserted": 0,
            }
        consume(sb, user_id, "serp_queries", amount=len(queries), meta={"company_id": company_id})

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
                    "company_id": company_id,
                    "title": title[:500],
                    "content": snippet[:4000],
                    "source": source,
                    "url": url,
                    "score": score,
                    "intent": intent,
                    "reasons": reasons,
                    "created_at": now_iso(),
                }

                saved = insert_lead(sb, lead_row)
                if saved:
                    # Not perfect (upsert may return row even if it was an update),
                    # but good enough for now. You can later compare timestamps.
                    inserted += 1

                    # Billing: notifications (only if we actually attempt to send)
                    # maybe_alert_hot_lead has its own "send once" guard via notifications table.
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
                            consume(sb, user_id, "notifications", amount=1, meta={"company_id": company_id})

            time.sleep(SERP_SLEEP_SECONDS)

        except Exception as e:
            errors.append(f"{q[:80]}... -> {str(e)}")

    return {
        "ok": True,
        "company_id": company_id,
        "queries_ran": len(queries),
        "inserted": inserted,
        "errors": errors[:10],
    }


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


@app.post("/users/<user_id>/referral-code")
def api_referral_code(user_id):
    sb = get_supabase()
    code = ensure_referral_code(sb, user_id)
    return jsonify({"ok": True, "referral_code": code})


@app.post("/users/<user_id>/apply-referral")
def api_apply_referral(user_id):
    """
    Body: { "referral_code": "ABC12345" }
    Attributes referral to this user if not already set.
    """
    sb = get_supabase()
    body = request.get_json(force=True) or {}
    code = (body.get("referral_code") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "referral_code required"}), 400

    ok = attribute_referral(sb, user_id, code)
    return jsonify({"ok": True, "attributed": ok})


@app.post("/companies")
def api_create_company():
    """
    Body must include:
      user_id, user_email, url
    Optional:
      name, niche, keywords[], locations[]
    """
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    try:
        company = upsert_company(sb, user_id, body)
        return jsonify({"ok": True, "company": company})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.get("/companies")
def api_list_companies():
    """
    Query params: user_id=...
    """
    sb = get_supabase()
    user_id = (request.args.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    resp = sb.table("companies").select("*").eq("user_id", user_id).order("created_at", desc=True).execute()
    return jsonify({"ok": True, "companies": resp.data or []})


@app.post("/companies/<company_id>/scan")
def api_scan_company(company_id):
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

    # Billing: scans quota
    if not can_use(sb, user_id, "scans", amount=1):
        return jsonify({"ok": False, "error": "Scan quota exceeded for this user/plan"}), 402
    consume(sb, user_id, "scans", amount=1, meta={"company_id": company_id})

    # fetch company
    resp = sb.table("companies").select("*").eq("id", company_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Company not found"}), 404

    company = rows[0]

    max_q = int(body.get("max_queries") or MAX_SERP_QUERIES_PER_SCAN)
    per_q = int(body.get("per_query") or SERP_RESULTS_PER_QUERY)

    # Must have SERP provider configured
    if not SERPAPI_KEY and not SERPER_API_KEY:
        return jsonify({"ok": False, "error": "No SERP provider configured. Set SERPAPI_KEY or SERPER_API_KEY."}), 500

    result = run_company_scan(
        sb,
        user_id=user_id,
        user_email=user_email,
        company=company,
        max_queries=max_q,
        per_query=per_q,
    )
    return jsonify(result)


@app.get("/leads")
def api_list_leads():
    """
    Query params:
      company_id (optional)
      limit (optional)
    """
    sb = get_supabase()
    company_id = (request.args.get("company_id") or "").strip() or None
    limit = int(request.args.get("limit") or "50")
    limit = max(1, min(200, limit))
    leads = list_leads(sb, company_id=company_id, limit=limit)
    return jsonify({"ok": True, "leads": leads})


@app.get("/usage")
def api_usage():
    """
    Query params: user_id=...
    Returns current usage counters row (if you created usage_counters table).
    """
    sb = get_supabase()
    user_id = (request.args.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    # just dump usage_counters for current rolling period_start used by billing.py
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
    # Local run (PythonAnywhere web app uses WSGI instead)
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)