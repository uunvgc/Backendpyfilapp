# app.py — FIILTHY backend API (Flask) — FULL (AUTO MODE + MULTI-SOURCE, BILLING OFF)
#
# Included:
# ✅ Projects (1 project = 1 URL)
# ✅ Create/List/Modify/Delete projects
# ✅ Multi-source scanning:
#    - Google SERP (SerpApi or Serper.dev)
#    - Reddit (public search.json)
#    - Hacker News (Algolia)
#    - IndieHackers (RSS + keyword filtering)
# ✅ Insert leads (dedupe by url unique)
# ✅ Rule scoring + AI analysis + blended score
# ✅ deep_link always points to the exact place to reply
# ✅ Site audit endpoint saves pain points + copy suggestions
# ✅ Manual lead analyze endpoint
# ✅ AUTO MODE background worker:
#    - scans + analyzes + queues best leads
#    - safe pacing + daily caps
#    - single-worker lock in Supabase
#
# REQUIRED ENV:
#   SUPABASE_URL
#   SUPABASE_KEY  (SERVICE_ROLE recommended on server)
#
# Optional:
#   FIILTHY_API_KEY (X-API-Key)
#
# SERP provider (pick ONE):
#   SERPAPI_KEY
#   SERPER_API_KEY
#
# OpenAI:
#   OPENAI_API_KEY (used by logic/site_audit.py and logic/lead_ai.py)
#
# Auto mode:
#   AUTO_WORKER_ENABLED=true|false (default true)
#   AUTO_WORKER_POLL_SECONDS=15
#   AUTO_PROJECT_SCAN_INTERVAL_MIN=30
#   AUTO_DAILY_CAP_DEFAULT=25
#   AUTO_QUEUE_MIN_SCORE=60
#
# Source defaults:
#   DEFAULT_SOURCES=serp,reddit,hn,indiehackers

import os
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request

try:
    from supabase import create_client
except Exception:
    create_client = None

from logic.query_builder import build_queries, CompanyProfile
from logic.scoring import score_lead
from logic.cache import filter_queries, mark_query_run
from logic.notify import maybe_alert_hot_lead
from logic.referrals import ensure_referral_code, attribute_referral

from logic.site_audit import audit_site
from logic.lead_ai import analyze_lead
from logic.sources import fetch_reddit, fetch_hn, fetch_indiehackers_rss


# -----------------------------
# Config
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

FIILTHY_API_KEY = os.getenv("FIILTHY_API_KEY", "").strip()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()

HTTP_TIMEOUT = int(os.getenv("FIILTHY_HTTP_TIMEOUT", "20"))
MAX_SERP_QUERIES_PER_SCAN = int(os.getenv("MAX_SERP_QUERIES_PER_SCAN", "20"))
SERP_RESULTS_PER_QUERY = int(os.getenv("SERP_RESULTS_PER_QUERY", "10"))
SERP_SLEEP_SECONDS = float(os.getenv("SERP_SLEEP_SECONDS", "0.2"))

AUTO_ANALYZE_MIN_SCORE = int(os.getenv("AUTO_ANALYZE_MIN_SCORE", "40"))

AUTO_WORKER_ENABLED = (os.getenv("AUTO_WORKER_ENABLED", "true").strip().lower() == "true")
AUTO_WORKER_POLL_SECONDS = int(os.getenv("AUTO_WORKER_POLL_SECONDS", "15"))
AUTO_PROJECT_SCAN_INTERVAL_MIN = int(os.getenv("AUTO_PROJECT_SCAN_INTERVAL_MIN", "30"))
AUTO_DAILY_CAP_DEFAULT = int(os.getenv("AUTO_DAILY_CAP_DEFAULT", "25"))
AUTO_QUEUE_MIN_SCORE = int(os.getenv("AUTO_QUEUE_MIN_SCORE", "60"))

DEFAULT_SOURCES = [s.strip() for s in os.getenv("DEFAULT_SOURCES", "serp,reddit,hn,indiehackers").split(",") if s.strip()]

AUTO_LOCK_KEY = "auto_worker"
AUTO_LOCK_TTL_SECONDS = int(os.getenv("AUTO_LOCK_TTL_SECONDS", "60"))

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
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return now_utc().isoformat()

def _clean_list(val) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str):
        return [x.strip() for x in val.split(",") if x.strip()]
    return [str(val).strip()] if str(val).strip() else []

def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        return default

def day_key(dt: Optional[datetime] = None) -> str:
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")

def _sleep_jitter(a=0.2, b=1.0):
    time.sleep(random.uniform(a, b))


# -----------------------------
# SERP Fetchers (Google)
# -----------------------------
def fetch_serp_results(query: str, per_query: int = SERP_RESULTS_PER_QUERY) -> List[Dict[str, str]]:
    per_query = max(1, min(100, int(per_query)))

    if SERPAPI_KEY:
        params = {
            "engine": "google",
            "q": query,
            "num": per_query,
            "api_key": SERPAPI_KEY,
            "hl": "en",
            "gl": "us",
        }
        r = requests.get("https://serpapi.com/search.json", params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        out = []
        for res in (data.get("organic_results") or [])[:per_query]:
            title = (res.get("title") or "").strip()
            url = (res.get("link") or "").strip()
            snippet = (res.get("snippet") or "").strip()
            if title and url:
                out.append({"title": title, "url": url, "deep_link": url, "snippet": snippet, "source": "serp/google"})
        return out

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
            if title and url:
                out.append({"title": title, "url": url, "deep_link": url, "snippet": snippet, "source": "serp/google"})
        return out

    return []


# -----------------------------
# DB helpers
# -----------------------------
def get_project(sb, project_id: str) -> Optional[dict]:
    resp = sb.table("projects").select("*").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None

def find_project_by_owner_url(sb, owner_id: str, url: str) -> Optional[dict]:
    resp = sb.table("projects").select("*").eq("owner_id", owner_id).eq("url", url).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None

def insert_lead(sb, row: dict) -> Optional[dict]:
    if not row.get("url"):
        return None
    resp = sb.table("leads").upsert(row, on_conflict="url").execute()
    data = resp.data or []
    return data[0] if data else row

def list_leads(sb, project_id: Optional[str] = None, limit: int = 50, status: Optional[str] = None) -> List[dict]:
    q = sb.table("leads").select("*").order("created_at", desc=True).limit(limit)
    if project_id:
        q = q.eq("project_id", project_id)
    if status:
        q = q.eq("status", status)
    resp = q.execute()
    return resp.data or []

def latest_positioning(sb, project_id: str) -> dict:
    audit_resp = (
        sb.table("site_audits")
        .select("positioning")
        .eq("project_id", project_id)
        .order("fetched_at", desc=True)
        .limit(1)
        .execute()
    )
    audits = audit_resp.data or []
    if not audits:
        return {}
    return audits[0].get("positioning") or {}

def get_auto_projects(sb, limit: int = 50) -> List[dict]:
    resp = (
        sb.table("projects")
        .select("*")
        .eq("auto_mode", True)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    return resp.data or []

def reset_daily_if_needed(sb, project: dict) -> dict:
    p = dict(project)
    current_day = day_key()
    stored_day = (p.get("auto_sent_day") or "").strip()
    if stored_day != current_day:
        sb.table("projects").update({
            "auto_sent_day": current_day,
            "auto_sent_today": 0,
        }).eq("id", p["id"]).execute()
        p["auto_sent_day"] = current_day
        p["auto_sent_today"] = 0
    return p

def bump_auto_count(sb, project_id: str, delta: int = 1):
    resp = sb.table("projects").select("auto_sent_today, auto_sent_day").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return
    row = rows[0]
    today = day_key()
    if (row.get("auto_sent_day") or "") != today:
        current = 0
    else:
        current = _safe_int(row.get("auto_sent_today"), 0)
    sb.table("projects").update({
        "auto_sent_day": today,
        "auto_sent_today": current + max(1, int(delta)),
    }).eq("id", project_id).execute()

def set_auto_last_scan(sb, project_id: str):
    sb.table("projects").update({"auto_last_scan_at": now_iso()}).eq("id", project_id).execute()


# -----------------------------
# Auto worker lock in Supabase
# -----------------------------
def acquire_or_renew_lock(sb, holder_id: str) -> bool:
    now = now_utc()
    expires = now + timedelta(seconds=AUTO_LOCK_TTL_SECONDS)
    expires_iso = expires.isoformat()

    resp = sb.table("locks").select("*").eq("key", AUTO_LOCK_KEY).limit(1).execute()
    rows = resp.data or []

    if not rows:
        try:
            sb.table("locks").insert({
                "key": AUTO_LOCK_KEY,
                "holder": holder_id,
                "expires_at": expires_iso,
            }).execute()
            return True
        except Exception:
            return False

    lock = rows[0]
    holder = (lock.get("holder") or "").strip()
    expires_at = lock.get("expires_at")
    try:
        exp_dt = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    except Exception:
        exp_dt = None

    is_expired = (exp_dt is None) or (exp_dt < now)

    if holder == holder_id or is_expired:
        sb.table("locks").update({
            "holder": holder_id,
            "expires_at": expires_iso,
        }).eq("key", AUTO_LOCK_KEY).execute()
        return True

    return False


# -----------------------------
# Source selection
# -----------------------------
def project_sources(project: dict) -> List[str]:
    # optional project column "sources" as text[] (recommended)
    s = project.get("sources")
    if isinstance(s, list) and s:
        return [str(x).strip() for x in s if str(x).strip()]
    return DEFAULT_SOURCES


def fetch_from_sources(query: str, project: dict, per_source: int = 10) -> List[Dict[str, str]]:
    """
    Returns normalized results:
      {title, url, deep_link, snippet, source}
    """
    sources = project_sources(project)
    out: List[Dict[str, str]] = []

    if "serp" in sources:
        out.extend(fetch_serp_results(query, per_query=per_source))

    if "reddit" in sources:
        out.extend(fetch_reddit(query, limit=per_source, timeout=HTTP_TIMEOUT))

    if "hn" in sources:
        out.extend(fetch_hn(query, limit=per_source, timeout=HTTP_TIMEOUT))

    if "indiehackers" in sources:
        # RSS is not queryable; we filter by project keywords + niche + query tokens
        kws = []
        kws.extend(_clean_list(project.get("keywords")))
        if project.get("niche"):
            kws.append(str(project.get("niche")))
        # also add a couple tokens from the query to help filter
        for tok in query.split():
            if len(tok) >= 4:
                kws.append(tok)
        out.extend(fetch_indiehackers_rss(kws, limit=per_source, timeout=HTTP_TIMEOUT))

    # light dedupe within batch by deep_link/url
    seen = set()
    deduped = []
    for r in out:
        key = (r.get("deep_link") or r.get("url") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return deduped


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
    per_source: int = 10,
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

    inserted = 0
    analyzed = 0
    queued = 0
    errors: List[str] = []

    positioning = latest_positioning(sb, project_id) if project_id else {}

    for q in queries:
        try:
            results = fetch_from_sources(q, project, per_source=per_source)
            mark_query_run(sb, q)

            for res in results:
                title = (res.get("title") or "").strip()
                url = (res.get("url") or "").strip()
                deep_link = (res.get("deep_link") or url).strip()
                snippet = (res.get("snippet") or "").strip()
                source = res.get("source") or "unknown"

                if not url or not title:
                    continue

                base_score, intent, reasons = score_lead(
                    title,
                    snippet,
                    url=deep_link,
                    source=source,
                    created_at_iso=None,
                )

                lead_row = {
                    "project_id": project_id,
                    "title": title[:500],
                    "content": snippet[:4000],
                    "source": source,
                    "url": deep_link,         # store deep link as the canonical URL
                    "deep_link": deep_link,
                    "status": "new",
                    "score": base_score,
                    "intent": intent,
                    "reasons": reasons,
                    "created_at": now_iso(),
                }

                saved = insert_lead(sb, lead_row)
                if not saved:
                    continue

                inserted += 1

                if base_score >= AUTO_ANALYZE_MIN_SCORE:
                    try:
                        analysis = analyze_lead(saved, positioning)
                        ai_score = _safe_int(analysis.get("score"), base_score)
                        final_score = int((base_score * 0.5) + (ai_score * 0.5))

                        new_status = "drafted"
                        if final_score >= AUTO_QUEUE_MIN_SCORE:
                            new_status = "queued"
                            queued += 1

                        sb.table("leads").update({
                            "score": final_score,
                            "why_this_is_a_lead": analysis.get("why_this_is_a_lead", ""),
                            "analysis": analysis,
                            "status": new_status,
                            "last_analyzed_at": now_iso(),
                        }).eq("id", saved.get("id")).execute()

                        analyzed += 1
                    except Exception as e:
                        print("AI analysis error:", e)

                try:
                    maybe_alert_hot_lead(
                        sb,
                        user_id=user_id,
                        user_email=user_email,
                        lead_id=str(saved.get("id") or deep_link),
                        lead_title=title,
                        lead_url=deep_link,
                        lead_source=source,
                        lead_score=base_score,
                        lead_snippet=snippet,
                        channel="email",
                    )
                except Exception as e:
                    print("Notify error:", e)

            time.sleep(SERP_SLEEP_SECONDS)

        except Exception as e:
            errors.append(f"{q[:80]}... -> {str(e)}")

    return {
        "ok": True,
        "project_id": project_id,
        "queries_ran": len(queries),
        "inserted": inserted,
        "auto_analyzed": analyzed,
        "queued": queued,
        "errors": errors[:10],
        "sources": project_sources(project),
    }


# -----------------------------
# AUTO MODE WORKER
# -----------------------------
_AUTO_STATE = {
    "running": False,
    "last_tick": None,
    "holder_id": None,
    "last_projects": 0,
    "last_actions": {},
}

def _auto_worker_loop():
    sb = None
    holder_id = f"host:{os.getenv('HOSTNAME','local')}|pid:{os.getpid()}"
    _AUTO_STATE["holder_id"] = holder_id
    _AUTO_STATE["running"] = True

    while True:
        try:
            _AUTO_STATE["last_tick"] = now_iso()
            if not AUTO_WORKER_ENABLED:
                time.sleep(5)
                continue

            if sb is None:
                sb = get_supabase()

            if not acquire_or_renew_lock(sb, holder_id):
                time.sleep(AUTO_WORKER_POLL_SECONDS)
                continue

            projects = get_auto_projects(sb, limit=50)
            _AUTO_STATE["last_projects"] = len(projects)
            actions = {"scanned": 0, "skipped_cap": 0, "skipped_interval": 0, "queued": 0}

            for p in projects:
                p = reset_daily_if_needed(sb, p)

                cap = _safe_int(p.get("auto_daily_cap"), AUTO_DAILY_CAP_DEFAULT)
                sent_today = _safe_int(p.get("auto_sent_today"), 0)

                if sent_today >= cap:
                    actions["skipped_cap"] += 1
                    continue

                last_scan = p.get("auto_last_scan_at")
                should_scan = True
                if last_scan:
                    try:
                        ls = datetime.fromisoformat(str(last_scan).replace("Z", "+00:00"))
                        mins = (now_utc() - ls).total_seconds() / 60.0
                        if mins < AUTO_PROJECT_SCAN_INTERVAL_MIN:
                            should_scan = False
                    except Exception:
                        pass

                if not should_scan:
                    actions["skipped_interval"] += 1
                    continue

                _sleep_jitter(0.2, 1.2)

                project_id = p["id"]
                user_id = str(p.get("owner_id") or "")
                user_email = "unknown@example.com"

                try:
                    res = run_project_scan(
                        sb,
                        user_id=user_id,
                        user_email=user_email,
                        project=p,
                        max_queries=MAX_SERP_QUERIES_PER_SCAN,
                        per_source=10,
                    )
                    set_auto_last_scan(sb, project_id)
                    actions["scanned"] += 1
                    actions["queued"] += _safe_int(res.get("queued"), 0)

                    if _safe_int(res.get("queued"), 0) > 0:
                        bump_auto_count(sb, project_id, delta=_safe_int(res.get("queued"), 0))

                except Exception as e:
                    print("Auto scan error:", e)

            _AUTO_STATE["last_actions"] = actions
            time.sleep(AUTO_WORKER_POLL_SECONDS)

        except Exception as e:
            print("Auto worker loop error:", e)
            time.sleep(5)

try:
    import threading
    t = threading.Thread(target=_auto_worker_loop, daemon=True)
    t.start()
except Exception:
    pass


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def index():
    return jsonify({"ok": True, "service": "fiilthy-backend", "default_sources": DEFAULT_SOURCES})

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

@app.get("/auto/status")
def auto_status():
    return jsonify({"ok": True, "state": _AUTO_STATE})


# --- Referral endpoints (kept for later) ---
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
    return jsonify({"ok": True, "attributed": bool(ok)})


# --- Projects ---
@app.post("/projects")
def api_create_project():
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    owner_id = (body.get("owner_id") or body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    name = (body.get("name") or "").strip()
    url = (body.get("url") or "").strip()

    if not owner_id or not user_email:
        return jsonify({"ok": False, "error": "owner_id (or user_id) and user_email required"}), 400
    if not name:
        return jsonify({"ok": False, "error": "name required"}), 400
    if not url:
        return jsonify({"ok": False, "error": "url required"}), 400

    existing = find_project_by_owner_url(sb, owner_id, url)
    if existing:
        update = {
            "name": name,
            "niche": (body.get("niche") or "").strip() or None,
            "keywords": _clean_list(body.get("keywords")),
            "locations": _clean_list(body.get("locations")),
        }
        if "sources" in body:
            update["sources"] = _clean_list(body.get("sources"))

        saved = sb.table("projects").update(update).eq("id", existing["id"]).execute()
        return jsonify({"ok": True, "project": (saved.data or [existing])[0], "updated_existing": True})

    row = {
        "owner_id": owner_id,
        "name": name,
        "url": url,
        "niche": (body.get("niche") or "").strip() or None,
        "keywords": _clean_list(body.get("keywords")),
        "locations": _clean_list(body.get("locations")),
        "sources": _clean_list(body.get("sources")) if "sources" in body else DEFAULT_SOURCES,
        "auto_mode": bool(body.get("auto_mode")) if "auto_mode" in body else False,
        "auto_daily_cap": _safe_int(body.get("auto_daily_cap"), AUTO_DAILY_CAP_DEFAULT),
        "auto_sent_day": day_key(),
        "auto_sent_today": 0,
        "auto_last_scan_at": None,
        "created_at": now_iso(),
    }

    resp = sb.table("projects").insert(row).execute()
    data = resp.data or []
    return jsonify({"ok": True, "project": data[0] if data else row, "updated_existing": False})

@app.get("/projects")
def api_list_projects():
    sb = get_supabase()
    owner_id = (request.args.get("owner_id") or request.args.get("user_id") or "").strip()
    if not owner_id:
        return jsonify({"ok": False, "error": "owner_id required"}), 400
    resp = sb.table("projects").select("*").eq("owner_id", owner_id).order("created_at", desc=True).execute()
    return jsonify({"ok": True, "projects": resp.data or []})

@app.patch("/projects/<project_id>")
def api_update_project(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    project = get_project(sb, project_id)
    if not project:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(project.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    update: Dict[str, Any] = {}
    if "name" in body:
        update["name"] = (body.get("name") or "").strip()
    if "url" in body:
        update["url"] = (body.get("url") or "").strip()
    if "niche" in body:
        update["niche"] = (body.get("niche") or "").strip() or None
    if "keywords" in body:
        update["keywords"] = _clean_list(body.get("keywords"))
    if "locations" in body:
        update["locations"] = _clean_list(body.get("locations"))
    if "auto_mode" in body:
        update["auto_mode"] = bool(body.get("auto_mode"))
    if "auto_daily_cap" in body:
        update["auto_daily_cap"] = _safe_int(body.get("auto_daily_cap"), AUTO_DAILY_CAP_DEFAULT)
    if "sources" in body:
        update["sources"] = _clean_list(body.get("sources"))

    update = {k: v for k, v in update.items() if not (k in ["name", "url"] and not v)}
    if not update:
        return jsonify({"ok": False, "error": "No valid fields to update"}), 400

    saved = sb.table("projects").update(update).eq("id", project_id).execute()
    return jsonify({"ok": True, "project": (saved.data or [project])[0]})

@app.patch("/projects/<project_id>/auto")
def api_toggle_auto(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    project = get_project(sb, project_id)
    if not project:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(project.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    enabled = bool(body.get("enabled"))
    daily_cap = _safe_int(body.get("daily_cap"), _safe_int(project.get("auto_daily_cap"), AUTO_DAILY_CAP_DEFAULT))

    saved = sb.table("projects").update({
        "auto_mode": enabled,
        "auto_daily_cap": daily_cap,
        "auto_sent_day": day_key(),
        "auto_sent_today": 0,
    }).eq("id", project_id).execute()

    return jsonify({"ok": True, "project": (saved.data or [project])[0]})


@app.delete("/projects/<project_id>")
def api_delete_project(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}
    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    project = get_project(sb, project_id)
    if not project:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(project.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    sb.table("projects").delete().eq("id", project_id).execute()
    return jsonify({"ok": True, "deleted": True, "project_id": project_id})


# --- Manual scan ---
@app.post("/projects/<project_id>/scan")
def api_scan_project(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    project = get_project(sb, project_id)
    if not project:
        return jsonify({"ok": False, "error": "Project not found"}), 404

    if "serp" in project_sources(project) and (not SERPAPI_KEY and not SERPER_API_KEY):
        return jsonify({"ok": False, "error": "SERP enabled but no SERP provider configured."}), 500

    max_q = _safe_int(body.get("max_queries"), MAX_SERP_QUERIES_PER_SCAN)
    per_source = _safe_int(body.get("per_source"), 10)

    result = run_project_scan(
        sb,
        user_id=user_id,
        user_email=user_email,
        project=project,
        max_queries=max_q,
        per_source=per_source,
    )
    return jsonify(result)


# --- Leads ---
@app.get("/leads")
def api_list_leads():
    sb = get_supabase()
    project_id = (request.args.get("project_id") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    limit = _safe_int(request.args.get("limit"), 50)
    limit = max(1, min(200, limit))
    leads = list_leads(sb, project_id=project_id, limit=limit, status=status)
    return jsonify({"ok": True, "leads": leads})

@app.post("/leads/<lead_id>/analyze")
def api_analyze_lead(lead_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    resp = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    lead = rows[0]
    project_id = lead.get("project_id")
    if not project_id:
        return jsonify({"ok": False, "error": "Lead missing project_id"}), 400

    positioning = latest_positioning(sb, project_id)
    analysis = analyze_lead(lead, positioning)

    base_score = _safe_int(lead.get("score"), 0)
    ai_score = _safe_int(analysis.get("score"), base_score)
    final_score = int((base_score * 0.5) + (ai_score * 0.5))

    new_status = "drafted"
    if final_score >= AUTO_QUEUE_MIN_SCORE:
        new_status = "queued"

    sb.table("leads").update({
        "score": final_score,
        "why_this_is_a_lead": analysis.get("why_this_is_a_lead", ""),
        "analysis": analysis,
        "status": new_status,
        "last_analyzed_at": now_iso(),
    }).eq("id", lead_id).execute()

    return jsonify({"ok": True, "lead_id": lead_id, "analysis": analysis, "status": new_status})


# --- Site Audit ---
@app.post("/projects/<project_id>/audit")
def api_audit_project(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    project = get_project(sb, project_id)
    if not project:
        return jsonify({"ok": False, "error": "Project not found"}), 404

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


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)