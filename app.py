# app.py — FIILTHY backend (Flask)
# Includes: Projects, Leads (pipeline+notes), Multi-source scanning, Job Queue API
# IMPORTANT: This version includes the DEDUPE FIX and SAFE UPSERT (project_id + url).
#
# Required ENV:
#   SUPABASE_URL
#   SUPABASE_KEY
#
# Optional:
#   FIILTHY_API_KEY
#   SERPAPI_KEY or SERPER_API_KEY
#
# Run locally:
#   python app.py
#
# Deploy (Render):
#   gunicorn app:app

import os
import time
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request

try:
    from supabase import create_client
except Exception:
    create_client = None

# Your logic modules
from logic.query_builder import build_queries, CompanyProfile
from logic.scoring import score_lead
from logic.cache import filter_queries, mark_query_run
from logic.notify import maybe_alert_hot_lead
from logic.referrals import ensure_referral_code, attribute_referral

from logic.sources import fetch_reddit, fetch_hn, fetch_indiehackers_rss
from logic.jobs import enqueue_job, get_job, list_jobs, cancel_job


# -----------------------------
# Config
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

FIILTHY_API_KEY = os.getenv("FIILTHY_API_KEY", "").strip()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()

HTTP_TIMEOUT = int(os.getenv("FIILTHY_HTTP_TIMEOUT", "20"))
MAX_QUERIES_PER_SCAN = int(os.getenv("MAX_QUERIES_PER_SCAN", "20"))
PER_SOURCE_RESULTS = int(os.getenv("PER_SOURCE_RESULTS", "10"))
SLEEP_BETWEEN_QUERIES = float(os.getenv("SLEEP_BETWEEN_QUERIES", "0.2"))

DEFAULT_SOURCES = [s.strip() for s in os.getenv("DEFAULT_SOURCES", "serp,reddit,hn,indiehackers").split(",") if s.strip()]
DEFAULT_PROJECT_CONCURRENCY = int(os.getenv("DEFAULT_PROJECT_CONCURRENCY", "1"))

ALLOWED_LEAD_STATUSES = {"new", "drafted", "queued", "approved", "sent", "replied", "closed", "lost"}

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

def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        return default

def _clean_list(val) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str):
        return [x.strip() for x in val.split(",") if x.strip()]
    return [str(val).strip()] if str(val).strip() else []

def _sleep_jitter(a=0.2, b=1.0):
    time.sleep(random.uniform(a, b))

def _clean_status(s: str) -> str:
    return (s or "").strip().lower()


# -----------------------------
# SERP (Google) Fetcher
# -----------------------------
def fetch_serp_results(query: str, per_query: int) -> List[Dict[str, str]]:
    """
    Returns normalized:
      {title,url,deep_link,snippet,source}
    """
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
                out.append({
                    "title": title,
                    "url": url,
                    "deep_link": url,
                    "snippet": snippet,
                    "source": "serp/google",
                })
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
                out.append({
                    "title": title,
                    "url": url,
                    "deep_link": url,
                    "snippet": snippet,
                    "source": "serp/google",
                })
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

def project_sources(project: dict) -> List[str]:
    s = project.get("sources")
    if isinstance(s, list) and s:
        return [str(x).strip() for x in s if str(x).strip()]
    return DEFAULT_SOURCES

def list_leads(sb, project_id: Optional[str], limit: int, status: Optional[str]) -> List[dict]:
    q = sb.table("leads").select("*").order("created_at", desc=True).limit(limit)
    if project_id:
        q = q.eq("project_id", project_id)
    if status:
        q = q.eq("status", status)
    resp = q.execute()
    return resp.data or []

# -----------------------------
# DEDUPE FIX + SAFE UPSERT
# -----------------------------
def insert_lead(sb, row: dict) -> Optional[dict]:
    """
    Smart upsert:
    - Unique per (project_id, url)
    - Preserves manual fields (status, note, drafts)
    - Improves score if better
    - Merges reasons
    Requires unique index/constraint on (project_id, url).
    """
    project_id = row.get("project_id")
    url = row.get("url")
    if not project_id or not url:
        return None

    existing_resp = (
        sb.table("leads")
        .select("*")
        .eq("project_id", project_id)
        .eq("url", url)
        .limit(1)
        .execute()
    )
    existing_rows = existing_resp.data or []

    if not existing_rows:
        row["status"] = row.get("status") or "new"
        row["created_at"] = row.get("created_at") or now_iso()
        resp = sb.table("leads").insert(row).execute()
        data = resp.data or []
        return data[0] if data else row

    existing = existing_rows[0]

    new_score = int(row.get("score") or 0)
    old_score = int(existing.get("score") or 0)

    merged_reasons = list(set((existing.get("reasons") or []) + (row.get("reasons") or [])))

    update = {
        "content": row.get("content") or existing.get("content"),
        "reasons": merged_reasons,
        "updated_at": now_iso(),
    }

    if new_score > old_score:
        update["score"] = new_score

    resp = sb.table("leads").update(update).eq("id", existing["id"]).execute()
    data = resp.data or []
    return data[0] if data else existing


# -----------------------------
# Source fetching
# -----------------------------
def fetch_from_sources(query: str, project: dict, per_source: int) -> List[Dict[str, str]]:
    """
    Normalized results:
      {title,url,deep_link,snippet,source}
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
        kws = []
        kws.extend(_clean_list(project.get("keywords")))
        if project.get("niche"):
            kws.append(str(project.get("niche")))
        for tok in query.split():
            if len(tok) >= 4:
                kws.append(tok)
        out.extend(fetch_indiehackers_rss(kws, limit=per_source, timeout=HTTP_TIMEOUT))

    seen = set()
    deduped: List[Dict[str, str]] = []
    for r in out:
        key = (r.get("deep_link") or r.get("url") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return deduped


# -----------------------------
# Core scan (used by worker)
# -----------------------------
def run_project_scan(
    sb,
    *,
    user_id: str,
    user_email: str,
    project: dict,
    max_queries: int = MAX_QUERIES_PER_SCAN,
    per_source: int = PER_SOURCE_RESULTS,
) -> Dict[str, Any]:
    """
    Build queries -> cache filter -> fetch sources -> score -> store leads.
    Heavy AI analysis happens via jobs (analyze_lead).
    """
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
    errors: List[str] = []

    for q in queries:
        try:
            results = fetch_from_sources(q, project, per_source=per_source)
            mark_query_run(sb, q)

            for res in results:
                title = (res.get("title") or "").strip()
                deep_link = (res.get("deep_link") or res.get("url") or "").strip()
                snippet = (res.get("snippet") or "").strip()
                source = (res.get("source") or "unknown").strip()

                if not title or not deep_link:
                    continue

                score, intent, reasons = score_lead(
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
                    "url": deep_link,
                    "deep_link": deep_link,
                    "status": "new",
                    "score": int(score),
                    "intent": intent,
                    "reasons": reasons,
                    "created_at": now_iso(),
                }

                saved = insert_lead(sb, lead_row)
                if saved:
                    inserted += 1

                    # Optional notify
                    try:
                        maybe_alert_hot_lead(
                            sb,
                            user_id=user_id,
                            user_email=user_email,
                            lead_id=str(saved.get("id") or deep_link),
                            lead_title=title,
                            lead_url=deep_link,
                            lead_source=source,
                            lead_score=int(score),
                            lead_snippet=snippet,
                            channel="email",
                        )
                    except Exception:
                        pass

            time.sleep(SLEEP_BETWEEN_QUERIES)

        except Exception as e:
            errors.append(f"{q[:80]}... -> {str(e)}")

    return {
        "ok": True,
        "project_id": project_id,
        "queries_ran": len(queries),
        "inserted": inserted,
        "errors": errors[:10],
        "sources": project_sources(project),
    }


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


# -------- Referral (kept for later) --------
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


# -------- Projects --------
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
    update = {
        "name": name,
        "niche": (body.get("niche") or "").strip() or None,
        "keywords": _clean_list(body.get("keywords")),
        "locations": _clean_list(body.get("locations")),
    }
    if "sources" in body:
        update["sources"] = _clean_list(body.get("sources"))
    if "max_concurrent_jobs" in body:
        update["max_concurrent_jobs"] = max(1, _safe_int(body.get("max_concurrent_jobs"), DEFAULT_PROJECT_CONCURRENCY))

    if existing:
        saved = sb.table("projects").update(update).eq("id", existing["id"]).execute()
        return jsonify({"ok": True, "project": (saved.data or [existing])[0], "updated_existing": True})

    row = {
        "owner_id": owner_id,
        "name": name,
        "url": url,
        "niche": update["niche"],
        "keywords": update["keywords"],
        "locations": update["locations"],
        "sources": update.get("sources") or DEFAULT_SOURCES,
        "max_concurrent_jobs": update.get("max_concurrent_jobs") or DEFAULT_PROJECT_CONCURRENCY,
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

    proj = get_project(sb, project_id)
    if not proj:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(proj.get("owner_id")) != user_id:
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
    if "sources" in body:
        update["sources"] = _clean_list(body.get("sources"))
    if "max_concurrent_jobs" in body:
        update["max_concurrent_jobs"] = max(1, _safe_int(body.get("max_concurrent_jobs"), DEFAULT_PROJECT_CONCURRENCY))

    update = {k: v for k, v in update.items() if v is not None and v != ""}
    if not update:
        return jsonify({"ok": False, "error": "No valid fields to update"}), 400

    saved = sb.table("projects").update(update).eq("id", project_id).execute()
    return jsonify({"ok": True, "project": (saved.data or [proj])[0]})


# -------- Scan (enqueue job) --------
@app.post("/projects/<project_id>/scan")
def api_scan_project(project_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    user_email = (body.get("user_email") or "").strip()
    if not user_id or not user_email:
        return jsonify({"ok": False, "error": "user_id and user_email required"}), 400

    proj = get_project(sb, project_id)
    if not proj:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(proj.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    if "serp" in project_sources(proj) and (not SERPAPI_KEY and not SERPER_API_KEY):
        return jsonify({"ok": False, "error": "SERP enabled but no SERP provider configured (SERPAPI_KEY or SERPER_API_KEY)."}), 500

    job = enqueue_job(
        sb,
        owner_id=user_id,
        project_id=project_id,
        job_type="scan_project",
        payload={
            "user_email": user_email,
            "max_queries": _safe_int(body.get("max_queries"), MAX_QUERIES_PER_SCAN),
            "per_source": _safe_int(body.get("per_source"), PER_SOURCE_RESULTS),
        },
        priority=_safe_int(body.get("priority"), 50),
        max_attempts=_safe_int(body.get("max_attempts"), 5),
    )
    return jsonify({"ok": True, "queued": True, "job": job})


# -------- Leads --------
@app.get("/leads")
def api_list_leads():
    sb = get_supabase()
    project_id = (request.args.get("project_id") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    limit = _safe_int(request.args.get("limit"), 50)
    limit = max(1, min(200, limit))
    leads = list_leads(sb, project_id=project_id, limit=limit, status=status)
    return jsonify({"ok": True, "leads": leads})

@app.patch("/leads/<lead_id>/status")
def api_update_lead_status(lead_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    status = _clean_status(body.get("status") or "")

    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400
    if status not in ALLOWED_LEAD_STATUSES:
        return jsonify({"ok": False, "error": f"Invalid status. Allowed: {sorted(list(ALLOWED_LEAD_STATUSES))}"}), 400

    resp = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    lead = rows[0]
    project_id = lead.get("project_id")
    proj = get_project(sb, project_id) if project_id else None
    if not proj:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(proj.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    saved = sb.table("leads").update({
        "status": status,
        "updated_at": now_iso(),
    }).eq("id", lead_id).execute()

    out = (saved.data or [lead])[0]
    return jsonify({"ok": True, "lead": out})

@app.post("/leads/<lead_id>/note")
def api_set_lead_note(lead_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    note = (body.get("note") or "").strip()

    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    resp = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    lead = rows[0]
    project_id = lead.get("project_id")
    proj = get_project(sb, project_id) if project_id else None
    if not proj:
        return jsonify({"ok": False, "error": "Project not found"}), 404
    if str(proj.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    saved = sb.table("leads").update({
        "note": note[:5000],
        "updated_at": now_iso(),
    }).eq("id", lead_id).execute()

    out = (saved.data or [lead])[0]
    return jsonify({"ok": True, "lead": out})


# -------- Jobs --------
@app.post("/jobs")
def api_create_job():
    sb = get_supabase()
    body = request.get_json(force=True) or {}

    user_id = (body.get("user_id") or "").strip()
    job_type = (body.get("type") or "").strip()
    project_id = (body.get("project_id") or "").strip() or None
    payload = body.get("payload") or {}
    priority = _safe_int(body.get("priority"), 100)
    run_after_seconds = _safe_int(body.get("run_after_seconds"), 0)
    max_attempts = _safe_int(body.get("max_attempts"), 5)

    if not user_id or not job_type:
        return jsonify({"ok": False, "error": "user_id and type required"}), 400

    if project_id:
        proj = get_project(sb, project_id)
        if not proj:
            return jsonify({"ok": False, "error": "Project not found"}), 404
        if str(proj.get("owner_id")) != user_id:
            return jsonify({"ok": False, "error": "Forbidden"}), 403

    job = enqueue_job(
        sb,
        owner_id=user_id,
        project_id=project_id,
        job_type=job_type,
        payload=payload,
        priority=priority,
        run_after_seconds=run_after_seconds,
        max_attempts=max_attempts,
    )
    return jsonify({"ok": True, "job": job})

@app.get("/jobs")
def api_list_jobs():
    sb = get_supabase()
    owner_id = (request.args.get("user_id") or "").strip()
    project_id = (request.args.get("project_id") or "").strip() or None
    limit = _safe_int(request.args.get("limit"), 50)
    limit = max(1, min(200, limit))

    if not owner_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    jobs = list_jobs(sb, owner_id=owner_id, project_id=project_id, limit=limit)
    return jsonify({"ok": True, "jobs": jobs})

@app.get("/jobs/<job_id>")
def api_get_job(job_id):
    sb = get_supabase()
    user_id = (request.args.get("user_id") or "").strip()
    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    job = get_job(sb, job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    if str(job.get("owner_id")) != user_id:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    return jsonify({"ok": True, "job": job})

@app.post("/jobs/<job_id>/cancel")
def api_cancel_job(job_id):
    sb = get_supabase()
    body = request.get_json(force=True) or {}
    user_id = (body.get("user_id") or "").strip()

    if not user_id:
        return jsonify({"ok": False, "error": "user_id required"}), 400

    ok = cancel_job(sb, job_id=job_id, owner_id=user_id)
    if not ok:
        return jsonify({"ok": False, "error": "Cannot cancel (not found / forbidden / already finished)"}), 400
    return jsonify({"ok": True, "cancelled": True, "job_id": job_id})


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)