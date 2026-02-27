import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

import requests
from flask import Flask, request, jsonify
from supabase import create_client

from logic.sources import fetch_reddit, fetch_hn, fetch_indiehackers_rss
from logic.scoring import score_lead
from logic.ai_reply import draft_reply


# -------------------------
# ENV
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()

HTTP_TIMEOUT = 20


# -------------------------
# App
# -------------------------
app = Flask(__name__)
sb = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------
# Helpers
# -------------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_project(project_id: str) -> dict | None:
    resp = sb.table("projects").select("*").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None


def get_lead(lead_id: str) -> dict | None:
    resp = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None


def insert_lead(row: dict):
    """
    Upsert per project.
    Requires UNIQUE(project_id, url)
    """
    if not row.get("project_id") or not row.get("url"):
        return None

    resp = sb.table("leads").upsert(row, on_conflict="project_id,url").execute()
    data = resp.data or []
    return data[0] if data else None


# -------------------------
# SERP Fetch
# -------------------------
def fetch_serp(query: str, limit: int = 10):
    if SERPAPI_KEY:
        params = {
            "engine": "google",
            "q": query,
            "num": limit,
            "api_key": SERPAPI_KEY,
        }
        r = requests.get("https://serpapi.com/search.json", params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        out = []
        for res in (data.get("organic_results") or [])[:limit]:
            out.append({
                "title": res.get("title"),
                "url": res.get("link"),
                "deep_link": res.get("link"),
                "snippet": res.get("snippet"),
                "source": "serp",
                "created_at_iso": None,
                "meta": {}
            })
        return out

    if SERPER_API_KEY:
        headers = {"X-API-KEY": SERPER_API_KEY}
        payload = {"q": query, "num": limit}
        r = requests.post("https://google.serper.dev/search", headers=headers, json=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        out = []
        for res in (data.get("organic") or [])[:limit]:
            out.append({
                "title": res.get("title"),
                "url": res.get("link"),
                "deep_link": res.get("link"),
                "snippet": res.get("snippet"),
                "source": "serp",
                "created_at_iso": None,
                "meta": {}
            })
        return out

    return []


# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True})


# -------------------------
# Scan Route
# -------------------------
@app.post("/scan")
def scan():
    body = request.get_json(force=True)

    project_id = body.get("project_id")
    keywords = body.get("keywords") or []
    max_per_source = int(body.get("limit", 10))

    if not project_id:
        return jsonify({"ok": False, "error": "project_id required"}), 400

    all_results: List[Dict[str, Any]] = []

    # Reddit
    for kw in keywords:
        all_results.extend(fetch_reddit(kw, limit=max_per_source))

    # HN
    for kw in keywords:
        all_results.extend(fetch_hn(kw, limit=max_per_source))

    # IndieHackers
    all_results.extend(fetch_indiehackers_rss(keywords, limit=max_per_source))

    # SERP
    for kw in keywords:
        all_results.extend(fetch_serp(kw, limit=max_per_source))

    inserted = 0

    for res in all_results:
        title = res.get("title") or ""
        snippet = res.get("snippet") or ""
        deep_link = res.get("deep_link") or res.get("url")
        source = res.get("source")

        score, intent, reasons = score_lead(
            title,
            snippet,
            url=deep_link,
            source=source,
            created_at_iso=res.get("created_at_iso"),
            meta=res.get("meta"),
        )

        row = {
            "project_id": project_id,
            "title": title[:500],
            "content": snippet[:4000],
            "url": deep_link,
            "source": source,
            "score": score,
            "intent": intent,
            "reasons": reasons,
            "created_at": now_iso(),
            "ai_lock": False,
        }

        saved = insert_lead(row)
        if saved:
            inserted += 1

        time.sleep(0.05)

    return jsonify({
        "ok": True,
        "inserted": inserted,
        "total_fetched": len(all_results),
    })


# -------------------------
# List Leads
# -------------------------
@app.get("/leads")
def list_leads():
    project_id = request.args.get("project_id")
    limit = int(request.args.get("limit", 50))

    if not project_id:
        return jsonify({"ok": False, "error": "project_id required"}), 400

    resp = (
        sb.table("leads")
        .select("*")
        .eq("project_id", project_id)
        .order("score", desc=True)
        .limit(limit)
        .execute()
    )

    return jsonify({"ok": True, "leads": resp.data or []})


# -------------------------
# Draft Reply (AI)
# -------------------------
@app.post("/leads/<lead_id>/draft")
def draft_lead_reply(lead_id: str):
    body = request.get_json(force=True) or {}
    user_id = (body.get("user_id") or "").strip()  # optional for now
    # (You can enforce ownership later. For now it's open within your API-key protected backend.)

    lead = get_lead(lead_id)
    if not lead:
        return jsonify({"ok": False, "error": "lead not found"}), 404

    if bool(lead.get("ai_lock")):
        return jsonify({"ok": False, "error": "ai_lock true (user edited). unlock to regenerate."}), 409

    project_id = lead.get("project_id")
    proj = get_project(project_id) if project_id else None
    if not proj:
        return jsonify({"ok": False, "error": "project not found"}), 404

    drafted = draft_reply(lead=lead, project=proj)
    msg = (drafted.get("message") or "").strip()

    sb.table("leads").update({
        "ai_draft": msg[:4000],
        "ai_draft_updated_at": now_iso(),
    }).eq("id", lead_id).execute()

    return jsonify({"ok": True, "lead_id": lead_id, "draft": drafted})


# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)