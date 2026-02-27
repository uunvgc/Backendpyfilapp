# app.py
import os
import time
import json
import threading
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client

# -----------------------------
# ENV
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()  # SERVICE_ROLE (backend only)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

WORKER_ENABLED = os.getenv("WORKER", "0").strip() == "1"
WORKER_POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "3"))
LOCK_TTL_SECONDS = int(os.getenv("LOCK_TTL_SECONDS", "120"))  # re-claim stuck jobs after this

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
FIILTHY_USER_AGENT = os.getenv("FIILTHY_USER_AGENT", "fiilthy/1.0 (+https://fiilthy)")

# -----------------------------
# INIT
# -----------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

if not SUPABASE_URL or not SUPABASE_KEY:
    # Don’t crash at import-time; health/envcheck will show missing.
    sb = None
else:
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

_worker_started = False
_worker_last_tick = None
_worker_last_error = None


# -----------------------------
# HELPERS
# -----------------------------
def utcnow():
    return datetime.now(timezone.utc)


def _safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None


def openai_chat_json(system: str, user: str, schema_hint: str = "") -> dict:
    """
    Minimal OpenAI call via HTTPS. Returns dict (best effort).
    Requires OPENAI_API_KEY in env.
    """
    if not OPENAI_API_KEY:
        return {"error": "OPENAI_API_KEY not set"}

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": FIILTHY_USER_AGENT,
    }
    payload = {
        "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": (user.strip() + ("\n\n" + schema_hint.strip() if schema_hint else "")).strip()},
        ],
    }

    r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    text = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""
    # Try parse JSON from the response (best effort)
    parsed = _safe_json_loads(text)
    if isinstance(parsed, dict):
        return parsed

    # If model returned prose, wrap it
    return {"text": text}


# -----------------------------
# API ROUTES
# -----------------------------
@app.get("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "supabase_configured": bool(SUPABASE_URL and SUPABASE_KEY),
            "worker_enabled": WORKER_ENABLED,
        }
    )


@app.get("/envcheck")
def envcheck():
    return jsonify(
        {
            "SUPABASE_URL_set": bool(SUPABASE_URL),
            "SUPABASE_KEY_set": bool(SUPABASE_KEY),
            "OPENAI_API_KEY_set": bool(OPENAI_API_KEY),
            "WORKER": os.getenv("WORKER", None),
        }
    )


@app.get("/worker_status")
def worker_status():
    return jsonify(
        {
            "enabled": WORKER_ENABLED,
            "started": _worker_started,
            "last_tick_utc": _worker_last_tick,
            "last_error": _worker_last_error,
            "poll_seconds": WORKER_POLL_SECONDS,
            "lock_ttl_seconds": LOCK_TTL_SECONDS,
        }
    )


@app.post("/queue/enqueue")
def enqueue_action():
    """
    Body:
    {
      "project_id": "...uuid...",
      "type": "enrich_lead" | "scan_target" | "...",
      "payload": { ...anything... }
    }
    """
    if sb is None:
        return jsonify({"error": "Supabase not configured"}), 500

    body = request.get_json(force=True, silent=True) or {}
    project_id = (body.get("project_id") or "").strip()
    action_type = (body.get("type") or "").strip()
    payload = body.get("payload") or {}

    if not project_id or not action_type:
        return jsonify({"error": "project_id and type are required"}), 400

    row = {
        "project_id": project_id,
        "type": action_type,
        "payload": payload,
        "status": "pending",
    }

    res = sb.table("actions_queue").insert(row).execute()
    return jsonify({"ok": True, "inserted": res.data})


@app.post("/queue/run_once")
def run_worker_once():
    """
    Manual trigger for debugging (no need to wait for the thread).
    """
    if sb is None:
        return jsonify({"error": "Supabase not configured"}), 500
    result = process_actions_queue_once()
    return jsonify({"ok": True, "result": result})


@app.get("/leads")
def leads():
    """
    Example leads endpoint (adjust to your schema):
      /leads?project_id=...&limit=50&status=new&min_score=0.4
    """
    if sb is None:
        return jsonify({"error": "Supabase not configured"}), 500

    project_id = (request.args.get("project_id") or "").strip()
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    limit = int(request.args.get("limit", "50"))
    status = request.args.get("status")
    min_score = request.args.get("min_score")

    q = sb.table("leads").select("*").eq("project_id", project_id).limit(limit)

    if status:
        q = q.eq("status", status)

    if min_score is not None and str(min_score).strip() != "":
        try:
            q = q.gte("score", float(min_score))
        except Exception:
            return jsonify({"error": "min_score must be a number"}), 400

    res = q.order("created_at", desc=True).execute()
    return jsonify({"ok": True, "data": res.data})


# -----------------------------
# WORKER CORE
# -----------------------------
def claim_one_pending_action():
    """
    Single-worker friendly claim:
    - Find one pending action where lock is free/expired
    - Lock it (set locked_at/locked_by/status)
    NOTE: If you later scale to multiple workers, switch to a SQL RPC function for atomic claim.
    """
    now = utcnow()
    lock_expired_before = (now - timedelta(seconds=LOCK_TTL_SECONDS)).isoformat()

    # find one claimable row
    # claimable if:
    #   status = 'pending' OR (status='working' and locked_at older than TTL)
    # We keep it simple: prefer pending first.
    res = (
        sb.table("actions_queue")
        .select("*")
        .or_(f"status.eq.pending,and(status.eq.working,locked_at.lt.{lock_expired_before})")
        .order("created_at", desc=False)
        .limit(1)
        .execute()
    )

    rows = res.data or []
    if not rows:
        return None

    row = rows[0]
    action_id = row["id"]

    # lock it (optimistic: only lock if still claimable)
    locked_by = os.getenv("RENDER_SERVICE_NAME", "render-web")
    upd = (
        sb.table("actions_queue")
        .update(
            {
                "status": "working",
                "locked_at": now.isoformat(),
                "locked_by": locked_by,
                "error": None,
            }
        )
        .eq("id", action_id)
        .in_("status", ["pending", "working"])  # allow re-claim of expired working
        .execute()
    )

    if not upd.data:
        return None

    # return latest row state
    return upd.data[0]


def mark_action_done(action_id: str, result: dict | None = None):
    now = utcnow().isoformat()
    payload = {"status": "done", "done_at": now, "error": None}
    if result is not None:
        payload["result"] = result
    sb.table("actions_queue").update(payload).eq("id", action_id).execute()


def mark_action_error(action_id: str, err: str):
    now = utcnow().isoformat()
    sb.table("actions_queue").update({"status": "error", "done_at": now, "error": err[:4000]}).eq("id", action_id).execute()


def handle_action(row: dict) -> dict:
    """
    Implement your automation here.
    Supported examples:
      - enrich_lead: expects payload.lead_id
      - scan_target: expects payload.target_url (placeholder)
    """
    action_type = row.get("type")
    payload = row.get("payload") or {}

    if action_type == "enrich_lead":
        lead_id = (payload.get("lead_id") or "").strip()
        if not lead_id:
            return {"skipped": True, "reason": "payload.lead_id missing"}

        # Load lead
        lead_res = sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()
        lead = (lead_res.data or [None])[0]
        if not lead:
            return {"skipped": True, "reason": f"lead not found: {lead_id}"}

        # Ask OpenAI to score + draft reply (best effort JSON)
        system = "You are a lead qualification assistant for a SaaS that finds people asking for help online."
        user = f"""
Lead:
title: {lead.get('title','')}
content: {lead.get('content','')}
source: {lead.get('source','')}
permalink: {lead.get('permalink','')}

Return JSON only.
"""
        schema_hint = """
JSON schema:
{
  "score": number,          // 0..1
  "reason": string,         // why it's a good lead
  "suggested_reply": string // short helpful reply (no spam)
}
"""
        ai = openai_chat_json(system, user, schema_hint=schema_hint)

        # Update lead with enrichment if fields exist in your schema
        update = {}
        if isinstance(ai, dict) and "score" in ai:
            try:
                update["score"] = float(ai["score"])
            except Exception:
                pass
        if isinstance(ai, dict) and "reason" in ai:
            update["ai_reason"] = str(ai["reason"])[:4000]
        if isinstance(ai, dict) and "suggested_reply" in ai:
            update["suggested_reply"] = str(ai["suggested_reply"])[:4000]

        if update:
            sb.table("leads").update(update).eq("id", lead_id).execute()

        return {"lead_id": lead_id, "enriched": True, "ai": ai}

    if action_type == "scan_target":
        # Placeholder: you can plug in your crawler/scanner here
        target_url = (payload.get("target_url") or "").strip()
        if not target_url:
            return {"skipped": True, "reason": "payload.target_url missing"}
        return {"scanned": True, "target_url": target_url, "note": "scanner not implemented in this file"}

    return {"skipped": True, "reason": f"unknown action type: {action_type}"}


def process_actions_queue_once():
    """
    Claims a single action and processes it.
    Returns a small status dict for debugging.
    """
    global _worker_last_error

    if sb is None:
        return {"error": "Supabase not configured"}

    row = claim_one_pending_action()
    if not row:
        return {"claimed": False}

    action_id = row["id"]
    try:
        result = handle_action(row)
        mark_action_done(action_id, result=result)
        return {"claimed": True, "action_id": action_id, "result": result}
    except Exception as e:
        err = repr(e)
        _worker_last_error = err
        mark_action_error(action_id, err=err)
        return {"claimed": True, "action_id": action_id, "error": err}


def worker_loop():
    global _worker_last_tick, _worker_last_error
    while True:
        try:
            _worker_last_tick = utcnow().isoformat()
            process_actions_queue_once()
            time.sleep(WORKER_POLL_SECONDS)
        except Exception as e:
            _worker_last_error = repr(e)
            time.sleep(5)


def start_worker_if_enabled():
    global _worker_started
    if _worker_started:
        return
    if not WORKER_ENABLED:
        print("Worker disabled (set WORKER=1 to enable).")
        return
    _worker_started = True
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    print("Worker thread started ✅")


# Start the worker when the process boots (Render/Gunicorn loads this module)
start_worker_if_enabled()


# Optional: allow local run
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True) 