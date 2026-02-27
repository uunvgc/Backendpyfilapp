"""
FIILTHY Backend (Flask) — full app.py

What this includes:
- Flask + CORS
- Supabase client (SERVICE_ROLE key on server)
- /health
- /leads (list with filters)
- /actions/enqueue (push a job into actions_queue)
- /actions/worker/tick (manual tick endpoint for debugging)
- Background worker loop (runs automatically on Render) that:
  - locks next pending job (Postgres RPC)
  - executes it
  - marks done/failed (Postgres RPC)

IMPORTANT (Supabase SQL you MUST run once):
- Create RPC functions:
  - public.dequeue_action(p_project_id uuid default null) returns public.actions_queue
  - public.finish_action(p_id uuid, p_status text, p_error text default null)
(If you already ran them, you're good.)
"""

import os
import time
import threading
from typing import Any, Dict, Optional

from flask import Flask, request, jsonify
from flask_cors import CORS

from supabase import create_client

# -----------------------------
# ENV
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()  # SERVICE_ROLE on server (Render)

FIILTHY_WORKER_ENABLED = os.getenv("FIILTHY_WORKER_ENABLED", "1").strip() == "1"
FIILTHY_WORKER_LOCKED_BY = os.getenv("FIILTHY_WORKER_LOCKED_BY", "worker").strip()

FIILTHY_WORKER_SLEEP_IDLE = float(os.getenv("FIILTHY_WORKER_SLEEP_IDLE", "2.0"))
FIILTHY_WORKER_SLEEP_BUSY = float(os.getenv("FIILTHY_WORKER_SLEEP_BUSY", "0.2"))

# optional: only work a specific project
FIILTHY_WORKER_PROJECT_ID = os.getenv("FIILTHY_WORKER_PROJECT_ID", "").strip() or None

# -----------------------------
# Supabase
# -----------------------------
if not SUPABASE_URL or not SUPABASE_KEY:
    # Don't crash on import; but endpoints will fail until env vars are set.
    sb = None
else:
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# Flask app
# -----------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# -----------------------------
# Helpers
# -----------------------------
def ok(data: Any = None, status: int = 200):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    return jsonify(payload), status


def err(message: str, status: int = 400, extra: Any = None):
    payload = {"ok": False, "error": message}
    if extra is not None:
        payload["extra"] = extra
    return jsonify(payload), status


def require_supabase():
    if sb is None:
        return False, err(
            "Supabase not configured. Set SUPABASE_URL and SUPABASE_KEY on the server.",
            500,
        )
    return True, None


# -----------------------------
# Health
# -----------------------------
@app.get("/health")
def health():
    return ok(
        {
            "service": "fiilthy-backend",
            "worker_enabled": FIILTHY_WORKER_ENABLED,
            "project_lock": FIILTHY_WORKER_PROJECT_ID,
        }
    )


# -----------------------------
# Leads API (basic list)
# -----------------------------
@app.get("/leads")
def leads():
    good, resp = require_supabase()
    if not good:
        return resp

    project_id = request.args.get("project_id", "").strip()
    if not project_id:
        return err("project_id is required", 400)

    limit = int(request.args.get("limit", "50"))
    status = request.args.get("status", "").strip() or None
    min_score = request.args.get("min_score", "").strip() or None

    q = sb.table("leads").select("*").eq("project_id", project_id).order(
        "created_at", desc=True
    )

    if status:
        q = q.eq("status", status)

    if min_score is not None:
        try:
            q = q.gte("score", float(min_score))
        except Exception:
            return err("min_score must be a number", 400)

    if limit > 200:
        limit = 200
    q = q.limit(limit)

    try:
        res = q.execute()
        return ok(res.data or [])
    except Exception as e:
        return err("Failed to fetch leads", 500, str(e))


# -----------------------------
# Actions Queue API
# -----------------------------
@app.post("/actions/enqueue")
def actions_enqueue():
    """
    Body JSON:
    {
      "project_id": "<uuid>",
      "type": "ping" | "enrich_lead" | ...,
      "payload": {...},
      "priority": 0,
      "run_at": "2026-02-26T00:00:00Z" (optional)
    }
    """
    good, resp = require_supabase()
    if not good:
        return resp

    body = request.get_json(silent=True) or {}
    project_id = (body.get("project_id") or "").strip()
    action_type = (body.get("type") or "").strip()

    if not project_id:
        return err("project_id is required", 400)
    if not action_type:
        return err("type is required", 400)

    payload = body.get("payload") or {}
    priority = int(body.get("priority") or 0)
    run_at = body.get("run_at")  # optional

    row = {
        "project_id": project_id,
        "type": action_type,
        "payload": payload,
        "priority": priority,
    }
    if run_at:
        row["run_at"] = run_at

    try:
        res = sb.table("actions_queue").insert(row).execute()
        return ok(res.data[0] if res.data else row, 201)
    except Exception as e:
        return err("Failed to enqueue action", 500, str(e))


@app.get("/actions")
def actions_list():
    """Debug list recent actions."""
    good, resp = require_supabase()
    if not good:
        return resp

    project_id = request.args.get("project_id", "").strip()
    limit = int(request.args.get("limit", "50"))
    if limit > 200:
        limit = 200

    q = sb.table("actions_queue").select("*").order("created_at", desc=True).limit(limit)
    if project_id:
        q = q.eq("project_id", project_id)

    try:
        res = q.execute()
        return ok(res.data or [])
    except Exception as e:
        return err("Failed to list actions", 500, str(e))


# -----------------------------
# Worker (queue runner)
# -----------------------------
def execute_action(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add your real automation types here.
    job fields:
      id, project_id, type, payload, status, priority, run_at, attempts, max_attempts...
    """
    action_type = job.get("type")
    payload = job.get("payload") or {}

    # Example: quick smoke test
    if action_type == "ping":
        return {"ok": True, "message": "pong"}

    # Example: enrich lead (hook your existing code here)
    if action_type == "enrich_lead":
        lead_id = payload.get("lead_id")
        if not lead_id:
            raise ValueError("payload.lead_id is required for enrich_lead")

        # TODO: your enrich logic (fetch lead -> enrich -> update lead)
        # sb.table("leads").update({...}).eq("id", lead_id).execute()
        return {"ok": True, "lead_id": lead_id, "enriched": True}

    raise ValueError(f"Unknown action type: {action_type}")


def dequeue_one() -> Optional[Dict[str, Any]]:
    """
    Calls your Postgres RPC: public.dequeue_action
    Must exist in Supabase.
    """
    args = {}
    if FIILTHY_WORKER_PROJECT_ID:
        args["p_project_id"] = FIILTHY_WORKER_PROJECT_ID

    res = sb.rpc("dequeue_action", args).execute()
    return res.data  # dict or None


def finish_one(job_id: str, status: str, error: Optional[str] = None):
    """
    Calls your Postgres RPC: public.finish_action
    Must exist in Supabase.
    """
    sb.rpc(
        "finish_action",
        {"p_id": job_id, "p_status": status, "p_error": error},
    ).execute()


def worker_tick() -> Dict[str, Any]:
    """
    Runs exactly 1 job if available.
    Returns a dict with what happened.
    """
    job = dequeue_one()
    if not job:
        return {"ran": False}

    job_id = job["id"]
    try:
        result = execute_action(job)
        finish_one(job_id, "done", None)
        return {"ran": True, "id": job_id, "status": "done", "result": result}
    except Exception as e:
        finish_one(job_id, "failed", str(e))
        return {"ran": True, "id": job_id, "status": "failed", "error": str(e)}


def worker_loop():
    while True:
        try:
            out = worker_tick()
            if not out.get("ran"):
                time.sleep(FIILTHY_WORKER_SLEEP_IDLE)
            else:
                time.sleep(FIILTHY_WORKER_SLEEP_BUSY)
        except Exception:
            # Avoid crashing the service if Supabase/network hiccups
            time.sleep(3)


def start_worker_once():
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()


# Manual tick endpoint (debug on phone / postman)
@app.post("/actions/worker/tick")
def worker_tick_endpoint():
    good, resp = require_supabase()
    if not good:
        return resp
    try:
        out = worker_tick()
        return ok(out)
    except Exception as e:
        return err("Worker tick failed", 500, str(e))


# Start background worker when the app boots (Render)
if FIILTHY_WORKER_ENABLED and sb is not None:
    start_worker_once()


# -----------------------------
# Run local
# -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
import time

def process_queue():
    while True:
        try:
            jobs = sb.table("actions_queue") \
                .select("*") \
                .eq("status", "pending") \
                .limit(5) \
                .execute()

            for job in jobs.data:
                print("Processing:", job["id"])

                # Example action
                if job["type"] == "ping":
                    print("Ping received")

                # Mark done
                sb.table("actions_queue") \
                    .update({"status": "done"}) \
                    .eq("id", job["id"]) \
                    .execute()

        except Exception as e:
            print("Worker error:", e)

        time.sleep(5)