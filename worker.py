import os
import time
import json
import socket
from datetime import datetime, timezone, timedelta

from supabase import create_client, Client

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
NOW = lambda: datetime.now(timezone.utc)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Optional: if you have external APIs later
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Tuning
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))  # how often we check queues
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
LOCK_TTL_MINUTES = int(os.environ.get("LOCK_TTL_MINUTES", "10"))

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ----------------------------
# Queue helpers
# ----------------------------
def pick_jobs(table: str, limit: int = 10):
    """
    Pick 'queued' jobs that are due (run_at <= now), and not locked recently.
    We'll "lock" them by setting locked_at/locked_by + status='processing'.
    """
    lock_expired_before = (NOW() - timedelta(minutes=LOCK_TTL_MINUTES)).isoformat()

    # Step 1: fetch candidate jobs
    candidates = (
        sb.table(table)
        .select("*")
        .eq("status", "queued")
        .lte("run_at", NOW().isoformat())
        .or_(f"locked_at.is.null,locked_at.lt.{lock_expired_before}")
        .order("run_at", desc=False)
        .limit(limit)
        .execute()
        .data
    )

    picked = []
    for job in candidates:
        job_id = job["id"]

        # Step 2: attempt to lock
        # Only lock if still queued (prevents races)
        updated = (
            sb.table(table)
            .update(
                {
                    "status": "processing",
                    "locked_at": NOW().isoformat(),
                    "locked_by": WORKER_ID,
                }
            )
            .eq("id", job_id)
            .eq("status", "queued")
            .execute()
            .data
        )

        if updated:
            picked.append(updated[0])

    return picked


def mark_done(table: str, job_id: int):
    sb.table(table).update(
        {
            "status": "done",
            "locked_at": None,
            "locked_by": None,
            "last_error": None,
        }
    ).eq("id", job_id).execute()


def mark_failed(table: str, job: dict, err: Exception):
    attempts = int(job.get("attempts") or 0) + 1
    status = "dead" if attempts >= MAX_ATTEMPTS else "queued"

    # simple backoff: 1m, 2m, 4m, 8m...
    delay_minutes = min(60, 2 ** (attempts - 1))
    run_at = (NOW() + timedelta(minutes=delay_minutes)).isoformat()

    sb.table(table).update(
        {
            "status": status,
            "attempts": attempts,
            "run_at": run_at if status == "queued" else job.get("run_at"),
            "locked_at": None,
            "locked_by": None,
            "last_error": f"{type(err).__name__}: {str(err)}",
        }
    ).eq("id", job["id"]).execute()


# ----------------------------
# Your FIILTHY logic (plug-ins)
# ----------------------------
def handle_action(job: dict):
    """
    actions_queue payload examples:
      { "type": "scan_site", "site_id": 123 }
      { "type": "find_intent", "site_id": 123, "keywords": ["crm", "saas"] }
      { "type": "match_leads", "site_id": 123 }
    """
    payload = job.get("payload") or {}
    job_type = payload.get("type")

    if job_type == "scan_site":
        scan_site(payload)
    elif job_type == "find_intent":
        find_intent(payload)
    elif job_type == "match_leads":
        match_leads(payload)
    else:
        raise ValueError(f"Unknown action type: {job_type}")


def scan_site(payload: dict):
    """
    Minimal version: marks a site as scanned and stores extracted keywords/niche placeholders.
    Replace this with your real crawler / analyzer.
    """
    site_id = payload["site_id"]

    site = sb.table("sites").select("*").eq("id", site_id).single().execute().data
    url = site.get("url") or site.get("site_url") or site.get("domain")

    # TODO: real fetch + parse + scoring
    # For now: pretend we extracted these
    extracted = {
        "scanned_at": NOW().isoformat(),
        "score": site.get("score") or 72,
        "keywords": site.get("keywords") or ["saas", "automation", "leads"],
        "niche": site.get("niche") or "b2b",
        "summary": site.get("summary") or f"Auto-scanned: {url}",
    }

    sb.table("sites").update(extracted).eq("id", site_id).execute()

    # Next actions: find intent + match leads
    enqueue_action({"type": "find_intent", "site_id": site_id, "keywords": extracted["keywords"]})
    enqueue_action({"type": "match_leads", "site_id": site_id})


def find_intent(payload: dict):
    """
    Minimal version: write intent_events stub rows.
    Replace with: reddit/x/linkedin/google alerts/etc ingestion.
    """
    site_id = payload["site_id"]
    keywords = payload.get("keywords") or []

    # Example intent event
    event = {
        "site_id": site_id,
        "source": "stub",
        "query": " OR ".join(keywords[:5]) if keywords else "general",
        "intent_score": 0.65,
        "raw": {"note": "Replace with real intent ingestion"},
        "created_at": NOW().isoformat(),
    }

    # If your intent_events columns differ, adjust keys.
    sb.table("intent_events").insert(event).execute()


def match_leads(payload: dict):
    """
    Minimal version: create a lead stub and queue outreach.
    Replace with your actual lead-finding logic + enrichment.
    """
    site_id = payload["site_id"]

    lead = {
        "site_id": site_id,
        "source": "stub",
        "handle": "example_user",
        "platform": "reddit",
        "intent_score": 0.72,
        "raw": {"note": "Replace with real lead match"},
        "created_at": NOW().isoformat(),
    }

    inserted = sb.table("leads").insert(lead).execute().data
    lead_id = inserted[0]["id"] if inserted else None

    if lead_id:
        enqueue_outreach({
            "type": "dm",
            "site_id": site_id,
            "lead_id": lead_id,
            "message": "Hey! Saw you were looking for help—quick question: what’s your budget/timeline?",
        })


def handle_outreach(job: dict):
    """
    outreach_queue payload examples:
      { "type": "dm", "platform": "reddit", "lead_id": 55, "message": "..." }
    This worker does NOT actually send DMs unless you wire an integration.
    For now it marks as 'sent' and records what WOULD be sent.
    """
    payload = job.get("payload") or {}

    # TODO: integrate real senders (email, reddit bot, etc)
    # For now, store the “sent” record in posts table (or outreach log)
    record = {
        "source": payload.get("platform", "stub"),
        "lead_id": payload.get("lead_id"),
        "site_id": payload.get("site_id"),
        "content": payload.get("message"),
        "status": "sent_stub",
        "created_at": NOW().isoformat(),
    }

    # If your posts table has different columns, adjust.
    sb.table("posts").insert(record).execute()


def enqueue_action(payload: dict, run_at: datetime | None = None):
    sb.table("actions_queue").insert(
        {
            "status": "queued",
            "run_at": (run_at or NOW()).isoformat(),
            "payload": payload,
        }
    ).execute()


def enqueue_outreach(payload: dict, run_at: datetime | None = None):
    sb.table("outreach_queue").insert(
        {
            "status": "queued",
            "run_at": (run_at or NOW()).isoformat(),
            "payload": payload,
        }
    ).execute()


# ----------------------------
# Main loop
# ----------------------------
def run_forever():
    print(f"[{WORKER_ID}] FIILTHY worker starting…")

    while True:
        # Actions
        action_jobs = pick_jobs("actions_queue", limit=BATCH_SIZE)
        for job in action_jobs:
            try:
                handle_action(job)
                mark_done("actions_queue", job["id"])
            except Exception as e:
                mark_failed("actions_queue", job, e)

        # Outreach
        outreach_jobs = pick_jobs("outreach_queue", limit=BATCH_SIZE)
        for job in outreach_jobs:
            try:
                handle_outreach(job)
                mark_done("outreach_queue", job["id"])
            except Exception as e:
                mark_failed("outreach_queue", job, e)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_forever()