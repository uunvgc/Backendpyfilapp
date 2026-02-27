import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.isoformat()

def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def enqueue_job(
    sb,
    *,
    owner_id: str,
    job_type: str,
    payload: Dict[str, Any],
    project_id: Optional[str] = None,
    priority: int = 100,
    run_after_seconds: int = 0,
    max_attempts: int = 5,
) -> Dict[str, Any]:
    run_after = now_utc() + timedelta(seconds=max(0, int(run_after_seconds)))
    row = {
        "owner_id": owner_id,
        "project_id": project_id,
        "type": job_type,
        "payload": payload or {},
        "status": "queued",
        "priority": int(priority),
        "attempts": 0,
        "max_attempts": int(max_attempts),
        "locked_by": None,
        "locked_at": None,
        "run_after": iso(run_after),
    }
    resp = sb.table("jobs").insert(row).execute()
    data = resp.data or []
    return data[0] if data else row


def get_job(sb, job_id: str) -> Optional[Dict[str, Any]]:
    resp = sb.table("jobs").select("*").eq("id", job_id).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None


def list_jobs(sb, *, owner_id: str, project_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    q = sb.table("jobs").select("*").eq("owner_id", owner_id).order("created_at", desc=True).limit(limit)
    if project_id:
        q = q.eq("project_id", project_id)
    resp = q.execute()
    return resp.data or []


def claim_next_job(sb, *, worker_id: str, lock_minutes: int = 10) -> Optional[Dict[str, Any]]:
    """
    Claims exactly one job using a two-step approach:
    1) Fetch one eligible job
    2) Attempt to lock it (update where status=queued and id=...)
    If lock fails, return None.
    """
    now = now_utc()
    lock_time = iso(now)

    # 1) get candidate
    resp = (
        sb.table("jobs")
        .select("*")
        .eq("status", "queued")
        .lte("run_after", iso(now))
        .order("priority", desc=False)
        .order("created_at", desc=False)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None

    job = rows[0]
    job_id = job["id"]

    # 2) lock (best-effort)
    # Note: supabase-py doesn't support atomic "update returning * with condition" perfectly,
    # but this is usually fine if you only run 1-2 workers early on.
    upd = (
        sb.table("jobs")
        .update({
            "status": "running",
            "locked_by": worker_id,
            "locked_at": lock_time,
            "attempts": _safe_int(job.get("attempts"), 0) + 1,
        })
        .eq("id", job_id)
        .eq("status", "queued")
        .execute()
    )

    updated = upd.data or []
    return updated[0] if updated else None


def heartbeat_job(sb, *, job_id: str, worker_id: str):
    sb.table("jobs").update({
        "locked_by": worker_id,
        "locked_at": iso(now_utc()),
    }).eq("id", job_id).eq("status", "running").execute()


def succeed_job(sb, *, job_id: str, result: Dict[str, Any]):
    sb.table("jobs").update({
        "status": "succeeded",
        "result": result or {},
        "error": None,
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()


def fail_job(sb, *, job_id: str, error: str):
    sb.table("jobs").update({
        "status": "failed",
        "error": (error or "")[:4000],
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()


def retry_job(sb, *, job: Dict[str, Any], error: str, backoff_seconds: int = 30) -> bool:
    """
    If attempts < max_attempts, requeue with run_after += backoff.
    Returns True if requeued, False if permanently failed.
    """
    attempts = _safe_int(job.get("attempts"), 0)
    max_attempts = _safe_int(job.get("max_attempts"), 5)
    job_id = job["id"]

    if attempts >= max_attempts:
        fail_job(sb, job_id=job_id, error=error)
        return False

    run_after = now_utc() + timedelta(seconds=max(5, int(backoff_seconds)))
    sb.table("jobs").update({
        "status": "queued",
        "run_after": iso(run_after),
        "error": (error or "")[:4000],
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()
    return True