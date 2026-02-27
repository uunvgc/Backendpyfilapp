import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple

# Statuses
JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_SUCCEEDED = "succeeded"
JOB_FAILED = "failed"
JOB_CANCELLED = "cancelled"

VALID_STATUSES = {JOB_QUEUED, JOB_RUNNING, JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.isoformat()

def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


# -----------------------------
# enqueue / fetch / list
# -----------------------------
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
        "status": JOB_QUEUED,
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


# -----------------------------
# concurrency helpers
# -----------------------------
def project_max_concurrency(sb, project_id: str, default: int = 1) -> int:
    resp = sb.table("projects").select("max_concurrent_jobs").eq("id", project_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return default
    return max(1, _safe_int(rows[0].get("max_concurrent_jobs"), default))


def project_running_count(sb, project_id: str) -> int:
    resp = sb.table("jobs").select("id", count="exact").eq("project_id", project_id).eq("status", JOB_RUNNING).execute()
    # supabase-py returns count in resp.count sometimes; be defensive
    c = getattr(resp, "count", None)
    if c is not None:
        return int(c)
    data = resp.data or []
    return len(data)


# -----------------------------
# claim job
# -----------------------------
def claim_next_job(
    sb,
    *,
    worker_id: str,
    stale_lock_minutes: int = 15,
) -> Optional[Dict[str, Any]]:
    """
    Claims 1 job in a safe-ish way without requiring stored procedures.
    Steps:
      1) Find one eligible queued job (sorted by priority, created_at)
      2) If job has project_id, enforce project concurrency
      3) Attempt to update status to running with "eq status=queued" guard
    """

    now = now_utc()
    # 1) get one candidate
    resp = (
        sb.table("jobs")
        .select("*")
        .eq("status", JOB_QUEUED)
        .lte("run_after", iso(now))
        .order("priority", desc=False)
        .order("created_at", desc=False)
        .limit(5)  # pull a few so we can skip ones blocked by concurrency
        .execute()
    )
    candidates = resp.data or []
    if not candidates:
        return None

    for job in candidates:
        job_id = job["id"]
        project_id = job.get("project_id")

        # 2) concurrency gate
        if project_id:
            max_c = project_max_concurrency(sb, project_id, default=1)
            running = project_running_count(sb, project_id)
            if running >= max_c:
                continue  # try next candidate

        # 3) attempt lock
        upd = (
            sb.table("jobs")
            .update({
                "status": JOB_RUNNING,
                "locked_by": worker_id,
                "locked_at": iso(now),
                "attempts": _safe_int(job.get("attempts"), 0) + 1,
            })
            .eq("id", job_id)
            .eq("status", JOB_QUEUED)
            .execute()
        )
        updated = upd.data or []
        if updated:
            return updated[0]

    return None


# -----------------------------
# heartbeats + finish
# -----------------------------
def heartbeat_job(sb, *, job_id: str, worker_id: str):
    sb.table("jobs").update({
        "locked_by": worker_id,
        "locked_at": iso(now_utc()),
    }).eq("id", job_id).eq("status", JOB_RUNNING).execute()


def succeed_job(sb, *, job_id: str, result: Dict[str, Any]):
    sb.table("jobs").update({
        "status": JOB_SUCCEEDED,
        "result": result or {},
        "error": None,
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()


def fail_job(sb, *, job_id: str, error: str):
    sb.table("jobs").update({
        "status": JOB_FAILED,
        "error": (error or "")[:4000],
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()


def retry_job(sb, *, job: Dict[str, Any], error: str, backoff_seconds: int = 30) -> bool:
    attempts = _safe_int(job.get("attempts"), 0)
    max_attempts = _safe_int(job.get("max_attempts"), 5)
    job_id = job["id"]

    if attempts >= max_attempts:
        fail_job(sb, job_id=job_id, error=error)
        return False

    run_after = now_utc() + timedelta(seconds=max(5, int(backoff_seconds)))
    sb.table("jobs").update({
        "status": JOB_QUEUED,
        "run_after": iso(run_after),
        "error": (error or "")[:4000],
        "locked_by": None,
        "locked_at": None,
    }).eq("id", job_id).execute()
    return True


# -----------------------------
# cancellation
# -----------------------------
def cancel_job(sb, *, job_id: str, owner_id: str) -> bool:
    """
    Cancels if job belongs to owner and is queued/running.
    Running jobs may still finish if worker doesn't check cancellation,
    but we also make workers check status before work starts.
    """
    resp = sb.table("jobs").select("*").eq("id", job_id).limit(1).execute()
    rows = resp.data or []
    if not rows:
        return False
    job = rows[0]
    if str(job.get("owner_id")) != str(owner_id):
        return False
    if job.get("status") not in {JOB_QUEUED, JOB_RUNNING}:
        return False

    sb.table("jobs").update({
        "status": JOB_CANCELLED,
        "locked_by": None,
        "locked_at": None,
        "error": None,
    }).eq("id", job_id).execute()
    return True


# -----------------------------
# reaper (stuck running jobs -> queued)
# -----------------------------
def reap_stuck_jobs(
    sb,
    *,
    stale_minutes: int = 15,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Re-queues jobs stuck in running where locked_at < now - stale_minutes.
    """
    cutoff = now_utc() - timedelta(minutes=max(1, int(stale_minutes)))

    resp = (
        sb.table("jobs")
        .select("*")
        .eq("status", JOB_RUNNING)
        .lt("locked_at", iso(cutoff))
        .order("locked_at", desc=False)
        .limit(limit)
        .execute()
    )
    stuck = resp.data or []
    requeued = 0

    for job in stuck:
        sb.table("jobs").update({
            "status": JOB_QUEUED,
            "run_after": iso(now_utc() + timedelta(seconds=10)),
            "locked_by": None,
            "locked_at": None,
            "error": "Reaped: worker heartbeat stale",
        }).eq("id", job["id"]).execute()
        requeued += 1

    return {"stuck_found": len(stuck), "requeued": requeued, "cutoff": iso(cutoff)} 