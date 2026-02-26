# logic/billing.py
#
# Enforces plans + quotas server-side (do NOT rely on frontend).
# Works with Supabase tables:
#   - users (must contain: plan text default 'free')
#   - usage_counters (recommended)
#   - credit_ledger (optional; for audits and billing)
#
# Typical usage in scanner:
#   from logic.billing import can_use, consume
#   if can_use(sb, user_id, "serp_queries", amount=n):
#       consume(sb, user_id, "serp_queries", amount=n, meta={...})
#       ...run serp...
#
# Env var overrides:
#   BILLING_PERIOD_DAYS (default 30)


from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional


BILLING_PERIOD_DAYS = int(os.getenv("BILLING_PERIOD_DAYS", "30"))


# -------------------------
# Plan definitions
# -------------------------

@dataclass(frozen=True)
class PlanLimits:
    scans: int
    serp_queries: int
    deep_fetches: int
    ai_classifications: int
    notifications: int


# You can change these numbers anytime.
PLAN_LIMITS: Dict[str, PlanLimits] = {
    "free": PlanLimits(
        scans=200,           # per billing period
        serp_queries=200,    # expensive
        deep_fetches=50,     # expensive
        ai_classifications=0,  # off by default
        notifications=100,
    ),
    "pro": PlanLimits(
        scans=5000,
        serp_queries=4000,
        deep_fetches=1500,
        ai_classifications=1500,
        notifications=5000,
    ),
    "agency": PlanLimits(
        scans=50000,
        serp_queries=30000,
        deep_fetches=20000,
        ai_classifications=20000,
        notifications=50000,
    ),
}


# Map actions to columns in usage_counters
ACTION_TO_FIELD = {
    "scans": "scans",
    "serp_queries": "serp_queries",
    "deep_fetches": "deep_fetches",
    "ai_classifications": "ai_classifications",
    "notifications": "notifications",
}


# -------------------------
# Period helpers
# -------------------------

def current_period_start(now_utc: Optional[datetime] = None) -> datetime:
    """
    Simple rolling period: last BILLING_PERIOD_DAYS days.
    You can switch to calendar-month periods later.
    """
    now = now_utc or datetime.now(timezone.utc)
    return now - timedelta(days=BILLING_PERIOD_DAYS)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


# -------------------------
# Supabase reads
# -------------------------

def get_user_plan(supabase, user_id: str) -> str:
    """
    Requires users table with `id` and `plan` fields.
    If missing, defaults to 'free'.
    """
    try:
        resp = (
            supabase.table("users")
            .select("plan")
            .eq("id", user_id)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            return "free"
        plan = (rows[0].get("plan") or "free").strip().lower()
        return plan if plan in PLAN_LIMITS else "free"
    except Exception:
        return "free"


def get_limits(plan: str) -> PlanLimits:
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])


def get_usage(supabase, user_id: str) -> dict:
    """
    Reads usage_counters row for user_id for the current period.
    """
    start = current_period_start()
    resp = (
        supabase.table("usage_counters")
        .select("*")
        .eq("user_id", user_id)
        .eq("period_start", _iso(start))
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else {}


# -------------------------
# Core API
# -------------------------

def can_use(supabase, user_id: str, action: str, amount: int = 1) -> bool:
    """
    Returns True if user has quota remaining for the action.
    """
    action = action.strip().lower()
    if action not in ACTION_TO_FIELD:
        return False

    plan = get_user_plan(supabase, user_id)
    limits = get_limits(plan)

    limit_value = getattr(limits, action)
    if limit_value <= 0:
        return False

    start = current_period_start()
    start_iso = _iso(start)

    # read or init usage row
    resp = (
        supabase.table("usage_counters")
        .select("*")
        .eq("user_id", user_id)
        .eq("period_start", start_iso)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        used = 0
    else:
        used = int(rows[0].get(ACTION_TO_FIELD[action], 0) or 0)

    return (used + amount) <= limit_value


def consume(supabase, user_id: str, action: str, amount: int = 1, meta: Optional[dict] = None):
    """
    Increment usage counters and optionally write to credit_ledger for audit.
    Safe to call even if row doesn't exist (it upserts).
    """
    action = action.strip().lower()
    if action not in ACTION_TO_FIELD:
        return

    start = current_period_start()
    start_iso = _iso(start)

    field = ACTION_TO_FIELD[action]

    # upsert base row
    base = {
        "user_id": user_id,
        "period_start": start_iso,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        field: amount,
    }

    # fetch existing to add (Supabase upsert doesn't auto-increment)
    resp = (
        supabase.table("usage_counters")
        .select(field)
        .eq("user_id", user_id)
        .eq("period_start", start_iso)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if rows:
        current = int(rows[0].get(field, 0) or 0)
        base[field] = current + amount

    supabase.table("usage_counters").upsert(base, on_conflict="user_id,period_start").execute()

    # Optional audit event in credit_ledger
    try:
        if meta is None:
            meta = {}
        supabase.table("credit_ledger").insert({
            "user_id": user_id,
            "event": f"usage:{action}",
            "delta": -amount,
            "meta": meta,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        # ledger is optional
        pass


def remaining(supabase, user_id: str, action: str) -> int:
    """
    Returns remaining quota for action in this period.
    """
    action = action.strip().lower()
    if action not in ACTION_TO_FIELD:
        return 0

    plan = get_user_plan(supabase, user_id)
    limits = get_limits(plan)
    limit_value = getattr(limits, action)

    start = current_period_start()
    start_iso = _iso(start)

    resp = (
        supabase.table("usage_counters")
        .select(ACTION_TO_FIELD[action])
        .eq("user_id", user_id)
        .eq("period_start", start_iso)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    used = int(rows[0].get(ACTION_TO_FIELD[action], 0) or 0) if rows else 0

    rem = limit_value - used
    return rem if rem > 0 else 0