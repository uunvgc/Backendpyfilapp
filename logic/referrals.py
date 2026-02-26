# logic/referrals.py
#
# Referral system (codes, attribution, rewards).
#
# Requires Supabase schema:
#   users:
#     id uuid PK
#     email text
#     plan text (optional)
#     referral_code text unique
#     referred_by text  (stores referrer's referral_code)
#     referral_count int default 0
#
# Optional:
#   referral_events table (recommended)
#   credit_ledger table (recommended)
#
# Typical flow:
# 1) Ensure user has referral_code (call ensure_referral_code on login/signup)
# 2) When new user signs up with ?ref=CODE, call attribute_referral(new_user_id, CODE)
# 3) Reward the referrer (credits or plan unlock) with reward_referrer(...)


from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from typing import Optional, Dict


REFERRAL_CODE_LEN = int(os.getenv("REFERRAL_CODE_LEN", "8"))
REFERRAL_REWARD_CREDITS = int(os.getenv("REFERRAL_REWARD_CREDITS", "50"))  # credits per signup
REFERRAL_REWARD_EVENT = "referral:signup"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_code(length: int = REFERRAL_CODE_LEN) -> str:
    # URL-safe code, uppercase for nice look
    return secrets.token_urlsafe(12).replace("-", "").replace("_", "").upper()[:length]


# -------------------------
# Core helpers
# -------------------------

def ensure_referral_code(supabase, user_id: str) -> str:
    """
    Ensures a user has a referral_code. Returns the referral_code.
    Retries a few times to avoid rare collisions.
    """
    resp = (
        supabase.table("users")
        .select("referral_code")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise RuntimeError("User not found")

    existing = rows[0].get("referral_code")
    if existing:
        return existing

    for _ in range(10):
        code = _gen_code()
        try:
            supabase.table("users").update({"referral_code": code}).eq("id", user_id).execute()
            return code
        except Exception:
            # collision or transient error: try another code
            continue

    raise RuntimeError("Failed to generate unique referral code")


def get_referrer_by_code(supabase, code: str) -> Optional[Dict]:
    """
    Returns referrer user row for a referral code, or None.
    """
    if not code:
        return None
    code = code.strip().upper()

    resp = (
        supabase.table("users")
        .select("id,email,referral_code,referral_count")
        .eq("referral_code", code)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def attribute_referral(supabase, new_user_id: str, referral_code: str) -> bool:
    """
    Attach referral to new user (if not already set), increment referrer count,
    and write a referral event + credit reward.
    Returns True if attributed, False if invalid/already attributed.
    """
    if not referral_code:
        return False
    referral_code = referral_code.strip().upper()

    # Check new user isn't already attributed
    resp_new = (
        supabase.table("users")
        .select("id,referred_by,referral_code")
        .eq("id", new_user_id)
        .limit(1)
        .execute()
    )
    new_rows = resp_new.data or []
    if not new_rows:
        return False

    if new_rows[0].get("referred_by"):
        return False  # already attributed

    # Prevent self-referral
    new_user_code = (new_rows[0].get("referral_code") or "").strip().upper()
    if new_user_code and new_user_code == referral_code:
        return False

    referrer = get_referrer_by_code(supabase, referral_code)
    if not referrer:
        return False

    referrer_id = referrer["id"]

    # Set referred_by on new user
    supabase.table("users").update({"referred_by": referral_code}).eq("id", new_user_id).execute()

    # Increment referrer's count
    current_count = int(referrer.get("referral_count") or 0)
    supabase.table("users").update({"referral_count": current_count + 1}).eq("id", referrer_id).execute()

    # Record event (optional table)
    try:
        supabase.table("referral_events").insert({
            "referrer_user_id": referrer_id,
            "referred_user_id": new_user_id,
            "referral_code": referral_code,
            "event": "signup",
            "created_at": _now_iso(),
        }).execute()
    except Exception:
        pass

    # Reward credits (optional ledger)
    reward_referrer_credits(supabase, referrer_id, new_user_id, referral_code)

    return True


# -------------------------
# Rewards
# -------------------------

def reward_referrer_credits(supabase, referrer_user_id: str, referred_user_id: str, referral_code: str):
    """
    Give the referrer credits for a successful signup.
    Uses credit_ledger so you can audit & prevent double rewards.
    """
    # Prevent double-reward: check ledger for this referred_user_id
    try:
        resp = (
            supabase.table("credit_ledger")
            .select("id")
            .eq("user_id", referrer_user_id)
            .eq("event", REFERRAL_REWARD_EVENT)
            .contains("meta", {"referred_user_id": referred_user_id})
            .limit(1)
            .execute()
        )
        if resp.data:
            return
    except Exception:
        # If credit_ledger doesn't exist, still try to insert once
        pass

    try:
        supabase.table("credit_ledger").insert({
            "user_id": referrer_user_id,
            "event": REFERRAL_REWARD_EVENT,
            "delta": REFERRAL_REWARD_CREDITS,
            "meta": {
                "referred_user_id": referred_user_id,
                "referral_code": referral_code,
            },
            "created_at": _now_iso(),
        }).execute()
    except Exception:
        # ledger optional; you can also store in users.referral_earnings if you prefer
        pass


def referral_tier(referral_count: int) -> str:
    """
    Simple tiering you can show in UI.
    """
    n = int(referral_count or 0)
    if n >= 50:
        return "legend"
    if n >= 20:
        return "vip"
    if n >= 5:
        return "starter"
    return "none"