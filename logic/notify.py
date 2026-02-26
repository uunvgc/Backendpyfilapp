# logic/notify.py

import requests
import datetime
import os
from supabase import create_client, Client

# Load Supabase from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = None

if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------- HOT LEAD SCORING ----------

HOT_SCORE_THRESHOLD = 70


def is_hot_lead(score: int) -> bool:
    """
    Returns True if lead score qualifies as hot.
    """
    return score >= HOT_SCORE_THRESHOLD


# ---------- SAVE HOT LEAD ----------

def save_hot_lead(lead_data: dict):
    """
    Saves hot lead to Supabase database.
    """

    if not supabase:
        print("Supabase not configured")
        return

    try:
        payload = {
            "url": lead_data.get("url"),
            "score": lead_data.get("score"),
            "keywords": lead_data.get("keywords"),
            "created_at": datetime.datetime.utcnow().isoformat()
        }

        supabase.table("hot_leads").insert(payload).execute()

        print(f"Hot lead saved: {payload['url']}")

    except Exception as e:
        print(f"Error saving hot lead: {e}")


# ---------- ALERT FUNCTION ----------

def send_console_alert(lead_data: dict):
    """
    Basic alert (safe default)
    """
    print("🔥 HOT LEAD DETECTED 🔥")
    print(f"URL: {lead_data.get('url')}")
    print(f"Score: {lead_data.get('score')}")


# ---------- OPTIONAL: WEBHOOK ALERT ----------

WEBHOOK_URL = os.getenv("WEBHOOK_URL")


def send_webhook_alert(lead_data: dict):
    """
    Sends lead to webhook (Discord, Slack, etc)
    """

    if not WEBHOOK_URL:
        return

    try:
        requests.post(
            WEBHOOK_URL,
            json={
                "text": f"🔥 HOT LEAD: {lead_data.get('url')} (Score: {lead_data.get('score')})"
            },
            timeout=5
        )

    except Exception as e:
        print(f"Webhook failed: {e}")


# ---------- MAIN ENTRY FUNCTION ----------

def maybe_alert_hot_lead(lead_data: dict):
    """
    Main function used by scanner.
    Call this after scoring a website.
    """

    score = lead_data.get("score", 0)

    if is_hot_lead(score):

        print("Hot lead detected")

        save_hot_lead(lead_data)
        send_console_alert(lead_data)
        send_webhook_alert(lead_data)

        return True

    return False