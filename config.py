"""
FIILTHY Configuration
Environment variables and settings
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Flask (if you use it elsewhere)
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    FLASK_ENV = os.getenv("FLASK_ENV", "production")

    # Supabase
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Backend URL for contact queue API (optional)
    FIILTHY_BACKEND_URL = os.getenv("FIILTHY_BACKEND_URL", "https://fiilthy.pythonanywhere.com")

    # Scanning
    SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))  # seconds
    MIN_INTENT_SCORE = int(os.getenv("MIN_INTENT_SCORE", "50"))
    HOT_LEAD_THRESHOLD = int(os.getenv("HOT_LEAD_THRESHOLD", "80"))

    # Optional platform creds (future / API-based scanners)
    REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "FIILTHY-Intent-Engine")

    TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
    TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
    TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")

    # Optional alerting
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    EMAIL_ALERTS = os.getenv("EMAIL_ALERTS", "false").lower() == "true"

    # Scoring weights (if your urgency engine uses these)
    SCORE_WEIGHTS = {
        "explicit_buying": 40,
        "switching": 35,
        "failure_moment": 35,
        "urgency": 30,
        "frustration": 30,
        "launch_signals": 30,
        "growth_signals": 25,
        "competitor_dissatisfaction": 40,
        "authority": 35,
    }