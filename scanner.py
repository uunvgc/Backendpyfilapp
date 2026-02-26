import os
import re
import time
from datetime import datetime, timezone

import requests

try:
    from supabase import create_client
except Exception:
    create_client = None


# -----------------------------
# ENV
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()  # Use SERVICE_ROLE key on server

USER_AGENT = os.getenv("FIILTHY_USER_AGENT", "fiilthy/1.0 (pythonanywhere)")
HTTP_TIMEOUT = int(os.getenv("FIILTHY_HTTP_TIMEOUT", "12"))

# Hard negatives (skip obvious non-leads)
HARD_NEGATIVE_RE = re.compile(
    r"\b(hiring|job\s+opening|career|resume|cv|apply\s+now|recruiter|internship)\b",
    re.IGNORECASE,
)

# Basic intent keywords (very simple starter)
HIGH_INTENT_RE = re.compile(
    r"\b(need|looking for|recommend|anyone know|can someone|help me|want to buy|budget|quote|price)\b",
    re.IGNORECASE,
)


# -----------------------------
# Supabase client
# -----------------------------
_supabase = None


def check_supabase_ready() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY and create_client is not None)


def get_supabase():
    global _supabase
    if _supabase is None:
        if not check_supabase_ready():
            raise RuntimeError("Supabase not configured. Set SUPABASE_URL and SUPABASE_KEY.")
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase


# -----------------------------
# Source fetchers
# -----------------------------
def fetch_hn_leads(limit=20):
    """
    Hacker News via Algolia search API (public).
    We search for keywords that often indicate someone wants help/tools/services.
    """
    q = "looking for OR need OR recommend OR quote OR pricing"
    url = "https://hn.algolia.com/api/v1/search"
    params = {"query": q, "tags": "story", "hitsPerPage": limit}
    r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    data = r.json()
    hits = data.get("hits", [])
    items = []
    for h in hits:
        title = (h.get("title") or "").strip()
        story_url = (h.get("url") or "").strip() or f"https://news.ycombinator.com/item?id={h.get('objectID')}"
        content = (h.get("story_text") or "").strip()
        if not title and not content:
            continue
        items.append(
            {
                "title": title[:500],
                "content": (content or title)[:4000],
                "source": "hackernews",
                "url": story_url,
            }
        )
    return items


def fetch_reddit_leads(subreddit="entrepreneur", limit=20):
    """
    Reddit JSON endpoint (no auth) — can work, but sometimes rate-limited.
    """
    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    params = {"limit": limit}
    r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    data = r.json()
    children = (data.get("data") or {}).get("children") or []
    items = []
    for c in children:
        p = (c.get("data") or {})
        title = (p.get("title") or "").strip()
        selftext = (p.get("selftext") or "").strip()
        permalink = p.get("permalink")
        full_url = f"https://www.reddit.com{permalink}" if permalink else (p.get("url") or "")
        if not title and not selftext:
            continue
        items.append(
            {
                "title": title[:500],
                "content": (selftext or title)[:4000],
                "source": f"reddit:r/{subreddit}",
                "url": full_url,
            }
        )
    return items


# -----------------------------
# Scoring / intent (starter)
# -----------------------------
def detect_intent(text: str) -> str:
    if not text:
        return "unknown"
    if HIGH_INTENT_RE.search(text):
        return "high"
    return "low"


def score_lead(title: str, content: str) -> int:
    text = f"{title}\n{content}".lower()
    score = 0

    # High-intent words
    if HIGH_INTENT_RE.search(text):
        score += 40

    # Words that often mean someone is ready to spend
    if any(k in text for k in ["budget", "quote", "pricing", "price", "pay", "invoice"]):
        score += 30

    # Longer content usually has more detail
    if len(content or "") > 300:
        score += 10
    if len(content or "") > 800:
        score += 10

    # Clamp 0-100
    return max(0, min(100, score))


def is_hard_negative(text: str) -> bool:
    if not text:
        return False
    return bool(HARD_NEGATIVE_RE.search(text))


# -----------------------------
# Insert into Supabase
# -----------------------------
def insert_leads(leads):
    """
    Inserts into public.leads.
    Your table columns: title, content, source, url, score, intent, created_at
    We use upsert on url to avoid duplicates.
    """
    sb = get_supabase()

    inserted = 0
    for lead in leads:
        title = lead.get("title") or ""
        content = lead.get("content") or ""
        source = lead.get("source") or ""
        url = (lead.get("url") or "").strip()

        if not url:
            continue

        full_text = f"{title}\n{content}"
        if is_hard_negative(full_text):
            continue

        intent = detect_intent(full_text)
        score = score_lead(title, content)

        row = {
            "title": title,
            "content": content,
            "source": source,
            "url": url,
            "score": score,
            "intent": intent,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Upsert by url (url is UNIQUE)
        resp = sb.table("leads").upsert(row, on_conflict="url").execute()

        # supabase-py returns data when insert/upsert occurs; duplicates may still return data depending on version.
        # We'll estimate inserted by checking response contains a row AND that it’s not an update:
        # (Not perfect across all versions; still fine for ops.)
        if getattr(resp, "data", None):
            inserted += 1

        # small pause to avoid rate limiting or bursts
        time.sleep(0.1)

    return inserted


# -----------------------------
# Main scan function
# -----------------------------
def run_scan():
    """
    Returns dict: {ok, inserted, error, ts}
    """
    ts = datetime.now(timezone.utc).isoformat()

    if not check_supabase_ready():
        return {"ok": False, "inserted": 0, "error": "Supabase not configured", "ts": ts}

    try:
        leads = []
        # Add sources you want:
        leads += fetch_hn_leads(limit=20)
        leads += fetch_reddit_leads(subreddit="entrepreneur", limit=20)
        leads += fetch_reddit_leads(subreddit="smallbusiness", limit=20)

        inserted = insert_leads(leads)
        return {"ok": True, "inserted": inserted, "error": None, "ts": ts}

    except Exception as e:
        return {"ok": False, "inserted": 0, "error": str(e), "ts": ts}