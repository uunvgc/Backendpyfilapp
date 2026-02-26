from datetime import datetime, timezone
from typing import Dict, Any, List

from config import DEFAULT_SUBREDDITS, DEFAULT_RSS_FEEDS, SERP_QUERIES
from supabase_client import get_supabase, supabase_ready
from utils import url_hash, safe_str
from scoring import score, is_hard_negative

from sources.serp import fetch_serp
from sources.reddit import fetch_reddit
from sources.hn import fetch_hn
from sources.rss import fetch_rss


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize(item: Dict[str, Any]) -> Dict[str, Any]:
    url = (item.get("url") or "").strip()

    if not url:
        return {}

    title = safe_str(item.get("title"), 500)
    content = safe_str(item.get("content"), 4000)

    text = f"{title}\n{content}"

    if is_hard_negative(text):
        return {}

    s, category, buyer_signal, keywords = score(text)

    intent = "high" if s >= 70 else "medium" if s >= 45 else "low"

    return {
        "url": url,
        "url_hash": url_hash(url),
        "title": title,
        "content": content,
        "source": item.get("source") or "",
        "author": item.get("author") or "",
        "score": s,
        "intent": intent,
        "category": category,
        "buyer_signal": buyer_signal,
        "hard_negative": False,
        "keywords": keywords,
        "raw": item.get("raw") or {},
        "last_seen_at": now_iso(),
        "created_at": now_iso(),
    }


def gather():
    items = []
    errors = []

    # SERP
    for q in SERP_QUERIES:
        try:
            items += fetch_serp(q, limit=20)
        except Exception as e:
            errors.append(str(e))

    # Reddit
    for sub in DEFAULT_SUBREDDITS:
        try:
            items += fetch_reddit(sub, limit=20)
        except Exception as e:
            errors.append(str(e))

    # Hacker News
    try:
        items += fetch_hn(limit=20)
    except Exception as e:
        errors.append(str(e))

    # RSS (Indie Hackers)
    for feed in DEFAULT_RSS_FEEDS:
        try:
            items += fetch_rss(feed, limit=20)
        except Exception as e:
            errors.append(str(e))

    return items, errors


def run_scan():
    ts = now_iso()

    if not supabase_ready():
        return {"ok": False, "error": "Supabase not configured"}

    items, errors = gather()

    rows = []

    for item in items:
        row = normalize(item)

        if row:
            rows.append(row)

    if rows:
        sb = get_supabase()

        sb.table("leads").upsert(
            rows,
            on_conflict="url_hash"
        ).execute()

    return {
        "ok": True,
        "total_found": len(items),
        "stored": len(rows),
        "errors": errors,
        "ts": ts
    }
