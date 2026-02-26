# logic/cache.py
#
# Prevents rerunning the same queries and refetching the same URLs.
# Saves money, prevents rate limits, and speeds up FIILTHY.


from datetime import datetime, timedelta, timezone
from typing import List


# How long to wait before running same query again
QUERY_COOLDOWN_HOURS = 24

# How long to cache page content
URL_CACHE_DAYS = 7


# ----------------------------
# QUERY CACHE
# ----------------------------

def should_run_query(supabase, query: str) -> bool:
    """
    Returns True if query should run, False if still on cooldown.
    """

    try:
        resp = (
            supabase
            .table("query_cache")
            .select("last_run")
            .eq("query", query)
            .limit(1)
            .execute()
        )

        rows = resp.data or []

        if not rows:
            return True

        last_run = rows[0]["last_run"]

        last_run_dt = datetime.fromisoformat(
            last_run.replace("Z", "+00:00")
        )

        now = datetime.now(timezone.utc)

        return now - last_run_dt > timedelta(hours=QUERY_COOLDOWN_HOURS)

    except Exception:
        return True


def mark_query_run(supabase, query: str):
    """
    Mark query as run now.
    """

    now = datetime.now(timezone.utc).isoformat()

    supabase.table("query_cache").upsert({
        "query": query,
        "last_run": now
    }).execute()


def filter_queries(supabase, queries: List[str]) -> List[str]:
    """
    Removes queries still on cooldown.
    """

    allowed = []

    for q in queries:
        if should_run_query(supabase, q):
            allowed.append(q)

    return allowed


# ----------------------------
# URL CACHE
# ----------------------------

def is_url_cached(supabase, url: str) -> bool:

    try:

        resp = (
            supabase
            .table("url_cache")
            .select("fetched_at")
            .eq("url", url)
            .limit(1)
            .execute()
        )

        rows = resp.data or []

        if not rows:
            return False

        fetched_at = rows[0]["fetched_at"]

        fetched_dt = datetime.fromisoformat(
            fetched_at.replace("Z", "+00:00")
        )

        now = datetime.now(timezone.utc)

        return now - fetched_dt < timedelta(days=URL_CACHE_DAYS)

    except Exception:
        return False


def mark_url_cached(supabase, url: str, content: str = None):

    now = datetime.now(timezone.utc).isoformat()

    supabase.table("url_cache").upsert({
        "url": url,
        "fetched_at": now,
        "content": content
    }).execute()


# ----------------------------
# CLEANUP
# ----------------------------

def cleanup_old_cache(supabase):

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=URL_CACHE_DAYS)
    ).isoformat()

    supabase.table("url_cache") \
        .delete() \
        .lt("fetched_at", cutoff) \
        .execute()