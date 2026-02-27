# logic/sources.py
#
# Multi-source lead fetchers:
# - Google SERP via SerpApi or Serper.dev (handled in app.py)
# - Reddit: public search.json (no auth) + deep link to exact post
# - Hacker News: Algolia Search API + deep link to story/item
# - IndieHackers: RSS feed + keyword filtering + deep link
#
# Notes:
# - Finder sources only. No auto-posting.
# - Reddit may rate limit; use pacing and a real User-Agent.
# - IndieHackers RSS is broad; we filter locally by keywords.

import time
import random
from typing import Dict, List
from urllib.parse import quote_plus

import requests
import xml.etree.ElementTree as ET


# -----------------------------
# Helpers
# -----------------------------
def _ua() -> str:
    return "fiilthy/1.0 (lead-finder; contact: support@fiilthy.ai)"

def _safe_text(x, limit: int) -> str:
    s = (x or "").strip()
    return s[:limit]

def _sleep_jitter(min_s=0.2, max_s=1.0):
    time.sleep(random.uniform(min_s, max_s))

def _text(node, tag: str) -> str:
    el = node.find(tag)
    if el is None or el.text is None:
        return ""
    return el.text

def _find_first_text(node, tags: List[str]) -> str:
    for t in tags:
        el = node.find(t)
        if el is not None and el.text:
            return el.text
    return ""


# -----------------------------
# Reddit
# -----------------------------
def fetch_reddit(query: str, limit: int = 15, timeout: int = 20) -> List[Dict[str, str]]:
    """
    Uses public Reddit search endpoint.
    Returns list of:
      {title, url, deep_link, snippet, source}
    """
    q = query.strip()
    if not q:
        return []

    url = f"https://www.reddit.com/search.json?q={quote_plus(q)}&sort=new&limit={int(limit)}"
    headers = {"User-Agent": _ua()}
    _sleep_jitter()

    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code == 429:
        return []
    r.raise_for_status()
    data = r.json()

    out: List[Dict[str, str]] = []
    children = ((data.get("data") or {}).get("children") or [])
    for c in children:
        d = (c.get("data") or {})
        title = _safe_text(d.get("title"), 500)

        permalink = (d.get("permalink") or "").strip()
        deep = ("https://www.reddit.com" + permalink) if permalink.startswith("/") else permalink

        # Some results have external URL; but user wants click -> exact place.
        url_final = deep or _safe_text(d.get("url"), 2000)

        selftext = _safe_text(d.get("selftext"), 1200)
        subreddit = (d.get("subreddit") or "").strip()
        author = (d.get("author") or "").strip()

        snippet_parts = []
        if subreddit:
            snippet_parts.append(f"r/{subreddit}")
        if author:
            snippet_parts.append(f"u/{author}")
        if selftext:
            snippet_parts.append(selftext)
        snippet = " — ".join(snippet_parts)[:1200]

        if not title or not url_final:
            continue

        out.append({
            "title": title,
            "url": url_final,
            "deep_link": deep or url_final,
            "snippet": snippet,
            "source": "reddit",
        })

    return out


# -----------------------------
# Hacker News (Algolia)
# -----------------------------
def fetch_hn(query: str, limit: int = 15, timeout: int = 20) -> List[Dict[str, str]]:
    """
    HN Algolia API:
    https://hn.algolia.com/api/v1/search_by_date?query=...&tags=story
    Returns:
      {title, url, deep_link, snippet, source}
    """
    q = query.strip()
    if not q:
        return []

    api = f"https://hn.algolia.com/api/v1/search_by_date?query={quote_plus(q)}&tags=story&hitsPerPage={int(limit)}"
    _sleep_jitter()

    r = requests.get(api, headers={"User-Agent": _ua()}, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    out: List[Dict[str, str]] = []
    hits = data.get("hits") or []
    for h in hits:
        title = _safe_text(h.get("title"), 500)
        story_url = (h.get("url") or "").strip()
        object_id = (h.get("objectID") or "").strip()

        hn_link = f"https://news.ycombinator.com/item?id={object_id}" if object_id else ""

        # deep_link should be the actual thread (exact place)
        deep = hn_link or story_url
        url_final = story_url or hn_link

        snippet = _safe_text(h.get("story_text") or h.get("comment_text") or "", 1200)
        if not snippet:
            author = h.get("author")
            points = h.get("points")
            created = h.get("created_at")
            snippet = _safe_text(f"author={author} points={points} created_at={created}", 400)

        if not title or not url_final:
            continue

        out.append({
            "title": title,
            "url": url_final,
            "deep_link": deep,
            "snippet": snippet,
            "source": "hn",
        })

    return out


# -----------------------------
# IndieHackers RSS
# -----------------------------
def fetch_indiehackers_rss(
    keywords: List[str],
    limit: int = 20,
    timeout: int = 20,
) -> List[Dict[str, str]]:
    """
    Pulls IndieHackers RSS and filters by keywords locally.
    Returns:
      {title, url, deep_link, snippet, source}
    """
    feed_url = "https://www.indiehackers.com/feed"
    _sleep_jitter()

    r = requests.get(feed_url, headers={"User-Agent": _ua()}, timeout=timeout)
    if r.status_code == 429:
        return []
    r.raise_for_status()

    xml_text = r.text or ""
    if not xml_text.strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []

    kw = [k.strip().lower() for k in (keywords or []) if k and k.strip()]
    out: List[Dict[str, str]] = []

    # Namespace-safe item search
    items = root.findall(".//item")

    for item in items[:300]:
        title = _safe_text(_find_first_text(item, ["title"]), 500)
        link = (_find_first_text(item, ["link"]) or "").strip()
        desc = _safe_text(_find_first_text(item, ["description"]), 2000)

        blob = f"{title}\n{desc}".lower()
        if kw and not any(k in blob for k in kw):
            continue

        if not title or not link:
            continue

        out.append({
            "title": title,
            "url": link,
            "deep_link": link,
            "snippet": _safe_text(desc, 1200),
            "source": "indiehackers",
        })
        if len(out) >= limit:
            break

    return out