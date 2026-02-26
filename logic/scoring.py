# logic/scoring.py
#
# Scores leads and returns:
#   score: int 0..100
#   intent: "high" | "medium" | "low"
#   reasons: dict (stored in leads.reasons jsonb)
#
# Plug into your pipeline:
#   score, intent, reasons = score_lead(title, content, url, source, created_at_iso)
#
# You can tune weights without changing other code.

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple


# -----------------------
# Intent phrase groups
# -----------------------

HIGH_INTENT_PHRASES = [
    "looking for",
    "need a",
    "need an",
    "anyone recommend",
    "recommend a",
    "who do you use",
    "who can",
    "help me find",
    "dm me",
    "quote",
    "get a quote",
    "pricing",
    "rates",
    "hire",
    "hiring",
    "seeking",
    "iso",  # in search of
]

MEDIUM_INTENT_PHRASES = [
    "best",
    "top",
    "suggest",
    "alternatives",
    "switching from",
    "compare",
    "recommendations",
]

SPAMMY_PHRASES = [
    "free money",
    "giveaway",
    "airdrop",
    "crypto pump",
    "onlyfans",
    "telegram",
]


# -----------------------
# Weights
# -----------------------

@dataclass(frozen=True)
class ScoreWeights:
    high_intent_hit: int = 40
    medium_intent_hit: int = 18
    contains_question: int = 8
    contains_money_signal: int = 10
    contains_urgency: int = 10
    good_length: int = 8
    spam_penalty: int = -40
    source_bonus: int = 0  # you can set per-source externally if you want


URGENCY_WORDS = ["today", "asap", "urgent", "now", "immediately", "this week"]
MONEY_WORDS = ["$", "budget", "price", "pricing", "cost", "rates", "quote"]


# -----------------------
# Helpers
# -----------------------

_space = re.compile(r"\s+")
_nonword = re.compile(r"[^a-z0-9$]+")


def _norm(text: str) -> str:
    text = (text or "").lower()
    text = _space.sub(" ", text).strip()
    return text


def _has_any(text: str, phrases) -> Tuple[bool, Optional[str]]:
    for p in phrases:
        if p in text:
            return True, p
    return False, None


def _count_any(text: str, phrases) -> int:
    c = 0
    for p in phrases:
        if p in text:
            c += 1
    return c


def _parse_time(created_at_iso: Optional[str]) -> Optional[datetime]:
    if not created_at_iso:
        return None
    try:
        return datetime.fromisoformat(created_at_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def freshness_score(created_at_iso: Optional[str]) -> Tuple[int, Optional[str]]:
    """
    Returns (freshness_points, label)
    """
    dt = _parse_time(created_at_iso)
    if not dt:
        return 0, None

    now = datetime.now(timezone.utc)
    age = now - dt

    if age <= timedelta(hours=1):
        return 20, "posted_within_1h"
    if age <= timedelta(hours=6):
        return 14, "posted_within_6h"
    if age <= timedelta(days=1):
        return 10, "posted_within_24h"
    if age <= timedelta(days=3):
        return 6, "posted_within_3d"
    if age <= timedelta(days=7):
        return 3, "posted_within_7d"
    return 0, None


# -----------------------
# Main scoring
# -----------------------

def score_lead(
    title: str,
    content: str,
    url: str = "",
    source: str = "",
    created_at_iso: Optional[str] = None,
    weights: ScoreWeights = ScoreWeights(),
) -> Tuple[int, str, Dict]:
    """
    Returns (score, intent, reasons)
    """
    t = _norm(title)
    c = _norm(content)
    text = f"{t} {c}".strip()

    score = 0
    reasons: Dict = {
        "hits": [],
        "source": source,
        "url": url,
    }

    # Spam penalty
    spam_hit, spam_phrase = _has_any(text, SPAMMY_PHRASES)
    if spam_hit:
        score += weights.spam_penalty
        reasons["hits"].append({"type": "spam", "phrase": spam_phrase, "points": weights.spam_penalty})

    # Intent hits
    hi_hit, hi_phrase = _has_any(text, HIGH_INTENT_PHRASES)
    if hi_hit:
        score += weights.high_intent_hit
        reasons["hits"].append({"type": "intent_high", "phrase": hi_phrase, "points": weights.high_intent_hit})

    med_hit, med_phrase = _has_any(text, MEDIUM_INTENT_PHRASES)
    if med_hit and not hi_hit:
        score += weights.medium_intent_hit
        reasons["hits"].append({"type": "intent_medium", "phrase": med_phrase, "points": weights.medium_intent_hit})

    # Question mark = often a request
    if "?" in (title or "") or "?" in (content or ""):
        score += weights.contains_question
        reasons["hits"].append({"type": "question", "points": weights.contains_question})

    # Money signal
    if any(w in text for w in MONEY_WORDS):
        score += weights.contains_money_signal
        reasons["hits"].append({"type": "money_signal", "points": weights.contains_money_signal})

    # Urgency
    urg_count = _count_any(text, URGENCY_WORDS)
    if urg_count:
        pts = min(weights.contains_urgency, 4 * urg_count)
        score += pts
        reasons["hits"].append({"type": "urgency", "count": urg_count, "points": pts})

    # Length heuristic
    length = len(text)
    if 120 <= length <= 3000:
        score += weights.good_length
        reasons["hits"].append({"type": "good_length", "points": weights.good_length, "chars": length})

    # Freshness
    fresh_pts, fresh_label = freshness_score(created_at_iso)
    if fresh_pts:
        score += fresh_pts
        reasons["hits"].append({"type": "freshness", "label": fresh_label, "points": fresh_pts})

    # Source bonus (optional)
    if weights.source_bonus:
        score += weights.source_bonus
        reasons["hits"].append({"type": "source_bonus", "points": weights.source_bonus})

    # Determine intent label
    intent = "low"
    if hi_hit:
        intent = "high"
    elif med_hit:
        intent = "medium"

    # Clamp score 0..100
    score = max(0, min(100, int(score)))
    reasons["final_score"] = score
    reasons["intent"] = intent

    return score, intent, reasons