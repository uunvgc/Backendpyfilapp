# logic/scoring.py
#
# Scores leads and returns:
#   score: int 0..100
#   intent: "high" | "medium" | "low"
#   reasons: dict (stored in leads.reasons jsonb)
#
# Plug into your pipeline:
#   score, intent, reasons = score_lead(title, content, url, source, created_at_iso, meta)
#
# You can tune weights without changing other code.

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple


# -----------------------
# Phrase groups
# -----------------------

# High buyer intent = explicit need + action
HIGH_INTENT_PHRASES = [
    "looking for",
    "need a",
    "need an",
    "need help",
    "need someone",
    "anyone recommend",
    "recommend a",
    "who do you use",
    "what tool do you use",
    "what are you using for",
    "help me find",
    "dm me",
    "quote",
    "get a quote",
    "request for proposal",
    "rfp",
    "pricing",
    "rates",
    "budget",
    "cost",
    "buy",
    "purchase",
    "trial",
    "demo",
    "book a demo",
    "implement",
    "set up",
    "setup",
    "migrate",
    "migration",
    "switching to",
    "switching from",
    "moving from",
    "replace",
    "replacing",
    "alternative to",
    "alternatives to",
]

# Medium intent = researching / comparing
MEDIUM_INTENT_PHRASES = [
    "best",
    "top",
    "suggest",
    "alternatives",
    "compare",
    "comparison",
    "recommendations",
    "what's better",
    "which is better",
    "pros and cons",
    "vs ",
    "versus",
]

# Urgency
URGENCY_WORDS = [
    "today",
    "asap",
    "urgent",
    "now",
    "immediately",
    "this week",
    "by friday",
    "deadline",
    "soon",
]

# Money / buying signals (explicit)
MONEY_WORDS = [
    "$",
    "usd",
    "budget",
    "price",
    "pricing",
    "cost",
    "rates",
    "quote",
    "invoice",
    "contract",
    "retainer",
    "monthly",
    "annually",
    "per month",
]

# Pain points (good leads even without explicit “need”)
PAIN_PHRASES = [
    "frustrating",
    "annoying",
    "pain",
    "problem",
    "issue",
    "broken",
    "doesn't work",
    "not working",
    "keeps failing",
    "too expensive",
    "overpriced",
    "slow",
    "bugs",
    "hard to use",
    "confusing",
    "time consuming",
    "wasting time",
    "missing feature",
    "lack of",
    "no way to",
    "need better",
]

# Hard negatives (we don't want these)
HARD_NEGATIVE_PHRASES = [
    "we are hiring",
    "we're hiring",
    "hiring",
    "job opening",
    "career",
    "careers",
    "apply now",
    "resume",
    "cv",
    "internship",
    "press release",
    "prnewswire",
    "earnings call",
    "10-k",
    "10q",
    "seo services",
    "backlinks",
    "guest post",
]

# Spam / low-signal garbage
SPAMMY_PHRASES = [
    "free money",
    "giveaway",
    "airdrop",
    "crypto pump",
    "onlyfans",
    "telegram",
    "casino",
    "betting",
]


# -----------------------
# Weights
# -----------------------

@dataclass(frozen=True)
class ScoreWeights:
    high_intent_hit: int = 38
    medium_intent_hit: int = 16

    # additive signals
    contains_question: int = 7
    contains_money_signal: int = 10
    contains_urgency: int = 10
    pain_point_hit: int = 6

    # freshness
    fresh_1h: int = 20
    fresh_6h: int = 14
    fresh_24h: int = 10
    fresh_3d: int = 6
    fresh_7d: int = 3

    # length
    good_length: int = 8

    # penalties
    spam_penalty: int = -45
    hard_negative_penalty: int = -60

    # source weighting
    bonus_reddit: int = 2
    bonus_hn: int = 4
    bonus_indiehackers: int = 3
    bonus_serp: int = 0


# -----------------------
# Helpers
# -----------------------

_space = re.compile(r"\s+")
def _norm(text: str) -> str:
    text = (text or "").lower()
    text = _space.sub(" ", text).strip()
    return text

def _has_any(text: str, phrases: List[str]) -> Tuple[bool, Optional[str]]:
    for p in phrases:
        if p in text:
            return True, p
    return False, None

def _all_hits(text: str, phrases: List[str], max_hits: int = 5) -> List[str]:
    hits = []
    for p in phrases:
        if p in text:
            hits.append(p)
            if len(hits) >= max_hits:
                break
    return hits

def _count_any(text: str, phrases: List[str]) -> int:
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

def freshness_score(created_at_iso: Optional[str], w: ScoreWeights) -> Tuple[int, Optional[str]]:
    dt = _parse_time(created_at_iso)
    if not dt:
        return 0, None

    now = datetime.now(timezone.utc)
    age = now - dt

    if age <= timedelta(hours=1):
        return w.fresh_1h, "posted_within_1h"
    if age <= timedelta(hours=6):
        return w.fresh_6h, "posted_within_6h"
    if age <= timedelta(days=1):
        return w.fresh_24h, "posted_within_24h"
    if age <= timedelta(days=3):
        return w.fresh_3d, "posted_within_3d"
    if age <= timedelta(days=7):
        return w.fresh_7d, "posted_within_7d"
    return 0, None

def _question_signal(title: str, content: str) -> bool:
    t = (title or "").strip()
    c = (content or "").strip()
    if "?" in t or "?" in c:
        return True
    # also treat typical “ask” openers as question intent
    text = _norm(f"{t} {c}")
    return any(p in text for p in ["anyone", "who knows", "recommend", "suggest", "help me", "what should", "how do i"])


def _source_bonus(source: str, w: ScoreWeights) -> int:
    s = (source or "").lower()
    if "reddit" in s:
        return w.bonus_reddit
    if s in ("hn", "hackernews") or "hn" == s or "ycombinator" in s:
        return w.bonus_hn
    if "indiehackers" in s:
        return w.bonus_indiehackers
    if "serp" in s or "google" in s:
        return w.bonus_serp
    return 0


# -----------------------
# Main scoring
# -----------------------
def score_lead(
    title: str,
    content: str,
    url: str = "",
    source: str = "",
    created_at_iso: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    weights: ScoreWeights = ScoreWeights(),
) -> Tuple[int, str, Dict]:
    """
    Returns (score, intent, reasons)
    """
    t = _norm(title)
    c = _norm(content)
    text = f"{t} {c}".strip()

    score = 0
    hits: List[Dict[str, Any]] = []
    buying_signals: List[str] = []
    pain_points: List[str] = []

    # Hard negatives first (kill it early)
    neg_hit, neg_phrase = _has_any(text, HARD_NEGATIVE_PHRASES)
    if neg_hit:
        score += weights.hard_negative_penalty
        hits.append({"type": "hard_negative", "phrase": neg_phrase, "points": weights.hard_negative_penalty})

    # Spam penalty
    spam_hit, spam_phrase = _has_any(text, SPAMMY_PHRASES)
    if spam_hit:
        score += weights.spam_penalty
        hits.append({"type": "spam", "phrase": spam_phrase, "points": weights.spam_penalty})

    # Intent hits
    hi_hit, hi_phrase = _has_any(text, HIGH_INTENT_PHRASES)
    if hi_hit:
        score += weights.high_intent_hit
        hits.append({"type": "intent_high", "phrase": hi_phrase, "points": weights.high_intent_hit})
        buying_signals.extend(_all_hits(text, HIGH_INTENT_PHRASES, max_hits=5))

    med_hit, med_phrase = _has_any(text, MEDIUM_INTENT_PHRASES)
    if med_hit and not hi_hit:
        score += weights.medium_intent_hit
        hits.append({"type": "intent_medium", "phrase": med_phrase, "points": weights.medium_intent_hit})
        buying_signals.extend(_all_hits(text, MEDIUM_INTENT_PHRASES, max_hits=4))

    # Question signal
    if _question_signal(title, content):
        score += weights.contains_question
        hits.append({"type": "question", "points": weights.contains_question})

    # Money / budget
    if any(wd in text for wd in MONEY_WORDS):
        score += weights.contains_money_signal
        hits.append({"type": "money_signal", "points": weights.contains_money_signal})
        buying_signals.extend(_all_hits(text, MONEY_WORDS, max_hits=4))

    # Urgency
    urg_count = _count_any(text, URGENCY_WORDS)
    if urg_count:
        pts = min(weights.contains_urgency, 4 * urg_count)
        score += pts
        hits.append({"type": "urgency", "count": urg_count, "points": pts})

    # Pain points (can exist without explicit buyer intent)
    pains = _all_hits(text, PAIN_PHRASES, max_hits=6)
    if pains:
        pts = min(18, weights.pain_point_hit * len(pains))
        score += pts
        hits.append({"type": "pain_points", "count": len(pains), "points": pts})
        pain_points.extend(pains)

    # Length heuristic
    length = len(text)
    if 120 <= length <= 3500:
        score += weights.good_length
        hits.append({"type": "good_length", "points": weights.good_length, "chars": length})

    # Freshness
    fresh_pts, fresh_label = freshness_score(created_at_iso, weights)
    if fresh_pts:
        score += fresh_pts
        hits.append({"type": "freshness", "label": fresh_label, "points": fresh_pts})

    # Source bonus
    sb = _source_bonus(source, weights)
    if sb:
        score += sb
        hits.append({"type": "source_bonus", "points": sb})

    # Meta-based boosts (optional, if you pass meta from sources)
    if meta:
        try:
            # HN points/comments
            pts = int(meta.get("points") or 0)
            cmts = int(meta.get("num_comments") or 0)
            if pts >= 50:
                score += 4
                hits.append({"type": "meta_hn_points", "points": 4, "value": pts})
            if cmts >= 30:
                score += 3
                hits.append({"type": "meta_comments", "points": 3, "value": cmts})

            # Reddit comment count
            rc = int(meta.get("num_comments") or 0)
            if rc >= 20:
                score += 3
                hits.append({"type": "meta_reddit_comments", "points": 3, "value": rc})
        except Exception:
            pass

    # Determine intent label
    intent = "low"
    if hi_hit and not neg_hit and not spam_hit:
        intent = "high"
    elif med_hit and not neg_hit and not spam_hit:
        intent = "medium"
    elif pain_points and not neg_hit and not spam_hit:
        intent = "medium" if score >= 45 else "low"

    # Clamp score 0..100
    score = max(0, min(100, int(score)))

    reasons: Dict[str, Any] = {
        "hits": hits,
        "buying_signals": list(dict.fromkeys(buying_signals))[:8],
        "pain_points": list(dict.fromkeys(pain_points))[:8],
        "source": source,
        "url": url,
        "created_at_iso": created_at_iso,
        "final_score": score,
        "intent": intent,
        "summary": {
            "top_signals": [h.get("type") for h in hits[:5]],
            "pain_points": list(dict.fromkeys(pain_points))[:4],
            "buying_signals": list(dict.fromkeys(buying_signals))[:4],
        },
    }

    return score, intent, reasons