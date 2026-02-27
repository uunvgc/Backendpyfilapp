# logic/ai_reply.py
import os
import json
from typing import Dict, Any
from openai import OpenAI

client = OpenAI()

MODEL = (os.getenv("FIILTHY_AI_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini").strip()

SYSTEM = """
You write outbound replies that feel like a real human.

Hard rules:
- Use ONLY the lead info + project context provided. Do NOT invent claims.
- No hype, no "AI" mentions, no emojis unless the lead context uses them.
- Keep it short: 1 opener line, 2-4 short lines, 1 CTA question.
- Make it relevant: reference the pain point / buying signal if present.
- Do NOT claim you've used their product.
- Do NOT be spammy. No aggressive selling.

Return JSON only:
{
  "message": "string",
  "tone": "calm|direct|friendly",
  "why_this_works": ["string", ...]
}
""".strip()

def draft_reply(*, lead: Dict[str, Any], project: Dict[str, Any]) -> Dict[str, Any]:
    reasons = lead.get("reasons") or {}
    payload = {
        "project": {
            "name": project.get("name"),
            "url": project.get("url"),
            "niche": project.get("niche"),
            "keywords": project.get("keywords") or [],
            "locations": project.get("locations") or [],
        },
        "lead": {
            "source": lead.get("source"),
            "title": lead.get("title"),
            "content": lead.get("content"),
            "deep_link": lead.get("url") or lead.get("deep_link"),
            "intent": lead.get("intent"),
            "score": lead.get("score"),
            "pain_points": reasons.get("pain_points") or [],
            "buying_signals": reasons.get("buying_signals") or [],
        }
    }

    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": json.dumps(payload)},
        ],
        response_format={"type": "json_object"},
    )

    return json.loads(resp.output_text)