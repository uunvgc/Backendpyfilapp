import os, json
from openai import OpenAI

client = OpenAI()
MODEL = (os.getenv("FIILTHY_AI_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini").strip()

SYSTEM = """
You write outbound replies that feel like a real human.

Rules:
- Use ONLY the lead text/snippet/title and the provided positioning (if any).
- No hype, no "AI-ish" language, no emojis unless context already uses them.
- Be specific, short, and on-topic.
- 1 opener line, then 2–4 concise lines, then 1 clear CTA question.
- Do NOT claim you used their product.
- Do NOT invent facts.

Return JSON only:
{ "message": "...", "tone": "calm|direct|friendly", "why_this_works": ["..."] }
""".strip()

def draft_reply(*, lead: dict, positioning: dict) -> dict:
    payload = {
        "lead": {
            "source": lead.get("source"),
            "title": lead.get("title"),
            "content": lead.get("content"),
            "url": lead.get("url") or lead.get("deep_link"),
        },
        "positioning": positioning or {},
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