# logic/lead_ai.py

import json
from openai import OpenAI

client = OpenAI()
MODEL = "gpt-4.1-mini"

LEAD_SYSTEM = """
You are a senior growth strategist and product-market-fit analyst.

Rules:
- Only use information provided.
- Quote exact pain phrases from the lead text.
- Connect that pain directly to how the project positioning solves it.
- Do NOT hallucinate features.
- Be specific, not generic.
- Score lead intent realistically (0-100).

Return JSON:
{
  "score": 0-100,
  "pain_quote": "",
  "pain_summary": "",
  "why_this_is_a_lead": "",
  "best_angle": "",
  "confidence": "low|medium|high",
  "drafts": {
    "short": "",
    "long": ""
  }
}
"""

def analyze_lead(lead: dict, positioning: dict) -> dict:
    payload = {
        "lead": {
            "title": lead.get("title"),
            "content": lead.get("content"),
            "source": lead.get("source"),
        },
        "project_positioning": positioning
    }

    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": LEAD_SYSTEM},
            {"role": "user", "content": json.dumps(payload)},
        ],
        response_format={"type": "json_object"},
    )

    return json.loads(resp.output_text)