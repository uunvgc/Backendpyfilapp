# logic/site_audit.py
import json
import re
import requests
from openai import OpenAI

client = OpenAI()

MODEL = "gpt-4.1-mini"

SITE_SYSTEM = """
You are a brutally honest conversion + product positioning expert.

Rules:
- Only use what is present in the page text I provide. Do NOT invent features, pricing, or claims.
- Every issue MUST include evidence (a direct quote or clear reference to the provided text).
- Focus on conversion: clarity, ICP, CTA, trust, pricing clarity, friction, objections.
- Output must be valid JSON.

Return JSON shape:
{
  "summary": "1-3 sentences",
  "positioning": {
    "who_its_for": "",
    "value_prop": "",
    "primary_cta": "",
    "likely_objections": []
  },
  "issues": [
    {"title":"", "severity":1-5, "evidence":"", "fix":"", "example_copy":""}
  ],
  "quick_wins": [
    {"title":"", "effort":"low|med|high", "impact":"low|med|high", "how":""}
  ],
  "suggested_copy": {
    "headline":"",
    "subheadline":"",
    "bullets":[],
    "cta":""
  }
}
"""

def _extract_text_from_html(html: str) -> str:
    # remove scripts/styles/noscript blocks
    html = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
    # remove all tags
    text = re.sub(r"(?is)<[^>]+>", " ", html)
    # normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def fetch_page_text(url: str, timeout: int = 15) -> dict:
    r = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "fiilthy/1.0"},
        allow_redirects=True,
    )
    html = r.text or ""
    text = _extract_text_from_html(html)
    return {
        "final_url": r.url,
        "status": r.status_code,
        "text_sample": text[:7000],  # limit tokens/cost
    }

def audit_site(url: str) -> dict:
    page = fetch_page_text(url)

    payload = {
        "target_url": url,
        "final_url": page["final_url"],
        "http_status": page["status"],
        "page_text_sample": page["text_sample"],
    }

    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": SITE_SYSTEM},
            {"role": "user", "content": json.dumps(payload)},
        ],
        response_format={"type": "json_object"},
    )

    data = json.loads(resp.output_text)

    # keep meta for debugging
    data["_meta"] = {
        "final_url": page["final_url"],
        "http_status": page["status"],
    }
    return data