# logic/query_builder.py

from dataclasses import dataclass, field
from typing import Optional, Sequence, Tuple, Set
import re


# Buyer-intent phrases (high conversion)
DEFAULT_INTENTS = [
    '"looking for"',
    '"need a"',
    '"anyone recommend"',
    '"recommend a"',
    '"who do you use"',
    '"quote"',
    '"pricing"',
    '"hire"',
    '"hiring"',
    '"best"',
]


# Sites to target (Google will find posts on these)
DEFAULT_SITES = [
    "reddit.com",
    "quora.com",
    "news.ycombinator.com",
    "indiehackers.com",
    "stackexchange.com",
]


# Company structure
@dataclass(frozen=True)
class CompanyProfile:
    url: str
    name: Optional[str] = None
    niche: Optional[str] = None
    keywords: Tuple[str, ...] = field(default_factory=tuple)
    locations: Tuple[str, ...] = field(default_factory=tuple)


# Clean text helper
def clean(term: str) -> str:
    return re.sub(r"\s+", " ", (term or "").strip())


# Remove duplicates safely
def dedupe(items):
    seen: Set[str] = set()
    out = []

    for x in items:
        cx = clean(x)

        if not cx:
            continue

        key = cx.lower()

        if key in seen:
            continue

        seen.add(key)
        out.append(cx)

    return out


# Turn keywords into OR search group
def keyword_group(keywords):

    keywords = dedupe(keywords)

    if not keywords:
        return ""

    quoted = []

    for k in keywords:
        quoted.append(f'"{k}"')

    return "(" + " OR ".join(quoted) + ")"


# MAIN FUNCTION — builds global search queries
def build_queries(
    company: CompanyProfile,
    max_queries: int = 50,
    sites: Sequence[str] = DEFAULT_SITES,
):

    keywords = list(company.keywords)

    if company.niche:
        keywords.append(company.niche)

    if company.name:
        keywords.append(company.name)

    kw_group = keyword_group(keywords)

    if not kw_group:
        return []

    queries = []

    for intent in DEFAULT_INTENTS:

        # Global search
        queries.append(f"{intent} {kw_group}")

        # Site-specific search
        for site in sites:
            queries.append(f"{intent} {kw_group} site:{site}")

        # Location-specific search
        for loc in company.locations:
            queries.append(f"{intent} {kw_group} \"{loc}\"")

            for site in sites:
                queries.append(f"{intent} {kw_group} \"{loc}\" site:{site}")

    return dedupe(queries)[:max_queries]


# Test block (optional)
if __name__ == "__main__":

    company = CompanyProfile(
        url="https://example.com",
        name="Example Co",
        niche="seo",
        keywords=("seo", "marketing", "website design"),
        locations=("Kelowna", "Vancouver"),
    )

    queries = build_queries(company)

    for q in queries:
        print(q)