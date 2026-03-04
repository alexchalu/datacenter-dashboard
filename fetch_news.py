#!/usr/bin/env python3
"""
Infrastructure Intel — News Fetcher
Fetches datacenter construction + CoreWeave news via Brave Search API.
Scores importance and outputs data/news.json for the dashboard.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "news.json")

DATACENTER_QUERIES = [
    "US datacenter construction 2025 2026",
    "new data center campus United States",
    "hyperscale datacenter construction announcement",
    "data center infrastructure investment US",
    "datacenter groundbreaking commissioning US",
]

COREWEAVE_QUERIES = [
    "CoreWeave news",
    "CoreWeave data center",
    "CoreWeave funding investment",
    "CoreWeave GPU cloud infrastructure",
]

# Importance scoring keywords
CRITICAL_KEYWORDS = [
    "billion", "gigawatt", "gw", "massive", "largest", "record",
    "ipo", "acquisition", "merge", "regulatory", "ban", "halt",
    "shutdown", "crisis", "emergency",
]
HIGH_KEYWORDS = [
    "million", "megawatt", "mw", "expansion", "partnership",
    "contract", "deal", "funding", "investment", "announce",
    "launch", "open", "groundbreaking", "approved",
]
MEDIUM_KEYWORDS = [
    "plan", "proposed", "permit", "review", "study", "development",
    "campus", "facility", "site", "construction", "build",
]

# Tag extraction patterns
TAG_PATTERNS = {
    "AI/ML": r"\b(artificial intelligence|machine learning|ai training|gpu|nvidia|ai infrastructure)\b",
    "Power": r"\b(megawatt|gigawatt|power grid|energy|electricity|nuclear|solar|renewable)\b",
    "Hyperscale": r"\b(hyperscal|mega.?campus|large.?scale)\b",
    "Cloud": r"\b(cloud computing|cloud infrastructure|aws|azure|google cloud|gcp)\b",
    "Cooling": r"\b(liquid cooling|immersion cooling|cooling system)\b",
    "Fiber": r"\b(fiber optic|submarine cable|network infrastructure)\b",
    "Real Estate": r"\b(land acquisition|real estate|zoning|property)\b",
    "Policy": r"\b(regulation|policy|legislation|government|federal|state)\b",
    "IPO": r"\b(ipo|public offering|stock|nasdaq|nyse)\b",
    "Funding": r"\b(funding|investment|venture|series [a-e]|raise)\b",
}


def brave_search(query, count=10):
    """Search using Brave Search API."""
    if not BRAVE_API_KEY:
        print(f"  ⚠ No BRAVE_API_KEY set, skipping: {query}", file=sys.stderr)
        return []

    params = urllib.parse.urlencode({
        "q": query,
        "count": count,
        "freshness": "pw",  # past week
        "text_decorations": "false",
    })
    url = f"https://api.search.brave.com/res/v1/web/search?{params}"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "X-Subscription-Token": BRAVE_API_KEY,
    })

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            return data.get("web", {}).get("results", [])
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        print(f"  ✗ Search failed for '{query}': {e}", file=sys.stderr)
        return []


def score_importance(title, snippet):
    """Score article importance based on keyword analysis."""
    text = f"{title} {snippet}".lower()

    critical_hits = sum(1 for kw in CRITICAL_KEYWORDS if kw in text)
    high_hits = sum(1 for kw in HIGH_KEYWORDS if kw in text)
    medium_hits = sum(1 for kw in MEDIUM_KEYWORDS if kw in text)

    if critical_hits >= 2:
        return "critical"
    if critical_hits >= 1 or high_hits >= 3:
        return "high"
    if high_hits >= 1 or medium_hits >= 2:
        return "medium"
    return "low"


def extract_tags(title, snippet):
    """Extract relevant tags from article text."""
    text = f"{title} {snippet}".lower()
    tags = []
    for tag, pattern in TAG_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            tags.append(tag)
    return tags[:4]  # Max 4 tags


def extract_source(url):
    """Extract clean source name from URL."""
    try:
        domain = urllib.parse.urlparse(url).netloc
        domain = domain.replace("www.", "")
        # Common source name mappings
        name_map = {
            "reuters.com": "Reuters",
            "bloomberg.com": "Bloomberg",
            "cnbc.com": "CNBC",
            "techcrunch.com": "TechCrunch",
            "datacenterdynamics.com": "DCD",
            "datacenterknowledge.com": "DCK",
            "theregister.com": "The Register",
            "arstechnica.com": "Ars Technica",
            "wsj.com": "WSJ",
            "ft.com": "Financial Times",
            "nytimes.com": "NYT",
            "theverge.com": "The Verge",
            "wired.com": "Wired",
            "zdnet.com": "ZDNet",
            "venturebeat.com": "VentureBeat",
            "siliconangle.com": "SiliconANGLE",
            "servethehome.com": "ServeTheHome",
            "coreweave.com": "CoreWeave",
            "prnewswire.com": "PR Newswire",
            "businesswire.com": "Business Wire",
            "globenewswire.com": "GlobeNewswire",
        }
        return name_map.get(domain, domain.split(".")[0].capitalize())
    except Exception:
        return "Unknown"


def deduplicate(articles):
    """Remove near-duplicate articles by title similarity."""
    seen_titles = set()
    unique = []
    for article in articles:
        # Normalize title for comparison
        normalized = re.sub(r'[^a-z0-9\s]', '', article['title'].lower())
        words = set(normalized.split())

        is_dupe = False
        for seen in seen_titles:
            overlap = len(words & seen) / max(len(words | seen), 1)
            if overlap > 0.6:
                is_dupe = True
                break

        if not is_dupe:
            seen_titles.add(frozenset(words))
            unique.append(article)

    return unique


def fetch_category(queries, label):
    """Fetch and process articles for a category."""
    print(f"\n{'='*50}")
    print(f"Fetching: {label}")
    print(f"{'='*50}")

    raw_articles = []
    for query in queries:
        print(f"  → {query}")
        results = brave_search(query)
        print(f"    {len(results)} results")

        for r in results:
            article = {
                "title": r.get("title", "").strip(),
                "snippet": r.get("description", "").strip(),
                "url": r.get("url", ""),
                "source": extract_source(r.get("url", "")),
                "date": "",
                "importance": "low",
                "tags": [],
            }

            # Extract date if available
            age = r.get("age", "")
            if age:
                article["date"] = age

            # Score and tag
            article["importance"] = score_importance(article["title"], article["snippet"])
            article["tags"] = extract_tags(article["title"], article["snippet"])

            if article["title"] and article["url"]:
                raw_articles.append(article)

    # Deduplicate
    articles = deduplicate(raw_articles)
    print(f"\n  Total: {len(raw_articles)} raw → {len(articles)} unique")

    # Sort by importance
    weight = {"critical": 4, "high": 3, "medium": 2, "low": 1}
    articles.sort(key=lambda a: weight.get(a["importance"], 0), reverse=True)

    return articles


def main():
    print("Infrastructure Intel — News Fetcher")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")

    if not BRAVE_API_KEY:
        print("\n⚠ BRAVE_API_KEY not set. Set it as a GitHub Actions secret or environment variable.")
        print("  Generating sample data for development...\n")
        generate_sample_data()
        return

    dc_articles = fetch_category(DATACENTER_QUERIES, "US Datacenter Construction")
    cw_articles = fetch_category(COREWEAVE_QUERIES, "CoreWeave")

    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "datacenter": dc_articles[:30],  # Cap at 30
        "coreweave": cw_articles[:20],   # Cap at 20
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Wrote {len(dc_articles)} datacenter + {len(cw_articles)} CoreWeave articles to {OUTPUT_PATH}")


def generate_sample_data():
    """Generate sample data for development/testing."""
    sample = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "datacenter": [
            {
                "title": "Microsoft Announces $10B Data Center Campus in Wisconsin",
                "snippet": "Microsoft will invest $10 billion to build a massive hyperscale data center campus in Mount Pleasant, Wisconsin, to power AI workloads.",
                "url": "https://example.com/microsoft-wisconsin",
                "source": "Reuters",
                "date": "2 hours ago",
                "importance": "critical",
                "tags": ["AI/ML", "Hyperscale", "Power"]
            },
            {
                "title": "Amazon Web Services Breaks Ground on 500MW Virginia Facility",
                "snippet": "AWS has begun construction on a new 500-megawatt data center facility in Northern Virginia, expanding its presence in the largest data center market.",
                "url": "https://example.com/aws-virginia",
                "source": "DCD",
                "date": "5 hours ago",
                "importance": "critical",
                "tags": ["Hyperscale", "Power", "Cloud"]
            },
            {
                "title": "Google Plans $3B Investment in Ohio Data Center Expansion",
                "snippet": "Google has announced plans to invest $3 billion in expanding its data center operations across multiple Ohio campuses to support growing cloud demand.",
                "url": "https://example.com/google-ohio",
                "source": "CNBC",
                "date": "1 day ago",
                "importance": "high",
                "tags": ["Cloud", "Hyperscale", "Funding"]
            },
            {
                "title": "Texas Grid Operator Raises Concerns Over Data Center Power Demand",
                "snippet": "ERCOT has flagged growing electricity demand from data centers as a potential strain on the Texas power grid, calling for updated infrastructure planning.",
                "url": "https://example.com/ercot-texas",
                "source": "Bloomberg",
                "date": "1 day ago",
                "importance": "high",
                "tags": ["Power", "Policy"]
            },
            {
                "title": "Equinix Receives Approval for New Chicago Data Center",
                "snippet": "Equinix has received zoning approval to build a new 60MW data center in the Chicago metropolitan area, targeting enterprise and cloud customers.",
                "url": "https://example.com/equinix-chicago",
                "source": "DCK",
                "date": "2 days ago",
                "importance": "medium",
                "tags": ["Real Estate", "Power"]
            },
            {
                "title": "Nuclear Power Seen as Key to Sustaining Data Center Growth",
                "snippet": "Industry leaders are increasingly looking to nuclear energy, including small modular reactors, as a sustainable power source for next-generation data centers.",
                "url": "https://example.com/nuclear-datacenter",
                "source": "Wired",
                "date": "3 days ago",
                "importance": "medium",
                "tags": ["Power", "Policy"]
            },
        ],
        "coreweave": [
            {
                "title": "CoreWeave Secures $7.5B in Debt Financing for GPU Cloud Expansion",
                "snippet": "CoreWeave has raised $7.5 billion in debt financing to expand its GPU cloud infrastructure, marking one of the largest funding rounds for an AI infrastructure company.",
                "url": "https://example.com/coreweave-funding",
                "source": "TechCrunch",
                "date": "3 hours ago",
                "importance": "critical",
                "tags": ["Funding", "AI/ML", "Cloud"]
            },
            {
                "title": "CoreWeave Opens New Data Center in New Jersey",
                "snippet": "CoreWeave has opened a new data center facility in Weehawken, New Jersey, adding thousands of NVIDIA GPUs to its cloud infrastructure platform.",
                "url": "https://example.com/coreweave-nj",
                "source": "SiliconANGLE",
                "date": "1 day ago",
                "importance": "high",
                "tags": ["AI/ML", "Cloud"]
            },
            {
                "title": "CoreWeave Partners with NVIDIA on Next-Gen AI Infrastructure",
                "snippet": "CoreWeave has announced an expanded partnership with NVIDIA to deploy next-generation GPU clusters optimized for large language model training.",
                "url": "https://example.com/coreweave-nvidia",
                "source": "VentureBeat",
                "date": "2 days ago",
                "importance": "high",
                "tags": ["AI/ML", "Cloud"]
            },
            {
                "title": "CoreWeave IPO Timeline Reportedly Pushed to Late 2025",
                "snippet": "Sources close to the company suggest CoreWeave's planned IPO may be pushed to late 2025 as the company focuses on scaling infrastructure.",
                "url": "https://example.com/coreweave-ipo",
                "source": "Bloomberg",
                "date": "4 days ago",
                "importance": "medium",
                "tags": ["IPO", "Funding"]
            },
        ]
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(sample, f, indent=2)

    print(f"✓ Sample data written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
