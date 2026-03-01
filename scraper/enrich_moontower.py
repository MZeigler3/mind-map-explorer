"""
AI Enrichment: Process raw Moontower articles with Gemini API to generate:
- Summaries, key concepts, difficulty levels
- Cross-article connections, clusters, learning paths
Output: data/moontower_enriched.json
"""

import json
import os
import sys
import time
from pathlib import Path

from google import genai

RAW_PATH = Path(__file__).parent.parent / "data" / "moontower_raw.json"
OUTPUT = Path(__file__).parent.parent / "data" / "moontower_enriched.json"

client = genai.Client()  # reads GEMINI_API_KEY or GOOGLE_API_KEY env var
MODEL = "gemini-2.5-flash"


def strip_fences(text):
    """Remove markdown code fences from response."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def enrich_single_article(article):
    """Get summary, concepts, difficulty for one article."""
    body = article["text"][:8000]  # truncate long articles
    if not body.strip():
        body = f"(No content available. Title: {article['title']})"

    prompt = f"""Analyze this article by Kris Abdelmessih (Moontower) about finance, options, volatility, or decision-making.

Title: {article['title']}
Category: {article.get('category', 'Unknown')}

Article content:
{body}

Return JSON (no markdown fences):
{{
  "summary": "2-3 sentence summary of the article's key message",
  "concepts": ["list", "of", "key", "concepts", "mentioned"],
  "difficulty": "beginner|intermediate|advanced",
  "concept_descriptions": {{"concept_name": "one-line description"}}
}}"""

    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = strip_fences(resp.text)
    return json.loads(text)


def generate_connections(articles_with_meta):
    """Second pass: generate cross-article connections, clusters, and learning paths."""
    article_summaries = []
    for a in articles_with_meta:
        article_summaries.append(
            f"ID: {a['id']}\nTitle: {a['title']}\nCategory: {a.get('category', 'N/A')}\n"
            f"Summary: {a.get('summary', 'N/A')}\n"
            f"Concepts: {', '.join(a.get('concepts', []))}\n"
            f"Difficulty: {a.get('difficulty', 'N/A')}"
        )

    all_summaries_text = "\n\n".join(article_summaries)

    prompt = f"""You have {len(articles_with_meta)} articles by Kris Abdelmessih (Moontower) covering options, volatility, probability, decision-making, and related topics.

Here are all articles:

{all_summaries_text}

Analyze the relationships between these articles and return JSON (no markdown fences):
{{
  "edges": [
    {{"source": "article_id", "target": "article_id", "type": "builds_on|contrasts_with|applies_concept_from|prerequisite_for", "strength": 0.1-1.0, "reason": "brief explanation"}}
  ],
  "clusters": [
    {{"name": "Cluster Name", "color": "#hexcolor", "thread_ids": ["id1", "id2"], "description": "what this cluster covers"}}
  ],
  "learning_paths": [
    {{"name": "Path Name", "description": "who this path is for", "thread_ids": ["id1", "id2", "...in reading order"]}}
  ]
}}

Guidelines:
- Create 6-10 topic clusters based on the actual content (e.g., options pricing, volatility surface, probability/calibration, decision-making, career/life advice, market microstructure, risk management, behavioral finance)
- Create 3-5 learning paths (e.g., "Options & Volatility Fundamentals", "Probability & Decision-Making", "Risk Management Deep Dive")
- Include 30-60 edges capturing the most meaningful connections
- Every article should belong to exactly one cluster
- Use these colors for clusters: #4e79a7, #f28e2b, #e15759, #76b7b2, #59a14f, #edc948, #b07aa1, #ff9da7, #9c755f, #bab0ac"""

    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = strip_fences(resp.text)
    return json.loads(text)


def main():
    if not RAW_PATH.exists():
        print(f"Raw data not found at {RAW_PATH}. Run scrape_moontower.py first.")
        sys.exit(1)

    with open(RAW_PATH) as f:
        raw_articles = json.load(f)

    print(f"Loaded {len(raw_articles)} articles")

    # Pass 1: Enrich individual articles
    enriched = []
    for i, article in enumerate(raw_articles):
        print(f"[{i+1}/{len(raw_articles)}] Enriching: {article['title'][:60]}")
        try:
            meta = enrich_single_article(article)
            enriched.append({
                **article,
                "summary": meta.get("summary", ""),
                "concepts": meta.get("concepts", []),
                "difficulty": meta.get("difficulty", "intermediate"),
            })
        except Exception as e:
            print(f"  Error: {e}")
            enriched.append({
                **article,
                "summary": "",
                "concepts": [],
                "difficulty": "intermediate",
            })
        # Rate limiting
        time.sleep(0.5)

    # Collect all unique concepts
    concept_map = {}
    for a in enriched:
        for c in a.get("concepts", []):
            c_lower = c.lower().strip()
            if c_lower not in concept_map:
                concept_map[c_lower] = {"name": c, "thread_ids": set()}
            concept_map[c_lower]["thread_ids"].add(a["id"])

    # Pass 2: Generate connections
    print("\nGenerating cross-article connections...")
    try:
        connections = generate_connections(enriched)
    except Exception as e:
        print(f"Error generating connections: {e}")
        connections = {"edges": [], "clusters": [], "learning_paths": []}

    # Build final output
    concepts_list = [
        {"name": v["name"], "thread_ids": list(v["thread_ids"]), "description": ""}
        for v in concept_map.values()
        if len(v["thread_ids"]) >= 2
    ]

    cluster_lookup = {}
    for cluster in connections.get("clusters", []):
        for tid in cluster.get("thread_ids", []):
            cluster_lookup[tid] = cluster["name"]

    for a in enriched:
        a["cluster"] = cluster_lookup.get(a["id"], "General")

    output = {
        "threads": enriched,
        "concepts": sorted(concepts_list, key=lambda x: len(x["thread_ids"]), reverse=True),
        "edges": connections.get("edges", []),
        "learning_paths": connections.get("learning_paths", []),
        "clusters": connections.get("clusters", []),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nDone! Saved to {OUTPUT}")
    print(f"  {len(enriched)} articles")
    print(f"  {len(concepts_list)} shared concepts")
    print(f"  {len(output['edges'])} edges")
    print(f"  {len(output['clusters'])} clusters")
    print(f"  {len(output['learning_paths'])} learning paths")


if __name__ == "__main__":
    main()
