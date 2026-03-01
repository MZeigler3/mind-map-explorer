"""
AI Enrichment: Process raw threads with Gemini API to generate:
- Summaries, key concepts, difficulty levels
- Cross-thread connections, clusters, learning paths
Output: data/threads_enriched.json
"""

import json
import os
import sys
import time
from pathlib import Path

from google import genai

RAW_PATH = Path(__file__).parent.parent / "data" / "threads_raw.json"
OUTPUT = Path(__file__).parent.parent / "data" / "threads_enriched.json"

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


def enrich_single_thread(thread):
    """Get summary, concepts, difficulty for one thread."""
    tweet_text = "\n---\n".join(thread["tweets"][:50])
    if not tweet_text.strip():
        tweet_text = f"(No tweet content available. Title: {thread['title']})"

    prompt = f"""Analyze this Twitter thread by @10kdiver about finance/investing.

Title: {thread['title']}

Thread content:
{tweet_text}

Return JSON (no markdown fences):
{{
  "summary": "2-3 sentence summary of the thread's key message",
  "concepts": ["list", "of", "key", "concepts", "mentioned"],
  "difficulty": "beginner|intermediate|advanced",
  "concept_descriptions": {{"concept_name": "one-line description"}}
}}"""

    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = strip_fences(resp.text)
    return json.loads(text)


def generate_connections(threads_with_meta):
    """Second pass: generate cross-thread connections, clusters, and learning paths."""
    thread_summaries = []
    for t in threads_with_meta:
        thread_summaries.append(
            f"ID: {t['id']}\nTitle: {t['title']}\n"
            f"Summary: {t.get('summary', 'N/A')}\n"
            f"Concepts: {', '.join(t.get('concepts', []))}\n"
            f"Difficulty: {t.get('difficulty', 'N/A')}"
        )

    all_summaries_text = "\n\n".join(thread_summaries)

    prompt = f"""You have {len(threads_with_meta)} finance/investing threads by @10kdiver.

Here are all threads:

{all_summaries_text}

Analyze the relationships between these threads and return JSON (no markdown fences):
{{
  "edges": [
    {{"source": "thread_id", "target": "thread_id", "type": "builds_on|contrasts_with|applies_concept_from|prerequisite_for", "strength": 0.1-1.0, "reason": "brief explanation"}}
  ],
  "clusters": [
    {{"name": "Cluster Name", "color": "#hexcolor", "thread_ids": ["id1", "id2"], "description": "what this cluster covers"}}
  ],
  "learning_paths": [
    {{"name": "Path Name", "description": "who this path is for", "thread_ids": ["id1", "id2", "...in reading order"]}}
  ]
}}

Guidelines:
- Create 6-8 topic clusters (compounding, valuation, probability/statistics, behavioral finance, portfolio theory, accounting/fundamentals, options/derivatives, general investing wisdom)
- Create 3-5 learning paths (e.g., "Compounding Fundamentals", "Probability & Risk", "Complete Beginner Path")
- Include 30-60 edges capturing the most meaningful connections
- Every thread should belong to exactly one cluster
- Use these colors for clusters: #4e79a7, #f28e2b, #e15759, #76b7b2, #59a14f, #edc948, #b07aa1, #ff9da7"""

    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = strip_fences(resp.text)
    return json.loads(text)


def generate_quizzes(threads, edges):
    """Pass 3: Generate quiz questions for spaced repetition."""
    quizzes = []

    # Batch threads ~10 at a time for concept_recall + multiple_choice
    batch_size = 10
    for batch_start in range(0, len(threads), batch_size):
        batch = threads[batch_start:batch_start + batch_size]
        thread_block = "\n\n".join(
            f"ID: {t['id']}\nTitle: {t['title']}\nSummary: {t.get('summary', 'N/A')}\n"
            f"Concepts: {', '.join(t.get('concepts', []))}\nDifficulty: {t.get('difficulty', 'intermediate')}"
            for t in batch
        )

        prompt = f"""Generate quiz questions for these finance/investing threads by @10kdiver.

{thread_block}

For EACH thread, generate exactly 2 questions:
1. A concept_recall question (open-ended, tests understanding of the key idea)
2. A multiple_choice question (4 choices, tests specific knowledge)

Return JSON array (no markdown fences):
[
  {{
    "id": "q_<thread_id>_0",
    "thread_id": "<thread_id>",
    "type": "concept_recall",
    "question": "...",
    "answer": "...",
    "difficulty": "beginner|intermediate|advanced",
    "concepts": ["concept1", "concept2"]
  }},
  {{
    "id": "q_mc_<thread_id>_0",
    "thread_id": "<thread_id>",
    "type": "multiple_choice",
    "question": "...",
    "choices": ["A) ...", "B) ...", "C) ...", "D) ..."],
    "correct_index": 0,
    "explanation": "...",
    "difficulty": "beginner|intermediate|advanced",
    "concepts": ["concept1", "concept2"]
  }}
]

Make questions test genuine understanding, not just memorization."""

        try:
            resp = client.models.generate_content(model=MODEL, contents=prompt)
            text = strip_fences(resp.text)
            batch_quizzes = json.loads(text)
            quizzes.extend(batch_quizzes)
        except Exception as e:
            print(f"  Quiz batch error: {e}")
        time.sleep(0.5)

    # Connection questions from edges
    edge_batch_size = 20
    for batch_start in range(0, len(edges), edge_batch_size):
        batch = edges[batch_start:batch_start + edge_batch_size]
        edge_block = "\n".join(
            f"Source: {e['source']} -> Target: {e['target']} | Type: {e.get('type', 'related')} | Reason: {e.get('reason', 'N/A')}"
            for e in batch
        )
        # Build a title lookup
        title_map = {t['id']: t['title'] for t in threads}
        edge_context = "\n".join(
            f"{title_map.get(e['source'], e['source'])} -> {title_map.get(e['target'], e['target'])}: {e.get('reason', 'N/A')}"
            for e in batch
        )

        prompt = f"""Generate 1 connection quiz question per edge. These edges connect finance/investing threads.

Edges:
{edge_context}

For each edge, generate a question testing understanding of how the two topics connect.

Return JSON array (no markdown fences):
[
  {{
    "id": "q_edge_<source_id>_<target_id>_0",
    "type": "connection",
    "source_thread_id": "<source_id>",
    "target_thread_id": "<target_id>",
    "question": "...",
    "answer": "...",
    "difficulty": "intermediate",
    "concepts": ["concept1"]
  }}
]"""

        try:
            resp = client.models.generate_content(model=MODEL, contents=prompt)
            text = strip_fences(resp.text)
            edge_quizzes = json.loads(text)
            quizzes.extend(edge_quizzes)
        except Exception as e:
            print(f"  Edge quiz batch error: {e}")
        time.sleep(0.5)

    return quizzes


def main():
    if not RAW_PATH.exists():
        print(f"Raw data not found at {RAW_PATH}. Run scrape_threads.py first.")
        sys.exit(1)

    with open(RAW_PATH) as f:
        raw_threads = json.load(f)

    print(f"Loaded {len(raw_threads)} threads")

    # Pass 1: Enrich individual threads
    enriched = []
    for i, thread in enumerate(raw_threads):
        print(f"[{i+1}/{len(raw_threads)}] Enriching: {thread['title']}")
        try:
            meta = enrich_single_thread(thread)
            enriched.append({
                **thread,
                "summary": meta.get("summary", ""),
                "concepts": meta.get("concepts", []),
                "difficulty": meta.get("difficulty", "intermediate"),
            })
        except Exception as e:
            print(f"  Error: {e}")
            enriched.append({
                **thread,
                "summary": "",
                "concepts": [],
                "difficulty": "intermediate",
            })
        # Rate limiting
        time.sleep(0.5)

    # Collect all unique concepts
    concept_map = {}
    for t in enriched:
        for c in t.get("concepts", []):
            c_lower = c.lower().strip()
            if c_lower not in concept_map:
                concept_map[c_lower] = {"name": c, "thread_ids": set()}
            concept_map[c_lower]["thread_ids"].add(t["id"])

    # Pass 2: Generate connections
    print("\nGenerating cross-thread connections...")
    try:
        connections = generate_connections(enriched)
    except Exception as e:
        print(f"Error generating connections: {e}")
        connections = {"edges": [], "clusters": [], "learning_paths": []}

    # Pass 3: Generate quizzes
    print("\nGenerating quiz questions...")
    try:
        quizzes = generate_quizzes(enriched, connections.get("edges", []))
    except Exception as e:
        print(f"Error generating quizzes: {e}")
        quizzes = []
    print(f"  Generated {len(quizzes)} quiz questions")

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

    for t in enriched:
        t["cluster"] = cluster_lookup.get(t["id"], "General")

    output = {
        "threads": enriched,
        "concepts": sorted(concepts_list, key=lambda x: len(x["thread_ids"]), reverse=True),
        "edges": connections.get("edges", []),
        "learning_paths": connections.get("learning_paths", []),
        "clusters": connections.get("clusters", []),
        "quizzes": quizzes,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nDone! Saved to {OUTPUT}")
    print(f"  {len(enriched)} threads")
    print(f"  {len(concepts_list)} shared concepts")
    print(f"  {len(output['edges'])} edges")
    print(f"  {len(output['clusters'])} clusters")
    print(f"  {len(output['learning_paths'])} learning paths")
    print(f"  {len(output['quizzes'])} quiz questions")


if __name__ == "__main__":
    main()
