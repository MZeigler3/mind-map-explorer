"""
Standalone quiz generation: adds quizzes to existing enriched JSON
without re-running Pass 1 (individual enrichment) or Pass 2 (connections).

Usage:
  python3 scraper/generate_quizzes.py                  # defaults to threads_enriched.json
  python3 scraper/generate_quizzes.py moontower        # for moontower_enriched.json
"""

import json
import sys
import time
from pathlib import Path

from google import genai

DATA_DIR = Path(__file__).parent.parent / "data"
client = genai.Client()
MODEL = "gemini-2.5-flash"


def strip_fences(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def generate_quizzes(threads, edges, topic_label):
    quizzes = []
    batch_size = 10

    for batch_start in range(0, len(threads), batch_size):
        batch = threads[batch_start:batch_start + batch_size]
        block = "\n\n".join(
            f"ID: {t['id']}\nTitle: {t['title']}\nSummary: {t.get('summary', 'N/A')}\n"
            f"Concepts: {', '.join(t.get('concepts', []))}\nDifficulty: {t.get('difficulty', 'intermediate')}"
            for t in batch
        )

        prompt = f"""Generate quiz questions for these {topic_label}.

{block}

For EACH item, generate exactly 2 questions:
1. A concept_recall question (open-ended, tests understanding of the key idea)
2. A multiple_choice question (4 choices, tests specific knowledge)

Return JSON array (no markdown fences):
[
  {{
    "id": "q_<item_id>_0",
    "thread_id": "<item_id>",
    "type": "concept_recall",
    "question": "...",
    "answer": "...",
    "difficulty": "beginner|intermediate|advanced",
    "concepts": ["concept1", "concept2"]
  }},
  {{
    "id": "q_mc_<item_id>_0",
    "thread_id": "<item_id>",
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

        print(f"  Batch {batch_start // batch_size + 1}/{(len(threads) + batch_size - 1) // batch_size}")
        try:
            resp = client.models.generate_content(model=MODEL, contents=prompt)
            text = strip_fences(resp.text)
            quizzes.extend(json.loads(text))
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(0.5)

    # Connection quizzes
    title_map = {t['id']: t['title'] for t in threads}
    edge_batch_size = 20
    for batch_start in range(0, len(edges), edge_batch_size):
        batch = edges[batch_start:batch_start + edge_batch_size]
        edge_context = "\n".join(
            f"{title_map.get(e['source'], e['source'])} -> {title_map.get(e['target'], e['target'])}: {e.get('reason', 'N/A')}"
            for e in batch
        )

        prompt = f"""Generate 1 connection quiz question per edge. These edges connect {topic_label}.

Edges:
{edge_context}

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

        print(f"  Edge batch {batch_start // edge_batch_size + 1}/{(len(edges) + edge_batch_size - 1) // edge_batch_size}")
        try:
            resp = client.models.generate_content(model=MODEL, contents=prompt)
            text = strip_fences(resp.text)
            quizzes.extend(json.loads(text))
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(0.5)

    return quizzes


def main():
    dataset = sys.argv[1] if len(sys.argv) > 1 else "10kdiver"

    if dataset == "moontower":
        filepath = DATA_DIR / "moontower_enriched.json"
        topic = "articles about options, volatility, and decision-making by Kris Abdelmessih"
    else:
        filepath = DATA_DIR / "threads_enriched.json"
        topic = "finance/investing threads by @10kdiver"

    if not filepath.exists():
        print(f"Not found: {filepath}. Run enrichment first.")
        sys.exit(1)

    with open(filepath) as f:
        data = json.load(f)

    threads = data.get("threads", [])
    edges = data.get("edges", [])
    print(f"Loaded {len(threads)} items, {len(edges)} edges from {filepath.name}")

    quizzes = generate_quizzes(threads, edges, topic)
    print(f"\nGenerated {len(quizzes)} quiz questions")

    data["quizzes"] = quizzes
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved to {filepath}")


if __name__ == "__main__":
    main()
