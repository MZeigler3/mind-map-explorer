"""
Scrape 10K Diver Twitter threads.
1. Fetch thread index from 10kdiver.com/twitter-threads/
2. Fetch each thread from akhileshs-twitter.com mirror
3. Output data/threads_raw.json
"""

import json
import re
import time
import sys
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

INDEX_URL = "https://10kdiver.com/twitter-threads/"
MIRROR_BASE = "https://akhileshs-twitter.com"
OUTPUT = Path(__file__).parent.parent / "data" / "threads_raw.json"
DELAY = 1.5  # seconds between requests


def fetch_thread_list():
    """Get all thread URLs and titles from the index page."""
    print(f"Fetching index from {INDEX_URL}...")
    resp = requests.get(INDEX_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    threads = []
    # Find all links to twitter.com/10kdiver/status/
    for link in soup.find_all("a", href=re.compile(r"twitter\.com/10kdiver/status/\d+")):
        url = link["href"]
        title = link.get_text(strip=True)
        if not title:
            continue
        # Extract status ID
        match = re.search(r"/status/(\d+)", url)
        if match:
            status_id = match.group(1)
            threads.append({
                "id": status_id,
                "title": title,
                "twitter_url": url,
            })

    # Deduplicate by ID
    seen = set()
    unique = []
    for t in threads:
        if t["id"] not in seen:
            seen.add(t["id"])
            unique.append(t)

    print(f"Found {len(unique)} threads")
    return unique


def extract_date_from_page(soup):
    """Try to extract the thread date from the mirror page."""
    # Look for datetime in time elements
    time_el = soup.find("time")
    if time_el:
        dt = time_el.get("datetime") or time_el.get_text(strip=True)
        return dt
    return None


def fetch_thread_content(thread):
    """Fetch thread content from mirror site."""
    mirror_url = f"{MIRROR_BASE}/10kdiver/status/{thread['id']}"
    try:
        resp = requests.get(mirror_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {thread['title']}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract date
    date = extract_date_from_page(soup)

    # Extract tweet texts - the mirror site puts tweets in timeline items
    tweets = []

    # Try multiple selectors for tweet content
    # Nitter-style mirrors typically use .tweet-content or .timeline-item
    for selector in [".tweet-content", ".content", ".tweet-body", "[class*='tweet']"]:
        elements = soup.select(selector)
        if elements:
            for el in elements:
                text = el.get_text(separator="\n", strip=True)
                if text and len(text) > 10:
                    tweets.append(text)
            break

    # Fallback: look for the main content area and split by structure
    if not tweets:
        # Try getting all text from the main/body area
        main = soup.find("main") or soup.find("body")
        if main:
            # Look for paragraph-like blocks
            for p in main.find_all(["p", "div"], recursive=True):
                text = p.get_text(strip=True)
                if text and len(text) > 20 and "cookie" not in text.lower():
                    tweets.append(text)

    return {
        "id": thread["id"],
        "title": thread["title"],
        "date": date,
        "url": thread["twitter_url"],
        "mirror_url": mirror_url,
        "tweets": tweets,
    }


def main():
    threads = fetch_thread_list()
    if not threads:
        print("No threads found! Check the index page.")
        sys.exit(1)

    results = []
    for i, thread in enumerate(threads):
        print(f"[{i+1}/{len(threads)}] {thread['title']}")
        content = fetch_thread_content(thread)
        if content and content["tweets"]:
            results.append(content)
            print(f"  → {len(content['tweets'])} tweets")
        else:
            # Still save it with empty tweets so we know what failed
            results.append({
                "id": thread["id"],
                "title": thread["title"],
                "date": None,
                "url": thread["twitter_url"],
                "mirror_url": f"{MIRROR_BASE}/10kdiver/status/{thread['id']}",
                "tweets": [],
            })
            print(f"  → No content extracted")
        time.sleep(DELAY)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2)

    total = len(results)
    with_content = sum(1 for r in results if r["tweets"])
    print(f"\nDone! {with_content}/{total} threads with content saved to {OUTPUT}")


if __name__ == "__main__":
    main()
