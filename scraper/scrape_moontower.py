"""
Scrape Moontower content by Kris Abdelmessih.
1. Fetch category index from moontowerquant.com
2. For each category, extract article links (moontowermeta.com)
3. Fetch each article's title + body text
4. Output data/moontower_raw.json
"""

import json
import re
import time
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

INDEX_URL = "https://moontowerquant.com/moontower-content-by-kris-abdelmessih"
OUTPUT = Path(__file__).parent.parent / "data" / "moontower_raw.json"
DELAY = 1.5  # seconds between requests


def fetch_category_urls():
    """Get all category page URLs from the index page."""
    print(f"Fetching index from {INDEX_URL}...")
    resp = requests.get(INDEX_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    categories = []
    # Find links to category pages on moontowerquant.com
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        # Category links are relative paths or full URLs on moontowerquant.com
        # Exclude the index page itself, external links, and anchors
        if not text or href.startswith("#") or href.startswith("mailto:"):
            continue
        # Normalize to absolute URL
        if href.startswith("/"):
            full_url = f"https://moontowerquant.com{href}"
        elif href.startswith("https://moontowerquant.com/"):
            full_url = href
        else:
            continue

        # Skip the index page itself and common non-category pages
        skip_paths = [
            "/moontower-content-by-kris-abdelmessih",
            "/about", "/contact", "/subscribe", "/newsletter",
        ]
        path = full_url.replace("https://moontowerquant.com", "")
        if path in skip_paths or not path or path == "/":
            continue

        # Deduplicate
        if full_url not in [c["url"] for c in categories]:
            categories.append({"url": full_url, "name": text})

    print(f"Found {len(categories)} category pages")
    return categories


def fetch_article_links(category_url, category_name):
    """Fetch article links from a category page."""
    print(f"  Fetching category: {category_name}")
    try:
        resp = requests.get(category_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    articles = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        # Article links are on moontowermeta.com
        if "moontowermeta.com" in href and text:
            if href not in [a["url"] for a in articles]:
                articles.append({"url": href, "title": text})

    print(f"    Found {len(articles)} articles")
    return articles


def fetch_article_content(url):
    """Fetch article page and extract title + body text."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching article: {e}")
        return None, None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title
    title = None
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)

    # Extract body text - try common content selectors
    text = ""
    for selector in ["article", ".post-content", ".entry-content", ".content", "main"]:
        content_el = soup.select_one(selector)
        if content_el:
            # Remove script/style tags
            for tag in content_el.find_all(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = content_el.get_text(separator="\n", strip=True)
            break

    # Fallback to body
    if not text:
        body = soup.find("body")
        if body:
            for tag in body.find_all(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = body.get_text(separator="\n", strip=True)

    return title, text


def main():
    categories = fetch_category_urls()
    if not categories:
        print("No category pages found! Check the index page structure.")
        sys.exit(1)

    results = []
    seen_urls = set()
    article_id = 0

    for cat in categories:
        articles = fetch_article_links(cat["url"], cat["name"])
        time.sleep(DELAY)

        for article in articles:
            if article["url"] in seen_urls:
                continue
            seen_urls.add(article["url"])
            article_id += 1

            print(f"  [{article_id}] {article['title'][:60]}")
            title, text = fetch_article_content(article["url"])
            time.sleep(DELAY)

            results.append({
                "id": f"mt_{article_id}",
                "title": title or article["title"],
                "url": article["url"],
                "category": cat["name"],
                "text": text or "",
            })

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2)

    with_content = sum(1 for r in results if r["text"])
    print(f"\nDone! {with_content}/{len(results)} articles with content saved to {OUTPUT}")


if __name__ == "__main__":
    main()
