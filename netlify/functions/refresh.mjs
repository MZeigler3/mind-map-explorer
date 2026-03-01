import { getStore } from "@netlify/blobs";
import { GoogleGenerativeAI } from "@google/generative-ai";
import * as cheerio from "cheerio";
import { readFile } from "fs/promises";
import { join } from "path";

const VALID_DATASETS = { moontower: "moontower_enriched", "10kdiver": "threads_enriched" };
const MODEL_NAME = "gemini-2.5-flash";

// ============================================================
// Helpers
// ============================================================
function stripFences(text) {
  text = text.trim();
  if (text.startsWith("```")) text = text.split("\n", 1).length > 1 ? text.slice(text.indexOf("\n") + 1) : text;
  if (text.endsWith("```")) text = text.slice(0, -3);
  return text.trim();
}

async function fetchPage(url) {
  const resp = await fetch(url, { headers: { "User-Agent": "MindMapBot/1.0" }, signal: AbortSignal.timeout(15000) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  return await resp.text();
}

// ============================================================
// Load existing data (from Blobs or static fallback)
// ============================================================
async function loadExistingData(blobKey) {
  try {
    const store = getStore("mindmap-data");
    const data = await store.get(blobKey, { type: "json" });
    if (data) return data;
  } catch (e) {
    console.log("Blobs read failed:", e.message);
  }
  // Fallback to static files
  const filePath = join(process.cwd(), "data", `${blobKey}.json`);
  const raw = await readFile(filePath, "utf-8");
  return JSON.parse(raw);
}

// ============================================================
// 10K Diver scraping
// ============================================================
async function scrape10kDiverNew(existingIds) {
  const html = await fetchPage("https://10kdiver.com/twitter-threads/");
  const $ = cheerio.load(html);

  const allThreads = [];
  $('a[href*="twitter.com/10kdiver/status/"]').each((_, el) => {
    const href = $(el).attr("href");
    const title = $(el).text().trim();
    const match = href.match(/\/status\/(\d+)/);
    if (match && title) {
      allThreads.push({ id: match[1], title, url: href });
    }
  });

  // Deduplicate
  const seen = new Set();
  const unique = allThreads.filter((t) => {
    if (seen.has(t.id)) return false;
    seen.add(t.id);
    return true;
  });

  // Find new ones
  const newThreads = unique.filter((t) => !existingIds.has(t.id));
  console.log(`10K Diver: ${unique.length} total, ${newThreads.length} new`);

  // Fetch content for new threads
  const results = [];
  for (const thread of newThreads) {
    try {
      const mirrorUrl = `https://akhileshs-twitter.com/10kdiver/status/${thread.id}`;
      const pageHtml = await fetchPage(mirrorUrl);
      const $page = cheerio.load(pageHtml);

      const tweets = [];
      for (const selector of [".tweet-content", ".content", ".tweet-body", "[class*='tweet']"]) {
        $page(selector).each((_, el) => {
          const text = $page(el).text().trim();
          if (text && text.length > 10) tweets.push(text);
        });
        if (tweets.length) break;
      }

      // Fallback
      if (!tweets.length) {
        $page("main p, body p, body div").each((_, el) => {
          const text = $page(el).text().trim();
          if (text && text.length > 20 && !text.toLowerCase().includes("cookie")) tweets.push(text);
        });
      }

      results.push({
        id: thread.id,
        title: thread.title,
        date: null,
        url: thread.url,
        mirror_url: mirrorUrl,
        tweets,
      });
    } catch (e) {
      console.log(`Error fetching thread ${thread.id}: ${e.message}`);
    }
  }
  return results;
}

// ============================================================
// Moontower scraping
// ============================================================
async function scrapeMoontowerNew(existingUrls) {
  const indexHtml = await fetchPage("https://moontowerquant.com/moontower-content-by-kris-abdelmessih");
  const $ = cheerio.load(indexHtml);

  // Get category page URLs
  const categoryUrls = [];
  const skipPaths = ["/moontower-content-by-kris-abdelmessih", "/about", "/contact", "/subscribe", "/newsletter"];

  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    const text = $(el).text().trim();
    if (!text || href.startsWith("#") || href.startsWith("mailto:")) return;

    let fullUrl;
    if (href.startsWith("/")) fullUrl = `https://moontowerquant.com${href}`;
    else if (href.startsWith("https://moontowerquant.com/")) fullUrl = href;
    else return;

    const path = fullUrl.replace("https://moontowerquant.com", "");
    if (skipPaths.includes(path) || !path || path === "/") return;
    if (!categoryUrls.find((c) => c.url === fullUrl)) {
      categoryUrls.push({ url: fullUrl, name: text });
    }
  });

  // For each category, get article links
  const allArticles = [];
  const seenUrls = new Set();

  for (const cat of categoryUrls) {
    try {
      const catHtml = await fetchPage(cat.url);
      const $cat = cheerio.load(catHtml);

      $cat('a[href*="moontowermeta.com"]').each((_, el) => {
        const href = $cat(el).attr("href");
        const title = $cat(el).text().trim();
        if (title && !seenUrls.has(href)) {
          seenUrls.add(href);
          allArticles.push({ url: href, title, category: cat.name });
        }
      });
    } catch (e) {
      console.log(`Error fetching category ${cat.name}: ${e.message}`);
    }
  }

  // Find new ones
  const newArticles = allArticles.filter((a) => !existingUrls.has(a.url));
  console.log(`Moontower: ${allArticles.length} total, ${newArticles.length} new`);

  // Fetch content for new articles
  const results = [];

  for (const article of newArticles) {
    try {
      const pageHtml = await fetchPage(article.url);
      const $page = cheerio.load(pageHtml);

      // Extract title
      let title = $page("h1").first().text().trim();
      if (!title) title = $page("title").text().trim();
      if (!title) title = article.title;

      // Extract body
      let text = "";
      for (const selector of ["article", ".post-content", ".entry-content", ".content", "main"]) {
        const el = $page(selector).first();
        if (el.length) {
          el.find("script, style, nav, footer, header").remove();
          text = el.text().trim();
          break;
        }
      }
      if (!text) {
        const body = $page("body");
        body.find("script, style, nav, footer, header").remove();
        text = body.text().trim();
      }

      results.push({
        id: null, // assigned later
        title: title || article.title,
        url: article.url,
        category: article.category,
        text: text || "",
      });
    } catch (e) {
      console.log(`Error fetching article ${article.url}: ${e.message}`);
    }
  }
  return results;
}

// ============================================================
// Gemini enrichment
// ============================================================
function getGenAI() {
  const key = process.env.GEMINI_API_KEY;
  if (!key) throw new Error("GEMINI_API_KEY not set");
  return new GoogleGenerativeAI(key);
}

async function enrichSingleItem(model, item, datasetType) {
  let content, prompt;

  if (datasetType === "10kdiver") {
    content = (item.tweets || []).slice(0, 50).join("\n---\n");
    if (!content.trim()) content = `(No content. Title: ${item.title})`;
    prompt = `Analyze this Twitter thread by @10kdiver about finance/investing.

Title: ${item.title}

Thread content:
${content}

Return JSON (no markdown fences):
{
  "summary": "2-3 sentence summary of the thread's key message",
  "concepts": ["list", "of", "key", "concepts"],
  "difficulty": "beginner|intermediate|advanced"
}`;
  } else {
    content = (item.text || "").slice(0, 8000);
    if (!content.trim()) content = `(No content. Title: ${item.title})`;
    prompt = `Analyze this article by Kris Abdelmessih (Moontower) about finance, options, volatility, or decision-making.

Title: ${item.title}
Category: ${item.category || "Unknown"}

Article content:
${content}

Return JSON (no markdown fences):
{
  "summary": "2-3 sentence summary of the article's key message",
  "concepts": ["list", "of", "key", "concepts"],
  "difficulty": "beginner|intermediate|advanced"
}`;
  }

  const result = await model.generateContent(prompt);
  const text = stripFences(result.response.text());
  return JSON.parse(text);
}

async function generateConnections(model, allItems, datasetType) {
  const summaries = allItems
    .map(
      (t) =>
        `ID: ${t.id}\nTitle: ${t.title}\nSummary: ${t.summary || "N/A"}\nConcepts: ${(t.concepts || []).join(", ")}\nDifficulty: ${t.difficulty || "N/A"}`
    )
    .join("\n\n");

  const clusterGuidance =
    datasetType === "10kdiver"
      ? "compounding, valuation, probability/statistics, behavioral finance, portfolio theory, accounting/fundamentals, options/derivatives, general investing wisdom"
      : "options pricing, volatility surface, probability/calibration, decision-making, career/life advice, market microstructure, risk management, behavioral finance";

  const colors =
    datasetType === "10kdiver"
      ? "#4e79a7, #f28e2b, #e15759, #76b7b2, #59a14f, #edc948, #b07aa1, #ff9da7"
      : "#4e79a7, #f28e2b, #e15759, #76b7b2, #59a14f, #edc948, #b07aa1, #ff9da7, #9c755f, #bab0ac";

  const prompt = `You have ${allItems.length} finance/investing items.

Here are all items:

${summaries}

Analyze relationships and return JSON (no markdown fences):
{
  "edges": [
    {"source": "id", "target": "id", "type": "builds_on|contrasts_with|applies_concept_from|prerequisite_for", "strength": 0.1-1.0, "reason": "brief explanation"}
  ],
  "clusters": [
    {"name": "Cluster Name", "color": "#hexcolor", "thread_ids": ["id1", "id2"], "description": "what this cluster covers"}
  ],
  "learning_paths": [
    {"name": "Path Name", "description": "who this path is for", "thread_ids": ["id1", "id2"]}
  ]
}

Guidelines:
- Create 6-10 topic clusters (${clusterGuidance})
- Create 3-5 learning paths
- Include 30-60 edges capturing the most meaningful connections
- Every item should belong to exactly one cluster
- Use these colors: ${colors}`;

  const result = await model.generateContent(prompt);
  const text = stripFences(result.response.text());
  return JSON.parse(text);
}

// ============================================================
// Main handler
// ============================================================
export default async (req) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "POST only" }), { status: 405, headers: { "Content-Type": "application/json" } });
  }

  const url = new URL(req.url);
  const dataset = url.searchParams.get("dataset") || "10kdiver";
  if (!VALID_DATASETS[dataset]) {
    return new Response(JSON.stringify({ error: "Invalid dataset" }), { status: 400, headers: { "Content-Type": "application/json" } });
  }

  const blobKey = VALID_DATASETS[dataset];

  try {
    // 1. Load existing data
    const existing = await loadExistingData(blobKey);
    const existingThreads = existing.threads || [];

    // 2. Scrape for new items
    let newRawItems;
    if (dataset === "10kdiver") {
      const existingIds = new Set(existingThreads.map((t) => t.id));
      newRawItems = await scrape10kDiverNew(existingIds);
    } else {
      const existingUrls = new Set(existingThreads.map((t) => t.url));
      newRawItems = await scrapeMoontowerNew(existingUrls);
    }

    if (newRawItems.length === 0) {
      return new Response(JSON.stringify({ success: true, newCount: 0, message: "No new articles found." }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // 3. Assign IDs to new Moontower articles
    if (dataset === "moontower") {
      const maxId = Math.max(0, ...existingThreads.map((t) => parseInt(t.id.replace("mt_", "")) || 0));
      newRawItems.forEach((item, i) => {
        item.id = `mt_${maxId + i + 1}`;
      });
    }

    // 4. Enrich new items via Gemini
    const genAI = getGenAI();
    const model = genAI.getGenerativeModel({ model: MODEL_NAME });

    const enrichedNew = [];
    for (const item of newRawItems) {
      try {
        const meta = await enrichSingleItem(model, item, dataset);
        enrichedNew.push({
          ...item,
          summary: meta.summary || "",
          concepts: meta.concepts || [],
          difficulty: meta.difficulty || "intermediate",
        });
      } catch (e) {
        console.log(`Enrichment error for ${item.title}: ${e.message}`);
        enrichedNew.push({
          ...item,
          summary: "",
          concepts: [],
          difficulty: "intermediate",
        });
      }
    }

    // 5. Merge with existing threads
    const allThreads = [...existingThreads, ...enrichedNew];

    // 6. Re-generate connections (Pass 2) on full dataset
    console.log("Regenerating connections for full dataset...");
    let connections;
    try {
      connections = await generateConnections(model, allThreads, dataset);
    } catch (e) {
      console.log(`Connections error: ${e.message}`);
      // Keep existing connections
      connections = {
        edges: existing.edges || [],
        clusters: existing.clusters || [],
        learning_paths: existing.learning_paths || [],
      };
    }

    // 7. Rebuild concepts list
    const conceptMap = {};
    for (const t of allThreads) {
      for (const c of t.concepts || []) {
        const key = c.toLowerCase().trim();
        if (!conceptMap[key]) conceptMap[key] = { name: c, thread_ids: new Set() };
        conceptMap[key].thread_ids.add(t.id);
      }
    }
    const conceptsList = Object.values(conceptMap)
      .filter((c) => c.thread_ids.size >= 2)
      .map((c) => ({ name: c.name, thread_ids: [...c.thread_ids], description: "" }))
      .sort((a, b) => b.thread_ids.length - a.thread_ids.length);

    // 8. Assign clusters
    const clusterLookup = {};
    for (const cluster of connections.clusters || []) {
      for (const tid of cluster.thread_ids || []) {
        clusterLookup[tid] = cluster.name;
      }
    }
    for (const t of allThreads) {
      t.cluster = clusterLookup[t.id] || t.cluster || "General";
    }

    // 9. Build final output
    const output = {
      threads: allThreads,
      concepts: conceptsList,
      edges: connections.edges || [],
      learning_paths: connections.learning_paths || [],
      clusters: connections.clusters || [],
    };

    // 10. Save to Blobs
    const store = getStore("mindmap-data");
    await store.setJSON(blobKey, output);

    return new Response(
      JSON.stringify({
        success: true,
        newCount: enrichedNew.length,
        totalCount: allThreads.length,
        message: `Added ${enrichedNew.length} new articles. Total: ${allThreads.length}.`,
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  } catch (e) {
    console.error("Refresh error:", e);
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
};
