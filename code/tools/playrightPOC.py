#!/usr/bin/env python3
"""
Path C PoC: Playwright fetch → Extruct scrape (json-ld, microdata) →
fallback heading-to-graph → wrangles.jsonl (one page per line).
"""

import json, re, sys, tiktoken
from pathlib import Path
from typing import List, Dict
from bs4 import BeautifulSoup
import extruct
from playwright.sync_api import sync_playwright
from tqdm import tqdm

ENC = tiktoken.encoding_for_model("text-embedding-3-small")
MAX_TOKENS = 512              # hard cap per @graph node

# ---------- 1 · Fetch ---------------------------------------------------------

def fetch_with_playwright(url: str) -> str:
    """Return fully-rendered HTML (JS executed) for the given URL."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        html = page.content()
        browser.close()
    return html

# ---------- 2 · Extract -------------------------------------------------------

def slugify(txt: str) -> str:
    return re.sub(r"[^\w\-]+", "-", txt.strip().lower()).strip("-")

def headings_to_graph(soup: BeautifulSoup) -> List[Dict]:
    """Fallback: build @graph nodes from H1–H4 headings."""
    graph = []
    for hx in soup.select("h1, h2, h3, h4"):
        text = hx.get_text(strip=True)
        if not text:
            continue
        tokens = ENC.encode(text)
        if len(tokens) > MAX_TOKENS:
            text = ENC.decode(tokens[:MAX_TOKENS])
        node_id = f"#{hx.get('id') or slugify(text)[:60]}"
        graph.append({
            "@type": "TechArticle",
            "@id": node_id,
            "name": text
        })
    return graph

def page_to_jsonl(url: str) -> str | None:
    """Return one `url<TAB>json` line ready for db_load.py, or None if blank."""
    html = fetch_with_playwright(url)

    # 2a. Try existing structured data first.
    data = extruct.extract(html, syntaxes=["json-ld", "microdata"])
    jsonld = data.get("json-ld") or []
    if jsonld:                    # assume first graph represents the page
        graph_obj = jsonld[0]
        # If the page already set @context/@graph, leave as-is.
        output = graph_obj if "@graph" in graph_obj else {"@graph": [graph_obj]}
        output.setdefault("@context", "https://schema.org")
        return f"{url}\t{json.dumps(output, ensure_ascii=False)}"

    # 2b. No JSON-LD found → build one from headings.
    soup = BeautifulSoup(html, "html.parser")
    graph = headings_to_graph(soup)
    if not graph:                 # nothing to store
        return None
    output = {"@context": "https://schema.org", "@graph": graph}
    return f"{url}\t{json.dumps(output, ensure_ascii=False)}"

# ---------- 3 · Driver --------------------------------------------------------

def main(urls: List[str], outfile: Path):
    with outfile.open("w", encoding="utf-8") as f:
        for url in tqdm(urls, desc="Scraping"):
            line = page_to_jsonl(url)
            if line:
                f.write(line + "\n")
    print(f"✓ Wrote {outfile} — load with: python -m tools.db_load {outfile} WrangleDocs")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("Usage: crawler.py output.jsonl URL [URL …]")
    out = Path(sys.argv[1])
    urls = sys.argv[2:]
    main(urls, out)
