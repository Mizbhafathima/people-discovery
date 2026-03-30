"""
People / Employee Extractor
----------------------------
Uses crawl4ai to crawl any team/leadership page and Google GenAI (Gemma-3-27b)
to extract structured people data.

Usage:
    python people_extractor.py <url>
    python people_extractor.py <url> --output results.json

.env file must contain:
    GOOGLE_API_KEY=your_key_here
"""

import asyncio
import json
import os
import sys
import argparse
import re
from typing import Optional, List

from dotenv import load_dotenv
load_dotenv()

from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────
# 1. Data schema
# ─────────────────────────────────────────────────────────────

class PersonContact(BaseModel):
    name: str = Field(description="Full name of the person")
    role: str = Field(description="Job title or role at the organisation")
    phone: Optional[str] = Field(None, description="Phone number if listed")
    email: Optional[str] = Field(None, description="Email address if listed")
    linkedin_url: Optional[str] = Field(None, description="Full LinkedIn profile URL")
    twitter_url: Optional[str] = Field(None, description="Full Twitter/X profile URL")
    instagram_url: Optional[str] = Field(None, description="Full Instagram profile URL")
    facebook_url: Optional[str] = Field(None, description="Full Facebook profile URL")
    bio: Optional[str] = Field(None, description="Short biography or description")
    profile_url: Optional[str] = Field(None, description="URL to their individual profile page")
    other_links: Optional[List[str]] = Field(None, description="Any other relevant URLs")


# ─────────────────────────────────────────────────────────────
# 2. Crawl the page -> clean markdown
# ─────────────────────────────────────────────────────────────

async def crawl_page(url: str) -> tuple[str, str]:
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1440,
        viewport_height=900,
    )

    md_generator = DefaultMarkdownGenerator(
        content_filter=PruningContentFilter(
            threshold=0.4,
            threshold_type="dynamic",
            min_word_threshold=3,
        ),
        options={"ignore_links": False},
    )

    run_config = CrawlerRunConfig(
        markdown_generator=md_generator,
        remove_overlay_elements=True,
        process_iframes=True,
        word_count_threshold=2,
        exclude_external_links=False,
        cache_mode=CacheMode.BYPASS,
        page_timeout=60000,
        js_code="""
            (async () => {
                for (let i = 0; i < 8; i++) {
                    window.scrollBy(0, window.innerHeight);
                    await new Promise(r => setTimeout(r, 700));
                }
                window.scrollTo(0, 0);
            })();
        """,
        wait_for="css:body",
        delay_before_return_html=3.0,
    )

    print(f"  Fetching page (headless browser)...")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)

    if not result.success:
        raise RuntimeError(
            f"Crawl failed (HTTP {result.status_code}): {result.error_message}"
        )

    content = result.markdown.fit_markdown or result.markdown.raw_markdown
    raw_html = result.html or ""
    if not content or len(content.strip()) < 100:
        raise RuntimeError("Crawl returned almost no content. The page may block bots.")

    print(f"  Page fetched - {len(content):,} chars of clean markdown")
    return content, raw_html


# ─────────────────────────────────────────────────────────────
# 3. Extract people from markdown using Gemma via Google GenAI
# ─────────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are a data extraction assistant. You will be given the markdown content of a company website page that lists employees, leadership team members, or staff profiles.

Your job is to extract EVERY person mentioned on the page.

For each person return a JSON object with these fields:
  - name          (string, required) : Full name
  - role          (string, required) : Job title or position
  - phone         (string or null)   : Phone number
  - email         (string or null)   : Email address
  - linkedin_url  (string or null)   : Full LinkedIn URL
  - twitter_url   (string or null)   : Full Twitter/X URL
  - instagram_url (string or null)   : Full Instagram URL
  - facebook_url  (string or null)   : Full Facebook URL
  - bio           (string or null)   : Short biography or description
  - profile_url   (string or null)   : Link to their profile page on the same website
  - other_links   (list of strings or null) : Any other URLs tied to this person

Rules:
  - Extract EVERY person, do not skip anyone.
    - The `name` field must be the COMPLETE person name as written on the page.
    - NEVER return first-name-only values when a full name exists (e.g. use "Amy Lawson", not "Amy").
    - If only a first name is visible for a person and no surname appears anywhere for that person, keep the first name.
  - For missing fields use null, never guess or invent data.
  - Return ONLY a raw JSON object in this exact shape (no markdown fences, no explanation):
    {"people": [ {...}, {...} ]}

Page content:
"""


def chunk_text(text: str, max_chars: int = 15000, overlap_chars: int = 500) -> List[str]:
    if len(text) <= max_chars:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        if end >= len(text):
            chunks.append(text[start:])
            break
        split_at = text.rfind("\n", start + max_chars // 2, end)
        if split_at == -1:
            split_at = end
        chunks.append(text[start:split_at])
        start = split_at - overlap_chars
    return chunks


def _is_single_word_name(name: str) -> bool:
    return bool(name) and len(name.split()) == 1


def _extract_full_name_candidates(text: str) -> List[str]:
    if not text:
        return []

    pattern = re.compile(
        r"\b([A-Z][A-Za-z'\-]+[ \t]+[A-Z][A-Za-z'\-]+(?:[ \t]+[A-Z][A-Za-z'\-]+)?)\b"
    )

    skip_tokens = {
        "Chief", "General", "Interim", "Executive", "Officer", "President",
        "Vice", "Global", "Senior", "Head", "Company", "Board", "Team",
        "Leadership", "About", "Sage", "Group", "Corporate", "Affairs",
        "Brand", "Technology", "Financial", "Product", "Commercial", "People"
    }

    out = []
    seen = set()
    for m in pattern.finditer(text):
        candidate = " ".join(m.group(1).split())
        words = candidate.split()
        if not (2 <= len(words) <= 3):
            continue

        # Keep only clean person-name tokens, not role/company fragments.
        if any(w in skip_tokens for w in words):
            continue

        # If a trailing third word looks like a role token, trim to first+last.
        if len(words) == 3 and words[2] in skip_tokens:
            candidate = f"{words[0]} {words[1]}"
            words = candidate.split()

        key = candidate.lower()
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    return out


def _looks_suspicious_name(name: str) -> bool:
    if not name:
        return False
    words = name.split()
    if len(words) < 2:
        return False

    bad_words = {
        "is", "was", "has", "had", "led", "joins", "joined", "with",
        "from", "for", "and", "our", "the"
    }
    return any(w.lower() in bad_words for w in words[1:])


def _resolve_full_name_from_markdown(first_name: str, markdown: str) -> Optional[str]:
    """Finds a likely full name for a first-name-only match using page text."""
    if not first_name or not markdown:
        return None

    escaped = re.escape(first_name)
    # Match names like "Amy Lawson", including simple hyphenated/apostrophe forms.
    pattern = re.compile(
        rf"\b((?i:{escaped})[ \t]+[A-Z][A-Za-z'\-]+(?:[ \t]+[A-Z][A-Za-z'\-]+)?)\b",
    )
    matches = [m.group(1).strip() for m in pattern.finditer(markdown)]
    if not matches:
        # Fallback: match URL slugs such as /amy-lawson/ or amy_lawson
        slug = re.search(
            rf"\b{escaped}[-_/]+([a-z][a-z'\-]+)\b",
            markdown,
            flags=re.IGNORECASE,
        )
        if slug:
            last = slug.group(1).replace("-", " ").replace("_", " ").strip()
            last = " ".join(part.capitalize() for part in last.split() if part)
            if last:
                return f"{first_name.title()} {last}"
        return None

    # Keep order but de-duplicate case-insensitively.
    seen = set()
    unique = []
    for m in matches:
        k = m.lower()
        if k not in seen:
            seen.add(k)
            unique.append(m)

    if len(unique) == 1:
        value = unique[0]
        value = " ".join(value.split())
        return value.title() if value.isupper() else value

    # If there are multiple candidates, choose the shortest plausible full name.
    # This avoids picking long heading fragments.
    unique.sort(key=lambda x: (len(x.split()), len(x)))
    value = unique[0]
    value = " ".join(value.split())
    return value.title() if value.isupper() else value


def _build_name_search_text(markdown: str, raw_html: str) -> str:
    parts = [markdown or ""]
    if raw_html:
        try:
            soup = BeautifulSoup(raw_html, "lxml")
            parts.append(soup.get_text(" ", strip=True))
            attr_chunks = []
            for tag in soup.find_all(True):
                for attr in ["alt", "title", "aria-label", "data-name", "href"]:
                    value = tag.get(attr)
                    if isinstance(value, str) and value.strip():
                        attr_chunks.append(value.strip())
            if attr_chunks:
                parts.append("\n".join(attr_chunks))
        except Exception:
            parts.append(raw_html)
    return "\n".join(parts)


def _expand_partial_names(people: List[dict], markdown: str, raw_html: str) -> List[dict]:
    search_text = _build_name_search_text(markdown, raw_html)
    full_name_candidates = _extract_full_name_candidates(search_text)

    by_first = {}
    for candidate in full_name_candidates:
        first = candidate.split()[0].lower()
        by_first.setdefault(first, []).append(candidate)

    expanded = []
    for person in people:
        item = dict(person)
        name = (item.get("name") or "").strip()

        if _looks_suspicious_name(name):
            first = name.split()[0].lower()
            options = by_first.get(first, [])
            if options:
                options = sorted(options, key=lambda x: (len(x.split()), len(x)))
                item["name"] = options[0]
                expanded.append(item)
                continue

        if _is_single_word_name(name):
            resolved = _resolve_full_name_from_markdown(name, search_text)
            if resolved:
                item["name"] = resolved
        expanded.append(item)
    return expanded


def extract_people_with_llm(markdown: str, raw_html: str, api_key: str) -> List[dict]:
    client = genai.Client(api_key=api_key)
    chunks = chunk_text(markdown, max_chars=15000, overlap_chars=500)
    print(f"  Sending to Gemma ({len(chunks)} chunk(s))...")

    all_people = []
    seen_names = set()

    for i, chunk in enumerate(chunks, 1):
        if len(chunks) > 1:
            print(f"    Processing chunk {i}/{len(chunks)}...")

        try:
            response = client.models.generate_content(
                model="gemma-3-27b-it",
                contents=EXTRACTION_PROMPT + chunk,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=8192,
                ),
            )
        except Exception as e:
            print(f"  WARNING: LLM call failed on chunk {i}: {e}")
            continue

        raw_text = response.text.strip()

        # Strip markdown code fences if model added them
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            start = raw_text.find("{")
            end = raw_text.rfind("}") + 1
            if start != -1 and end > start:
                try:
                    parsed = json.loads(raw_text[start:end])
                except json.JSONDecodeError as e2:
                    print(f"  WARNING: Could not parse JSON from chunk {i}: {e2}")
                    print(f"  Raw response: {raw_text[:300]}")
                    continue
            else:
                print(f"  WARNING: No JSON found in chunk {i} response")
                continue

        people_in_chunk = parsed.get("people", [])
        for person in people_in_chunk:
            name = (person.get("name") or "").strip().lower()
            if name and name not in seen_names:
                seen_names.add(name)
                all_people.append(person)

    return _expand_partial_names(all_people, markdown, raw_html)


# ─────────────────────────────────────────────────────────────
# 4. Display & save
# ─────────────────────────────────────────────────────────────

def print_person(person: dict, index: int) -> None:
    print(f"\n{'=' * 60}")
    print(f"  #{index}  {person.get('name', 'N/A')}")
    print(f"{'-' * 60}")
    fields = [
        ("Role",        "role"),
        ("Phone",       "phone"),
        ("Email",       "email"),
        ("LinkedIn",    "linkedin_url"),
        ("Twitter/X",   "twitter_url"),
        ("Instagram",   "instagram_url"),
        ("Facebook",    "facebook_url"),
        ("Profile URL", "profile_url"),
        ("Bio",         "bio"),
    ]
    for label, key in fields:
        value = person.get(key)
        if value:
            if key == "bio" and len(value) > 200:
                value = value[:197] + "..."
            print(f"  {label:<13}: {value}")
    others = person.get("other_links") or []
    if others:
        print(f"  {'Other links':<13}: {', '.join(others)}")


def save_to_json(people: List[dict], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(people, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(people)} people to: {output_path}")


# ─────────────────────────────────────────────────────────────
# 5. Main
# ─────────────────────────────────────────────────────────────

async def run(url: str, api_key: str, output_path: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  URL : {url}")
    print(f"{'=' * 60}\n")

    try:
        markdown, raw_html = await crawl_page(url)
    except RuntimeError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    people = extract_people_with_llm(markdown, raw_html, api_key)

    if not people:
        print("\nNo people extracted.")
        print("  Possible reasons:")
        print("  - Page requires login or blocks bots")
        print("  - Content is deeply nested in JS components")
        print("  - Try increasing delay_before_return_html or scroll iterations")
        return

    print(f"\n\nFound {len(people)} people\n")
    for i, person in enumerate(people, start=1):
        print_person(person, i)
    print(f"\n{'=' * 60}\n")

    save_to_json(people, output_path)


def main():
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY not found.")
        print("Add it to your .env file:  GOOGLE_API_KEY=your_key_here")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Extract employee/people info from any webpage."
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="https://www.sage.com/en-gb/company/about-sage/leadership/sage-leadership-team/",
        help="URL to extract people from",
    )
    parser.add_argument(
        "--output",
        default="people_output.json",
        help="Output JSON file path (default: people_output.json)",
    )
    args = parser.parse_args()
    asyncio.run(run(args.url, api_key, args.output))


if __name__ == "__main__":
    main()