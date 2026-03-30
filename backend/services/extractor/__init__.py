import asyncio
import logging
import re
from typing import List
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from backend.services.extractor.regex_extractor import RegexExtractor
from backend.services.extractor.llm_extractor import LLMExtractor
from backend.services.extractor.jsonld_extractor import JSONLDExtractor
from backend.services.extractor.gliner_extractor import GLiNERExtractor
from backend.core.utils import calculate_confidence, estimate_tokens


class ExtractionPipeline:
    def __init__(self):
        self.regex = RegexExtractor()
        self.llm = LLMExtractor()
        self.jsonld = JSONLDExtractor()
        self.gliner = GLiNERExtractor()
        self.logger = logging.getLogger(__name__)

    def _name_tokens(self, name: str) -> List[str]:
        cleaned = re.sub(r"[^a-zA-Z\s\-']", " ", (name or "").lower())
        parts = [p for p in re.split(r"[\s\-]+", cleaned) if p]
        stop = {"mr", "mrs", "ms", "dr", "prof"}
        return [p for p in parts if p not in stop]

    def _extract_linkedin_urls_from_html(self, raw_html: str) -> List[str]:
        if not raw_html:
            return []
        urls = []
        seen = set()
        try:
            soup = BeautifulSoup(raw_html, "lxml")
            for link in soup.find_all("a", href=True):
                href = (link.get("href") or "").strip()
                if not href:
                    continue
                href_l = href.lower()
                if "linkedin.com/" not in href_l:
                    continue
                # Keep only personal profiles.
                if "/in/" not in href_l:
                    continue
                if href not in seen:
                    seen.add(href)
                    urls.append(href)
        except Exception:
            return urls
        return urls

    def _match_linkedin_by_slug(self, people: List[dict], raw_html: str) -> None:
        linkedin_urls = self._extract_linkedin_urls_from_html(raw_html)
        if not linkedin_urls:
            return

        slug_index = []
        for url in linkedin_urls:
            try:
                parsed = urlparse(url)
                path = (parsed.path or "").lower()
                if "/in/" not in path:
                    continue
                slug = path.split("/in/", 1)[1].strip("/")
                if not slug:
                    continue
                slug_tokens = [t for t in re.split(r"[-_]+", slug) if t]
                slug_index.append((url, set(slug_tokens)))
            except Exception:
                continue

        for person in people:
            if person.get("linkedin_url"):
                continue
            name = person.get("name") or ""
            tokens = self._name_tokens(name)
            if len(tokens) < 2:
                continue

            first = tokens[0]
            last = tokens[-1]
            candidates = []
            for url, slug_tokens in slug_index:
                if first in slug_tokens and last in slug_tokens:
                    candidates.append(url)

            if len(candidates) == 1:
                person["linkedin_url"] = candidates[0]

    async def run(self, content: str, source_url: str, raw_html: str = "") -> List[dict]:
        """5-layer pipeline: JSON-LD -> Regex -> GLiNER -> LLM -> Merge"""

        if not content or len(content.strip()) < 50:
            self.logger.warning(f"Empty content for {source_url}")
            return []

        self.logger.info(f"Pipeline starting | {source_url} | {len(content)} chars")

        # LAYER 1: JSON-LD
        jsonld_people = []
        if raw_html:
            jsonld_people = self.jsonld.extract(raw_html, source_url)
            if jsonld_people:
                self.logger.info(f"JSON-LD found {len(jsonld_people)} people")

        # LAYER 2: Regex
        regex_results = self.regex.extract_all(content, source_url)
        self.logger.info(
            f"Regex: {len(regex_results['emails'])} emails, "
            f"{len(regex_results['phones'])} phones"
        )

        # LAYER 3: GLiNER
        ner_results = {"persons": [], "job_titles": [], "emails": [], "phones": []}
        name_hint = ""
        if self.gliner.is_available():
            ner_results = self.gliner.extract(content)
            name_hint = self.gliner.build_hint_for_llm(ner_results)
            self.logger.info(
                f"GLiNER found {len(ner_results['persons'])} persons"
            )

        # LAYER 4: LLM
        jsonld_names = {p.get("name", "").lower() for p in jsonld_people if p.get("name")}
        ner_names = {n.lower() for n in ner_results.get("persons", [])}
        missing_names = ner_names - jsonld_names

        llm_people = []
        if jsonld_people and not missing_names:
            self.logger.info("JSON-LD captured all people. Skipping LLM.")
        else:
            self.logger.info(
                f"Calling LLM {'with' if name_hint else 'without'} GLiNER hints"
            )
            CHUNK_SIZE = 24000
            chunks = []
            remaining = content
            while remaining:
                chunks.append(remaining[:CHUNK_SIZE])
                remaining = remaining[CHUNK_SIZE:]

            if len(chunks) > 1:
                self.logger.info(f"Split into {len(chunks)} chunks")

            for i, chunk in enumerate(chunks):
                if len(chunks) > 1:
                    self.logger.info(f"Chunk {i + 1}/{len(chunks)}")
                people = await self.llm.extract(chunk, name_hint=name_hint)
                llm_people.extend(people)
                if len(chunks) > 1:
                    await asyncio.sleep(2)

            self.logger.info(f"LLM found {len(llm_people)} people")

        # LAYER 5: Collect all people from all sources
        # Deduplication is handled exclusively by EnricherService
        all_people = []

        for p in jsonld_people:
            p["source_url"] = source_url
            all_people.append(p)

        for p in llm_people:
            p["source_url"] = source_url
            email = p.get("email", "")
            if email and ("example.com" in email.lower() or "@" not in email):
                p["email"] = None
            all_people.append(p)

        # Do not create person records by zipping independent name/title lists.
        # GLiNER is used only as a hint for LLM extraction.

        # Enrich missing personal LinkedIn URLs from raw HTML profile links.
        if raw_html:
            self._match_linkedin_by_slug(all_people, raw_html)

        regex_emails_added = {
            p.get("email", "").lower()
            for p in all_people if p.get("email")
        }
        for email in regex_results.get("emails", []):
            if email.lower() not in regex_emails_added:
                all_people.append({
                    "name": None, "job_title": None,
                    "email": email, "phone": None,
                    "linkedin_url": None,
                    "instagram_url": None,
                    "twitter_url": None,
                    "source_url": source_url
                })

        regex_phones_added = {
            (p.get("phone") or "").strip()
            for p in all_people if p.get("phone")
        }
        for phone in regex_results.get("phones", []):
            if phone.strip() and phone.strip() not in regex_phones_added:
                all_people.append({
                    "name": None, "job_title": None,
                    "email": None, "phone": phone,
                    "linkedin_url": None,
                    "instagram_url": None,
                    "twitter_url": None,
                    "source_url": source_url
                })

        self.logger.info(
            f"Pipeline collected {len(all_people)} raw people "
            f"(dedup handled by enricher)"
        )
        return all_people
