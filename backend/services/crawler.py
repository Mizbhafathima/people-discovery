from crawl4ai import AsyncWebCrawler
from backend.config import settings
from backend.services.extractor import ExtractionPipeline
from backend.services.enricher import EnricherService
from backend.database import crud
from backend.core.utils import (
    score_url_for_people,
    looks_like_people_page,
    extract_people_sections,
)
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import httpx
import asyncio
import logging
import re
from typing import List, Tuple


class CrawlerService:
    def __init__(self):
        """Initialize crawler pipeline, enricher, and logger."""
        self.pipeline = ExtractionPipeline()
        self.enricher = EnricherService()
        self.logger = logging.getLogger(__name__)

    def detect_input_type(self, raw_input: str) -> Tuple[str, str]:
        """Returns (input_type, clean_input) where input_type is 'url' or 'domain'."""
        raw = raw_input.strip().rstrip("/")
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        
        # If there is a substantial path component, it's a specific endpoint URL
        path = parsed.path.strip("/")
        if path and len(path) > 0:
            full_url = raw if raw.startswith("http") else f"https://{raw}"
            return ("url", full_url)
            
        # Otherwise, it's a root domain
        clean = parsed.netloc or parsed.path
        clean = clean.replace("www.", "")
        return ("domain", clean.lower())

    def extract_domain(self, url: str) -> str:
        """Extracts clean domain from any URL."""
        parsed = urlparse(url)
        netloc = parsed.netloc or parsed.path
        return netloc.replace("www.", "").split(":")[0]

    def _render_js_for_people_sections(self) -> str:
        """Scrolls and clicks likely tab/filter/expand controls to load dynamic people content."""
        return """
            (async () => {
                const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                const clickKeywords = [
                    'team', 'leadership', 'management', 'executive', 'board',
                    'people', 'staff', 'committee', 'director', 'all', 'more',
                    'view', 'load', 'show'
                ];

                for (let i = 0; i < 7; i++) {
                    window.scrollBy(0, window.innerHeight);
                    await sleep(450);
                }
                window.scrollTo(0, 0);
                await sleep(300);

                const clickable = Array.from(document.querySelectorAll(
                    'button, [role="tab"], [role="button"], .tab, .tabs button, .accordion button, .filter button, a[aria-controls]'
                ));

                for (const el of clickable) {
                    const txt = ((el.innerText || el.textContent || '') + ' ' + (el.getAttribute('aria-label') || '')).toLowerCase();
                    if (!txt) continue;
                    if (!clickKeywords.some(k => txt.includes(k))) continue;
                    try {
                        el.click();
                        await sleep(250);
                    } catch (_) {}
                }

                for (let i = 0; i < 5; i++) {
                    window.scrollBy(0, window.innerHeight);
                    await sleep(400);
                }
                window.scrollTo(0, 0);
            })();
        """

    def _looks_human_name(self, value: str) -> bool:
        text = " ".join((value or "").split()).strip()
        if not text:
            return False
        if len(text) < 4 or len(text) > 60:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 5:
            return False
        if re.search(r"[^A-Za-z\s\-\.'’]", text):
            return False
        noise = {
            "featured", "read more", "view all", "learn more", "our team",
            "leadership", "management", "people", "about us", "board"
        }
        if text.lower() in noise:
            return False
        return True

    def _split_combined_name_title(self, text: str) -> Tuple[str, str]:
        """Splits strings like 'Jane Doe Chief Executive Officer' into name/title."""
        value = " ".join((text or "").split()).strip()
        if not value:
            return "", ""

        match = re.match(
            r"^([A-Z][A-Za-z'’.-]+(?:\s+[A-Z][A-Za-z'’.-]+){1,3})\s+(.+)$",
            value,
        )
        if not match:
            return "", ""

        candidate_name = " ".join(match.group(1).split()).strip()
        candidate_title = " ".join(match.group(2).split()).strip()
        if not self._looks_human_name(candidate_name):
            return "", ""

        role_tokens = [
            "chief", "officer", "director", "president", "vice president",
            "head", "manager", "lead", "partner", "counsel", "chair",
            "ceo", "cto", "cfo", "coo", "founder",
        ]
        if not any(tok in candidate_title.lower() for tok in role_tokens):
            return "", ""

        return candidate_name, candidate_title

    def _extract_name_and_title_from_container(self, container) -> Tuple[str, str]:
        name = ""
        title = ""

        name_selectors = [
            "[itemprop='name']",
            "h1", "h2", "h3", "h4", "strong", "b",
            "[class*='name']", "[class*='person']", "[class*='member']",
        ]
        for sel in name_selectors:
            try:
                el = container.select_one(sel)
            except Exception:
                el = None
            if not el:
                continue
            candidate = " ".join(el.get_text(" ", strip=True).split())
            if self._looks_human_name(candidate):
                name = candidate
                break
            split_name, split_title = self._split_combined_name_title(candidate)
            if split_name:
                return split_name, split_title

        if not name:
            return "", ""

        title_selectors = [
            "[itemprop='jobTitle']",
            "[class*='title']", "[class*='role']", "[class*='position']", "[class*='job']",
            "p", "span", "div"
        ]
        bad_title_tokens = {
            "read more", "view profile", "linkedin", "follow", "bio", "featured"
        }

        for sel in title_selectors:
            try:
                elems = container.select(sel)
            except Exception:
                elems = []
            for el in elems:
                txt = " ".join(el.get_text(" ", strip=True).split())
                if not txt or txt == name:
                    continue
                low = txt.lower()
                if any(tok in low for tok in bad_title_tokens):
                    continue

                # Recover cases where one node contains both name and role.
                split_name, split_title = self._split_combined_name_title(txt)
                if split_name and split_title:
                    return split_name, split_title

                if self._looks_human_name(txt):
                    continue
                if len(txt) > 120:
                    continue
                title = txt
                return name, title

        return name, title

    async def crawl_page(self, crawler: AsyncWebCrawler, url: str) -> dict:
        """
        Crawls a single page and extracts only main content using BeautifulSoup.
        Removes nav, header, footer, scripts to reduce 77K chars to ~6K chars.
        Falls back to markdown if HTML extraction gives too little content.
        """
        try:
            result = await crawler.arun(
                url=url,
                bypass_cache=True,
                word_count_threshold=10,
                wait_for="css:body",
                page_timeout=45000,
                delay_before_return_html=3.0,
                remove_overlay_elements=True,
                js_code=self._render_js_for_people_sections(),
            )

            raw_html = result.html or ""
            content = ""

            if raw_html:
                soup = BeautifulSoup(raw_html, "lxml")

                for tag in soup.find_all([
                    "nav", "header", "script",
                    "style", "noscript", "iframe", "form"
                ]):
                    tag.decompose()

                for selector in [
                    "[class*='nav']", "[class*='menu']",
                    "[class*='header']",
                    "[class*='cookie']", "[class*='sidebar']",
                    "[id*='nav']", "[id*='menu']",
                    "[id*='header']"
                ]:
                    try:
                        for el in soup.select(selector):
                            el.decompose()
                    except Exception:
                        continue

                main = (
                    soup.find("main") or
                    soup.find(id="main-content") or
                    soup.find(id="main") or
                    soup.find(attrs={"role": "main"}) or
                    soup.find("article") or
                    soup.find("body")
                )

                if main:
                    text = main.get_text(separator="\n", strip=True)
                    seen = set()
                    clean_lines = []
                    for line in text.split("\n"):
                        s = line.strip()
                        if s and len(s) > 1 and s not in seen:
                            seen.add(s)
                            clean_lines.append(s)
                    content = "\n".join(clean_lines)

            if len(content.strip()) < 500 and result.markdown:
                content = result.markdown
                self.logger.warning(
                    f"HTML extraction too short, using markdown fallback: {url}"
                )

            # Reduce navigation/news/footer noise to lower hallucinated people records.
            content = extract_people_sections(content)

            if len(content) > 30000:
                self.logger.warning(
                    f"Large page ({len(content)} chars), "
                    f"truncating to 30000 chars: {url}"
                )
                content = content[:30000]

            self.logger.info(
                f"Crawled {url} | {len(content)} chars | "
                f"success={len(content) > 100}"
            )

            return {
                "url": url,
                "content": content,
                "html": raw_html,
                "success": len(content) > 100,
            }

        except Exception as e:
            self.logger.warning(f"Failed to crawl {url}: {e}")
            return {"url": url, "content": "", "html": "", "success": False}

    async def fetch_sitemap_links(self, domain: str) -> List[str]:
        """Tries sitemap.xml and robots.txt to find internal links."""
        sitemap_urls = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/robots.txt",
        ]
        links = []
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            for sitemap_url in sitemap_urls:
                try:
                    response = await client.get(sitemap_url)
                    if response.status_code == 200:
                        text = response.text
                        if "sitemap" in sitemap_url:
                            found = re.findall(r"<loc>(.*?)</loc>", text)
                            links.extend(found)
                        else:
                            found = re.findall(r"Sitemap:\s*(https?://\S+)", text)
                            links.extend(found)
                        if links:
                            self.logger.info(f"Found {len(links)} links from {sitemap_url}")
                            break
                except Exception:
                    continue
        return [l for l in links if domain in l]

    async def fetch_homepage_links(self, domain: str, crawler: AsyncWebCrawler) -> List[str]:
        """Crawls homepage and extracts all internal links."""
        homepage_url = f"https://{domain}"
        try:
            result = await crawler.arun(
                url=homepage_url,
                bypass_cache=True,
                word_count_threshold=5,
                wait_for="networkidle",
                page_timeout=20000,
                delay_before_return_html=2.0,
            )
            if not result.success or not result.html:
                return []
            soup = BeautifulSoup(result.html, "lxml")
            links = []
            for tag in soup.find_all("a", href=True):
                href = tag["href"].strip()
                full_url = urljoin(homepage_url, href)
                parsed = urlparse(full_url)
                if domain in parsed.netloc and parsed.scheme in ("http", "https"):
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
                    if clean not in links and clean != homepage_url:
                        links.append(clean)
            self.logger.info(f"Found {len(links)} internal links from {domain} homepage")
            return links
        except Exception as e:
            self.logger.warning(f"Homepage link extraction failed: {e}")
            return []

    async def find_people_section_url(self, domain: str, client: httpx.AsyncClient) -> List[str]:
        paths = [
            "/team",
            "/leadership",
            "/people",
            "/about/team",
            "/our-team",
            "/staff",
            "/board",
            "/directors",
            "/executives",
            "/management",
            "/attorneys",
            "/faculty",
            "/professionals",
            "/partners",
            "/advisors",
            "/members",
            "/about-us",
            "/about",
            "/contact",
            "/contact-us",
            "/contacts",
            "/get-in-touch",
            "/reach-us",
            "/office",
            "/locations",
        ]
        found = []

        for path in paths:
            url = f"https://{domain}{path}"
            try:
                response = await client.head(url, timeout=5.0)
                if response.status_code == 200:
                    found.append(url)
                    continue
                if response.status_code in (403, 405):
                    response = await client.get(url, timeout=5.0)
                    if response.status_code == 200:
                        found.append(url)
            except Exception:
                continue

        self.logger.info(
            f"People-section detector found {len(found)} valid URLs for {domain}"
        )
        return found[:3]

    async def scrape_people_cards(self, url: str, crawler: AsyncWebCrawler) -> List[dict]:
        page = await self.crawl_page(crawler, url)
        raw_html = page.get("html") or ""
        if not raw_html:
            self.logger.info(f"People card scraper found 0 cards for {url}")
            return []

        soup = BeautifulSoup(raw_html, "lxml")

        semantic_selectors = [
            "[itemtype*='Person']",
            "[class*='team'] [class*='member']",
            "[class*='leadership'] [class*='card']",
            "[class*='people'] [class*='card']",
            "[class*='profile']",
            "article",
            "li",
        ]

        candidate_containers = []
        seen_ids = set()
        for selector in semantic_selectors:
            try:
                elements = soup.select(selector)
            except Exception:
                elements = []
            for el in elements:
                marker = id(el)
                if marker in seen_ids:
                    continue
                seen_ids.add(marker)
                candidate_containers.append(el)

        if not candidate_containers:
            groups = {}
            for container in soup.find_all(["div", "li", "article", "section"]):
                classes = tuple(sorted(container.get("class", [])))
                signature = (container.name, classes)
                groups.setdefault(signature, []).append(container)
            for (_, classes), group in groups.items():
                cls_txt = " ".join(classes).lower()
                if len(group) < 2:
                    continue
                if any(k in cls_txt for k in ["team", "member", "leader", "profile", "card"]):
                    candidate_containers.extend(group)

        email_pattern = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
        cards = []
        for container in candidate_containers:
            name, job_title = self._extract_name_and_title_from_container(container)
            if not name:
                continue
            if not self._looks_human_name(name):
                self.logger.warning(
                    f"Discarded record | name='{name}' | reason='invalid_human_name' | source='{url}'"
                )
                continue

            container_text = container.get_text(" ", strip=True)
            email_match = email_pattern.search(container_text)
            email = email_match.group(0) if email_match else None

            linkedin_url = None
            instagram_url = None
            twitter_url = None
            for link in container.find_all("a", href=True):
                href = (link.get("href") or "").strip()
                if not href:
                    continue
                href_lower = href.lower()
                if "linkedin.com/" in href_lower and not linkedin_url:
                    linkedin_url = href
                elif "instagram.com/" in href_lower and not instagram_url:
                    instagram_url = href
                elif ("twitter.com/" in href_lower or "x.com/" in href_lower) and not twitter_url:
                    twitter_url = href

            cards.append(
                {
                    "name": name,
                    "job_title": job_title or None,
                    "email": email,
                    "linkedin_url": linkedin_url,
                    "instagram_url": instagram_url,
                    "twitter_url": twitter_url,
                    "source_url": url,
                }
            )

        self.logger.info(f"People card scraper found {len(cards)} cards for {url}")
        return cards

    async def scrape_contact_page(self, url: str, crawler: AsyncWebCrawler) -> List[dict]:
        page = await self.crawl_page(crawler, url)
        raw_html = page.get("html") or ""
        if not raw_html:
            self.logger.info(f"Contact scraper found 0 records for {url}")
            return []

        soup = BeautifulSoup(raw_html, "lxml")
        email_pattern = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
        phone_pattern = re.compile(r"(\+?[\d\s\-\(\)]{7,20})")

        def normalize_email(value: str) -> str:
            return value.strip().lower()

        def normalize_phone(value: str) -> str:
            cleaned = " ".join(value.split()).strip()
            digits = re.sub(r"\D", "", cleaned)
            if len(digits) < 7:
                return ""
            return cleaned

        def looks_like_name(value: str) -> bool:
            text = " ".join(value.split()).strip()
            if len(text) < 3:
                return False
            if "@" in text:
                return False
            if len(re.sub(r"[^A-Za-z]", "", text)) < 2:
                return False
            return True

        def infer_name_near_contact(text: str, token: str) -> str:
            if not text or not token:
                return ""
            idx = text.lower().find(token.lower())
            if idx == -1:
                return ""
            before = text[max(0, idx - 100):idx].strip(" -|:,;.")
            after = text[idx + len(token): idx + len(token) + 100].strip(" -|:,;.")

            for candidate in [
                re.split(r"[|,;/]", before)[-1].strip(),
                re.split(r"[|,;/]", after)[0].strip(),
            ]:
                if looks_like_name(candidate):
                    return candidate
            return ""

        records = []
        seen = set()

        def add_record(name: str, email: str, phone: str):
            n = " ".join((name or "").split()).strip()
            e = normalize_email(email) if email else ""
            p = normalize_phone(phone) if phone else ""
            key = (n.lower(), e, p)
            if key in seen:
                return
            seen.add(key)
            records.append(
                {
                    "name": n,
                    "email": e or None,
                    "phone": p or None,
                    "job_title": None,
                    "source_url": url,
                }
            )

        for container in soup.find_all(["p", "li", "div"]):
            text = " ".join(container.get_text(" ", strip=True).split())
            if not text:
                continue

            emails = set()
            phones = set()

            for link in container.find_all("a", href=True):
                href = (link.get("href") or "").strip()
                if href.lower().startswith("mailto:"):
                    value = href.split(":", 1)[1].split("?", 1)[0].strip()
                    if value:
                        emails.add(value)
                if href.lower().startswith("tel:"):
                    value = href.split(":", 1)[1].split("?", 1)[0].strip()
                    normalized = normalize_phone(value)
                    if normalized:
                        phones.add(normalized)

            for match in email_pattern.findall(text):
                emails.add(match)

            for match in phone_pattern.findall(text):
                normalized = normalize_phone(match)
                if normalized:
                    phones.add(normalized)

            if not emails and not phones:
                continue

            strong_name = ""
            for tag in container.find_all(["strong", "b"]):
                candidate = " ".join(tag.get_text(" ", strip=True).split())
                if looks_like_name(candidate):
                    strong_name = candidate
                    break

            if emails:
                for email in emails:
                    name = strong_name or infer_name_near_contact(text, email)
                    add_record(name, email, "")

            if phones:
                for phone in phones:
                    name = strong_name or infer_name_near_contact(text, phone)
                    if emails:
                        for email in emails:
                            add_record(name, email, phone)
                    else:
                        add_record(name, "", phone)

        page_text = " ".join(soup.get_text(" ", strip=True).split())
        all_emails = {
            normalize_email(m)
            for m in email_pattern.findall(page_text)
            if normalize_email(m)
        }
        for link in soup.find_all("a", href=True):
            href = (link.get("href") or "").strip()
            if href.lower().startswith("mailto:"):
                value = href.split(":", 1)[1].split("?", 1)[0].strip()
                if value:
                    all_emails.add(normalize_email(value))

        existing_emails = {
            (record.get("email") or "").lower()
            for record in records
            if record.get("email")
        }
        for email in all_emails:
            if email not in existing_emails:
                add_record("", email, "")

        self.logger.info(f"Contact scraper found {len(records)} records for {url}")
        return records

    async def get_best_urls(self, domain: str, crawler: AsyncWebCrawler) -> List[str]:
        """Finds top 10 URLs most likely to have people data."""
        links = await self.fetch_sitemap_links(domain)
        if len(links) < 5:
            homepage_links = await self.fetch_homepage_links(domain, crawler)
            links.extend(homepage_links)

        links = list(dict.fromkeys(links))

        heuristic_scored = [(url, score_url_for_people(url)) for url in links]
        high_value = [url for url, score in heuristic_scored if score == 10]
        medium_value = [url for url, score in heuristic_scored if score == 5]

        self.logger.info(
            f"URL scoring for {domain}: "
            f"{len(high_value)} high-value, {len(medium_value)} medium-value"
        )

        if len(high_value) >= 10:
            return high_value[:20]

        if len(high_value) + len(medium_value) >= 3:
            return (high_value + medium_value)[:20]

        llm_scored = await self.pipeline.llm.score_links(
            (high_value + medium_value + [url for url, s in heuristic_scored if s > 0])[:50],
            domain,
        )
        if llm_scored:
            top = [item["url"] for item in llm_scored if item.get("score", 0) >= 5]
            if top:
                return top[:20]

        return [
            f"https://{domain}/about",
            f"https://{domain}/team",
            f"https://{domain}/leadership",
            f"https://{domain}/about-us",
            f"https://{domain}/contact",
        ]

    async def run_crawl(self, job_id: str, raw_input: str, db) -> None:
        """Main entry point. Detects URL vs domain and crawls accordingly."""
        try:
            input_type, clean_input = self.detect_input_type(raw_input)
            domain = self.extract_domain(clean_input) if input_type == "url" else clean_input

            self.logger.info(
                f"Crawl started | type={input_type} | "
                f"input={clean_input} | domain={domain}"
            )
            crud.update_crawl_job(db, job_id, status="running")

            async with AsyncWebCrawler(verbose=False) as crawler:
                section_urls = []
                section_people = []

                if input_type == "domain":
                    # Try fast section scraping first
                    try:
                        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
                            section_urls = await self.find_people_section_url(domain, client)
                    except Exception as e:
                        self.logger.warning(f"People section detection failed for {domain}: {e}")

                    if section_urls:
                        self.logger.info(f"Found {len(section_urls)} people section pages, scraping directly")
                        for section_url in section_urls:
                            try:
                                cards = await self.scrape_people_cards(section_url, crawler)
                                section_people.extend(cards)
                                self.logger.info(f"Got {len(cards)} people cards from {section_url}")
                            except Exception as e:
                                self.logger.warning(f"People card scraping failed for {section_url}: {e}")

                            if any(k in section_url.lower() for k in ["contact", "reach", "office", "location"]):
                                try:
                                    contact_records = await self.scrape_contact_page(section_url, crawler)
                                    section_people.extend(contact_records)
                                    self.logger.info(
                                        f"Got {len(contact_records)} contact records from {section_url}"
                                    )
                                except Exception as e:
                                    self.logger.warning(f"Contact page scraping failed for {section_url}: {e}")

                if input_type == "url":
                    urls_to_crawl = [clean_input]
                else:
                    if section_urls and len(section_people) > 5:
                        urls_to_crawl = section_urls
                        self.logger.info(
                            f"Using people section fast-path for {domain} "
                            f"with {len(section_people)} cards"
                        )
                    else:
                        urls_to_crawl = await self.get_best_urls(domain, crawler)

                    self.logger.info(f"Selected {len(urls_to_crawl)} URLs for {domain}")

                all_people = []
                successful_pages = []

                for url in urls_to_crawl:
                    page = await self.crawl_page(crawler, url)

                    if not page["success"]:
                        self.logger.warning(f"Skipping failed page: {url}")
                        continue

                    successful_pages.append(url)
                    crud.save_raw_extraction(db, job_id, url, page["content"])

                    # For direct URL runs, try card scraping to capture href-only social links.
                    if input_type == "url":
                        try:
                            cards = await self.scrape_people_cards(url, crawler)
                            if cards:
                                all_people.extend(cards)
                                self.logger.info(
                                    f"Card scraper added {len(cards)} people from {url}"
                                )
                        except Exception as e:
                            self.logger.warning(f"Card scraper failed for {url}: {e}")

                    people = await self.pipeline.run(
                        page["content"],
                        url,
                        raw_html=page.get("html", "")
                    )
                    all_people.extend(people)
                    self.logger.info(f"Extracted {len(people)} people from {url}")

                    await asyncio.sleep(0.5)


            enriched = self.enricher.enrich(all_people, domain, job_id)

            # Warn if likely people pages produce suspiciously low record counts.
            people_pages = [u for u in successful_pages if looks_like_people_page(u)]
            if people_pages:
                if len(enriched) == 0:
                    self.logger.warning(
                        f"**CRITICAL** Zero records extracted from likely people page for {domain}. "
                        f"Page may be dead, blocked, or has no real person data. "
                        f"Pages checked: {people_pages}"
                    )
                elif len(enriched) < 3:
                    self.logger.warning(
                        f"Very low extraction ({len(enriched)} records) from likely people page for {domain}. "
                        f"Page may have limited data or extraction issues. Pages: {people_pages}"
                    )

            crud.bulk_create_people(db, enriched)

            crud.update_crawl_job(
                db,
                job_id,
                status="done",
                pages_crawled=len(successful_pages),
                people_found=len(enriched),
                credits_used=0,
                completed_at=datetime.utcnow(),
            )
            self.logger.info(
                f"Crawl complete | domain={domain} | "
                f"pages={len(successful_pages)} | people={len(enriched)}"
            )
        except Exception as e:
            self.logger.error(f"Crawl failed: {e}", exc_info=True)
            crud.update_crawl_job(
                db,
                job_id,
                status="failed",
                error_message=str(e),
                completed_at=datetime.utcnow(),
            )
            raise
