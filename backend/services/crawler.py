from crawl4ai import AsyncWebCrawler
from backend.config import settings
from backend.services.extractor import ExtractionPipeline
from backend.services.enricher import EnricherService
from backend.database import crud
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import asyncio
import logging
from typing import List, Set, Tuple

class CrawlerService:
    def __init__(self):
        """
        Initialize the CrawlerService with an extraction pipeline, an enricher service,
        and a logger.
        """
        self.pipeline = ExtractionPipeline()
        self.enricher = EnricherService()
        self.logger = logging.getLogger(__name__)

    def detect_input_type(self, raw_input: str) -> Tuple[str, str]:
        """
        Detect whether the input is a full URL or a plain domain.
        """
        raw_input = raw_input.strip()
        if raw_input.startswith("http://") or raw_input.startswith("https://"):
            return "url", raw_input.rstrip("/")
        else:
            clean = raw_input.removeprefix("http://").removeprefix("https://").removeprefix("www.").rstrip("/")
            return "domain", clean

    def extract_domain(self, url: str) -> str:
        """
        Extract the clean domain from a URL.
        """
        netloc = urlparse(url).netloc
        return netloc.lstrip("www.")

    def get_priority_urls(self, domain: str) -> List[str]:
        """
        Generate a list of priority URLs for crawling based on common paths.
        """
        paths = [
            "/about", "/about-us", "/team", "/our-team", "/people",
            "/contact", "/contact-us", "/staff", "/leadership",
            "/management", "/company", "/who-we-are", "/meet-the-team",
            "/our-people", "/board", "/executives", "/directors"
        ]
        return [f"https://{domain}{path}" for path in paths]

    async def crawl_page(self, crawler: AsyncWebCrawler, url: str) -> dict:
        """
        Crawl a single page and return its content.
        """
        try:
            result = await crawler.arun(url, bypass_cache=True, word_count_threshold=10)
            return {
                "url": url,
                "html": result.html if result.html else "",
                "markdown": result.markdown if result.markdown else "",
                "success": result.success,
            }
        except Exception as e:
            self.logger.warning("Failed to crawl %s: %s", url, e)
            return {"url": url, "html": "", "markdown": "", "success": False}

    async def crawl_single_url(self, crawler: AsyncWebCrawler, url: str) -> List[dict]:
        """
        Crawl exactly one URL and return a list of page results.
        """
        self.logger.info("Single URL mode: crawling %s", url)
        return [await self.crawl_page(crawler, url)]

    async def crawl_domain_priority_pages(self, crawler: AsyncWebCrawler, domain: str) -> List[dict]:
        """
        Crawl only high-value priority pages of a domain.
        """
        priority_urls = self.get_priority_urls(domain)
        results = []
        for url in priority_urls:
            page = await self.crawl_page(crawler, url)
            if page["success"]:
                results.append(page)
            if len(results) >= settings.MAX_PAGES_PER_CRAWL:
                break
            await asyncio.sleep(0.3)  # Be respectful with a small delay
        self.logger.info("Domain mode: crawled %d priority pages for %s", len(results), domain)
        return results

    async def run_crawl(self, job_id: str, raw_input: str, db) -> None:
        """
        Main crawl method. Accepts either a domain or full URL and handles both correctly.
        """
        try:
            input_type, clean_input = self.detect_input_type(raw_input)
            domain = self.extract_domain(clean_input) if input_type == "url" else clean_input

            self.logger.info("Crawl started | type=%s | input=%s | domain=%s", input_type, clean_input, domain)
            crud.update_crawl_job(db, job_id, status="running")

            async with AsyncWebCrawler(verbose=False) as crawler:
                if input_type == "url":
                    pages = await self.crawl_single_url(crawler, clean_input)
                else:
                    pages = await self.crawl_domain_priority_pages(crawler, domain)

                all_people = []
                for page in pages:
                    if not page["success"]:
                        continue
                    content = page["markdown"] or page["html"]
                    if not content:
                        continue
                    crud.save_raw_extraction(db, job_id, page["url"], content)
                    people = self.pipeline.run(content, page["url"])
                    all_people.extend(people)
                    self.logger.info("Extracted %d people from %s", len(people), page["url"])

                enriched = self.enricher.enrich(all_people, domain, job_id)
                self.logger.info("Enriched %d people records", len(enriched))

                crud.bulk_create_people(db, enriched)

                crud.update_crawl_job(
                    db,
                    job_id,
                    status="done",
                    pages_crawled=len(pages),
                    people_found=len(enriched),
                    credits_used=0,
                    completed_at=datetime.utcnow(),
                )
                self.logger.info(
                    "Crawl complete | domain=%s | pages=%d | people=%d",
                    domain,
                    len(pages),
                    len(enriched),
                )
        except Exception as e:
            self.logger.error("Crawl failed for job %s: %s", job_id, e)
            crud.update_crawl_job(
                db,
                job_id,
                status="failed",
                error_message=str(e),
                completed_at=datetime.utcnow(),
            )
            raise
