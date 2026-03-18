from datetime import datetime
import logging
from typing import Any
from typing import List

from firecrawl import FirecrawlApp

from backend.config import settings
from backend.core.utils import extract_domain
from backend.database import crud
from backend.services.enricher import EnricherService
from backend.services.extractor import ExtractionPipeline


class CrawlerService:
    def __init__(self):
        self.app = FirecrawlApp(api_key=settings.FIRECRAWL_API_KEY)
        self.pipeline = ExtractionPipeline()
        self.enricher = EnricherService()
        self.logger = logging.getLogger(__name__)

    def get_priority_urls(self, domain: str) -> List[str]:
        paths = [
            "/about",
            "/about-us",
            "/team",
            "/our-team",
            "/people",
            "/contact",
            "/contact-us",
            "/staff",
            "/leadership",
            "/management",
        ]
        return [f"https://{domain}{path}" for path in paths]

    @staticmethod
    def _to_page_dict(page: Any) -> dict:
        if isinstance(page, dict):
            return page
        if hasattr(page, "model_dump"):
            return page.model_dump()
        return {}

    async def run_crawl(self, job_id: str, domain_or_url: str, db) -> None:
        try:
            crud.update_crawl_job(db, job_id, status="running")

            normalized = (domain_or_url or "").strip().rstrip("/")
            if normalized.lower().startswith(("http://", "https://")):
                start_url = normalized
            else:
                start_url = f"https://{normalized}"

            domain = extract_domain(start_url)

            crawl_result = self.app.crawl(
                url=start_url,
                limit=settings.MAX_PAGES_PER_CRAWL,
                crawl_entire_domain=True,
                max_discovery_depth=2,
                scrape_options={"formats": ["markdown", "html"]},
            )

            if isinstance(crawl_result, dict):
                pages = crawl_result.get("data", [])
            else:
                pages = getattr(crawl_result, "data", []) or []

            all_people = []
            for page in pages:
                page_data = self._to_page_dict(page)
                content = page_data.get("markdown")
                if content is None:
                    content = page_data.get("html") or page_data.get("raw_html")

                metadata = page_data.get("metadata") or {}
                source_url = (
                    page_data.get("url")
                    or metadata.get("source_url")
                    or metadata.get("url")
                    or start_url
                )

                if not content:
                    continue

                crud.save_raw_extraction(db, job_id, source_url, content)
                all_people.extend(self.pipeline.run(content, source_url))

            enriched = self.enricher.enrich(all_people, domain, job_id)
            crud.bulk_create_people(db, enriched)

            crud.update_crawl_job(
                db,
                job_id,
                status="done",
                pages_crawled=len(pages),
                people_found=len(enriched),
                credits_used=len(pages),
                completed_at=datetime.utcnow(),
            )
        except Exception as exception:
            crud.update_crawl_job(
                db,
                job_id,
                status="failed",
                error_message=str(exception),
            )
            self.logger.error("Crawl failed for job %s: %s", job_id, exception)
            raise
        finally:
            self.logger.info("Crawl finished for job %s", job_id)
