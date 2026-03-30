import asyncio
import json
from datetime import datetime
import logging
from pathlib import Path
import sys
from urllib.parse import urlparse
from dotenv import load_dotenv
from backend.config import settings
from backend.database import crud
from backend.database.session import SessionLocal, init_db
from backend.services.crawler import CrawlerService
from backend.services.exporter import ExporterService

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "backend" / "data"
INPUTS_FILE = PROJECT_ROOT / "inputs.txt"
RESULTS_JSON = DATA_DIR / "results.json"
RESULTS_XLSX = DATA_DIR / "results.xlsx"

DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_display_name(raw_input: str) -> str:
    if raw_input.startswith("http"):
        parsed = urlparse(raw_input)
        return parsed.netloc.replace("www.", "")
    return raw_input.replace("www.", "").strip("/")


def load_inputs() -> list[str]:
    if not INPUTS_FILE.exists():
        print("ERROR: inputs.txt not found in project root.")
        sys.exit(1)
    inputs = []
    for line in INPUTS_FILE.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        inputs.append(value)
    if not inputs:
        print("ERROR: inputs.txt is empty.")
        sys.exit(1)
    logger.info("Loaded %d inputs from inputs.txt", len(inputs))
    return inputs


def load_existing_results() -> list[dict]:
    """Load existing results from results.json if it exists."""
    if not RESULTS_JSON.exists():
        return []
    try:
        data = json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
        if isinstance(data, list):
            logger.info("Loaded %d existing results from results.json", len(data))
            return data
        return []
    except Exception as e:
        logger.warning("Could not load existing results: %s", e)
        return []


async def main() -> None:
    inputs = load_inputs()
    init_db()

    crawler = CrawlerService()
    exporter = ExporterService()
    db = SessionLocal()

    summary = []
    current_run_job_ids = []
    start_time = datetime.utcnow()

    print("NOTE: Results accumulate across all runs in results.json and results.xlsx")
    print("=" * 72)
    print("People Discovery Demo Runner")
    print(f"Inputs         : {len(inputs)}")
    print(f"Max pages/crawl: {settings.MAX_PAGES_PER_CRAWL}")
    print(f"LLM provider   : {settings.LLM_PROVIDER}")
    print(f"Start time UTC : {start_time.isoformat()}")
    print("=" * 72)

    try:
        for index, raw_input in enumerate(inputs):
            display = raw_input if raw_input.startswith("http") else f"Domain: {raw_input}"
            print(f"[{index + 1}/{len(inputs)}] Crawling: {display}")
            try:
                display_name = extract_display_name(raw_input)
                job = crud.create_crawl_job(db, display_name)
                current_run_job_ids.append(job.id)

                try:
                    await asyncio.wait_for(
                        crawler.run_crawl(job.id, raw_input, db),
                        timeout=600
                    )
                except asyncio.TimeoutError:
                    print("    ⏱ Timed out after 120s")
                    summary.append({
                        "input": raw_input,
                        "status": "failed",
                        "pages_crawled": 0,
                        "people_found": 0,
                        "error": "Timed out after 120 seconds"
                    })
                    await asyncio.sleep(1)
                    continue

                updated_job = crud.get_crawl_job(db, job.id)
                pages = updated_job.pages_crawled if updated_job else 0
                people_count = updated_job.people_found if updated_job else 0
                print(f"    ✓ pages={pages} | people={people_count}")
                summary.append({
                    "input": raw_input,
                    "status": "success",
                    "pages_crawled": pages,
                    "people_found": people_count,
                    "error": None
                })

            except Exception as exc:
                print(f"    ✗ Failed: {exc}")
                summary.append({
                    "input": raw_input,
                    "status": "failed",
                    "pages_crawled": 0,
                    "people_found": 0,
                    "error": str(exc)
                })

            await asyncio.sleep(1)

        # Get people from current run only
        current_run_people = []
        for job_id in current_run_job_ids:
            people = crud.get_people_by_crawl(db, job_id)
            current_run_people.extend(people)
        current_run_dicts = exporter.people_to_dicts(current_run_people)

        # Load all previous results
        existing_results = load_existing_results()

        # Deduplicate across runs using name+domain+source_url as key
        existing_keys = set()
        for p in existing_results:
            name = (p.get("name") or "").lower().strip()
            domain = (p.get("domain") or "").lower().strip()
            source = (p.get("source_url") or "").lower().strip()
            if name:
                existing_keys.add(f"{name}|{domain}|{source}")

        new_people = []
        for p in current_run_dicts:
            name = (p.get("name") or "").lower().strip()
            domain = (p.get("domain") or "").lower().strip()
            source = (p.get("source_url") or "").lower().strip()
            key = f"{name}|{domain}|{source}"
            if key not in existing_keys:
                existing_keys.add(key)
                new_people.append(p)

        all_results = existing_results + new_people

        # Save accumulated JSON
        RESULTS_JSON.write_text(
            exporter.to_json(all_results),
            encoding="utf-8"
        )

        # Save accumulated Excel
        RESULTS_XLSX.write_bytes(
            exporter.to_excel_bytes(all_results)
        )

        success_count = sum(1 for item in summary if item["status"] == "success")
        fail_count = sum(1 for item in summary if item["status"] == "failed")
        this_run_count = len(current_run_dicts)
        total_count = len(all_results)

        print("\nRun Summary")
        print("-" * 72)
        print(f"Total inputs    : {len(summary)}")
        print(f"Success         : {success_count}")
        print(f"Failed          : {fail_count}")
        print(f"People this run : {this_run_count}")
        print(f"Total in file   : {total_count} people across all runs")
        print(f"Results JSON    : {RESULTS_JSON}")
        print(f"Results Excel   : {RESULTS_XLSX}")

        failed = [item for item in summary if item["status"] == "failed"]
        if failed:
            print("\nFailed Inputs")
            print("-" * 72)
            for item in failed:
                print(f"  {item['input']}: {item['error']}")

    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
