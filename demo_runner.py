import asyncio
from datetime import datetime
import logging
from pathlib import Path
import sys
import time

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

DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_inputs() -> list[str]:
    """
    Load inputs from inputs.txt, skipping empty lines and comments.
    """
    if not INPUTS_FILE.exists():
        print("Error: inputs.txt not found in project root.")
        sys.exit(1)

    inputs = []
    for line in INPUTS_FILE.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        inputs.append(value)

    if not inputs:
        print("Error: inputs.txt has no valid entries.")
        sys.exit(1)

    logger.info("Loaded %d inputs from inputs.txt", len(inputs))
    return inputs

async def main() -> None:
    inputs = load_inputs()
    init_db()

    crawler = CrawlerService()
    exporter = ExporterService()
    db = SessionLocal()

    summary = []
    start_time = datetime.utcnow()

    print("=" * 72)
    print("People Discovery Demo Runner")
    print(f"Inputs: {len(inputs)}")
    print(f"Max pages per crawl: {settings.MAX_PAGES_PER_CRAWL}")
    print(f"LLM provider: {settings.LLM_PROVIDER}")
    print(f"Start time (UTC): {start_time.isoformat()}")
    print("=" * 72)

    try:
        for index, raw_input in enumerate(inputs):
            display = raw_input if raw_input.startswith("http") else f"Domain: {raw_input}"
            print(f"[{index + 1}/{len(inputs)}] Crawling: {display}")
            try:
                job = crud.create_crawl_job(db, raw_input)
                await crawler.run_crawl(job.id, raw_input, db)
                updated_job = crud.get_crawl_job(db, job.id)

                pages = updated_job.pages_crawled if updated_job else 0
                people_count = updated_job.people_found if updated_job else 0
                print(f"  Success: pages={pages}, people={people_count}")

                summary.append(
                    {
                        "input": raw_input,
                        "status": "success",
                        "pages_crawled": pages,
                        "people_found": people_count,
                        "error": None,
                    }
                )
            except Exception as exc:
                print(f"  Failed: {exc}")
                summary.append(
                    {
                        "input": raw_input,
                        "status": "failed",
                        "pages_crawled": 0,
                        "people_found": 0,
                        "error": str(exc),
                    }
                )

            time.sleep(1)

        all_people = crud.get_all_people(db)
        people_dicts = exporter.people_to_dicts(all_people)

        json_path = DATA_DIR / "demo_output.json"
        xlsx_path = DATA_DIR / "demo_output.xlsx"

        json_path.write_text(exporter.to_json(people_dicts), encoding="utf-8")
        xlsx_path.write_bytes(exporter.to_excel_bytes(people_dicts))

        success_count = sum(1 for item in summary if item["status"] == "success")
        fail_count = sum(1 for item in summary if item["status"] == "failed")

        print("\nRun Summary")
        print("-" * 72)
        print(f"Total inputs: {len(summary)}")
        print(f"Success: {success_count}")
        print(f"Failed: {fail_count}")
        print(f"JSON export: {json_path}")
        print(f"Excel export: {xlsx_path}")

        failed = [item for item in summary if item["status"] == "failed"]
        if failed:
            print("\nFailed Inputs")
            print("-" * 72)
            for item in failed:
                print(f"{item['input']}: {item['error']}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main())
