# People Discovery

People Discovery is a Python-based pipeline that crawls company websites and extracts real people records (name, title, contact, social links), then exports results to JSON and Excel.

It supports two usage modes:

1. API mode (FastAPI)
2. Batch mode (demo runner using inputs.txt)

## What This Project Does

Given a domain or a specific URL, the system:

1. Finds likely leadership/team/contact pages
2. Crawls and renders pages (including JS-heavy content)
3. Extracts people data using multiple extractors (JSON-LD, regex, NER hints, LLM)
4. Cleans and validates records to reduce hallucinations
5. Deduplicates and assigns confidence
6. Stores results in database
7. Exports results to JSON/Excel

## Project Structure

- [backend/main.py](backend/main.py): FastAPI app entry point
- [backend/api/routes/crawl.py](backend/api/routes/crawl.py): Create and monitor crawl jobs
- [backend/api/routes/people.py](backend/api/routes/people.py): Query extracted people
- [backend/api/routes/export.py](backend/api/routes/export.py): Export JSON/Excel
- [backend/services/crawler.py](backend/services/crawler.py): Crawl orchestration and URL strategy
- [backend/services/extractor/__init__.py](backend/services/extractor/__init__.py): Multi-stage extraction pipeline
- [backend/services/enricher.py](backend/services/enricher.py): Normalization, filtering, dedupe, confidence
- [backend/services/exporter.py](backend/services/exporter.py): JSON and Excel generation
- [backend/database/models.py](backend/database/models.py): SQLAlchemy data models
- [backend/database/crud.py](backend/database/crud.py): DB operations
- [backend/database/session.py](backend/database/session.py): DB session and initialization
- [backend/core/utils.py](backend/core/utils.py): Shared heuristics and helpers
- [demo_runner.py](demo_runner.py): Batch run script using inputs.txt
- [backend/data/results.json](backend/data/results.json): Accumulated output JSON
- [backend/data/results.xlsx](backend/data/results.xlsx): Accumulated output Excel

## Architecture

### 1) Interface Layer

- API endpoints for crawl, people, and export
- Batch runner for multi-domain runs

### 2) Crawl Layer

Implemented in [backend/services/crawler.py](backend/services/crawler.py).

Main responsibilities:

1. Detect input type: full URL vs root domain
2. Discover likely people pages via:
   - known path probes (team, leadership, board, etc.)
   - sitemap/robots/homepage links
   - URL scoring
3. Render pages with JS interaction (scroll/click tabs/buttons)
4. Remove obvious non-content noise (nav/header/cookie blocks)
5. Send cleaned page content to extraction pipeline

### 3) Extraction Layer

Implemented in [backend/services/extractor/__init__.py](backend/services/extractor/__init__.py).

Pipeline stages:

1. JSON-LD extractor (structured people data)
2. Regex extractor (emails/phones)
3. GLiNER extractor (name hints)
4. LLM extractor (name/title/social extraction)
5. Merge all raw candidate records

Important note: GLiNER names are used as hints for LLM, not blindly converted to person records.

### 4) Enrichment and Quality Layer

Implemented in [backend/services/enricher.py](backend/services/enricher.py).

This is the main quality gate:

1. Normalize fields (whitespace, casing, cleanup)
2. Validate real-person names
3. Validate plausible job titles
4. Reject organization names and navigation phrases
5. Normalize and reject appointment/timeline-only hallucinated titles
6. Deduplicate records by fuzzy name/email/phone logic
7. Set confidence score

Confidence (from [backend/core/utils.py](backend/core/utils.py)):

- 2: name + job title
- 1: name only
- 0: no valid name

### 5) Persistence Layer

SQLAlchemy models in [backend/database/models.py](backend/database/models.py):

- CrawlJob
- Person
- RawExtraction

Crawl status and extracted data are saved through [backend/database/crud.py](backend/database/crud.py).

### 6) Export Layer

Implemented in [backend/services/exporter.py](backend/services/exporter.py).

- JSON export with UTF-8 preservation
- Excel export grouped by domain with formatting

## End-to-End Flow

1. Input submitted (API request or line in inputs.txt)
2. Crawl job created
3. Candidate URLs selected
4. Pages crawled and cleaned
5. Raw extraction performed
6. Records enriched/filtered/deduped
7. People saved to DB
8. Job marked done/failed with counts
9. Results exported to JSON/Excel

## Setup

## Requirements

- Python 3.11+
- Virtual environment
- Google API key for LLM extraction

## Environment

Create [backend/.env](backend/.env) with at least:

- GOOGLE_API_KEY=your_key
- GOOGLE_MODEL=gemma-2-27b-it
- DATABASE_URL=sqlite:///./data/people_discovery.db

## Install dependencies

From project root:

```powershell
python -m venv env
.\env\Scripts\Activate.ps1
pip install -r backend/requirements.txt
```

## How To Run

### Option A: Batch mode (recommended for many domains)

1. Put domains/URLs in [inputs.txt](inputs.txt), one per line
2. Run:

```powershell
python demo_runner.py
```

Output files:

- [backend/data/results.json](backend/data/results.json)
- [backend/data/results.xlsx](backend/data/results.xlsx)

### Option B: API mode

Start API server:

```powershell
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Use docs:

- http://127.0.0.1:8000/docs

## API Summary

- POST /api/crawl
- GET /api/crawl
- GET /api/crawl/{job_id}
- GET /api/crawl/{job_id}/status
- GET /api/people
- GET /api/people/{crawl_id}
- GET /api/export/{job_id}/json
- GET /api/export/{job_id}/excel
- GET /api/export/all/excel

## Data Quality Rules (Current)

The project actively filters common hallucinations:

1. Reject company/category/navigation phrases as names
2. Reject social/site boilerplate as people
3. Reject titles that are actually person names
4. Normalize appointment-style timeline text
5. Drop weak or malformed records

This improves precision, especially on noisy corporate pages.

## Known Challenges

1. Some JS-heavy pages hide profiles until interaction
2. Very long pages can still under-extract if key sections are trimmed
3. Some pages mix board metadata and biography timelines, which can create noisy titles

## Troubleshooting

### Python cannot open file path

If your path contains spaces, use quotes in PowerShell:

```powershell
cd "C:\Users\Mizhba\Documents\projects\people discovery\people-discovery"
python demo_runner.py
```

Use ; instead of && in older PowerShell versions.

### Too few people extracted

1. Confirm input URL points to actual leadership/team page
2. Check logs for low extraction warnings
3. Re-run single problematic URL for targeted debugging

### Hallucinated title or role text

This is filtered in [backend/services/enricher.py](backend/services/enricher.py). If a new pattern appears, add it to validation rules there.

## Recommended Workflow For Production Runs

1. Maintain target URLs in [inputs.txt](inputs.txt)
2. Run [demo_runner.py](demo_runner.py)
3. Inspect [backend/data/results.json](backend/data/results.json)
4. Spot-check random records by domain
5. Export/share [backend/data/results.xlsx](backend/data/results.xlsx)

## Notes

- Results are cumulative in batch mode
- Duplicate prevention happens both in enricher and runner merge logic
- For best results, prefer direct leadership URLs instead of only root domains
