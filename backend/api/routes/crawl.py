import logging
from typing import List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.database import crud, schemas
from backend.database.session import SessionLocal, get_db
from backend.core.utils import extract_domain
from backend.services.crawler import CrawlerService

router = APIRouter(prefix="/api/crawl", tags=["crawl"])
crawler_service = CrawlerService()


def run_crawl_background(job_id: str, raw_input: str):
    db = SessionLocal()
    try:
        import asyncio

        asyncio.run(crawler_service.run_crawl(job_id, raw_input, db))
    except Exception as e:
        logging.error(f"Background crawl failed: {e}")
    finally:
        db.close()


@router.post("", response_model=schemas.CrawlJobResponse, status_code=202)
def create_crawl_job(
    payload: schemas.CrawlJobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    raw_input = payload.domain.strip()
    domain = extract_domain(raw_input)
    job = crud.create_crawl_job(db, domain)
    background_tasks.add_task(run_crawl_background, job.id, raw_input)
    return job


@router.get("", response_model=List[schemas.CrawlJobResponse])
def get_all_crawl_jobs(db: Session = Depends(get_db)):
    return crud.get_all_crawl_jobs(db)


@router.get("/{job_id}", response_model=schemas.CrawlJobResponse)
def get_crawl_job(job_id: str, db: Session = Depends(get_db)):
    job = crud.get_crawl_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")
    return job


@router.get("/{job_id}/status", response_model=schemas.CrawlStatusResponse)
def get_crawl_status(job_id: str, db: Session = Depends(get_db)):
    job = crud.get_crawl_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")
    people = crud.get_people_by_crawl(db, job_id)
    return {"job": job, "people": people}


@router.delete("/{job_id}", status_code=204)
def delete_crawl_job(job_id: str, db: Session = Depends(get_db)):
    deleted = crud.delete_crawl_job(db, job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Crawl job not found")
    return None
