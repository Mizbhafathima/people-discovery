import io
import json

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from backend.database import crud
from backend.database.session import get_db
from backend.services.exporter import ExporterService

router = APIRouter(prefix="/api/export", tags=["export"])
exporter = ExporterService()


@router.get("/{job_id}/json")
def export_job_json(job_id: str, db: Session = Depends(get_db)):
    job = crud.get_crawl_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")

    people = crud.get_people_by_crawl(db, job_id)
    people_dicts = exporter.people_to_dicts(people)
    json_payload = exporter.to_json(people_dicts)

    return JSONResponse(
        content=json.loads(json_payload),
        headers={"Content-Disposition": f'attachment; filename="{job.domain}_people.json"'},
    )


@router.get("/{job_id}/excel")
def export_job_excel(job_id: str, db: Session = Depends(get_db)):
    job = crud.get_crawl_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job not found")

    people = crud.get_people_by_crawl(db, job_id)
    if not people:
        raise HTTPException(status_code=404, detail="No people found for crawl job")

    people_dicts = exporter.people_to_dicts(people)
    excel_bytes = exporter.to_excel_bytes(people_dicts)

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{job.domain}_people.xlsx"'},
    )


@router.get("/all/excel")
def export_all_excel(db: Session = Depends(get_db)):
    people = crud.get_all_people(db)
    people_dicts = exporter.people_to_dicts(people)
    excel_bytes = exporter.to_excel_bytes(people_dicts)

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="all_people.xlsx"'},
    )
