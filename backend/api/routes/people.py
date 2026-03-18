from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.database import crud, schemas
from backend.database.session import get_db

router = APIRouter(prefix="/api/people", tags=["people"])


@router.get("", response_model=List[schemas.PersonResponse])
def get_people(
    domain: Optional[str] = None,
    min_confidence: int = Query(default=1, ge=1, le=3),
    has_email: Optional[bool] = None,
    has_phone: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    return crud.get_all_people(
        db,
        domain=domain,
        min_confidence=min_confidence,
        has_email=has_email,
        has_phone=has_phone,
    )


@router.get("/{crawl_id}", response_model=List[schemas.PersonResponse])
def get_people_by_crawl(crawl_id: str, db: Session = Depends(get_db)):
    people = crud.get_people_by_crawl(db, crawl_id)
    if not people:
        raise HTTPException(status_code=404, detail="No people records found")
    return people
