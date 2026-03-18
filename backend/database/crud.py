from datetime import datetime
import uuid
from typing import List, Optional

from sqlalchemy.orm import Session

from backend.database.models import CrawlJob, Person, RawExtraction


def create_crawl_job(db: Session, domain: str) -> CrawlJob:
    job = CrawlJob(domain=domain, status="pending")
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_crawl_job(db: Session, job_id: str) -> Optional[CrawlJob]:
    return db.query(CrawlJob).filter(CrawlJob.id == job_id).first()


def update_crawl_job(db: Session, job_id: str, **kwargs) -> Optional[CrawlJob]:
    job = get_crawl_job(db, job_id)
    if not job:
        return None

    for key, value in kwargs.items():
        setattr(job, key, value)

    db.commit()
    db.refresh(job)
    return job


def get_all_crawl_jobs(db: Session) -> List[CrawlJob]:
    return db.query(CrawlJob).order_by(CrawlJob.created_at.desc()).all()


def create_person(db: Session, person_data: dict) -> Person:
    person = Person(**person_data)
    db.add(person)
    db.commit()
    db.refresh(person)
    return person


def bulk_create_people(db: Session, people: List[dict]) -> int:
    if not people:
        return 0

    payload = []
    for person in people:
        record = dict(person)
        if not record.get("id"):
            record["id"] = str(uuid.uuid4())
        if not record.get("created_at"):
            record["created_at"] = datetime.utcnow()
        payload.append(record)

    db.bulk_insert_mappings(Person, payload)
    db.commit()
    return len(payload)


def get_people_by_crawl(db: Session, crawl_id: str) -> List[Person]:
    return db.query(Person).filter(Person.crawl_id == crawl_id).all()


def get_all_people(
    db: Session,
    domain=None,
    min_confidence=1,
    has_email=None,
    has_phone=None,
) -> List[Person]:
    query = db.query(Person)

    if domain is not None:
        query = query.filter(Person.domain == domain)

    if min_confidence is not None:
        query = query.filter(Person.confidence >= min_confidence)

    if has_email is True:
        query = query.filter(Person.email.isnot(None), Person.email != "")
    elif has_email is False:
        query = query.filter((Person.email.is_(None)) | (Person.email == ""))

    if has_phone is True:
        query = query.filter(Person.phone.isnot(None), Person.phone != "")
    elif has_phone is False:
        query = query.filter((Person.phone.is_(None)) | (Person.phone == ""))

    return query.all()


def save_raw_extraction(db: Session, crawl_id: str, page_url: str, raw_text: str) -> RawExtraction:
    extraction = RawExtraction(crawl_id=crawl_id, page_url=page_url, raw_text=raw_text)
    db.add(extraction)
    db.commit()
    db.refresh(extraction)
    return extraction


def delete_crawl_job(db: Session, job_id: str) -> bool:
    job = get_crawl_job(db, job_id)
    if not job:
        return False

    db.query(Person).filter(Person.crawl_id == job_id).delete(synchronize_session=False)
    db.query(RawExtraction).filter(RawExtraction.crawl_id == job_id).delete(synchronize_session=False)
    db.delete(job)
    db.commit()
    return True
