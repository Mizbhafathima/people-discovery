from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class CrawlJobCreate(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    domain: str


class CrawlJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    domain: str
    status: str
    pages_crawled: int
    people_found: int
    credits_used: int
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class PersonResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    crawl_id: str
    domain: str
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    postcode: Optional[str] = None
    job_title: Optional[str] = None
    linkedin_url: Optional[str] = None
    source_url: str
    confidence: int
    created_at: datetime


class PeopleFilterParams(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    domain: Optional[str] = None
    min_confidence: Optional[int] = 1
    has_email: Optional[bool] = None
    has_phone: Optional[bool] = None


class CrawlStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job: CrawlJobResponse
    people: List[PersonResponse]
