import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    domain = Column(String(255), nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    pages_crawled = Column(Integer, default=0)
    people_found = Column(Integer, default=0)
    credits_used = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    people = relationship("Person", back_populates="crawl_job")
    raw_extractions = relationship("RawExtraction", back_populates="crawl_job")


class Person(Base):
    __tablename__ = "people"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    crawl_id = Column(String(36), ForeignKey("crawl_jobs.id"), nullable=False)
    domain = Column(String(255), nullable=False)
    name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    postcode = Column(String(20), nullable=True)
    job_title = Column(String(255), nullable=True)
    linkedin_url = Column(String(500), nullable=True)
    source_url = Column(Text, nullable=False)
    confidence = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

    crawl_job = relationship("CrawlJob", back_populates="people")


class RawExtraction(Base):
    __tablename__ = "raw_extractions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    crawl_id = Column(String(36), ForeignKey("crawl_jobs.id"), nullable=False)
    page_url = Column(Text, nullable=False)
    raw_text = Column(Text, nullable=True)
    extracted_at = Column(DateTime, default=datetime.utcnow)

    crawl_job = relationship("CrawlJob", back_populates="raw_extractions")
