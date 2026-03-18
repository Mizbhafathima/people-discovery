import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes import crawl, export, people
from backend.config import settings
from backend.database.session import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="People Discovery API",
    description="B2B people data extraction - crawls domains and extracts contact info",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    logger.info("Database initialized")
    logger.info("People Discovery API started")


app.include_router(crawl.router)
app.include_router(people.router)
app.include_router(export.router)


@app.get("/")
def health_check() -> dict:
    return {"status": "ok", "version": "1.0.0", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=True)
