from pathlib import Path
import warnings

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./backend/data/people_discovery.db"
    FIRECRAWL_API_KEY: str = ""
    MAX_PAGES_PER_CRAWL: int = 50
    LLM_PROVIDER: str = "google"
    GOOGLE_API_KEY: str = ""
    GOOGLE_MODEL: str = "gemma-2-27b-it"
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "gemma2:9b"
    APP_ENV: str = "development"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

if not settings.FIRECRAWL_API_KEY:
    warnings.warn("FIRECRAWL_API_KEY is not set in backend/.env")
if not settings.GOOGLE_API_KEY:
    warnings.warn("GOOGLE_API_KEY is not set in backend/.env")
