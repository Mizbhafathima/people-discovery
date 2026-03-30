from pathlib import Path
import warnings

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
ENV_FILE = BASE_DIR / ".env"
DATABASE_URL = f"sqlite:///{(DATA_DIR / 'people_discovery.db').as_posix()}"


class Settings(BaseSettings):
    DATABASE_URL: str = DATABASE_URL
    MAX_PAGES_PER_CRAWL: int = 50
    LLM_PROVIDER: str = "google"
    GOOGLE_API_KEY: str = ""
    GOOGLE_MODEL: str = "gemma-2-27b-it"
    APP_ENV: str = "development"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

if not settings.GOOGLE_API_KEY:
    warnings.warn("GOOGLE_API_KEY is not set in backend/.env")
