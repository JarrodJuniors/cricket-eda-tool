"""Application configuration loaded from environment variables."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # App
    app_env: str = "development"
    log_level: str = "INFO"

    # PostgreSQL
    database_url: str = "postgresql://cricket:changeme@localhost:5432/cricket_eda"

    # DuckDB
    duckdb_path: str = "data/duckdb/cricket.duckdb"

    # Data paths
    data_raw_dir: str = "data/raw"
    data_processed_dir: str = "data/processed"

    # LLM
    llm_provider: str = "ollama"  # "ollama" | "openai"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.1"
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"

    # CORS
    allowed_origins: list[str] = ["http://localhost:5173", "http://localhost:3000"]

    @property
    def duckdb_path_obj(self) -> Path:
        return Path(self.duckdb_path)

    @property
    def data_raw_path(self) -> Path:
        return Path(self.data_raw_dir)

    @property
    def data_processed_path(self) -> Path:
        return Path(self.data_processed_dir)


@lru_cache
def get_settings() -> Settings:
    return Settings()
