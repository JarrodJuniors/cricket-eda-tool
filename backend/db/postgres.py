"""SQLAlchemy setup for PostgreSQL (app metadata)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from backend.config import get_settings

settings = get_settings()

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_all_tables():
    """Create all PostgreSQL tables. Call on startup or via migration."""
    from backend.db import models  # noqa: F401 — import to register models
    Base.metadata.create_all(bind=engine)
