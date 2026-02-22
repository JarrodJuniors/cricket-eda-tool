"""SQLAlchemy ORM models for PostgreSQL app metadata."""

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from backend.db.postgres import Base


class QueryLog(Base):
    __tablename__ = "query_logs"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, index=True, nullable=False)
    question = Column(Text, nullable=False)
    sql_generated = Column(Text, nullable=True)
    answer = Column(Text, nullable=False)
    execution_time_ms = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    feedbacks = relationship("Feedback", back_populates="query_log")


class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(Integer, primary_key=True, index=True)
    query_log_id = Column(Integer, ForeignKey("query_logs.id"), nullable=False)
    rating = Column(Integer, nullable=False)  # 1-5
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    query_log = relationship("QueryLog", back_populates="feedbacks")


class SyncLog(Base):
    """Tracks ETL incremental sync state."""
    __tablename__ = "sync_logs"

    id = Column(Integer, primary_key=True, index=True)
    competition = Column(String, nullable=False)   # "ipl", "t20i", "odi", "test"
    last_match_id = Column(String, nullable=True)
    last_synced_at = Column(DateTime, default=datetime.utcnow)
    total_matches = Column(Integer, default=0)
    success = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)
