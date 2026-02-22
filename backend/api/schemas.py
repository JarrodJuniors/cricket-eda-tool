"""Pydantic schemas for request/response models."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000, description="Natural language question")
    session_id: UUID = Field(default_factory=uuid4, description="Chat session identifier")


class QueryResponse(BaseModel):
    session_id: UUID
    question: str
    answer: str
    sql_generated: str | None = None
    data: list[dict[str, Any]] | None = None
    sources: list[str] = Field(default_factory=list)
    execution_time_ms: float
    created_at: datetime = Field(default_factory=datetime.utcnow)


class FeedbackRequest(BaseModel):
    query_log_id: int
    rating: int = Field(..., ge=1, le=5)
    comment: str | None = None


class FeedbackResponse(BaseModel):
    success: bool
    message: str
