"""History route — returns past queries for a session."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.db.postgres import get_db
from backend.db import models

router = APIRouter()


@router.get("/history/{session_id}")
def get_history(session_id: str, limit: int = 20, db: Session = Depends(get_db)):
    logs = (
        db.query(models.QueryLog)
        .filter(models.QueryLog.session_id == session_id)
        .order_by(models.QueryLog.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": log.id,
            "question": log.question,
            "answer": log.answer,
            "created_at": log.created_at,
            "execution_time_ms": log.execution_time_ms,
        }
        for log in logs
    ]
