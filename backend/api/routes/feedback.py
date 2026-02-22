"""Feedback route — allows users to rate query responses."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.api.schemas import FeedbackRequest, FeedbackResponse
from backend.db.postgres import get_db
from backend.db import models

router = APIRouter()


@router.post("/feedback", response_model=FeedbackResponse)
def submit_feedback(request: FeedbackRequest, db: Session = Depends(get_db)):
    log = db.query(models.QueryLog).filter(models.QueryLog.id == request.query_log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="Query log not found")

    feedback = models.Feedback(
        query_log_id=request.query_log_id,
        rating=request.rating,
        comment=request.comment,
    )
    db.add(feedback)
    db.commit()
    return FeedbackResponse(success=True, message="Feedback recorded. Thank you!")
