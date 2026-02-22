"""Query route — accepts NL question and returns analytics answer."""

import time
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.api.schemas import QueryRequest, QueryResponse
from backend.agent.graph import run_agent
from backend.db.postgres import get_db
from backend.db import models

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query_endpoint(request: QueryRequest, db: Session = Depends(get_db)):
    """Accept a natural language question and return cricket analytics."""
    start = time.perf_counter()

    try:
        result = await run_agent(question=request.question, session_id=str(request.session_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {e}") from e

    elapsed_ms = (time.perf_counter() - start) * 1000

    # Persist to query log
    log = models.QueryLog(
        session_id=str(request.session_id),
        question=request.question,
        sql_generated=result.get("sql"),
        answer=result["answer"],
        execution_time_ms=elapsed_ms,
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    return QueryResponse(
        session_id=request.session_id,
        question=request.question,
        answer=result["answer"],
        sql_generated=result.get("sql"),
        data=result.get("data"),
        sources=result.get("sources", []),
        execution_time_ms=elapsed_ms,
    )
