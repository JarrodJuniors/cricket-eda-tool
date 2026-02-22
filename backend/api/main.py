"""FastAPI application entrypoint."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import get_settings
from backend.api.routes import query, history, feedback
from backend.db.duckdb_client import get_duckdb

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    # Verify DuckDB is reachable on startup
    conn = get_duckdb()
    conn.execute("SELECT 1").fetchone()
    print("✅ DuckDB connection verified")
    yield
    # Cleanup on shutdown
    conn.close()
    print("👋 DuckDB connection closed")


app = FastAPI(
    title="Cricket EDA API",
    description="Natural-language cricket analytics powered by DuckDB + LangGraph",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(query.router, prefix="/api/v1", tags=["Query"])
app.include_router(history.router, prefix="/api/v1", tags=["History"])
app.include_router(feedback.router, prefix="/api/v1", tags=["Feedback"])


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok", "version": "0.1.0"}
