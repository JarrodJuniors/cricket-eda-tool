# Cricket EDA

> Natural-language cricket analytics powered by DuckDB, FastAPI, and LangGraph.

Ask plain English questions about cricket and get data-backed answers:

> *"How much did Virat Kohli score in the 2016 IPL?"*
> → **973 runs** across 16 innings, with an average of 81.08 and a strike rate of 152.03

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React + Vite |
| Backend | FastAPI + LangGraph |
| Analytics DB | DuckDB + Parquet |
| App Metadata | PostgreSQL + SQLAlchemy |
| LLM (local) | Ollama (llama3.1) |
| LLM (hosted) | OpenAI GPT-4o |
| ETL | Polars + httpx + Typer |

**Data source:** [Cricsheet](https://cricsheet.org) ball-by-ball JSON

---

## Quick Start

```bash
# 1. Install Python dependencies
python -m venv .venv && .venv\Scripts\activate
pip install -e ".[dev]"

# 2. Configure environment
copy .env.example .env   # then edit .env

# 3. Start PostgreSQL
docker compose up postgres -d

# 4. Download and ingest all data (~500MB)
python -m etl.pipeline run --full

# 5. Start backend
uvicorn backend.api.main:app --reload
# API docs: http://localhost:8000/docs
```

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for the full system design, schema reference, agent flow, and development guide.

---

## Project Structure

```
cricket-eda/
├── backend/       FastAPI app, LangGraph agent, DB clients
├── etl/           Download → Parse → Transform → Load pipeline
├── data/          raw/ · processed/ · duckdb/ (gitignored)
├── frontend/      React + Vite chat UI
├── docker/        Dockerfiles
└── ARCHITECTURE.md
```

---

## Roadmap

- [x] IPL, T20I, ODI, Test (men's)
- [ ] Women's cricket
- [ ] Domestic cricket (Ranji, Sheffield Shield, etc.)
- [ ] Wikipedia MCP enrichment
- [ ] Charts and data visualization in chat
