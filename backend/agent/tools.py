"""
LangGraph Agent Tools
=====================
These tools are made available to the LLM via LangChain's tool-calling interface.
The LangGraph agent can call any of these tools at any point during its reasoning loop.

Tools defined here:
    get_schema       — Returns DuckDB table schema so the LLM knows what to query
    run_sql          — Executes a SQL SELECT against DuckDB (read-only, safe)
    lookup_player    — Resolves ambiguous player names to canonical registry entries

Security note:
    run_sql only permits SELECT statements, preventing any data mutation.
    All inputs go through DuckDB's parameterised query interface.
"""

import json
from typing import Any

from langchain_core.tools import tool

from backend.db.duckdb_client import get_duckdb


@tool
def get_schema() -> str:
    """
    Return the DuckDB schema (table names + column definitions) as plain text.

    The LLM calls this at the start of complex queries to understand what
    tables and columns are available before writing SQL. Keeps the agent
    grounded in the actual data model rather than hallucinating column names.
    """
    conn = get_duckdb()

    # Fetch all table names in the database
    tables = conn.execute("SHOW TABLES").fetchall()

    schema_parts = []
    for (table_name,) in tables:
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        cols = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        col_defs = ", ".join(f"{c[1]} {c[2]}" for c in cols)
        schema_parts.append(f"{table_name}({col_defs})")

    return "\n".join(schema_parts)


@tool
def run_sql(sql: str) -> str:
    """
    Execute a read-only SQL query against DuckDB and return results as a JSON string.

    Results are capped at 200 rows to avoid overwhelming the LLM context window.
    Only SELECT statements are accepted — any other statement type returns an error.

    The LLM uses this as the primary data retrieval mechanism. Results are returned
    as a JSON array of row objects, which the LLM then interprets to form its answer.

    Args:
        sql: A SELECT SQL statement targeting the cricket DuckDB database.

    Returns:
        JSON string: list of row dicts, or {"error": "message"} on failure.
    """
    sql = sql.strip()

    # Safety guard: only allow read operations
    if not sql.upper().startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are permitted."})

    conn = get_duckdb()
    try:
        # Fetch as a Pandas DataFrame and convert to records
        result = conn.execute(sql).fetchdf()
        # Cap at 200 rows — LLM doesn't need (and can't effectively use) more
        records = result.head(200).to_dict(orient="records")
        # Use default=str to handle non-JSON-serialisable types (dates, Decimals, etc.)
        return json.dumps(records, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def lookup_player(name: str) -> str:
    """
    Search the players registry for a player matching the given name.

    WHY THIS MATTERS:
    Cricsheet stores player names inconsistently across competitions and eras
    (e.g. "Virat Kohli", "V Kohli", "V Kohli (2)"). The players table is
    sourced from Cricsheet's people.csv and provides stable canonical IDs.

    Always call this BEFORE querying deliveries for a specific player to:
    1. Confirm the player exists in the database
    2. Get the exact name string used in the deliveries table

    Args:
        name: Partial or full player name to search for (case-insensitive).

    Returns:
        JSON string: list of up to 5 matching player records.
    """
    conn = get_duckdb()
    try:
        result = conn.execute(
            """
            SELECT player_key, full_name, unique_name, nationality
            FROM players
            WHERE lower(full_name)   LIKE lower(?)
               OR lower(unique_name) LIKE lower(?)
            LIMIT 5
            """,
            [f"%{name}%", f"%{name}%"],
        ).fetchdf()

        if result.empty:
            return json.dumps({"message": f"No player found matching '{name}'. "
                                          "Try a shorter name or check spelling."})

        return result.to_json(orient="records")
    except Exception as e:
        return json.dumps({"error": str(e)})
