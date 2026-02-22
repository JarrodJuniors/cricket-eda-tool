"""
DuckDB Connection Manager
=========================
Provides a singleton DuckDB connection that persists for the application's
lifetime. DuckDB is used as the analytical engine — it reads Parquet files
from data/processed/ and exposes them via structured SQL tables.

Why singleton?
    DuckDB supports multiple readers but only one writer at a time.
    Reusing a single connection avoids file locking conflicts and
    eliminates per-request connection overhead.
"""

from functools import lru_cache
from pathlib import Path

import duckdb

from backend.config import get_settings

settings = get_settings()


@lru_cache(maxsize=1)
def get_duckdb() -> duckdb.DuckDBPyConnection:
    """
    Return the singleton DuckDB connection.

    - Creates the database file and parent directories if they don't exist.
    - Calls _init_schema() to ensure all core tables are present.
    - Cached via lru_cache so only one connection is ever created.
    """
    db_path = Path(settings.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    _init_schema(conn)
    return conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create core DuckDB tables if they don't already exist.

    These tables are populated by the ETL pipeline (etl/load.py).
    Schema is kept minimal here — all aggregation is done at query time,
    following the Cricsheet principle that raw data has no pre-computed stats.

    Tables:
        matches    — One row per match (metadata, teams, result)
        deliveries — One row per ball (primary fact table for all aggregations)
        players    — Canonical player registry sourced from Cricsheet people.csv
        teams      — Canonical team names (handles franchise rebrandings)
    """
    # ── matches ───────────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id        VARCHAR PRIMARY KEY,
            competition     VARCHAR,        -- e.g. "IPL", "T20I", "ODI", "TEST"
            season          VARCHAR,        -- e.g. "2016/17" (Cricsheet format)
            date            DATE,           -- First day of the match
            venue           VARCHAR,
            city            VARCHAR,
            team1           VARCHAR,        -- Teams in alphabetical order per Cricsheet
            team2           VARCHAR,
            toss_winner     VARCHAR,
            toss_decision   VARCHAR,        -- "bat" | "field"
            winner          VARCHAR,        -- Team name, or e.g. "no result"
            win_by_runs     INTEGER,        -- Set when batting team wins
            win_by_wickets  INTEGER,        -- Set when chasing team wins
            player_of_match VARCHAR,        -- Comma-separated if multiple
            umpire1         VARCHAR,
            umpire2         VARCHAR,
            source_file     VARCHAR         -- Original JSON filename for traceability
        )
    """)

    # ── deliveries ────────────────────────────────────────────────────────────
    # Primary fact table — every ball bowled in every match.
    # Joins to matches via match_id for competition/season context.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id     VARCHAR PRIMARY KEY,  -- Composite: match_id_inning_over_ball
            match_id        VARCHAR,              -- FK → matches.match_id
            inning          INTEGER,              -- 1 or 2 (T20/ODI) | 1–4 (Test)
            batting_team    VARCHAR,
            bowling_team    VARCHAR,
            over            INTEGER,              -- 0-indexed (over 0 = first over)
            ball            INTEGER,              -- Ball index within over (1-indexed)
            batter          VARCHAR,              -- Name as in Cricsheet
            bowler          VARCHAR,
            non_striker     VARCHAR,
            runs_batter     INTEGER,              -- Runs credited to batter
            runs_extras     INTEGER,              -- Wide/no-ball/bye runs
            runs_total      INTEGER,              -- runs_batter + runs_extras
            extras_type     VARCHAR,              -- "wides","noballs","byes","legbyes"
            is_wicket       BOOLEAN,              -- True if a wicket fell on this ball
            wicket_kind     VARCHAR,              -- "caught","bowled","lbw","run out",etc.
            player_out      VARCHAR,              -- Name of dismissed batter
            fielder         VARCHAR               -- Fielder involved (catches/run outs)
        )
    """)

    # ── players ───────────────────────────────────────────────────────────────
    # Sourced from Cricsheet people.csv. Provides stable unique keys for each
    # player, solving the name ambiguity problem (e.g. "R Sharma" is ambiguous).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_key      VARCHAR PRIMARY KEY,  -- Cricsheet stable identifier
            full_name       VARCHAR,              -- Display name (e.g. "Virat Kohli")
            unique_name     VARCHAR,              -- Disambiguated name (e.g. "V Kohli")
            batting_style   VARCHAR,              -- Populated in future ETL phase
            bowling_style   VARCHAR,
            nationality     VARCHAR
        )
    """)

    # ── teams ─────────────────────────────────────────────────────────────────
    # Maps all historical team names to their current canonical name.
    # Example: "Delhi Daredevils" → "Delhi Capitals"
    conn.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_name       VARCHAR PRIMARY KEY,  -- Name as it appears in Cricsheet
            canonical_name  VARCHAR,              -- Current/preferred brand name
            competition     VARCHAR,
            country         VARCHAR
        )
    """)
