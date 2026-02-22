"""ETL Load — load Parquet files into DuckDB."""

from pathlib import Path

import duckdb


def load_parquets_to_duckdb(
    processed_dir: Path,
    duckdb_path: Path,
    competitions: list[str],
) -> None:
    """Load all processed Parquet files into DuckDB tables.

    Uses INSERT OR REPLACE to handle idempotent loads.
    """
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))

    # Load players registry
    players_parquet = processed_dir / "players.parquet"
    if players_parquet.exists():
        conn.execute(f"""
            INSERT OR REPLACE INTO players
            SELECT
                identifier   AS player_key,
                name         AS full_name,
                unique_name,
                NULL         AS batting_style,
                NULL         AS bowling_style,
                NULL         AS nationality
            FROM read_parquet('{players_parquet.as_posix()}')
        """)
        print(f"  ✅ Players loaded from {players_parquet.name}")

    # Load match and delivery data per competition
    for comp in competitions:
        comp_dir = processed_dir / comp

        matches_parquet = comp_dir / "matches.parquet"
        deliveries_parquet = comp_dir / "deliveries.parquet"

        if matches_parquet.exists():
            conn.execute(f"""
                INSERT OR REPLACE INTO matches
                SELECT * FROM read_parquet('{matches_parquet.as_posix()}')
            """)
            count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
            print(f"  ✅ {comp} matches loaded → total {count} matches in DuckDB")

        if deliveries_parquet.exists():
            conn.execute(f"""
                INSERT OR REPLACE INTO deliveries
                SELECT * FROM read_parquet('{deliveries_parquet.as_posix()}')
            """)
            count = conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
            print(f"  ✅ {comp} deliveries loaded → total {count} deliveries in DuckDB")

    conn.close()
