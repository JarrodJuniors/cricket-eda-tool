"""ETL Transform — enrich and write data to Parquet files."""

from pathlib import Path

import polars as pl


def write_parquets(
    matches: list[dict],
    deliveries: list[dict],
    processed_dir: Path,
    competition: str,
) -> tuple[Path, Path]:
    """Convert parsed records to Polars DataFrames and write Parquet files.

    Returns:
        (matches_parquet_path, deliveries_parquet_path)
    """
    comp_dir = processed_dir / competition
    comp_dir.mkdir(parents=True, exist_ok=True)

    matches_path = comp_dir / "matches.parquet"
    deliveries_path = comp_dir / "deliveries.parquet"

    if not matches:
        print(f"  ⚠️  No new matches to write for {competition}")
        return matches_path, deliveries_path

    matches_df = pl.DataFrame(matches)
    deliveries_df = pl.DataFrame(deliveries)

    # Cast date column
    matches_df = matches_df.with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False)
    )

    # Append to existing Parquet if it exists
    if matches_path.exists():
        existing_matches = pl.read_parquet(matches_path)
        matches_df = pl.concat([existing_matches, matches_df]).unique(subset=["match_id"])

    if deliveries_path.exists():
        existing_deliveries = pl.read_parquet(deliveries_path)
        deliveries_df = pl.concat([existing_deliveries, deliveries_df]).unique(subset=["delivery_id"])

    matches_df.write_parquet(matches_path, compression="zstd")
    deliveries_df.write_parquet(deliveries_path, compression="zstd")

    print(f"  💾 {competition}: {len(matches_df)} matches, {len(deliveries_df)} deliveries written to Parquet")
    return matches_path, deliveries_path


def write_players_parquet(people_df: pl.DataFrame, processed_dir: Path) -> Path:
    """Write players registry to Parquet."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / "players.parquet"
    people_df.write_parquet(path, compression="zstd")
    print(f"  💾 Players registry: {len(people_df)} records written")
    return path
