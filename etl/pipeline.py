"""ETL Pipeline CLI — orchestrates full and incremental data loads."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from etl.download import download_competition, download_people
from etl.parse import parse_competition_dir, parse_people_csv
from etl.transform import write_parquets, write_players_parquet
from etl.load import load_parquets_to_duckdb

app = typer.Typer(name="cricket-etl", help="Cricket EDA ETL pipeline CLI")
console = Console()

COMPETITIONS = ["ipl", "t20i", "odi", "test"]


@app.command()
def run(
    competitions: list[str] = typer.Option(COMPETITIONS, "--competition", "-c", help="Competition(s) to load"),
    full: bool = typer.Option(False, "--full", help="Force full re-download and re-ingest"),
    raw_dir: str = typer.Option("data/raw", "--raw-dir"),
    processed_dir: str = typer.Option("data/processed", "--processed-dir"),
    duckdb_path: str = typer.Option("data/duckdb/cricket.duckdb", "--duckdb"),
):
    """Run the Cricket EDA ETL pipeline (download → parse → transform → load)."""
    raw = Path(raw_dir)
    processed = Path(processed_dir)
    duckdb = Path(duckdb_path)

    console.rule("[bold green]Cricket EDA ETL Pipeline")

    # Step 1: Download
    console.print("\n[bold]Step 1: Download[/]")
    download_people(raw)
    for comp in competitions:
        download_competition(comp, raw, force=full)

    # Step 2: Parse
    console.print("\n[bold]Step 2: Parse[/]")
    people_df = parse_people_csv(raw / "people.csv")
    all_matches, all_deliveries = {}, {}
    for comp in competitions:
        matches, deliveries = parse_competition_dir(raw / comp, comp)
        all_matches[comp] = matches
        all_deliveries[comp] = deliveries

    # Step 3: Transform → Parquet
    console.print("\n[bold]Step 3: Transform → Parquet[/]")
    write_players_parquet(people_df, processed)
    for comp in competitions:
        write_parquets(all_matches[comp], all_deliveries[comp], processed, comp)

    # Step 4: Load → DuckDB
    console.print("\n[bold]Step 4: Load → DuckDB[/]")
    load_parquets_to_duckdb(processed, duckdb, competitions)

    console.rule("[bold green]✅ Pipeline Complete")


if __name__ == "__main__":
    app()
