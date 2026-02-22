"""
ETL Parse Module
================
Parses Cricsheet JSON match files into Python dictionaries ready
for conversion to Parquet by the transform module.

Cricsheet JSON structure (simplified):
    {
      "info": {
        "teams": ["India", "Australia"],
        "dates": ["2023-11-19"],
        "venue": "Narendra Modi Stadium",
        "season": "2023/24",
        "toss": {"winner": "India", "decision": "bat"},
        "outcome": {"winner": "India", "by": {"runs": 6}},
        "player_of_match": ["V Kohli"],
        "officials": {"umpires": ["Nitin Menon", "Richard Kettleborough"]}
      },
      "innings": [
        {
          "team": "India",
          "overs": [
            {
              "over": 0,
              "deliveries": [
                {
                  "batter": "RG Sharma",
                  "bowler": "MA Starc",
                  "non_striker": "V Kohli",
                  "runs": {"batter": 4, "extras": 0, "total": 4},
                  "wickets": []
                },
                ...
              ]
            }
          ]
        }
      ]
    }

Key design decision:
    We use JSON (not YAML) format from Cricsheet for performance —
    Python's built-in `json` module is significantly faster than PyYAML.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def parse_match_file(json_path: Path, competition: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Parse a single Cricsheet JSON file into a match record and delivery records.

    The match record contains match-level metadata (teams, venue, result, etc.).
    The delivery records contain one dict per ball (the primary fact table).

    Args:
        json_path:   Full path to a Cricsheet .json match file.
        competition: Competition label to embed in the match record (e.g. "ipl").

    Returns:
        Tuple of (match_record dict, list of delivery record dicts).
    """
    with open(json_path, encoding="utf-8") as f:
        raw = json.load(f)

    info = raw.get("info", {})
    innings_data = raw.get("innings", [])

    # The match ID is the Cricsheet filename stem (a unique integer ID)
    match_id = json_path.stem

    # ── Build the match-level record ──────────────────────────────────────────
    teams = info.get("teams", [])
    dates = info.get("dates", [])
    outcome = info.get("outcome", {})
    toss = info.get("toss", {})

    match_record: dict[str, Any] = {
        "match_id":        match_id,
        "competition":     competition.upper(),          # Normalise to uppercase
        "season":          str(info.get("season", "")),
        "date":            dates[0] if dates else None,  # Use first date (multi-day matches)
        "venue":           info.get("venue", ""),
        "city":            info.get("city", ""),
        "team1":           teams[0] if len(teams) > 0 else "",
        "team2":           teams[1] if len(teams) > 1 else "",
        "toss_winner":     toss.get("winner", ""),
        "toss_decision":   toss.get("decision", ""),    # "bat" or "field"
        # outcome.winner is the team name; outcome.result handles "no result" etc.
        "winner":          outcome.get("winner", outcome.get("result", "")),
        "win_by_runs":     outcome.get("by", {}).get("runs"),
        "win_by_wickets":  outcome.get("by", {}).get("wickets"),
        # player_of_match is a list — join to string as there can be co-winners
        "player_of_match": ", ".join(info.get("player_of_match", [])),
        "umpire1":         info.get("officials", {}).get("umpires", [""])[0],
        "umpire2":         (info.get("officials", {}).get("umpires", [None, ""])[1]
                           if len(info.get("officials", {}).get("umpires", [])) > 1 else ""),
        "source_file":     json_path.name,
    }

    # ── Build delivery records (one per ball) ─────────────────────────────────
    delivery_records: list[dict[str, Any]] = []

    for inning_idx, inning in enumerate(innings_data, start=1):
        batting_team = inning.get("team", "")
        # The bowling team is the other team in the match
        bowling_team = next((t for t in teams if t != batting_team), "")
        overs = inning.get("overs", [])

        for over_data in overs:
            over_num = over_data.get("over", 0)  # 0-indexed per Cricsheet
            deliveries = over_data.get("deliveries", [])

            for ball_idx, delivery in enumerate(deliveries, start=1):
                runs = delivery.get("runs", {})
                wickets = delivery.get("wickets", [])   # List — usually 0 or 1 item
                extras = delivery.get("extras", {})

                # extras dict keys are the extras type (e.g. {"wides": 1})
                extras_type = next(iter(extras), None) if extras else None

                # Take the first wicket (multiple wickets per ball is extremely rare)
                wicket = wickets[0] if wickets else {}
                fielders = wicket.get("fielders", [])
                fielder = fielders[0].get("name", "") if fielders else ""

                # Composite primary key: unique across the entire database
                delivery_id = f"{match_id}_{inning_idx}_{over_num}_{ball_idx}"

                delivery_records.append({
                    "delivery_id":   delivery_id,
                    "match_id":      match_id,
                    "inning":        inning_idx,
                    "batting_team":  batting_team,
                    "bowling_team":  bowling_team,
                    "over":          over_num,
                    "ball":          ball_idx,
                    "batter":        delivery.get("batter", ""),
                    "bowler":        delivery.get("bowler", ""),
                    "non_striker":   delivery.get("non_striker", ""),
                    "runs_batter":   runs.get("batter", 0),
                    "runs_extras":   runs.get("extras", 0),
                    "runs_total":    runs.get("total", 0),
                    "extras_type":   extras_type,
                    "is_wicket":     len(wickets) > 0,
                    "wicket_kind":   wicket.get("kind", ""),
                    "player_out":    wicket.get("player_out", ""),
                    "fielder":       fielder,
                })

    return match_record, delivery_records


def parse_competition_dir(
    comp_dir: Path,
    competition: str,
    existing_match_ids: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Parse all JSON match files in a competition directory.

    For incremental loads, pass `existing_match_ids` to skip already-ingested
    matches. The ETL pipeline fetches these from PostgreSQL's sync_logs table.

    Args:
        comp_dir:           Directory containing Cricsheet JSON files.
        competition:        Competition label (e.g., "ipl").
        existing_match_ids: Set of match_id strings already in DuckDB.

    Returns:
        Tuple of (all_match_records, all_delivery_records) for new matches only.
    """
    existing_match_ids = existing_match_ids or set()
    all_matches, all_deliveries = [], []

    json_files = sorted(comp_dir.glob("*.json"))
    new_files = [f for f in json_files if f.stem not in existing_match_ids]
    print(f"  📂 {competition}: {len(json_files)} total files, {len(new_files)} new to parse")

    for f in new_files:
        try:
            match_rec, delivery_recs = parse_match_file(f, competition)
            all_matches.append(match_rec)
            all_deliveries.extend(delivery_recs)
        except Exception as e:
            # Log and continue — a single bad file shouldn't abort the pipeline
            print(f"  ⚠️  Skipping {f.name}: {e}")

    print(f"  ✅ {competition}: parsed {len(all_matches)} matches, {len(all_deliveries)} deliveries")
    return all_matches, all_deliveries


def parse_people_csv(people_csv: Path) -> pl.DataFrame:
    """
    Parse Cricsheet's people.csv into a Polars DataFrame for the players registry.

    people.csv provides stable unique identifiers for every player in Cricsheet,
    solving the name ambiguity problem (e.g. two players called "R Sharma").

    Cricsheet people.csv columns (as of 2024):
        identifier  — Stable unique key (used as player_key in our schema)
        name        — Full display name
        unique_name — Disambiguated name when full names clash
        ...         — Additional columns (batting/bowling style) planned

    Args:
        people_csv: Path to the downloaded people.csv file.

    Returns:
        Polars DataFrame with normalised column names.
    """
    df = pl.read_csv(people_csv)
    # Normalise column names: lowercase + replace spaces with underscores
    df = df.rename({c: c.lower().replace(" ", "_") for c in df.columns})
    return df
