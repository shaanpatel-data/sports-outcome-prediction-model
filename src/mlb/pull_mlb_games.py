"""
Pull MLB game schedules from the MLB Stats API.

Features:
- automatic retry/backoff if MLB API stalls
- caching of raw responses
- filters to REGULAR SEASON games only
- saves clean CSV for modeling
"""

from pathlib import Path
from datetime import datetime
import time
import requests
import pandas as pd

from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.utils_cache import load_json, save_json


MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"


def fetch_schedule(start_date, end_date):

    cache_path = (
        Path(RAW_DATA_DIR)
        / "mlb"
        / f"schedule_{start_date}_{end_date}.json"
    )

    cached = load_json(cache_path)
    if cached:
        return cached

    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "probablePitcher,team,venue",
    }

    retries = 5

    for attempt in range(retries):

        try:
            r = requests.get(
                MLB_SCHEDULE_URL,
                params=params,
                timeout=90
            )

            r.raise_for_status()

            data = r.json()

            save_json(data, cache_path)

            return data

        except Exception as e:

            if attempt == retries - 1:
                raise e

            wait = 2 ** attempt
            print(f"API timeout... retrying in {wait}s")
            time.sleep(wait)


def parse_games(schedule_json):

    rows = []

    for date_block in schedule_json.get("dates", []):

        game_date = date_block["date"]

        for g in date_block.get("games", []):

            game_type = g.get("gameType")

            # keep ONLY regular season
            if game_type != "R":
                continue

            teams = g.get("teams", {})

            away = teams.get("away", {}).get("team", {})
            home = teams.get("home", {}).get("team", {})

            rows.append(
                {
                    "game_date": game_date,
                    "game_pk": g.get("gamePk"),
                    "game_type": game_type,
                    "status": g.get("status", {}).get("detailedState"),
                    "venue": g.get("venue", {}).get("name"),
                    "game_datetime_utc": g.get("gameDate"),
                    "away_team": away.get("name"),
                    "home_team": home.get("name"),
                    "away_team_id": away.get("id"),
                    "home_team_id": home.get("id"),
                    "away_probable_pitcher": (
                        g.get("probablePitchers", {})
                        .get("away", {})
                        .get("fullName")
                    ),
                    "home_probable_pitcher": (
                        g.get("probablePitchers", {})
                        .get("home", {})
                        .get("fullName")
                    ),
                }
            )

    df = pd.DataFrame(rows)

    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["game_datetime_utc"] = pd.to_datetime(df["game_datetime_utc"])

    return df


def main():

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    args = parser.parse_args()

    schedule = fetch_schedule(args.start, args.end)

    games = parse_games(schedule)

    out_path = (
        Path(PROCESSED_DATA_DIR)
        / f"mlb_games_{args.start.replace('-','')}_{args.end.replace('-','')}.csv"
    )

    games.to_csv(out_path, index=False)

    print(f"\nSaved {len(games):,} games -> {out_path}\n")

    print(games.head())


if __name__ == "__main__":
    main()