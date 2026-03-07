from pathlib import Path
import json
import pandas as pd


def extract_starting_pitchers(feed):
    """
    MLB game feed stores pitcher IDs in liveData.boxscore.teams.{side}.pitchers
    The first pitcher listed is the starter.
    """
    try:
        away_pitcher_ids = feed["liveData"]["boxscore"]["teams"]["away"]["pitchers"]
        home_pitcher_ids = feed["liveData"]["boxscore"]["teams"]["home"]["pitchers"]

        away_starter_id = away_pitcher_ids[0] if away_pitcher_ids else None
        home_starter_id = home_pitcher_ids[0] if home_pitcher_ids else None

        players = feed["gameData"]["players"]

        away_name = None
        home_name = None

        if away_starter_id is not None:
            away_name = players.get(f"ID{away_starter_id}", {}).get("fullName")

        if home_starter_id is not None:
            home_name = players.get(f"ID{home_starter_id}", {}).get("fullName")

        return away_name, home_name

    except Exception:
        return None, None


def main():
    ROOT_DIR = Path(__file__).resolve().parents[2]
    processed_dir = ROOT_DIR / "data" / "processed"
    raw_feed_dir = ROOT_DIR / "data" / "raw" / "mlb" / "game_feed"

    data_path = processed_dir / "mlb_model_dataset_features_2021_2025.csv"
    df = pd.read_csv(data_path)

    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values("game_date").reset_index(drop=True)

    # Build actual starter names from cached game feeds
    away_actual = []
    home_actual = []

    for game_pk in df["game_pk"]:
        feed_path = raw_feed_dir / f"{int(game_pk)}.json"

        if not feed_path.exists():
            away_actual.append(None)
            home_actual.append(None)
            continue

        try:
            with open(feed_path, "r", encoding="utf-8") as f:
                feed = json.load(f)

            away_name, home_name = extract_starting_pitchers(feed)

            away_actual.append(away_name)
            home_actual.append(home_name)

        except Exception:
            away_actual.append(None)
            home_actual.append(None)

    df["away_starting_pitcher"] = away_actual
    df["home_starting_pitcher"] = home_actual

    # Build long-form pitcher appearance table
    away_df = df[
        ["game_pk", "game_date", "away_starting_pitcher", "home_runs_through5"]
    ].copy()
    away_df = away_df.rename(
        columns={
            "away_starting_pitcher": "pitcher_name",
            "home_runs_through5": "runs_allowed_through5",
        }
    )
    away_df["side"] = "away"

    home_df = df[
        ["game_pk", "game_date", "home_starting_pitcher", "away_runs_through5"]
    ].copy()
    home_df = home_df.rename(
        columns={
            "home_starting_pitcher": "pitcher_name",
            "away_runs_through5": "runs_allowed_through5",
        }
    )
    home_df["side"] = "home"

    pitcher_df = pd.concat([away_df, home_df], ignore_index=True)
    pitcher_df = pitcher_df.dropna(subset=["pitcher_name"])
    pitcher_df = pitcher_df.sort_values(["pitcher_name", "game_date"]).reset_index(drop=True)

    pitcher_df["pitcher_last5_runs_allowed"] = (
        pitcher_df.groupby("pitcher_name")["runs_allowed_through5"]
        .transform(lambda s: s.shift(1).rolling(5, min_periods=1).mean())
    )

    away_features = pitcher_df[pitcher_df["side"] == "away"][
        ["game_pk", "pitcher_name", "pitcher_last5_runs_allowed"]
    ].copy()
    away_features = away_features.rename(
        columns={
            "pitcher_name": "away_starting_pitcher",
            "pitcher_last5_runs_allowed": "away_pitcher_last5_runs_allowed",
        }
    )

    home_features = pitcher_df[pitcher_df["side"] == "home"][
        ["game_pk", "pitcher_name", "pitcher_last5_runs_allowed"]
    ].copy()
    home_features = home_features.rename(
        columns={
            "pitcher_name": "home_starting_pitcher",
            "pitcher_last5_runs_allowed": "home_pitcher_last5_runs_allowed",
        }
    )

    df = df.merge(
        away_features,
        on=["game_pk", "away_starting_pitcher"],
        how="left",
    )

    df = df.merge(
        home_features,
        on=["game_pk", "home_starting_pitcher"],
        how="left",
    )

    out_path = processed_dir / "mlb_model_dataset_features_v2_2021_2025.csv"
    df.to_csv(out_path, index=False)

    print("\nPitcher feature dataset saved ->", out_path)
    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    print("\nNon-null counts:")
    print("away_starting_pitcher:", df["away_starting_pitcher"].notna().sum())
    print("home_starting_pitcher:", df["home_starting_pitcher"].notna().sum())
    print("away_pitcher_last5_runs_allowed:", df["away_pitcher_last5_runs_allowed"].notna().sum())
    print("home_pitcher_last5_runs_allowed:", df["home_pitcher_last5_runs_allowed"].notna().sum())


if __name__ == "__main__":
    main()