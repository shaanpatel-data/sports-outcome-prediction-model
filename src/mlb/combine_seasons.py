from pathlib import Path
import re
import pandas as pd


def main():
    ROOT_DIR = Path(__file__).resolve().parents[2]
    data_dir = ROOT_DIR / "data" / "processed"

    # only keep files that look like one season:
    # mlb_games_YYYYMMDD_YYYYMMDD.csv
    season_pattern = re.compile(r"mlb_games_\d{8}_\d{8}\.csv$")

    files = sorted(
        f for f in data_dir.glob("mlb_games_*.csv")
        if season_pattern.match(f.name)
    )

    if not files:
        raise FileNotFoundError(f"No season files found in {data_dir}")

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset=["game_pk"])

    out_path = data_dir / "mlb_games_2021_2025.csv"
    combined.to_csv(out_path, index=False)

    print(f"\nCombined dataset saved -> {out_path}")
    print(f"Total games: {len(combined):,}")
    print(f"Files combined: {len(files)}")


if __name__ == "__main__":
    main()