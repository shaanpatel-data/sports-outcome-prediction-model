from pathlib import Path
import pandas as pd


def main():

    ROOT_DIR = Path(__file__).resolve().parents[2]

    processed_dir = ROOT_DIR / "data" / "processed"

    data_path = processed_dir / "mlb_model_dataset_2021_2025.csv"

    df = pd.read_csv(data_path)

    df["game_date"] = pd.to_datetime(df["game_date"])

    df = df.sort_values("game_date")

    # Away team rolling runs scored
    df["away_runs_last10"] = (
        df.groupby("away_team")["away_runs_through5"]
        .rolling(10)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Home team rolling runs scored
    df["home_runs_last10"] = (
        df.groupby("home_team")["home_runs_through5"]
        .rolling(10)
        .mean()
        .reset_index(level=0, drop=True)
    )

    out_path = processed_dir / "mlb_model_dataset_features_2021_2025.csv"

    df.to_csv(out_path, index=False)

    print("\nFeature dataset saved ->", out_path)

    print("\nColumns:", len(df.columns))

    print(df.head())


if __name__ == "__main__":
    main()