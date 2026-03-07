from pathlib import Path
import pandas as pd


def main():

    ROOT_DIR = Path(__file__).resolve().parents[2]

    processed_dir = ROOT_DIR / "data" / "processed"

    games_path = processed_dir / "mlb_games_2021_2025.csv"
    labels_path = processed_dir / "mlb_f5_labels_2021_2025.csv"

    games = pd.read_csv(games_path)
    labels = pd.read_csv(labels_path)

    df = games.merge(labels, on="game_pk", how="inner")

    out_path = processed_dir / "mlb_model_dataset_2021_2025.csv"

    df.to_csv(out_path, index=False)

    print("\nModel dataset saved ->", out_path)
    print("Rows:", len(df))
    print("Columns:", len(df.columns))
    print("\nPreview:")
    print(df.head())


if __name__ == "__main__":
    main()