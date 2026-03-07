from pathlib import Path
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss


def main():

    ROOT_DIR = Path(__file__).resolve().parents[2]
    processed_dir = ROOT_DIR / "data" / "processed"

    data_path = processed_dir / "mlb_model_dataset_features_v2_2021_2025.csv"

    df = pd.read_csv(data_path)

    df["game_date"] = pd.to_datetime(df["game_date"])

    df = df.dropna(
        subset=[
            "away_runs_last10",
            "home_runs_last10",
            "away_pitcher_last5_runs_allowed",
            "home_pitcher_last5_runs_allowed",
        ]
    )

    features = [
        "away_runs_last10",
        "home_runs_last10",
        "away_pitcher_last5_runs_allowed",
        "home_pitcher_last5_runs_allowed",
    ]

    # -------- Time based split --------

    split_date = "2024-01-01"

    train = df[df["game_date"] < split_date]
    test = df[df["game_date"] >= split_date]

    X_train = train[features]
    y_train = train["home_leading_after5"]

    X_test = test[features]
    y_test = test["home_leading_after5"]

    # -------- Train model --------

    model = LogisticRegression()

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    probs = model.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, preds)

    loss = log_loss(y_test, probs)

    print("\nModel Results")
    print("------------------")
    print("Accuracy:", round(accuracy, 4))
    print("Log Loss:", round(loss, 4))


if __name__ == "__main__":
    main()