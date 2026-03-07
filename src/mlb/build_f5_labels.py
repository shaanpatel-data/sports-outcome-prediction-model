from pathlib import Path
import time
import requests
import pandas as pd
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session():
    session = requests.Session()
    retries = Retry(
        total=8,
        connect=8,
        read=8,
        status=8,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def game_feed_url(game_pk):
    return f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"


def load_cached_json(path):
    if path.exists():
        return pd.read_json(path, typ="series").to_dict()
    return None


def save_cached_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.Series(data).to_json(path)


def runs_through_5(feed):
    innings = (
        feed.get("liveData", {})
        .get("linescore", {})
        .get("innings", [])
    )

    if len(innings) < 5:
        return None, None

    away = 0
    home = 0

    for i in range(5):
        away += innings[i].get("away", {}).get("runs", 0) or 0
        home += innings[i].get("home", {}).get("runs", 0) or 0

    return away, home


def fetch_game_feed(game_pk, raw_dir, session):
    cache_path = raw_dir / f"{game_pk}.json"

    cached = load_cached_json(cache_path)
    if cached is not None:
        return cached

    url = game_feed_url(game_pk)

    try:
        r = session.get(url, timeout=90)
        r.raise_for_status()
        data = r.json()
        save_cached_json(data, cache_path)
        return data
    except Exception as e:
        print(f"Failed for game_pk={game_pk}: {e}")
        return None


def main():
    ROOT_DIR = Path(__file__).resolve().parents[2]
    processed_dir = ROOT_DIR / "data" / "processed"
    raw_dir = ROOT_DIR / "data" / "raw" / "mlb" / "game_feed"
    raw_dir.mkdir(parents=True, exist_ok=True)

    games_path = processed_dir / "mlb_games_2021_2025.csv"
    if not games_path.exists():
        raise FileNotFoundError(f"Missing {games_path}")

    games = pd.read_csv(games_path)

    session = make_session()
    rows = []

    for game_pk in tqdm(games["game_pk"].dropna().astype(int).unique(), desc="Fetching game feeds"):
        feed = fetch_game_feed(game_pk, raw_dir, session)
        if feed is None:
            continue

        away5, home5 = runs_through_5(feed)
        if away5 is None or home5 is None:
            continue

        rows.append(
            {
                "game_pk": game_pk,
                "away_runs_through5": away5,
                "home_runs_through5": home5,
                "home_leading_after5": int(home5 > away5),
                "tied_after5": int(home5 == away5),
            }
        )

        time.sleep(0.05)  # gentle pacing

    df = pd.DataFrame(rows)

    out_path = processed_dir / "mlb_f5_labels_2021_2025.csv"
    df.to_csv(out_path, index=False)

    print(f"\nSaved {len(df):,} labeled games -> {out_path}")
    if not df.empty:
        print(df.head())


if __name__ == "__main__":
    main()