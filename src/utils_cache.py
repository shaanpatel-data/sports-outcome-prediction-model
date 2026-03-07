import json
from pathlib import Path


def save_json(data, path):
    """
    Save JSON data to disk.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_json(path):
    """
    Load JSON from disk if it exists.
    """
    path = Path(path)

    if not path.exists():
        return None

    with open(path, "r") as f:
        return json.load(f)