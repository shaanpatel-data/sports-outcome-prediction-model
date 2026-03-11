"""
download_games.py
-----------------

This script downloads historical and current MLB game data used for the First Five Innings (F5) model. It retrieves data from publicly available MLB APIs and stores it in the data/raw directory for further processing.

Usage:
    python scripts/download_games.py

Note:
    The script will read any required API keys from your environment (e.g., an ODDS_API_KEY defined in your .env file). Ensure you have created a .env file locally and have the necessary credentials before running this script.
"""
