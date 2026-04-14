"""
Configuration settings for TTV Analysis app.
"""
from pathlib import Path

# Paths
APP_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = APP_DIR / "data"
CLIENTS_DIR = APP_DIR / "api_clients"

# Salesforce config - reuse the existing config.json from clients/
SF_CONFIG_PATH = CLIENTS_DIR / "config.json"

# SQLite database for mappings
DB_PATH = DATA_DIR / "mappings.db"

# BigQuery
BQ_PROJECT = "arched-photon-194421"
BQ_CONVERSATIONS_TABLE = "arched-photon-194421.DWH2.conversation_journeys"

# TTV parameters
NEW_CUSTOMER_CUTOFF = "2026-01-01"
MILESTONES = [10, 50, 100]
