"""
SQLite persistence for account <-> bot_id mappings.
"""
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import DB_PATH


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS account_bot_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_name TEXT NOT NULL,
            account_id TEXT,
            opportunity_name TEXT,
            opportunity_id TEXT,
            close_date TEXT,
            amount REAL,
            sf_url TEXT,
            bot_id TEXT,
            mapped_at TEXT,
            UNIQUE(opportunity_id)
        )
    """)
    conn.commit()
    return conn


def upsert_accounts(accounts: List[Dict]):
    """Insert or update Salesforce accounts into the mapping table."""
    conn = _get_conn()
    for acct in accounts:
        conn.execute("""
            INSERT INTO account_bot_mappings
                (account_name, account_id, opportunity_name, opportunity_id,
                 close_date, amount, sf_url, bot_id, mapped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(opportunity_id) DO UPDATE SET
                account_name = excluded.account_name,
                account_id = excluded.account_id,
                opportunity_name = excluded.opportunity_name,
                close_date = excluded.close_date,
                amount = excluded.amount,
                sf_url = excluded.sf_url
        """, (
            acct.get("account_name"),
            acct.get("account_id"),
            acct.get("opportunity_name"),
            acct.get("opportunity_id"),
            acct.get("close_date"),
            acct.get("amount"),
            acct.get("sf_url"),
            acct.get("bot_id"),
        ))
    conn.commit()
    conn.close()


def save_mapping(opportunity_id: str, bot_id: str):
    """Save a bot_id mapping for an opportunity."""
    conn = _get_conn()
    conn.execute("""
        UPDATE account_bot_mappings
        SET bot_id = ?, mapped_at = datetime('now')
        WHERE opportunity_id = ?
    """, (bot_id, opportunity_id))
    conn.commit()
    conn.close()


def clear_mapping(opportunity_id: str):
    """Remove a bot_id mapping for an opportunity."""
    conn = _get_conn()
    conn.execute("""
        UPDATE account_bot_mappings
        SET bot_id = NULL, mapped_at = NULL
        WHERE opportunity_id = ?
    """, (opportunity_id,))
    conn.commit()
    conn.close()


def get_all_mappings() -> List[Dict]:
    """Return all rows from the mapping table."""
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM account_bot_mappings ORDER BY close_date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_mapped() -> List[Dict]:
    """Return only rows that have a bot_id assigned."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM account_bot_mappings WHERE bot_id IS NOT NULL ORDER BY close_date DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_unmapped() -> List[Dict]:
    """Return rows without a bot_id."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM account_bot_mappings WHERE bot_id IS NULL ORDER BY close_date DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
