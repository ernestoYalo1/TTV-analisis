"""
Persistence layer for account <-> bot_id mappings.
Uses Supabase when available (production), falls back to SQLite (local dev).
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import DB_PATH
from services import supabase_client as sb

TABLE = "ttv_account_mappings"


# ---------------------------------------------------------------------------
# Public API — delegates to Supabase or SQLite
# ---------------------------------------------------------------------------

def upsert_accounts(accounts: List[Dict]):
    """Insert or update Salesforce accounts."""
    if sb.is_available():
        rows = [_to_sb_row(a) for a in accounts]
        sb.upsert_many(TABLE, rows, on_conflict="opportunity_id")
    else:
        _sqlite_upsert_accounts(accounts)


def save_mapping(opportunity_id: str, bot_id: str):
    """Save a bot_id mapping for an opportunity."""
    if sb.is_available():
        sb.update(TABLE, {"bot_id": bot_id, "mapped_at": datetime.utcnow().isoformat()},
                  "opportunity_id", opportunity_id)
    else:
        _sqlite_save_mapping(opportunity_id, bot_id)


def clear_mapping(opportunity_id: str):
    """Remove a bot_id mapping for an opportunity."""
    if sb.is_available():
        sb.update(TABLE, {"bot_id": None, "mapped_at": None},
                  "opportunity_id", opportunity_id)
    else:
        _sqlite_clear_mapping(opportunity_id)


def get_all_mappings() -> List[Dict]:
    """Return all rows."""
    if sb.is_available():
        return sb.select(TABLE, order_by="close_date", desc=True)
    return _sqlite_get_all()


def get_mapped() -> List[Dict]:
    """Return only rows that have a bot_id assigned."""
    if sb.is_available():
        return sb.select_not_null(TABLE, "bot_id", order_by="close_date", desc=True)
    return _sqlite_get_mapped()


def get_unmapped() -> List[Dict]:
    """Return rows without a bot_id."""
    if sb.is_available():
        return sb.select_is_null(TABLE, "bot_id", order_by="close_date", desc=True)
    return _sqlite_get_unmapped()


# ---------------------------------------------------------------------------
# Supabase row conversion
# ---------------------------------------------------------------------------

def _to_sb_row(acct: Dict) -> Dict:
    return {
        "account_name": acct.get("account_name"),
        "account_id": acct.get("account_id"),
        "opportunity_name": acct.get("opportunity_name"),
        "opportunity_id": acct.get("opportunity_id"),
        "close_date": acct.get("close_date"),
        "amount": acct.get("amount") or 0,
        "sf_url": acct.get("sf_url"),
        "bot_id": acct.get("bot_id"),
    }


# ---------------------------------------------------------------------------
# SQLite fallback (local development)
# ---------------------------------------------------------------------------

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
            opportunity_id TEXT UNIQUE,
            close_date TEXT,
            amount REAL,
            sf_url TEXT,
            bot_id TEXT,
            mapped_at TEXT
        )
    """)
    conn.commit()
    return conn


def _sqlite_upsert_accounts(accounts: List[Dict]):
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
            acct.get("account_name"), acct.get("account_id"),
            acct.get("opportunity_name"), acct.get("opportunity_id"),
            acct.get("close_date"), acct.get("amount"),
            acct.get("sf_url"), acct.get("bot_id"),
        ))
    conn.commit()
    conn.close()


def _sqlite_save_mapping(opportunity_id: str, bot_id: str):
    conn = _get_conn()
    conn.execute("""
        UPDATE account_bot_mappings
        SET bot_id = ?, mapped_at = datetime('now')
        WHERE opportunity_id = ?
    """, (bot_id, opportunity_id))
    conn.commit()
    conn.close()


def _sqlite_clear_mapping(opportunity_id: str):
    conn = _get_conn()
    conn.execute("""
        UPDATE account_bot_mappings
        SET bot_id = NULL, mapped_at = NULL
        WHERE opportunity_id = ?
    """, (opportunity_id,))
    conn.commit()
    conn.close()


def _sqlite_get_all() -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM account_bot_mappings ORDER BY close_date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _sqlite_get_mapped() -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM account_bot_mappings WHERE bot_id IS NOT NULL ORDER BY close_date DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _sqlite_get_unmapped() -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM account_bot_mappings WHERE bot_id IS NULL ORDER BY close_date DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
