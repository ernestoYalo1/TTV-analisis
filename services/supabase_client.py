"""
Lightweight Supabase client for TTV Analysis.
Singleton pattern — one connection per process.
Falls back gracefully when SUPABASE_URL is not set (local dev uses SQLite).
"""
import os
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_client = None
_available = None  # tri-state: None = not checked, True/False


def is_available() -> bool:
    """Check if Supabase is configured and reachable."""
    global _available
    if _available is not None:
        return _available

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        _available = False
        logger.info("Supabase not configured (no SUPABASE_URL/KEY). Using SQLite fallback.")
        return False

    try:
        _get_client()
        _available = True
    except Exception as e:
        logger.warning(f"Supabase connection failed: {e}. Using SQLite fallback.")
        _available = False
    return _available


def _get_client():
    """Lazy-init the Supabase client singleton."""
    global _client
    if _client is not None:
        return _client

    from supabase import create_client, Client
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    _client = create_client(url, key)
    return _client


def _retry(operation, retries=3, delay=2):
    """Retry an operation with exponential backoff."""
    for attempt in range(retries):
        try:
            return operation()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay * (attempt + 1))


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------

def upsert(table: str, data: Dict, on_conflict: str = "") -> Optional[Dict]:
    """Insert or update a single row."""
    client = _get_client()
    q = client.table(table).upsert(data, on_conflict=on_conflict)
    result = _retry(lambda: q.execute())
    return result.data[0] if result.data else None


def upsert_many(table: str, rows: List[Dict], on_conflict: str = "") -> int:
    """Batch upsert. Returns count of rows processed."""
    if not rows:
        return 0
    client = _get_client()
    batch_size = 100
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        q = client.table(table).upsert(batch, on_conflict=on_conflict)
        _retry(lambda: q.execute())
        total += len(batch)
    return total


def select(table: str, columns: str = "*", filters: Optional[Dict] = None,
           order_by: Optional[str] = None, desc: bool = True,
           limit: Optional[int] = None) -> List[Dict]:
    """Select rows with optional filters, ordering, and limit."""
    client = _get_client()
    q = client.table(table).select(columns)
    if filters:
        for col, val in filters.items():
            if val is None:
                q = q.is_(col, "null")
            else:
                q = q.eq(col, val)
    if order_by:
        q = q.order(order_by, desc=desc)
    if limit:
        q = q.limit(limit)
    result = _retry(lambda: q.execute())
    return result.data if result.data else []


def select_not_null(table: str, column: str, columns: str = "*",
                    order_by: Optional[str] = None, desc: bool = True) -> List[Dict]:
    """Select rows where a column IS NOT NULL."""
    client = _get_client()
    q = client.table(table).select(columns).neq(column, None)
    if order_by:
        q = q.order(order_by, desc=desc)
    result = _retry(lambda: q.execute())
    return result.data if result.data else []


def select_is_null(table: str, column: str, columns: str = "*",
                   order_by: Optional[str] = None, desc: bool = True) -> List[Dict]:
    """Select rows where a column IS NULL."""
    client = _get_client()
    q = client.table(table).select(columns).is_(column, "null")
    if order_by:
        q = q.order(order_by, desc=desc)
    result = _retry(lambda: q.execute())
    return result.data if result.data else []


def update(table: str, data: Dict, match_col: str, match_val: Any) -> Optional[Dict]:
    """Update rows matching a condition."""
    client = _get_client()
    q = client.table(table).update(data).eq(match_col, match_val)
    result = _retry(lambda: q.execute())
    return result.data[0] if result.data else None


def delete(table: str, match_col: str, match_val: Any) -> bool:
    """Delete rows matching a condition."""
    client = _get_client()
    q = client.table(table).delete().eq(match_col, match_val)
    _retry(lambda: q.execute())
    return True
