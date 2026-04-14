"""
Orchestrates the full TTV computation.
Pulls mapped accounts, queries BigQuery for milestones, caches results
in Supabase (or returns live), and builds the final TTV table.
"""
from datetime import datetime
from typing import Dict, List, Optional

from services.mapping_service import get_mapped
from services.bigquery_service import get_milestones, get_total_unique_contacts
from services import supabase_client as sb
from config.settings import MILESTONES

CACHE_TABLE = "ttv_milestones"


def compute_ttv_table(use_cache: bool = False) -> List[Dict]:
    """
    Build the Time-to-Value table for all mapped accounts.

    If use_cache=True and Supabase is available, returns cached milestones
    without querying BigQuery. Otherwise queries BQ live and caches results.
    """
    mapped = get_mapped()
    if not mapped:
        return []

    # Try cache first
    if use_cache and sb.is_available():
        cached = _load_cached_milestones(mapped)
        if cached:
            return cached

    # Live computation from BigQuery
    ttv_rows = []
    for acct in mapped:
        bot_id = acct["bot_id"]
        close_date = acct.get("close_date", "")

        milestones = get_milestones(bot_id, close_date) if close_date else {}
        total_contacts = get_total_unique_contacts(bot_id, close_date) if close_date else 0

        row = _build_row(acct, milestones, total_contacts)
        ttv_rows.append(row)

        # Cache to Supabase
        if sb.is_available():
            _cache_milestone(acct, row)

    return ttv_rows


def load_cached_ttv_table() -> List[Dict]:
    """Load TTV table from Supabase cache. Returns empty list if not cached."""
    mapped = get_mapped()
    if not mapped:
        return []
    if not sb.is_available():
        return []
    return _load_cached_milestones(mapped)


def compute_summary(ttv_rows: List[Dict]) -> Dict:
    """Compute summary metrics from the TTV table."""
    if not ttv_rows:
        return {"total_accounts": 0, "reached": {}, "avg_days": {}}

    total = len(ttv_rows)
    reached = {m: 0 for m in MILESTONES}
    avg_days = {m: None for m in MILESTONES}

    for m in MILESTONES:
        days_vals = [r[f"days_to_{m}"] for r in ttv_rows if r.get(f"days_to_{m}") is not None]
        reached[m] = len(days_vals)
        if days_vals:
            avg_days[m] = round(sum(days_vals) / len(days_vals), 1)

    return {
        "total_accounts": total,
        "reached": reached,
        "avg_days": avg_days,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_row(acct: Dict, milestones: Dict, total_contacts: int) -> Dict:
    close_date = acct.get("close_date", "")
    row = {
        "account_name": acct.get("account_name", ""),
        "opportunity_name": acct.get("opportunity_name", ""),
        "opportunity_id": acct.get("opportunity_id", ""),
        "close_date": close_date,
        "bot_id": acct["bot_id"],
        "total_unique_contacts": total_contacts,
    }

    base_dt = datetime.strptime(close_date, "%Y-%m-%d") if close_date else None

    for m in MILESTONES:
        date_val = milestones.get(m)
        row[f"date_{m}"] = date_val
        if date_val and base_dt:
            m_dt = datetime.strptime(date_val, "%Y-%m-%d")
            row[f"days_to_{m}"] = (m_dt - base_dt).days
        else:
            row[f"days_to_{m}"] = None

    return row


def _cache_milestone(acct: Dict, row: Dict):
    """Write a milestone row to Supabase cache."""
    data = {
        "opportunity_id": acct.get("opportunity_id"),
        "bot_id": acct["bot_id"],
        "total_unique_contacts": row.get("total_unique_contacts", 0),
        "date_10": row.get("date_10"),
        "days_to_10": row.get("days_to_10"),
        "date_50": row.get("date_50"),
        "days_to_50": row.get("days_to_50"),
        "date_100": row.get("date_100"),
        "days_to_100": row.get("days_to_100"),
        "computed_at": datetime.utcnow().isoformat(),
    }
    try:
        sb.upsert(CACHE_TABLE, data, on_conflict="opportunity_id")
    except Exception as e:
        # Non-fatal — cache miss is fine, BQ results are already in memory
        pass


def _load_cached_milestones(mapped: List[Dict]) -> List[Dict]:
    """Load milestone cache from Supabase and merge with mapping data."""
    cache_rows = sb.select(CACHE_TABLE)
    if not cache_rows:
        return []

    cache_by_opp = {r["opportunity_id"]: r for r in cache_rows}

    ttv_rows = []
    for acct in mapped:
        opp_id = acct.get("opportunity_id")
        cached = cache_by_opp.get(opp_id)
        if not cached:
            continue  # no cache for this account

        row = {
            "account_name": acct.get("account_name", ""),
            "opportunity_name": acct.get("opportunity_name", ""),
            "opportunity_id": opp_id,
            "close_date": acct.get("close_date", ""),
            "bot_id": acct["bot_id"],
            "total_unique_contacts": cached.get("total_unique_contacts", 0),
        }
        for m in MILESTONES:
            row[f"date_{m}"] = cached.get(f"date_{m}")
            row[f"days_to_{m}"] = cached.get(f"days_to_{m}")

        ttv_rows.append(row)

    return ttv_rows
