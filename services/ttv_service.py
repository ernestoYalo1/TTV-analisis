"""
Orchestrates the full TTV computation.
Pulls mapped accounts from SQLite, queries BigQuery for milestones,
and returns the final TTV table.
"""
from datetime import datetime
from typing import Dict, List, Optional

from services.mapping_service import get_mapped
from services.bigquery_service import get_milestones, get_total_unique_contacts
from config.settings import MILESTONES


def compute_ttv_table() -> List[Dict]:
    """
    Build the Time-to-Value table for all mapped accounts.

    For each account with a bot_id, queries BigQuery for milestone dates
    and computes days-to-milestone from the opportunity close date.
    """
    mapped = get_mapped()
    if not mapped:
        return []

    ttv_rows = []
    for acct in mapped:
        bot_id = acct["bot_id"]
        close_date = acct.get("close_date", "")

        milestones = get_milestones(bot_id, close_date) if close_date else {}
        total_contacts = get_total_unique_contacts(bot_id, close_date) if close_date else 0

        row = {
            "account_name": acct["account_name"],
            "opportunity_name": acct.get("opportunity_name", ""),
            "close_date": close_date,
            "bot_id": bot_id,
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

        ttv_rows.append(row)

    return ttv_rows


def compute_summary(ttv_rows: List[Dict]) -> Dict:
    """Compute summary metrics from the TTV table."""
    if not ttv_rows:
        return {"total_accounts": 0}

    total = len(ttv_rows)
    reached = {m: 0 for m in MILESTONES}
    avg_days = {m: None for m in MILESTONES}

    for m in MILESTONES:
        days_vals = [r[f"days_to_{m}"] for r in ttv_rows if r[f"days_to_{m}"] is not None]
        reached[m] = len(days_vals)
        if days_vals:
            avg_days[m] = round(sum(days_vals) / len(days_vals), 1)

    return {
        "total_accounts": total,
        "reached": reached,
        "avg_days": avg_days,
    }
