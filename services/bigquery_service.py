"""
BigQuery data extraction for TTV analysis.
Gets active bots and computes unique-contact milestones.
"""
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

CLIENTS_DIR = Path(__file__).resolve().parents[1] / "api_clients"
sys.path.insert(0, str(CLIENTS_DIR))

from gcp_client import GCPClient
from config.settings import BQ_CONVERSATIONS_TABLE, MILESTONES


_client = None

def _get_client() -> GCPClient:
    global _client
    if _client is None:
        _client = GCPClient()
    return _client


def get_active_bots() -> List[Dict]:
    """
    Return all bot_ids that have had at least one conversation,
    with first/last conversation dates and total unique contacts.
    Uses DWH.fct_conversation (data from 2020 onward).
    """
    client = _get_client()
    query = f"""
        SELECT
            bot_id,
            COUNT(DISTINCT user_id) as unique_contacts,
            MIN(DATE(fact_date_time_utc)) as first_conversation_date,
            MAX(DATE(fact_date_time_utc)) as last_conversation_date
        FROM `{BQ_CONVERSATIONS_TABLE}`
        WHERE bot_id IS NOT NULL
          AND LOWER(message_rol) = 'user'
          AND user_id NOT LIKE '%qa_test%'
          AND user_id NOT LIKE '%@yalo.com'
        GROUP BY bot_id
        ORDER BY unique_contacts DESC
    """
    logger.info("Querying BigQuery for active bots...")
    results = client.query_bigquery(query, as_dataframe=False)
    if not results:
        logger.warning("BigQuery returned no results for active bots query")
        return []

    logger.info(f"BigQuery returned {len(results)} bots")
    bots = []
    for r in results:
        bots.append({
            "bot_id": r.get("bot_id"),
            "unique_contacts": int(r.get("unique_contacts", 0)),
            "first_conversation_date": str(r.get("first_conversation_date", "")),
            "last_conversation_date": str(r.get("last_conversation_date", "")),
        })
    return bots


def get_milestones(bot_id: str, start_date: str) -> Dict[int, Optional[str]]:
    """
    For a given bot_id, compute when cumulative unique contacts hit each milestone.

    Each user_id counts once on the date of their first conversation.
    Accumulates chronologically from start_date.

    Returns: {10: '2026-02-15', 50: '2026-03-01', 100: None}
    """
    client = _get_client()

    milestone_unions = "\nUNION ALL\n".join([
        f"SELECT {m} as milestone, MIN(first_contact_date) as milestone_date "
        f"FROM cumulative WHERE cumulative_contacts >= {m}"
        for m in MILESTONES
    ])

    query = f"""
        WITH first_contact AS (
            SELECT
                user_id,
                MIN(DATE(fact_date_time_utc)) as first_contact_date
            FROM `{BQ_CONVERSATIONS_TABLE}`
            WHERE bot_id = '{bot_id}'
              AND DATE(fact_date_time_utc) >= '{start_date}'
              AND LOWER(message_rol) = 'user'
              AND user_id NOT LIKE '%qa_test%'
              AND user_id NOT LIKE '%@yalo.com'
            GROUP BY user_id
        ),
        daily_new_contacts AS (
            SELECT
                first_contact_date,
                COUNT(*) as new_contacts
            FROM first_contact
            GROUP BY first_contact_date
        ),
        cumulative AS (
            SELECT
                first_contact_date,
                new_contacts,
                SUM(new_contacts) OVER (ORDER BY first_contact_date) as cumulative_contacts
            FROM daily_new_contacts
        ),
        milestones AS (
            {milestone_unions}
        )
        SELECT milestone, CAST(milestone_date AS STRING) as milestone_date
        FROM milestones
        ORDER BY milestone
    """

    results = client.query_bigquery(query, as_dataframe=False)
    out = {m: None for m in MILESTONES}
    if results:
        for r in results:
            out[int(r["milestone"])] = r.get("milestone_date")
    return out


def get_total_unique_contacts(bot_id: str, start_date: str) -> int:
    """Return total unique contacts for a bot since start_date."""
    client = _get_client()
    query = f"""
        SELECT COUNT(DISTINCT user_id) as total
        FROM `{BQ_CONVERSATIONS_TABLE}`
        WHERE bot_id = '{bot_id}'
          AND DATE(fact_date_time_utc) >= '{start_date}'
          AND LOWER(message_rol) = 'user'
          AND user_id NOT LIKE '%qa_test%'
          AND user_id NOT LIKE '%@yalo.com'
    """
    results = client.query_bigquery(query, as_dataframe=False)
    if results and len(results) > 0:
        return int(results[0].get("total", 0))
    return 0
