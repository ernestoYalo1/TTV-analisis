#!/usr/bin/env python3
"""
Daily TTV Ingestion — runs as a cron job once a day.

Pulls all data from Salesforce and BigQuery, writes to Supabase.
The dashboard then reads only from Supabase (fast page loads).

Steps:
  1. Pull new customers from Salesforce (accounts + opportunities)
  2. Pull delivery process data (Tech Assist + Project) from Salesforce
  3. Upsert accounts to Supabase with delivery fields
  4. Compute BigQuery milestones for mapped accounts
  5. Cache milestones to Supabase
  6. (Phase 2 - future) Extract expected go-live from SOW documents

Usage:
    python scripts/daily_ingest.py

Cron (once a day at 6 AM UTC):
    0 6 * * * cd /opt/ttv_analysis && /opt/scanning_agent/venv/bin/python scripts/daily_ingest.py >> /opt/ttv_analysis/logs/daily_ingest.log 2>&1
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_ingest")


def main():
    start = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("TTV Daily Ingestion — started at %s UTC", start.isoformat())
    logger.info("=" * 60)

    from services import supabase_client as sb
    if not sb.is_available():
        logger.error("Supabase not available. Set SUPABASE_URL and SUPABASE_KEY. Aborting.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 1: Pull accounts from Salesforce
    # ------------------------------------------------------------------
    logger.info("[1/5] Pulling accounts from Salesforce...")
    from services.salesforce_service import get_new_customers, get_delivery_process_data
    try:
        customers = get_new_customers()
        logger.info("  -> %d accounts from Salesforce", len(customers))
    except Exception as e:
        logger.error("  Salesforce error: %s", e)
        sys.exit(1)

    if not customers:
        logger.warning("  No accounts found. Nothing to ingest.")
        return

    # ------------------------------------------------------------------
    # Step 2: Pull delivery process data from Salesforce
    # ------------------------------------------------------------------
    logger.info("[2/5] Pulling delivery process data (Tech Assist + Project)...")
    opp_ids = [c["opportunity_id"] for c in customers if c.get("opportunity_id")]
    delivery_data = {}
    try:
        if opp_ids:
            delivery_data = get_delivery_process_data(opp_ids)
            logger.info("  -> Delivery data for %d accounts", len(delivery_data))
    except Exception as e:
        logger.warning("  Could not load delivery data: %s", e)

    # Merge delivery fields into customer dicts before upsert
    for c in customers:
        opp_id = c.get("opportunity_id", "")
        dd = delivery_data.get(opp_id, {})
        for field in ("tech_assist_start", "tech_assist_end", "pm_start",
                       "go_live_date", "sow_url", "expected_go_live_pm"):
            c[field] = dd.get(field)

    # ------------------------------------------------------------------
    # Step 3: Upsert accounts to Supabase (with delivery fields)
    # ------------------------------------------------------------------
    logger.info("[3/5] Upserting %d accounts to Supabase...", len(customers))
    from services.mapping_service import upsert_accounts, get_mapped, get_unmapped
    try:
        upsert_accounts(customers)
        logger.info("  -> Upserted successfully")
    except Exception as e:
        logger.error("  Upsert error: %s", e)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 4: Compute BigQuery milestones for mapped accounts
    # ------------------------------------------------------------------
    logger.info("[4/5] Computing BigQuery milestones for mapped accounts...")
    from services.bigquery_service import get_milestones, get_total_unique_contacts
    from services.ttv_service import _cache_milestone, _build_row

    mapped = get_mapped()
    logger.info("  -> %d mapped accounts to process", len(mapped))

    for i, acct in enumerate(mapped, 1):
        bot_id = acct["bot_id"]
        close_date = acct.get("close_date", "")
        acct_name = acct.get("account_name", "unknown")

        if not close_date:
            logger.warning("  [%d/%d] %s — no close_date, skipping", i, len(mapped), acct_name)
            continue

        try:
            milestones = get_milestones(bot_id, close_date)
            total_contacts = get_total_unique_contacts(bot_id, close_date)
            row = _build_row(acct, milestones, total_contacts)
            _cache_milestone(acct, row)
            logger.info("  [%d/%d] %s — %d contacts, milestones: %s",
                        i, len(mapped), acct_name, total_contacts,
                        {k: v for k, v in milestones.items() if v})
        except Exception as e:
            logger.error("  [%d/%d] %s — error: %s", i, len(mapped), acct_name, e)

    # ------------------------------------------------------------------
    # Step 5: SOW document extraction (only for new/unprocessed accounts)
    # ------------------------------------------------------------------
    logger.info("[5/5] SOW go-live extraction...")
    from services.sow_extraction import extract_sow_dates
    from services.mapping_service import update_delivery_data

    all_accounts = sb.select("ttv_account_mappings")
    try:
        sow_results = extract_sow_dates(all_accounts)
        for opp_id, meta in sow_results.items():
            update_delivery_data(opp_id, {
                "expected_go_live_sow": meta["go_live_date"],
                "sow_extraction_confidence": meta.get("confidence"),
                "sow_extraction_source": (meta.get("source") or "")[:500],
                "sow_extracted_at": datetime.utcnow().isoformat(),
            })
            logger.info("  Saved expected_go_live_sow=%s (%s) for %s",
                        meta["go_live_date"], meta.get("confidence", "?"), opp_id)
        logger.info("  -> Extracted SOW dates for %d accounts", len(sow_results))
    except Exception as e:
        logger.warning("  SOW extraction error: %s", e)

    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info("=" * 60)
    logger.info("Ingestion complete in %.1f seconds", elapsed)
    logger.info("  Accounts: %d total, %d mapped, %d unmapped",
                len(customers), len(mapped), len(customers) - len(mapped))
    logger.info("  SOW dates extracted: %d", len(sow_results) if 'sow_results' in dir() else 0)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
