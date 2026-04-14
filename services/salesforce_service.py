"""
Salesforce data extraction for TTV analysis.
Finds accounts whose first-ever closed-won opportunity is from the cutoff date onward.
"""
import os
import sys
import json
from pathlib import Path
from typing import List, Dict

CLIENTS_DIR = Path(__file__).resolve().parents[1] / "api_clients"
sys.path.insert(0, str(CLIENTS_DIR))

from salesforce_client import SalesforceClient
from config.settings import SF_CONFIG_PATH, NEW_CUSTOMER_CUTOFF


def _get_sf_client() -> SalesforceClient:
    """
    Build a SalesforceClient.
    If SF_USERNAME env var is set, write a temp config from env vars.
    Otherwise use the bundled config.json.
    """
    if os.environ.get("SF_USERNAME"):
        # Build config from env vars (VM deployment uses shared .env)
        config_data = {
            "salesforce": {
                "instance_url": os.environ.get("SF_INSTANCE_URL", "https://yalo.my.salesforce.com"),
                "username": os.environ["SF_USERNAME"],
                "password": os.environ["SF_PASSWORD"],
                "security_token": os.environ.get("SF_SECURITY_TOKEN", ""),
                "api_version": "59.0",
            },
            "settings": {"timeout": 30, "max_records": 2000},
        }
        tmp_config = CLIENTS_DIR / "_sf_config_env.json"
        with open(tmp_config, "w") as f:
            json.dump(config_data, f)
        return SalesforceClient(config_path=str(tmp_config))

    return SalesforceClient(config_path=str(SF_CONFIG_PATH))


def get_new_customers(cutoff: str = NEW_CUSTOMER_CUTOFF) -> List[Dict]:
    """
    Return accounts whose first-ever closed-won opportunity is on or after `cutoff`.

    Steps:
      1. Query ALL closed-won opportunities (all time)
      2. Group by Account.Id, find MIN(CloseDate) per account
      3. Keep only those where MIN(CloseDate) >= cutoff
      4. Return the first opportunity record for each qualifying account
    """
    client = _get_sf_client()
    if not client.connect():
        raise ConnectionError("Could not connect to Salesforce")

    soql = """
        SELECT Id, Name, Account.Name, Account.Id, StageName,
               Amount, CloseDate, Type, CreatedDate
        FROM Opportunity
        WHERE IsWon = true
        ORDER BY CloseDate ASC
    """
    results = client.query_all(soql)
    if not results:
        return []

    # Group by account — keep the first (earliest) opportunity per account
    first_opp_by_account: Dict[str, Dict] = {}
    for r in results:
        acct_id = r.get("Account", {}).get("Id") if r.get("Account") else None
        if not acct_id:
            continue
        if acct_id not in first_opp_by_account:
            first_opp_by_account[acct_id] = r

    # Filter: only accounts whose first opp is >= cutoff
    new_customers = []
    for acct_id, r in first_opp_by_account.items():
        close_date = r.get("CloseDate", "")
        if close_date >= cutoff:
            new_customers.append({
                "account_name": r.get("Account", {}).get("Name"),
                "account_id": acct_id,
                "opportunity_name": r.get("Name"),
                "opportunity_id": r.get("Id"),
                "close_date": close_date,
                "amount": r.get("Amount"),
                "sf_url": f"https://yalo.my.salesforce.com/{r.get('Id')}",
            })

    new_customers.sort(key=lambda x: x["close_date"])
    return new_customers
