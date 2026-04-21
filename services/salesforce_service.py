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
        SELECT Id, Name, Account.Name, Account.Id, Account.Business_Type__c,
               StageName, Amount, CloseDate, Type, CreatedDate
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
                "business_type": r.get("Account", {}).get("Business_Type__c") or "",
                "sf_url": f"https://yalo.my.salesforce.com/{r.get('Id')}",
            })

    new_customers.sort(key=lambda x: x["close_date"])
    return new_customers


def get_tech_assist_lightning_url(opportunity_id: str) -> str:
    """
    Look up the Tech_Assist__c record linked to this opportunity and return
    a Salesforce URL the PM can open to add/edit the SOW link.
    Returns empty string if no Tech Assist exists for the opportunity.
    """
    client = _get_sf_client()
    if not client.connect():
        raise ConnectionError("Could not connect to Salesforce")

    soql = (
        "SELECT Id FROM Tech_Assist__c "
        f"WHERE Opportunity__c = '{opportunity_id}' "
        "ORDER BY CreatedDate DESC LIMIT 1"
    )
    results = client.query(soql)
    if not results:
        return ""
    ta_id = results[0].get("Id")
    if not ta_id:
        return ""
    instance = os.environ.get("SF_INSTANCE_URL", "https://yalo.my.salesforce.com").rstrip("/")
    return f"{instance}/{ta_id}"


def update_opportunity_name(opportunity_id: str, new_name: str) -> bool:
    """
    Write a new Name back to an Opportunity record in Salesforce.
    Requires the connected user to have Edit permission on Opportunity.
    Returns True on success, False if the update failed.
    """
    client = _get_sf_client()
    if not client.connect():
        raise ConnectionError("Could not connect to Salesforce")
    return client.update_opportunity(opportunity_id, {"Name": new_name})


def get_delivery_process_data(opportunity_ids: List[str]) -> Dict[str, Dict]:
    """
    Query Tech_Assist__c and Project__c for delivery process dates.
    Returns dict keyed by opportunity_id with fields:
      tech_assist_start, tech_assist_end, pm_start, go_live_date,
      sow_url, expected_go_live_pm
    """
    if not opportunity_ids:
        return {}

    client = _get_sf_client()
    if not client.connect():
        raise ConnectionError("Could not connect to Salesforce")

    ids_str = "','".join(opportunity_ids)

    # Query Tech Assists linked to these opportunities
    ta_soql = f"""
        SELECT Opportunity__c, Assist_Started_Date__c, Assist_Completed_Date__c,
               Handover_Meeting_Date__c, Status__c
        FROM Tech_Assist__c
        WHERE Opportunity__c IN ('{ids_str}')
    """
    ta_results = client.query_all(ta_soql)

    # Query Projects linked to these opportunities
    proj_soql = f"""
        SELECT Opportunity__c, Internal_Kick_Off_Started_Date__c, Planned_Start_Date__c,
               Go_Live_Date__c, Completed_Date__c, SOW_URL__c, Status__c,
               Expected_Go_Live_informed_by_PM__c
        FROM Project__c
        WHERE Opportunity__c IN ('{ids_str}')
    """
    proj_results = client.query_all(proj_soql)

    result: Dict[str, Dict] = {}

    for ta in ta_results:
        opp_id = ta.get("Opportunity__c")
        if not opp_id:
            continue
        result.setdefault(opp_id, {})
        ta_start = ta.get("Assist_Started_Date__c") or ""
        if ta_start and "T" in ta_start:
            ta_start = ta_start[:10]
        ta_end = ta.get("Assist_Completed_Date__c") or ta.get("Handover_Meeting_Date__c") or ""
        if ta_end and "T" in ta_end:
            ta_end = ta_end[:10]
        result[opp_id]["tech_assist_start"] = ta_start or None
        result[opp_id]["tech_assist_end"] = ta_end or None
        result[opp_id]["tech_assist_status"] = ta.get("Status__c")

    for proj in proj_results:
        opp_id = proj.get("Opportunity__c")
        if not opp_id:
            continue
        result.setdefault(opp_id, {})
        result[opp_id]["pm_start"] = (
            proj.get("Internal_Kick_Off_Started_Date__c")
            or proj.get("Planned_Start_Date__c")
        )
        result[opp_id]["go_live_date"] = proj.get("Go_Live_Date__c")
        result[opp_id]["sow_url"] = proj.get("SOW_URL__c")
        result[opp_id]["expected_go_live_pm"] = proj.get("Expected_Go_Live_informed_by_PM__c")
        result[opp_id]["project_status"] = proj.get("Status__c")

    return result
