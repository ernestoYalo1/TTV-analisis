"""
SOW Document Extraction — Phase 2.

Fetches Google Docs SOW documents, extracts the expected go-live date
using Claude (Anthropic API), and writes to Supabase.

Only processes accounts where:
  - sow_url is a valid Google Docs URL
  - expected_go_live_sow is NULL (not yet extracted)
"""
import json
import logging
import os
import re
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Google Docs fetching
# ---------------------------------------------------------------------------

def _extract_doc_id(url: str) -> Optional[str]:
    """Extract Google Doc ID from various URL formats."""
    if not url or url in ("N/A", "-", ""):
        return None

    # Standard: docs.google.com/document/d/{id}/...
    m = re.search(r"docs\.google\.com/document/d/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    # Drive file: drive.google.com/file/d/{id}/...
    m = re.search(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    # Wrapped in a redirect URL: open?id={id}
    m = re.search(r"open\?id[=%]3[Dd]?([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)

    # Drive folder — resolved separately via _pick_latest_doc_in_folder()
    return None


def _extract_folder_id(url: str) -> Optional[str]:
    """Return the folder ID for a drive.google.com/drive/folders/<id> URL, else None."""
    if not url:
        return None
    m = re.search(r"drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def _pick_latest_doc_in_folder(folder_id: str) -> Optional[str]:
    """
    List Google Docs inside a Drive folder and return the ID of the most
    recently modified one. Returns None if the folder is empty or the API
    call fails.
    """
    try:
        token = _get_access_token()
    except Exception as e:
        logger.error("Could not get Google access token for folder listing: %s", e)
        return None

    params = {
        "q": f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.document' and trashed = false",
        "fields": "files(id,name,modifiedTime)",
        "orderBy": "modifiedTime desc",
        "pageSize": "10",
    }
    try:
        resp = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("Drive folder list failed for %s: HTTP %d — %s",
                           folder_id, resp.status_code, resp.text[:200])
            return None
        files = resp.json().get("files", [])
        if not files:
            logger.info("Folder %s has no Google Docs", folder_id)
            return None
        chosen = files[0]
        logger.info("  Folder → picked '%s' (modified %s)",
                    chosen.get("name"), chosen.get("modifiedTime"))
        return chosen.get("id")
    except Exception as e:
        logger.error("Error listing folder %s: %s", folder_id, e)
        return None


def resolve_sow_url_to_doc_id(url: str) -> Optional[str]:
    """
    Resolve any supported SOW URL format to a single Google Doc ID.
    Handles direct doc URLs and Drive folder URLs (picks most recent doc).
    """
    doc_id = _extract_doc_id(url)
    if doc_id:
        return doc_id
    folder_id = _extract_folder_id(url)
    if folder_id:
        return _pick_latest_doc_in_folder(folder_id)
    return None


def _get_access_token() -> str:
    """Get a Google access token from the gcloud CLI with Drive scope."""
    account = os.environ.get("CLOUDSDK_CORE_ACCOUNT", "ernesto.espriella@yalo.com")

    # Try with Drive scope first
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token", f"--account={account}",
         "--scopes=https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()

    # Fallback: token without explicit scopes
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token", f"--account={account}"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gcloud auth failed: {result.stderr.strip()}")
    return result.stdout.strip()


def fetch_doc_text(doc_id: str) -> Optional[str]:
    """
    Fetch a Google Doc as plain text.
    Tries multiple methods: Drive API export, then Docs export URL.
    """
    try:
        token = _get_access_token()
    except Exception as e:
        logger.error("Could not get Google access token: %s", e)
        return None

    headers = {"Authorization": f"Bearer {token}"}

    # Method 1: Drive API v3 export (works with drive.readonly scope)
    drive_url = f"https://www.googleapis.com/drive/v3/files/{doc_id}/export?mimeType=text/plain"
    try:
        resp = requests.get(drive_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.debug("Drive API export failed for %s: HTTP %d", doc_id, resp.status_code)
    except Exception:
        pass

    # Method 2: Docs export URL (works with docs scope)
    docs_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    try:
        resp = requests.get(docs_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning("Failed to fetch doc %s: HTTP %d", doc_id, resp.status_code)
    except Exception as e:
        logger.error("Error fetching doc %s: %s", doc_id, e)

    return None


# ---------------------------------------------------------------------------
# Claude extraction
# ---------------------------------------------------------------------------

def extract_go_live_date(doc_text: str, account_name: str, close_date: str) -> Optional[Dict]:
    """
    Use Claude to extract the expected go-live date from a SOW document.

    Returns a dict {"go_live_date", "confidence", "source"} when found,
    or None when extraction fails or no date could be determined.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return None

    # Truncate very long docs to stay within token limits
    max_chars = 30000
    if len(doc_text) > max_chars:
        doc_text = doc_text[:max_chars] + "\n\n[... document truncated ...]"

    prompt = f"""You are analyzing a Scope of Work (SOW) document for a customer project.

Customer: {account_name}
Opportunity Close Date: {close_date}

Your task: Extract the expected go-live date from this SOW document.

Look for:
- Explicit go-live dates (e.g., "Go-live: March 15, 2026")
- Project timelines with end dates
- Duration-based timelines (e.g., "8 weeks from kick-off") — calculate the date using the close date as reference
- Delivery milestones showing the final launch/go-live date
- Gantt charts or phase timelines

Respond in JSON format only:
{{"go_live_date": "YYYY-MM-DD", "confidence": "high|medium|low", "source": "brief quote or description of where you found it"}}

If you cannot determine a go-live date, respond:
{{"go_live_date": null, "confidence": "none", "source": "reason why not found"}}

--- SOW DOCUMENT ---
{doc_text}
--- END OF DOCUMENT ---"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )

        if resp.status_code != 200:
            logger.error("Claude API error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return None

        data = resp.json()
        text = data.get("content", [{}])[0].get("text", "")

        # Parse JSON from response
        json_match = re.search(r'\{[^}]+\}', text)
        if not json_match:
            logger.warning("Could not parse JSON from Claude response: %s", text[:200])
            return None

        result = json.loads(json_match.group())
        go_live = result.get("go_live_date")
        confidence = result.get("confidence", "unknown")
        source = result.get("source", "")

        if go_live and confidence != "none":
            logger.info("  Extracted go-live: %s (confidence: %s, source: %s)",
                        go_live, confidence, source[:80])
            return {"go_live_date": go_live, "confidence": confidence, "source": source}
        else:
            logger.info("  No go-live date found (source: %s)", source[:80])
            return None

    except Exception as e:
        logger.error("Claude extraction error: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------

def extract_sow_dates(accounts: List[Dict]) -> Dict[str, Dict]:
    """
    For each account with a SOW URL but no extracted date, fetch and parse.

    Args:
        accounts: List of account dicts from Supabase (must have
                  sow_url, expected_go_live_sow, opportunity_id, account_name, close_date)

    Returns:
        Dict mapping opportunity_id -> metadata dict:
            {"go_live_date", "confidence", "source"}
    """
    # Filter to accounts that need processing
    to_process = [
        a for a in accounts
        if a.get("sow_url")
        and a["sow_url"] not in ("N/A", "-", "")
        and not a.get("expected_go_live_sow")
    ]

    if not to_process:
        logger.info("No accounts need SOW extraction")
        return {}

    logger.info("Processing %d accounts for SOW extraction", len(to_process))
    results = {}

    for i, acct in enumerate(to_process, 1):
        opp_id = acct["opportunity_id"]
        name = acct.get("account_name", "unknown")
        sow_url = acct["sow_url"]
        close_date = acct.get("close_date", "")

        logger.info("[%d/%d] %s — %s", i, len(to_process), name, sow_url[:80])

        # Resolve doc ID (handles direct URLs and Drive folders)
        doc_id = resolve_sow_url_to_doc_id(sow_url)
        if not doc_id:
            logger.warning("  Could not extract doc ID from URL, skipping")
            continue

        # Fetch document text
        doc_text = fetch_doc_text(doc_id)
        if not doc_text:
            logger.warning("  Could not fetch document, skipping")
            continue

        logger.info("  Fetched %d chars of text", len(doc_text))

        # Extract go-live date using Claude
        extracted = extract_go_live_date(doc_text, name, close_date)
        if extracted:
            results[opp_id] = extracted

    return results
