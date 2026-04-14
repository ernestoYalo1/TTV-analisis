#!/usr/bin/env python3
"""
GCP Client for Yalo Workspace
Provides BigQuery querying capabilities using gcloud CLI.

Supports:
- BigQuery SQL queries
- Multiple output formats (JSON, pandas DataFrame, CSV)
- Query history and caching
- Connection testing

Usage:
    from gcp_client import GCPClient

    client = GCPClient()

    # Test connection
    if client.test_connection():
        # Run a query
        df = client.query_bigquery("SELECT * FROM dataset.table LIMIT 10")
        print(df)
"""

import json
import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_PROJECT = "arched-photon-194421"
DEFAULT_TIMEOUT = 300  # 5 minutes
DEFAULT_MAX_ROWS = 100000
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_DELAY = 30  # seconds


class GCPClient:
    """
    Google Cloud Platform client for BigQuery operations.

    Uses the gcloud/bq CLI tools for authentication and querying.
    Assumes gcloud is already configured and authenticated.
    """

    def __init__(
        self,
        project_id: str = DEFAULT_PROJECT,
        timeout: int = DEFAULT_TIMEOUT,
        max_rows: int = DEFAULT_MAX_ROWS,
        retry_count: int = DEFAULT_RETRY_COUNT,
        retry_delay: int = DEFAULT_RETRY_DELAY,
    ):
        """
        Initialize the GCP client.

        Args:
            project_id: GCP project ID (default: arched-photon-194421)
            timeout: Query timeout in seconds (default: 300)
            max_rows: Maximum rows to return (default: 100000)
            retry_count: Number of retries on failure (default: 3)
            retry_delay: Delay between retries in seconds (default: 30)
        """
        self.project_id = project_id
        self.timeout = timeout
        self.max_rows = max_rows
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self._config_cache: Optional[Dict] = None

    def get_config(self) -> Dict[str, str]:
        """
        Get current gcloud configuration.

        Returns:
            Dictionary with account, project, and other config values
        """
        if self._config_cache:
            return self._config_cache

        try:
            result = subprocess.run(
                ['gcloud', 'config', 'list', '--format=json'],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                self._config_cache = json.loads(result.stdout)
                return self._config_cache
            else:
                logger.error(f"Failed to get gcloud config: {result.stderr}")
                return {}

        except Exception as e:
            logger.error(f"Error getting gcloud config: {e}")
            return {}

    def test_connection(self) -> bool:
        """
        Test GCP connection by verifying gcloud authentication.

        Returns:
            True if connected and authenticated, False otherwise
        """
        try:
            # Check gcloud is installed and configured
            result = subprocess.run(
                ['gcloud', 'auth', 'list', '--format=json'],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                logger.error(f"gcloud auth check failed: {result.stderr}")
                return False

            auth_info = json.loads(result.stdout)
            active_accounts = [a for a in auth_info if a.get('status') == 'ACTIVE']

            if not active_accounts:
                logger.error("No active gcloud account found")
                return False

            logger.info(f"Connected as: {active_accounts[0].get('account')}")

            # Verify BigQuery access with a simple query
            test_query = "SELECT 1 as test"
            result = self._execute_query(test_query, max_rows=1)

            if result is not None:
                logger.info("BigQuery connection verified")
                return True

            return False

        except FileNotFoundError:
            logger.error("gcloud CLI not found. Please install Google Cloud SDK.")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def _execute_query(
        self,
        query: str,
        max_rows: Optional[int] = None,
        output_format: str = 'json'
    ) -> Optional[str]:
        """
        Execute a BigQuery query using the bq CLI.

        Args:
            query: SQL query to execute
            max_rows: Override max rows for this query
            output_format: Output format (json, csv, prettyjson)

        Returns:
            Query output string, or None on failure
        """
        max_rows = max_rows or self.max_rows

        for attempt in range(self.retry_count):
            try:
                logger.info(f"Executing BigQuery query (attempt {attempt + 1}/{self.retry_count})")

                cmd = [
                    'bq', 'query',
                    '--use_legacy_sql=false',
                    f'--format={output_format}',
                    f'--max_rows={max_rows}',
                    f'--project_id={self.project_id}',
                    query
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout
                )

                if result.returncode == 0:
                    return result.stdout
                else:
                    logger.error(f"BigQuery query failed: {result.stderr}")

            except subprocess.TimeoutExpired:
                logger.error(f"Query timed out after {self.timeout} seconds")
            except Exception as e:
                logger.error(f"Error executing query: {e}")

            if attempt < self.retry_count - 1:
                logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

        return None

    def query_bigquery(
        self,
        query: str,
        as_dataframe: bool = True,
        max_rows: Optional[int] = None
    ) -> Union['pd.DataFrame', List[Dict], None]:
        """
        Execute a BigQuery query and return results.

        Args:
            query: SQL query to execute
            as_dataframe: If True, return pandas DataFrame; else return list of dicts
            max_rows: Override max rows for this query

        Returns:
            Query results as DataFrame or list of dictionaries, None on failure
        """
        result = self._execute_query(query, max_rows=max_rows, output_format='json')

        if result is None:
            return None

        try:
            data = json.loads(result)

            if not data:
                logger.warning("Query returned no results")
                if as_dataframe:
                    import pandas as pd
                    return pd.DataFrame()
                return []

            if as_dataframe:
                import pandas as pd
                return pd.DataFrame(data)

            return data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse query results: {e}")
            return None

    def query_to_csv(
        self,
        query: str,
        output_path: Union[str, Path],
        max_rows: Optional[int] = None
    ) -> bool:
        """
        Execute a BigQuery query and save results to CSV.

        Args:
            query: SQL query to execute
            output_path: Path to save CSV file
            max_rows: Override max rows for this query

        Returns:
            True if successful, False otherwise
        """
        result = self._execute_query(query, max_rows=max_rows, output_format='csv')

        if result is None:
            return False

        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(result)
            logger.info(f"Results saved to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")
            return False

    def list_datasets(self) -> List[str]:
        """
        List all datasets in the project.

        Returns:
            List of dataset names
        """
        try:
            result = subprocess.run(
                ['bq', 'ls', '--format=json', f'--project_id={self.project_id}'],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                datasets = json.loads(result.stdout)
                return [ds.get('datasetReference', {}).get('datasetId', '')
                        for ds in datasets]
            else:
                logger.error(f"Failed to list datasets: {result.stderr}")
                return []

        except Exception as e:
            logger.error(f"Error listing datasets: {e}")
            return []

    def list_tables(self, dataset: str) -> List[str]:
        """
        List all tables in a dataset.

        Args:
            dataset: Dataset name

        Returns:
            List of table names
        """
        try:
            result = subprocess.run(
                ['bq', 'ls', '--format=json', f'{self.project_id}:{dataset}'],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                tables = json.loads(result.stdout)
                return [t.get('tableReference', {}).get('tableId', '')
                        for t in tables]
            else:
                logger.error(f"Failed to list tables: {result.stderr}")
                return []

        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def get_table_schema(self, dataset: str, table: str) -> Optional[List[Dict]]:
        """
        Get schema for a specific table.

        Args:
            dataset: Dataset name
            table: Table name

        Returns:
            List of column definitions, or None on failure
        """
        try:
            result = subprocess.run(
                ['bq', 'show', '--format=json', f'{self.project_id}:{dataset}.{table}'],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                table_info = json.loads(result.stdout)
                return table_info.get('schema', {}).get('fields', [])
            else:
                logger.error(f"Failed to get table schema: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            return None

    def get_table_info(self, dataset: str, table: str) -> Optional[Dict]:
        """
        Get full information about a table.

        Args:
            dataset: Dataset name
            table: Table name

        Returns:
            Table metadata dictionary, or None on failure
        """
        try:
            result = subprocess.run(
                ['bq', 'show', '--format=json', f'{self.project_id}:{dataset}.{table}'],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                logger.error(f"Failed to get table info: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return None


# Convenience functions for quick usage
def query(sql: str, project_id: str = DEFAULT_PROJECT) -> Optional['pd.DataFrame']:
    """
    Quick helper to run a BigQuery query.

    Args:
        sql: SQL query string
        project_id: GCP project ID

    Returns:
        pandas DataFrame with results
    """
    client = GCPClient(project_id=project_id)
    return client.query_bigquery(sql)


def test_connection(project_id: str = DEFAULT_PROJECT) -> bool:
    """
    Quick helper to test GCP connection.

    Args:
        project_id: GCP project ID

    Returns:
        True if connected
    """
    client = GCPClient(project_id=project_id)
    return client.test_connection()


if __name__ == '__main__':
    # Test the module
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("GCP Client Test")
    print("=" * 50)

    client = GCPClient()

    # Test connection
    print("\n1. Testing connection...")
    if client.test_connection():
        print("   Connection successful")

        # Get config
        print("\n2. Getting gcloud config...")
        config = client.get_config()
        if config:
            core = config.get('core', {})
            print(f"   Account: {core.get('account', 'N/A')}")
            print(f"   Project: {core.get('project', 'N/A')}")

        # List datasets
        print("\n3. Listing datasets...")
        datasets = client.list_datasets()
        print(f"   Found {len(datasets)} datasets")
        for ds in datasets[:5]:
            print(f"   - {ds}")
        if len(datasets) > 5:
            print(f"   ... and {len(datasets) - 5} more")

        # Run a test query
        print("\n4. Running test query...")
        df = client.query_bigquery("SELECT 'Hello from BigQuery' as message")
        if df is not None and not df.empty:
            print(f"   Result: {df.iloc[0]['message']}")
    else:
        print("   Connection failed. Please check gcloud configuration.")
