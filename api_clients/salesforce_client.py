"""
Salesforce API Client for Yalo

Full read/write functionality for Salesforce objects:
- Accounts, Opportunities, Contracts, Contacts
- Project status queries
- Revenue and pipeline analysis

Author: Yalo Project
Last Updated: November 2025
"""

import json
import requests
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

try:
    from simple_salesforce import Salesforce
    from simple_salesforce.exceptions import SalesforceError, SalesforceAuthenticationFailed
except ImportError:
    print("Please install simple-salesforce: pip install simple-salesforce")
    raise


class SalesforceClient:
    """
    Salesforce API client with organized method groups.

    Method Groups:
    - CONNECTION: Authentication and connection management
    - DISCOVERY: List objects, describe schemas
    - READ: Query data (Accounts, Opportunities, Contracts)
    - WRITE: Create/update records
    - ANALYSIS: Revenue, pipeline, metrics

    Usage:
        client = SalesforceClient()
        if client.connect():
            accounts = client.search_accounts(name="Unilever")
            print(accounts)
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the SalesforceClient with configuration.

        Args:
            config_path (str): Path to the configuration JSON file.
                             If None, looks for config.json in same directory.
        """
        if config_path is None:
            # Default to config.json in same directory as this script
            self.config_path = Path(__file__).parent / "config.json"
        else:
            self.config_path = Path(config_path)

        self.config = None
        self.sf = None  # Salesforce connection object
        self.connected = False
        self._load_config()

    # ==================== CONNECTION METHODS ====================

    def _load_config(self):
        """Load configuration from JSON file."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}\n"
                f"Please copy config.template.json to config.json and fill in your credentials."
            )

        with open(self.config_path, 'r') as f:
            self.config = json.load(f)

    def connect(self) -> bool:
        """
        Establish connection to Salesforce.

        Tries multiple authentication methods:
        1. OAuth2 Client Credentials (for Connected Apps)
        2. Username + Password (traditional auth)
        3. Session ID (for temporary tokens)

        Returns:
            bool: True if connection successful, False otherwise
        """
        sf_config = self.config['salesforce']
        instance_url = sf_config['instance_url']

        print(f"Connecting to Salesforce...")
        print(f"  Instance: {instance_url}")

        # Method 1: OAuth2 Password Grant Flow (Connected App + User credentials)
        if all(k in sf_config for k in ['client_id', 'client_secret', 'username', 'password']):
            print("  Trying OAuth2 Password Grant...")
            try:
                auth_result = self._authenticate_oauth2_password(
                    sf_config['client_id'],
                    sf_config['client_secret'],
                    sf_config['username'],
                    sf_config['password'],
                    instance_url
                )
                if auth_result:
                    self.connected = True
                    print(f"✓ Connected successfully using OAuth2 Password Grant")
                    return True
            except Exception as e:
                print(f"  OAuth2 Password Grant failed: {e}")

        # Method 2: OAuth2 Client Credentials Flow (Connected App only)
        if 'client_id' in sf_config and 'client_secret' in sf_config:
            print("  Trying OAuth2 Client Credentials...")
            try:
                auth_result = self._authenticate_oauth2_client_credentials(
                    sf_config['client_id'],
                    sf_config['client_secret'],
                    instance_url
                )
                if auth_result:
                    self.connected = True
                    print(f"✓ Connected successfully using OAuth2 Client Credentials")
                    return True
            except Exception as e:
                print(f"  OAuth2 Client Credentials failed: {e}")

        # Method 3: Username + Password (traditional simple-salesforce)
        if 'username' in sf_config and 'password' in sf_config:
            username = sf_config['username']
            password = sf_config['password']
            security_token = sf_config.get('security_token', '')

            # For My Domain URLs like yalo.my.salesforce.com, use instance_url directly
            # For standard URLs, extract domain
            if '.my.salesforce.com' in instance_url:
                # Use instance_url parameter instead of domain
                print(f"  Trying Username/Password for {username} (My Domain)...")
                try:
                    self.sf = Salesforce(
                        username=username,
                        password=password,
                        security_token=security_token,
                        instance_url=instance_url,
                        version=sf_config.get('api_version', '59.0')
                    )
                    self.connected = True
                    print(f"✓ Connected successfully using username/password")
                    return True
                except SalesforceAuthenticationFailed as e:
                    print(f"  Username/Password failed: {e}")
                except Exception as e:
                    print(f"  Username/Password error: {e}")
            else:
                # Standard Salesforce login
                domain = 'login'
                print(f"  Trying Username/Password for {username} (standard)...")
                try:
                    self.sf = Salesforce(
                        username=username,
                        password=password,
                        security_token=security_token,
                        domain=domain,
                        version=sf_config.get('api_version', '59.0')
                    )
                    self.connected = True
                    print(f"✓ Connected successfully using username/password")
                    return True
                except SalesforceAuthenticationFailed as e:
                    print(f"  Username/Password failed: {e}")
                except Exception as e:
                    print(f"  Username/Password error: {e}")

        # Method 4: Session ID (temporary token)
        if 'session_id' in sf_config:
            print("  Trying Session ID...")
            try:
                self.sf = Salesforce(
                    instance_url=instance_url,
                    session_id=sf_config['session_id'],
                    version=sf_config.get('api_version', '59.0')
                )
                self.connected = True
                print(f"✓ Connected successfully using Session ID")
                return True
            except Exception as e:
                print(f"  Session ID failed: {e}")

        print("✗ All authentication methods failed")
        return False

    def _authenticate_oauth2_client_credentials(self, client_id: str, client_secret: str, instance_url: str) -> bool:
        """
        Authenticate using OAuth2 Client Credentials flow.

        This is for Connected Apps with OAuth2 enabled.

        Args:
            client_id: Consumer Key from Connected App
            client_secret: Consumer Secret from Connected App
            instance_url: Salesforce instance URL

        Returns:
            bool: True if successful
        """
        # Try the standard login endpoint first
        token_url = "https://login.salesforce.com/services/oauth2/token"

        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }

        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            instance = token_data.get('instance_url', instance_url)

            self.sf = Salesforce(
                instance_url=instance,
                session_id=access_token,
                version=self.config['salesforce'].get('api_version', '59.0')
            )
            return True

        # If standard login fails, try the instance-specific URL
        token_url = f"{instance_url}/services/oauth2/token"
        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            instance = token_data.get('instance_url', instance_url)

            self.sf = Salesforce(
                instance_url=instance,
                session_id=access_token,
                version=self.config['salesforce'].get('api_version', '59.0')
            )
            return True

        # Log error details
        print(f"    Client Credentials Error: {response.status_code}")
        try:
            error_data = response.json()
            print(f"    Error: {error_data.get('error')}: {error_data.get('error_description')}")
        except:
            print(f"    Response: {response.text[:200]}")

        return False

    def _authenticate_oauth2_password(self, client_id: str, client_secret: str, username: str, password: str, instance_url: str) -> bool:
        """
        Authenticate using OAuth2 Password Grant flow.

        This combines Connected App credentials with user credentials.

        Args:
            client_id: Consumer Key from Connected App
            client_secret: Consumer Secret from Connected App
            username: Salesforce username (email)
            password: Password + Security Token
            instance_url: Salesforce instance URL

        Returns:
            bool: True if successful
        """
        token_url = "https://login.salesforce.com/services/oauth2/token"

        payload = {
            'grant_type': 'password',
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password
        }

        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            instance = token_data.get('instance_url', instance_url)

            self.sf = Salesforce(
                instance_url=instance,
                session_id=access_token,
                version=self.config['salesforce'].get('api_version', '59.0')
            )
            return True

        # Try instance-specific URL
        token_url = f"{instance_url}/services/oauth2/token"
        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            instance = token_data.get('instance_url', instance_url)

            self.sf = Salesforce(
                instance_url=instance,
                session_id=access_token,
                version=self.config['salesforce'].get('api_version', '59.0')
            )
            return True

        # Log error details
        print(f"    Password Grant Error: {response.status_code}")
        try:
            error_data = response.json()
            print(f"    Error: {error_data.get('error')}: {error_data.get('error_description')}")
        except:
            print(f"    Response: {response.text[:200]}")

        return False

    def test_connection(self) -> bool:
        """
        Test the connection to Salesforce and display user/org info.

        Returns:
            bool: True if connection successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False

        try:
            # Query organization info
            org_info = self.sf.query("SELECT Id, Name, OrganizationType FROM Organization LIMIT 1")

            if org_info['records']:
                org = org_info['records'][0]
                print(f"\n✓ Connection Test Successful!")
                print(f"  Organization: {org.get('Name')}")
                print(f"  Type: {org.get('OrganizationType')}")
                print(f"  Org ID: {org.get('Id')}")

            # Get API limits
            limits = self.sf.limits()
            daily_api = limits.get('DailyApiRequests', {})
            print(f"  API Calls: {daily_api.get('Remaining', 'N/A')}/{daily_api.get('Max', 'N/A')} remaining")

            return True

        except Exception as e:
            print(f"✗ Connection test failed: {e}")
            return False

    def ensure_connected(self) -> bool:
        """Ensure connection is established before operations."""
        if not self.connected:
            return self.connect()
        return True

    # ==================== DISCOVERY METHODS ====================

    def list_available_objects(self) -> List[Dict]:
        """
        List all accessible Salesforce objects.

        Returns:
            List[Dict]: List of object metadata including name, label, queryable status
        """
        if not self.ensure_connected():
            return []

        try:
            describe = self.sf.describe()
            objects = []

            for obj in describe['sobjects']:
                objects.append({
                    'name': obj['name'],
                    'label': obj['label'],
                    'queryable': obj['queryable'],
                    'createable': obj['createable'],
                    'updateable': obj['updateable'],
                    'custom': obj['custom']
                })

            return objects
        except Exception as e:
            print(f"✗ Error listing objects: {e}")
            return []

    def describe_object(self, object_name: str) -> Optional[Dict]:
        """
        Get detailed schema/fields for a specific object.

        Args:
            object_name (str): Salesforce object name (e.g., 'Account', 'Opportunity')

        Returns:
            Optional[Dict]: Object description including fields, or None if error
        """
        if not self.ensure_connected():
            return None

        try:
            obj = getattr(self.sf, object_name)
            description = obj.describe()
            return description
        except Exception as e:
            print(f"✗ Error describing {object_name}: {e}")
            return None

    def get_object_fields(self, object_name: str) -> List[str]:
        """
        Get list of field names for an object.

        Args:
            object_name (str): Salesforce object name

        Returns:
            List[str]: List of field names
        """
        description = self.describe_object(object_name)
        if description:
            return [field['name'] for field in description.get('fields', [])]
        return []

    def get_queryable_fields(self, object_name: str) -> List[str]:
        """
        Get list of queryable field names for an object.

        Args:
            object_name (str): Salesforce object name

        Returns:
            List[str]: List of queryable field names
        """
        description = self.describe_object(object_name)
        if description:
            return [
                field['name']
                for field in description.get('fields', [])
                if field.get('type') != 'address'  # Compound fields can't be queried directly
            ]
        return []

    # ==================== READ METHODS ====================

    def query(self, soql: str) -> List[Dict]:
        """
        Execute a SOQL query.

        Args:
            soql (str): SOQL query string

        Returns:
            List[Dict]: Query results as list of records
        """
        if not self.ensure_connected():
            return []

        try:
            result = self.sf.query(soql)
            return result.get('records', [])
        except Exception as e:
            print(f"✗ Query error: {e}")
            return []

    def query_all(self, soql: str) -> List[Dict]:
        """
        Execute a SOQL query and fetch all records (handles pagination).

        Args:
            soql (str): SOQL query string

        Returns:
            List[Dict]: All query results
        """
        if not self.ensure_connected():
            return []

        try:
            result = self.sf.query_all(soql)
            return result.get('records', [])
        except Exception as e:
            print(f"✗ Query error: {e}")
            return []

    # --- Account Methods ---

    def get_account(self, account_id: str) -> Optional[Dict]:
        """
        Get account by ID.

        Args:
            account_id (str): Salesforce Account ID

        Returns:
            Optional[Dict]: Account data or None if not found
        """
        if not self.ensure_connected():
            return None

        try:
            return self.sf.Account.get(account_id)
        except Exception as e:
            print(f"✗ Error getting account {account_id}: {e}")
            return None

    def search_accounts(self, name: str = None, account_type: str = None, limit: int = 100) -> List[Dict]:
        """
        Search accounts with filters.

        Args:
            name (str): Account name to search (partial match)
            account_type (str): Filter by account type
            limit (int): Maximum records to return

        Returns:
            List[Dict]: List of matching accounts
        """
        query = "SELECT Id, Name, Type, Industry, BillingCountry, Phone, Website, CreatedDate "
        query += "FROM Account WHERE IsDeleted = false"

        if name:
            query += f" AND Name LIKE '%{name}%'"
        if account_type:
            query += f" AND Type = '{account_type}'"

        query += f" ORDER BY Name LIMIT {limit}"

        return self.query(query)

    def get_all_accounts(self, account_type: str = None) -> List[Dict]:
        """
        Get all accounts, optionally filtered by type.

        Args:
            account_type (str): Filter by account type (e.g., 'Customer', 'Partner')

        Returns:
            List[Dict]: List of accounts
        """
        query = """
            SELECT Id, Name, Type, Industry, BillingCountry, BillingCity,
                   Phone, Website, CreatedDate, LastModifiedDate
            FROM Account
            WHERE IsDeleted = false
        """

        if account_type:
            query += f" AND Type = '{account_type}'"

        query += " ORDER BY Name"

        return self.query_all(query)

    def get_account_by_name(self, name: str) -> Optional[Dict]:
        """
        Get account by exact name match.

        Args:
            name (str): Exact account name

        Returns:
            Optional[Dict]: Account data or None if not found
        """
        results = self.query(f"SELECT Id, Name, Type, Industry FROM Account WHERE Name = '{name}' LIMIT 1")
        return results[0] if results else None

    # --- Opportunity Methods ---

    def get_opportunity(self, opp_id: str) -> Optional[Dict]:
        """
        Get opportunity by ID.

        Args:
            opp_id (str): Salesforce Opportunity ID

        Returns:
            Optional[Dict]: Opportunity data or None if not found
        """
        if not self.ensure_connected():
            return None

        try:
            return self.sf.Opportunity.get(opp_id)
        except Exception as e:
            print(f"✗ Error getting opportunity {opp_id}: {e}")
            return None

    def get_opportunities_by_account(self, account_id: str, include_closed: bool = False) -> List[Dict]:
        """
        Get all opportunities for an account.

        Args:
            account_id (str): Salesforce Account ID
            include_closed (bool): Whether to include closed opportunities

        Returns:
            List[Dict]: List of opportunities
        """
        query = f"""
            SELECT Id, Name, StageName, Amount, CloseDate, Probability,
                   Type, LeadSource, Description, CreatedDate, IsClosed, IsWon
            FROM Opportunity
            WHERE AccountId = '{account_id}'
        """

        if not include_closed:
            query += " AND IsClosed = false"

        query += " ORDER BY CloseDate DESC"

        return self.query(query)

    def get_opportunities_by_stage(self, stage: str, min_amount: float = None) -> List[Dict]:
        """
        Get opportunities by stage.

        Args:
            stage (str): Opportunity stage name
            min_amount (float): Minimum amount filter

        Returns:
            List[Dict]: List of opportunities
        """
        query = f"""
            SELECT Id, Name, StageName, Amount, CloseDate, Probability,
                   AccountId, Account.Name
            FROM Opportunity
            WHERE StageName = '{stage}'
        """

        if min_amount:
            query += f" AND Amount >= {min_amount}"

        query += " ORDER BY Amount DESC"

        return self.query(query)

    def get_all_open_opportunities(self) -> List[Dict]:
        """
        Get all open opportunities.

        Returns:
            List[Dict]: List of open opportunities
        """
        query = """
            SELECT Id, Name, StageName, Amount, CloseDate, Probability,
                   AccountId, Account.Name, Type, CreatedDate
            FROM Opportunity
            WHERE IsClosed = false
            ORDER BY CloseDate ASC
        """
        return self.query_all(query)

    # --- Contract Methods ---

    def get_contract(self, contract_id: str) -> Optional[Dict]:
        """
        Get contract by ID.

        Args:
            contract_id (str): Salesforce Contract ID

        Returns:
            Optional[Dict]: Contract data or None if not found
        """
        if not self.ensure_connected():
            return None

        try:
            return self.sf.Contract.get(contract_id)
        except Exception as e:
            print(f"✗ Error getting contract {contract_id}: {e}")
            return None

    def get_contracts_by_account(self, account_id: str, active_only: bool = True) -> List[Dict]:
        """
        Get contracts for an account.

        Args:
            account_id (str): Salesforce Account ID
            active_only (bool): Only return active contracts

        Returns:
            List[Dict]: List of contracts
        """
        query = f"""
            SELECT Id, ContractNumber, Status, StartDate, EndDate,
                   ContractTerm, Description, CreatedDate
            FROM Contract
            WHERE AccountId = '{account_id}'
        """

        if active_only:
            query += " AND Status = 'Activated'"

        query += " ORDER BY StartDate DESC"

        return self.query(query)

    def get_active_contracts(self, expiring_within_days: int = None) -> List[Dict]:
        """
        Get all active contracts.

        Args:
            expiring_within_days (int): Only return contracts expiring within N days

        Returns:
            List[Dict]: List of active contracts
        """
        query = """
            SELECT Id, ContractNumber, Status, StartDate, EndDate,
                   AccountId, Account.Name, ContractTerm, Description
            FROM Contract
            WHERE Status = 'Activated'
        """

        if expiring_within_days:
            future_date = (datetime.now() + timedelta(days=expiring_within_days)).strftime('%Y-%m-%d')
            query += f" AND EndDate <= {future_date}"

        query += " ORDER BY EndDate ASC"

        return self.query(query)

    # --- Contact Methods ---

    def get_contacts_by_account(self, account_id: str) -> List[Dict]:
        """
        Get contacts for an account.

        Args:
            account_id (str): Salesforce Account ID

        Returns:
            List[Dict]: List of contacts
        """
        query = f"""
            SELECT Id, Name, Title, Email, Phone, Department, CreatedDate
            FROM Contact
            WHERE AccountId = '{account_id}'
            ORDER BY Name
        """
        return self.query(query)

    def search_contacts(self, name: str = None, email: str = None, limit: int = 100) -> List[Dict]:
        """
        Search contacts.

        Args:
            name (str): Name to search
            email (str): Email to search
            limit (int): Maximum records

        Returns:
            List[Dict]: List of contacts
        """
        query = "SELECT Id, Name, Title, Email, Phone, AccountId, Account.Name FROM Contact WHERE IsDeleted = false"

        if name:
            query += f" AND Name LIKE '%{name}%'"
        if email:
            query += f" AND Email LIKE '%{email}%'"

        query += f" ORDER BY Name LIMIT {limit}"

        return self.query(query)

    # --- Full Account Details ---

    def get_full_account_details(self, account_id: str) -> Optional[Dict]:
        """
        Get comprehensive account details including opportunities and contracts.

        Args:
            account_id (str): Salesforce Account ID

        Returns:
            Optional[Dict]: Account with nested opportunities and contracts
        """
        account = self.get_account(account_id)
        if account:
            account['opportunities'] = self.get_opportunities_by_account(account_id, include_closed=True)
            account['contracts'] = self.get_contracts_by_account(account_id, active_only=False)
            account['contacts'] = self.get_contacts_by_account(account_id)
        return account

    # ==================== WRITE METHODS ====================

    def create_account(self, data: Dict) -> Optional[str]:
        """
        Create a new account.

        Args:
            data (Dict): Account data (Name is required)

        Returns:
            Optional[str]: New account ID or None if failed
        """
        if not self.ensure_connected():
            return None

        try:
            result = self.sf.Account.create(data)
            if result.get('success'):
                print(f"✓ Created account: {result['id']}")
                return result['id']
            else:
                print(f"✗ Failed to create account: {result}")
                return None
        except Exception as e:
            print(f"✗ Error creating account: {e}")
            return None

    def update_account(self, account_id: str, data: Dict) -> bool:
        """
        Update an existing account.

        Args:
            account_id (str): Account ID to update
            data (Dict): Fields to update

        Returns:
            bool: True if successful
        """
        if not self.ensure_connected():
            return False

        try:
            self.sf.Account.update(account_id, data)
            print(f"✓ Updated account: {account_id}")
            return True
        except Exception as e:
            print(f"✗ Error updating account {account_id}: {e}")
            return False

    def create_opportunity(self, data: Dict) -> Optional[str]:
        """
        Create a new opportunity.

        Args:
            data (Dict): Opportunity data (Name, StageName, CloseDate required)

        Returns:
            Optional[str]: New opportunity ID or None if failed
        """
        if not self.ensure_connected():
            return None

        try:
            result = self.sf.Opportunity.create(data)
            if result.get('success'):
                print(f"✓ Created opportunity: {result['id']}")
                return result['id']
            else:
                print(f"✗ Failed to create opportunity: {result}")
                return None
        except Exception as e:
            print(f"✗ Error creating opportunity: {e}")
            return None

    def update_opportunity(self, opp_id: str, data: Dict) -> bool:
        """
        Update an existing opportunity.

        Args:
            opp_id (str): Opportunity ID to update
            data (Dict): Fields to update

        Returns:
            bool: True if successful
        """
        if not self.ensure_connected():
            return False

        try:
            self.sf.Opportunity.update(opp_id, data)
            print(f"✓ Updated opportunity: {opp_id}")
            return True
        except Exception as e:
            print(f"✗ Error updating opportunity {opp_id}: {e}")
            return False

    def create_contract(self, data: Dict) -> Optional[str]:
        """
        Create a new contract.

        Args:
            data (Dict): Contract data (AccountId required)

        Returns:
            Optional[str]: New contract ID or None if failed
        """
        if not self.ensure_connected():
            return None

        try:
            result = self.sf.Contract.create(data)
            if result.get('success'):
                print(f"✓ Created contract: {result['id']}")
                return result['id']
            else:
                print(f"✗ Failed to create contract: {result}")
                return None
        except Exception as e:
            print(f"✗ Error creating contract: {e}")
            return None

    def update_contract(self, contract_id: str, data: Dict) -> bool:
        """
        Update an existing contract.

        Args:
            contract_id (str): Contract ID to update
            data (Dict): Fields to update

        Returns:
            bool: True if successful
        """
        if not self.ensure_connected():
            return False

        try:
            self.sf.Contract.update(contract_id, data)
            print(f"✓ Updated contract: {contract_id}")
            return True
        except Exception as e:
            print(f"✗ Error updating contract {contract_id}: {e}")
            return False

    def create_contact(self, data: Dict) -> Optional[str]:
        """
        Create a new contact.

        Args:
            data (Dict): Contact data (LastName required)

        Returns:
            Optional[str]: New contact ID or None if failed
        """
        if not self.ensure_connected():
            return None

        try:
            result = self.sf.Contact.create(data)
            if result.get('success'):
                print(f"✓ Created contact: {result['id']}")
                return result['id']
            else:
                print(f"✗ Failed to create contact: {result}")
                return None
        except Exception as e:
            print(f"✗ Error creating contact: {e}")
            return None

    def update_contact(self, contact_id: str, data: Dict) -> bool:
        """
        Update an existing contact.

        Args:
            contact_id (str): Contact ID to update
            data (Dict): Fields to update

        Returns:
            bool: True if successful
        """
        if not self.ensure_connected():
            return False

        try:
            self.sf.Contact.update(contact_id, data)
            print(f"✓ Updated contact: {contact_id}")
            return True
        except Exception as e:
            print(f"✗ Error updating contact {contact_id}: {e}")
            return False

    # ==================== ANALYSIS METHODS ====================

    def get_revenue_by_account(self, start_date: datetime = None, end_date: datetime = None) -> List[Dict]:
        """
        Get total closed-won revenue by account.

        Args:
            start_date (datetime): Start of date range
            end_date (datetime): End of date range

        Returns:
            List[Dict]: Revenue aggregated by account
        """
        date_filter = ""
        if start_date:
            date_filter += f" AND CloseDate >= {start_date.strftime('%Y-%m-%d')}"
        if end_date:
            date_filter += f" AND CloseDate <= {end_date.strftime('%Y-%m-%d')}"

        query = f"""
            SELECT Account.Name, Account.Id, SUM(Amount) TotalRevenue,
                   COUNT(Id) OpportunityCount
            FROM Opportunity
            WHERE IsWon = true {date_filter}
            GROUP BY Account.Name, Account.Id
            ORDER BY SUM(Amount) DESC
        """

        return self.query(query)

    def get_pipeline_by_stage(self) -> List[Dict]:
        """
        Get pipeline value grouped by stage.

        Returns:
            List[Dict]: Pipeline aggregated by stage
        """
        query = """
            SELECT StageName, SUM(Amount) TotalAmount,
                   COUNT(Id) OpportunityCount, AVG(Probability) AvgProbability
            FROM Opportunity
            WHERE IsClosed = false
            GROUP BY StageName
            ORDER BY SUM(Amount) DESC
        """

        return self.query(query)

    def get_revenue_forecast(self, months_ahead: int = 3) -> List[Dict]:
        """
        Get weighted revenue forecast for upcoming months.

        Args:
            months_ahead (int): Number of months to forecast

        Returns:
            List[Dict]: Forecast data with weighted amounts
        """
        future_date = (datetime.now() + timedelta(days=months_ahead * 30)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')

        query = f"""
            SELECT Account.Name, StageName, Amount, CloseDate, Probability
            FROM Opportunity
            WHERE IsClosed = false
              AND CloseDate >= {today}
              AND CloseDate <= {future_date}
            ORDER BY CloseDate ASC
        """

        results = self.query(query)

        # Calculate weighted amounts
        for opp in results:
            amount = opp.get('Amount') or 0
            probability = opp.get('Probability') or 0
            opp['WeightedAmount'] = amount * probability / 100

        return results

    def get_contract_coverage_summary(self) -> Dict:
        """
        Get summary of contract coverage across accounts.

        Returns:
            Dict: Coverage metrics including rates and counts
        """
        # Active customer accounts
        accounts_result = self.query("""
            SELECT COUNT(Id) TotalAccounts
            FROM Account
            WHERE Type = 'Customer' AND IsDeleted = false
        """)

        # Accounts with active contracts
        covered_result = self.query("""
            SELECT COUNT(DISTINCT AccountId) CoveredAccounts
            FROM Contract
            WHERE Status = 'Activated'
        """)

        total = accounts_result[0].get('TotalAccounts', 0) if accounts_result else 0
        covered = covered_result[0].get('CoveredAccounts', 0) if covered_result else 0

        return {
            'total_accounts': total,
            'covered_accounts': covered,
            'coverage_rate': (covered / total * 100) if total > 0 else 0,
            'uncovered_accounts': total - covered
        }

    def get_expiring_contracts(self, days: int = 30) -> List[Dict]:
        """
        Get contracts expiring within specified days.

        Args:
            days (int): Number of days to look ahead

        Returns:
            List[Dict]: Expiring contracts
        """
        return self.get_active_contracts(expiring_within_days=days)

    def get_accounts_without_contracts(self) -> List[Dict]:
        """
        Get active accounts that have no active contracts.

        Returns:
            List[Dict]: Accounts without contracts
        """
        query = """
            SELECT Id, Name, Type, CreatedDate
            FROM Account
            WHERE Type = 'Customer'
              AND IsDeleted = false
              AND Id NOT IN (
                  SELECT AccountId FROM Contract WHERE Status = 'Activated'
              )
            ORDER BY Name
        """

        return self.query(query)

    def get_account_health_metrics(self, account_id: str) -> Dict:
        """
        Get health metrics for a single account.

        Args:
            account_id (str): Salesforce Account ID

        Returns:
            Dict: Health metrics including revenue, pipeline, contracts
        """
        account = self.get_full_account_details(account_id)

        if not account:
            return {'error': 'Account not found'}

        # Calculate metrics
        active_contracts = [
            c for c in account.get('contracts', [])
            if c.get('Status') == 'Activated'
        ]
        closed_won_opps = [
            o for o in account.get('opportunities', [])
            if o.get('IsWon')
        ]
        open_opps = [
            o for o in account.get('opportunities', [])
            if not o.get('IsClosed')
        ]

        total_revenue = sum(o.get('Amount', 0) or 0 for o in closed_won_opps)
        pipeline_value = sum(o.get('Amount', 0) or 0 for o in open_opps)

        return {
            'account_id': account_id,
            'account_name': account.get('Name'),
            'active_contracts': len(active_contracts),
            'has_coverage': len(active_contracts) > 0,
            'total_opportunities': len(account.get('opportunities', [])),
            'closed_won': len(closed_won_opps),
            'open_pipeline': len(open_opps),
            'total_revenue': total_revenue,
            'pipeline_value': pipeline_value,
            'contacts_count': len(account.get('contacts', []))
        }


# ==================== MAIN ====================

if __name__ == "__main__":
    print("=" * 60)
    print("SALESFORCE CLIENT TEST")
    print("=" * 60)

    client = SalesforceClient()

    if client.connect():
        print("\n--- Testing Connection ---")
        client.test_connection()

        print("\n--- Listing Objects ---")
        objects = client.list_available_objects()
        key_objects = ['Account', 'Opportunity', 'Contract', 'Contact', 'Lead']
        print(f"Total objects: {len(objects)}")
        print("Key objects:")
        for obj in objects:
            if obj['name'] in key_objects:
                print(f"  - {obj['name']}: {obj['label']} (queryable: {obj['queryable']})")

        print("\n--- Sample Accounts ---")
        accounts = client.search_accounts(limit=5)
        for acc in accounts:
            print(f"  - {acc.get('Name')} ({acc.get('Type', 'N/A')})")
    else:
        print("\n✗ Could not connect to Salesforce")
        print("Please check your credentials in config.json")
