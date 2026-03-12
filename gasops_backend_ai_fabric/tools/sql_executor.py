

######################### connecting using Managed Identity #########################


import pyodbc
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from datetime import datetime
from typing import List, Dict, Any
import struct
import logging
import time
import os
from openai import AzureOpenAI  



# Suppress Azure Identity verbose logging
logging.getLogger('azure.identity').setLevel(logging.ERROR)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.ERROR)

# Enable ODBC connection pooling
pyodbc.pooling = True

# Global token cache
_token_cache = {
    "token": None,
    "expires_at": 0
}

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
FABRIC_SERVER = os.getenv("FABRIC_SERVER")
FABRIC_DATABASE = os.getenv("FABRIC_DATABASE")

# For User-Assigned MI only (leave unset for System-Assigned MI)
MANAGED_IDENTITY_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")

# ------------------------------------------------------------------------------
# Get Fabric Access Token
# ------------------------------------------------------------------------------
def get_fabric_token(force_refresh: bool = False) -> str:
    """
    Get Microsoft Fabric access token.
    
    Authentication Chain:
    1. Managed Identity (System or User-Assigned) - for Azure environments
    2. Azure CLI - for local/Docker development
    
    Args:
        force_refresh: Force token refresh even if cached token is valid
    
    Returns:
        str: Valid access token
    """
    global _token_cache
    now = time.time()

    # Return cached token if still valid (5 min buffer before expiration)
    if not force_refresh and _token_cache["token"] and now < (_token_cache["expires_at"] - 300):
        expires_in_min = (_token_cache["expires_at"] - now) / 60
        print(f"[Token] Using cached token (expires in {expires_in_min:.1f} min)")
        return _token_cache["token"]

    print("[Token] Acquiring new token...")
    
    try:
        credentials = []
        
        # 1. Add Managed Identity credential (System or User-Assigned)
        if MANAGED_IDENTITY_CLIENT_ID:
            print(f"[Token] Using User-Assigned MI: {MANAGED_IDENTITY_CLIENT_ID}")
            credentials.append(ManagedIdentityCredential(client_id=MANAGED_IDENTITY_CLIENT_ID))
        else:
            print("[Token] Using System-Assigned MI")
            credentials.append(ManagedIdentityCredential())
        
        # 2. Add Azure CLI as fallback for local development
        credentials.append(AzureCliCredential())
        
        # Create chained credential (tries in order)
        credential = ChainedTokenCredential(*credentials)
        
        # Get token for Microsoft Fabric
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        
        # Cache the token
        _token_cache["token"] = token.token
        _token_cache["expires_at"] = token.expires_on
        
        expires_in_min = (token.expires_on - now) / 60
        print(f"[Token] ✓ Token acquired (expires in {expires_in_min:.1f} min)")
        
        return token.token
        
    except Exception as e:
        print(f"[Token] ✗ ERROR: {str(e)}")
        print("\n[Token] Troubleshooting:")
        print("  • Azure Web App: Enable System-Assigned MI in Portal → Identity")
        print("  • Azure Web App: Grant MI 'Contributor' role on Fabric workspace")
        print("  • Local/Docker: Run 'az login' in terminal before starting app")
        print("  • Docker: Mount credentials with -v ~/.azure:/root/.azure:ro")
        raise Exception(f"Authentication failed: {str(e)}")

# ------------------------------------------------------------------------------
# Create Fabric SQL Connection
# ------------------------------------------------------------------------------
def get_fabric_connection(retry: int = 0) -> pyodbc.Connection:
    """
    Create connection to Microsoft Fabric SQL Endpoint using token authentication.

    Returns:
        pyodbc.Connection: Active database connection

    Raises:
        Exception: If connection fails
    """
    try:
        print(f"[Connection] Connecting to {FABRIC_SERVER}/{FABRIC_DATABASE}")

        # Get access token
        token_str = get_fabric_token()

        # Convert token to ODBC format (SQL_COPT_SS_ACCESS_TOKEN)
        token_bytes = token_str.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

        # Build connection string
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={FABRIC_SERVER};"
            f"DATABASE={FABRIC_DATABASE};"
            f"PORT=1433;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )

        # Establish connection with token authentication
        # 1256 = SQL_COPT_SS_ACCESS_TOKEN
        conn = pyodbc.connect(
            connection_string,
            attrs_before={1256: token_struct},
            timeout=30
        )

        print("[Connection] ✓ Connected successfully")
        return conn

    except pyodbc.Error as e:
        # Retry once if token/auth error
        if retry < 1 and ("token" in str(e).lower() or "authentication" in str(e).lower()):
            print("[Connection] Token error detected, retrying with fresh token...")
            _token_cache["token"] = None
            _token_cache["expires_at"] = 0
            return get_fabric_connection(retry=retry + 1)

        print(f"[Connection] ✗ Connection failed: {str(e)}")
        raise Exception(f"Fabric connection failed: {str(e)}")

    except Exception as e:
        print(f"[Connection] ✗ Unexpected error: {str(e)}")
        raise Exception(f"Connection error: {str(e)}")

# ------------------------------------------------------------------------------
# SQL Query Splitter
# ------------------------------------------------------------------------------
def split_sql_queries(sql_query: str) -> List[str]:
    """
    Split multiple SQL queries by semicolon, but respect string literals.
    Handles semicolons inside strings like 'task1; task2; task3'.
    
    Args:
        sql_query: SQL query or queries separated by semicolons
    
    Returns:
        List[str]: Individual SQL queries
    """
    queries = []
    current_query = []
    in_single_quote = False
    in_double_quote = False
    
    i = 0
    while i < len(sql_query):
        char = sql_query[i]
        
        # Handle escaped quotes
        if char == "'" and (i == 0 or sql_query[i-1] != '\\'):
            in_single_quote = not in_single_quote
            current_query.append(char)
        elif char == '"' and (i == 0 or sql_query[i-1] != '\\'):
            in_double_quote = not in_double_quote
            current_query.append(char)
        elif char == ';' and not in_single_quote and not in_double_quote:
            # This is a query separator
            query = ''.join(current_query).strip()
            if query:
                queries.append(query)
            current_query = []
        else:
            current_query.append(char)
        
        i += 1
    
    # Add last query if any
    query = ''.join(current_query).strip()
    if query:
        queries.append(query)
    
    return queries

# ------------------------------------------------------------------------------
# Execute SQL Query
# ------------------------------------------------------------------------------
def execute_sql_query(sql_query: str) -> List[Dict[str, Any]]:
    """
    Execute SQL query(ies) against Microsoft Fabric warehouse.
    Supports multiple SELECT statements separated by semicolons.
    
    Args:
        sql_query: SQL SELECT query or queries (semicolon-separated)
    
    Returns:
        List[Dict[str, Any]]: Query results as list of dictionaries
    
    Raises:
        Exception: If query execution fails after retries
    """
    # print(f"Query from agent: {sql_query}")
    max_retries = 2
    retry_count = 0

    while retry_count < max_retries:
        try:
            print(f"[Query] Attempt {retry_count + 1}/{max_retries}")
            print(f"[Query] SQL Preview: {sql_query}")
            
            # Get database connection
            conn = get_fabric_connection()
            cursor = conn.cursor()

            # Split queries intelligently (respecting string literals)
            queries = split_sql_queries(sql_query)
            print(f"[Query] Found {len(queries)} statement(s) to execute")
            all_results = []

            # Execute each query
            for idx, query in enumerate(queries, 1):
                print(f"[Query] Executing statement {idx}/{len(queries)}")
                cursor.execute(query)
                
                # Get column names
                columns = [col[0] for col in cursor.description]
                
                # Fetch all rows
                rows = cursor.fetchall()
                
                # Convert rows to list of dictionaries
                for row in rows:
                    record = {}
                    for i, col in enumerate(columns):
                        value = row[i]
                        # Handle datetime serialization
                        if isinstance(value, datetime):
                            value = value.strftime('%Y-%m-%d %H:%M:%S')
                        record[col] = value
                    all_results.append(record)

            # Clean up
            cursor.close()
            conn.close()
            
            print(f"[Query] ✓ Total: {len(all_results)} rows from {len(queries)} statement(s)")
            return all_results

        except pyodbc.Error as e:
            retry_count += 1
            print(f"[Query] ✗ Database error: {str(e)}")
            
            # Close connection on error
            try:
                cursor.close()
                conn.close()
            except:
                pass
            
            # Retry if token/auth error
            if retry_count < max_retries and ("token" in str(e).lower() or "authentication" in str(e).lower()):
                print("[Query] Retrying with fresh token...")
                _token_cache["token"] = None
                _token_cache["expires_at"] = 0
                continue
            
            raise Exception(f"Database error: {str(e)}")
        
        except Exception as e:
            retry_count += 1
            print(f"[Query] ✗ Unexpected error: {str(e)}")
            
            # Close connection on error
            try:
                cursor.close()
                conn.close()
            except:
                pass
            
            # Retry if token/auth error
            if retry_count < max_retries and ("token" in str(e).lower() or "authentication" in str(e).lower()):
                print("[Query] Retrying with fresh token...")
                _token_cache["token"] = None
                _token_cache["expires_at"] = 0
                continue
            
            raise Exception(f"Unexpected error: {str(e)}")

    raise Exception("Query execution failed after maximum retries")

# ------------------------------------------------------------------------------
# OpenAI Tool Definition
# ------------------------------------------------------------------------------
def get_sql_tool_definition() -> Dict[str, Any]:
    """
    Returns the OpenAI function tool definition for SQL query execution.
    
    Returns:
        Dict: Tool definition for OpenAI function calling
    """
    return {
        "type": "function",
        "function": {
            "name": "execute_sql_query",
            "description": "Executes a SQL SELECT query against the Microsoft Fabric warehouse and returns the results",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "The SQL SELECT query to execute. Must be a valid SELECT statement without markdown formatting."
                    }
                },
                "required": ["sql_query"]
            }
        }
    }
    
    
    

# correcting the errors in the SQL query using LLM and then executing the corrected query

def fix_sql_with_llm(failed_query: str, error_message: str) -> str:
    """
    Use LLM to correct SQL syntax/logic errors.
    
    Args:
        failed_query: The SQL that failed
        error_message: Database error message
    
    Returns:
        str: Corrected SQL query
    """

    
    # Correct Azure OpenAI client initialization
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),  # Or your API version
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")  # Just the base endpoint
    )
    
    correction_prompt = f"""You are a SQL error correction expert for Microsoft SQL Server / Microsoft Fabric Warehouse.

**Failed Query:**
```sql
{failed_query}
```

**Error Message:**
{error_message}

**Task:** Fix the SQL query to resolve the error. Common issues in Microsoft Fabric/SQL Server:

1. **STRING_AGG with DISTINCT (ERROR 102: "Incorrect syntax near ','"):**
   - ❌ WRONG: `STRING_AGG(DISTINCT column, ', ')`
   - ✅ RIGHT: `STRING_AGG(column, ', ') WITHIN GROUP (ORDER BY column)`
   - ✅ OR: Remove DISTINCT: `STRING_AGG(column, ', ')`
   
2. **Multiple queries:** Ensure each query ends with semicolon

3. **Column aliases:** Check for missing or duplicate aliases

4. **CAST/CONVERT:** Use proper SQL Server syntax

5. **Date functions:** Use GETDATE(), DATEADD(), etc.

**Rules:**
- Return ONLY the corrected SQL
- No markdown code blocks (no ```sql)
- No explanations
- Must end with semicolon
- Preserve the original query logic and structure

Corrected SQL:"""

    try:
        response = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            messages=[
                {"role": "system", "content": "You are a SQL syntax correction expert for Microsoft SQL Server. Return only corrected SQL without any formatting or explanations."},
                {"role": "user", "content": correction_prompt}
            ],
            temperature=0,  # Deterministic corrections
            max_tokens=2000
        )
        
        corrected_sql = response.choices[0].message.content.strip()
        
        # Clean up any markdown that might have slipped through
        corrected_sql = corrected_sql.replace("```sql", "").replace("```", "").strip()
        
        return corrected_sql
    
    except Exception as e:
        print(f"[SQL Fixer] ✗ LLM correction failed: {str(e)}")
        raise Exception(f"SQL correction failed: {str(e)}")


def execute_sql_query_with_retry(sql_query: str, max_correction_attempts: int = 2) -> List[Dict[str, Any]]:
    """
    Execute SQL with automatic error correction using LLM.
    
    Args:
        sql_query: Original SQL query
        max_correction_attempts: Maximum correction attempts (default: 2)
    
    Returns:
        List[Dict[str, Any]]: Query results
    
    Raises:
        Exception: If query fails after all correction attempts
    """
    attempt = 0
    current_query = sql_query
    original_query = sql_query
    
    while attempt <= max_correction_attempts:
        try:
            # Try executing the query
            print(f"[SQL Fixer] Execution attempt {attempt + 1}/{max_correction_attempts + 1}")
            results = execute_sql_query(current_query)
            
            if attempt > 0:
                print(f"[SQL Fixer] ✓ Query succeeded after {attempt} correction(s)")
                print(f"[SQL Fixer] Original query had syntax errors, corrected version executed successfully")
            
            return results
            
        except Exception as e:
            error_message = str(e)
            
            # Check if it's a connection/auth error - never try to fix these with LLM
            is_connection_error = any(keyword in error_message.lower() for keyword in [
                'authentication', 'login', 'connection', 'fabric connection', '18456', '28000'
            ])

            # Check if it's a SQL syntax error (not connection/auth error)
            is_syntax_error = not is_connection_error and any(keyword in error_message.lower() for keyword in [
                'syntax', 'incorrect', 'invalid', 'near',
                'missing', 'unrecognized', 'cannot parse', '42000'
            ])
            
            if not is_syntax_error:
                # Not a syntax error (connection, auth, timeout, etc.) - don't try to fix
                print(f"[SQL Fixer] Non-syntax error detected, not attempting correction")
                raise
            
            attempt += 1
            print(f"[SQL Fixer] ✗ Attempt {attempt}/{max_correction_attempts + 1} failed: {error_message}")
            
            if attempt > max_correction_attempts:
                print(f"[SQL Fixer] ✗ Max correction attempts ({max_correction_attempts}) reached")
                raise Exception(f"SQL execution failed after {max_correction_attempts} correction attempts. Last error: {error_message}")
            
            # Call LLM to fix the query
            print(f"[SQL Fixer] Requesting LLM correction (attempt {attempt})...")
            try:
                current_query = fix_sql_with_llm(current_query, error_message)
                print(f"[SQL Fixer] LLM suggested correction:")
                print(f"[SQL Fixer] {current_query[:300]}{'...' if len(current_query) > 300 else ''}")
            except Exception as fix_error:
                print(f"[SQL Fixer] ✗ LLM correction failed: {str(fix_error)}")
                raise Exception(f"Could not correct SQL: {error_message}")
    
    # Should never reach here
    raise Exception("SQL execution failed unexpectedly")

