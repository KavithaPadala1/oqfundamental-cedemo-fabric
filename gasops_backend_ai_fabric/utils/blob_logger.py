import os
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import io
import logging

logger = logging.getLogger(__name__)

class BlobStorageLogger:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = "gasopsoqfundamentallogs"
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment variables")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self._ensure_container_exists()
    
    def _ensure_container_exists(self):
        """Create container if it doesn't exist"""
        try:
            self.blob_service_client.create_container(self.container_name)
            logger.info(f"Container '{self.container_name}' created successfully")
        except ResourceExistsError:
            logger.info(f"Container '{self.container_name}' already exists")
        except Exception as e:
            logger.error(f"Error creating container: {str(e)}")
    
    def log_request(self, log_data: dict):
        """Log a single request to Excel file in blob storage"""
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            blob_name = f"logs_{date_str}.xlsx"
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Try to download existing file
            try:
                download_stream = blob_client.download_blob()
                existing_data = download_stream.readall()
                df = pd.read_excel(io.BytesIO(existing_data))
            except Exception:
                # File doesn't exist, create new DataFrame
                df = pd.DataFrame(columns=[
                    'Timestamp', 'User_ID', 'LoginMasterID', 'Database_Name', 'OrgID',
                    'Original_Query', 'Rewritten_Query', 'Agent_Routed', 'SQL_Query',
                    'Response', 'Agent_Type', 'Response_Status', 'Response_Time_MS', 
                    'Error_Message', 'Metadata'
                ])
            
            # Append new log entry
            new_row = {
                'Timestamp': log_data.get('timestamp', datetime.now()),
                'User_ID': log_data.get('user_id', ''),
                'LoginMasterID': log_data.get('login_master_id', ''),
                'Database_Name': log_data.get('database_name', ''),
                'OrgID': log_data.get('org_id', ''),
                'Original_Query': log_data.get('query', '')[:500],
                'Rewritten_Query': log_data.get('rewritten_query', '')[:500],
                'Agent_Routed': log_data.get('agent_routed', ''),
                'SQL_Query': log_data.get('sql_query', '')[:2000],
                'Response': log_data.get('response', '')[:1000],
                'Agent_Type': log_data.get('agent_type', ''),
                'Response_Status': log_data.get('response_status', ''),
                'Response_Time_MS': log_data.get('response_time_ms', 0),
                'Error_Message': log_data.get('error_message', '')[:1000],
                'Metadata': str(log_data.get('metadata', {}))[:500]
            }
            
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            # Convert DataFrame to Excel in memory
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Logs')
            
            output.seek(0)
            
            # Upload to blob storage
            blob_client.upload_blob(output, overwrite=True)
            logger.info(f"Log entry added to {blob_name}")
            
        except Exception as e:
            logger.error(f"Error logging to blob storage: {str(e)}")

# Singleton instance
_blob_logger = None

def get_blob_logger():
    """Get or create blob logger instance"""
    global _blob_logger
    if _blob_logger is None:
        _blob_logger = BlobStorageLogger()
    return _blob_logger