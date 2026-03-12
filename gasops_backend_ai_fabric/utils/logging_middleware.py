import os
from fastapi import Request
from datetime import datetime
import time
from utils.blob_logger import get_blob_logger
import logging
import json

logger = logging.getLogger(__name__)

async def log_requests_middleware(request: Request, call_next):
    """Middleware to log all API requests"""
    start_time = time.time()
    
    # Get request details
    user_id = request.headers.get("user-id", "anonymous")
    path = request.url.path
    method = request.method
    
    # Capture request body for POST requests
    query_text = None
    request_body = None
    
    if method == "POST" and path == "/ask":
        try:
            body_bytes = await request.body()
            request_body = json.loads(body_bytes)
            query_text = request_body.get("query", "")
            
            # Recreate request
            async def receive():
                return {"type": "http.request", "body": body_bytes}
            request._receive = receive
        except Exception as e:
            logger.error(f"Failed to read request body: {e}")
            query_text = path
    else:
        query_text = path
    
    # Process request
    response = None
    error_message = ""
    response_status = "success"
    status_code = 200
    response_text = ""
    
    try:
        response = await call_next(request)
        status_code = response.status_code
        
        # Capture response body
        if path == "/ask" and status_code == 200:
            try:
                response_body = b""
                async for chunk in response.body_iterator:
                    response_body += chunk
                
                response_json = json.loads(response_body)
                response_text = response_json.get("answer", "")[:1000]
                
                from starlette.responses import Response
                response = Response(
                    content=response_body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type
                )
            except Exception as e:
                logger.error(f"Failed to capture response: {e}")
        
        if response.status_code >= 400:
            response_status = "error"
    except Exception as e:
        response_status = "error"
        error_message = str(e)[:1000]
        status_code = 500
        logger.error(f"Request processing error: {e}")
        raise
    finally:
        response_time_ms = (time.time() - start_time) * 1000
        
        # Determine agent type
        agent_type = "unknown"
        path_lower = path.lower()
        if "/ask" in path_lower:
            agent_type = "chat"
        elif "mtr" in path_lower:
            agent_type = "mtr"
        elif "edi" in path_lower:
            agent_type = "edidata"
        elif "analysis" in path_lower or "analyze" in path_lower:
            agent_type = "analysis"
        elif "eventgrid" in path_lower:
            agent_type = "eventgrid"
        elif "health" in path_lower:
            agent_type = "health"
        
        # Get detailed log info from request state
        log_details = getattr(request.state, 'log_details', {})
        
        # # Log to blob storage
        # try:
        #     blob_logger = get_blob_logger()
        #     log_data = {
        #         'timestamp': datetime.now(),
        #         'user_id': user_id,
        #         'login_master_id': log_details.get('login_master_id', ''),
        #         'database_name': log_details.get('database_name', ''),
        #         'org_id': log_details.get('org_id', ''),
        #         'query': query_text or path,
        #         'rewritten_query': log_details.get('rewritten_query', ''),
        #         'agent_routed': log_details.get('agent_routed', ''),
        #         'sql_query': log_details.get('sql_query', ''),
        #         'response': response_text,
        #         'agent_type': agent_type,
        #         'response_status': response_status,
        #         'response_time_ms': round(response_time_ms, 2),
        #         'error_message': error_message,
        #         'metadata': {
        #             'method': method,
        #             'path': path,
        #             'client_host': request.client.host if request.client else None,
        #             'status_code': status_code
        #         }
        #     }
        #     blob_logger.log_request(log_data)
        #     logger.info(f"Successfully logged request to blob: {path}")
        # except Exception as e:
        #     logger.error(f"Failed to log to blob storage: {str(e)}", exc_info=True)
        
        
        
         # Log to blob storage ONLY in production
        IS_PRODUCTION = os.getenv("ENVIRONMENT", "LOCAL").upper() != "LOCAL"
        
        if IS_PRODUCTION:
            try:
                blob_logger = get_blob_logger()
                log_data = {
                    'timestamp': datetime.now(),
                    'user_id': user_id,
                    'login_master_id': log_details.get('login_master_id', ''),
                    'database_name': log_details.get('database_name', ''),
                    'org_id': log_details.get('org_id', ''),
                    'query': query_text or path,
                    'rewritten_query': log_details.get('rewritten_query', ''),
                    'agent_routed': log_details.get('agent_routed', ''),
                    'sql_query': log_details.get('sql_query', ''),
                    'response': response_text,
                    'agent_type': agent_type,
                    'response_status': response_status,
                    'response_time_ms': round(response_time_ms, 2),
                    'error_message': error_message,
                    'metadata': {
                        'method': method,
                        'path': path,
                        'client_host': request.client.host if request.client else None,
                        'status_code': status_code
                    }
                }
                blob_logger.log_request(log_data)
                logger.info(f"Successfully logged request to blob: {path}")
            except Exception as e:
                logger.error(f"Failed to log to blob storage: {str(e)}", exc_info=True)
        else:
            logger.info(f"Skipping blob logging (LOCAL mode): {path}")
    
    return response