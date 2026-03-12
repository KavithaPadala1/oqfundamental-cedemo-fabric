# Azure client configuration

import openai
# from config import keyvault
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file



AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_MODEL_NAME = os.getenv("AZURE_OPENAI_MODEL_NAME")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

# Create AzureOpenAI client (OpenAI SDK v1+)
azure_client = openai.AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)


# azure_client = OpenAI(
#     base_url=AZURE_OPENAI_ENDPOINT,
#     api_key=AZURE_OPENAI_API_KEY
# )


def get_azure_chat_openai():
    """
    Returns the AzureOpenAI client and deployment name for use in agents.
    """
    return azure_client, AZURE_OPENAI_DEPLOYMENT

# Usage in agents:
# azure_client, azureopenai = get_azure_chat_openai()
# response = azure_client.chat.completions.create(
#     model=azureopenai,
#     messages=[{"role": "user", "content": prompt}]
# )



##### Load balance for azure openai models  #####
# Azure client configuration with load balancing and token usage tracking
# Azure client configuration with load balancing and token usage tracking
# Azure client configuration with load balancing and token usage tracking

# import openai
# from dotenv import load_dotenv
# import os
# from openai import OpenAI
# import random
# from typing import Tuple, Optional, Dict
# import logging
# from datetime import datetime
# import threading

# load_dotenv()  # Load environment variables from .env 

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Primary Model Configuration - GasOpsOpenAI-eastus-2
# AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
# AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
# AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
# AZURE_OPENAI_MODEL_NAME = os.getenv("AZURE_OPENAI_MODEL_NAME")
# AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

# # Secondary Model Configuration - Prod-Azure openai1
# AZURE_OPENAI_ENDPOINT_2 = os.getenv("AZURE_OPENAI_ENDPOINT_2")
# AZURE_OPENAI_API_KEY_2 = os.getenv("AZURE_OPENAI_API_KEY_2")
# AZURE_OPENAI_DEPLOYMENT_2 = os.getenv("AZURE_OPENAI_DEPLOYMENT_2")
# AZURE_OPENAI_MODEL_NAME_2 = os.getenv("AZURE_OPENAI_MODEL_NAME_2")
# AZURE_OPENAI_API_VERSION_2 = os.getenv("AZURE_OPENAI_API_VERSION_2")


# class TokenUsageTracker:
#     """Simple tracker to monitor token usage per model."""
    
#     def __init__(self, model_name: str):
#         self.model_name = model_name
#         self.lock = threading.Lock()
        
#         # Cumulative statistics
#         self.total_requests = 0
#         self.total_prompt_tokens = 0
#         self.total_completion_tokens = 0
#         self.total_tokens = 0
        
#         # Request history (last 100 requests)
#         self.request_history = []
#         self.max_history = 100
        
#     def add_usage(self, prompt_tokens: int, completion_tokens: int, total_tokens: int):
#         """Record token usage from a request."""
#         with self.lock:
#             self.total_requests += 1
#             self.total_prompt_tokens += prompt_tokens
#             self.total_completion_tokens += completion_tokens
#             self.total_tokens += total_tokens
            
#             # Store request history
#             request_record = {
#                 "timestamp": datetime.now().isoformat(),
#                 "prompt_tokens": prompt_tokens,
#                 "completion_tokens": completion_tokens,
#                 "total_tokens": total_tokens
#             }
            
#             self.request_history.append(request_record)
            
#             # Keep only last N requests
#             if len(self.request_history) > self.max_history:
#                 self.request_history.pop(0)
    
#     def get_statistics(self) -> Dict:
#         """Get usage statistics for this model."""
#         with self.lock:
#             avg_prompt_tokens = self.total_prompt_tokens / self.total_requests if self.total_requests > 0 else 0
#             avg_completion_tokens = self.total_completion_tokens / self.total_requests if self.total_requests > 0 else 0
#             avg_total_tokens = self.total_tokens / self.total_requests if self.total_requests > 0 else 0
            
#             return {
#                 "model": self.model_name,
#                 "total_requests": self.total_requests,
#                 "total_prompt_tokens": self.total_prompt_tokens,
#                 "total_completion_tokens": self.total_completion_tokens,
#                 "total_tokens": self.total_tokens,
#                 "avg_prompt_tokens": round(avg_prompt_tokens, 2),
#                 "avg_completion_tokens": round(avg_completion_tokens, 2),
#                 "avg_total_tokens": round(avg_total_tokens, 2),
#                 "recent_requests": self.request_history[-10:]  # Last 10 requests
#             }


# class _CompletionsWrapper:
#     """Wrapper for completions to track token usage."""
    
#     def __init__(self, completions, deployment, model_name, tracker_name, tracker):
#         self._completions = completions
#         self._deployment = deployment
#         self._model_name = model_name
#         self._tracker_name = tracker_name
#         self._tracker = tracker
    
#     def create(self, **kwargs):
#         logger.info(f"\n{'='*80}")
#         logger.info(f" MAKING AZURE OPENAI CALL")
#         logger.info(f"{'='*80}")
#         logger.info(f" Model: {self._tracker_name}")
#         logger.info(f" Deployment: {self._deployment}")
        
#         response = self._completions.create(**kwargs)
        
#         # Track token usage
#         if hasattr(response, 'usage') and response.usage:
#             prompt_tokens = response.usage.prompt_tokens
#             completion_tokens = response.usage.completion_tokens
#             total_tokens = response.usage.total_tokens
            
#             if self._tracker:
#                 self._tracker.add_usage(prompt_tokens, completion_tokens, total_tokens)
            
#             logger.info(f"\n{'='*80}")
#             logger.info(f"✅ SUCCESS - RESPONSE RECEIVED")
#             logger.info(f"{'='*80}")
#             logger.info(f" RESPONDING MODEL: {self._tracker_name}")
#             logger.info(f" DEPLOYMENT: {self._deployment}")
#             logger.info(f" MODEL NAME: {self._model_name}")
#             logger.info(f"\n TOKEN USAGE FOR THIS REQUEST:")
#             logger.info(f"   ├─ Input Tokens (Prompt):     {prompt_tokens:,}")
#             logger.info(f"   ├─ Output Tokens (Response):  {completion_tokens:,}")
#             logger.info(f"   └─ Total Tokens:              {total_tokens:,}")
#             logger.info(f"{'='*80}\n")
#         else:
#             logger.info(f"\n{'='*80}")
#             logger.info(f"✅ SUCCESS - RESPONSE RECEIVED (No token data)")
#             logger.info(f"{'='*80}")
#             logger.info(f" RESPONDING MODEL: {self._tracker_name}")
#             logger.info(f"{'='*80}\n")
        
#         return response


# class _ChatWrapper:
#     """Wrapper for chat to intercept completions."""
    
#     def __init__(self, chat, deployment, model_name, tracker_name, tracker):
#         self._chat = chat
#         self._deployment = deployment
#         self._model_name = model_name
#         self._tracker_name = tracker_name
#         self._tracker = tracker
    
#     @property
#     def completions(self):
#         return _CompletionsWrapper(
#             self._chat.completions, 
#             self._deployment, 
#             self._model_name, 
#             self._tracker_name, 
#             self._tracker
#         )


# class TrackedAzureOpenAI:
#     """Wrapper around AzureOpenAI client to track which model responds and token usage."""
    
#     def __init__(self, client, deployment, model_name, tracker_name, tracker):
#         self._client = client
#         self._deployment = deployment
#         self._model_name = model_name
#         self._tracker_name = tracker_name
#         self._tracker = tracker
    
#     @property
#     def chat(self):
#         return _ChatWrapper(
#             self._client.chat, 
#             self._deployment, 
#             self._model_name, 
#             self._tracker_name, 
#             self._tracker
#         )


# # Create multiple Azure OpenAI clients for load balancing
# azure_clients = [
#     {
#         "client": openai.AzureOpenAI(
#             api_key=AZURE_OPENAI_API_KEY,
#             api_version=AZURE_OPENAI_API_VERSION,
#             azure_endpoint=AZURE_OPENAI_ENDPOINT,
#         ),
#         "deployment": AZURE_OPENAI_DEPLOYMENT,
#         "model_name": AZURE_OPENAI_MODEL_NAME,
#         "name": "Primary Model (eastus-2)",
#         "tracker": TokenUsageTracker(AZURE_OPENAI_MODEL_NAME)
#     },
#     {
#         "client": openai.AzureOpenAI(
#             api_key=AZURE_OPENAI_API_KEY_2,
#             api_version=AZURE_OPENAI_API_VERSION_2,
#             azure_endpoint=AZURE_OPENAI_ENDPOINT_2,
#         ),
#         "deployment": AZURE_OPENAI_DEPLOYMENT_2,
#         "model_name": AZURE_OPENAI_MODEL_NAME_2,
#         "name": "Secondary Model (Prod-AI-OpenAI1)",
#         "tracker": TokenUsageTracker(AZURE_OPENAI_MODEL_NAME_2)
#     }
# ]

# # Counter for round-robin selection
# _current_client_index = 0
# _lock = threading.Lock()


# def get_azure_chat_openai(strategy: str = "round_robin") -> Tuple[TrackedAzureOpenAI, str]:
#     """
#     Returns the AzureOpenAI client and deployment name with load balancing.
#     Returns a wrapped client that automatically tracks token usage.
    
#     Args:
#         strategy: Load balancing strategy 
#                  - 'round_robin': Distributes requests evenly across models (default)
#                  - 'random': Randomly selects a model
#                  - 'primary_first': Always uses primary model first
    
#     Returns:
#         Tuple of (tracked_azure_client, deployment_name)
#     """
#     global _current_client_index
    
#     if strategy == "round_robin":
#         with _lock:
#             selected = azure_clients[_current_client_index]
#             _current_client_index = (_current_client_index + 1) % len(azure_clients)
#         logger.info(f"🔄 [ROUND-ROBIN] Selected {selected['name']} | Deployment: {selected['deployment']}")
        
#     elif strategy == "random":
#         selected = random.choice(azure_clients)
#         logger.info(f"🎲 [RANDOM] Selected {selected['name']} | Deployment: {selected['deployment']}")
        
#     else:  # primary_first
#         selected = azure_clients[0]
#         logger.info(f"⭐ [PRIMARY] Selected {selected['name']} | Deployment: {selected['deployment']}")
    
#     # Return tracked client that will automatically log token usage
#     tracked_client = TrackedAzureOpenAI(
#         selected["client"],
#         selected["deployment"],
#         selected["model_name"],
#         selected["name"],
#         selected["tracker"]
#     )
    
#     return tracked_client, selected["deployment"]


# def get_azure_chat_completion_with_fallback(messages: list, **kwargs) -> Optional[object]:
#     """
#     Makes a chat completion request with automatic fallback and token tracking.
#     This function tries each model in sequence until one succeeds.
    
#     Args:
#         messages: List of message dictionaries for the chat
#         **kwargs: Additional parameters (temperature, max_tokens, etc.)
    
#     Returns:
#         Chat completion response object with token usage tracked
        
#     Raises:
#         Exception: If all models fail to respond
#     """
#     errors = []
    
#     # Try each client in sequence
#     for idx, client_config in enumerate(azure_clients):
#         try:
#             logger.info(f"\n{'='*80}")
#             logger.info(f"ATTEMPTING REQUEST #{idx+1}")
#             logger.info(f"{'='*80}")
#             logger.info(f" Model: {client_config['name']}")
#             logger.info(f" Deployment: {client_config['deployment']}")
#             logger.info(f" Model Name: {client_config['model_name']}")
            
#             response = client_config["client"].chat.completions.create(
#                 model=client_config["deployment"],
#                 messages=messages,
#                 **kwargs
#             )
            
#             # Track token usage from response
#             if hasattr(response, 'usage') and response.usage:
#                 prompt_tokens = response.usage.prompt_tokens
#                 completion_tokens = response.usage.completion_tokens
#                 total_tokens = response.usage.total_tokens
                
#                 client_config["tracker"].add_usage(prompt_tokens, completion_tokens, total_tokens)
                
#                 logger.info(f"\n{'='*80}")
#                 logger.info(f"✅ SUCCESS - RESPONSE RECEIVED")
#                 logger.info(f"{'='*80}")
#                 logger.info(f" RESPONDING MODEL: {client_config['name']}")
#                 logger.info(f" DEPLOYMENT: {client_config['deployment']}")
#                 logger.info(f" MODEL NAME: {client_config['model_name']}")
#                 logger.info(f"\n TOKEN USAGE FOR THIS REQUEST:")
#                 logger.info(f"   ├─ Input Tokens (Prompt):     {prompt_tokens:,}")
#                 logger.info(f"   ├─ Output Tokens (Response):  {completion_tokens:,}")
#                 logger.info(f"   └─ Total Tokens:              {total_tokens:,}")
#                 logger.info(f"{'='*80}\n")
#             else:
#                 logger.info(f"\n{'='*80}")
#                 logger.info(f"✅ SUCCESS - RESPONSE RECEIVED")
#                 logger.info(f"{'='*80}")
#                 logger.info(f" RESPONDING MODEL: {client_config['name']}")
#                 logger.info(f" DEPLOYMENT: {client_config['deployment']}")
#                 logger.info(f" WARNING  No token usage data available from API")
#                 logger.info(f"{'='*80}\n")
            
#             return response
            
#         except openai.RateLimitError as e:
#             logger.warning(f"\n WARNING  RATE LIMIT EXCEEDED on {client_config['name']}")
#             logger.warning(f"   Trying next model...\n")
#             errors.append(f"{client_config['name']}: Rate limit exceeded")
#             continue
            
#         except openai.APIError as e:
#             logger.error(f"\n API ERROR on {client_config['name']}: {e}")
#             logger.error(f"   Trying next model...\n")
#             errors.append(f"{client_config['name']}: API Error - {str(e)}")
#             continue
            
#         except Exception as e:
#             logger.error(f"\n UNEXPECTED ERROR on {client_config['name']}: {e}")
#             logger.error(f"   Trying next model...\n")
#             errors.append(f"{client_config['name']}: {str(e)}")
#             continue
    
#     # All models failed
#     error_msg = f"All Azure OpenAI models failed: {'; '.join(errors)}"
#     logger.error(f"\n{'='*80}")
#     logger.error(f"ALL MODELS FAILED")
#     logger.error(f"{'='*80}")
#     logger.error(error_msg)
#     logger.error(f"{'='*80}\n")
#     raise Exception(error_msg)


# def get_token_usage_statistics() -> Dict:
#     """
#     Get token usage statistics for all models.
    
#     Returns:
#         Dictionary with usage stats for all models
#     """
#     stats = {
#         "timestamp": datetime.now().isoformat(),
#         "models": []
#     }
    
#     for client_config in azure_clients:
#         model_stats = client_config["tracker"].get_statistics()
#         stats["models"].append(model_stats)
    
#     return stats


# def log_token_usage_statistics():
#     """Log current token usage statistics in a readable format."""
#     stats = get_token_usage_statistics()
    
#     logger.info("\n" + "=" * 80)
#     logger.info("TOKEN USAGE STATISTICS - SUMMARY")
#     logger.info("=" * 80)
    
#     for model_stat in stats["models"]:
#         logger.info(f"\n🔹 MODEL: {model_stat['model']}")
#         logger.info(f"   {'─' * 70}")
#         logger.info(f"   Total Requests:                {model_stat['total_requests']:,}")
#         logger.info(f"   Total Input Tokens (Prompts):  {model_stat['total_prompt_tokens']:,}")
#         logger.info(f"   Total Output Tokens (Replies): {model_stat['total_completion_tokens']:,}")
#         logger.info(f"   Total Tokens Used:             {model_stat['total_tokens']:,}")
#         logger.info(f"   {'─' * 70}")
#         logger.info(f"   Avg Input Tokens/Request:      {model_stat['avg_prompt_tokens']}")
#         logger.info(f"   Avg Output Tokens/Request:     {model_stat['avg_completion_tokens']}")
#         logger.info(f"   Avg Total Tokens/Request:      {model_stat['avg_total_tokens']}")
        
#         if model_stat['recent_requests']:
#             logger.info(f"\n  LAST 5 REQUESTS:")
#             for i, req in enumerate(model_stat['recent_requests'][-5:], 1):
#                 logger.info(f"      {i}. {req['timestamp']}")
#                 logger.info(f"         Input: {req['prompt_tokens']}, Output: {req['completion_tokens']}, Total: {req['total_tokens']}")
    
#     logger.info("\n" + "=" * 80 + "\n")


# # Backward compatibility - returns primary client by default
# azure_client = TrackedAzureOpenAI(
#     azure_clients[0]["client"],
#     azure_clients[0]["deployment"],
#     azure_clients[0]["model_name"],
#     azure_clients[0]["name"],
#     azure_clients[0]["tracker"]
# )

# # Export for backward compatibility
# def get_azure_chat_openai_legacy():
#     """
#     Legacy function for backward compatibility.
#     Returns primary client and deployment.
#     """
#     return azure_client, azure_clients[0]["deployment"]