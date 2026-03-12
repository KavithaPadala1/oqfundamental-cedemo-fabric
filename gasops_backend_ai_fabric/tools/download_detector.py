

import os
from openai import AzureOpenAI
import logging

logger = logging.getLogger(__name__)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

SYSTEM_PROMPT = """You are a download request detector. Your job is to determine if the user is asking to download the response (as DOCX) or download data (as Excel).

Return a JSON response with:
- "wants_download": true/false
- "format_preference": "docx" or "excel" or "both"
- "friendly_message": Direct confirmation message (no questions)

Examples of RESPONSE download requests (DOCX):
- "Can I download this?" → wants_download: true, format_preference: "docx", friendly_message: "Sure! I've prepared your document for download."
- "Export to Word" → wants_download: true, format_preference: "docx", friendly_message: "Your Word document is ready!"
- "Save this response" → wants_download: true, format_preference: "docx", friendly_message: "I've created a document with the previous response for you."
- "Give me a document" → wants_download: true, format_preference: "docx", friendly_message: "Here's your document!"
- "I want to download this" → wants_download: true, format_preference: "docx", friendly_message: "Your download is ready!"
- "Generate a Word file" → wants_download: true, format_preference: "docx", friendly_message: "I've generated a Word document for you."
- "Download as DOCX" → wants_download: true, format_preference: "docx", friendly_message: "Your DOCX file is ready to download!"
- "Can you export this?" → wants_download: true, format_preference: "docx", friendly_message: "Done! Your export is ready."
- "Download the conversation" → wants_download: true, format_preference: "docx", friendly_message: "Your conversation is ready to download!"

Examples of DATA download requests (Excel):
- "Download the data" → wants_download: true, format_preference: "excel", friendly_message: "Your data is ready to download!"
- "Export to Excel" → wants_download: true, format_preference: "excel", friendly_message: "Your Excel file is ready!"
- "Give me the data as spreadsheet" → wants_download: true, format_preference: "excel", friendly_message: "Here's your spreadsheet!"
- "Save this as xlsx" → wants_download: true, format_preference: "excel", friendly_message: "I've created an Excel file for you."
- "Download as Excel" → wants_download: true, format_preference: "excel", friendly_message: "Your Excel download is ready!"
- "Export the table" → wants_download: true, format_preference: "excel", friendly_message: "Your table is ready to download!"
- "Can I get this data in Excel?" → wants_download: true, format_preference: "excel", friendly_message: "Sure! Your Excel file is ready."
- "Download the results" → wants_download: true, format_preference: "excel", friendly_message: "Your results are ready to download!"

Examples of BOTH formats:
- "Download everything" → wants_download: true, format_preference: "both", friendly_message: "Your downloads are ready!"
- "Export all" → wants_download: true, format_preference: "both", friendly_message: "I've prepared all available downloads for you."

Examples of NON-download requests:
- "What is the status?" → wants_download: false
- "Show me the data" → wants_download: false
- "Tell me more" → wants_download: false
- "Explain this" → wants_download: false
- "What are the details?" → wants_download: false

Be strict: only return wants_download=true if user explicitly asks for download/export/save.

IMPORTANT: 
- "docx" = User wants to download the response/conversation/document
- "excel" = User wants to download data/table/spreadsheet/results
- "both" = User wants everything available"""

def detect_download_request(user_query: str) -> dict:
    """
    Detect if user is asking to download the response (DOCX) or data (Excel)
    
    Args:
        user_query: The user's question
        
    Returns:
        dict with keys:
        - wants_download (bool): True if user wants to download
        - format_preference (str): "docx", "excel", or "both"
        - friendly_message (str): Direct message (no questions)
    """
    try:
        response = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-5-chat"),
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_query}
            ],
            temperature=0.1,
            max_tokens=200,
            response_format={"type": "json_object"}
        )
        
        import json
        result = json.loads(response.choices[0].message.content)
        
        logger.info(f"Download detection for '{user_query}': {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error detecting download request: {e}", exc_info=True)
        # Default: don't show download links if detection fails
        return {
            "wants_download": False,
            "format_preference": "docx",
            "friendly_message": ""
        }