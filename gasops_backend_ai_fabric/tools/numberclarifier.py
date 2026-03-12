from config.azure_client import get_azure_chat_openai
from tools.sql_executor import execute_sql_query
import json
import re


async def number_clarifier_llm(query: str, auth_token=None):
    """
    Identifies the category of an ambiguous number in the user query.
    Can either rewrite the query for routing OR answer verification questions directly.
    
    Args:
        query: User's original query containing an ambiguous number
        auth_token: Authentication token for database access
    
    Returns:
        dict: Contains either rewritten query OR direct answer for verification questions
    """
    
    # Get Azure OpenAI client
    azure_client, azureopenai = get_azure_chat_openai()
    
    # Schema for number categories
    schema = """
    TABLE: vm_cedemo_companyemployees_active
    - ITSID (varchar): ITS identifier for the company employee.
    - EmployeeNumber (varchar): Unique employee number for the company employee.
    
    TABLE: vm_cedemo_contractoremployees_active
    - ITSID (varchar): ITS identifier for the contractor employee.
    """
    
    # Prompt for LLM to generate SQL query
    prompt = f"""
    You are a number classifier that identifies ambiguous numbers in user queries.
    
    User Query: {query}
    
    {schema}
    
    Your task:
    1. Extract the numeric employee identifier from the query
       - Employee identifiers are purely NUMERIC (no letters or special characters)
       - Examples:
         * "Show me employee 7653" → extract "7653"
         * "Is 12345 an ITSID?" → extract "12345"
         * "What qualifications does 9876 have?" → extract "9876"
       
    2. Generate a SQL query to check all three employee categories with EXACT MATCHING to execute in SQL Server (Microsoft Fabric Data Warehouse)
    
    Generate a combined SQL query with UNION for exact numeric matching:
    
    IMPORTANT: Use SQL Server syntax with '+' for string concatenation (NOT '||').

    SELECT COUNT(*) as count, 'CompanyEmployeeITSID' as category, MAX(ITSID) as matched_value 
    FROM vm_cedemo_companyemployees_active 
    WHERE ITSID = '<number>'
    UNION ALL
    SELECT COUNT(*) as count, 'CompanyEmployeeNumber' as category, MAX(EmployeeNumber) as matched_value 
    FROM vm_cedemo_companyemployees_active 
    WHERE EmployeeNumber = '<number>'
    UNION ALL
    SELECT COUNT(*) as count, 'ContractorEmployeeITSID' as category, MAX(ITSID) as matched_value 
    FROM vm_cedemo_contractoremployees_active 
    WHERE ITSID = '<number>'
    
    Return your response as JSON only, without markdown formatting:
    {{
        "number": "<extracted number>",
        "sql_query": "<the complete SQL query>"
    }}
    """
    
    # Get SQL query from LLM
    response = azure_client.chat.completions.create(
        model=azureopenai,
        messages=[
            {"role": "system", "content": "You are a SQL query generator that identifies employee number categories with exact matching. Return only valid JSON without markdown formatting."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    
    result = response.choices[0].message.content.strip()
    print("Number Clarifier LLM response:", result)
    
    try:
        # Clean the response - remove markdown code blocks if present
        cleaned_result = result
        if result.startswith("```"):
            cleaned_result = re.sub(r'^```(?:json)?\s*\n', '', result)
            cleaned_result = re.sub(r'\n```\s*$', '', cleaned_result)
            cleaned_result = cleaned_result.strip()
            print("Cleaned response:", cleaned_result)
        
        parsed = json.loads(cleaned_result)
        number = parsed.get("number")
        sql_query = parsed.get("sql_query")
        
        if not sql_query:
            return {
                "success": False,
                "error": "Oops! 🤔 I had trouble understanding that number. Could you try again with a clearer format?"
            }
        
        # print(f"Executing combined query with exact matching: {sql_query}")
        
        try:
            # Execute SQL query
            results = execute_sql_query(sql_query)
            
            # Find the category with count > 0
            found_category = None
            matched_value = None
            
            for row in results:
                count = row.get("count", 0)
                category = row.get("category")
                print(f"Category '{category}': count = {count}")
                
                if count > 0:
                    found_category = category
                    matched_value = row.get("matched_value", number)
                    print(f"Found match: {found_category} (count: {count}), matched value: {matched_value}")
                    break
            
            if found_category:
                # Fetch additional employee details
                employee_details = None
                try:
                    if found_category == "CompanyEmployeeITSID":
                        detail_query = f"SELECT TOP 1 EmployeeName, EmployeeNumber FROM vm_cedemo_companyemployees_active WHERE ITSID = '{matched_value}'"
                    elif found_category == "CompanyEmployeeNumber":
                        detail_query = f"SELECT TOP 1 EmployeeName, EmployeeNumber FROM vm_cedemo_companyemployees_active WHERE EmployeeNumber = '{matched_value}'"
                    elif found_category == "ContractorEmployeeITSID":
                        detail_query = f"SELECT TOP 1 EmployeeName FROM vm_cedemo_contractoremployees_active WHERE ITSID = '{matched_value}'"
                    
                    employee_details = execute_sql_query(detail_query)
                    if employee_details and len(employee_details) > 0:
                        employee_details = employee_details[0]
                except Exception as detail_error:
                    print(f"Error fetching employee details: {detail_error}")
                    employee_details = None
                
                # Use LLM to decide: verification question or rewrite for routing
                return await handle_clarification_result(query, number, matched_value, found_category, employee_details)
            else:
                # Number not found
                return {
                    "success": False,
                    "error": f"Sorry, I couldn't find '{number}' in our system. 😔\n\n"
                           f"I checked:\n"
                           f"  • Company Employee ITS IDs\n"
                           f"  • Company Employee Numbers\n"
                           f"  • Contractor Employee ITS IDs\n\n"
                           f"Could you please:\n"
                           f"  ✓ Double-check the number for typos\n"
                           f"  ✓ Make sure it's a valid employee identifier\n\n"
                           f"I'm here to help! 💪"
                }
                
        except Exception as sql_error:
            print(f"SQL execution error: {sql_error}")
            return {
                "success": False,
                "error": f"Oops! 😅 I ran into a technical hiccup while searching for that number.\n\n"
                       f"Don't worry though - let's try again! If this keeps happening, please reach out to support."
            }
            
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return {
            "success": False,
            "error": "Sorry! 🤖 I'm having trouble processing that request. Could you rephrase your question?"
        }
    except Exception as e:
        print(f"Error in number clarifier: {str(e)}")
        return {
            "success": False,
            "error": "Oops! Something unexpected happened. 😓 Let's try that again, shall we?"
        }


async def handle_clarification_result(original_query: str, user_number: str, matched_value: str, found_category: str, employee_details: dict = None):
    """
    Uses LLM to determine if this is a verification question or a data query.
    Returns either a direct answer OR a rewritten query for routing.
    
    Args:
        original_query: User's original question
        user_number: Number user typed
        matched_value: Actual database value
        found_category: Category found (CompanyEmployeeITSID, CompanyEmployeeNumber, ContractorEmployeeITSID)
        employee_details: Dict containing EmployeeName and optionally EmployeeNumber
    
    Returns:
        dict: Either {"answer": "..."} for verification OR {"success": True, "rewritten_query": "..."} for routing
    """
    
    azure_client, azureopenai = get_azure_chat_openai()
    
    prompt = f"""
    You are an intelligent query analyzer. Analyze the user's question and the clarification result.
    
    Original User Query: {original_query}
    User's Number: {user_number}
    Actual Database Value: {matched_value}
    Found Category: {found_category}
    Employee Details: {employee_details if employee_details else 'Not available'}
    
    IMPORTANT: The found_category can be one of:
    - CompanyEmployeeITSID (Company Employee ITS ID)
    - CompanyEmployeeNumber (Company Employee Number)
    - ContractorEmployeeITSID (Contractor Employee ITS ID)
    
    Determine the user's intent:
    
    1. VERIFICATION QUESTION: User is ONLY asking to identify/verify what type of number it is.
       Examples of VERIFICATION questions:
       - "Is 7653 an employee number?"
       - "Is 7653 an ITSID?"
       - "What type of number is 7653?"
       - "What is 7653?" (asking about the type/category)
       - "Identify 7653"
       
       For verification questions, provide a direct answer in friendly chatbot style.
       Use emojis and be conversational.
       
       If user asked about the WRONG type:
       Answer: "No, **{matched_value}** is not a [asked type]. ❌\n\nIt's actually a **{found_category}**! ✅"
       
       If user asked about the CORRECT type:
       Answer: "Yes, **{matched_value}** is a {found_category}! ✅"
       
       If user asked what type it is:
       Answer: "**{matched_value}** is a **{found_category}**! ✅"
       
       NOTE: For employee categories, use VERY clear friendly names in answers:
       - CompanyEmployeeITSID → "**Company Employee** ITS ID" (emphasize "Company Employee")
       - CompanyEmployeeNumber → "**Company Employee** Number" (emphasize "Company Employee")
       - ContractorEmployeeITSID → "**Contractor Employee** ITS ID" (emphasize "Contractor Employee")
       
       IMPORTANT: If employee_details are available, ALWAYS include them in verification answers:
       - Include **Employee Name:** {employee_details.get('EmployeeName')} on a new line
       - For company employees, include **Employee Number:** {employee_details.get('EmployeeNumber')} if it exists
       - Format example:
         "**322535** is a **Contractor Employee** ITS ID! ✅\n\n**Employee Name:** Daniel Lopez"
    
    2. DATA QUERY: User wants to retrieve information, details, or data ABOUT that number.
       Examples of DATA queries:
       - "Show me employee 7653"
       - "What qualifications does 7653 have?"
       - "Get details for ITSID 7653"
       - "Tell me about employee 7653"
       - "Show me details for 7653"
       - "Who is the supervisor and manager for 372982"
       
       Keywords indicating DATA query: tell me about, show me, get, details, information, how many, who, what are, list, qualifications, supervisor, manager
       
       For data queries, rewrite the query by inserting the FULL category before the number.
       Format: Replace the number with "{found_category} {matched_value}"
       
       CRITICAL: Keep the full category name to distinguish Company vs Contractor:
       - For CompanyEmployeeITSID → "CompanyEmployeeITSID 7653" (NOT just "employee 7653")
       - For ContractorEmployeeITSID → "ContractorEmployeeITSID 372982" (NOT just "employee 372982")
       
       Examples:
       - "Show me employee 7653" → "Show me employee CompanyEmployeeITSID 7653"
       - "Who is the supervisor for 372982" → "Who is the supervisor for ContractorEmployeeITSID 372982"
    
    IMPORTANT: If the query contains words like "about", "details", "show", "get", "information", it is a DATA query, NOT verification!
    
    Respond in JSON format:
    - For verification: {{"answer": "<direct friendly answer with emojis>"}}
    - For data query: {{"rewritten_query": "<query with category specified>"}}
    
    Return ONLY valid JSON without markdown formatting.
    """
    
    response = azure_client.chat.completions.create(
        model=azureopenai,
        messages=[
            {"role": "system", "content": "You are a query analyzer that determines user intent and responds appropriately. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    
    result = response.choices[0].message.content.strip()
    print(f"Clarification handler LLM response: {result}")
    
    try:
        # Clean response
        cleaned = result
        if result.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*\n', '', result)
            cleaned = re.sub(r'\n```\s*$', '', cleaned)
            cleaned = cleaned.strip()
        
        parsed = json.loads(cleaned)
        
        # Return based on LLM's decision
        if parsed.get("answer"):
            # Verification question - return direct answer
            return {
                "answer": parsed.get("answer")
            }
        elif parsed.get("rewritten_query"):
            # Data query - return rewritten query for routing
            return {
                "success": True,
                "original_query": original_query,
                "rewritten_query": parsed.get("rewritten_query"),
                "number": matched_value,
                "original_number": user_number,
                "category": found_category
            }
        else:
            # Fallback - treat as data query
            rewritten = f"{original_query.replace(user_number, f'{found_category} {matched_value}')}"
            return {
                "success": True,
                "rewritten_query": rewritten,
                "number": matched_value,
                "category": found_category
            }
            
    except Exception as e:
        print(f"Error parsing clarification handler response: {e}")
        # Fallback - treat as data query
        rewritten = f"{original_query.replace(user_number, f'{found_category} {matched_value}')}"
        return {
            "success": True,
            "rewritten_query": rewritten,
            "number": matched_value,
            "category": found_category
        }