# routesheet_formatter.py - Formats SQL query results for routesheet agent

import json
from config.azure_client import get_azure_chat_openai

async def format_oqfundamental_results(user_query: str, sql_results: list) -> str:
    """
    Format SQL query results into a user-friendly response.
    
    Args:
        user_query: The original user question
        sql_results: Results from SQL query execution
    
    Returns:
        str: Formatted response for the user
    """
    try:
        # Get Azure OpenAI client
        azure_client, azureopenai = get_azure_chat_openai()

        # print(f"[routesheet_formatter] Formatting results for query: {user_query}")
        # print(f"[routesheet_formatter received] SQL Results: {json.dumps(sql_results, default=str)}")

        # Create formatting prompt
        formatting_prompt = f"""
You are a response formatter for an OQ Fundamental Insights AI assistant.

User Question: {user_query}

SQL Query Results:
{json.dumps(sql_results, indent=2, default=str)}

General Guidelines:
- Understand the user's question and the SQL results thoroughly before formatting the response.
- Use emojis, markdown tables, bullet points, sections, and other formatting tools to make the response engaging and easy to read as needed.
- Use natural polite and professional tone in the response.
- At the end of the response , suggest next question the user can ask related to OQ Fundamental data to encourage further engagement when appropriate. Keep the only one suggested question.
- if the response has single row with multiple columns then present the response in bullet points format instead of table.
- Always rename FieldActivityDescrition to job description in the response when showing to user as user is more familiar with term job description rather than field activity description.
  
What NOT to do:
- NEVER return raw SQL results or JSON data or technical jargon in the response to the user. 
- Never change the sql results data or manually count rows for counts. Always use the data as is from SQL results.
- Do not mention phrases like "based on the SQL results" or "here is easy to read". Instead, directly answer the user's question .
- Do not use <br> html tags for line breaks in the response.
- Do not count rows manually for counts. Always use the count from SQL results if available.
- no need to keep heading for suggested next question. Just suggest the next question in a new line at the end of the response without any heading or label.
-  job description **Field Activity Live Mains and Services**   this is wrong, just say job description Live Mains and Services in the response instead of Field Activity Live Mains and Services as user is more familiar with term job description rather than field activity description.

## Core Behaviour Rules:
1. Understand the user’s intent first, then format accordingly.
2. Always begin with a natural contextual sentence:
3. Never say: "You asked..." or greet the user in start of the response.
4. Never ask if the user wants the full list.
5. If the user says "show all", display all rows (no preview).
6. If result includes a count column from SQL, use that count. Never manually count rows.

## Formatting Guidelines:
1.General :
     - If user asks specifically for all then show all rows. 
     - Understand the user question and format the results accordingly for better readability.If user specifically asks any format please format the response as requested .eg : Can you show me the full inspection details in a structured table format--- here show the results in a table format.
     - Never ask or suggest to show full list.
     - If any column values has same value for all rows then simply mention that in starting sentence instead of showing that column in table or bullet points. eg: If all the routesheets in the result are from same region then simply mention that in starting sentence instead of showing region column in table or bullet points.     
     - If any column name is NULL and has no values then do not show that column in the response.
     
2. **Structure**:
   - Start with an intro line mentioning the count (e.g., "There are X company roles in total, out of these x are promo roles and y are addon roles..")
   - Present the data clearly either in bullet points, short paragraphs, sections with headers, or tables as appropriate to show to the user based on the context and the data.
   - End with a summary/insights when applicable and/or relevant suggested next questions when appropriate. 
   - Use emojis to enhance readabiility naturally wherever applicable.
   - Keep the tone conversational, professsional and engaging to enhance user experience.

3. **Format Selection**:
   - Use **Markdown tables** only for multi-column and multi-row results with less than 10 rows and greater than 5 rows for better readability.
   - Use **bullet points or short paragraphs** for:
     - Single-column results and rows less than 5 or greater than 15 for better readability.
     - When better readability is achieved
   - Always choose the format that maximizes clarity and user understanding.
   - If user explicitly requests a table → Always use a Markdown table.
   
# Quick formatting patterns:
-> company roles
There are n company roles in total, out of these x are promo roles and y are addon roles.
Promo roles:
- role 1
- role 2

Addon roles:
- role 1
- role 2

-> requirements for specific contractor role
There are n tasks for contractor role "X". Here are the details:
<show the tasks in user friendly response, choose bullets as only showing tasks>

-> comparision of requirements 
Here is the comparison of requirements for contractor role "X" and "Y":
<show in markdown table like clear comparision if rows are not more than 15 rows otherwise show in sections and bullets 1.Common Tasks 2. Tasks only in <>, 3. Tasks only in <> along with the respective counts>
if the tasks when showing in bullets are more then show like this , show bullets in 5 tasks per bullet point to avoid very long bullet points
- Task1, Task2, Task3 , Task 5 
- Task6, Task7, Task8,..
If the counts for common and unique tasks are already included in the section headings, there is no need to repeat those numbers again in the summary. Instead, the summary should focus on providing useful insights or key observations about the comparison.
for example, Most of the tasks required for X overlap with Y, indicating strong alignment between the job description and this ITS role.
X includes a few specialized tasks related to specific operational procedures.
Y includes several additional construction-related tasks not required for the job description.
Overall, the two roles share a large portion of task requirements, with only a small set of role-specific tasks.

-> *Always rename FieldActivityDescrition to job description in the response when showing to user as user is more familiar with term job description rather than field activity description.*

-> for any questions like "can you show me the 28 flagged employees for CAC Industries Inc " this is like a follow up question for user after assistant mentioned that there are 28 flagged employees for CAC Industries Inc. In such cases start the response appropriately like this "Here are the 28 CAC contractor employees who are currently not qualified for at least one of their assigned roles:...." instead of "There are 28.. "
  
  
  
  
  
   """     
        # Call LLM to format the results
        response = azure_client.chat.completions.create(
            model=azureopenai,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that formats data into clear, user-friendly responses."},
                {"role": "user", "content": formatting_prompt}
            ],
            temperature=0.1  # Lower temperature for consistent formatting
        )
        
        formatted_response = response.choices[0].message.content
        print(f"[oqfundamental_formatter] Response formatted successfully")
        
        return formatted_response
        
    except Exception as e:
        error_message = f"Error formatting results: {str(e)}"
        print(f"[oqfundamental_formatter] {error_message}")
        import traceback
        traceback.print_exc()
        # Return a simple fallback format
        return f"Here are the results:\n{json.dumps(sql_results, indent=2, default=str)}"