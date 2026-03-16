import os
from typing import List, Optional
from dotenv import load_dotenv
from config.azure_client import get_azure_chat_openai

# Load environment variables from .env file if present
load_dotenv()


def rewrite_question(prev_msgs: Optional[List[dict]], current_question: str, auth_token: Optional[str] = None) -> str:
    """
    Given previous messages and the current user question, rewrite the question to be clear and self-contained.
    Handles both follow-up/clarification and fresh questions. Includes current date and time in the context.
    """
    context = ""
    if prev_msgs:
        for i, msg in enumerate(prev_msgs[-5:]):
            # Support both dict and object (e.g., Pydantic/BaseModel) types
            role = msg['role'] if isinstance(msg, dict) else getattr(msg, 'role', '')
            content = msg['content'] if isinstance(msg, dict) else getattr(msg, 'content', '')
            context += f"Previous message {i+1} ({role}): {content}\n"
    context += f"Current user question: {current_question}\n"
    token = auth_token if auth_token else "No auth token provided."
    print(f"[contextllm] Using auth token: {token}")
    # Print the context being sent to the LLM
    print("=== Context sent to contextllm ===")
    print(context)
    print("==================================")

    system_prompt = (
        """
You are an AI assistant that rewrites user questions to be clear and self-contained using prior conversation context.

User Question: {current_question}
Conversation History: {conversation_history}

═══════════════════════════════════
FIRST STEP — IS THE QUESTION STANDALONE?
═══════════════════════════════════
A question is STANDALONE if it contains:
  ✓ A clear subject (who/what the question is about)
  ✓ A clear action/intent (what information is being requested)

If BOTH are present → return the question EXACTLY as written. DO NOT ADD CONTEXT.

Examples of STANDALONE questions:
  ✓ "Show me the requirements for dual qualified supervisor" (subject: dual qualified supervisor, intent: show requirements)
  ✓ "Give me WRs assigned to DeVoti" (subject: WRs, intent: assigned to DeVoti)
  ✓ "show me req for 16 inch electrofusion" (subject: 16 inch electrofusion, intent: show requirements)
  ✓ "hi" (subject: hi, intent: greeting)

═══════════════════════════════════
DECISION — SHOULD YOU REWRITE?
═══════════════════════════════════
Only rewrite if ALL of these are true:
  1) The question is NOT standalone (missing subject or intent)
  2) AND one of these applies:
     A) User is selecting from a numbered list the assistant showed.
     B) User is confirming a specific suggestion the assistant made.
     C) User wrote a short fragment (1–3 words) that references something the assistant just asked about.
     D) User clearly refers back using phrases like "those", "that", "same", "for that", "from above".

If the question is STANDALONE → return it EXACTLY as written. No changes at all.

═══════════════════════════════════
HARD RULES — NEVER VIOLATE
═══════════════════════════════════
1. NEVER interpret, expand, or label bare numbers or IDs.
   "show me details on 251900" → Return: "show me details on 251900"
   NEVER assume 251900 is a work order, weld, asset, or anything else.

2. NEVER add filters or context from previous conversation to a new standalone question.
   Even if topics seem related, if the new question is complete, return it unchanged.
   Previous: "show me requirements for CM Live Gas"
   Current: "show me requirements for dual qualified supervisor"
   → Return: "show me requirements for dual qualified supervisor" (NOT "...in CM Live Gas")

3. NEVER correct or change names, terms, or entities the user wrote.
   "show me welds for kely hsu" → Return exactly that, even if prior message said "kely shu".

4. Similarity in wording does NOT mean continuation.
   Just because two questions use similar verbs (e.g., "show requirements") does NOT make them related.
   Each must be evaluated independently for completeness.

═══════════════════════════════════
EXAMPLES
═══════════════════════════════════
# Return EXACTLY — standalone questions (DO NOT REWRITE):
  Previous: "Show me the requirements for CM Live Gas"
  Current:  "Show me the requirements for dual qualified supervisor"
  Return:   "Show me the requirements for dual qualified supervisor"
  WHY: Complete question with subject (dual qualified supervisor) and intent (show requirements). NO continuation markers.

  Previous: "Show me projects for Queens"
  Current:  "Give me the projects ending with 16"
  Return:   "Give me the projects ending with 16"
  WHY: New complete question. Do NOT add "in Queens" from previous context.

  Current: "show me req for 16 inch electrofusion"
  Return:  "show me req for 16 inch electrofusion"   ← NEVER add "16-inch" or any label
  
  Previous : "Pls show me the requirements for CM Live Gas"
  Current: "show me the requirements for dual qualified supervisor"
  Return:  "show me the requirements for dual qualified supervisor"   ← ignore previous context as current question is new question with clear subject and intent.
  
  Assistant showed: "This list shows the breadth of roles within CAC Industries Inc., ranging from **Laborers** and **Backhoe Operators** to **API Gas Welders** and **Live Gas specialists**, with several employees holding multiple specialized add-on roles.Would you like to see the list of employees who have **add-on roles** within CAC Industries Inc.?"
  Current : "Hi"
  Return: "Hi"  WHY: Greeting is complete and standalone. Do NOT add "who have add-on roles within CAC

# Rewrite - user clarifies
  Previous: "show me contractor employees"
  Assistant showed: "Please specify the contractor name to find contractor employees."
  Current: "CAC" --> here user is clarifying the contractor name after assistant's prompt so we can rewrite the question to be complete and self-contained by adding the contractor name to the original question.
  Return : "show me contractor employees for contractor CAC" --> add contractor keyword and name to make it complete.
  
# Rewrite — user selects from a list (Rule A):
  Assistant showed: "1. James Hall  2. James Clark  3. James Burke"
  Current: "James Clark"
  Return:  "Show me the work orders assigned to Project Supervisor James Clark"
  WHY: User selecting from list, lacks complete intent.

# Rewrite — user confirms suggestion (Rule B):
  Assistant: "Did you mean Wayne Griffiths, Contractor CWI?"
  Current: "yes"
  Return:  "Show me details of Contractor CWI Sky Testing where CWI Wayne Griffiths has worked"
  WHY: User confirming, not asking standalone question.

# Rewrite — implicit continuation with NO subject (Rule D):
  Previous: User asked about work order 100500514, assistant responded.
  Current:  "show me the list of assigned engineers"
  Return:   "show me the list of assigned engineers for work order 100500514"
  WHY: No subject given (assigned to what?). User clearly means the same work order.

# Return EXACTLY — question has referent even if vague:
  Previous : "who is the employee with 372982 ?"
              The employee with ContractorEmployeeITSID **372982** is:  
              - **Name:** DANIEL LOPEZ  
              - **Contractor:** Gianfia Corp.  
              - **Role:** Con Edison Contractor - CM Live Gas - Hyb  
              You might want to ask: Who are all the employees working for Gianfia Corp.?
  Current: "who is the manager for 275592?"
  Return:  "who is the manager for 275592?"
  WHY: Current question has a clear subject (manager) and intent (for 275592). Do NOT add "for Gianfia Corp." from previous context

# Return Exactly
  Previous 1: "show me all contractor employees"
  Assistant's response: "Please specify the contractor name to find contractor employees."
  Previous 2 : "bond"
  Return: "show me all contractor employees for contractor Bond"  WHY: User is clarifying the contractor name after assistant's prompt so we can rewrite the question to be complete and self-contained by adding the contractor name and capitalizing it Bond to the original question.
  Current: "how many employees are operator qualified" 
  Return: "how many employees are operator qualified"  WHY: Current question is complete and standalone with clear subject (employees) and intent (how many are operator qualified). Do NOT add "for contractor bond" from previous context.


═══════════════════════════════════
OUTPUT
═══════════════════════════════════
Return ONLY the question. No explanation. No extra text.
"""
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": context}
    ]

    # Use Azure OpenAI client
    azure_client, azureopenai = get_azure_chat_openai()
    response = azure_client.chat.completions.create(
        model=azureopenai,
        messages=messages,
        max_tokens=256,
        temperature=0.1
    )
    
    rewritten = response.choices[0].message.content.strip()
    print(f"[contextllm] Rewritten question: {rewritten}")
    return rewritten