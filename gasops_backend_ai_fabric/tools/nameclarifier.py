from config.azure_client import get_azure_chat_openai
from tools.sql_executor import execute_sql_query
import json
import re


async def name_clarifier_llm(query: str, auth_token=None):
    """
    Identifies categories of ALL ambiguous names in the user query using a 3-step process:
    1. LLM extracts ALL names from user query (without classifying as person/company)
    2. Static SQL query searches for matches in ALL categories (person AND company)
    3. LLM rewrites query with all clarified names OR generates clarification message
    
    Args:
        query: User's original query containing ambiguous names
        auth_token: Authentication token (optional)
    
    Returns:
        dict: Contains either rewritten query OR clarification request
    """
    
    azure_client, azureopenai = get_azure_chat_openai()
    
    print(f"[Name Clarifier] Received query: {query}")
    
    # ============================================================
    # STEP 1: LLM extracts ALL names from user query (NO CLASSIFICATION)
    # ============================================================
    extraction_prompt = f"""
    Extract ALL ambiguous terms (people, companies, contractors, tasks, roles, activities, etc.) from this query.
    Return them in a JSON array called "names".
    DO NOT try to classify them - just extract all potential terms that need clarification.
    
    CRITICAL - Keep multi-word phrases TOGETHER as single entities:
    - Activity names with "and" stay together (e.g., "Live Man and Services", "Excavate and Backfill", "Mains and Services")
    - Company names with descriptive words stay together (e.g., "gas construction services", "Shaw Pipeline")
    - Multi-word person names stay together (e.g., "Wayne Griffiths", "Joseph Clark")
    
    Do NOT extract:
    - Field names or column references when used as metadata (e.g., "FieldActivityDescription", "ContractorName")
    - Category labels (e.g., "Field Activity", "Contractor", "Task", "Employee")
    - Common SQL keywords or operators (e.g., "where", "and", "or", "select")
    - Query structure words (e.g., "compare", "show me", "requirements for")
    - Generic terms (e.g., "requirements", "projects", "tasks" when used generically)
    
    DO extract as COMPLETE PHRASES:
    - Specific person names (e.g., "Joseph", "Wayne Griffiths", "manju")
    - Specific company/contractor names (e.g., "Acme Corp", "Shaw Pipeline", "gas construction services")
    - Specific task descriptions or numbers
    - Specific role names
    - Specific activity names - KEEP "AND" PHRASES TOGETHER (e.g., "16inch electrofusion", "excavate and backfill", "live mains and services")
    - Specific department/section/region names (e.g., "engineering dept")
    
    User Query: {query}
    
    Examples:
    - "compare requirements for 16inch and gas construction srvices"
      → {{"names": ["16inch", "gas construction srvices"]}}
      (Two separate activities: "16inch" and "gas construction srvices" - split by "and" because they're different)
    
    - "show me req for Live Man and Services"
      → {{"names": ["Live Man and Services"]}}
      (ONE activity name that contains "and" - keep together, don't split into "Live Man" + "Services")
    
    - "req for excavate and backfill activity"
      → {{"names": ["excavate and backfill"]}}
      (ONE activity, keep "and" together)
    
    - "show me req for 16 inc electr" 
      → {{"names": ["16 inc electr"]}}
    
    - "tasks for 16inch electrofusion activity"
      → {{"names": ["16inch electrofusion"]}}
      (NOT "activity" - that's a category label)
    
    - "show me projects handled by Joseph at Acme Corp"
      → {{"names": ["Joseph", "Acme Corp"]}}
      (NOT "projects" - that's generic)
    
    - "req for mains and services"
      → {{"names": ["mains and services"]}}
      (ONE activity phrase with "and")
     
    Return ONLY the JSON object, no explanation.
    """
    
    try:
        response = azure_client.chat.completions.create(
            model=azureopenai,
            messages=[
                {"role": "system", "content": "You extract person and company names from queries. Ignore field names, technical terms, and column references. Return only JSON format."},
                {"role": "user", "content": extraction_prompt}
            ],
            temperature=0,
            max_tokens=150
        )
        
        extracted_json = response.choices[0].message.content.strip()
        extracted_data = json.loads(extracted_json)
        all_names = extracted_data.get("names", [])
        
        print(f"[Name Clarifier] Extracted names: {all_names}")
        
        if not all_names:
            return {
                "success": False,
                "error": "Sorry, I couldn't identify any names in your question. 🤔\n\n"
                       "Could you please rephrase with clear names?"
            }
    
    except Exception as e:
        print(f"[Name Clarifier] Error extracting names: {str(e)}")
        return {
            "success": False,
            "error": "Oops! I had trouble understanding that question. 😅 Could you rephrase it?"
        }
    
    # ============================================================
    # STEP 2: Search for ALL extracted names in ALL categories
    # ============================================================
    all_clarifications = []
    
    # Search each name in BOTH person AND company categories
    for name in all_names:
        result = await search_name_in_all_categories(name)
        
        if result and len(result) > 0:
            all_clarifications.append({
                "original_name": name,
                "matches": result
            })
        else:
            # Name not found
            all_clarifications.append({
                "original_name": name,
                "matches": [],
                "not_found": True
            })
    
    # Check if any names were not found
    not_found_names = [item["original_name"] for item in all_clarifications if item.get("not_found")]
    if not_found_names:
        names_str = ", ".join(f"'{name}'" for name in not_found_names)
        return {
            "success": False,
            "error": f"Sorry, I couldn't find {names_str} in our system. 😔\n\n"
                   f"Please check:\n"
                   f"  ✓ Spelling of the name(s)\n"
                   f"  ✓ Try using full names\n"
                   f"  ✓ Verify they exist in our system"
        }
    
    # ============================================================
    # STEP 3: Handle results
    # ============================================================
    # Auto-select best match if it's clearly superior
    filtered_clarifications = []
    for item in all_clarifications:
        matches = item.get("matches", [])
        
        if len(matches) > 1:
            # Check if top match is clearly the best
            top_match = matches[0]  # Already sorted by similarity in search_name_in_all_categories
            second_match = matches[1] if len(matches) > 1 else None
            
            top_similarity = top_match.get("similarity", 0)
            second_similarity = second_match.get("similarity", 0) if second_match else 0
            top_length = top_match.get("match_length", 999)
            second_length = second_match.get("match_length", 999) if second_match else 999
            
            # Debug logging
            print(f"[Name Clarifier] Top match for '{item['original_name']}': '{top_match['name']}' (sim: {top_similarity:.2f}, len: {top_length})")
            if second_match:
                print(f"[Name Clarifier] 2nd match: '{second_match['name']}' (sim: {second_similarity:.2f}, len: {second_length})")
            
            # Auto-select if any of these conditions are met:
            # 1. Perfect exact match (1.0) - only exact matches auto-select without gap check
            # 2. High similarity (≥0.8) AND significant gap (≥0.15) - clearly better than other options
            # 3. Similarity ≥0.75 AND top match is significantly shorter (≤60% of 2nd match length) AND there's some gap (≥0.05)
            # 4. Similar scores (gap <0.1) but top match is much shorter (≤50% of 2nd match length)
            # NOTE: Removed auto-select on ≥0.95 without gap check to prevent selecting when multiple prefix matches exist
            auto_select = (
                (top_similarity >= 1.0) or
                (top_similarity >= 0.8 and (top_similarity - second_similarity) >= 0.15) or
                (top_similarity >= 0.75 and top_length <= 0.6 * second_length and (top_similarity - second_similarity) >= 0.05) or
                ((top_similarity - second_similarity) < 0.1 and top_length <= 0.5 * second_length and top_similarity >= 0.7)
            )
            
            if auto_select:
                print(f"[Name Clarifier] Auto-selecting '{top_match['name']}' (similarity: {top_similarity:.2f}) over other matches")
                filtered_clarifications.append({
                    "original_name": item["original_name"],
                    "matches": [top_match]  # Keep only the best match
                })
            else:
                # Keep all matches for user selection
                filtered_clarifications.append(item)
        else:
            # Single match or no match - keep as is
            filtered_clarifications.append(item)
    
    # Check if ANY name still has multiple matches OR multiple categories
    needs_user_clarification = any(
        len(item["matches"]) > 1 or 
        (len(item["matches"]) == 1 and len(item["matches"][0].get("all_categories", [])) > 1)
        for item in filtered_clarifications
    )
    
    if needs_user_clarification:
        return await handle_multiple_names_clarification(query, filtered_clarifications)
    else:
        # All names have single matches in single categories - rewrite query
        return await handle_all_single_matches(query, filtered_clarifications)


# Category configuration: defines table, search column, return column, and entity type
CATEGORY_CONFIG = {
    # Contractors (cedemo_ContractorMaster) - search in display name, return actual name
    "ContractorDisplayName": {
        "table": "cedemo_ContractorMaster",
        "search_column": "ContractorDisplayName",
        "return_column": "ContractorName",
        "type": "contractor"
    },
    # Tasks (cedemo_CoveredTask)
    "TaskNum": {
        "table": "cedemo_CoveredTask",
        "search_column": "TaskNum",
        "return_column": "TaskNum",
        "type": "task"
    },
    "TaskDesc": {
        "table": "cedemo_CoveredTask",
        "search_column": "TaskDesc",
        "return_column": "TaskDesc",
        "type": "task"
    },
    # Company Employees (vm_cedemo_companyemployees_active)
    "CompanyEmployeeName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "EmployeeName",
        "return_column": "EmployeeName",
        "type": "person"
    },
    "OrgName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "OrgName",
        "return_column": "OrgName",
        "type": "organization"
    },
    "DepartmentName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "DepartmentName",
        "return_column": "DepartmentName",
        "type": "department"
    },
    "SectionName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "SectionName",
        "return_column": "SectionName",
        "type": "section"
    },
    "CompanySupervisorName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "SupervisorName",
        "return_column": "SupervisorName",
        "type": "person"
    },
    "CompanyManagerName": {
        "table": "vm_cedemo_companyemployees_active",
        "search_column": "ManagerName",
        "return_column": "ManagerName",
        "type": "person"
    },
    # Contractor Employees (vm_cedemo_contractoremployees_active)
    "ContractorEmployeeName": {
        "table": "vm_cedemo_contractoremployees_active",
        "search_column": "EmployeeName",
        "return_column": "EmployeeName",
        "type": "person"
    },
    # Field Activities (cedemo_FieldActivityAliasesMaster) - search alias, return description
    "FieldActivityAlias": {
        "table": "cedemo_FieldActivityAliasesMaster",
        "search_column": "AliasName",
        "return_column": "FieldActivityDescription",
        "type": "activity"
    },
    # Field Activities Description (cedemo_FieldActivityAliasesMaster) - also search in description column
    "FieldActivityDescription": {
        "table": "cedemo_FieldActivityAliasesMaster",
        "search_column": "FieldActivityDescription",
        "return_column": "FieldActivityDescription",
        "type": "activity"
    },
    # ITS Roles - Contractor (cedemo_ContractorITSRoleMasterAlias) - search alias, return role
    "ContractorITSRoleAlias": {
        "table": "cedemo_ContractorITSRoleMasterAlias",
        "search_column": "AliasContractorITSRoleName",
        "return_column": "ContractorITSRoleName",
        "type": "role"
    },
    # ITS Roles - Company (cedemo_CompanyITSRoleMaster)
    "CompanyITSRoleName": {
        "table": "cedemo_CompanyITSRoleMaster",
        "search_column": "CompanyITSRoleName",
        "return_column": "CompanyITSRoleName",
        "type": "role"
    }
}


async def search_name_in_all_categories(name: str):
    """
    Search for a name/term in ALL categories using fuzzy matching.
    Returns distinct matches with their corrected spelling and category.
    
    Args:
        name: Name or term to search for
    
    Returns:
        list: List of matches with category, corrected name, type, and similarity score
    """
    
    # Get all category keys from configuration
    all_categories = list(CATEGORY_CONFIG.keys())
    
    sql_query = generate_name_search_query(name, all_categories)
    
    # Print the generated SQL query
    # print(f"\n[Name Clarifier] ========== SQL QUERY for '{name}' ==========")
    # # print(sql_query)
    # print("[Name Clarifier] =============================================\n")
    
    try:
        results = execute_sql_query(sql_query)
        
        # Print the SQL query results
        print(f"\n[Name Clarifier] ========== SQL RESULTS for '{name}' ==========")
        # print(f"[Name Clarifier] Total rows returned: {len(results) if results else 0}")
        # if results and len(results) > 0:
        #     for idx, row in enumerate(results, 1):
        #         print(f"[Name Clarifier] Row {idx}: {row}")
        # else:
        #     print("[Name Clarifier] No results found")
        # print("[Name Clarifier] =============================================\n")
        
        if not results or len(results) == 0:
            return None
        
        # Process and deduplicate results
        # Group by matched value and category, then collect all instances
        name_groups = {}
        
        for row in results:
            category = row.get("category")
            matched_value = row.get("matched_value")
            similarity = float(row.get("similarity", 0))
            match_length = row.get("match_length", 999)  # Default to high number if not present
            
            if similarity > 0 and matched_value:
                # Get entity type from configuration
                entity_type = CATEGORY_CONFIG.get(category, {}).get("type", "unknown")
                
                # Create unique key for name+category combination
                group_key = f"{matched_value}||{category}"
                
                if group_key not in name_groups:
                    name_groups[group_key] = {
                        "name": matched_value,
                        "category": category,
                        "type": entity_type,
                        "similarity": similarity,
                        "match_length": match_length,
                        "categories": [category]
                    }
                else:
                    # Update if higher similarity, or same similarity but shorter
                    if (similarity > name_groups[group_key]["similarity"] or 
                        (similarity == name_groups[group_key]["similarity"] and 
                         match_length < name_groups[group_key]["match_length"])):
                        name_groups[group_key]["similarity"] = similarity
                        name_groups[group_key]["match_length"] = match_length
        
        if not name_groups:
            return None
        
        # Convert to list and group by actual matched value to find duplicate names across categories
        value_to_groups = {}
        for group_key, group_data in name_groups.items():
            matched_value = group_data["name"]
            if matched_value not in value_to_groups:
                value_to_groups[matched_value] = []
            value_to_groups[matched_value].append(group_data)
        
        # Build final result list
        all_matches = []
        for matched_value, groups in value_to_groups.items():
            if len(groups) == 1:
                # Single category match
                group = groups[0]
                all_matches.append({
                    "name": group["name"],
                    "category": group["category"],
                    "type": group["type"],
                    "similarity": group["similarity"],
                    "match_length": group.get("match_length", 999),
                    "all_categories": [group["category"]],
                    "unique_roles": [format_category_name(group["category"])]
                })
            else:
                # Multiple categories - pick best by similarity (DESC), then by length (ASC)
                best_group = max(groups, key=lambda x: (x["similarity"], -x.get("match_length", 999)))
                all_categories = [g["category"] for g in groups]
                unique_roles = list(set([format_category_name(g["category"]) for g in groups]))
                
                all_matches.append({
                    "name": best_group["name"],
                    "category": best_group["category"],
                    "type": best_group["type"],
                    "similarity": best_group["similarity"],
                    "match_length": best_group.get("match_length", 999),
                    "all_categories": all_categories,
                    "unique_roles": unique_roles
                })
        
        # Sort by similarity DESC, then by match length ASC (prefer shorter matches)
        all_matches.sort(key=lambda x: (-x["similarity"], x.get("match_length", 999)))
        
        # Print final processed matches
        print(f"\n[Name Clarifier] ========== FINAL MATCHES for '{name}' ==========")
        print(f"[Name Clarifier] Total unique matches after deduplication: {len(all_matches)}")
        for idx, match in enumerate(all_matches, 1):
            print(f"[Name Clarifier] Match {idx}: name='{match['name']}', category={match['category']}, "
                  f"type={match['type']}, similarity={match['similarity']:.2f}, length={match['match_length']}")
        print("[Name Clarifier] =============================================\n")
        
        return all_matches
        
    except Exception as e:
        print(f"[Name Clarifier] Error searching for '{name}': {str(e)}")
        return None


def generate_name_search_query(name: str, categories: list) -> str:
    """
    Generate SQL query for specific name/term and categories.
    Uses fuzzy matching (SOUNDEX, LIKE) to handle typos and partial names.
    Supports alias columns - searches in one column, returns another.
    
    Args:
        name: The name/term to search for
        categories: List of category keys from CATEGORY_CONFIG
    
    Returns:
        str: Complete SQL query
    """
    sanitized_name = name.replace("'", "''").strip()
    
    union_parts = []
    
    for category in categories:
        # Get configuration for this category
        config = CATEGORY_CONFIG.get(category)
        if not config:
            continue
        
        table_name = config["table"]
        search_column = config["search_column"]
        return_column = config["return_column"]
        
        # Tables that require IsActive = 1 filter
        tables_with_isactive = [
            "cedemo_CompanyITSRoleMaster",
            "cedemo_ContractorITSRoleMasterAlias",
            "cedemo_FieldActivityAliasesMaster",
            "cedemo_CoveredTask",
            "cedemo_ContractorMaster"
        ]
        
        # Add IsActive filter if applicable
        isactive_filter = " AND IsActive = 1" if table_name in tables_with_isactive else ""
        
        # Add exclusion filter for O&R Contractor roles
        exclusion_filter = ""
        if category == "ContractorITSRoleAlias":
            exclusion_filter = " AND ContractorITSRoleName NOT LIKE 'O&R Contractor%'"
        
        # Build query part with phrase-level fuzzy matching (no word splitting)
        query_part = f"""
        SELECT DISTINCT
            '{category}' as category,
            {return_column} as matched_value,
            LEN({return_column}) as match_length,
            CASE 
                -- Level 1: Perfect exact match (case-insensitive, trimmed)
                WHEN LOWER(LTRIM(RTRIM({search_column}))) = LOWER('{sanitized_name}') THEN 1.0
                
                -- Level 2: Starts with search term (prefix match)
                WHEN LOWER({search_column}) LIKE LOWER('{sanitized_name}%') THEN 0.95
                
                -- Level 3: Ends with search term (suffix match)
                WHEN LOWER({search_column}) LIKE LOWER('%{sanitized_name}') THEN 0.93
                
                -- Level 4: Contains as substring (middle match)
                WHEN LOWER({search_column}) LIKE LOWER('%{sanitized_name}%') THEN 0.90
                
                -- Level 5: High-quality SOUNDEX match (DIFFERENCE score of 4 = very similar)
                -- Only if lengths are similar (within 40% difference)
                WHEN DIFFERENCE({search_column}, '{sanitized_name}') = 4 
                     AND ABS(LEN({search_column}) - LEN('{sanitized_name}')) <= LEN('{sanitized_name}') * 0.4
                THEN 0.87
                
                -- Level 6: Good SOUNDEX match (DIFFERENCE score of 3 = similar)
                -- Only if lengths are very similar (within 30% difference)
                WHEN DIFFERENCE({search_column}, '{sanitized_name}') = 3 
                     AND ABS(LEN({search_column}) - LEN('{sanitized_name}')) <= LEN('{sanitized_name}') * 0.3
                THEN 0.85
                
                ELSE 0.0
            END as similarity
        FROM {table_name}
        WHERE {search_column} IS NOT NULL
          AND LTRIM(RTRIM({search_column})) != ''{isactive_filter}{exclusion_filter}
          AND (
              -- Primary filter: direct string matching (whole phrase only)
              LOWER({search_column}) LIKE LOWER('%{sanitized_name}%')
              
              -- Secondary filter: phonetic matching (SOUNDEX with strict length validation)
              -- Only allow SOUNDEX if lengths are similar (prevents "Live Mains and Services" matching "L&I Services")
              OR (
                  DIFFERENCE({search_column}, '{sanitized_name}') >= 4
                  AND ABS(LEN({search_column}) - LEN('{sanitized_name}')) <= LEN('{sanitized_name}') * 0.4
              )
          )
        """
        union_parts.append(query_part.strip())
    
    if not union_parts:
        return "SELECT NULL as category, NULL as matched_value, 0 as match_length, 0 as similarity WHERE 1=0"
    
    full_query = "\nUNION ALL\n".join(union_parts)
    # Order by similarity DESC, then by length ASC (prefer shorter matches)
    full_query += "\nORDER BY similarity DESC, match_length ASC"
    return full_query


async def handle_all_single_matches(original_query: str, clarifications: list):
    """
    All names have single matches - rewrite query with all clarified names.
    
    Args:
        original_query: User's original question
        clarifications: List of clarification results (each with single match)
    
    Returns:
        dict: Success response with rewritten query
    """
    
    azure_client, azureopenai = get_azure_chat_openai()
    
    # Build replacement instructions
    replacements = []
    seen_clarified = {}  # Track unique clarified values to avoid duplicates
    
    for item in clarifications:
        match = item["matches"][0]
        
        # Get the return column name from config for alias categories
        category_key = match["category"]
        config = CATEGORY_CONFIG.get(category_key, {})
        return_column = config.get("return_column", category_key)
        
        clarified_value = f"{return_column} {match['name']}"
        
        # Check if we've already added this clarified value
        if clarified_value in seen_clarified:
            # Append the original name to the existing entry
            seen_clarified[clarified_value]["original_names"].append(item["original_name"])
            print(f"[Name Clarifier] Skipping duplicate: '{item['original_name']}' → '{clarified_value}' (already added)")
        else:
            # New unique clarification
            replacement = {
                "original": item["original_name"],
                "original_names": [item["original_name"]],  # Track all original names that map to this
                "clarified": clarified_value,
                "type": match["type"],
                "category": match["category"],
                "full_name": match["name"]
            }
            replacements.append(replacement)
            seen_clarified[clarified_value] = replacement
    
    replacements_text = "\n".join([
        f"- Replace {', '.join(repr(name) for name in r['original_names'])} with '{r['clarified']}' ({r['type']})"
        for r in replacements
    ])
    
    rewrite_prompt = f"""
    Rewrite this query by replacing ALL ambiguous names with their clarified versions.
    
    Original Query: {original_query}
    
    Replacements to make:
    {replacements_text}
    
    Examples:
    - Original: "show me projects by Joseph at Shaw Pipeline"
      Replacements: Joseph → ProjectSupervisor1Name Joseph Clark (person), Shaw Pipeline → WeldingContractorName Shaw Pipeline Services (company)
      Rewritten: "show me projects by ProjectSupervisor1Name Joseph Clark at WeldingContractorName Shaw Pipeline Services"
    
    - Original: "show me requ for 16incg electr"
      Replacements: 16incg electr → FieldActivityDescription 16inch Electrofusion (activity)
      Rewritten: "show me requ for FieldActivityDescription 16inch Electrofusion"
    
    - Original: "show me req for excavate and backfill"
      Replacements: 'excavate', 'backfill' → FieldActivityDescription Excavation and Backfill (activity)
      Rewritten: "show me req for FieldActivityDescription Excavation and Backfill"
    
    Return ONLY the rewritten query, nothing else.
    """
    
    try:
        response = azure_client.chat.completions.create(
            model=azureopenai,
            messages=[
                {"role": "system", "content": "You rewrite queries by replacing ambiguous names with clarified names and categories. Return only the rewritten query."},
                {"role": "user", "content": rewrite_prompt}
            ],
            temperature=0,
            max_tokens=200
        )
        
        rewritten_query = response.choices[0].message.content.strip()
        print(f"[Name Clarifier] Rewritten query: {rewritten_query}")
        
        return {
            "success": True,
            "original_query": original_query,
            "rewritten_query": rewritten_query,
            "clarifications": replacements
        }
        
    except Exception as e:
        print(f"[Name Clarifier] Error rewriting query: {str(e)}")
        # Fallback to simple replacement
        rewritten = original_query
        for r in replacements:
            # Replace all original names that map to this clarified value
            for original_name in r["original_names"]:
                rewritten = rewritten.replace(original_name, r["clarified"], 1)  # Replace only first occurrence
        
        return {
            "success": True,
            "original_query": original_query,
            "rewritten_query": rewritten,
            "clarifications": replacements
        }


async def handle_multiple_names_clarification(original_query: str, clarifications: list):
    """
    At least one name has multiple matches - ask user for clarification.
    Only shows names with multiple matches (skips confirmed single matches).
    Uses LLM to filter out irrelevant matches before showing to user.
    
    Args:
        original_query: User's original question
        clarifications: List of clarification results (some with multiple matches)
    
    Returns:
        dict: Clarification request with only relevant matches listed
    """
    
    azure_client, azureopenai = get_azure_chat_openai()
    
    # First, ask LLM to filter irrelevant matches based on semantic similarity
    filtering_prompt = f"""
    User query: {original_query}
    
    We found multiple matches for names in the query. Filter out ONLY the truly irrelevant matches based on semantic similarity.
    
    **Core Principle:** Only keep matches where the matched name is semantically the same as or very close to the search term.
    
    Guidelines:
    - Remove matches where the name is phonetically similar but semantically different
      * Example: "gas construction srvices" → "Gage" (person name) - these are phonetically similar but semantically unrelated → REMOVE
      * Example: "gas construction services" → "Gage Furrow" - "Gage" sounds like "gas" but the full context shows they're different → REMOVE
      * Example: "Sky Testing" → "Shaw Pipeline" - phonetically similar but different companies → REMOVE
    
    - Keep matches where the name is actually the same or a variation:
      * Example: "Wayne Griffiths" → "Wayne Griffiths Jr" - same person, variation → KEEP
      * Example: "16incg electr" → "16inch Electrofusion" - same activity, typo correction → KEEP
      * Example: "shaw" → "Shaw Pipeline Services" - same company, abbreviation → KEEP
    
    - For multi-word search terms (especially with 2+ words like "gas construction services"):
      * If the search term has multiple words, it's describing a single entity with compound meaning
      * Don't match based on just one word matching phonetically - the FULL term must match semantically
      * Example: "gas construction services" has 3 words → needs to match a company name with similar meaning
      * Example: "Gage Furrow" is a person's name → semantically unrelated to "gas construction services" → REMOVE
      * Example: "Gage Construction Services" would be relevant → KEEP (if it existed)
    
    - Context matters:
      * If search has business-related multi-word pattern and match is a person name → likely irrelevant → REMOVE
      * If search is technical term (like "16inch electrofusion") and match is a person → likely irrelevant → REMOVE
    
    - Be strict: When in doubt about semantic relevance, REMOVE the match
      * Better to show "not found" than to show irrelevant phonetic coincidences
    
    - If ALL matches are irrelevant phonetic coincidences, return EMPTY matches array for that name
      * This tells the system "not found" rather than showing garbage matches
    
    **Decision Rule:**
    Ask yourself: "Is this matched name actually referring to the same entity as the search term, or is it just a phonetic coincidence?"
    - Same entity or close variation → KEEP
    - Phonetic coincidence with different meaning → REMOVE
    - All matches are phonetic coincidences → Return empty matches array []
    
    Here are the matches found:
    {json.dumps(clarifications, indent=2)}
    
    Return the filtered JSON array with irrelevant matches removed.
    Only modify the "matches" arrays - keep everything else the same.
    If ALL matches for a name are irrelevant, set its "matches" to an empty array [].
    
    Return ONLY the filtered JSON array (not wrapped in any object), no explanation.
    The response must be a JSON array of objects like: [{{"original_name": "...", "matches": [...]}}, ...]
    """
    
    try:
        filter_response = azure_client.chat.completions.create(
            model=azureopenai,
            messages=[
                {"role": "system", "content": "You filter irrelevant name matches. Be strict about phonetic matches - only keep if names are actually the same. Return only the filtered JSON array with the same structure as the input."},
                {"role": "user", "content": filtering_prompt}
            ],
            temperature=0,
            max_tokens=1000
        )
        
        filtered_json = filter_response.choices[0].message.content.strip()
        # Remove markdown code blocks if present
        filtered_json = filtered_json.replace("```json", "").replace("```", "").strip()
        
        # Parse and validate the JSON
        filtered_clarifications = json.loads(filtered_json)
        
        # Ensure it's a list
        if not isinstance(filtered_clarifications, list):
            print(f"[Name Clarifier] Filtering returned non-list: {type(filtered_clarifications)}, using original matches")
            filtered_clarifications = clarifications
        else:
            # Validate structure
            valid_structure = True
            for item in filtered_clarifications:
                if not isinstance(item, dict) or "matches" not in item or "original_name" not in item:
                    print(f"[Name Clarifier] Invalid filtered structure in item: {item}, using original matches")
                    filtered_clarifications = clarifications
                    valid_structure = False
                    break
            
            if valid_structure:
                print(f"[Name Clarifier] Successfully filtered irrelevant matches using LLM")
        
    except Exception as e:
        print(f"[Name Clarifier] Error filtering matches: {str(e)}, using original matches")
        filtered_clarifications = clarifications
    
    # If after filtering, no ambiguity remains, try to rewrite
    # Check both: multiple different names OR single name with multiple roles
    # Also check if matches array is empty (all filtered out as irrelevant)
    
    # Check for names where ALL matches were filtered out (irrelevant phonetic matches)
    names_with_no_matches = [
        item["original_name"] 
        for item in filtered_clarifications 
        if isinstance(item, dict) and len(item.get("matches", [])) == 0
    ]
    
    if names_with_no_matches:
        # All matches were filtered as irrelevant - return "not found" error
        names_str = ", ".join(f"'{name}'" for name in names_with_no_matches)
        return {
            "success": False,
            "error": f"Sorry, I couldn't find {names_str} in our system. 😔\n\n"
                   f"The phonetic matches found were not semantically relevant.\n\n"
                   f"Please check:\n"
                   f"  ✓ Spelling of the name(s)\n"
                   f"  ✓ Try using full names or abbreviations\n"
                   f"  ✓ Verify they exist in our system"
        }
    
    if all(
        isinstance(item, dict) and
        len(item.get("matches", [])) == 1 and 
        len(item.get("matches", [{}])[0].get("unique_roles", [])) <= 1
        for item in filtered_clarifications
    ):
        print("[Name Clarifier] After filtering, all names have single matches in single roles - rewriting query")
        return await handle_all_single_matches(original_query, filtered_clarifications)
    
    # Second chance: Even with multiple matches, check if top match is clearly the best
    # Apply same auto-selection logic to filtered results
    final_clarifications = []
    for item in filtered_clarifications:
        if not isinstance(item, dict):
            final_clarifications.append(item)
            continue
        
        matches = item.get("matches", [])
        
        if len(matches) > 1:
            # Check if top match is significantly better
            top_match = matches[0]
            second_match = matches[1] if len(matches) > 1 else None
            
            top_similarity = top_match.get("similarity", 0)
            second_similarity = second_match.get("similarity", 0) if second_match else 0
            top_length = top_match.get("match_length", 999)
            second_length = second_match.get("match_length", 999) if second_match else 999
            
            print(f"[Name Clarifier - Post-Filter] Checking auto-selection for '{item['original_name']}':")
            print(f"  Top: '{top_match['name']}' (sim: {top_similarity:.2f}, len: {top_length})")
            if second_match:
                print(f"  2nd: '{second_match['name']}' (sim: {second_similarity:.2f}, len: {second_length})")
            
            # Auto-select if clearly superior (same criteria as first pass)
            auto_select = (
                (top_similarity >= 1.0) or
                (top_similarity >= 0.8 and (top_similarity - second_similarity) >= 0.15) or
                (top_similarity >= 0.75 and top_length <= 0.6 * second_length and (top_similarity - second_similarity) >= 0.05) or
                ((top_similarity - second_similarity) < 0.1 and top_length <= 0.5 * second_length and top_similarity >= 0.7)
            )
            
            if auto_select:
                print(f"[Name Clarifier - Post-Filter] Auto-selecting '{top_match['name']}' after LLM filtering")
                final_clarifications.append({
                    "original_name": item["original_name"],
                    "matches": [top_match]
                })
            else:
                # Keep all matches for user selection
                final_clarifications.append(item)
        else:
            # Single match or no match - keep as is
            final_clarifications.append(item)
    
    # Check again if all have single matches after second auto-selection pass
    if all(
        isinstance(item, dict) and
        len(item.get("matches", [])) == 1 and 
        len(item.get("matches", [{}])[0].get("unique_roles", [])) <= 1
        for item in final_clarifications
    ):
        print("[Name Clarifier] After post-filter auto-selection, all names have single matches - rewriting query")
        return await handle_all_single_matches(original_query, final_clarifications)
    
    # Use final clarifications for message formatting
    filtered_clarifications = final_clarifications
    
    # Format names: single matches as confirmation, multiple matches for selection
    single_match_confirmations = []
    multiple_match_items = []
    
    for item in filtered_clarifications:
        # Safety check
        if not isinstance(item, dict):
            print(f"[Name Clarifier] Skipping non-dict item: {item}")
            continue
        
        name = item.get("original_name", "")
        matches = item.get("matches", [])
        
        if len(matches) == 1:
            # Single match - show as confirmation question
            match = matches[0]
            # Use unique_roles to avoid duplication
            unique_roles = match.get("unique_roles", [])
            if len(unique_roles) > 1:
                roles = "  or  ".join(unique_roles)
                single_match_confirmations.append(
                    f"For **{name}**, you mean **{match['name']}** — {roles}, right?"
                )
            elif len(unique_roles) == 1:
                single_match_confirmations.append(
                    f"For **{name}**, you mean **{match['name']}** — {unique_roles[0]}, right?"
                )
            else:
                # Fallback if no unique_roles
                single_match_confirmations.append(
                    f"For **{name}**, you mean **{format_category_name(match['category'])} {match['name']}**, right?"
                )
        
        elif len(matches) > 1:
            # Multiple matches - show to user for selection
            match_list = []
            for i, m in enumerate(matches[:5]):  # Top 5 matches
                # Use unique_roles to avoid duplication
                unique_roles = m.get("unique_roles", [])
                if len(unique_roles) > 1:
                    roles = "  or  ".join(unique_roles)
                    match_list.append(f"  {i+1}. **{m['name']}** — {roles}")
                elif len(unique_roles) == 1:
                    match_list.append(f"  {i+1}. **{m['name']}** — {unique_roles[0]}")
                else:
                    # Fallback if no unique_roles
                    match_list.append(f"  {i+1}. **{m['name']}** — {format_category_name(m['category'])}")
            
            multiple_match_items.append(
                f"**{name}**:\n" + "\n".join(match_list)
            )
    
    # Build clarification text
    clarification_parts = []
    
    if single_match_confirmations:
        clarification_parts.append("\n".join(single_match_confirmations))
    
    if multiple_match_items:
        if single_match_confirmations:
            clarification_parts.append("\nFor the following, please select:")
        clarification_parts.append("\n\n".join(multiple_match_items))
    
    clarification_text = "\n\n".join(clarification_parts)
    
    clarification_prompt = f"""
    The user's query contains names that need confirmation or selection.
    
    Message to show:
    {clarification_text}
    
    Generate a friendly, concise message:
    - If there are single match confirmations, acknowledge them warmly
    - If there are multiple matches, ask the user to select
    - Be warm and helpful with 1-2 emojis
    - Keep it brief and conversational
    - Include the formatted content above in your response
    
    Return ONLY the clarification message text.
    """
    
    try:
        response = azure_client.chat.completions.create(
            model=azureopenai,
            messages=[
                {"role": "system", "content": "You create friendly clarification messages that confirm single matches and ask for selection on multiple matches."},
                {"role": "user", "content": clarification_prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        clarification_message = response.choices[0].message.content.strip()
        print(f"[Name Clarifier] Generated clarification message: {clarification_message}")
        
        return {
            "success": False,
            "needs_clarification": True,
            "clarification_message": clarification_message,
            "matches": filtered_clarifications,
            "original_query": original_query
        }
        
    except Exception as e:
        print(f"[Name Clarifier] Error generating clarification: {str(e)}")
        # Fallback
        return {
            "success": False,
            "needs_clarification": True,
            "clarification_message": f"I found multiple matches! 😊 Please select:\n\n{clarification_text}",
            "matches": filtered_clarifications,
            "original_query": original_query
        }


def format_category_name(category: str) -> str:
    """
    Format category name for display.
    Uses configuration to get proper entity type labels.
    
    Examples:
        ContractorDisplayName -> Contractor
        TaskDesc -> Task
        CompanyEmployeeName -> Company Employee
        DepartmentName -> Department
        FieldActivityAlias -> Field Activity
    """
    # Check if we have config for this category
    config = CATEGORY_CONFIG.get(category)
    if config:
        entity_type = config.get("type", "")
        
        # Create friendly names based on type and category
        type_labels = {
            "contractor": "Contractor",
            "task": "Task",
            "person": "Employee",
            "organization": "Organization",
            "department": "Department",
            "section": "Section",
            "region": "Region",
            "activity": "Field Activity",
            "role": "ITS Role"
        }
        
        # Special handling for specific categories
        if category == "CompanySupervisorName":
            return "Supervisor"
        elif category == "CompanyManagerName":
            return "Manager"
        elif category == "CompanyEmployeeName":
            return "Company Employee"
        elif category == "ContractorEmployeeName":
            return "Contractor Employee"
        elif category == "TaskNum":
            return "Task Number"
        elif category == "TaskDesc":
            return "Task Description"
        
        return type_labels.get(entity_type, entity_type.title())
    
    # Fallback: format by removing "Name" and adding spaces
    display = category.replace("Name", "")
    display = re.sub(r'([A-Z])(?=[a-z])', r' \1', display).strip()
    return display