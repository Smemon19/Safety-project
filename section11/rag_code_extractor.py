"""RAG-based code extraction from documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from utils import (
    get_chroma_client,
    get_default_chroma_dir,
    get_or_create_collection,
    query_collection,
)


def extract_codes_with_rag(document_text: str, collection_name: str) -> List[str]:
    """Extract UFGS codes from document text using regex. 
    
    Handles UFGS-XX-XX-XX, UFGS-XX-XX-XX-XX, and UFGS-XX-XX-XX-XX-XX formats.
    Returns codes in their original format (preserves 4-part and 5-part codes).
    IMPORTANT: Extracts codes in order of specificity (longest first) to avoid partial matches.
    """
    if not document_text or not document_text.strip():
        print("[rag_code_extractor] ERROR: Document text is empty")
        return []
    
    print(f"[rag_code_extractor] Extracting UFGS codes from document ({len(document_text)} chars)")
    
    import re
    
    codes_found = []
    seen = set()
    
    # Extract in order of specificity (longest first) to avoid partial matches
    # Pattern 1: UFGS-XX-XX-XX-XX-XX (5 parts)
    ufgs_5part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_5part.finditer(document_text):
        code = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}-{match.group(4)}-{match.group(5)}"
        code_upper = code.upper()
        if code_upper not in seen:
            seen.add(code_upper)
            codes_found.append(code_upper)
    
    # Pattern 2: UFGS-XX-XX-XX-XX (4 parts)
    ufgs_4part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_4part.finditer(document_text):
        code = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}-{match.group(4)}"
        code_upper = code.upper()
        # Check if this is a substring of an already-found 5-part code
        is_substring = any(code_upper in found_code for found_code in codes_found)
        if not is_substring and code_upper not in seen:
            seen.add(code_upper)
            codes_found.append(code_upper)
    
    # Pattern 3: UFGS-XX-XX-XX (3 parts)
    ufgs_3part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_3part.finditer(document_text):
        code = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}"
        code_upper = code.upper()
        # Check if this is a substring of an already-found longer code
        is_substring = any(code_upper in found_code for found_code in codes_found)
        if not is_substring and code_upper not in seen:
            seen.add(code_upper)
            codes_found.append(code_upper)
    
    # Pattern 4: XX XX XX format (space-separated)
    ufgs_spaces = re.compile(r'\b(\d{2})\s+(\d{2})\s+(\d{2})\b')
    for match in ufgs_spaces.finditer(document_text):
        code = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}"
        code_upper = code.upper()
        # Check if this is a substring of an already-found longer code
        is_substring = any(code_upper in found_code for found_code in codes_found)
        if not is_substring and code_upper not in seen:
            seen.add(code_upper)
            codes_found.append(code_upper)
    
    codes_list = sorted(codes_found)
    print(f"[rag_code_extractor] Found {len(codes_list)} UFGS codes: {codes_list}")
    return codes_list


def verify_document_parsing(document_text: str, parsed_codes: List[str]) -> dict:
    """Verify that document was properly parsed."""
    verification = {
        "document_length": len(document_text),
        "has_content": len(document_text.strip()) > 0,
        "codes_found": len(parsed_codes),
        "codes": parsed_codes,
        "valid": True,
        "issues": []
    }
    
    if not verification["has_content"]:
        verification["valid"] = False
        verification["issues"].append("Document text is empty")
    
    if verification["document_length"] < 100:
        verification["issues"].append(f"Document seems very short ({verification['document_length']} chars)")
    
    if verification["codes_found"] == 0:
        verification["issues"].append("No codes found in document")
    
    return verification


def check_codes_against_firebase(codes: List[str]) -> dict:
    """Check which codes require AHA by querying Firebase."""
    from section11.firebase_service import initialize_firestore_app, fetch_code_decisions
    
    if not codes:
        return {"codes_requiring_aha": [], "codes_not_requiring": [], "codes_unknown": []}
    
    print(f"[check_codes_against_firebase] Checking {len(codes)} codes against Firebase...")
    
    try:
        db = initialize_firestore_app()
        decisions = fetch_code_decisions(db, codes)
        
        requiring_aha = []
        not_requiring = []
        unknown = []
        
        for code in codes:
            decision = decisions.get(code, {})
            requires_aha = decision.get("requiresAha")
            status = decision.get("status", "unknown")
            
            # Debug: print what we found for each code
            print(f"[check_codes_against_firebase] Code {code}: decision={decision}, requiresAha={requires_aha}, status={status}")
            
            if requires_aha is True:
                requiring_aha.append(code)
            elif requires_aha is False:
                not_requiring.append(code)
            else:
                # If status is "firestore" but requiresAha is missing, that's a data issue
                if status == "firestore":
                    print(f"[check_codes_against_firebase] WARNING: Code {code} exists in Firebase but has no requiresAha field!")
                unknown.append(code)
        
        print(f"[check_codes_against_firebase] Results: {len(requiring_aha)} require AHA, {len(not_requiring)} don't, {len(unknown)} unknown")
        
        return {
            "codes_requiring_aha": requiring_aha,
            "codes_not_requiring": not_requiring,
            "codes_unknown": unknown,
            "total_checked": len(codes),
        }
    except Exception as e:
        import traceback
        print(f"[check_codes_against_firebase] ERROR: {e}")
        print(f"[check_codes_against_firebase] Traceback: {traceback.format_exc()}")
        return {
            "codes_requiring_aha": [],
            "codes_not_requiring": [],
            "codes_unknown": codes,
            "error": str(e),
        }

