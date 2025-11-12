"""RAG-based intelligent code categorization and grouping."""

from __future__ import annotations

from typing import Dict, List, Optional
from collections import defaultdict

from utils import (
    get_chroma_client,
    get_default_chroma_dir,
    get_or_create_collection,
    query_collection,
    keyword_search_collection,
)


def group_codes_with_rag(
    codes: List[str],
    scope_text: str,
    collection_name: str,
) -> Dict[str, List[str]]:
    """
    Use RAG AND Firebase to intelligently group codes into categories based on their actual meaning and similarity.
    
    This function:
    1. Gets code explanations from Firebase (codes collection)
    2. Queries RAG to understand what each code is about
    3. Analyzes code descriptions to find similar codes
    4. Groups codes into categories based on actual work activities and hazards
    
    Args:
        codes: List of UFGS codes that require AHA
        scope_text: Scope of work from the document
        collection_name: RAG collection name
        
    Returns:
        Dictionary mapping category names to lists of codes
    """
    if not codes:
        return {}
    
    print(f"[rag_category_grouper] Grouping {len(codes)} codes using Firebase and RAG...")
    print(f"[rag_category_grouper] Scope context: {scope_text[:200]}...")
    
    # First, get code explanations from Firebase
    from section11.firebase_service import initialize_firestore_app, fetch_code_metadata
    
    firebase_descriptions = {}
    firebase_titles = {}
    
    try:
        db = initialize_firestore_app()
        metadata = fetch_code_metadata(db, codes)
        
        for code in codes:
            meta = metadata.get(code, {})
            if meta:
                # Get full text/explanation from Firebase
                firebase_text = meta.get("text", "")
                firebase_title = meta.get("title", "")
                
                # Extract meaningful text (may be XML, so parse it)
                if firebase_text:
                    # If it's XML, try to extract text content
                    import re
                    # Remove XML tags and get text content
                    text_content = re.sub(r'<[^>]+>', ' ', firebase_text)
                    text_content = ' '.join(text_content.split())[:1000]  # Limit to 1000 chars
                    firebase_descriptions[code] = text_content
                else:
                    firebase_descriptions[code] = ""
                
                firebase_titles[code] = firebase_title if firebase_title and not firebase_title.startswith("<?xml") else ""
                
                print(f"[rag_category_grouper]   Code {code}: Firebase title={firebase_titles[code][:50] if firebase_titles[code] else 'None'}")
    except Exception as e:
        print(f"[rag_category_grouper] WARNING: Could not fetch Firebase metadata: {e}")
    
    if not collection_name:
        print("[rag_category_grouper] WARNING: No collection name provided, using Firebase-only grouping")
        return _group_using_firebase_only(codes, firebase_descriptions, firebase_titles, scope_text)
    
    try:
        client = get_chroma_client(get_default_chroma_dir())
        collection = get_or_create_collection(client, collection_name)
        
        # For each code, combine Firebase explanation with RAG results
        code_categories = {}
        code_descriptions = {}
        code_keywords = {}
        
        for code in codes:
            print(f"[rag_category_grouper] Analyzing code {code}...")
            
            # Start with Firebase explanation
            firebase_desc = firebase_descriptions.get(code, "")
            firebase_title = firebase_titles.get(code, "")
            
            # Query RAG to get additional context
            rag_descriptions = []
            try:
                results1 = query_collection(collection, f"UFGS {code} activities work", n_results=5)
                results2 = query_collection(collection, f"{code} scope requirements", n_results=5)
                
                documents1 = results1.get("documents", [[]])[0] if results1.get("documents") else []
                documents2 = results2.get("documents", [[]])[0] if results2.get("documents") else []
                rag_descriptions = documents1 + documents2
            except Exception as e:
                print(f"[rag_category_grouper]   RAG query failed for {code}: {e}")
            
            # Combine Firebase explanation with RAG results
            combined_description = f"{firebase_title} {firebase_desc} {' '.join(rag_descriptions[:2])}"
            code_descriptions[code] = combined_description
            
            # Extract keywords from combined description
            keywords = _extract_keywords(combined_description)
            code_keywords[code] = keywords
            
            # Extract category from combined description and scope
            category = _extract_category_from_description(combined_description, code, scope_text)
            code_categories[code] = category
            
            print(f"[rag_category_grouper]   Code {code}: Category={category}, Keywords={keywords[:3]}")
        
        # Group codes by similarity - codes with similar keywords and categories go together
        grouped = _group_by_similarity(codes, code_descriptions, code_categories, code_keywords)
        
        print(f"[rag_category_grouper] Grouped into {len(grouped)} categories:")
        for cat, code_list in grouped.items():
            print(f"[rag_category_grouper]   {cat} ({len(code_list)} codes): {code_list}")
        
        return grouped
        
    except Exception as e:
        import traceback
        print(f"[rag_category_grouper] ERROR grouping codes: {e}")
        print(f"[rag_category_grouper] Traceback: {traceback.format_exc()}")
        return _group_using_firebase_only(codes, firebase_descriptions, firebase_titles, scope_text)


def _extract_keywords(description: str) -> List[str]:
    """Extract key terms from description."""
    import re
    # Common words to exclude
    stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "from", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will", "would", "shall", "should", "may", "might", "must", "can", "could"}
    
    words = re.findall(r'\b[a-z]{3,}\b', description.lower())
    keywords = [w for w in words if w not in stop_words]
    
    # Count frequency and return most common
    from collections import Counter
    word_counts = Counter(keywords)
    return [word for word, count in word_counts.most_common(10)]


def _extract_category_from_description(description: str, code: str, scope_text: str = "") -> str:
    """Extract category name from RAG description and scope."""
    description_lower = description.lower()
    scope_lower = scope_text.lower() if scope_text else ""
    combined = f"{description_lower} {scope_lower}"
    
    # Category mapping based on keywords - prioritize more specific matches
    category_keywords = {
        "Electrical / Energy Control": ["electrical", "power", "wiring", "circuit", "voltage", "energized", "electrical equipment", "electric", "energy control", "lockout", "tagout"],
        "Fall Protection & Prevention": ["fall", "falling", "elevated", "height", "roof", "scaffold", "ladder", "fall protection", "fall prevention"],
        "Excavation & Trenching": ["excavation", "trench", "digging", "underground", "soil", "cave-in", "trenching", "excavate"],
        "Confined Space Entry": ["confined space", "tank", "vault", "manhole", "enclosed", "confined"],
        "Cranes & Rigging": ["crane", "rigging", "lift", "hoist", "sling", "cranes"],
        "Demolition": ["demolition", "demolish", "removal", "tear down", "demolishing"],
        "Material Handling & Storage": ["material", "handling", "storage", "warehouse", "forklift", "material handling"],
        "Fire Prevention & Hot Work": ["fire", "welding", "hot work", "flame", "spark", "ignition", "fire prevention"],
        "Scaffolding & Access Systems": ["scaffold", "scaffolding", "platform", "walkway", "scaffolds"],
        "Hazardous Energy / LOTO": ["lockout", "tagout", "energy", "isolation", "de-energize", "lot", "lockout tagout"],
        "Environmental Controls": ["environmental", "hazardous material", "waste", "chemical", "environmental control"],
        "Mechanical Equipment": ["mechanical", "equipment", "machinery", "machine", "mechanical equipment"],
        "Structural Work": ["structural", "concrete", "steel", "masonry", "construction", "structural work"],
    }
    
    # Score each category based on keyword matches
    category_scores = {}
    for category, keywords in category_keywords.items():
        score = sum(1 for keyword in keywords if keyword in combined)
        if score > 0:
            category_scores[category] = score
    
    if category_scores:
        # Return category with highest score
        return max(category_scores.items(), key=lambda x: x[1])[0]
    
    # If no match, infer from code pattern
    if code.startswith("UFGS-01-11") or code.startswith("UFGS-01-12"):
        return "Electrical / Energy Control"
    elif code.startswith("UFGS-01-21") or "21" in code:
        return "Fall Protection & Prevention"
    elif code.startswith("UFGS-01-22") or "22" in code:
        return "Excavation & Trenching"
    
    return "General Construction"


def _group_by_similarity(
    codes: List[str],
    descriptions: Dict[str, str],
    categories: Dict[str, str],
    keywords: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Group codes by category, merging similar codes based on keyword overlap."""
    grouped = defaultdict(list)
    
    # First, group by explicit category
    for code in codes:
        category = categories.get(code, "Unmapped")
        grouped[category].append(code)
    
    # Merge categories with very similar keywords
    # If codes share significant keyword overlap, merge their categories
    category_list = list(grouped.keys())
    for i, cat1 in enumerate(category_list):
        for cat2 in category_list[i+1:]:
            codes1 = grouped[cat1]
            codes2 = grouped[cat2]
            
            # Check if codes in these categories share keywords
            keywords1 = set()
            keywords2 = set()
            for code in codes1:
                keywords1.update(keywords.get(code, []))
            for code in codes2:
                keywords2.update(keywords.get(code, []))
            
            # If significant overlap, merge categories
            overlap = keywords1.intersection(keywords2)
            if len(overlap) >= 3 and len(keywords1) > 0 and len(keywords2) > 0:
                # Merge cat2 into cat1
                grouped[cat1].extend(codes2)
                del grouped[cat2]
                print(f"[rag_category_grouper] Merged categories '{cat2}' into '{cat1}' based on keyword overlap: {overlap}")
                break
    
    return dict(grouped)


def _group_using_firebase_only(
    codes: List[str],
    firebase_descriptions: Dict[str, str],
    firebase_titles: Dict[str, str],
    scope_text: str,
) -> Dict[str, List[str]]:
    """Group codes using only Firebase explanations when RAG is unavailable."""
    grouped = defaultdict(list)
    
    for code in codes:
        # Use Firebase description and title to determine category
        description = f"{firebase_titles.get(code, '')} {firebase_descriptions.get(code, '')} {scope_text}"
        category = _extract_category_from_description(description, code, scope_text)
        grouped[category].append(code)
    
    return dict(grouped)


def _simple_grouping_fallback(codes: List[str]) -> Dict[str, List[str]]:
    """Fallback grouping when both Firebase and RAG are unavailable."""
    grouped = defaultdict(list)
    for code in codes:
        # Simple pattern-based grouping
        if "11" in code or "12" in code:
            grouped["Electrical / Energy Control"].append(code)
        elif "21" in code:
            grouped["Fall Protection & Prevention"].append(code)
        elif "22" in code:
            grouped["Excavation & Trenching"].append(code)
        else:
            grouped["General Construction"].append(code)
    return dict(grouped)

