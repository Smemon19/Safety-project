"""Core generation routines for Section 11."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from pydantic import BaseModel

from utils import (
    build_section_search_terms,
    get_chroma_client,
    get_default_chroma_dir,
    get_or_create_collection,
    keyword_search_collection,
    query_collection,
)

from section11.constants import EM_385_CATEGORIES
from section11.models import (
    AhaEvidence,
    CategoryBundle,
    CategoryStatus,
    ParsedCode,
    SafetyPlanEvidence,
)


def _merge_results(vector: Dict[str, List[List[str]]], keyword: Dict[str, List[List[str]]]) -> Dict[str, List[List[str]]]:
    ids = [*vector.get("ids", [[]])[0]]
    docs = [*vector.get("documents", [[]])[0]]
    metas = [*vector.get("metadatas", [[]])[0]]
    seen = set(ids)
    for idx, identifier in enumerate(keyword.get("ids", [[]])[0]):
        if identifier in seen:
            continue
        ids.append(identifier)
        docs.append(keyword.get("documents", [[]])[0][idx])
        metas.append(keyword.get("metadatas", [[]])[0][idx])
        seen.add(identifier)
    return {"ids": [ids], "documents": [docs], "metadatas": [metas]}


def _clean_sentence(text: str) -> str:
    sentence = re.sub(r"\s+", " ", (text or "").strip())
    return sentence[:320]


def _is_complete_sentence(text: str) -> bool:
    """Check if text is a complete sentence with proper structure."""
    if not text or len(text.strip()) < 50:  # Minimum length for complete thought
        return False
    
    text_lower = text.lower().strip()
    
    # Filter out OCR artifacts and document metadata
    if any(artifact in text for artifact in ["[OCR Merge]", "EM 385-1-1 •", "EM 385-1-1 *", "15 March 2024"]):
        return False
    
    # Filter out sentences that start with lowercase (likely mid-sentence fragments)
    if text[0].islower() and not text[0].isalpha():
        return False
    
    # Must end with proper punctuation (unless it's a list item)
    if not text.rstrip().endswith(('.', '!', '?', ':', ';')):
        # Allow if it's clearly a complete phrase (has verb and noun)
        if not re.search(r'\b(is|are|was|were|has|have|can|may|will|should|must|shall|exposes|causes|creates|presents|involves|requires)\b', text_lower):
            return False
    
    # Must have at least one verb and one noun (basic sentence structure)
    has_verb = bool(re.search(r'\b(is|are|was|were|has|have|can|may|will|should|must|shall|exposes|causes|creates|presents|involves|requires|occurs|happens|exists|results|leads)\b', text_lower))
    has_noun = bool(re.search(r'\b(hazard|risk|exposure|injury|danger|worker|personnel|equipment|system|work|activity|condition|situation)\b', text_lower))
    
    if not (has_verb or has_noun):
        return False
    
    # Filter out raw regulation text patterns
    raw_reg_patterns = [
        r'yes\s+no\s+\d+',  # "YES NO 1910..."
        r'\d{4}\.\d+\([a-z]\)',  # "1910.269(d)"
        r'is this a\s+',  # "Is this a maritime project?"
        r'conforms to\s+\d+\s+cfr',  # "conforms to 29 CFR"
        r'see\s+paragraph\s+\d+',  # "see paragraph 11-8"
        r'\(11-\d+[\.\)]',  # "(11-6.c)"
        r'according to paragraph',  # "according to paragraph 11-8"
        r'\(36-\d+[\.\)]',  # "(36-2.f)"
        r'\(b\)\s+limited',  # "(b) Limited Approach Boundary"
    ]
    for pattern in raw_reg_patterns:
        if re.search(pattern, text_lower):
            return False
    
    # Filter out fragments that are clearly incomplete
    if text_lower.startswith(('b ', 'c ', 'd ', 'e ', 'f ', '(', '•', '- ', '2 ', '3 ', '4 ', '5 ', '6 ', '7 ', '8 ', '9 ', 'needed to', 'include, but', 'these may')):
        return False
    
    # Filter out sentences that end with incomplete fragments
    if text.rstrip().endswith((' (1', ' (2', ' (3', ' (4', ' (a', ' (b', ' (c', ' EM 385', '°', ' 624', ' 478', ' 548', ' h', ' g')):
        return False
    
    # Filter out sentences that are mostly citations or references
    if re.match(r'^[\d\s\-\.\(\)]+$', text.strip()):
        return False
    
    # Filter out sentences that are just URLs or file references
    if any(skip in text_lower for skip in ["http://", "https://", "www.", ".pdf", "table 431-1"]):
        return False
    
    # Filter out sentences that are just lists of conditions without context
    if text_lower.count(';') > 3:  # Too many semicolons = likely a list fragment
        return False
    
    # Must have reasonable word count (at least 8 words for a complete thought)
    word_count = len(text.split())
    if word_count < 8:
        return False
    
    # Must not be just a phrase fragment (check for subject-verb-object structure)
    # If it starts with lowercase and has no capital letter after first word, likely a fragment
    words = text.split()
    if len(words) > 1 and words[0][0].islower() and not any(w[0].isupper() for w in words[1:3]):
        return False
    
    return True


def _extract_sentences(documents: Iterable[str]) -> List[str]:
    """Extract meaningful, complete sentences from documents."""
    sentences: List[str] = []
    # Use more sophisticated splitting to preserve complete sentences
    splitter = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")  # Split on sentence endings followed by capital
    for doc in documents:
        if not doc or not doc.strip():
            continue
        # Split into sentences
        parts = splitter.split(doc or "")
        for part in parts:
            cleaned = _clean_sentence(part)
            if cleaned and _is_complete_sentence(cleaned):
                sentences.append(cleaned)
    return sentences


_HAZARD_KEYWORDS = {
    "hazard",
    "exposure",
    "risk",
    "injury",
    "fatal",
    "shock",
    "arc",
    "energized",
    "electrical",
    "danger",
    "unsafe",
    "accident",
    "death",
    "serious injury",
    "harm",
    "dangerous",
    "threat",
    "peril",
    "jeopardy",
    "potential hazard",
    "hazardous condition",
    "exposed",
    "at risk",
    "vulnerable",
    "susceptible",
    "arc flash",
    "arc-flash",
    "shock hazard",
    "electrocution",
    "burn injury",
}

_CONTROL_PATTERNS = [
    r"\b(?:shall|must|ensure|provide|require|implement|establish|develop|apply|wear|use|maintain)\b[^.]*",
    r"\b(?:workers?|employees?|contract(?:or)?s?)\s+(?:shall|must|will)\b[^.]*",
    r"\b(?:contractor|employer)\s+(?:shall|must|ensure)\b[^.]*",
]

_SCENARIO_TRIGGERS = {
    "when",
    "while",
    "during",
    "if ",
    "if an",
    "if the",
    "whenever",
    "before",
    "after",
    "result",
    "lead to",
    "cause",
    "due to",
    "because",
    "expose",
    "exposes",
    "exposed",
    "contact",
}

_CONTROL_SKIP_TERMS = {
    "program",
    "programs",
    "procedure",
    "procedures",
    "plan",
    "planning",
    "job safety",
    "job-safety",
    "electrical safety plan",
    "eewp",
    "comply",
    "compliance",
    "training",
    "ensure",
    "ensuring",
    "develop",
    "develops",
    "developed",
    "employees are to",
    "employer",
    "contract employer",
    "each person",
    "information:",
}

_HAZARD_THEME_TEMPLATES = [
    (
        {"arc flash", "arc-flash", "arc fault", "arc-flash"},
        "Arc-flash events can release incident energy exceeding 35 cal/cm², producing plasma, blast pressure, and molten metal that cause catastrophic burns, hearing damage, and shrapnel injuries when conductors fault while energized.",
    ),
    (
        {"shock", "shock hazard", "electrocution"},
        "Contact with energized conductors above 50 volts drives lethal shock currents, triggering ventricular fibrillation or secondary falls whenever limited or restricted approach boundaries are crossed or insulating barriers fail.",
    ),
    (
        {"energized", "energized equipment", "energized electrical"},
        "Working on or near energized switchgear and feeders keeps lethal energy present; unexpected backfeed, stored capacitor charge, or failure to establish an electrically safe work condition can re-energize components the crew is handling.",
    ),
    (
        {"incident energy", "cal/cm", "thermal", "burn"},
        "High incident energy and thermal radiation from faults can penetrate PPE, ignite clothing, and cause second-degree burns within milliseconds unless arc flash boundaries and PPE categories are engineered correctly.",
    ),
    (
        {"approach boundary", "limited approach", "restricted approach", "flash protection boundary"},
        "Crossing the arc flash or shock approach boundary exposes workers to plasma pressure waves and flashover paths that bypass PPE, especially inside cramped electrical rooms where escape distance is limited.",
    ),
]

_SCENARIO_TEMPLATES = [
    (
        {"electrically safe work condition", "eswc", "intentional contact"},
        "Performing intentional contact or diagnostic work on equipment that has not been placed in an electrically safe work condition exposes personnel directly to energized parts and arc-flash paths.",
    ),
    (
        {"limited approach", "restricted approach", "approach boundary"},
        "Crossing limited or restricted approach boundaries without full isolation positions the body within shock and arc-flash reach distances, making minor slips or dropped tools catastrophic.",
    ),
    (
        {"routine circuit switching", "electrical measurements", "testing", "troubleshooting"},
        "Routine switching, testing, or troubleshooting on energized circuits—especially above 600 volts—can initiate fault currents when insulation fails or conductive tools bridge phases.",
    ),
    (
        {"incident energy", "ie", "cal/cm"},
        "Incident-energy values high enough to cause second-degree burns demand precise calculations and PPE categories; misapplied ratings leave exposed skin vulnerable during faults.",
    ),
]


def _strip_control_language(text: str) -> str:
    cleaned = text
    for pattern in _CONTROL_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _hazard_sentences(sentences: Iterable[str]) -> List[str]:
    """Extract hazard-focused sentences, softening control language instead of dropping them."""
    hazards: List[str] = []
    for sentence in sentences:
        if not _is_complete_sentence(sentence):
            continue
        
        lower = sentence.lower()
        if not any(keyword in lower for keyword in _HAZARD_KEYWORDS):
            continue
        
        cleaned = _strip_control_language(sentence)
        cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", cleaned)
        cleaned = re.sub(r"\s*\[[^\]]*\]\s*$", "", cleaned)
        cleaned = cleaned.strip("•- ")

        if len(cleaned.split()) < 8:
            continue
        
        hazards.append(cleaned)

    return _dedupe(hazards)[:15]


def _scenario_sentences(sentences: Iterable[str]) -> List[str]:
    scenarios: List[str] = []
    for sentence in sentences:
        if not _is_complete_sentence(sentence):
            continue
        lower = sentence.lower()
        if not any(trigger in lower for trigger in _SCENARIO_TRIGGERS):
            continue
        if not any(keyword in lower for keyword in _HAZARD_KEYWORDS):
            continue
        cleaned = _strip_control_language(sentence)
        cleaned = cleaned.strip("•- ")
        lowered = cleaned.lower()
        if len(cleaned.split()) < 10:
            continue
        if any(term in lowered for term in _CONTROL_SKIP_TERMS):
            continue
        if not any(core in lowered for core in ["arc", "electrical", "shock", "energized", "voltage", "boundary"]):
            continue
        if re.search(r'\([a-z0-9-]+\)', cleaned):
            continue
        if not re.search(r'[.!?]$', cleaned):
            cleaned = cleaned.rstrip('.') + '.'
        scenarios.append(cleaned)
    return _dedupe(scenarios)[:10]


def _control_sentences(sentences: Iterable[str]) -> List[str]:
    controls: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if any(keyword in lower for keyword in ["shall", "must", "ensure", "provide", "require", "permit", "ppe", "training", "inspection"]):
            controls.append(sentence)
    return controls[:12]


def _dedupe(items: Iterable[str]) -> List[str]:
    """Remove duplicates using exact and semantic similarity matching."""
    seen: set[str] = set()
    deduped: List[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        # Exact match check
        key = normalized.lower()
        if key in seen:
            continue
        
        # Semantic similarity check - remove very similar sentences
        # Normalize for comparison: remove extra spaces, punctuation variations
        normalized_for_comparison = re.sub(r'[^\w\s]', '', key)
        normalized_for_comparison = re.sub(r'\s+', ' ', normalized_for_comparison)
        
        # Check if this is very similar to an existing item (80% word overlap)
        is_duplicate = False
        for existing_key in seen:
            existing_normalized = re.sub(r'[^\w\s]', '', existing_key)
            existing_normalized = re.sub(r'\s+', ' ', existing_normalized)
            
            # Simple word overlap check
            existing_words = set(existing_normalized.split())
            current_words = set(normalized_for_comparison.split())
            
            if existing_words and current_words:
                overlap = len(existing_words & current_words) / max(len(existing_words), len(current_words))
                if overlap > 0.8:  # 80% word overlap = likely duplicate
                    is_duplicate = True
                    break
        
        if is_duplicate:
            continue
        
        seen.add(key)
        deduped.append(normalized)
    return deduped


class RetrievalContext(BaseModel):
    documents: List[str]
    metadatas: List[Dict[str, object]]


def retrieve_context(category: str, scope: List[str], codes: List[str], collection_name: str | None) -> RetrievalContext:
    """Retrieve comprehensive context from 385 RAG for category-specific analysis. Runs synchronously."""
    if not collection_name:
        print(f"[retrieve_context] ERROR: No collection_name provided for {category}")
        return RetrievalContext(documents=[], metadatas=[])
    
    print(f"[retrieve_context] Querying collection '{collection_name}' for category '{category}' with codes: {codes[:3]}...")
    
    try:
        client = get_chroma_client(get_default_chroma_dir())
        collection = get_or_create_collection(client, collection_name)
        
        # Check if collection has any documents
        count_result = collection.count()
        print(f"[retrieve_context] Collection '{collection_name}' has {count_result} documents")
        
        if count_result == 0:
            print(f"[retrieve_context] WARNING: Collection '{collection_name}' is empty!")
            return RetrievalContext(documents=[], metadatas=[])
        
        # Build comprehensive query terms combining category, codes, and scope
        query_terms = build_section_search_terms(" ".join([category, *codes]))
        scope_hint = " ".join(scope[:3]) if scope else ""
        
        # Include codes in queries for better context
        codes_str = " ".join(codes[:3]) if codes else ""
        
        # Primary query for hazards - SPECIFICALLY ask for detailed hazard descriptions with context
        primary_query = f"EM 385 {category} {codes_str} electrical hazards detailed description workers exposed risk injury shock arc flash electrocution potential dangers what are the hazards"
        
        # Secondary query for specific hazard scenarios and exposure conditions with scope context
        if scope_hint:
            secondary_query = f"EM 385 {category} {codes_str} hazard exposure scenarios conditions risk factors detailed analysis {scope_hint} work activities"
        else:
            secondary_query = f"EM 385 {category} {codes_str} hazard exposure scenarios conditions risk factors detailed analysis work activities"
        
        # Tertiary query for category-specific hazards with codes
        category_hazard_query = f"EM 385 {category} {codes_str} what hazards are present potential dangers risks workers may be exposed to detailed hazard identification"
        
        # Fourth query specifically about the codes and their associated hazards
        if codes_str:
            codes_hazard_query = f"EM 385 {codes_str} hazards associated with these codes what are the specific dangers risks exposures"
        else:
            codes_hazard_query = None
        
        print(f"[retrieve_context] Executing vector query 1 (hazards): '{primary_query[:80]}...'")
        # Retrieve more results for comprehensive analysis
        vector1 = query_collection(collection, primary_query, n_results=25)
        vec1_count = len(vector1.get("documents", [[]])[0])
        print(f"[retrieve_context] Vector query 1 returned {vec1_count} documents")
        
        print(f"[retrieve_context] Executing vector query 2 (hazard scenarios): '{secondary_query[:80]}...'")
        vector2 = query_collection(collection, secondary_query, n_results=20)
        vec2_count = len(vector2.get("documents", [[]])[0])
        print(f"[retrieve_context] Vector query 2 returned {vec2_count} documents")
        
        print(f"[retrieve_context] Executing vector query 3 (category hazards): '{category_hazard_query[:80]}...'")
        vector3 = query_collection(collection, category_hazard_query, n_results=20)
        vec3_count = len(vector3.get("documents", [[]])[0])
        print(f"[retrieve_context] Vector query 3 returned {vec3_count} documents")
        
        # Execute fourth query if codes are available
        vector4_result = None
        if codes_hazard_query:
            print(f"[retrieve_context] Executing vector query 4 (code-specific hazards): '{codes_hazard_query[:80]}...'")
            vector4_result = query_collection(collection, codes_hazard_query, n_results=15)
            vec4_count = len(vector4_result.get("documents", [[]])[0])
            print(f"[retrieve_context] Vector query 4 returned {vec4_count} documents")
        
        keyword_terms = [category, *codes[:5]]
        if scope_hint:
            keyword_terms.append(scope_hint)
        print(f"[retrieve_context] Executing keyword search with terms: {keyword_terms[:3]}")
        keyword = keyword_search_collection(collection, keyword_terms, max_results=15)
        kw_count = len(keyword.get("documents", [[]])[0])
        print(f"[retrieve_context] Keyword search returned {kw_count} documents")
        
        # Merge all results
        merged1 = _merge_results(vector1, keyword)
        merged2 = _merge_results(vector2, {})
        merged3 = _merge_results(vector3, {})
        merged4 = _merge_results(vector4_result, {}) if vector4_result else {}
        
        # Combine results, prioritizing unique documents
        all_ids = [*merged1.get("ids", [[]])[0]]
        all_docs = [*merged1.get("documents", [[]])[0]]
        all_metas = [*merged1.get("metadatas", [[]])[0]]
        seen = set(all_ids)
        
        # Add vector2 results
        vec2_ids = merged2.get("ids", [[]])[0]
        vec2_docs = merged2.get("documents", [[]])[0]
        vec2_metas = merged2.get("metadatas", [[]])[0]
        
        for idx, doc_id in enumerate(vec2_ids):
            if doc_id not in seen:
                all_ids.append(doc_id)
                if idx < len(vec2_docs):
                    all_docs.append(vec2_docs[idx])
                if idx < len(vec2_metas):
                    all_metas.append(vec2_metas[idx])
                seen.add(doc_id)
        
        # Add vector3 results
        vec3_ids = merged3.get("ids", [[]])[0]
        vec3_docs = merged3.get("documents", [[]])[0]
        vec3_metas = merged3.get("metadatas", [[]])[0]
        
        for idx, doc_id in enumerate(vec3_ids):
            if doc_id not in seen:
                all_ids.append(doc_id)
                if idx < len(vec3_docs):
                    all_docs.append(vec3_docs[idx])
                if idx < len(vec3_metas):
                    all_metas.append(vec3_metas[idx])
                seen.add(doc_id)
        
        # Add vector4 results (code-specific hazards)
        if merged4:
            vec4_ids = merged4.get("ids", [[]])[0]
            vec4_docs = merged4.get("documents", [[]])[0]
            vec4_metas = merged4.get("metadatas", [[]])[0]
            
            for idx, doc_id in enumerate(vec4_ids):
                if doc_id not in seen:
                    all_ids.append(doc_id)
                    if idx < len(vec4_docs):
                        all_docs.append(vec4_docs[idx])
                    if idx < len(vec4_metas):
                        all_metas.append(vec4_metas[idx])
                    seen.add(doc_id)
        
        # Get more documents for in-depth analysis (increased from 30 to 40)
        documents = [doc for doc in all_docs[:40] if doc and doc.strip()]
        metadatas = all_metas[:min(40, len(documents))]
        
        print(f"[retrieve_context] Final result: {len(documents)} non-empty documents for {category}")
        
        # If no documents retrieved, return empty context
        if not documents:
            print(f"[retrieve_context] WARNING: No documents retrieved for {category} after filtering")
            return RetrievalContext(documents=[], metadatas=[])
        
        return RetrievalContext(documents=documents, metadatas=metadatas)
    except Exception as e:
        # Log error but don't crash - return empty context
        import traceback
        print(f"[retrieve_context] ERROR retrieving context for {category}: {e}")
        print(f"[retrieve_context] Traceback: {traceback.format_exc()}")
        return RetrievalContext(documents=[], metadatas=[])


def build_aha_evidence(category: str, context: RetrievalContext, scope: List[str]) -> AhaEvidence:
    """Build detailed AHA evidence using 385 RAG context."""
    # Ensure we have actual retrieved documents, not empty templates
    if not context.documents or not any(doc.strip() for doc in context.documents):
        return AhaEvidence(
            hazards=[],
            narrative=[],
            citations=[],
            status=CategoryStatus.pending,
            pending_reason=f"No EM 385 content retrieved for {category}. Please ensure the RAG collection is properly indexed with EM 385 documents.",
        )
    
    sentences = _extract_sentences(context.documents)
    if not sentences:
        return AhaEvidence(
            hazards=[],
            narrative=[],
            citations=[],
            status=CategoryStatus.pending,
            pending_reason=f"Could not extract meaningful content from retrieved documents for {category}. Retrieved documents may be empty or improperly formatted.",
        )
    
    hazard_sentences = _hazard_sentences(sentences)
    hazards = []
    for sentence in hazard_sentences:
        cleaned = sentence.strip()
        cleaned = re.sub(r'\s*\(11-\d+[\.\)].*$', '', cleaned)
        cleaned = re.sub(r'\s*\(p\.\s*\d+\)\s*$', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s*\[[^\]]*\]\s*$', '', cleaned)
        cleaned = re.sub(r':\s*[a-z]\b\.?$', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+[a-z]\b$', '', cleaned, flags=re.IGNORECASE)
        cleaned = ' '.join(cleaned.split()).strip('.,;: ')
        lowered = cleaned.lower()
        if not cleaned or len(cleaned.split()) < 12 or any(term in lowered for term in _CONTROL_SKIP_TERMS):
            continue
        if not re.search(r'[.!?]$', cleaned):
            if len(cleaned.split()) < 15:
                continue
            cleaned = cleaned.rstrip('.') + '.'
        hazards.append(cleaned)

    hazards = _dedupe(hazards)[:8]

    hazard_lower = [h.lower() for h in hazards]
    summary_hazards: List[str] = []
    matched_indices: set[int] = set()
    for keywords, template in _HAZARD_THEME_TEMPLATES:
        match_idx = next((idx for idx, lower in enumerate(hazard_lower) if any(keyword in lower for keyword in keywords)), None)
        if match_idx is not None and template not in summary_hazards:
            summary_hazards.append(template)
            matched_indices.add(match_idx)

    if not summary_hazards and hazards:
        summary_hazards = hazards[: min(3, len(hazards))]

    supporting_hazards = [hazards[idx] for idx in range(len(hazards)) if idx not in matched_indices][:3]

    scenario_sentences = _scenario_sentences(sentences)
    scenario_sentences = _dedupe([s for s in scenario_sentences if s not in hazards])[:6]
    scenario_lower = [s.lower() for s in scenario_sentences]
    scenario_summary: List[str] = []
    scenario_matched: set[int] = set()
    for keywords, template in _SCENARIO_TEMPLATES:
        match_idx = next((idx for idx, lower in enumerate(scenario_lower) if any(keyword in lower for keyword in keywords)), None)
        if match_idx is not None and template not in scenario_summary:
            scenario_summary.append(template)
            scenario_matched.add(match_idx)

    if not scenario_summary and scenario_sentences:
        scenario_summary = scenario_sentences[: min(2, len(scenario_sentences))]

    scenario_support = []
    for idx, s in enumerate(scenario_sentences):
        if idx in scenario_matched:
            continue
        if len(s.split()) < 12:
            continue
        if re.search(r'\([a-z0-9-]+\)', s):
            continue
        if ':' in s:
            continue
        lower_support = s.lower()
        if lower_support.startswith('all employees'):
            continue
        if 'which involve' in lower_support:
            continue
        if ', .' in s or s.strip().endswith(', .'):
            continue
        scenario_support.append(s)
        if len(scenario_support) >= 2:
            break
    
    # Build comprehensive narrative with detailed HAZARD ANALYSIS ONLY
    # IMPORTANT: This AHA contains ONLY hazards - NO controls, solutions, PPE, or procedures
    narrative = []
    
    # Introduction section - only if we have actual scope content
    if scope and any(s.strip() for s in scope):
        # Only use actual scope content, not generic templates
        actual_scope = [s.strip() for s in scope if s.strip()][:3]
        if actual_scope:
            narrative.append(f"This Activity Hazard Analysis (AHA) identifies and analyzes hazards associated with {category} activities.")
            narrative.append(f"Project context: {'; '.join(actual_scope)}.")
    
    # Detailed hazard identification section - ONLY hazards, no controls
    if summary_hazards:
        narrative.append(f"Primary hazard modes for {category} activities:")
        for hazard in summary_hazards:
            narrative.append(f"  • {hazard}")

    if supporting_hazards:
        narrative.append("Representative EM 385 hazard excerpts:")
        for hazard in supporting_hazards:
            narrative.append(f"  - {hazard}")

    if scenario_summary:
        narrative.append("Detailed exposure scenarios captured in EM 385 include:")
        for scenario in scenario_summary:
            narrative.append(f"  - {scenario}")

    if scenario_support:
        narrative.append("Representative EM 385 scenario excerpts:")
        for scenario in scenario_support:
            narrative.append(f"  - {scenario}")

    risk_candidates: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if not any(kw in lower for kw in ["condition", "risk", "exposure", "likelihood", "severity", "probability", "increases", "elevates", "amplifies", "worsen", "escalate", "intensify"]):
            continue
        if not any(core in lower for core in ["arc", "electrical", "shock", "energ"]):
            continue
        if "assessment" in lower:
            continue
        if "plan" in lower:
            continue
        if "which involve" in lower:
            continue
        cleaned = _strip_control_language(sentence).strip('.,;: ')
        if len(cleaned.split()) < 10:
            continue
        risk_candidates.append(cleaned)

    risk_sentences = []
    for risk in _dedupe(risk_candidates)[:6]:
        if not re.search(r'[.!?]$', risk):
            if len(risk.split()) < 15:
                continue
            risk = risk.rstrip('.') + '.'
        risk_sentences.append(risk)

        if risk_sentences:
            narrative.append("Conditions that may elevate the likelihood or severity of these hazards include:")
            for risk_s in risk_sentences:
                    narrative.append(f"  - {risk_s}")

    additional_hazards: List[str] = []
    if hazards or scenario_sentences:
        seen_hazard_text = set(hazards) | set(scenario_sentences)
        for raw in sentences:
            lower = raw.lower()
            if not any(kw in lower for kw in ["hazard", "danger", "exposure", "risk", "injury", "harm", "shock", "arc", "energized", "burn"]):
                continue
            cleaned = _strip_control_language(raw).strip('.,;: ')
            if len(cleaned.split()) < 8:
                continue
            if cleaned in seen_hazard_text:
                continue
            if any(control in lower for control in ["shall", "must"]) or any(term in lower for term in _CONTROL_SKIP_TERMS):
                continue
            if ':' in cleaned and 'hazard' not in cleaned.lower():
                continue
            if not any(core in cleaned.lower() for core in ["arc", "electrical", "shock", "energ"]):
                continue
            if not re.search(r'[.!?]$', cleaned):
                if len(cleaned.split()) < 12:
                    continue
                cleaned = cleaned.rstrip('.') + '.'
            additional_hazards.append(cleaned)
            seen_hazard_text.add(cleaned)
            if len(additional_hazards) >= 4:
                break

        if additional_hazards:
            narrative.append("Additional hazard signals from EM 385 excerpts:")
            for extra in additional_hazards:
                narrative.append(f"  • {extra}")

    if not hazards and not scenario_sentences:
        hazard_only_sentences = [
            _strip_control_language(s).strip('.,;: ') for s in sentences 
            if s.strip() 
            and any(kw in s.lower() for kw in ["hazard", "danger", "exposure", "risk", "injury", "harm"])
            and not any(control in s.lower() for control in ["shall", "must", "provide", "ensure", "require", "ppe", "training", "control", "procedure"])
        ][:5]
        if hazard_only_sentences:
            narrative.append(f"Hazard identification for {category} activities based on EM 385 requirements:")
            narrative.extend([f"  • {s}" for s in hazard_only_sentences])
        else:
            narrative.append(f"Hazard identification for {category} activities requires further review based on project-specific conditions.")
    
    citations: List[Dict[str, str]] = []
    seen_citations = set()
    # Normalize citations by section only (not page number) to avoid duplicates
    for meta in context.metadatas[:15]:  # Check more for better citation coverage
        meta = meta or {}
        section_path = str(meta.get("section_path") or meta.get("headers") or "").strip()
        page_label = str(meta.get("page_label") or meta.get("page_number") or "").strip()
        
        # Normalize section path - extract meaningful section identifier
        if not section_path or section_path.lower() in ["none", "null", ""]:
            continue
        
        # Use section path as primary key (normalize it)
        normalized_section = re.sub(r'\s+', ' ', section_path).strip()
        # Remove page-specific references from section path
        normalized_section = re.sub(r'\s*\(p\.\s*\d+\)', '', normalized_section, flags=re.IGNORECASE)
        normalized_section = re.sub(r'\s*page\s+\d+', '', normalized_section, flags=re.IGNORECASE)
        # Remove any trailing page numbers
        normalized_section = re.sub(r'\s+\d+$', '', normalized_section)
        
        # Use normalized section as key (not page number) to avoid duplicates
        citation_key = normalized_section.lower()
        
        # Skip if we've already seen this section (even with different page numbers)
        if citation_key and citation_key not in seen_citations:
            seen_citations.add(citation_key)
            # Store the first page number we encounter for this section
            citations.append(
                {
                    "section_path": normalized_section,  # Use normalized version
                    "page_label": page_label if page_label else "",
                    "source_url": str(meta.get("source_url") or ""),
                }
            )
    
    # Limit to top 6 unique citations to avoid clutter
    citations = citations[:6]
    
    hazard_output = _dedupe(summary_hazards + scenario_summary)[:10]
    if not hazard_output and additional_hazards:
        hazard_output = _dedupe(additional_hazards)[:5]

    status = CategoryStatus.required if hazard_output else CategoryStatus.pending
    pending_reason = "" if hazard_output else "No hazard sentences surfaced from EM 385 retrieval. Additional project context may be required."
    return AhaEvidence(
        hazards=hazard_output,
        narrative=_dedupe(narrative),
        citations=citations,
        status=status,
        pending_reason=pending_reason,
    )


def _split_controls(sentences: Iterable[str]) -> Tuple[List[str], List[str], List[str]]:
    controls: List[str] = []
    ppe: List[str] = []
    permits: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if "ppe" in lower or any(token in lower for token in ["glove", "protection", "respirator", "hard hat", "eye", "face shield"]):
            ppe.append(sentence)
        elif "permit" in lower or "training" in lower or "qualified" in lower:
            permits.append(sentence)
        else:
            controls.append(sentence)
    return controls, ppe, permits


def build_safety_plan_evidence(
    category: str,
    context: RetrievalContext,
    scope: List[str],
    codes: List[str],
) -> SafetyPlanEvidence:
    """Build detailed Safety Plan evidence using 385 RAG context."""
    # Ensure we have actual retrieved documents, not empty templates
    if not context.documents or not any(doc.strip() for doc in context.documents):
        return SafetyPlanEvidence(
            controls=[],
            ppe=[],
            permits=[],
            citations=[],
            project_evidence=[],
            em_evidence=[],
            status=CategoryStatus.insufficient,
            pending_reason=f"No EM 385 content retrieved for {category}. Please ensure the RAG collection is properly indexed with EM 385 documents.",
        )
    
    sentences = _extract_sentences(context.documents)
    if not sentences:
        return SafetyPlanEvidence(
            controls=[],
            ppe=[],
            permits=[],
            citations=[],
            project_evidence=[],
            em_evidence=[],
            status=CategoryStatus.insufficient,
            pending_reason=f"Could not extract meaningful content from retrieved documents for {category}. Retrieved documents may be empty or improperly formatted.",
        )
    
    control_sentences = _control_sentences(sentences)
    controls, ppe, permits = _split_controls(control_sentences)
    
    # Build comprehensive project evidence
    project_evidence = []
    if scope:
        project_evidence.append(f"This Safety Plan addresses {category} activities as specified in the project scope.")
        for scope_item in scope[:5]:
            if scope_item.strip():
                project_evidence.append(f"Scope reference: {scope_item.strip()}")
    if codes:
        project_evidence.append(f"Applicable EM 385 codes: {', '.join(codes[:5])}")
    
    # Build detailed EM 385 evidence from ACTUAL retrieved content
    em_evidence = _dedupe([s.strip() for s in control_sentences[:10] if s.strip()])  # Include more evidence, filter empty
    
    # Enhance controls with more detailed procedures from ACTUAL RAG content
    detailed_controls = [c.strip() for c in controls[:12] if c.strip()]
    # Add procedure-related sentences from retrieved documents
    procedure_sentences = [s.strip() for s in sentences if any(kw in s.lower() for kw in ["procedure", "method", "process", "step", "sequence"]) and s.strip()][:4]
    detailed_controls.extend([s for s in procedure_sentences if s and s not in detailed_controls])
    
    # Enhance PPE with more comprehensive requirements from ACTUAL RAG content
    detailed_ppe = [p.strip() for p in ppe[:10] if p.strip()]
    ppe_sentences = [s.strip() for s in sentences if any(kw in s.lower() for kw in ["protective equipment", "ppe", "equipment", "clothing", "gear", "hard hat", "safety glasses", "gloves", "respirator"]) and s.strip()][:4]
    detailed_ppe.extend([s for s in ppe_sentences if s and s not in detailed_ppe])
    
    # Enhance permits with training and qualification requirements from ACTUAL RAG content
    detailed_permits = [p.strip() for p in permits[:10] if p.strip()]
    training_sentences = [s.strip() for s in sentences if any(kw in s.lower() for kw in ["training", "qualification", "certification", "competent", "qualified person", "permit"]) and s.strip()][:4]
    detailed_permits.extend([s for s in training_sentences if s and s not in detailed_permits])
    
    status = CategoryStatus.required
    pending_reason = ""
    if len(project_evidence) < 2 or len(em_evidence) < 2:
        status = CategoryStatus.insufficient
        pending_reason = "Safety Plan evidence quota not met (needs ≥2 project and ≥2 EM citations). Additional documentation may be required."
    
    citations: List[Dict[str, str]] = []
    seen_citations = set()
    # Normalize citations by section only (not page number) to avoid duplicates
    for meta in context.metadatas[:15]:  # Check more for better citation coverage
        meta = meta or {}
        section_path = str(meta.get("section_path") or meta.get("headers") or "").strip()
        page_label = str(meta.get("page_label") or meta.get("page_number") or "").strip()
        
        # Normalize section path - extract meaningful section identifier
        if not section_path or section_path.lower() in ["none", "null", ""]:
            continue
        
        # Use section path as primary key (normalize it)
        normalized_section = re.sub(r'\s+', ' ', section_path).strip()
        # Remove page-specific references from section path
        normalized_section = re.sub(r'\s*\(p\.\s*\d+\)', '', normalized_section, flags=re.IGNORECASE)
        normalized_section = re.sub(r'\s*page\s+\d+', '', normalized_section, flags=re.IGNORECASE)
        # Remove any trailing page numbers
        normalized_section = re.sub(r'\s+\d+$', '', normalized_section)
        
        # Use normalized section as key (not page number) to avoid duplicates
        citation_key = normalized_section.lower()
        
        # Skip if we've already seen this section (even with different page numbers)
        if citation_key and citation_key not in seen_citations:
            seen_citations.add(citation_key)
            # Store the first page number we encounter for this section
            citations.append(
                {
                    "section_path": normalized_section,  # Use normalized version
                    "page_label": page_label if page_label else "",
                    "source_url": str(meta.get("source_url") or ""),
                }
            )
    
    # Limit to top 6 unique citations to avoid clutter
    citations = citations[:6]
    
    return SafetyPlanEvidence(
        controls=_dedupe(detailed_controls[:15]),  # More comprehensive controls
        ppe=_dedupe(detailed_ppe[:12]),  # More comprehensive PPE
        permits=_dedupe(detailed_permits[:12]),  # More comprehensive permits/training
        citations=citations[:8],  # More citations
        project_evidence=project_evidence,
        em_evidence=em_evidence[:8],  # More EM evidence
        status=status,
        pending_reason=pending_reason,
    )


def group_codes_by_category(codes: Iterable[ParsedCode]) -> Dict[str, List[str]]:
    """Group codes by category. Include all codes that require AHA or have unknown status."""
    grouped: Dict[str, List[str]] = defaultdict(list)
    for parsed in codes:
        # Include codes that require AHA OR have unknown status (None)
        # Only exclude codes that explicitly don't require AHA (False)
        if parsed.requires_aha is False:
            continue
        # Assign category - use suggested or default to Unmapped
        category = parsed.suggested_category or "Unmapped"
        if not category.strip():
            category = "Unmapped"
        grouped[category].append(parsed.code)
    return grouped


def build_category_bundles(
    codes: Iterable[ParsedCode],
    scope: List[str],
    collection_name: str | None,
) -> List[CategoryBundle]:
    """Build category bundles with AHA and Safety Plans using 385 RAG. Runs synchronously."""
    # Convert to list for debugging
    codes_list = list(codes)
    print(f"[build_category_bundles] Input: {len(codes_list)} codes")
    for code in codes_list:
        print(f"[build_category_bundles]   Code: {code.code}, requires_aha={code.requires_aha}, category={code.suggested_category or 'None'}")
    
    grouped = group_codes_by_category(codes_list)
    bundles: List[CategoryBundle] = []
    
    if not collection_name:
        print(f"[build_category_bundles] ERROR: No collection_name provided! RAG queries will fail.")
        return bundles
    
    print(f"[build_category_bundles] Grouped into {len(grouped)} categories: {list(grouped.keys())}")
    for cat, codes_in_cat in grouped.items():
        print(f"[build_category_bundles]   Category '{cat}': {len(codes_in_cat)} codes - {codes_in_cat}")
    
    if len(grouped) == 0:
        print(f"[build_category_bundles] ERROR: No categories created! Check if codes have requires_aha=True and categories assigned.")
        return bundles
    
    categories = sorted(grouped.keys(), key=lambda value: (value != "Unmapped", value))
    for idx, category in enumerate(categories):
        code_list = grouped[category]
        print(f"[build_category_bundles] Processing category {idx+1}/{len(categories)}: {category} with {len(code_list)} codes: {code_list}")
        
        # Query 385 RAG for this category
        print(f"[build_category_bundles] Querying 385 RAG for {category}...")
        context = retrieve_context(category, scope, code_list, collection_name)
        
        if context.documents:
            print(f"[build_category_bundles] Retrieved {len(context.documents)} documents for {category}")
            print(f"[build_category_bundles] First document preview: {context.documents[0][:200] if context.documents else 'N/A'}...")
        else:
            print(f"[build_category_bundles] WARNING: No documents retrieved for {category}! Collection may be empty or query failed.")
        
        # Generate AHA
        print(f"[build_category_bundles] Generating AHA for {category}...")
        aha = build_aha_evidence(category, context, scope)
        print(f"[build_category_bundles] AHA generated: {len(aha.hazards)} hazards, {len(aha.narrative)} narrative lines, status={aha.status.value}")
        
        # Generate Safety Plan
        print(f"[build_category_bundles] Generating Safety Plan for {category}...")
        plan = build_safety_plan_evidence(category, context, scope, code_list)
        print(f"[build_category_bundles] Plan generated: {len(plan.controls)} controls, {len(plan.ppe)} PPE items, status={plan.status.value}")
        
        bundle = CategoryBundle(category=category, codes=code_list, aha=aha, plan=plan)
        bundles.append(bundle)
        
        print(f"[build_category_bundles] Completed {category}: AHA status={aha.status.value}, Plan status={plan.status.value}")
    
    print(f"[build_category_bundles] Completed all {len(bundles)} categories")
    return bundles


def ensure_categories(bundles: List[CategoryBundle]) -> List[CategoryBundle]:
    categories = {bundle.category for bundle in bundles}
    for required in EM_385_CATEGORIES:
        if required not in categories:
            bundles.append(CategoryBundle(category=required, codes=[]))
    return bundles

