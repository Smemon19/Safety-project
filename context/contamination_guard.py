from __future__ import annotations

"""Contamination guard to remove banned phrases and filler from generated content."""

import re
from typing import List, Tuple, Set
from collections import Counter


# Banned phrases that indicate generic filler or template content
BANNED_PHRASES = [
    r"(?i)\bllm\s+guidance\b",
    r"(?i)\bbest\s+practice\s+recommendations\b",
    r"(?i)\bbased\s+on\s+industry\s+standards\b",
    r"(?i)\bit\s+is\s+important\s+to\s+note\b",
    r"(?i)\bplease\s+refer\s+to\b",
    r"(?i)\bfor\s+more\s+information\b",
    r"(?i)\bthis\s+document\s+outlines\b",
    r"(?i)\bas\s+a\s+general\s+rule\b",
    r"(?i)\btypically\s+involves\b",
    r"(?i)\bit\s+should\s+be\s+noted\b",
    r"(?i)\busers\s+should\b",
    r"(?i)\bit\s+is\s+recommended\s+that\b",
    r"(?i)\bconsult\s+with\s+your\b",
    r"(?i)\bthis\s+section\s+provides\b",
    r"(?i)\baccording\s+to\s+the\s+model\b",
    r"(?i)\bthe\s+model\s+suggests\b",
    r"(?i)\bai\s+generated\b",
    r"(?i)\bartificially\s+generated\b",
    r"(?i)\bplaceholder\s+text\b",
    r"(?i)\binsert\s+here\b",
    r"(?i)\b\[insert\b",
    r"(?i)\btbd\b",
    r"(?i)\bto\s+be\s+determined\b",
    r"(?i)\bgeneric\s+information\b",
    r"(?i)\bexample\s+only\b",
]

# Generic filler patterns
FILLER_PATTERNS = [
    r"(?i)\bthis\s+is\s+an?\s+important\s+consideration\b",
    r"(?i)\bit\s+cannot\s+be\s+overstated\b",
    r"(?i)\bin\s+conclusion\b",
    r"(?i)\bto\s+summarize\b",
    r"(?i)\boverall\b",  # When used generically
    r"(?i)\bgenerally\s+speaking\b",
]


def _calculate_token_overlap(text: str, evidence_texts: List[str], threshold: float = 0.1) -> bool:
    """Check if text has sufficient token overlap with evidence.
    
    Returns True if overlap is sufficient (above threshold).
    """
    if not evidence_texts:
        return False
    
    # Tokenize
    def tokenize(t: str) -> Set[str]:
        tokens = re.findall(r'\b\w+\b', t.lower())
        return set(tokens)
    
    text_tokens = tokenize(text)
    if not text_tokens:
        return False
    
    # Calculate overlap with each evidence text
    max_overlap = 0.0
    for evidence in evidence_texts:
        evidence_tokens = tokenize(evidence)
        if not evidence_tokens:
            continue
        
        # Jaccard similarity
        intersection = len(text_tokens & evidence_tokens)
        union = len(text_tokens | evidence_tokens)
        if union > 0:
            overlap = intersection / union
            max_overlap = max(max_overlap, overlap)
    
    return max_overlap >= threshold


def filter_contaminated_content(
    text: str,
    evidence_texts: List[str] = None,
    min_token_overlap: float = 0.1,
) -> Tuple[str, int]:
    """Remove sentences containing banned phrases or with low token overlap.
    
    Args:
        text: Generated text to filter
        evidence_texts: Evidence texts for overlap checking
        min_token_overlap: Minimum token overlap threshold
        
    Returns:
        Tuple of (cleaned_text, contamination_count)
    """
    if not text:
        return "", 0
    
    evidence_texts = evidence_texts or []
    
    # Split into sentences while retaining punctuation
    sentence_pairs = []
    for match in re.finditer(r'[^.!?]+[.!?]?', text):
        sentence = match.group(0)
        if not sentence:
            continue
        # Separate punctuation for later reassembly
        stripped = sentence.rstrip()
        trailing = sentence[len(stripped):]
        if not trailing and stripped and stripped[-1] in ".!?":
            trailing = stripped[-1]
            stripped = stripped[:-1]
        sentence_pairs.append((stripped, trailing))

    clean_sentences: List[str] = []
    contamination_count = 0

    for sentence, punctuation in sentence_pairs:
        sentence_stripped = sentence.strip()
        if not sentence_stripped:
            continue
        
        # Check for banned phrases
        contains_banned = False
        for pattern in BANNED_PHRASES:
            if re.search(pattern, sentence_stripped):
                contains_banned = True
                break
        
        if contains_banned:
            contamination_count += 1
            continue
        
        # Check for filler patterns
        contains_filler = False
        for pattern in FILLER_PATTERNS:
            if re.search(pattern, sentence_stripped):
                contains_filler = True
                break
        
        if contains_filler:
            contamination_count += 1
            continue
        
        # Check token overlap with evidence (if evidence provided)
        if evidence_texts:
            if not _calculate_token_overlap(sentence_stripped, evidence_texts, min_token_overlap):
                # Low overlap - likely filler
                contamination_count += 1
                continue
        
        clean_sentences.append(sentence + punctuation)
    
    cleaned_text = "".join(clean_sentences).strip()
    
    # If content collapsed (too much removed), mark as insufficient
    original_length = len(text.split())
    cleaned_length = len(cleaned_text.split()) if cleaned_text else 0
    
    if cleaned_length < original_length * 0.3:  # Less than 30% remains
        return "", contamination_count
    
    return cleaned_text, contamination_count


def detect_contamination(text: str) -> List[str]:
    """Detect banned phrases in text without removing them.
    
    Returns:
        List of detected banned phrases
    """
    detected = []
    for pattern in BANNED_PHRASES:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            detected.extend(matches)
    return list(set(detected))


__all__ = [
    "filter_contaminated_content",
    "detect_contamination",
    "BANNED_PHRASES",
    "FILLER_PATTERNS",
]

