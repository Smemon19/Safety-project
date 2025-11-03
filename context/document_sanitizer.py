from __future__ import annotations

"""Document sanitization to remove TOC, headers/footers, page chrome, and boilerplate."""

import re
from typing import List, Set
from pathlib import Path


# Common TOC patterns
TOC_PATTERNS = [
    r'(?i)^\s*(table\s+of\s+contents|contents|toc|index)\s*$',
    r'(?i)^\s*page\s+\d+\s*$',
    r'(?i)^\s*(section|chapter|part)\s+\d+[\.\s]',
]

# Header/footer patterns
HEADER_FOOTER_PATTERNS = [
    r'(?i)^\s*(project\s+name|document\s+title|company\s+name).*\|\s*page\s+\d+',
    r'(?i)^\s*page\s+\d+\s+of\s+\d+\s*$',
    r'(?i)^\s*\d+\s*$',  # Standalone page numbers
    r'(?i)^\s*(confidential|proprietary|draft|final)\s*$',
]

# Procurement boilerplate patterns
PROCUREMENT_BOILERPLATE = [
    r'(?i)(?:this\s+document|the\s+following|pursuant\s+to)\s+is\s+subject\s+to',
    r'(?i)terms\s+and\s+conditions\s+of\s+contract',
    r'(?i)no\s+part\s+of\s+this\s+document',
    r'(?i)all\s+rights\s+reserved',
    r'(?i)copyright\s+\d{4}',
    r'(?i)proprietary\s+and\s+confidential',
    r'(?i)rfp|request\s+for\s+proposal',
    r'(?i)bid\s+package|contract\s+documents',
    r'(?i)disclaimer|warranty',
]

# Page chrome patterns (repeated elements)
PAGE_CHROME_PATTERNS = [
    r'(?i)^\s*date[:\s]+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s*$',
    r'(?i)^\s*revision[:\s]+[\w\d]+\s*$',
    r'(?i)^\s*file[:\s]+path.*\s*$',
]


def _is_toc_line(line: str) -> bool:
    """Check if a line appears to be from a table of contents."""
    line_stripped = line.strip()
    if not line_stripped or len(line_stripped) < 2:
        return False
    
    # Check for TOC patterns
    for pattern in TOC_PATTERNS:
        if re.match(pattern, line_stripped):
            return True
    
    # Check for TOC-style formatting (dots/leaders with page numbers)
    if re.match(r'^[\s\w\.]+\s+\.{3,}\s+\d+\s*$', line_stripped):
        return True
    
    # Check for section number patterns common in TOCs
    if re.match(r'^\d+[\.\s]+\d*\s+[A-Z][a-z]+.*\.{2,}\s*\d+', line_stripped):
        return True
    
    return False


def _is_header_footer(line: str) -> bool:
    """Check if a line is a header or footer."""
    line_stripped = line.strip()
    if not line_stripped:
        return False
    
    for pattern in HEADER_FOOTER_PATTERNS:
        if re.match(pattern, line_stripped):
            return True
    
    return False


def _contains_boilerplate(text: str) -> bool:
    """Check if text contains procurement boilerplate."""
    text_lower = text.lower()
    for pattern in PROCUREMENT_BOILERPLATE:
        if re.search(pattern, text_lower):
            return True
    return False


def _is_page_chrome(line: str) -> bool:
    """Check if a line is page chrome (repeated metadata)."""
    line_stripped = line.strip()
    if not line_stripped:
        return False
    
    for pattern in PAGE_CHROME_PATTERNS:
        if re.match(pattern, line_stripped):
            return True
    
    return False


def sanitize_document_text(text: str) -> str:
    """Remove TOC blocks, headers/footers, page chrome, and procurement boilerplate.
    
    Args:
        text: Raw document text
        
    Returns:
        Sanitized text with boilerplate removed
    """
    if not text:
        return ""
    
    lines = text.splitlines()
    sanitized_lines: List[str] = []
    in_toc_block = False
    toc_block_start = 0
    consecutive_toc_lines = 0
    
    # Track seen lines to remove duplicates (common in headers/footers)
    seen_lines: Set[str] = set()
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Skip empty lines at start/end of blocks
        if not line_stripped and i > 0 and i < len(lines) - 1:
            # Check if next/prev lines suggest we're in boilerplate
            if i < len(lines) - 1 and _is_toc_line(lines[i + 1].strip()):
                continue
        
        # Detect TOC blocks (consecutive TOC lines)
        if _is_toc_line(line_stripped):
            if not in_toc_block:
                in_toc_block = True
                toc_block_start = i
                consecutive_toc_lines = 1
            else:
                consecutive_toc_lines += 1
            continue
        else:
            if in_toc_block:
                # End of TOC block - check if it was substantial (5+ lines)
                if consecutive_toc_lines >= 5:
                    # Remove the entire TOC block
                    sanitized_lines = sanitized_lines[:toc_block_start - len(sanitized_lines)]
                in_toc_block = False
                consecutive_toc_lines = 0
        
        # Skip headers/footers
        if _is_header_footer(line_stripped):
            continue
        
        # Skip page chrome
        if _is_page_chrome(line_stripped):
            continue
        
        # Skip exact duplicates (common in headers/footers)
        line_normalized = re.sub(r'\s+', ' ', line_stripped.lower())
        if line_normalized in seen_lines and len(line_normalized) < 50:
            continue
        seen_lines.add(line_normalized)
        
        # Remove boilerplate paragraphs (if entire paragraph is boilerplate)
        # Check this after we've collected a potential paragraph
        if line_stripped:
            # If this line and next few contain boilerplate, skip paragraph
            next_lines = ' '.join(lines[i:min(i+5, len(lines))])
            if _contains_boilerplate(next_lines) and len(next_lines) < 200:
                # Skip this paragraph
                j = i + 1
                while j < len(lines) and lines[j].strip():
                    j += 1
                # Skip to end of paragraph
                continue
        
        sanitized_lines.append(line)
    
    # Join and clean up
    result = '\n'.join(sanitized_lines)
    
    # Remove excessive blank lines
    result = re.sub(r'\n{3,}', '\n\n', result)
    
    # Remove boilerplate sections (large blocks)
    paragraphs = result.split('\n\n')
    clean_paragraphs = []
    for para in paragraphs:
        para_stripped = para.strip()
        if len(para_stripped) < 10:
            continue
        # Skip paragraphs that are mostly boilerplate
        if _contains_boilerplate(para_stripped) and len(para_stripped) < 300:
            continue
        clean_paragraphs.append(para)
    
    result = '\n\n'.join(clean_paragraphs)
    
    return result.strip()


def tag_chunk_for_exclusion(text: str) -> bool:
    """Determine if a chunk should be excluded from retrieval.
    
    Returns True if chunk should be excluded (TOC/boilerplate).
    """
    if not text or len(text.strip()) < 20:
        return True
    
    # Check if entire chunk is TOC-like
    lines = text.splitlines()[:5]  # Check first 5 lines
    toc_count = sum(1 for line in lines if _is_toc_line(line.strip()))
    if toc_count >= 3:
        return True
    
    # Check if chunk is mostly boilerplate
    if _contains_boilerplate(text) and len(text) < 500:
        return True
    
    return False


__all__ = ["sanitize_document_text", "tag_chunk_for_exclusion"]

