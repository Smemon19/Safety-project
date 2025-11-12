"""Document parsing utilities for the Section 11 Generator."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, List

from pdf_loader.pdf_text import extract_text

from section11.constants import (
    DEFAULT_CATEGORY_BY_KEYWORD,
    HAZARD_KEYWORDS,
    SCOPE_LINE_KEYWORDS,
)
from section11.models import ParsedCode, ParsedSpec, SpecSourceHit


# EM 385 codes are typically like: 385-11-1, 385-1.1, 385-11, etc.
# Pattern: 385 followed by dash/space and digits, optionally with sub-sections (dashes, dots, or spaces)
CODE_RE = re.compile(r"\b385[-\s]+(\d+(?:[-\s\.]\d+)*)\b")
RANGE_RE = re.compile(r"\b(\d{3,4})\s*[–-]\s*(\d{3,4})\b")
UFGS_LINE_START_RE = re.compile(r"^(?:SECTION\s+)?(\d{2})\s+(\d{2})\s+(\d{2})(?:\b|\.|\s)")


def _expand_ranges(line: str) -> List[str]:
    tokens: List[str] = []
    if "385" not in line:
        return tokens
    for match in RANGE_RE.finditer(line):
        a = int(match.group(1))
        b = int(match.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if lo < 1 or hi > 9999 or (hi - lo) > 200:
            continue
        for value in range(lo, hi + 1):
            tokens.append(f"385-{value}")
    return tokens


def _extract_codes_with_sources(text: str) -> List[ParsedCode]:
    codes: dict[str, ParsedCode] = {}
    for idx, raw_line in enumerate((text or "").splitlines()):
        if "385" not in raw_line:
            continue
        line = raw_line.strip()
        for match in CODE_RE.finditer(line):
            # Normalize the code format: 385-11-1 or 385-1.1 or 385-11
            code_part = match.group(1).strip()
            # Replace spaces with dashes, keep existing dashes and dots
            code_part = re.sub(r'\s+', '-', code_part)
            code_token = f"385-{code_part}"
            codes.setdefault(code_token, ParsedCode(code=code_token))
            snippet = line[:240]
            codes[code_token].sources.append(
                SpecSourceHit(page=None, heading="", excerpt=snippet)
            )
        for token in _expand_ranges(line):
            codes.setdefault(token, ParsedCode(code=token))
            codes[token].sources.append(SpecSourceHit(page=None, heading="", excerpt=line[:240]))
    return list(codes.values())


def _extract_ufgs_codes(text: str) -> Iterable[str]:
    """Extract UFGS codes from text. Handles UFGS-XX-XX-XX, UFGS-XX-XX-XX-XX, and UFGS-XX-XX-XX-XX-XX formats.
    
    Returns codes in their original format (preserves 4-part and 5-part codes).
    IMPORTANT: Extracts codes in order of specificity (longest first) to avoid partial matches.
    """
    seen: set[str] = set()
    all_codes = []
    
    # Extract in order of specificity (longest first) to avoid partial matches
    # Pattern 1: UFGS-XX-XX-XX-XX-XX (5 parts, e.g., UFGS-01-32-01-00-10)
    ufgs_5part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_5part.finditer(text):
        token = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}-{match.group(4)}-{match.group(5)}"
        if token not in seen:
            seen.add(token)
            all_codes.append(token)
    
    # Pattern 2: UFGS-XX-XX-XX-XX (4 parts, e.g., UFGS-01-33-23-33)
    ufgs_4part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_4part.finditer(text):
        token = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}-{match.group(4)}"
        # Check if this is a substring of an already-found 5-part code
        is_substring = any(token in code for code in all_codes)
        if not is_substring and token not in seen:
            seen.add(token)
            all_codes.append(token)
    
    # Pattern 3: UFGS-XX-XX-XX (3 parts, e.g., UFGS-01-11-00)
    ufgs_3part = re.compile(r'\bUFGS-(\d{2})-(\d{2})-(\d{2})\b', re.IGNORECASE)
    for match in ufgs_3part.finditer(text):
        token = f"UFGS-{match.group(1)}-{match.group(2)}-{match.group(3)}"
        # Check if this is a substring of an already-found longer code
        is_substring = any(token in code for code in all_codes)
        if not is_substring and token not in seen:
            seen.add(token)
            all_codes.append(token)
    
    # Pattern 4: XX XX XX format at line start (original pattern)
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        alpha = sum(1 for char in line if char.isalpha())
        if alpha < 6:
            continue
        match = UFGS_LINE_START_RE.search(line)
        if not match:
            continue
        a, b, c = match.group(1), match.group(2), match.group(3)
        token = f"UFGS-{a}-{b}-{c}"
        # Check if this is a substring of an already-found longer code
        is_substring = any(token in code for code in all_codes)
        if not is_substring and token not in seen:
            seen.add(token)
            all_codes.append(token)
    
    for code in all_codes:
        yield code


def _extract_scope_lines(text: str) -> List[str]:
    """Extract actual scope content from document structure, not generic keywords."""
    # Look for actual section headings and structured content
    # Find sections that likely contain scope information
    lines = text.splitlines()
    scope_sections = []
    
    # Look for common section headers that indicate scope
    scope_indicators = ["scope of work", "scope", "work description", "project description", 
                       "work includes", "work consists", "project scope", "statement of work"]
    
    found_section = False
    collecting = False
    current_section = []
    
    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        
        # Check if this line is a section header
        if any(indicator in line_lower for indicator in scope_indicators):
            # Save previous section if we were collecting
            if collecting and current_section:
                scope_sections.extend(current_section[:10])  # Limit to first 10 lines of section
                current_section = []
            collecting = True
            found_section = True
            continue
        
        # Collect content from scope section
        if collecting:
            cleaned = line.strip()
            if cleaned and len(cleaned) > 10:  # Only meaningful lines
                current_section.append(cleaned)
                # Stop collecting after reasonable section length
                if len(current_section) >= 15:
                    scope_sections.extend(current_section)
                    collecting = False
                    current_section = []
    
    # Add any remaining collected content
    if current_section:
        scope_sections.extend(current_section[:10])
    
    # If no structured scope found, return empty - don't use generic fallback
    if not found_section:
        return []
    
    # Return unique, meaningful scope lines
    seen = set()
    unique_scope = []
    for line in scope_sections:
        line_normalized = line.lower().strip()
        if line_normalized and line_normalized not in seen and len(line) > 15:
            seen.add(line_normalized)
            unique_scope.append(line)
            if len(unique_scope) >= 10:
                break
    
    return unique_scope


def _extract_hazard_phrases(text: str) -> List[str]:
    """REMOVED: No longer extracting generic hazard phrases from keywords.
    Hazards should be identified from actual document content and codes, not generic keyword matching.
    """
    # Return empty - we don't want generic hazard phrase extraction
    # Hazards will be determined from actual code requirements and document analysis
    return []


def _suggest_category_from_context(code: str, snippet: str) -> str:
    haystack = f"{code} {snippet}".lower()
    for needle, category in DEFAULT_CATEGORY_BY_KEYWORD.items():
        if needle in haystack:
            return category
    return ""


def parse_document_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        pages = extract_text(path, include_tables=True)
        return "\n\n".join(pages[k] for k in sorted(pages.keys())) if pages else ""
    if suffix == ".docx":
        from docx import Document  # type: ignore

        document = Document(str(path))
        parts: List[str] = []
        for paragraph in document.paragraphs:
            text = paragraph.text.strip()
            if text:
                parts.append(text)
        for table in getattr(document, "tables", []):
            for row in table.rows:
                for cell in row.cells:
                    cell_text = cell.text.strip()
                    if cell_text:
                        parts.append(cell_text)
        return "\n".join(parts)
    return path.read_text(encoding="utf-8", errors="ignore")


def parse_spec(path: Path, work_dir: Path) -> ParsedSpec:
    """Parse document and extract actual content - no templates or generic content."""
    work_dir.mkdir(parents=True, exist_ok=True)
    text = parse_document_text(path)
    
    # Save full document text for context (not summaries)
    raw_dump = work_dir / f"{path.stem}.text.json"
    raw_dump.write_text(json.dumps({"text": text, "length": len(text)}, indent=2), encoding="utf-8")

    # IMPORTANT: Only extract UFGS codes from user documents
    # DO NOT extract EM 385 codes - they only exist in the RAG system
    codes = []
    ufgs = list(_extract_ufgs_codes(text))
    for token in ufgs:
        codes.append(ParsedCode(code=token))

    # Categorize codes based on actual document context
    for parsed in codes:
        if parsed.sources:
            # Use actual document excerpt, not generic keywords
            parsed.suggested_category = _suggest_category_from_context(parsed.code, parsed.sources[0].excerpt)
        else:
            parsed.suggested_category = _suggest_category_from_context(parsed.code, parsed.code)

    # Extract actual scope from document structure (not generic fallbacks)
    scope_lines = _extract_scope_lines(text)
    
    # No generic hazard phrase extraction - hazards come from codes and RAG analysis
    hazard_phrases = []

    return ParsedSpec(
        scope_summary=scope_lines,  # Only actual scope found in document, not generic lines
        codes=codes,
        hazard_phrases=hazard_phrases,  # Always empty - no generic extraction
        raw_text_path=raw_dump,
    )

