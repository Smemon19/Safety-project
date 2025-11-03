from __future__ import annotations

"""Document sanitisation helpers used by the CSP ingestion pipeline.

The original repository snapshot was missing the required imports which meant
this module could not be imported at all.  The test-suite exercises the
functions exposed here so we provide a concise description of the utilities and
ensure the imports are present at the top of the file.
"""

import re
from collections import Counter
from typing import List, Set


# Common phrases/patterns to remove completely
LINE_BLACKLIST = [
    "for receipt by the contracting officer",
    "prepared for submission to the u.s. army corps of engineers",
    "update the table of contents",
    "quality control approval. submit the following",
    "add absorbent material to absorb residue oil remaining after draining",
    "subject to terms and conditions",
    "all rights reserved",
    "copyright",
]

# Scope-guarded contamination phrases (drop sentences unless scope requires)
SCOPE_GUARDED_SENTENCES = [
    "asbestos waste",
    "contaminated wastewater filters",
]

TOC_TRIGGER = re.compile(r"(?i)table\s+of\s+contents")
TOC_PATTERNS = [
    r"(?i)^\s*(table\s+of\s+contents|contents|toc|index)\s*$",
    r"(?i)^\s*page\s+\d+\s*$",
    r"(?i)^\s*(section|chapter|part)\s+\d+[\.\s]",
]

CHUNK_BOILERPLATE_PATTERNS = [
    r"(?i)quality\s+control\s+approval\.\s+submit\s+the\s+following",
    r"(?i)prepared\s+for\s+submission\s+to\s+the\s+u\.s\.\s+army\s+corps",
    r"(?i)for\s+receipt\s+by\s+the\s+contracting\s+officer",
    r"(?i)add\s+absorbent\s+material\s+to\s+absorb\s+residue\s+oil",
    r"(?i)update\s+the\s+table\s+of\s+contents",
]
STRONG_HEADING = re.compile(
    r"^(?:section|division|part)\s+\d+|summary\s+of\s+work|\b\d{2}\s{1,2}\d{2}\s{1,2}\d{2}\b",
    re.IGNORECASE | re.MULTILINE,
)


def _split_pages(text: str) -> List[str]:
    """Split text into logical pages using form-feed or heuristic breaks."""

    if "\f" in text:
        pages = re.split(r"\f+", text)
    else:
        # Heuristic: treat two or more consecutive blank lines with at least 60 chars of preceding
        pages = re.split(r"\n{2,}(?=\s*(?:page\s+\d+|section\s+\d+|division\s+\d+))", text, flags=re.IGNORECASE)

    return [page for page in pages if page]


def _normalize_line(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip()).lower()


def _contains_strong_heading(page: str) -> bool:
    for line in (page or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _is_toc_line(stripped):
            continue
        if STRONG_HEADING.search(stripped):
            return True
    return False


def _drop_toc_pages(pages: List[str]) -> List[str]:
    cleaned: List[str] = []
    i = 0
    while i < len(pages):
        page = pages[i]
        lines = page.splitlines()
        toc_lines = sum(1 for line in lines if _is_toc_line(line.strip()))
        if TOC_TRIGGER.search(page) or (lines and toc_lines >= max(2, len(lines) // 2 + 1)):
            has_real_content = any(
                line.strip() and not _is_toc_line(line.strip()) and len(line.strip().split()) >= 3
                for line in page.splitlines()
            )
            if _contains_strong_heading(page) or has_real_content:
                cleaned.append(page)
                i += 1
                continue

            # Drop this page and optionally the next if it is also TOC-like
            skip_next = False
            if i + 1 < len(pages) and not _contains_strong_heading(pages[i + 1]):
                skip_next = True
            i += 1
            if skip_next:
                i += 1
            continue
        cleaned.append(page)
        i += 1
    return cleaned


def _compute_repeated_lines(pages: List[str], line_count: int = 4) -> Set[str]:
    """Return normalized lines that appear in top/bottom blocks on ≥60% of pages."""

    if not pages:
        return set()

    if len(pages) == 1:
        return set()

    top_counter: Counter[str] = Counter()
    bottom_counter: Counter[str] = Counter()

    for page in pages:
        lines = [ln for ln in page.splitlines() if ln.strip()]
        if not lines:
            continue
        top_block = lines[:line_count]
        bottom_block = lines[-line_count:]
        for ln in top_block:
            top_counter[_normalize_line(ln)] += 1
        for ln in bottom_block:
            bottom_counter[_normalize_line(ln)] += 1

    threshold = max(1, int(len(pages) * 0.6))
    repeated = {line for line, count in top_counter.items() if count >= threshold}
    repeated |= {line for line, count in bottom_counter.items() if count >= threshold}
    return repeated


def _should_drop_line(line: str, repeated_lines: Set[str]) -> bool:
    normalized = _normalize_line(line)
    if not normalized:
        return False
    if normalized in repeated_lines:
        return True
    if _is_toc_line(line):
        return True
    return any(phrase in normalized for phrase in LINE_BLACKLIST)


def _clean_page(page: str, repeated_lines: Set[str]) -> str:
    lines: List[str] = []
    seen: Set[str] = set()
    for raw in page.splitlines():
        if _should_drop_line(raw, repeated_lines):
            continue
        normalized = _normalize_line(raw)
        if normalized and normalized in seen and len(normalized) < 60:
            # repeated chrome within same page
            continue
        if normalized:
            seen.add(normalized)
        if raw.strip():
            lines.append(re.sub(r"[ \t]+", " ", raw.rstrip()))
        else:
            lines.append("")
    text = "\n".join(lines)
    # Collapse more than two blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _remove_scope_guarded_sentences(text: str, scope_text: str) -> str:
    scope_lower = scope_text.lower()
    keep_sensitive = "asbestos abatement" in scope_lower
    sentences: List[str] = []
    pattern = re.compile(r"[^.!?]+[.!?]?", re.MULTILINE)
    for match in pattern.finditer(text):
        sentence = match.group(0).strip()
        if not sentence:
            continue
        if sentence.startswith("[[PAGE_BREAK_"):
            sentences.append(sentence)
            continue
        lower = sentence.lower()
        if any(key in lower for key in SCOPE_GUARDED_SENTENCES) and not keep_sensitive:
            continue
        sentences.append(sentence)
    result = " ".join(sentences)
    # Restore paragraph breaks roughly by splitting on double spaces from join
    result = re.sub(r"\s{2,}", "\n\n", result)
    return result.strip()


def _is_toc_line(line: str) -> bool:
    """Check if a line appears to be from a table of contents."""
    line_stripped = line.strip()
    if not line_stripped or len(line_stripped) < 2:
        return False
    
    # Distinguish between actual section headings and TOC entries.  If the line
    # looks like "Section 1" but does not contain dot leaders or a trailing page
    # number we treat it as real content, not TOC.
    if re.match(r"(?i)^\s*(section|chapter|part)\s+\d+\b", line_stripped):
        if not re.search(r"\.{2,}\s*\d+$", line_stripped):
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


def _chunk_has_blacklisted_phrase(text: str) -> bool:
    text_lower = text.lower()
    return any(re.search(pattern, text_lower) for pattern in CHUNK_BOILERPLATE_PATTERNS)


def sanitize_document_text(text: str) -> str:
    """Remove TOC pages, repeated chrome, and procurement boilerplate."""
    if not text:
        return ""

    # Page level operations first
    pages = _split_pages(text)
    pages = _drop_toc_pages(pages)

    repeated_lines = _compute_repeated_lines(pages)

    cleaned_pages = [_clean_page(page, repeated_lines) for page in pages]

    page_blocks: List[str] = []
    for idx, page in enumerate(cleaned_pages, start=1):
        marker = f"[[PAGE_BREAK_{idx}]]"
        page_blocks.append(marker)
        if page:
            page_blocks.append(page)

    combined = "\n".join(page_blocks)

    # Fix broken hyphenation across line breaks
    combined = re.sub(r"(\w+)-\n(\w+)", r"\1\2", combined)
    combined = re.sub(r"\n{3,}", "\n\n", combined)

    # Remove scope guarded contamination sentences
    cleaned = _remove_scope_guarded_sentences(combined, text)
    if not cleaned:
        cleaned = combined

    return cleaned.strip()


def tag_chunk_for_exclusion(text: str) -> bool:
    """Determine if a chunk should be excluded from retrieval.

    Returns True if chunk should be excluded (TOC/boilerplate).
    """
    if not text:
        return True

    cleaned_text = re.sub(r"\[\[PAGE_BREAK_\d+\]\]\s*", "", text)
    if len(cleaned_text.strip()) < 20:
        return True

    # Check if entire chunk is TOC-like
    lines = cleaned_text.splitlines()[:5]  # Check first 5 lines
    toc_count = sum(1 for line in lines if _is_toc_line(line.strip()))
    if toc_count >= 3:
        return True

    # Check if chunk is mostly boilerplate
    if _chunk_has_blacklisted_phrase(cleaned_text) and len(cleaned_text) < 500:
        return True

    return False


__all__ = ["sanitize_document_text", "tag_chunk_for_exclusion"]

