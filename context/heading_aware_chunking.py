from __future__ import annotations

"""Heading-aware chunking that preserves section metadata."""

import re
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class ChunkWithMetadata:
    """A text chunk with section metadata."""
    text: str
    section_title: str = ""
    section_level: int = 0
    headings: List[str] = field(default_factory=list)
    page_number: Optional[int] = None
    chunk_id: str = ""
    page_range: Tuple[Optional[int], Optional[int]] = (None, None)
    division: str = ""


def _derive_division(heading: str, heading_stack: List[str]) -> str:
    candidates = [heading] + list(reversed(heading_stack))
    for candidate in candidates:
        if not candidate:
            continue
        match = re.search(r"(division\s+\d+)", candidate, re.IGNORECASE)
        if match:
            return match.group(1).title()
        match = re.search(r"(section\s+\d+)", candidate, re.IGNORECASE)
        if match:
            return match.group(1).title()
    return ""


def chunk_by_headings(
    text: str,
    target_tokens: int = 900,
    overlap_tokens: int = 100,
    min_tokens: int = 200,
    *,
    max_chunk_size: Optional[int] = None,
    overlap: Optional[int] = None,
    min_chunk_size: Optional[int] = None,
) -> List[ChunkWithMetadata]:
    """Chunk text by headings with section metadata preservation."""

    if not text:
        return []

    if max_chunk_size:
        target_tokens = max(200, max_chunk_size // 4)
    if overlap is not None:
        overlap_tokens = max(0, overlap // 4)
    if min_chunk_size is not None:
        min_tokens = max(50, min_chunk_size // 4)

    heading_patterns = [
        r'^(section\s+\d+(?:\.\d+)*)\s*[:\-–—]?\s*(.+)$',
        r'^(section\s+\d{2}(?:\s+\d{2}){1,3})\s*[:\-–—]?\s*(.+)$',
        r'^(division\s+\d+(?:\.\d+)*)\s*[:\-–—]?\s*(.+)$',
        r'^(part\s+[ivx]+)\s*[:\-–—]?\s*(.+)$',
        r'^(part\s+\d+)\s*[:\-–—]?\s*(.+)$',
        r'^(\d+[A-Z]?\.\d+(?:\.\d+)*)\s+(.+)$',
        r'^((?:\d{2}\s+){1,4}\d{2})\s+(.+)$',
    ]

    lines = text.splitlines()
    chunks: List[ChunkWithMetadata] = []
    current_section_title = "Front Matter"
    current_section_level = 0
    current_heading_stack: List[Tuple[str, int]] = []
    current_segments: List[Tuple[str, Optional[int], int]] = []
    current_tokens = 0
    current_page: Optional[int] = None

    def looks_like_freeform_heading(line: str) -> bool:
        if not line:
            return False
        if len(line) < 6 or len(line) > 120:
            return False
        if line.endswith('.'):
            return False
        words = line.split()
        if len(words) < 2:
            return False
        letters = sum(1 for ch in line if ch.isalpha())
        if letters == 0:
            return False
        uppercase = sum(1 for ch in line if ch.isupper())
        if uppercase / max(letters, 1) < 0.6:
            return False
        return True

    def add_segment(segment_text: str, page: Optional[int]) -> None:
        nonlocal current_tokens
        tokens = len(segment_text.split())
        if tokens == 0:
            return
        current_segments.append((segment_text, page, tokens))
        current_tokens += tokens

    def flush_chunk(force: bool = False) -> None:
        nonlocal current_segments, current_tokens
        if not current_segments:
            return
        if not force and current_tokens < min_tokens:
            return

        chunk_text = "\n".join(segment for segment, _, _ in current_segments).strip()
        if not chunk_text:
            current_segments = []
            current_tokens = 0
            return

        pages = [page for _, page, _ in current_segments if page is not None]
        page_start = min(pages) if pages else None
        page_end = max(pages) if pages else None

        headings = [title for title, _ in current_heading_stack]
        section_title = current_section_title or (current_heading_stack[-1][0] if current_heading_stack else "Front Matter")
        chunk = ChunkWithMetadata(
            text=chunk_text,
            section_title=section_title,
            section_level=current_section_level,
            headings=headings,
            page_number=page_start,
            page_range=(page_start, page_end),
            division=_derive_division(section_title, headings),
        )
        chunks.append(chunk)

        if overlap_tokens > 0 and current_tokens > 0:
            overlap_segments: List[Tuple[str, Optional[int], int]] = []
            tally = 0
            for segment, page, tokens in reversed(current_segments):
                if tally >= overlap_tokens:
                    break
                needed = overlap_tokens - tally
                if tokens > needed:
                    words = segment.split()
                    trimmed = " ".join(words[-needed:])
                    overlap_segments.insert(0, (trimmed, page, needed))
                    tally += needed
                    break
                overlap_segments.insert(0, (segment, page, tokens))
                tally += tokens
            current_segments = overlap_segments
            current_tokens = sum(seg[2] for seg in current_segments)
        else:
            current_segments = []
            current_tokens = 0

    def heading_level_from_pattern(pattern: str, match: re.Match[str]) -> int:
        if pattern.startswith('^(section') or pattern.startswith('^(division') or pattern.startswith('^(part'):
            return 1
        if pattern.startswith(r'^(\d+'):
            return match.group(1).count('.') + 1
        if pattern.startswith(r'^(\d+[A-Z]?\.'):
            return match.group(1).count('.') + 1
        if pattern.startswith('^((?:'):
            return 1
        return 1

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith('[[PAGE_BREAK_'):
            try:
                current_page = int(re.findall(r"\d+", line)[0])
            except Exception:
                current_page = current_page
            continue

        is_heading = False
        heading_text = ""
        heading_level = 0
        for pattern in heading_patterns:
            match = re.match(pattern, line, flags=re.IGNORECASE)
            if match:
                heading_text = match.group(1).strip()
                remainder = match.group(2).strip() if len(match.groups()) > 1 else ""
                if remainder:
                    heading_text = f"{heading_text} {remainder}".strip()
                heading_level = heading_level_from_pattern(pattern, match)
                is_heading = True
                break

        if not is_heading and looks_like_freeform_heading(line):
            heading_text = line.strip()
            heading_level = 1
            is_heading = True

        if is_heading and heading_text:
            flush_chunk(force=True)
            while current_heading_stack and current_heading_stack[-1][1] >= heading_level:
                current_heading_stack.pop()
            current_heading_stack.append((heading_text, heading_level))
            current_section_title = heading_text
            current_section_level = heading_level
            add_segment(raw_line, current_page)
            continue

        if current_tokens + len(line.split()) > target_tokens and current_tokens >= min_tokens:
            flush_chunk(force=True)

        add_segment(raw_line, current_page)

    flush_chunk(force=True)

    return chunks


def create_chunk_metadata(chunk: ChunkWithMetadata, doc_id: str, chunk_idx: int) -> Dict[str, Any]:
    """Create ChromaDB metadata for a chunk."""
    import hashlib
    
    chunk_id = f"{doc_id}_chunk_{chunk_idx:05d}"
    chunk_hash = hashlib.sha256(chunk.text.encode()).hexdigest()[:8]
    
    page_start, page_end = chunk.page_range
    page_range = None
    if page_start is not None and page_end is not None:
        page_range = f"{page_start}-{page_end}" if page_start != page_end else str(page_start)
    elif page_start is not None:
        page_range = str(page_start)

    metadata: Dict[str, Any] = {
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "section_title": chunk.section_title,
        "section_level": chunk.section_level,
        "headings": " | ".join(chunk.headings) if chunk.headings else "",
        "chunk_hash": chunk_hash,
        "source_type": "project_document",
        "division": chunk.division,
        "page_range": page_range,
        "token_count": len(chunk.text.split()),
    }
    if page_start is not None:
        metadata["page_number"] = page_start
    return metadata


__all__ = ["chunk_by_headings", "ChunkWithMetadata", "create_chunk_metadata"]

