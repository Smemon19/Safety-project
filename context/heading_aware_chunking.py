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


def chunk_by_headings(
    text: str,
    max_chunk_size: int = 2000,
    overlap: int = 200,
    min_chunk_size: int = 100,
) -> List[ChunkWithMetadata]:
    """Chunk text by headings with section metadata preservation.
    
    Args:
        text: Full document text
        max_chunk_size: Maximum characters per chunk
        overlap: Character overlap between chunks within same section
        min_chunk_size: Minimum chunk size (merge small chunks)
        
    Returns:
        List of chunks with metadata
    """
    if not text:
        return []
    
    # Detect headings (Markdown-style or numbered)
    heading_patterns = [
        r'^(#{1,6})\s+(.+)$',  # Markdown headings
        r'^(\d+[\.\d]*)\s+(.+)$',  # Numbered sections
        r'^(Section\s+\d+|Chapter\s+\d+)[:\s]+(.+)$',  # Section/Chapter labels
        r'^([A-Z][A-Z\s]+)$',  # ALL CAPS headings
    ]
    
    lines = text.splitlines()
    chunks: List[ChunkWithMetadata] = []
    current_section_title = ""
    current_section_level = 0
    current_headings: List[str] = []
    current_text: List[str] = []
    current_size = 0
    
    def _flush_chunk():
        """Flush current chunk if it has enough content."""
        nonlocal current_text, current_size
        if current_text and current_size >= min_chunk_size:
            chunk_text = "\n".join(current_text).strip()
            if chunk_text:
                chunks.append(
                    ChunkWithMetadata(
                        text=chunk_text,
                        section_title=current_section_title,
                        section_level=current_section_level,
                        headings=list(current_headings),
                    )
                )
            current_text = []
            current_size = 0
    
    i = 0
    while i < len(lines):
        line = lines[i]
        line_stripped = line.strip()
        
        # Check if this line is a heading
        is_heading = False
        heading_text = ""
        heading_level = 0
        
        for pattern in heading_patterns:
            match = re.match(pattern, line_stripped)
            if match:
                is_heading = True
                if pattern.startswith(r'^(#{1,6})'):  # Markdown
                    heading_level = len(match.group(1))
                    heading_text = match.group(2).strip()
                elif pattern.startswith(r'^(\d+[\.\d]*)'):  # Numbered
                    heading_level = match.group(1).count('.') + 1
                    heading_text = match.group(2).strip()
                else:
                    heading_level = 1
                    heading_text = match.group(2).strip() if len(match.groups()) > 1 else match.group(1).strip()
                break
        
        if is_heading and heading_text:
            # Flush current chunk if we're starting a new section
            if current_size > 0:
                _flush_chunk()
            
            # Update section context
            if heading_level <= current_section_level:
                # Pop headings at same or higher level
                current_headings = [
                    h for h, lvl in zip(
                        current_headings,
                        [len(re.findall(r'\.', h.split()[0])) + 1 if re.match(r'^\d+', h.split()[0]) else 1
                         for h in current_headings]
                    ) if lvl < heading_level
                ]
            
            current_section_title = heading_text
            current_section_level = heading_level
            current_headings.append(heading_text)
            
            # Include heading in chunk
            current_text.append(line)
            current_size += len(line)
            i += 1
            continue
        
        # Regular content line
        line_len = len(line)
        
        # Check if adding this line would exceed max size
        if current_size + line_len > max_chunk_size and current_size >= min_chunk_size:
            # Flush and start new chunk with overlap
            _flush_chunk()
            
            # Add overlap from previous chunk
            if chunks:
                last_chunk_text = chunks[-1].text
                overlap_text = last_chunk_text[-overlap:] if len(last_chunk_text) > overlap else last_chunk_text
                current_text.append(overlap_text)
                current_size = len(overlap_text)
        
        current_text.append(line)
        current_size += line_len
        i += 1
    
    # Flush final chunk
    _flush_chunk()
    
    return chunks


def create_chunk_metadata(chunk: ChunkWithMetadata, doc_id: str, chunk_idx: int) -> Dict[str, Any]:
    """Create ChromaDB metadata for a chunk."""
    import hashlib
    
    chunk_id = f"{doc_id}_chunk_{chunk_idx:05d}"
    chunk_hash = hashlib.sha256(chunk.text.encode()).hexdigest()[:8]
    
    return {
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "section_title": chunk.section_title,
        "section_level": chunk.section_level,
        "headings": " | ".join(chunk.headings) if chunk.headings else "",
        "page_number": chunk.page_number,
        "chunk_hash": chunk_hash,
        "source_type": "project_document",
    }


__all__ = ["chunk_by_headings", "ChunkWithMetadata", "create_chunk_metadata"]

