from __future__ import annotations

"""Tests for evidence-based CSP generation system."""

import pytest
from pathlib import Path

from context.document_sanitizer import sanitize_document_text, tag_chunk_for_exclusion
from context.heading_aware_chunking import chunk_by_headings
from context.contamination_guard import filter_contaminated_content, detect_contamination, BANNED_PHRASES
from context.dfow_mapping import map_dfow_to_plans


def test_sanitizer_removes_toc():
    """Test that sanitizer removes table of contents."""
    text = """
    TABLE OF CONTENTS
    
    Section 1     ............... 1
    Section 2     ............... 5
    Section 3     ............... 10
    
    Section 1 Content
    This is actual content.
    """
    sanitized = sanitize_document_text(text)
    assert "TABLE OF CONTENTS" not in sanitized.upper()
    assert "Section 1 Content" in sanitized
    assert "actual content" in sanitized


def test_sanitizer_removes_boilerplate():
    """Test that sanitizer removes procurement boilerplate."""
    text = """
    This document is subject to terms and conditions.
    All rights reserved. Copyright 2024.
    
    Project Name: Test Project
    Location: Test Location
    
    Actual project content goes here.
    """
    sanitized = sanitize_document_text(text)
    assert "subject to terms" not in sanitized.lower()
    assert "all rights reserved" not in sanitized.lower()
    assert "Project Name: Test Project" in sanitized
    assert "Actual project content" in sanitized


def test_chunk_exclusion_tagging():
    """Test that TOC chunks are tagged for exclusion."""
    toc_chunk = """
    Page 1
    Section 1     ............... 1
    Section 2     ............... 5
    Page 2
    """
    assert tag_chunk_for_exclusion(toc_chunk) is True
    
    valid_chunk = "The SSHO conducts daily inspections covering all active work areas."
    assert tag_chunk_for_exclusion(valid_chunk) is False


def test_heading_aware_chunking():
    """Test heading-aware chunking preserves section structure."""
    text = """
    Section 1: Safety Program
    
    This is content under section 1.
    It has multiple paragraphs.
    
    Section 2: Training
    
    Training content here.
    More training details.
    """
    chunks = chunk_by_headings(text, max_chunk_size=500)
    
    assert len(chunks) >= 2
    section_titles = [c.section_title for c in chunks if c.section_title]
    assert any("Safety" in title for title in section_titles)
    assert any("Training" in title for title in section_titles)


def test_contamination_detection():
    """Test contamination guard detects banned phrases."""
    contaminated = "This is based on LLM guidance and best practice recommendations."
    detected = detect_contamination(contaminated)
    assert len(detected) > 0
    assert any("llm" in phrase.lower() for phrase in detected)


def test_contamination_filtering():
    """Test contamination guard removes banned phrases."""
    text = "The SSHO conducts inspections. This is based on LLM guidance. Workers must wear PPE."
    evidence = ["SSHO conducts inspections", "Workers must wear PPE"]
    
    cleaned, count = filter_contaminated_content(text, evidence_texts=evidence)
    
    assert "LLM guidance" not in cleaned
    assert "SSHO conducts" in cleaned or "inspections" in cleaned
    assert count > 0  # Should have removed at least one sentence


def test_dfow_to_plan_mapping():
    """Test DFOW correctly maps to site plans."""
    # Silica should NOT trigger from generic welding
    dfow = ["welding", "steel erection"]
    hazards = []
    plans = map_dfow_to_plans(dfow, hazards)
    
    silica_plan = plans.get("Silica Compliance Plan", {})
    # Silica should not be triggered by generic welding
    assert silica_plan.get("status") == "Not Applicable" or not any(
        "silica" in k.lower() for k in plans.keys()
    )
    
    # But concrete cutting should trigger Silica
    dfow_concrete = ["concrete cutting", "masonry grinding"]
    plans_concrete = map_dfow_to_plans(dfow_concrete, hazards)
    silica_concrete = plans_concrete.get("Silica Compliance Plan", {})
    # Silica should be required/pending for concrete work
    assert silica_concrete.get("status") in ("Required", "Pending", "Generated")


def test_retriever_coherence():
    """Test that section retriever builds coherent queries."""
    from context.section_retriever import SectionScopedRetriever
    
    retriever = SectionScopedRetriever(
        collection_name="test_collection",
        chroma_client=None,  # Will skip actual retrieval
    )
    
    project_context = {
        "dfow": ["excavation", "trenching"],
        "hazards": ["cave-in"],
        "project_name": "Test Project",
    }
    
    query = retriever.build_section_query("section_11", project_context)
    assert "excavation" in query.lower() or "trench" in query.lower()
    assert len(query) > 0


def test_evidence_extraction():
    """Test evidence extraction produces valid bullets."""
    from generators.evidence_generator import EvidenceBasedSectionGenerator
    
    generator = EvidenceBasedSectionGenerator(
        collection_name="test",
        chroma_client=None,
    )
    
    # Test sentence extraction
    chunk_text = "The SSHO must complete daily site inspections. Inspections cover all active DFOW. Deficiencies are recorded immediately."
    sentences = generator._extract_key_sentences(chunk_text)
    
    assert len(sentences) > 0
    assert any("SSHO" in s or "inspection" in s.lower() for s in sentences)
    assert all(len(s) >= 20 for s in sentences)  # Minimum length


__all__ = [
    "test_sanitizer_removes_toc",
    "test_sanitizer_removes_boilerplate",
    "test_chunk_exclusion_tagging",
    "test_heading_aware_chunking",
    "test_contamination_detection",
    "test_contamination_filtering",
    "test_dfow_to_plan_mapping",
    "test_retriever_coherence",
    "test_evidence_extraction",
]

