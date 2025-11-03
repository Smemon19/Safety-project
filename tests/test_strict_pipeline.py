import asyncio
import os

import pytest

from context.document_sanitizer import sanitize_document_text
from context.section_retriever import SectionScopedRetriever
from context.dfow_mapping import map_dfow_to_plans
from generators.evidence_generator import EvidenceBasedSectionGenerator
from pipelines.services.defaults import DefaultValidator
from pipelines.csp_pipeline import MetadataState, ProcessingState
from models.csp import CspSection, CspCitation
from context.context_builder import SECTION_DEFINITIONS
from generators.csp import _normalize_em_ref


def test_sanitizer_drops_toc_and_boilerplate():
    raw = (
        "TABLE OF CONTENTS\nProject Overview\n........ 1\f"
        "for receipt by the Contracting Officer\nUpdate the Table of Contents\f"
        "Section 01 Safety\nAdd absorbent material to absorb residue oil remaining after draining.\n"
        "The pro-\nject team shall mobilize."
    )

    sanitized = sanitize_document_text(raw)
    lower = sanitized.lower()
    assert "table of contents" not in lower
    assert "for receipt by the contracting officer" not in lower
    assert "add absorbent material" not in lower
    assert "project team" in sanitized  # hyphenation repaired


def _make_full_section(name: str) -> CspSection:
    return CspSection(
        name=name,
        paragraphs=[
            "Purpose: Supported text [CH1]",
            "Procedures / Policy / Requirements: Supported text",
            "Responsibilities: Supported text",
            "Forms, Logs, or Records: Supported text",
        ],
        citations=[CspCitation(section_path="", page_label="", source_url="doc1"),
                   CspCitation(section_path="", page_label="", source_url="doc2"),
                   CspCitation(section_path="", page_label="", source_url="doc3")],
    )


def test_titleblock_contamination_fails():
    metadata = MetadataState(
        data={
            "project_name": "Valid Project",
            "project_number": "TABLE OF CONTENTS leak",
            "location": "Site A",
            "owner": "Owner",
            "prime_contractor": "Prime",
        },
        placeholders={},
        warnings=[],
        source=None,
        sources=[],
    )
    sections = [_make_full_section(defn.title) for defn in SECTION_DEFINITIONS]
    processing = ProcessingState(
        context_packs={},
        sections=sections,
        sub_plan_matrix={"Fall Protection": {"status": "Pending", "justification": ""}},
        manifest_fragments={},
        logs=[],
    )

    validator = DefaultValidator()
    state = validator.validate(metadata, processing)
    assert state.errors, "Validation should fail when title block is contaminated"
    assert any("title block" in line.lower() for line in state.errors[0].splitlines())


def test_section_retrieval_is_coherent():
    retriever = SectionScopedRetriever("test", chroma_client=None)
    ids = ["c1", "c2", "c3"]
    docs = ["text1", "text2", "text3"]
    metas = [
        {"section_title": "Training"},
        {"section_title": "Training"},
        {"section_title": "Other"},
    ]

    new_ids, _, new_metas = retriever._enforce_section_domain(ids, docs, metas)
    assert new_ids == ["c1", "c2"]
    assert all(meta["section_title"] == "Training" for meta in new_metas)


def test_extractive_quota_enforced():
    async def _run():
        generator = EvidenceBasedSectionGenerator("dummy", chroma_client=None)

        async def fake_stage(*args, **kwargs):  # type: ignore
            return []

        generator._stage_one_prefilter = fake_stage  # type: ignore
        generator._stage_two_rerank = fake_stage  # type: ignore

        definition = SECTION_DEFINITIONS[0]
        result = await generator.build_context_packet(definition, {})
        assert result.insufficient_reasons

    asyncio.run(_run())


def test_citation_normalization():
    assert _normalize_em_ref("EM 385-1-1 §1.A.3") == "§01.A.03"
    assert _normalize_em_ref("§02.A.04") == "§02.A.04"


def test_section11_rule_mapping():
    dfow = [
        {"text": "Concrete grinding", "chunk_id": "CH-SI"},
        {"text": "Roof work", "chunk_id": "CH-FP"},
    ]
    hazards = [{"text": "Heavy equipment noise", "chunk_id": "CH-NO"}]
    matrix = map_dfow_to_plans(dfow, hazards)
    silica = matrix["Silica Compliance Plan"]
    assert silica["status"] == "Pending"
    assert "CH-SI" in silica["justification"]

    welding_only = map_dfow_to_plans([{"text": "Welding operations", "chunk_id": "W1"}], [])
    assert welding_only["Silica Compliance Plan"]["status"] == "Not Applicable"


def test_offline_mode_does_not_abstrate(monkeypatch):
    async def _run():
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)

        generator = EvidenceBasedSectionGenerator("dummy", chroma_client=None)

        from context.section_retriever import EvidenceChunk

        async def fake_stage_one(*args, **kwargs):  # type: ignore
            return [
                EvidenceChunk(
                    chunk_id="P1",
                    text="The SSHO completes daily inspections covering all active work areas.",
                    source="project_doc.pdf",
                    page_number=1,
                ),
                EvidenceChunk(
                    chunk_id="P2",
                    text="Supervisors document findings and track closure of deficiencies in the project log.",
                    source="project_doc.pdf",
                    page_number=2,
                ),
                EvidenceChunk(
                    chunk_id="EM1",
                    text="EM 385-1-1 §01.A.13 requires SSHO authority to stop work when hazards are observed.",
                    source="EM 385-1-1",
                    page_number=12,
                ),
            ]

        async def fake_stage_two(*args, **kwargs):  # type: ignore
            return await fake_stage_one()

        generator._stage_one_prefilter = fake_stage_one  # type: ignore
        generator._stage_two_rerank = fake_stage_two  # type: ignore

        definition = SECTION_DEFINITIONS[1]  # Section 2 requires 1 EM bullet
        result = await generator.build_context_packet(definition, {})
        assert result.insufficient_reasons

    asyncio.run(_run())

