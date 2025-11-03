"""Template-free CSP assembly utilities."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from context.context_builder import SECTION_DEFINITIONS
from generators.evidence_generator import EvidenceBasedSectionGenerator
from generators.section_orchestrator import SectionOrchestrator, SectionRunResult
from models.csp import CspDoc, CspSection


def _normalize_em_ref(ref: str) -> str:
    """Normalize EM 385 references to §XX.A.XX style."""

    ref = (ref or "").strip()
    if not ref:
        return ""

    ref = ref.replace("EM 385-1-1", "").replace("EM385-1-1", "").strip()
    if not ref.startswith("§"):
        ref = "§" + ref

    import re

    match = re.match(r"§(\d+)\.([A-Za-z]+)\.(\d+)", ref)
    if match:
        chapter = match.group(1).zfill(2)
        letter = match.group(2).upper()
        section = match.group(3).zfill(2)
        return f"§{chapter}.{letter}.{section}"

    match = re.match(r"§(\d+)-(\d+)(?:\.(\w+))?", ref)
    if match:
        return ref

    return ref


async def _orchestrate_sections_async(
    context_packs: Dict[str, Dict[str, Any]],
    generator: EvidenceBasedSectionGenerator,
) -> List[SectionRunResult]:
    results: List[SectionRunResult] = []
    for definition in SECTION_DEFINITIONS:
        pack = context_packs.get(definition.identifier, {})
        pack.setdefault("title", definition.title)
        orchestrator = SectionOrchestrator(definition, generator)
        result = await orchestrator.run(pack)
        results.append(result)
    return results


def build_csp_sections(
    context_packs: Dict[str, Dict[str, Any]],
    *,
    generator: Optional[EvidenceBasedSectionGenerator] = None,
) -> List[CspSection]:
    """Construct CSP sections. If a generator is provided, run evidence orchestration."""

    if generator is None:
        return [
            CspSection(name=definition.title, paragraphs=[], citations=[], context_packet=None)
            for definition in SECTION_DEFINITIONS
        ]

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    results = loop.run_until_complete(
        _orchestrate_sections_async(context_packs, generator)
    )
    return [result.section for result in results]


def assemble_csp_doc(
    metadata: Dict[str, Any],
    context_packs: Dict[str, Dict[str, Any]],
    sections: Optional[List[CspSection]] = None,
) -> CspDoc:
    project_name = str(metadata.get("project_name", ""))
    project_number = str(metadata.get("project_number", ""))
    location = str(metadata.get("location", ""))
    owner = str(metadata.get("owner", ""))
    prime_contractor = str(metadata.get("prime_contractor", metadata.get("gc", "")))

    if sections is None:
        sections = build_csp_sections(context_packs)

    return CspDoc(
        project_name=project_name,
        project_number=project_number,
        location=location,
        owner=owner,
        general_contractor=prime_contractor,
        sections=sections,
    )


def generate_csp(spec: Dict[str, Any], collection_name: Optional[str] = None) -> CspDoc:
    """Legacy helper that assembles a CSP doc without templated prose."""

    metadata = {
        "project_name": spec.get("project_name", ""),
        "project_number": spec.get("project_number", ""),
        "location": spec.get("location", ""),
        "owner": spec.get("owner", ""),
        "prime_contractor": spec.get("gc", spec.get("prime_contractor", "")),
    }

    dfow: List[str] = []
    for wp in spec.get("work_packages", []) or []:
        dfow.extend([act for act in wp.get("activities", []) or []])
    if not dfow and spec.get("activities"):
        dfow.extend(spec.get("activities"))
    hazards = spec.get("hazards", []) or []

    context_packs: Dict[str, Dict[str, Any]] = {}
    for definition in SECTION_DEFINITIONS:
        context_packs[definition.identifier] = {
            "title": definition.title,
            "intent": definition.intent,
            "must_answer": definition.must_answer,
            "em385_refs": definition.em385_refs,
            "topic_tags": definition.topic_tags,
            "allowed_heading_paths": definition.allowed_heading_paths,
            "project_evidence_quota": definition.project_evidence_quota,
            "em_evidence_quota": definition.em_evidence_quota,
            "max_project_evidence": definition.max_project_evidence,
            "max_em_evidence": definition.max_em_evidence,
            "metadata": metadata,
            "metadata_sources": ["legacy-spec"],
            "placeholders": {},
            "dfow": dfow,
            "hazards": hazards,
            "documents": spec.get("documents", []),
            "sub_plans": {},
            "citations": [],
        }

    generator: Optional[EvidenceBasedSectionGenerator] = None

    if collection_name:
        try:
            from utils import get_chroma_client, get_default_chroma_dir

            chroma_client = get_chroma_client(get_default_chroma_dir())
            generator = EvidenceBasedSectionGenerator(
                collection_name=collection_name,
                chroma_client=chroma_client,
            )
        except Exception:
            generator = None

    sections = build_csp_sections(context_packs, generator=generator)
    return assemble_csp_doc(metadata, context_packs, sections=sections)


__all__ = ["build_csp_sections", "assemble_csp_doc", "generate_csp", "_normalize_em_ref"]

