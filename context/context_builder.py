from __future__ import annotations

"""Context pack assembly for the ENG Form 6293 CSP sections."""

from dataclasses import dataclass
from typing import Any, Dict, List

from pipelines.csp_pipeline import DocumentIngestionResult, MetadataState


@dataclass(frozen=True)
class SectionDefinition:
    identifier: str
    title: str
    em385_refs: List[str]
    keywords: List[str]
    description: str


SECTION_DEFINITIONS: List[SectionDefinition] = [
    SectionDefinition(
        identifier="section_01",
        title="Section 1 – Signatures and Roles",
        em385_refs=["§01.A.01", "§01.A.02", "§01.A.03", "§01.A.05", "§01.A.13"],
        keywords=["signature", "role", "responsibility", "authority"],
        description="Plan preparer, approvers, SSHO authority, and role assignments.",
    ),
    SectionDefinition(
        identifier="section_02",
        title="Section 2 – Project Information",
        em385_refs=["§01.A.01"],
        keywords=["project", "location", "scope", "phase", "address"],
        description="Project overview, location, schedule, scope, and definable features of work.",
    ),
    SectionDefinition(
        identifier="section_03",
        title="Section 3 – Prime Contractor Information",
        em385_refs=["§01.A.01", "§01.A.06"],
        keywords=["contract", "prime", "subcontractor", "chain of command"],
        description="Prime contractor leadership, subcontractors, and responsibility matrix.",
    ),
    SectionDefinition(
        identifier="section_04",
        title="Section 4 – Safety and Occupational Health (SOH) Commitment and Policy",
        em385_refs=["§01.B.01", "§01.B.02", "§01.B.03", "§01.B.04"],
        keywords=["policy", "commitment", "goal", "objective", "disciplinary"],
        description="Corporate safety policy, goals, accountability, and disciplinary actions.",
    ),
    SectionDefinition(
        identifier="section_05",
        title="Section 5 – Training Program",
        em385_refs=["§01.A.17", "§02.A", "§02.B", "§02.C", "§02.D"],
        keywords=["training", "orientation", "toolbox", "competent person"],
        description="Training requirements, orientation processes, and competency tracking.",
    ),
    SectionDefinition(
        identifier="section_06",
        title="Section 6 – Safety and Health Inspections Program",
        em385_refs=["§01.A.13", "§01.A.14", "§01.A.15", "§02.A.04"],
        keywords=["inspection", "audit", "deficiency", "corrective"],
        description="Inspection cadence, documentation, corrective actions, and retention.",
    ),
    SectionDefinition(
        identifier="section_07",
        title="Section 7 – Accident Reporting and Investigation",
        em385_refs=["§01.A.16", "Chapter 3"],
        keywords=["accident", "incident", "report", "investigation", "notification"],
        description="Accident classification, notification timelines, investigation, and reporting forms.",
    ),
    SectionDefinition(
        identifier="section_08",
        title="Section 8 – SOH Oversight and Risk Management",
        em385_refs=["§01.A.09", "§02.A.01", "Chapter 3"],
        keywords=["risk", "management", "aha", "residual", "approval"],
        description="Integration of risk management, document updates, and residual risk approvals.",
    ),
    SectionDefinition(
        identifier="section_09",
        title="Section 9 – Severe Weather Plan",
        em385_refs=["§06.A.01", "§06.A.02", "§06.A.03", "§06.A.04", "§06.A.05"],
        keywords=["weather", "wind", "lightning", "temperature", "evacuation"],
        description="Weather monitoring, thresholds, communication, and restart procedures.",
    ),
    SectionDefinition(
        identifier="section_10",
        title="Section 10 – Activity Hazard Analysis (AHA) Management Plan",
        em385_refs=["§01.A.09", "§02.A.02", "§02.A.03", "§02.A.04", "§02.A.05", "§02.A.06"],
        keywords=["aha", "activity hazard", "review", "approval", "update"],
        description="AHA development, review workflow, worker acknowledgements, and revision control.",
    ),
    SectionDefinition(
        identifier="section_11",
        title="Section 11 – Site-Specific Compliance Plans",
        em385_refs=["§21-7.a", "§34-7.b", "§25-7", "§11-7", "§12-7", "§9-7"],
        keywords=["plan", "program", "compliance", "special"],
        description="Applicability and status of required EM 385 sub-plans tied to DFOW hazards.",
    ),
    SectionDefinition(
        identifier="section_12",
        title="Section 12 – Multi-Employer Coordination Procedures",
        em385_refs=["§01.A.06", "§01.A.14"],
        keywords=["coordination", "multi-employer", "communication", "meeting"],
        description="Coordination with subcontractors and suppliers, document sharing, and language access.",
    ),
    SectionDefinition(
        identifier="section_13",
        title="Section 13 – Appendices Index and Attachments",
        em385_refs=["Chapter 1"],
        keywords=["appendix", "attachment", "revision", "log", "document control"],
        description="Appendix list, document control, and revision tracking placeholders.",
    ),
]


def _extract_snippets(text: str, keywords: List[str], max_snippets: int = 5) -> List[str]:
    if not text:
        return []
    sentences = [s.strip() for s in text.replace("\n", " ").split(".")]
    snippets: List[str] = []
    for sentence in sentences:
        low = sentence.lower()
        if any(keyword in low for keyword in keywords):
            if sentence and sentence not in snippets:
                snippets.append(sentence + "." if not sentence.endswith(".") else sentence)
        if len(snippets) >= max_snippets:
            break
    return snippets


def build_context_packs(
    ingestion: DocumentIngestionResult,
    metadata: MetadataState,
    sub_plan_matrix: Dict[str, Dict[str, object]],
) -> Dict[str, Dict[str, Any]]:
    """Construct context packs for each CSP section."""

    packs: Dict[str, Dict[str, Any]] = {}

    for section in SECTION_DEFINITIONS:
        snippets = _extract_snippets(ingestion.extracted_text, section.keywords)
        packs[section.identifier] = {
            "title": section.title,
            "em385_refs": section.em385_refs,
            "description": section.description,
            "snippets": snippets,
            "metadata": metadata.data,
            "metadata_sources": metadata.sources,
            "placeholders": metadata.placeholders,
            "dfow": ingestion.dfow,
            "hazards": ingestion.hazards,
            "documents": ingestion.documents,
            "sub_plans": sub_plan_matrix if section.identifier == "section_11" else {},
            "citations": ingestion.citations,
        }

    return packs


__all__ = ["build_context_packs", "SECTION_DEFINITIONS", "SectionDefinition"]

