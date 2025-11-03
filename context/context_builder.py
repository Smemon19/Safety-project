from __future__ import annotations

"""Context pack assembly for the ENG Form 6293 CSP sections."""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from pipelines.csp_pipeline import DocumentIngestionResult, MetadataState


@dataclass(frozen=True)
class SectionDefinition:
    identifier: str
    title: str
    intent: str
    must_answer: List[str]
    em385_refs: List[str]
    keywords: List[str]
    description: str
    topic_tags: List[str] = field(default_factory=list)
    allowed_heading_paths: List[str] = field(default_factory=list)
    project_evidence_quota: int = 3
    em_evidence_quota: int = 1
    max_project_evidence: int = 6
    max_em_evidence: int = 4


SECTION_DEFINITIONS: List[SectionDefinition] = [
    SectionDefinition(
        identifier="section_01",
        title="Section 1 – Signatures and Roles",
        intent="Document who signs the CSP, the authority they hold, and how safety decisions escalate.",
        must_answer=[
            "Identify signatories or role holders for ENG Form 6293 and CSP approval.",
            "Explain how the SSHO exercises stop-work authority on this project.",
            "Describe the communication chain for safety and health decisions among leaders.",
        ],
        em385_refs=["§01.A.01", "§01.A.02", "§01.A.03", "§01.A.05", "§01.A.13"],
        keywords=["signature", "role", "responsibility", "authority"],
        description="Plan preparer, approvers, SSHO authority, and role assignments.",
        topic_tags=["roles", "authority", "org chart", "eng 6293"],
        allowed_heading_paths=["01 11 00", "01 35 26"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_02",
        title="Section 2 – Project Information",
        intent="Capture project identifiers, scope elements, definable features of work, and key hazards.",
        must_answer=[
            "State verified project name/number/location from cover documentation.",
            "List major definable features of work or phases planned.",
            "Highlight notable project hazards or operational constraints.",
        ],
        em385_refs=["§01.A.01"],
        keywords=["project", "location", "scope", "phase", "address"],
        description="Project overview, location, schedule, scope, and definable features of work.",
        topic_tags=["project summary", "dfow", "scope"],
        allowed_heading_paths=["01 11 00", "Summary of Work"],
        project_evidence_quota=3,
        em_evidence_quota=1,
    ),
    SectionDefinition(
        identifier="section_03",
        title="Section 3 – Prime Contractor Information",
        intent="Summarize the prime contractor's team, subcontractor oversight, and communication pathways.",
        must_answer=[
            "List key prime contractor leadership roles supporting safety management.",
            "Describe how subcontractors are coordinated or overseen.",
            "Note communication or reporting channels for multi-employer activities.",
        ],
        em385_refs=["§01.A.01", "§01.A.06"],
        keywords=["contract", "prime", "subcontractor", "chain of command"],
        description="Prime contractor leadership, subcontractors, and responsibility matrix.",
        topic_tags=["prime contractor", "subcontractor", "coordination"],
        allowed_heading_paths=["01 35 23", "01 35 26"],
        project_evidence_quota=3,
        em_evidence_quota=1,
    ),
    SectionDefinition(
        identifier="section_04",
        title="Section 4 – Safety and Occupational Health (SOH) Commitment and Policy",
        intent="Record corporate SOH policy statements, measurable objectives, and accountability mechanisms.",
        must_answer=[
            "Identify corporate or project-level safety policy commitments.",
            "Document measurable safety objectives or goals relevant to the project.",
            "Explain accountability or disciplinary measures tied to safety performance.",
        ],
        em385_refs=["§01.B.01", "§01.B.02", "§01.B.03", "§01.B.04"],
        keywords=["policy", "commitment", "goal", "objective", "disciplinary"],
        description="Corporate safety policy, goals, accountability, and disciplinary actions.",
        topic_tags=["policy", "commitment", "objectives"],
        allowed_heading_paths=["01 35 00", "corporate policy"],
        project_evidence_quota=3,
        em_evidence_quota=1,
    ),
    SectionDefinition(
        identifier="section_05",
        title="Section 5 – Training Program",
        intent="Lay out training, orientation, and competency validation requirements for the workforce.",
        must_answer=[
            "Describe site-specific orientation or onboarding requirements before work.",
            "Detail required or planned competency/qualified person training for roles.",
            "Explain how training records or matrices are maintained and updated.",
        ],
        em385_refs=["§01.A.17", "§02.A", "§02.B", "§02.C", "§02.D"],
        keywords=["training", "orientation", "toolbox", "competent person"],
        description="Training requirements, orientation processes, and competency tracking.",
        topic_tags=["training", "orientation", "toolbox"],
        allowed_heading_paths=["01 35 26", "training"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_06",
        title="Section 6 – Safety and Health Inspections Program",
        intent="Explain inspection cadence, responsible roles, and how deficiencies are corrected.",
        must_answer=[
            "Clarify inspection frequency and the individuals conducting them.",
            "Describe how findings or deficiencies are documented and closed.",
            "Note specialized inspection triggers (e.g., scaffolds, excavations).",
        ],
        em385_refs=["§01.A.13", "§01.A.14", "§01.A.15", "§02.A.04"],
        keywords=["inspection", "audit", "deficiency", "corrective"],
        description="Inspection cadence, documentation, corrective actions, and retention.",
        topic_tags=["inspection", "audit", "deficiency"],
        allowed_heading_paths=["01 35 26", "inspections"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_07",
        title="Section 7 – Accident Reporting and Investigation",
        intent="Outline notification timelines, investigation expectations, and reporting tools for incidents.",
        must_answer=[
            "Document reporting timelines for serious incidents and near misses.",
            "Summarize investigation steps or responsibilities after an event.",
            "List required forms or communication channels for notifications.",
        ],
        em385_refs=["§01.A.16", "Chapter 3"],
        keywords=["accident", "incident", "report", "investigation", "notification"],
        description="Accident classification, notification timelines, investigation, and reporting forms.",
        topic_tags=["incident", "reporting", "investigation"],
        allowed_heading_paths=["01 35 23", "incident"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_08",
        title="Section 8 – SOH Oversight and Risk Management",
        intent="Capture how risk decisions are made, approvals recorded, and changes managed.",
        must_answer=[
            "Explain the risk management or residual risk evaluation process used.",
            "Identify who approves different residual risk levels or stop-work decisions.",
            "Describe how changes or new hazards trigger updates to plans or AHAs.",
        ],
        em385_refs=["§01.A.09", "§02.A.01", "Chapter 3"],
        keywords=["risk", "management", "aha", "residual", "approval"],
        description="Integration of risk management, document updates, and residual risk approvals.",
        topic_tags=["risk", "residual", "approval"],
        allowed_heading_paths=["01 35 23", "risk"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_09",
        title="Section 9 – Severe Weather Plan",
        intent="Identify weather monitoring methods, protective actions, and restart requirements.",
        must_answer=[
            "State weather monitoring frequency, sources, or triggers.",
            "Describe protective actions for severe weather conditions (heat, cold, wind, lightning).",
            "Explain restart or inspection steps before resuming work after weather events.",
        ],
        em385_refs=["§06.A.01", "§06.A.02", "§06.A.03", "§06.A.04", "§06.A.05"],
        keywords=["weather", "wind", "lightning", "temperature", "evacuation"],
        description="Weather monitoring, thresholds, communication, and restart procedures.",
        topic_tags=["weather", "heat", "lightning", "evacuation"],
        allowed_heading_paths=["01 35 23", "weather"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_10",
        title="Section 10 – Activity Hazard Analysis (AHA) Management Plan",
        intent="Describe how AHAs are developed, reviewed, distributed, and kept current.",
        must_answer=[
            "Clarify when AHAs are required relative to DFOW activities.",
            "Identify approval workflow or reviewers for AHAs.",
            "Explain how crews access AHAs and acknowledge controls before work.",
        ],
        em385_refs=["§01.A.09", "§02.A.02", "§02.A.03", "§02.A.04", "§02.A.05", "§02.A.06"],
        keywords=["aha", "activity hazard", "review", "approval", "update"],
        description="AHA development, review workflow, worker acknowledgements, and revision control.",
        topic_tags=["aha", "activity", "residual risk"],
        allowed_heading_paths=["01 35 26", "AHA"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_11",
        title="Section 11 – Site-Specific Compliance Plans",
        intent="Determine which EM 385-driven compliance plans apply and why they are required, pending, or NA.",
        must_answer=[
            "List compliance plans triggered by DFOW or hazards with their status.",
            "Provide justification text for each Required or Pending plan tied to evidence tags.",
            "Explain why any plan is marked Not Applicable based on project scope.",
        ],
        em385_refs=["§21-7.a", "§34-7.b", "§25-7", "§11-7", "§12-7", "§9-7"],
        keywords=["plan", "program", "compliance", "special"],
        description="Applicability and status of required EM 385 sub-plans tied to DFOW hazards.",
        topic_tags=["plan", "compliance", "register"],
        allowed_heading_paths=["01 35 23", "plans"],
        project_evidence_quota=3,
        em_evidence_quota=2,
        max_project_evidence=8,
        max_em_evidence=5,
    ),
    SectionDefinition(
        identifier="section_12",
        title="Section 12 – Multi-Employer Coordination Procedures",
        intent="Capture how the prime contractor coordinates safety information and meetings with other employers.",
        must_answer=[
            "Describe coordination meetings or touchpoints among employers.",
            "Explain how safety information, plans, or AHAs are distributed across employers.",
            "Note how language access or worker briefings are handled for diverse crews.",
        ],
        em385_refs=["§01.A.06", "§01.A.14"],
        keywords=["coordination", "multi-employer", "communication", "meeting"],
        description="Coordination with subcontractors and suppliers, document sharing, and language access.",
        topic_tags=["coordination", "meeting", "communication"],
        allowed_heading_paths=["01 35 23", "coordination"],
        project_evidence_quota=3,
        em_evidence_quota=2,
    ),
    SectionDefinition(
        identifier="section_13",
        title="Section 13 – Appendices Index and Attachments",
        intent="List appendices, revision control artifacts, and how updates are managed.",
        must_answer=[
            "Identify appendices or attachments that accompany the CSP.",
            "Document how revisions are tracked and communicated to stakeholders.",
            "Explain how distribution or manifest information is maintained.",
        ],
        em385_refs=["Chapter 1"],
        keywords=["appendix", "attachment", "revision", "log", "document control"],
        description="Appendix list, document control, and revision tracking placeholders.",
        topic_tags=["appendix", "revision", "distribution"],
        allowed_heading_paths=["01 33 00", "01 78 00"],
        project_evidence_quota=3,
        em_evidence_quota=1,
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
            "intent": section.intent,
            "must_answer": section.must_answer,
            "topic_tags": section.topic_tags,
            "allowed_heading_paths": section.allowed_heading_paths,
            "project_evidence_quota": section.project_evidence_quota,
            "em_evidence_quota": section.em_evidence_quota,
            "max_project_evidence": section.max_project_evidence,
            "max_em_evidence": section.max_em_evidence,
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

