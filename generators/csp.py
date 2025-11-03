from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from context.context_builder import SECTION_DEFINITIONS
from context.citation_manager import generate_section_citations
from context.placeholder_manager import format_placeholder, contains_placeholder
from context.dfow_mapping import map_dfow_to_plans
from models.csp import CspDoc, CspSection


def _meta_value(metadata: Dict[str, Any], placeholders: Dict[str, str], key: str) -> str:
    value = metadata.get(key)
    if value and not contains_placeholder(str(value)):
        return str(value)
    placeholder = placeholders.get(key) or f"Insert {key.replace('_', ' ').title()}"
    return format_placeholder(placeholder)


def _get_role_name_or_title(metadata: Dict[str, Any], placeholders: Dict[str, str], key: str, default_title: str) -> str:
    """Get role name from metadata if available, otherwise return role title (no placeholder).
    
    This prevents placeholder insertion when role names aren't in the document.
    Use this for roles where the title is acceptable if a name isn't available.
    """
    value = metadata.get(key)
    if value and not contains_placeholder(str(value)):
        return str(value)
    # Return role title instead of placeholder - acceptable for these roles
    return default_title


def _doc_reference(documents: List[str]) -> str:
    names = [Path(p).name for p in documents if p]
    if names:
        return f"Project documents ({', '.join(names)})"
    return "User-provided project documents"


def _normalize_em_ref(ref: str) -> str:
    """Normalize EM 385 reference format: §01.A.13 (2-digit chapter, dot, letter, 2-digit section)."""
    ref = (ref or "").strip()
    if not ref:
        return ""
    # Remove EM 385-1-1 prefix if present
    ref = ref.replace("EM 385-1-1", "").replace("EM385-1-1", "").strip()
    # Ensure it starts with §
    if not ref.startswith("§"):
        ref = "§" + ref
    # Normalize format: ensure proper padding (e.g., §1.A.13 -> §01.A.13)
    import re
    # Match pattern §(\d+)\.(\w+)\.(\d+)
    match = re.match(r'§(\d+)\.([A-Za-z]+)\.(\d+)', ref)
    if match:
        chapter = match.group(1).zfill(2)  # Pad to 2 digits
        letter = match.group(2).upper()
        section = match.group(3).zfill(2)  # Pad to 2 digits
        return f"§{chapter}.{letter}.{section}"
    # Handle §XX-YY format (e.g., §21-7.a)
    match = re.match(r'§(\d+)-(\d+)(?:\.(\w+))?', ref)
    if match:
        return ref  # Keep as-is for this format
    return ref


def _format_references(em_refs: List[str], documents: List[str]) -> str:
    """Format references without LLM guidance. Normalize EM refs and deduplicate."""
    if not em_refs and not documents:
        return "References: EM 385-1-1 (relevant sections as applicable)."
    
    # Normalize and deduplicate EM refs
    normalized_refs = []
    seen_refs = set()
    for ref in em_refs:
        norm = _normalize_em_ref(ref)
        if norm and norm not in seen_refs:
            normalized_refs.append(norm)
            seen_refs.add(norm)
    
    em_text = ", ".join(normalized_refs) if normalized_refs else "relevant sections"
    doc_text = _doc_reference(documents)
    
    # Remove LLM guidance citation
    if doc_text:
        return f"References: EM 385-1-1 {em_text}; {doc_text}."
    return f"References: EM 385-1-1 {em_text}."


def _build_section_01(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    project_manager = _meta_value(metadata, placeholders, "project_manager")
    prime_contractor = _meta_value(metadata, placeholders, "prime_contractor")
    ssho = _meta_value(metadata, placeholders, "ssho")
    owner = _meta_value(metadata, placeholders, "owner")

    placeholders = context.get("placeholders", {})
    def _detail_for(key: str, default_title: str) -> str:
        value = _meta_value(metadata, placeholders, key)
        # If we have a name, use it; otherwise use the title
        if value and not contains_placeholder(str(value)):
            return str(value)
        return default_title

    # Use standardized role titles with proper casing
    ssho = _detail_for("ssho", "SSHO")
    superintendent = _detail_for("superintendent", "Superintendent")
    qc_manager = _detail_for("quality_control_manager", "QC Manager")
    corp_safety = _detail_for("corporate_safety_officer", "Corporate Safety Officer")
    foreman = _detail_for("foreman", "Foreman")

    doc_refs = context.get("documents", [])
    doc_context = (
        f"Key project references include: {', '.join(Path(d).name for d in doc_refs)}." if doc_refs
        else "Project documents will be cross-referenced upon upload."
    )

    paragraphs = [
        (
            "Purpose: Establish signatures and concurrence for ENG Form 6293 so that "
            f"{prime_contractor} and the U.S. Army Corps of Engineers can verify roles, "
            "delegations, and safety authority before mobilization. No work may begin until all "
            "required signatures are completed."
        ),
        (
            "Procedures / Policy / Requirements: The plan preparer issues signature pages "
            "capturing printed name, title, company, email, phone, and date for the Corporate "
            "Safety Officer, Project Manager, Superintendent, SSHO, QC Manager, and "
            "USACE representative. Digital or ink signatures are acceptable when archived in the "
            "project document control system. The SSHO is granted explicit authority to stop work in "
            "accordance with EM 385-1-1 §01.A.13."
        ),
        (
            "Responsibilities: "
            f"{corp_safety} approves the CSP and provides oversight; {project_manager} ensures resources and schedule alignment; "
            f"{ssho} monitors implementation and exercises stop-work authority; {superintendent} coordinates field execution; "
            f"{qc_manager} integrates safety verifications into QC processes; the Owner and contracting officer provide concurrence; "
            f"{foreman} communicates expectations to foremen and craft leads."
        ),
        (
            "Forms, Logs, or Records: Maintain a signature roster and concurrence log in Appendix 3, "
            "including updates when personnel change. Record stop-work events and resolutions in the SSHO logbook."
        ),
        doc_context,
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_02(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    project_name = _meta_value(metadata, placeholders, "project_name")
    location = _meta_value(metadata, placeholders, "location")
    owner = _meta_value(metadata, placeholders, "owner")
    prime_contractor = _meta_value(metadata, placeholders, "prime_contractor")
    dfow = context.get("dfow", [])
    hazards = context.get("hazards", [])
    snippets = context.get("snippets", [])

    dfow_text = ", ".join(dfow) if dfow else "Definable features of work to be confirmed"
    hazard_text_list = hazards[:6]
    hazard_text = ", ".join(hazard_text_list) if hazard_text_list else "Project-specific hazards pending confirmation"
    snippet_text = " ".join(snippets[:2]) if snippets else ""

    paragraphs = [
        (
            f"Purpose: Document project identification, location, and scope so that {prime_contractor} and "
            f"{owner} maintain a shared understanding of operations at {location}."
        ),
        (
            "Procedures / Policy / Requirements: Maintain a project data sheet referencing the USACE contract, "
            "start and completion dates, and major phases. Provide a site location map in Appendix 1 and update it "
            "if access routes or laydown areas change. Summarize definable features of work (DFOW) and associated "
            "hazards to frame risk planning."
        ),
        (
            "Responsibilities: The Project Manager keeps schedule and scope data current; the Quality Control Manager "
            "verifies DFOW lists before each phase; the SSHO integrates DFOW information into hazard analyses and "
            "orientation briefings."
        ),
        (
            f"Forms, Logs, or Records: Maintain the DFOW register and scope summary in Appendix 4 and cross-reference "
            f"the schedule of work packages. Hazard highlights include: {hazard_text}."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    # Remove Document Insight / TOC leakage - only include project summary
    summary_lines = [
        f"Project Summary: {project_name} located at {location} for {owner}.",
        f"Definable Features of Work: {dfow_text}."
    ]
    # Do NOT include Document Insight or TOC dumps
    paragraphs.insert(1, " ".join(summary_lines))
    return paragraphs


def _build_section_03(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    prime_contractor = _meta_value(metadata, placeholders, "prime_contractor")
    # Use role title if name not available (acceptable for these roles)
    project_manager = _get_role_name_or_title(metadata, placeholders, "project_manager", "Project Manager")
    ssho = _get_role_name_or_title(metadata, placeholders, "ssho", "SSHO")

    paragraphs = [
        (
            f"Purpose: Define {prime_contractor}'s project team, subcontractor oversight, and communication lines to assure "
            "consistent enforcement of this CSP."
        ),
        (
            "Procedures / Policy / Requirements: Maintain a chain-of-command diagram displaying corporate, project, SSHO, "
            "QC, and subcontractor leads. Verify subcontractor safety plans before mobilization and ensure contractual flow-down "
            "of CSP requirements."
        ),
        (
            f"Responsibilities: The Project Manager ({project_manager}) serves as the point of contact with USACE; "
            f"the SSHO ({ssho}) executes day-to-day oversight; each subcontractor designates a competent person to attend "
            "coordination meetings."
        ),
        (
            "Forms, Logs, or Records: Appendix 2 will house the subcontractor/supplier roster, insurance certificates, "
            "and contact directory. Maintain a communication log capturing directives provided to subcontractors."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_04(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    prime_contractor = _meta_value(metadata, placeholders, "prime_contractor")

    paragraphs = [
        (
            f"Purpose: Articulate {prime_contractor}'s corporate Safety and Occupational Health policy and set measurable "
            "objectives for this project."
        ),
        (
            "Procedures / Policy / Requirements: The corporate safety policy commits to preventing incidents through "
            "worker engagement, hazard elimination, and compliance with EM 385-1-1. Project goals include zero lost-time "
            "incidents, completion of daily toolbox talks, and closure of corrective actions within 48 hours. Disciplinary "
            "steps range from coaching to removal from the project for repeated safety violations. Subcontractors must sign "
            "acknowledgements confirming alignment with the APP."
        ),
        (
            "Responsibilities: Corporate Safety Officer issues policy statements and resources; the Project Manager ensures "
            "program execution; supervisors hold workers accountable; employees stop work and report hazards without retaliation."
        ),
        (
            "Forms, Logs, or Records: Maintain the signed corporate safety policy, disciplinary documentation, and safety "
            "goal scorecards in Appendix 5. Track leading indicators such as observations and near-miss reports."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_05(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    # Use role title if name not available (acceptable for these roles)
    ssho = _get_role_name_or_title(metadata, placeholders, "ssho", "SSHO")
    project_manager = _get_role_name_or_title(metadata, placeholders, "project_manager", "Project Manager")

    paragraphs = [
        (
            "Purpose: Provide a comprehensive training and competency program to ensure all personnel meet EM 385-1-1 "
            "requirements before performing work."
        ),
        (
            f"Procedures / Policy / Requirements: The SSHO ({ssho}) verifies that SSHO qualifications include OSHA 30-hour plus "
            "24-hour refresher within the past three years. Supervisors must hold competent-person training for their disciplines. "
            "All workers receive site-specific orientation covering emergency response, DFOW hazards, reporting expectations, and "
            "stop-work procedures before site access. Task-specific toolbox talks precede each DFOW. Training frequency and "
            f"expiration dates are tracked in a matrix updated weekly by {project_manager}'s team."
        ),
        (
            "Responsibilities: Corporate training maintains central records; the Project Manager coordinates resources for "
            "specialty training; the SSHO reviews credentials before mobilization; supervisors conduct toolbox talks and document "
            "attendance; workers sign acknowledgements and request clarification when unsure."
        ),
        (
            "Forms, Logs, or Records: Store orientation rosters, training certificates, toolbox talk records, and the role-based "
            "training matrix in Appendix 3. Retain electronic backups within the project document control system."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_06(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    # Use role title if name not available (acceptable for these roles)
    ssho = _get_role_name_or_title(metadata, placeholders, "ssho", "SSHO")

    paragraphs = [
        (
            "Purpose: Establish the inspection program necessary to identify and correct hazards throughout construction."
        ),
        (
            f"Procedures / Policy / Requirements: The SSHO ({ssho}) completes and documents daily site inspections covering all "
            "active DFOW. Supervisors conduct pre-task inspections before shifts. Weekly comprehensive inspections involve the SSHO, "
            "Project Manager, and Quality Control Manager. Specialized inspections (scaffolding, excavation, electrical) occur prior "
            "to first use and after significant changes. Deficiencies are recorded with corrective actions, responsible party, and "
            "closure verification. Records are retained for the project duration plus contracts requirements."
        ),
        (
            "Responsibilities: The SSHO leads inspections and tracks corrective actions; competent persons inspect their systems; "
            "Quality Control integrates safety checks into preparatory meetings; subcontractors correct hazards immediately or stop "
            "work."
        ),
        (
            "Forms, Logs, or Records: Daily inspection log, deficiency tracker, and weekly safety meeting minutes maintained in Appendix 5. "
            "Track metrics in the project dashboard for trend analysis."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_07(context: Dict[str, Any]) -> List[str]:
    documents = context["documents"]

    paragraphs = [
        (
            "Purpose: Define accident, incident, near miss, and property damage reporting and investigation processes to meet EM 385-1-1."
        ),
        (
            "Procedures / Policy / Requirements: Immediately notify emergency services for life-threatening incidents. Report fatalities "
            "or property damage exceeding $600,000 to USACE within 8 hours; all other reportable incidents within 24 hours. Secure the scene, "
            "collect witness statements, perform root cause analysis, and identify corrective actions. Submit initial and follow-up reports "
            "through USACE channels."
        ),
        (
            "Responsibilities: The SSHO coordinates emergency response and reporting; supervisors control the scene and gather initial facts; "
            "the Project Manager notifies corporate leadership and USACE; Quality Control assists with data collection; employees cooperate with "
            "investigations and report near misses."
        ),
        (
            "Forms, Logs, or Records: Maintain ENG Form 3394, OSHA 301/300 logs, near-miss reports, corrective action logs, and photo documentation "
            "within Appendix 5 and the incident management system. Track lessons learned for future toolbox talks."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_08(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    # Use role title if name not available (acceptable for these roles)
    project_manager = _get_role_name_or_title(metadata, placeholders, "project_manager", "Project Manager")
    ssho = _get_role_name_or_title(metadata, placeholders, "ssho", "SSHO")
    # Pull approvers from people.* tokens; use role title if name not available
    def _get_approver(key: str, role_title: str) -> str:
        value = metadata.get(key)
        if value and not contains_placeholder(str(value)):
            return str(value)
        # Try people.* structure
        people_key = f"people_{key}" if not key.startswith("people_") else key
        value = metadata.get(people_key)
        if value and not contains_placeholder(str(value)):
            return str(value)
        # Use role title instead of placeholder - acceptable for residual risk approvers
        return role_title
    
    residual_matrix_lines = [
        "Residual Risk Approval Matrix:",
        f"- **Extremely High:** {_get_approver('residual_risk_extreme', 'Corporate Safety Officer')}",
        f"- **High:** {_get_approver('residual_risk_high', 'Project Manager')}",
        f"- **Medium:** {_get_approver('residual_risk_medium', 'SSHO')}",
        f"- **Low:** {_get_approver('residual_risk_low', 'Superintendent')}",
    ]

    paragraphs = [
        (
            "Purpose: Integrate safety oversight and risk management into planning, execution, and change management activities."
        ),
        (
            f"Procedures / Policy / Requirements: Review the Accident Prevention Plan (APP) and all Activity Hazard Analyses (AHAs) before work. "
            f"When conditions change, update AHAs and the CSP, then brief affected crews. During preparatory meetings, {project_manager} and {ssho} "
            "evaluate residual risk and confirm controls. Implement stop-work whenever new or uncontrolled hazards arise. Risk decisions follow the matrix below."
        ),
        (
            "Responsibilities: Corporate Safety provides governance and approves extremely high risks; the Project Manager adjudicates high risks and "
            "allocates resources; the SSHO approves medium risks and monitors field execution; foremen manage low risks and elevate issues; workers "
            "participate in risk discussions and stop work when uncertain."
        ),
        (
            "Forms, Logs, or Records: Maintain the risk register, stop-work log, and change management forms in Appendix 5. Document approvals and "
            "residual risk acceptance in the AHA log."
        ),
    ]
    paragraphs.extend(residual_matrix_lines)
    paragraphs.append(_format_references(context["em385_refs"], documents))
    return paragraphs


def _build_section_09(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    # Use role title if name not available (acceptable for these roles)
    ssho = _get_role_name_or_title(metadata, placeholders, "ssho", "SSHO")

    paragraphs = [
        (
            "Purpose: Protect personnel and equipment from severe weather hazards through proactive monitoring and response."
        ),
        (
            f"Procedures / Policy / Requirements: Weather is checked twice daily via NOAA and site instrumentation. Continuous monitoring begins when "
            "winds exceed 20 mph, thunderstorms are forecast, heat index surpasses 90°F, or wind chill falls below 20°F. Suspend elevated work at sustained "
            "winds ≥ 30 mph, stop crane operations per manufacturer limits, and clear work areas when lightning is within 10 miles. Evacuate to designated "
            f"shelters and conduct accountability within 10 minutes. Restart only after a documented inspection by {ssho} and competent persons." 
        ),
        (
            "Responsibilities: The SSHO monitors forecasts and issues alerts; supervisors brief crews on protective actions; the Project Manager coordinates "
            "schedule adjustments; workers report weather changes and follow evacuation routes."
        ),
        (
            "Forms, Logs, or Records: Maintain the weather monitoring log, shelter map (Appendix 1), and post-event inspection checklist in Appendix 5. "
            "Document restart authorizations and lessons learned."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_10(context: Dict[str, Any]) -> List[str]:
    metadata = context["metadata"]
    placeholders = context["placeholders"]
    documents = context["documents"]
    project_manager = _meta_value(metadata, placeholders, "project_manager")
    ssho = _meta_value(metadata, placeholders, "ssho")

    paragraphs = [
        (
            "Purpose: Govern Activity Hazard Analysis (AHA) development, review, approval, and workforce engagement."
        ),
        (
            f"Procedures / Policy / Requirements: Develop an AHA for every DFOW before work. The SSHO drafts or reviews each AHA, the Project Manager "
            "approves high-risk activities, and the Contracting Officer Representative provides final concurrence. Workers review and sign AHAs before "
            "performing the task. Update AHAs when conditions, crew members, equipment, or controls change and retain prior versions for reference." 
        ),
        (
            "Responsibilities: Supervisors ensure AHAs are available at the point of work; the SSHO audits compliance; workers acknowledge and follow controls; "
            "Quality Control verifies AHAs during preparatory meetings." 
        ),
        (
            "Forms, Logs, or Records: Store approved AHAs and signature sheets in Appendix 4. Maintain an AHA index showing approval dates, residual risk, and "
            "responsible supervisors." 
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_11(context: Dict[str, Any]) -> List[str]:
    documents = context["documents"]
    sub_plans = context.get("sub_plans", {})

    # Categorize plans by status (Required/Pending/Not Applicable)
    required_plans = []
    pending_plans = []
    not_applicable_plans = []
    
    for name, data in sub_plans.items():
        status = data.get("status", "Not Applicable")
        justification = data.get("justification", "")
        
        if status == "Generated":
            # Treat Generated as Required
            required_plans.append((name, justification))
        elif status == "Pending":
            pending_plans.append((name, justification))
        elif status == "Required":
            required_plans.append((name, justification))
        else:  # Not Applicable
            not_applicable_plans.append((name, justification))

    paragraphs = [
        (
            "Purpose: Summarize all site-specific compliance plans mandated by EM 385-1-1 based on project DFOW and hazards."
        ),
        (
            "Procedures / Policy / Requirements: Develop or update each action-required plan listed below. Plans marked 'Required' or 'Pending' must be drafted, reviewed, and "
            "filed in Appendix 5 before associated work begins. Plans assessed as 'Not Applicable' require documentation of the scope rationale and validation during "
            "preparatory meetings. All required plans shall be maintained in Appendix 5 with revision control."
        ),
    ]

    if required_plans:
        paragraphs.append("Required Plans:")
        for plan_name, justification in required_plans:
            paragraphs.append(f"- {plan_name} — {justification}")
    
    if pending_plans:
        paragraphs.append("Pending Plans (action required):")
        for plan_name, justification in pending_plans:
            paragraphs.append(f"- {plan_name} — {justification}")
    
    if not_applicable_plans:
        paragraphs.append("Not Applicable Plans:")
        for plan_name, justification in not_applicable_plans:
            paragraphs.append(f"- {plan_name} — {justification}")

    paragraphs.extend(
        [
            "Responsibilities: Corporate Safety drafts high-risk rescue, fall protection, and electrical energy control plans; the Project Manager assigns authors and "
            "due dates; the SSHO verifies plans are briefed to crews; subcontractors submit discipline-specific plans aligned with the CSP." ,
            "Forms, Logs, or Records: Maintain the plan register, status tracker, and approvals within Appendix 5. Attach completed plans and link them to related AHAs." ,
            _format_references(context["em385_refs"], documents),
        ]
    )
    return paragraphs


def _build_section_12(context: Dict[str, Any]) -> List[str]:
    documents = context["documents"]

    paragraphs = [
        (
            "Purpose: Coordinate safety expectations across prime, subcontractors, suppliers, and government representatives to maintain a unified safety culture."
        ),
        (
            "Procedures / Policy / Requirements: Conduct daily toolbox talks with all employers, weekly coordination meetings, and monthly executive safety reviews. "
            "Share CSP updates, AHAs, inspection findings, and lessons learned via a common document platform. Provide bilingual briefings and translators where required. "
            "Include suppliers in pre-delivery safety planning."
        ),
        (
            "Responsibilities: The prime contractor convenes meetings and maintains minutes; subcontractor competent persons communicate requirements to their crews; "
            "the SSHO tracks corrective actions across employers; the Quality Control Manager ensures subcontractor work plans incorporate CSP controls."
        ),
        (
            "Forms, Logs, or Records: Meeting agendas, attendance rosters, communication logs, and subcontractor acknowledgement forms stored in Appendix 5."
        ),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


def _build_section_13(context: Dict[str, Any]) -> List[str]:
    documents = context["documents"]

    appendices = [
        "Appendix 1: Project Map",
        "Appendix 2: Subcontractor Roster",
        "Appendix 3: Personnel Qualifications",
        "Appendix 4: Activity Hazard Analyses Index",
        "Appendix 5: Site-Specific Plans Register",
        "Appendix 6: Revision Log",
    ]

    paragraphs = [
        (
            "Purpose: Provide a comprehensive index of appendices, revision log, and document control approach for the CSP package."
        ),
        (
            "Procedures / Policy / Requirements: Maintain appendices in the project document management system with version control. Update entries when new sub-plans, "
            "training records, or AHAs are added. Revision changes require date, description, and author initials."
        ),
        (
            "Responsibilities: The Project Administrator manages document control; the SSHO verifies safety content; the Project Manager approves revisions before issue; "
            "subcontractors receive updated appendices and confirm receipt."
        ),
        (
            "Forms, Logs, or Records: Maintain revision log, distribution matrix, and appendix checklist in Appendix 6. Manifest.json captures placeholders requiring update."
        ),
        "Appendices Index: " + "; ".join(appendices),
        _format_references(context["em385_refs"], documents),
    ]
    return paragraphs


SECTION_BUILDERS = {
    "section_01": _build_section_01,
    "section_02": _build_section_02,
    "section_03": _build_section_03,
    "section_04": _build_section_04,
    "section_05": _build_section_05,
    "section_06": _build_section_06,
    "section_07": _build_section_07,
    "section_08": _build_section_08,
    "section_09": _build_section_09,
    "section_10": _build_section_10,
    "section_11": _build_section_11,
    "section_12": _build_section_12,
    "section_13": _build_section_13,
}


def build_csp_sections(
    context_packs: Dict[str, Dict[str, Any]],
) -> List[CspSection]:
    sections: List[CspSection] = []
    for definition in SECTION_DEFINITIONS:
        builder = SECTION_BUILDERS.get(definition.identifier)
        if not builder:
            continue
        context = context_packs.get(definition.identifier, {
            "metadata": {},
            "metadata_sources": [],
            "placeholders": {},
            "documents": [],
            "dfow": [],
            "hazards": [],
            "sub_plans": {},
            "citations": [],
            "em385_refs": definition.em385_refs,
            "snippets": [],
        })
        paragraphs = builder(context)
        citations = generate_section_citations(context)
        sections.append(CspSection(name=definition.title, paragraphs=paragraphs, citations=citations))
    return sections


def assemble_csp_doc(
    metadata: Dict[str, Any],
    context_packs: Dict[str, Dict[str, Any]],
) -> CspDoc:
    placeholders = context_packs.get("section_01", {}).get("placeholders", {})
    project_name = _meta_value(metadata, placeholders, "project_name")
    project_number = metadata.get("project_number", "")
    location = _meta_value(metadata, placeholders, "location")
    owner = _meta_value(metadata, placeholders, "owner")
    prime_contractor = _meta_value(metadata, placeholders, "prime_contractor")

    sections = build_csp_sections(context_packs)

    return CspDoc(
        project_name=project_name,
        project_number=str(project_number) if project_number else "",
        location=location,
        owner=owner,
        general_contractor=prime_contractor,
        sections=sections,
    )


def generate_csp(spec: Dict[str, Any], collection_name: str | None = None) -> CspDoc:
    """Backward-compatible wrapper for legacy callers."""

    metadata = {
        "project_name": spec.get("project_name", ""),
        "project_number": spec.get("project_number", ""),
        "location": spec.get("location", ""),
        "owner": spec.get("owner", ""),
        "prime_contractor": spec.get("gc", spec.get("prime_contractor", "")),
        "project_manager": spec.get("project_manager", ""),
        "ssho": spec.get("ssho", ""),
    }
    placeholders = {
        key: format_placeholder(f"Insert {key.replace('_', ' ').title()}")
        for key, value in metadata.items()
        if not value
    }

    dfow: List[str] = []
    for wp in spec.get("work_packages", []) or []:
        dfow.extend([act for act in wp.get("activities", []) or []])
    if not dfow and spec.get("activities"):
        dfow.extend(spec.get("activities"))
    hazards = spec.get("hazards", [])
    sub_plan_matrix = map_dfow_to_plans(dfow, hazards)

    context_packs: Dict[str, Dict[str, Any]] = {}
    for definition in SECTION_DEFINITIONS:
        context_packs[definition.identifier] = {
            "title": definition.title,
            "em385_refs": definition.em385_refs,
            "description": definition.description,
            "snippets": [],
            "metadata": metadata,
            "metadata_sources": ["legacy-spec"],
            "placeholders": placeholders,
            "dfow": dfow,
            "hazards": hazards,
            "documents": [],
            "sub_plans": sub_plan_matrix if definition.identifier == "section_11" else {},
            "citations": [],
        }

    return assemble_csp_doc(metadata, context_packs)


