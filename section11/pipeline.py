"""High-level orchestration for the Section 11 Generator."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from section11.firebase_service import (
    fetch_code_decisions,
    fetch_code_metadata,
    initialize_firestore_app,
    write_manifest,
    write_run_to_firestore,
)
from section11.generator import build_category_bundles, ensure_categories
from section11.models import (
    CategoryAssignment,
    CategoryBundle,
    CategoryStatus,
    ComplianceMatrixRow,
    ParsedCode,
    ParsedSpec,
    RunDiagnostics,
    Section11Artifacts,
    Section11Run,
)
from section11.parser import parse_spec, parse_document_text
from section11.writer import (
    allocate_artifacts,
    write_bundle_markdown,
    write_section11_docx,
    write_section11_json,
    write_section11_markdown,
)


@dataclass
class Section11Context:
    run_id: str
    work_dir: Path
    collection_name: Optional[str] = None


@dataclass
class PreparedSection11:
    """Container for pre-generation Section 11 assets."""

    context: Section11Context
    source_path: Path
    collection_name: Optional[str]
    parsed_all_codes: ParsedSpec
    parsed_for_generation: ParsedSpec
    assignments: List[CategoryAssignment]
    document_context: List[str]
    verification: Dict[str, object]
    firebase_results: Dict[str, List[str]]
    rag_codes: List[str]
    parser_codes: List[str]
    combined_codes: List[str]
    codes_to_process: List[str]
    overrides: Dict[str, str]


def _timestamped_run_id(prefix: str = "section11") -> str:
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    suffix = os.urandom(4).hex()
    return f"{prefix}-{now}-{suffix}"


def _resolve_work_dir(base: Optional[Path] = None) -> Path:
    root = base or Path("uploads") / "section11"
    root.mkdir(parents=True, exist_ok=True)
    return root


def create_context(collection_name: Optional[str] = None) -> Section11Context:
    run_id = _timestamped_run_id()
    work_dir = _resolve_work_dir()
    return Section11Context(run_id=run_id, work_dir=work_dir, collection_name=collection_name)


def persist_uploaded_file(context: Section11Context, file_name: str, data: bytes) -> Path:
    dest_dir = context.work_dir / context.run_id / "source"
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / file_name
    path.write_bytes(data)
    return path


def parse_and_detect(path: Path, context: Section11Context) -> ParsedSpec:
    parsed = parse_spec(path, context.work_dir / context.run_id / "parse")
    return parsed


def _extract_title_from_metadata(payload: Dict[str, object]) -> str:
    """Best-effort extraction of a readable UFGS title from Firestore metadata."""
    import xml.etree.ElementTree as ET

    def _from_xml(raw: str) -> str:
        try:
            root = ET.fromstring(raw)
        except Exception:
            return ""

        preferred_tags = {"STL", "TTL", "TITLE", "TITLE1", "TITLE2"}
        fallback_text: Optional[str] = None

        for node in root.iter():
            text = (node.text or "").strip()
            if not text:
                continue

            tag = node.tag.split("}")[-1].upper()  # Strip namespaces
            style = (node.attrib.get("Style") or node.attrib.get("STYLE") or "").upper()
            if tag in preferred_tags or style == "TITLE":
                return text

            # Capture the first meaningful string as a fallback (e.g., section numbers)
            if fallback_text is None:
                fallback_text = text

        return fallback_text or ""

    candidates = [
        str(payload.get("title") or "").strip(),
        str(payload.get("text") or "").strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if candidate.lstrip().startswith("<?xml"):
            extracted = _from_xml(candidate)
            if extracted:
                return extracted
        else:
            return candidate
    return ""


def enrich_codes_with_firestore(parsed: ParsedSpec) -> ParsedSpec:
    db = initialize_firestore_app()
    codes = [code.code for code in parsed.codes]
    decisions = fetch_code_decisions(db, codes)
    metadata = fetch_code_metadata(db, codes)
    for code in parsed.codes:
        decision = decisions.get(code.code, {})
        code.requires_aha = bool(decision.get("requiresAha")) if "requiresAha" in decision else None
        code.decision_source = str(decision.get("status") or "unknown")
        code.confidence = float(decision.get("confidence", 0.0)) if "confidence" in decision else None
        code.rationale = str(decision.get("rationale") or "")
        code.notes = str(decision.get("notes") or "")
        if code.code in metadata:
            code.title = str(metadata[code.code].get("title") or "")
            if code.title.startswith("<?xml"):
                extracted_title = _extract_title_from_metadata(metadata[code.code])
                if extracted_title:
                    code.title = extracted_title
            elif not code.title:
                extracted_title = _extract_title_from_metadata(metadata[code.code])
                if extracted_title:
                    code.title = extracted_title
            if not code.suggested_category:
                code.suggested_category = str(metadata[code.code].get("category") or "")
    return parsed


def build_assignments(parsed: ParsedSpec) -> List[CategoryAssignment]:
    assignments: List[CategoryAssignment] = []
    # First pass: use existing suggested categories
    for code in parsed.codes:
        category = code.suggested_category or ""
        # If no category suggested, try to infer from code context
        if not category and code.sources:
            # Use the excerpt to infer category
            excerpt = " ".join(s.excerpt for s in code.sources[:3])
            category = _infer_category_from_context(code.code, excerpt)
        # If still no category, use a default category based on code pattern
        if not category:
            category = _infer_category_from_code_pattern(code.code)
        assignments.append(
            CategoryAssignment(
                code=code.code,
                suggested_category=category or "Unmapped",
                why=(code.rationale or ""),
            )
        )
    return assignments


def _infer_category_from_context(code: str, context: str) -> str:
    """Infer category from code context text using keyword matching."""
    from section11.constants import DEFAULT_CATEGORY_BY_KEYWORD
    haystack = f"{code} {context}".lower()
    for keyword, category in DEFAULT_CATEGORY_BY_KEYWORD.items():
        if keyword in haystack:
            return category
    return ""


def _infer_category_from_code_pattern(code: str) -> str:
    """Infer category from code number pattern (e.g., electrical codes often in specific ranges)."""
    # Extract numeric part from code like "385-11-1" or "385-11" or "385-1.1"
    import re
    # Match first number after 385- (handles 385-11-1, 385-11, 385-1.1, etc.)
    match = re.search(r"385-(\d+)", code)
    if not match:
        return ""
    
    try:
        section_num = int(match.group(1))
    except ValueError:
        return ""
    
    # Map common EM 385 section numbers to categories
    # These are heuristic mappings based on typical EM 385 structure
    if section_num == 1 or (section_num >= 11 and section_num <= 15):
        return "Electrical / Energy Control"
    elif section_num == 21:
        return "Fall Protection & Prevention"
    elif section_num == 22:
        return "Excavation & Trenching"
    elif section_num == 23:
        return "Confined Space Entry"
    elif section_num == 24:
        return "Cranes & Rigging"
    elif section_num == 25:
        return "Demolition"
    elif section_num == 10:
        return "Material Handling & Storage"
    elif section_num == 12:
        return "Fire Prevention & Hot Work"
    elif section_num == 13:
        return "Scaffolding & Access Systems"
    elif section_num == 15:
        return "Hazardous Energy / LOTO"
    elif section_num == 6:
        return "Environmental Controls"
    elif section_num == 7:
        return "Mechanical Equipment"
    elif section_num == 8:
        return "Structural Work"
    elif section_num == 5:
        return "Electrical / Energy Control"
    return ""


def apply_overrides(assignments: List[CategoryAssignment], overrides: Dict[str, str]) -> None:
    for assignment in assignments:
        if assignment.code in overrides:
            assignment.override = overrides[assignment.code]


def reconcile_categories(parsed: ParsedSpec, assignments: List[CategoryAssignment]) -> None:
    category_by_code = {assignment.code: assignment.effective_category for assignment in assignments}
    for code in parsed.codes:
        effective = category_by_code.get(code.code)
        if effective:
            code.suggested_category = effective


def build_matrix(bundles: List[CategoryBundle]) -> List[ComplianceMatrixRow]:
    rows: List[ComplianceMatrixRow] = []
    for bundle in bundles:
        rows.append(
            ComplianceMatrixRow(
                category=bundle.category,
                codes=bundle.codes,
                aha_status=bundle.aha.status,
                plan_status=bundle.plan.status,
                project_evidence_count=len(bundle.plan.project_evidence),
                em_evidence_count=len(bundle.plan.em_evidence),
            )
        )
    return rows


def save_artifacts(
    run_id: str,
    source_file: Path,
    parsed: ParsedSpec,
    bundles: List[CategoryBundle],
    matrix: List[ComplianceMatrixRow],
    base_dir: Path,
) -> Section11Artifacts:
    artifacts = allocate_artifacts(base_dir)
    markdown_path = write_section11_markdown(base_dir, bundles, matrix)
    docx_path = write_section11_docx(base_dir, bundles, matrix)
    json_path = write_section11_json(base_dir, Section11Run(
        run_id=run_id,
        source_file=source_file,
        parsed=parsed,
        assignments=[],
        bundles=bundles,
        matrix=matrix,
        artifacts=artifacts,
        diagnostics=RunDiagnostics(run_id=run_id),
    ))
    artifacts.markdown_path = markdown_path
    artifacts.docx_path = docx_path
    artifacts.json_report_path = json_path
    artifacts.aha_markdown_paths = {}
    artifacts.plan_markdown_paths = {}
    aha_dir = base_dir / "ahas"
    plan_dir = base_dir / "plans"
    for bundle in bundles:
        if not bundle.codes:
            continue
        aha_path = write_bundle_markdown(aha_dir, bundle, "aha")
        plan_path = write_bundle_markdown(plan_dir, bundle, "plan")
        artifacts.aha_markdown_paths[bundle.category] = aha_path
        artifacts.plan_markdown_paths[bundle.category] = plan_path
    return artifacts


def _get_document_context(parsed: ParsedSpec) -> List[str]:
    """Extract meaningful context from the full document for RAG queries."""
    context = []
    
    # Load full document text if available
    full_text = ""
    if parsed.raw_text_path and parsed.raw_text_path.exists():
        try:
            import json
            data = json.loads(parsed.raw_text_path.read_text(encoding="utf-8"))
            full_text = data.get("text", "")
        except Exception as e:
            print(f"[_get_document_context] Could not load full document: {e}")
    
    # Use actual scope if found
    if parsed.scope_summary:
        context.extend(parsed.scope_summary)
    
    # Extract context from code sources (actual document excerpts)
    code_contexts = []
    for code in parsed.codes:
        if code.sources:
            for source in code.sources[:2]:  # Use first 2 sources per code
                excerpt = source.excerpt.strip()
                if excerpt and len(excerpt) > 20:  # Meaningful excerpts only
                    code_contexts.append(excerpt)
    
    # Add unique code contexts
    seen = set()
    for ctx in code_contexts:
        ctx_lower = ctx.lower().strip()
        if ctx_lower and ctx_lower not in seen:
            seen.add(ctx_lower)
            context.append(ctx)
            if len(context) >= 15:  # Limit context size
                break
    
    # If we have full text but limited context, use chunks of full text
    if full_text and len(context) < 5:
        # Use first 2000 chars of document as additional context
        text_chunks = [full_text[i:i+500] for i in range(0, min(2000, len(full_text)), 500)]
        context.extend(text_chunks[:3])
    
    return context


def prepare_section11(
    source_path: Path,
    context: Section11Context,
    collection_name: Optional[str],
    overrides: Optional[Dict[str, str]] = None,
) -> PreparedSection11:
    """Parse document, determine AHA requirements, and prepare data for generation."""
    overrides = overrides or {}

    from section11.rag_code_extractor import (
        check_codes_against_firebase,
        extract_codes_with_rag,
        verify_document_parsing,
    )

    print(f"[prepare_section11] Loading document text for {source_path.name}...")
    document_text = parse_document_text(source_path)
    parser_results = parse_and_detect(source_path, context)

    rag_codes = extract_codes_with_rag(document_text, collection_name or "")
    parser_codes = [code.code for code in parser_results.codes]

    combined_codes = sorted({code for code in (rag_codes + parser_codes) if code.startswith("UFGS-")})
    verification = verify_document_parsing(document_text, combined_codes)

    print(f"[prepare_section11] Checking Firebase decisions for {len(combined_codes)} codes...")
    firebase_results = check_codes_against_firebase(combined_codes)
    codes_requiring = firebase_results.get("codes_requiring_aha", [])
    codes_unknown = firebase_results.get("codes_unknown", [])
    codes_to_process = sorted(set(codes_requiring + codes_unknown))

    # Preserve original parsed data (all codes) for UI/diagnostics
    parsed_all = parser_results.model_copy(deep=True)

    # Ensure codes discovered via Firebase/RAG but not parser are tracked
    existing_codes = {code.code for code in parsed_all.codes}
    for code_value in combined_codes:
        if code_value not in existing_codes:
            parsed_all.codes.append(ParsedCode(code=code_value))
            existing_codes.add(code_value)

    # Enrich with Firebase metadata before filtering
    parsed_all = enrich_codes_with_firestore(parsed_all)

    # Mark requires_aha flags for all codes
    for code in parsed_all.codes:
        if code.code in codes_requiring:
            code.requires_aha = True
        elif code.code in codes_unknown:
            code.requires_aha = True
        else:
            code.requires_aha = False

    # Build filtered spec for generation (only codes requiring AHA or unknown)
    filtered_codes: List[ParsedCode] = []
    for code in parsed_all.codes:
        if code.requires_aha:
            filtered_codes.append(code.model_copy(deep=True))

    parsed_for_generation = ParsedSpec(
        scope_summary=list(parsed_all.scope_summary),
        codes=filtered_codes,
        hazard_phrases=list(parsed_all.hazard_phrases),
        raw_text_path=parsed_all.raw_text_path,
    )

    assignments = build_assignments(parsed_for_generation)
    apply_overrides(assignments, overrides)
    reconcile_categories(parsed_for_generation, assignments)

    # Ensure every code has a concrete category
    for code_obj in parsed_for_generation.codes:
        if not code_obj.suggested_category or not code_obj.suggested_category.strip():
            code_obj.suggested_category = "Unmapped"

    document_context = _get_document_context(parsed_for_generation)
    scope_text = " ".join(document_context) if document_context else ""

    # Attempt proactive RAG grouping so the UI starts with mapped categories
    if collection_name and parsed_for_generation.codes:
        try:
            from section11.rag_category_grouper import group_codes_with_rag

            code_list = [code.code for code in parsed_for_generation.codes]
            rag_grouped = group_codes_with_rag(code_list, scope_text, collection_name)
            code_to_category = {}
            for category, grouped_codes in rag_grouped.items():
                for grouped_code in grouped_codes:
                    code_to_category[grouped_code] = category

            if code_to_category:
                for code_obj in parsed_for_generation.codes:
                    category = code_to_category.get(code_obj.code)
                    if category:
                        code_obj.suggested_category = category

                # Keep original parsed list in sync for diagnostics/UI
                for code_obj in parsed_all.codes:
                    if code_obj.requires_aha and code_obj.code in code_to_category:
                        code_obj.suggested_category = code_to_category[code_obj.code]

                # Refresh assignments and apply overrides to reflect newly inferred categories
                assignments = build_assignments(parsed_for_generation)
                apply_overrides(assignments, overrides)
                reconcile_categories(parsed_for_generation, assignments)
                reconcile_categories(parsed_all, assignments)
        except Exception as exc:  # pragma: no cover - defensive logging only
            print(f"[prepare_section11] WARNING: RAG grouping failed: {exc}")

    return PreparedSection11(
        context=context,
        source_path=source_path,
        collection_name=collection_name,
        parsed_all_codes=parsed_all,
        parsed_for_generation=parsed_for_generation,
        assignments=assignments,
        document_context=document_context,
        verification=verification,
        firebase_results=firebase_results,
        rag_codes=sorted(rag_codes),
        parser_codes=sorted(parser_codes),
        combined_codes=combined_codes,
        codes_to_process=codes_to_process,
        overrides=dict(overrides),
    )


def run_pipeline(
    source_path: Path,
    collection_name: Optional[str] = None,
    overrides: Optional[Dict[str, str]] = None,
    upload_artifacts: bool = False,
) -> Section11Run:
    """Run the full pipeline: parse document, extract codes with RAG, check Firebase, generate AHAs/Plans."""
    run_id = _timestamped_run_id()
    work_dir = _resolve_work_dir()
    context = Section11Context(run_id=run_id, work_dir=work_dir, collection_name=collection_name)
    
    print(f"[run_pipeline] Step 1: Parsing document {source_path.name}...")
    prepared = prepare_section11(
        source_path=source_path,
        context=context,
        collection_name=collection_name,
        overrides=overrides,
    )

    verification = prepared.verification
    if not verification["valid"]:
        print(f"[run_pipeline] WARNING: Document parsing issues: {verification['issues']}")
        if not verification["has_content"]:
            raise ValueError(
                f"Document appears to be empty or could not be parsed properly. Issues: {verification['issues']}"
            )

    print(
        f"[run_pipeline] Document parsed: {verification['document_length']} chars, has_content={verification['has_content']}"
    )
    if verification["document_length"] < 100:
        print(
            f"[run_pipeline] WARNING: Document is very short ({verification['document_length']} chars) - may not contain enough content"
        )

    print("[run_pipeline] Step 2: Extracting UFGS codes from document...")
    print(
        f"[run_pipeline] Found {len(prepared.combined_codes)} total UFGS codes: "
        f"RAG={len(prepared.rag_codes)}, Parser={len(prepared.parser_codes)}"
    )
    print(f"[run_pipeline] Extracted codes: {prepared.combined_codes}")

    print("[run_pipeline] Step 3: Checking codes against Firebase...")
    firebase_results = prepared.firebase_results

    codes_requiring_aha = firebase_results.get("codes_requiring_aha", [])
    codes_not_requiring = firebase_results.get("codes_not_requiring", [])
    codes_unknown = firebase_results.get("codes_unknown", [])
    
    print(f"[run_pipeline] ========================================")
    print(f"[run_pipeline] CODE ANALYSIS RESULTS:")
    print(f"[run_pipeline] Total codes found: {len(prepared.combined_codes)}")
    print(f"[run_pipeline] Codes requiring AHA: {len(codes_requiring_aha)}")
    print(f"[run_pipeline] Codes NOT requiring AHA: {len(codes_not_requiring)}")
    print(f"[run_pipeline] Codes with unknown status: {len(codes_unknown)}")
    print(f"[run_pipeline] ========================================")
    print(f"[run_pipeline] CODES REQUIRING AHA (will be processed):")
    for code in codes_requiring_aha:
        print(f"[run_pipeline]   ✓ {code}")
    if codes_not_requiring:
        print(f"[run_pipeline] Codes NOT requiring AHA (skipped):")
        for code in codes_not_requiring[:10]:  # Show first 10
            print(f"[run_pipeline]   - {code}")
        if len(codes_not_requiring) > 10:
            print(f"[run_pipeline]   ... and {len(codes_not_requiring) - 10} more")
    if codes_unknown:
        print(f"[run_pipeline] Codes with unknown status (will be processed):")
        for code in codes_unknown:
            print(f"[run_pipeline]   ? {code}")
    print(f"[run_pipeline] ========================================")
    
    parsed = prepared.parsed_for_generation
    assignments = prepared.assignments

    print(f"[run_pipeline] After Firebase enrichment: {len(parsed.codes)} codes ready for processing")
    for code_obj in parsed.codes:
        print(
            f"[run_pipeline]   Code: {code_obj.code}, requires_aha={code_obj.requires_aha}, "
            f"category={code_obj.suggested_category or 'Unmapped'}"
        )
    
    # Ensure all codes have categories - assign to Unmapped if missing
    for code_obj in parsed.codes:
        if not code_obj.suggested_category or code_obj.suggested_category.strip() == "":
            code_obj.suggested_category = "Unmapped"
            print(f"[run_pipeline] Assigned 'Unmapped' category to code {code_obj.code}")
    
    print(f"[run_pipeline] Final codes before grouping: {len(parsed.codes)}")
    for code_obj in parsed.codes:
        print(f"[run_pipeline]   {code_obj.code}: requires_aha={code_obj.requires_aha}, category={code_obj.suggested_category}")
    
    # Use actual document context instead of generic scope summaries
    document_context = prepared.document_context
    scope_text = " ".join(document_context) if document_context else ""
    
    print(f"[run_pipeline] Step 4: Using RAG to intelligently group codes into categories...")
    # Use RAG to intelligently group codes based on their actual meaning and similarity
    from section11.rag_category_grouper import group_codes_with_rag
    
    codes_requiring_aha_list = [code.code for code in parsed.codes if code.requires_aha]
    if codes_requiring_aha_list:
        rag_grouped = group_codes_with_rag(
            codes_requiring_aha_list,
            scope_text,
            context.collection_name or "",
        )
        
        # Update code categories based on RAG grouping
        code_to_category = {}
        for category, code_list in rag_grouped.items():
            for code in code_list:
                code_to_category[code] = category
        
        # Update parsed codes with RAG-determined categories
        for code_obj in parsed.codes:
            if code_obj.code in code_to_category:
                code_obj.suggested_category = code_to_category[code_obj.code]
                print(f"[run_pipeline]   Code {code_obj.code} assigned to category: {code_obj.suggested_category}")
    
    print(f"[run_pipeline] Step 5: Generating AHAs for {len(parsed.codes)} codes requiring AHA...")
    bundles = build_category_bundles(parsed.codes, document_context, context.collection_name)
    bundles = ensure_categories(bundles)
    matrix = build_matrix(bundles)
    output_dir = work_dir / run_id / "artifacts"
    artifacts = save_artifacts(run_id, source_path, parsed, bundles, matrix, output_dir)
    run = Section11Run(
        run_id=run_id,
        source_file=source_path,
        parsed=parsed,
        assignments=assignments,
        bundles=bundles,
        matrix=matrix,
        artifacts=artifacts,
        diagnostics=RunDiagnostics(run_id=run_id, overrides=[{"code": code, "category": cat} for code, cat in (overrides or {}).items()]),
    )
    write_manifest(run)
    if upload_artifacts:
        write_run_to_firestore(run, upload_artifacts=True)
    
    print(f"[run_pipeline] Complete! Generated run {run_id}")
    return run

