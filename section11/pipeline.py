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
from section11.parser import parse_spec
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
            if not code.suggested_category:
                code.suggested_category = str(metadata[code.code].get("category") or "")
    return parsed


def build_assignments(parsed: ParsedSpec) -> List[CategoryAssignment]:
    assignments: List[CategoryAssignment] = []
    for code in parsed.codes:
        assignments.append(
            CategoryAssignment(
                code=code.code,
                suggested_category=code.suggested_category or "Unmapped",
                why=(code.rationale or ""),
            )
        )
    return assignments


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


def run_pipeline(
    source_path: Path,
    collection_name: Optional[str] = None,
    overrides: Optional[Dict[str, str]] = None,
    upload_artifacts: bool = False,
) -> Section11Run:
    run_id = _timestamped_run_id()
    work_dir = _resolve_work_dir()
    context = Section11Context(run_id=run_id, work_dir=work_dir, collection_name=collection_name)
    parsed = parse_and_detect(source_path, context)
    parsed = enrich_codes_with_firestore(parsed)
    assignments = build_assignments(parsed)
    apply_overrides(assignments, overrides or {})
    reconcile_categories(parsed, assignments)
    bundles = build_category_bundles(parsed.codes, parsed.scope_summary, context.collection_name)
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
    return run

