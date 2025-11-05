"""Firebase helpers for the Section 11 Generator."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

import importlib

from section11.models import CategoryBundle, CategoryStatus, RunDiagnostics, Section11Run


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_firebase_modules():
    firebase_admin = importlib.import_module("firebase_admin")
    credentials = importlib.import_module("firebase_admin.credentials")
    firestore = importlib.import_module("firebase_admin.firestore")
    storage = importlib.import_module("firebase_admin.storage")
    return firebase_admin, credentials, firestore, storage


def initialize_firestore_app() -> "firestore.Client":
    firebase_admin, credentials, firestore, _ = _load_firebase_modules()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path:
        creds_path = str(_project_root() / "firebase-admin.json")
    creds_path = os.path.abspath(creds_path)
    project_id = (
        os.getenv("FIREBASE_PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(creds_path)
        if project_id:
            firebase_admin.initialize_app(cred, {"projectId": project_id})
        else:
            firebase_admin.initialize_app(cred)
    return firestore.client()


def fetch_code_decisions(db: "firestore.Client", codes: Iterable[str]) -> Dict[str, Dict[str, object]]:
    decisions: Dict[str, Dict[str, object]] = {}
    for code in codes:
        doc = db.collection("decisions").document(code).get()
        if getattr(doc, "exists", False):
            payload = dict(doc.to_dict() or {})
            payload.setdefault("status", "firestore")
            decisions[code] = payload
        else:
            decisions[code] = {"status": "unknown"}
    return decisions


def fetch_code_metadata(db: "firestore.Client", codes: Iterable[str]) -> Dict[str, Dict[str, object]]:
    metadata: Dict[str, Dict[str, object]] = {}
    for code in codes:
        doc = db.collection("codes").document(code).get()
        if getattr(doc, "exists", False):
            metadata[code] = dict(doc.to_dict() or {})
    return metadata


def _bundle_payload(bundle: CategoryBundle) -> Dict[str, object]:
    return {
        "codes": bundle.codes,
        "aha": {
            "status": bundle.aha.status.value,
            "hazards": bundle.aha.hazards,
            "narrative": bundle.aha.narrative,
            "citations": bundle.aha.citations,
            "pending_reason": bundle.aha.pending_reason,
        },
        "plan": {
            "status": bundle.plan.status.value,
            "controls": bundle.plan.controls,
            "ppe": bundle.plan.ppe,
            "permits": bundle.plan.permits,
            "citations": bundle.plan.citations,
            "pending_reason": bundle.plan.pending_reason,
            "project_evidence": bundle.plan.project_evidence,
            "em_evidence": bundle.plan.em_evidence,
        },
    }


def write_run_to_firestore(run: Section11Run, upload_artifacts: bool = False) -> None:
    db = initialize_firestore_app()
    firebase_admin, _, _, storage = _load_firebase_modules()
    run_ref = db.collection("runs").document(run.run_id)
    metrics = {
        "codes_found": len(run.parsed.codes),
        "aha_required": sum(1 for c in run.parsed.codes if c.requires_aha),
        "categories": len(run.bundles),
        "pending_ahas": sum(1 for b in run.bundles if b.aha.status != CategoryStatus.required),
        "pending_plans": sum(1 for b in run.bundles if b.plan.status != CategoryStatus.required),
    }
    run_ref.set(
        {
            "run_id": run.run_id,
            "source_file": str(run.source_file.name),
            "source_hash": _hash_file(run.source_file),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
        },
        merge=True,
    )

    codes_collection = run_ref.collection("codes")
    for code in run.parsed.codes:
        codes_collection.document(code.code).set(
            {
                "code": code.code,
                "title": code.title,
                "requires_aha": code.requires_aha,
                "category": code.suggested_category,
                "sources": [hit.model_dump() for hit in code.sources],
                "decision_source": code.decision_source,
                "confidence": code.confidence,
                "notes": code.notes,
            },
            merge=True,
        )

    categories_collection = run_ref.collection("categories")
    for bundle in run.bundles:
        categories_collection.document(bundle.category).set(_bundle_payload(bundle), merge=True)

    overrides_collection = run_ref.collection("overrides")
    for override in run.diagnostics.overrides:
        key = f"{override['code']}->{override['category']}"
        overrides_collection.document(key).set(override, merge=True)

    artifacts_collection = run_ref.collection("artifacts")
    artifacts_collection.document("section11").set(
        {
            "markdown": str(run.artifacts.markdown_path),
            "docx": str(run.artifacts.docx_path),
            "json": str(run.artifacts.json_report_path),
            "manifest": str(run.artifacts.manifest_path),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        merge=True,
    )

    if upload_artifacts:
        bucket = storage.bucket()
        base = run.artifacts.base_dir
        for path in [
            run.artifacts.markdown_path,
            run.artifacts.docx_path,
            run.artifacts.json_report_path,
            run.artifacts.manifest_path,
        ]:
            if not path.exists():
                continue
            blob = bucket.blob(f"runs/{run.run_id}/{path.relative_to(base)}")
            blob.upload_from_filename(str(path))


def _hash_file(path: Path) -> str:
    import hashlib

    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_manifest(run: Section11Run) -> Path:
    manifest = {
        "run_id": run.run_id,
        "source_file": run.source_file.name,
        "artifacts": {
            "markdown": str(run.artifacts.markdown_path.name),
            "docx": str(run.artifacts.docx_path.name),
            "json": str(run.artifacts.json_report_path.name),
        },
        "categories": {
            bundle.category: _bundle_payload(bundle)
            for bundle in run.bundles
        },
    }
    run.artifacts.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return run.artifacts.manifest_path


def build_diagnostics(run: Section11Run) -> RunDiagnostics:
    summary = {
        "codes_found": len(run.parsed.codes),
        "aha_required": sum(1 for code in run.parsed.codes if code.requires_aha),
        "categories": len(run.bundles),
    }
    codes = {
        code.code: {
            "requires_aha": code.requires_aha,
            "category": code.suggested_category,
            "decision_source": code.decision_source,
        }
        for code in run.parsed.codes
    }
    categories = {
        bundle.category: {
            "codes": bundle.codes,
            "aha_status": bundle.aha.status.value,
            "plan_status": bundle.plan.status.value,
        }
        for bundle in run.bundles
    }
    return RunDiagnostics(
        run_id=run.run_id,
        summary=summary,
        codes=codes,
        categories=categories,
        overrides=run.diagnostics.overrides,
    )

