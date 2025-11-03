from __future__ import annotations

"""Document ingestion helpers for CSP pipeline Phase 1."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from generators.analyze import analyze_scope
from pipelines.csp_pipeline import DocumentIngestionResult
from context.document_sanitizer import sanitize_document_text
from context.project_metadata_extractor import extract_title_block_fields, BANNED_SUBSTRINGS


EM385_CODE_RE = re.compile(r"385[-\s]?([0-9]{1,4}(?:\.[0-9]{1,4})*)", re.IGNORECASE)


@dataclass(slots=True)
class ParsedDocument:
    path: Path
    text: str
    metadata: Dict[str, str]


def _read_pdf_text(path: Path, ocr_threshold: int, diag_dir: Optional[Path]) -> str:
    try:
        from pdf_loader.pdf_text import extract_text
        pages = extract_text(
            path,
            include_tables=True,
            diagnostic_dir=(diag_dir / "text") if diag_dir else None,
        )
        combined = "\n\n".join([pages[k] for k in sorted(pages.keys())]) if pages else ""
        if len(combined) >= max(0, ocr_threshold):
            return combined
    except Exception:
        combined = ""

    # OCR fallback
    try:
        from pdf_loader import process_pdf

        tmp_json = (diag_dir / "chunks.json") if diag_dir else (path.with_suffix(".ocr.json"))
        tmp_imgdir = (diag_dir / "images") if diag_dir else (path.parent / f"{path.stem}_images")
        chunks = process_pdf(path, tmp_json, tmp_imgdir, diagnostic_dir=diag_dir)
        return "\n\n".join([str(c.get("text", "")) for c in chunks])
    except Exception:
        return combined


def _read_docx_text(path: Path) -> str:
    try:
        from docx import Document  # type: ignore
    except Exception:
        return ""

    try:
        doc = Document(str(path))
    except Exception:
        return ""

    parts: List[str] = []
    for p in getattr(doc, "paragraphs", []) or []:
        text = (p.text or "").strip()
        if text:
            parts.append(text)
    try:
        for table in getattr(doc, "tables", []) or []:
            for row in table.rows:
                for cell in row.cells:
                    text = (cell.text or "").strip()
                    if text:
                        parts.append(text)
    except Exception:
        pass
    return "\n".join(parts)


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return path.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""


def _extract_project_metadata(text: str) -> Dict[str, str]:
    """Extract project metadata using structured cover-page heuristics."""

    extracted = extract_title_block_fields(text)

    sanitized: Dict[str, str] = {}
    for key, value in extracted.items():
        if not value:
            continue
        if any(bad in value.lower() for bad in BANNED_SUBSTRINGS):
            continue
        sanitized[key] = value

    return sanitized


def _extract_em385_references(text: str) -> List[str]:
    refs: List[str] = []
    seen = set()
    for match in EM385_CODE_RE.finditer(text or ""):
        code = match.group(1)
        token = f"EM385-{code.upper()}"
        if token not in seen:
            seen.add(token)
            refs.append(token)
    return refs


def _aggregate_metadata(candidates: Iterable[Dict[str, str]]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for candidate in candidates:
        for key, value in candidate.items():
            if value and not merged.get(key):
                merged[key] = value
    return merged


class DocumentIngestionEngine:
    """Parses source documents and extracts CSP-relevant context."""

    def __init__(
        self,
        ocr_threshold: int = 200,
        diagnostics_base_dir: Optional[Path] = None,
    ) -> None:
        self.ocr_threshold = ocr_threshold
        self.diagnostics_base_dir = diagnostics_base_dir or Path("logs/csp_pipeline/ingestion")

    def ingest(self, document_paths: List[str], run_id: str) -> DocumentIngestionResult:
        parsed_docs: List[ParsedDocument] = []
        logs: List[str] = []
        diagnostics_dir = self.diagnostics_base_dir / run_id
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        metadata_files: List[Path] = []

        for path_str in document_paths:
            path = Path(path_str).expanduser().resolve()
            if not path.exists():
                logs.append(f"Document not found: {path}")
                continue

            text = ""
            if path.suffix.lower() == ".pdf":
                text = _read_pdf_text(path, self.ocr_threshold, diagnostics_dir / path.stem)
            elif path.suffix.lower() == ".docx":
                text = _read_docx_text(path)
            else:
                text = _read_text_file(path)
            
            # Sanitize: remove TOC, headers/footers, boilerplate
            sanitized_text = sanitize_document_text(text)
            logs.append(f"Sanitized document: {path.name} ({len(text)} -> {len(sanitized_text)} chars)")

            metadata = _extract_project_metadata(sanitized_text)
            parsed_docs.append(ParsedDocument(path=path, text=sanitized_text, metadata=metadata))
            logs.append(f"Parsed document: {path.name} ({len(text)} chars)")

            for candidate_name in ("project_meta.json", "manifest.json"):
                candidate = path.parent / candidate_name
                if (
                    candidate.exists()
                    and candidate.suffix.lower() == ".json"
                    and candidate not in metadata_files
                ):
                    metadata_files.append(candidate)

        combined_text = "\n\n".join(doc.text for doc in parsed_docs)
        metadata_candidates = _aggregate_metadata(doc.metadata for doc in parsed_docs)

        for meta_path in metadata_files:
            if meta_path.suffix.lower() != ".json":
                continue
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                logs.append(f"Failed to parse metadata file: {meta_path}")
                continue
            if isinstance(data, dict):
                payload = data.get("project_meta") if isinstance(data.get("project_meta"), dict) else data
                for key, value in payload.items():
                    if isinstance(value, str) and value and not metadata_candidates.get(key):
                        metadata_candidates[key] = value

        analysis = analyze_scope(combined_text)
        citations = _extract_em385_references(combined_text)

        try:
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            (diagnostics_dir / "combined_text.txt").write_text(combined_text, encoding="utf-8")
            (diagnostics_dir / "metadata_candidates.json").write_text(
                json.dumps(metadata_candidates, indent=2),
                encoding="utf-8",
            )
            (diagnostics_dir / "analysis.json").write_text(
                json.dumps(analysis, indent=2),
                encoding="utf-8",
            )
        except Exception:
            # Best effort diagnostics; ignore filesystem errors.
            pass

        return DocumentIngestionResult(
            documents=[str(doc.path) for doc in parsed_docs],
            extracted_text=combined_text,
            metadata_candidates=metadata_candidates,
            metadata_files=[str(p) for p in metadata_files],
            dfow=list(analysis.get("activities", [])),
            hazards=list(analysis.get("hazards", [])),
            citations=citations,
            logs=logs,
        )


__all__ = ["DocumentIngestionEngine", "ParsedDocument"]

