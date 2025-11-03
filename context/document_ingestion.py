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
    """Extract project metadata using comprehensive pattern matching and context awareness."""
    
    # Extended pattern dictionary with multiple variations
    meta_patterns = {
        "project_name": [
            r"(?:^|\n)\s*(?:project\s+name|project\s+title|project)[\s:—-]+([^\n:—-]{2,100})",
            r"project[\s:—-]+([a-z0-9\s&,\.\-]{5,100})",
            r"job\s+name[\s:—-]+([^\n]{2,100})",
        ],
        "project_number": [
            r"(?:^|\n)\s*(?:project\s+number|project\s+no\.?|project\s+#|job\s+number|contract\s+number)[\s:—-]+([a-z0-9\-]{1,50})",
            r"#[\s:—-]+([a-z0-9\-]{1,50})",
        ],
        "location": [
            r"(?:^|\n)\s*(?:location|site\s+location|project\s+location|address)[\s:—-]+([^\n:—-]{3,200})",
            r"located\s+at[\s:—-]+([^\n]{3,200})",
            r"at[\s:—-]+([a-z0-9\s,\.\-]{5,200})\s+(?:for|contract|project)",
        ],
        "owner": [
            r"(?:^|\n)\s*(?:owner|project\s+owner|client|agency|federal\s+agency)[\s:—-]+([^\n:—-]{2,150})",
            r"(?:prepared\s+for|contract\s+with)[\s:—-]+([^\n]{2,150})",
        ],
        "prime_contractor": [
            r"(?:^|\n)\s*(?:prime\s+contractor|general\s+contractor|gc|contractor)[\s:—-]+([^\n:—-]{2,150})",
            r"(?:prepared\s+by|contractor\s+name)[\s:—-]+([^\n]{2,150})",
        ],
        "ssho": [
            r"(?:^|\n)\s*(?:ssho|site\s+safety\s+and\s+health\s+officer|safety\s+officer)[\s:—-]+([^\n:—-]{2,100})",
            r"(?:safety\s+officer|ssho)[\s:—-]+([a-z\s,\.]{2,100})",
        ],
        "project_manager": [
            r"(?:^|\n)\s*(?:project\s+manager|pm|program\s+manager)[\s:—-]+([^\n:—-]{2,100})",
            r"(?:project\s+manager|pm)[\s:—-]+([a-z\s,\.]{2,100})",
        ],
        "corporate_safety_officer": [
            r"(?:^|\n)\s*(?:corporate\s+safety\s+officer|cso|corporate\s+safety)[\s:—-]+([^\n:—-]{2,100})",
        ],
        "quality_control_manager": [
            r"(?:^|\n)\s*(?:quality\s+control\s+manager|qc\s+manager|qcm|quality\s+manager)[\s:—-]+([^\n:—-]{2,100})",
        ],
        "superintendent": [
            r"(?:^|\n)\s*(?:superintendent|field\s+superintendent)[\s:—-]+([^\n:—-]{2,100})",
        ],
        "foreman": [
            r"(?:^|\n)\s*(?:foreman|field\s+supervisor)[\s:—-]+([^\n:—-]{2,100})",
        ],
        "contract_number": [
            r"(?:^|\n)\s*(?:contract\s+number|contract\s+no\.?|contract\s+#)[\s:—-]+([a-z0-9\-]{1,50})",
        ],
        "start_date": [
            r"(?:^|\n)\s*(?:start\s+date|notice\s+to\s+proceed|ntp)[\s:—-]+([^\n:—-]{5,50})",
        ],
        "completion_date": [
            r"(?:^|\n)\s*(?:completion\s+date|substantial\s+completion)[\s:—-]+([^\n:—-]{5,50})",
        ],
    }

    results: Dict[str, str] = {}
    text_lower = text.lower()
    
    # First pass: strict pattern matching
    for key, patterns in meta_patterns.items():
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                value = match.group(1).strip()
                # Clean up common prefixes/suffixes
                value = re.sub(r'^(?:the|a|an)\s+', '', value, flags=re.IGNORECASE).strip()
                value = re.sub(r'[.,;:]+$', '', value).strip()
                
                # Validate extracted value
                if len(value) >= 2 and len(value) <= 200:
                    # Check if it's not just a label/header
                    if not re.match(r'^(project|location|owner|contractor|manager|officer)[\s:]*$', value, re.IGNORECASE):
                        results[key] = value
                        break
            if results.get(key):
                break
    
    # Second pass: context-aware extraction (search for names near role keywords)
    if not results.get("ssho"):
        ssho_patterns = [
            r"(?:ssho|site\s+safety)[\s:—-]+(?:is|name|assigned\s+to|contact)[\s:—-]+([a-z][a-z\s,\.]{5,80})",
            r"([a-z][a-z\s,\.]{5,80})[\s,]+(?:is\s+the|as\s+the)\s+ssho",
        ]
        for pattern in ssho_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if len(name.split()) <= 5:  # Reasonable name length
                    results["ssho"] = name
                    break
    
    if not results.get("project_manager"):
        pm_patterns = [
            r"(?:project\s+manager|pm)[\s:—-]+(?:is|name)[\s:—-]+([a-z][a-z\s,\.]{5,80})",
            r"([a-z][a-z\s,\.]{5,80})[\s,]+(?:is\s+the|as)\s+project\s+manager",
        ]
        for pattern in pm_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if len(name.split()) <= 5:
                    results["project_manager"] = name
                    break
    
    # Third pass: extract from document headers/first 2000 chars (most likely location for metadata)
    header_text = text[:2000]
    lines = header_text.splitlines()[:50]  # First 50 lines
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 3:
            continue
        
        # Look for structured data (key-value pairs)
        for separator in [":", "-", "—", "–"]:
            if separator in line and len(line.split(separator)) == 2:
                key_part, value_part = line.split(separator, 1)
                key_lower = key_part.strip().lower()
                value = value_part.strip()
                
                if not results.get("project_name") and any(term in key_lower for term in ["project", "job"]):
                    if "name" in key_lower or "title" in key_lower:
                        results["project_name"] = value
                if not results.get("location") and "location" in key_lower:
                    results["location"] = value
                if not results.get("owner") and "owner" in key_lower:
                    results["owner"] = value
                if not results.get("prime_contractor") and ("contractor" in key_lower or "gc" == key_lower):
                    results["prime_contractor"] = value
    
    # Clean all results
    for key in list(results.keys()):
        value = results[key]
        # Remove extra whitespace
        value = " ".join(value.split())
        # Remove trailing punctuation that's not part of the value
        value = value.strip(".,;:")
        if len(value) < 2:
            del results[key]
        else:
            results[key] = value
    
    return results


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

