from __future__ import annotations

"""Utilities for constructing CSP citation objects."""

from pathlib import Path
from typing import Any, Dict, Iterable, List

from models.csp import CspCitation


def _normalize_em_ref(ref: str) -> str:
    ref = (ref or "").strip()
    if not ref:
        return ""
    if ref.startswith("§"):
        return ref
    if ref.lower().startswith("em 385"):
        return ref
    return f"§{ref}" if ref[0].isdigit() else ref


def _build_em_citations(refs: Iterable[str]) -> List[CspCitation]:
    citations: List[CspCitation] = []
    for ref in refs:
        norm = _normalize_em_ref(ref)
        if not norm:
            continue
        citations.append(CspCitation(section_path=f"EM 385-1-1 {norm}"))
    return citations


def _build_document_citations(documents: Iterable[str]) -> List[CspCitation]:
    citations: List[CspCitation] = []
    for doc in documents:
        if not doc:
            continue
        path = Path(doc)
        section = f"Project Document: {path.name}"
        citations.append(
            CspCitation(
                section_path=section,
                source_url=f"file://{path.resolve()}",
            )
        )
    return citations


def generate_section_citations(context: Dict[str, Any]) -> List[CspCitation]:
    """Return a deduplicated citation list for a CSP section context pack."""

    citations: List[CspCitation] = []
    seen: set[tuple[str, str, str]] = set()

    em_refs = context.get("em385_refs", []) or []
    citations.extend(_build_em_citations(em_refs))

    doc_refs = context.get("documents", []) or []
    citations.extend(_build_document_citations(doc_refs))

    extra_refs = context.get("citations", []) or []
    for raw in extra_refs:
        token = (raw or "").strip()
        if not token:
            continue
        section = token
        if token.upper().startswith("EM385-"):
            section = token.upper().replace("EM385-", "EM 385-1-1 §")
        citations.append(CspCitation(section_path=section))

    deduped: List[CspCitation] = []
    for cit in citations:
        key = (cit.section_path, cit.page_label, cit.source_url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cit)
    return deduped


__all__ = ["generate_section_citations"]

