"""Core generation routines for Section 11."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from pydantic import BaseModel

from utils import (
    build_section_search_terms,
    get_chroma_client,
    get_default_chroma_dir,
    get_or_create_collection,
    keyword_search_collection,
    query_collection,
)

from section11.constants import EM_385_CATEGORIES
from section11.models import (
    AhaEvidence,
    CategoryBundle,
    CategoryStatus,
    ParsedCode,
    SafetyPlanEvidence,
)


def _merge_results(vector: Dict[str, List[List[str]]], keyword: Dict[str, List[List[str]]]) -> Dict[str, List[List[str]]]:
    ids = [*vector.get("ids", [[]])[0]]
    docs = [*vector.get("documents", [[]])[0]]
    metas = [*vector.get("metadatas", [[]])[0]]
    seen = set(ids)
    for idx, identifier in enumerate(keyword.get("ids", [[]])[0]):
        if identifier in seen:
            continue
        ids.append(identifier)
        docs.append(keyword.get("documents", [[]])[0][idx])
        metas.append(keyword.get("metadatas", [[]])[0][idx])
        seen.add(identifier)
    return {"ids": [ids], "documents": [docs], "metadatas": [metas]}


def _clean_sentence(text: str) -> str:
    sentence = re.sub(r"\s+", " ", (text or "").strip())
    return sentence[:320]


def _extract_sentences(documents: Iterable[str]) -> List[str]:
    sentences: List[str] = []
    splitter = re.compile(r"(?<=[.!?])\s+")
    for doc in documents:
        for part in splitter.split(doc or ""):
            cleaned = _clean_sentence(part)
            if cleaned:
                sentences.append(cleaned)
    return sentences


def _hazard_sentences(sentences: Iterable[str]) -> List[str]:
    hazards: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if any(keyword in lower for keyword in ["hazard", "exposure", "risk", "injury", "fatal", "shock", "fall", "collapse"]):
            hazards.append(sentence)
    return hazards[:8]


def _control_sentences(sentences: Iterable[str]) -> List[str]:
    controls: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if any(keyword in lower for keyword in ["shall", "must", "ensure", "provide", "require", "permit", "ppe", "training", "inspection"]):
            controls.append(sentence)
    return controls[:12]


def _dedupe(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    deduped: List[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


class RetrievalContext(BaseModel):
    documents: List[str]
    metadatas: List[Dict[str, object]]


def retrieve_context(category: str, scope: List[str], codes: List[str], collection_name: str | None) -> RetrievalContext:
    client = get_chroma_client(get_default_chroma_dir())
    collection = get_or_create_collection(client, collection_name or "")
    query_terms = build_section_search_terms(" ".join([category, *codes]))
    scope_hint = " ".join(scope[:3])
    query = " ".join(query_terms[:12]) or f"EM 385 {category} hazards"
    vector = query_collection(collection, query, n_results=12)
    keyword = keyword_search_collection(collection, [category, *codes, scope_hint], max_results=12)
    merged = _merge_results(vector, keyword)
    documents = merged.get("documents", [[]])[0]
    metadatas = merged.get("metadatas", [[]])[0]
    return RetrievalContext(documents=list(documents), metadatas=list(metadatas))


def build_aha_evidence(category: str, context: RetrievalContext, scope: List[str]) -> AhaEvidence:
    sentences = _extract_sentences(context.documents)
    hazard_sentences = _hazard_sentences(sentences)
    hazards = _dedupe([sentence.split(".")[0] for sentence in hazard_sentences])
    narrative = []
    if scope:
        narrative.append(f"Project scope highlights: {'; '.join(scope[:2])}.")
    narrative.extend(hazard_sentences[:6])
    citations: List[Dict[str, str]] = []
    for meta in context.metadatas[:5]:
        meta = meta or {}
        citations.append(
            {
                "section_path": str(meta.get("section_path") or meta.get("headers") or ""),
                "page_label": str(meta.get("page_label") or meta.get("page_number") or ""),
                "source_url": str(meta.get("source_url") or ""),
            }
        )
    status = CategoryStatus.required if hazards else CategoryStatus.pending
    pending_reason = "" if hazards else "No hazard sentences surfaced from EM 385 retrieval."
    return AhaEvidence(
        hazards=hazards,
        narrative=_dedupe(narrative),
        citations=citations,
        status=status,
        pending_reason=pending_reason,
    )


def _split_controls(sentences: Iterable[str]) -> Tuple[List[str], List[str], List[str]]:
    controls: List[str] = []
    ppe: List[str] = []
    permits: List[str] = []
    for sentence in sentences:
        lower = sentence.lower()
        if "ppe" in lower or any(token in lower for token in ["glove", "protection", "respirator", "hard hat", "eye", "face shield"]):
            ppe.append(sentence)
        elif "permit" in lower or "training" in lower or "qualified" in lower:
            permits.append(sentence)
        else:
            controls.append(sentence)
    return controls, ppe, permits


def build_safety_plan_evidence(
    category: str,
    context: RetrievalContext,
    scope: List[str],
    codes: List[str],
) -> SafetyPlanEvidence:
    sentences = _extract_sentences(context.documents)
    control_sentences = _control_sentences(sentences)
    controls, ppe, permits = _split_controls(control_sentences)
    project_evidence = [f"Scope reference: {scope_item}" for scope_item in scope[:4]]
    em_evidence = _dedupe(control_sentences[:6])
    status = CategoryStatus.required
    pending_reason = ""
    if len(project_evidence) < 2 or len(em_evidence) < 2:
        status = CategoryStatus.insufficient
        pending_reason = "Safety Plan evidence quota not met (needs ≥2 project and ≥2 EM citations)."
    citations: List[Dict[str, str]] = []
    for meta in context.metadatas[:5]:
        meta = meta or {}
        citations.append(
            {
                "section_path": str(meta.get("section_path") or meta.get("headers") or ""),
                "page_label": str(meta.get("page_label") or meta.get("page_number") or ""),
                "source_url": str(meta.get("source_url") or ""),
            }
        )
    return SafetyPlanEvidence(
        controls=_dedupe(controls[:10]),
        ppe=_dedupe(ppe[:8]),
        permits=_dedupe(permits[:8]),
        citations=citations[:5],
        project_evidence=project_evidence,
        em_evidence=em_evidence[:5],
        status=status,
        pending_reason=pending_reason,
    )


def group_codes_by_category(codes: Iterable[ParsedCode]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = defaultdict(list)
    for parsed in codes:
        if not parsed.requires_aha:
            continue
        category = parsed.suggested_category or "Unmapped"
        grouped[category].append(parsed.code)
    return grouped


def build_category_bundles(
    codes: Iterable[ParsedCode],
    scope: List[str],
    collection_name: str | None,
) -> List[CategoryBundle]:
    grouped = group_codes_by_category(codes)
    bundles: List[CategoryBundle] = []
    for category in sorted(grouped.keys(), key=lambda value: (value != "Unmapped", value)):
        code_list = grouped[category]
        context = retrieve_context(category, scope, code_list, collection_name)
        aha = build_aha_evidence(category, context, scope)
        plan = build_safety_plan_evidence(category, context, scope, code_list)
        bundle = CategoryBundle(category=category, codes=code_list, aha=aha, plan=plan)
        bundles.append(bundle)
    return bundles


def ensure_categories(bundles: List[CategoryBundle]) -> List[CategoryBundle]:
    categories = {bundle.category for bundle in bundles}
    for required in EM_385_CATEGORIES:
        if required not in categories:
            bundles.append(CategoryBundle(category=required, codes=[]))
    return bundles

