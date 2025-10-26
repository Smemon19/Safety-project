from __future__ import annotations

from typing import Dict, Any, List
from models.csp import CspDoc, CspSection, CspCitation
from generators.analyze import analyze_scope
from utils import get_chroma_client, get_default_chroma_dir, get_or_create_collection, query_collection


SECTION_MAP = {
    "Diving Operations": "Diving Program",
    "Welding & Cutting": "Welding & Cutting Program",
    "Electrical Systems": "Electrical Safety & LOTO",
    "Excavation & Trenching": "Excavation & Trenching Safety",
    "Cranes & Rigging": "Cranes & Rigging",
    "Confined Space Entry": "Confined Space Program",
    "Demolition": "Demolition Plan",
}


def _citations_from_query(col, query: str, limit: int = 3) -> List[CspCitation]:
    res = query_collection(col, query, n_results=limit)
    metas = res.get("metadatas", [[]])[0]
    out: List[CspCitation] = []
    for m in metas[:limit]:
        m = m or {}
        out.append(CspCitation(
            section_path=str(m.get("section_path") or m.get("headers") or m.get("title") or ""),
            page_label=str(m.get("page_label") or ""),
            page_number=(m.get("page_number") if isinstance(m.get("page_number"), int) else None),
            quote_anchor=str(m.get("quote_anchor") or ""),
            source_url=str(m.get("source_url") or ""),
        ))
    return out


def generate_csp(spec: Dict[str, Any], collection_name: str) -> CspDoc:
    # Flatten scope text
    parts: List[str] = []
    for wp in spec.get('work_packages', []):
        parts.append(wp.get('title',''))
        parts.extend(wp.get('activities', []))
    parts.extend(spec.get('deliverables', []))
    parts.extend(spec.get('assumptions', []))
    text = "\n".join([p for p in parts if p])

    analysis = analyze_scope(text)
    acts = analysis['activities']

    # Build sections triggered by activities
    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)

    sections: List[CspSection] = []
    used_names: set[str] = set()
    for a in acts:
        sec_name = SECTION_MAP.get(a)
        if not sec_name or sec_name in used_names:
            continue
        used_names.add(sec_name)
        # retrieval prompt for overview
        q = f"EM 385 requirements and program elements for {a}. Include training, procedures, equipment, inspections."
        cits = _citations_from_query(col, q, limit=3)
        paras = [
            f"This section outlines EM 385 requirements applicable to {a}. It summarizes program elements, training, procedures, equipment, and inspections relevant to this project.",
        ]
        sections.append(CspSection(name=sec_name, paragraphs=paras, citations=cits))

    # Add general sections
    gen = CspSection(
        name="Project Overview & Scope",
        paragraphs=[
            f"Project: {spec.get('project_name','')} — {spec.get('project_number','')}",
            f"Location: {spec.get('location','')}",
            f"Owner: {spec.get('owner','')} | GC: {spec.get('gc','')}",
        ],
        citations=[],
    )
    sections.insert(0, gen)

    return CspDoc(
        project_name=str(spec.get('project_name','')),
        project_number=str(spec.get('project_number','')),
        location=str(spec.get('location','')),
        owner=str(spec.get('owner','')),
        general_contractor=str(spec.get('gc','')),
        sections=sections,
    )


