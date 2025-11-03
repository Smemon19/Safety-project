from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from utils import (
    get_chroma_client,
    get_or_create_collection,
    get_default_chroma_dir,
)


def detect_activities_from_msf(
    *,
    doc_id: str,
    msf_collection: str = "msf_index",
    ontology_path: str = "mappings/activities.json",
    top_k: int = 50,
    min_similarity: float = 0.35,
) -> Dict[str, List[Dict[str, Any]]]:
    """Detect activities grounded in MSF chunks for a specific doc_id.

    Returns mapping: activity_label -> list of section metadata dicts where activity was detected.
    """
    import json
    from pathlib import Path

    try:
        mp = json.loads(Path(ontology_path).read_text(encoding="utf-8"))
    except Exception:
        mp = {
            "Excavation & Trenching": ["excavate", "trench", "shoring", "shielding"],
            "Confined Space Entry": ["confined space", "prcs"],
            "Cranes & Rigging": ["crane", "rigging", "lift", "hoist"],
            "Welding & Cutting": ["weld", "cut", "hot work"],
            "Electrical Systems": ["electrical", "loto", "energized"],
            "Demolition": ["demo", "demolish"],
            "Diving Operations": ["diving", "underwater"],
        }

    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, msf_collection)

    # We'll use vector search; utils.query_collection is in another module, so duplicate minimal call inline
    def _query(q: str) -> Dict[str, Any]:
        where = {"$and": [{"doc_id": {"$eq": doc_id}}, {"source_type": {"$eq": "MSF"}}]}
        print(f"[msf-activities] query where={where}")
        return col.query(query_texts=[q], n_results=top_k, where=where, include=["documents", "metadatas", "distances"])  # type: ignore

    out: Dict[str, List[Dict[str, Any]]] = {}
    seen_by_label: Dict[str, set[Tuple[str, str]]] = {}
    for label, tokens in mp.items():
        q = f"{label} " + " ".join(tokens[:6])
        try:
            res = _query(q)
        except Exception:
            continue
        docs = res.get("documents", [[]])[0]
        metas = res.get("metadatas", [[]])[0]
        dists = res.get("distances", [[]])[0]
        if not docs:
            continue
        bucket: List[Dict[str, Any]] = []
        seen_key = seen_by_label.setdefault(label, set())
        for m, dist in zip(metas, dists):
            sim = 1.0 - float(dist or 1.0)
            if sim < min_similarity:
                continue
            section_code = str((m or {}).get("section_code") or "")
            section_title = str((m or {}).get("section_title") or "")
            key = (section_code, section_title)
            if key in seen_key:
                continue
            seen_key.add(key)
            bucket.append({
                "section_code": section_code,
                "section_title": section_title,
                "division": str((m or {}).get("division") or ""),
                "page_start": (m or {}).get("page_start") or "",
                "page_end": (m or {}).get("page_end") or "",
            })
        if bucket:
            out[label] = bucket

    # Fallback: brute keyword scan within doc_id if vector results were empty
    if not out:
        try:
            where = {"$and": [{"doc_id": {"$eq": doc_id}}, {"source_type": {"$eq": "MSF"}}]}
            offset = 0
            page = 200
            print("[msf-activities] fallback keyword scan")
            while True:
                res = col.get(include=["documents", "metadatas"], where=where, limit=page, offset=offset)  # type: ignore
                docs = res.get("documents", []) or []
                metas = res.get("metadatas", []) or []
                if not docs:
                    break
                for d, m in zip(docs, metas):
                    text = (d or "").lower()
                    for label, tokens in mp.items():
                        if any((t or "").lower() in text for t in tokens):
                            bucket = out.setdefault(label, [])
                            section_code = str((m or {}).get("section_code") or "")
                            section_title = str((m or {}).get("section_title") or "")
                            ent = {
                                "section_code": section_code,
                                "section_title": section_title,
                                "division": str((m or {}).get("division") or ""),
                                "page_start": (m or {}).get("page_start") or "",
                                "page_end": (m or {}).get("page_end") or "",
                            }
                            if ent not in bucket:
                                bucket.append(ent)
                if len(docs) < page:
                    break
                offset += page
        except Exception:
            pass
    return out


