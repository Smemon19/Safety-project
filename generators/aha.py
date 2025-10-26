from __future__ import annotations

from typing import List, Dict, Any, Tuple
from models.aha import AhaDoc, AhaItem, AhaCitation
from generators.hazard_map import hazards_for_activity
from utils import get_chroma_client, get_default_chroma_dir, get_or_create_collection, query_collection, keyword_search_collection
import re


def _build_retrieval_query(activity: str, hazard: str) -> str:
    # Encourage section tokens to surface with keywords
    return f"{activity} {hazard} requirements procedure training PPE permit EM 385"


def _best_citations_from_results(res: Dict[str, Any], limit: int = 3) -> List[AhaCitation]:
    ids = res.get("ids", [[]])[0]
    metas = res.get("metadatas", [[]])[0]
    out: List[AhaCitation] = []
    for i in range(min(limit, len(ids))):
        m = metas[i] or {}
        out.append(AhaCitation(
            section_path=str(m.get("section_path") or m.get("headers") or m.get("title") or ""),
            page_label=str(m.get("page_label") or ""),
            page_number=(m.get("page_number") if isinstance(m.get("page_number"), int) else None),
            quote_anchor=str(m.get("quote_anchor") or ""),
            source_url=str(m.get("source_url") or ""),
        ))
    return out


def generate_basic_aha(activity: str, collection_name: str) -> AhaDoc:
    """Generate a minimal AHA skeleton for an activity using retrieval-backed citations."""
    hazards = hazards_for_activity(activity)

    # Retrieve up to N citations across hazards using hybrid retrieval
    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)
    citations: List[AhaCitation] = []
    for hz in hazards:
        q = _build_retrieval_query(activity, hz)
        res_vec = query_collection(col, q, n_results=10)
        # Expand substrings to broaden matches (tokenize + simple stemming)
        def _tokens(s: str) -> list[str]:
            toks = [t for t in re.split(r"[^A-Za-z0-9]+", (s or "").lower()) if t]
            extra: list[str] = []
            for t in toks:
                if t.endswith("ing") and len(t) > 4:
                    extra.append(t[:-3])
                if t.endswith("ed") and len(t) > 3:
                    extra.append(t[:-2])
            return list(dict.fromkeys([*toks, *extra]))

        base_terms = set(["em 385", "em385", "em 385-1-1", "permit", "training", "ppe", "diving", "dive", "welding", "excavation", "electrical"])  # common anchors
        substrings = list(base_terms | set(_tokens(activity)) | set(_tokens(hz)))
        res_kw = keyword_search_collection(col, substrings, max_results=10)
        # Merge like rag_agent.retrieve
        vec_ids = [*res_vec.get("ids", [[]])[0]]
        vec_docs = [*res_vec.get("documents", [[]])[0]]
        vec_metas = [*res_vec.get("metadatas", [[]])[0]]
        kw_ids = res_kw.get("ids", [[]])[0]
        kw_docs = res_kw.get("documents", [[]])[0]
        kw_metas = res_kw.get("metadatas", [[]])[0]
        seen = set(vec_ids)
        for i, id_ in enumerate(kw_ids):
            if id_ not in seen:
                vec_ids.append(id_)
                vec_docs.append(kw_docs[i])
                vec_metas.append(kw_metas[i])
                seen.add(id_)
        merged = {"ids": [vec_ids], "documents": [vec_docs], "metadatas": [vec_metas]}
        citations.extend(_best_citations_from_results(merged, limit=1))

    # Minimal placeholder steps; detailed synthesis will be added later
    items = [
        AhaItem(step="Pre-task planning and briefing", hazards=hazards[:3], controls=["JHA review", "Permits in place"], ppe=["Hard hat", "Eye protection"], permits_training=["Tailgate meeting"]),
        AhaItem(step=f"Perform {activity} per procedure", hazards=hazards[:3], controls=["Follow SOP", "Supervision present"], ppe=["As required"], permits_training=["Qualified personnel"]),
        AhaItem(step="Closeout and housekeeping", hazards=[], controls=["Area cleared", "Tools accounted for"], ppe=[], permits_training=[]),
    ]

    return AhaDoc(
        name=f"AHA - {activity}",
        activity=activity,
        hazards=hazards,
        items=items,
        citations=citations[:5],
    )


def _extract_lists_from_docs(docs: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Heuristically extract controls, PPE, and permits/training from text docs."""
    controls: List[str] = []
    ppe: List[str] = []
    permits: List[str] = []

    ctrl_keys = [r"\bshall\b", r"\bmust\b", r"control", r"ensure", r"required", r"prohibit", r"procedure"]
    ppe_keys = ["ppe", "glove", "eye", "goggle", "face shield", "hearing", "respirator", "life jacket", "flotation", "harness", "lanyard", "hi-vis", "hard hat", "steel-toe"]
    permit_keys = ["permit", "training", "qualified", "competent person", "authorization", "certified", "supervisor", "JHA", "JSA", "LOTO"]

    def add_unique(lst: List[str], val: str, max_len: int = 160):
        v = (val or "").strip()
        if not v:
            return
        v = re.sub(r"\s+", " ", v)[:max_len]
        if v not in lst:
            lst.append(v)

    for d in docs:
        text = (d or "")
        for line in text.splitlines():
            l = line.strip()
            if not l:
                continue
            l_low = l.lower()
            if any(re.search(k, l_low) for k in ctrl_keys):
                add_unique(controls, l)
            if any(k in l_low for k in ppe_keys):
                add_unique(ppe, l)
            if any(k in l_low for k in permit_keys):
                add_unique(permits, l)

    # Trim lists for initial output cleanliness
    return controls[:12], ppe[:10], permits[:10]


def generate_full_aha(activity: str, collection_name: str) -> AhaDoc:
    """Generate a fuller AHA with heuristic extraction of controls/PPE/permits from retrieval context."""
    hazards = hazards_for_activity(activity)
    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)

    citations: List[AhaCitation] = []
    gathered_docs: List[str] = []

    for hz in hazards:
        q = _build_retrieval_query(activity, hz)
        res_vec = query_collection(col, q, n_results=15)
        base_terms = set(["em 385", "em385", "em 385-1-1", activity.lower(), hz.lower()])
        substrings = list(base_terms | {"permit", "training", "ppe", "shall", "must"})
        res_kw = keyword_search_collection(col, substrings, max_results=15)
        vec_docs = [*res_vec.get("documents", [[]])[0]]
        kw_docs = res_kw.get("documents", [[]])[0]
        gathered_docs.extend(vec_docs[:8])
        gathered_docs.extend(kw_docs[:8])
        merged = {"ids": [res_vec.get("ids", [[]])[0]], "metadatas": [res_vec.get("metadatas", [[]])[0]]}
        citations.extend(_best_citations_from_results(merged, limit=2))

    controls, ppe, permits = _extract_lists_from_docs(gathered_docs)

    items = [
        AhaItem(step="Pre-task planning and briefing", hazards=hazards[:3], controls=["JHA review", "Permits in place", *controls[:4]], ppe=ppe[:4], permits_training=["Tailgate meeting", *permits[:3]]),
        AhaItem(step=f"Perform {activity} per procedure", hazards=hazards[:3], controls=["Follow SOP", "Supervision present", *controls[4:8]], ppe=ppe[4:8], permits_training=permits[3:6]),
        AhaItem(step="Closeout and housekeeping", hazards=[], controls=["Area cleared", "Tools accounted for", *controls[8:12]], ppe=ppe[8:10], permits_training=permits[6:10]),
    ]

    return AhaDoc(
        name=f"AHA - {activity}",
        activity=activity,
        hazards=hazards,
        items=items,
        citations=citations[:5],
    )


