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
def _clean_text_block(text: str) -> str:
    """Normalize and clean OCR artifacts and boilerplate from retrieved blocks."""
    if not text:
        return ""
    t = text.replace("\r", "\n")
    lines: List[str] = []
    seen: set[str] = set()
    for raw in t.splitlines():
        s = raw.strip()
        if not s:
            continue
        low = s.lower()
        # Drop boilerplate and headers/footers
        if any(k in low for k in [
            "department of the army",
            "u.s. army corps of engineers",
            "safety and occupational health requirements",
            "em 385-1-1",
            "page ",
            "table ",
            "figure ",
        ]):
            continue
        # Remove bracketed OCR notes like [OCR Merge]
        if "[" in s and "]" in s:
            s = re.sub(r"\[(?:[^\]]*ocr[^\]]*|[^\]]*merge[^\]]*)\]", "", s, flags=re.IGNORECASE).strip()
            if not s:
                continue
        # Drop mostly-nonalpha lines and very short debris
        alpha = sum(c.isalpha() for c in s)
        total = max(1, len(s))
        if total < 20 or (alpha / total) < 0.6:
            continue
        # Collapse whitespace and dedupe (global within this block)
        s = re.sub(r"\s+", " ", s)
        if s in seen:
            continue
        seen.add(s)
        lines.append(s)
    return "\n".join(lines)


def _normalize_quote_anchor(s: str, max_chars: int = 240) -> str:
    if not s:
        return ""
    t = re.sub(r"\s+", " ", s).strip()
    # Keep at most first 3 sentences
    parts = re.split(r"(?<=[.!?])\s+", t)
    t = " ".join(parts[:3])
    return t[:max_chars].rstrip()


def _is_relevant_quote(activity: str, hazard: str, quote: str) -> bool:
    a = set(re.findall(r"[a-z0-9]+", (activity or "").lower()))
    h = set(re.findall(r"[a-z0-9]+", (hazard or "").lower()))
    q = set(re.findall(r"[a-z0-9]+", (quote or "").lower()))
    overlap = (a | h) & q
    return len(overlap) >= 1



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
    """Heuristically extract controls, PPE, and permits/training/roles from text docs."""
    controls: List[str] = []
    ppe: List[str] = []
    permits: List[str] = []

    ctrl_keys = [r"\bshall\b", r"\bmust\b", r"control", r"ensure", r"required", r"prohibit", r"procedure"]
    ppe_keys = ["ppe", "glove", "eye", "goggle", "face shield", "hearing", "respirator", "life jacket", "flotation", "harness", "lanyard", "hi-vis", "hard hat", "steel-toe"]
    permit_keys = [
        "permit",
        "training",
        "qualified",
        "competent person",
        "entry supervisor",
        "authorization",
        "certified",
        "supervisor",
        "jha",
        "jsa",
        "loto",
        "confined space permit",
        "hot work permit",
        "lift plan",
    ]

    def add_unique(lst: List[str], val: str, max_len: int = 160):
        v = (val or "").strip()
        if not v:
            return
        v = re.sub(r"\s+", " ", v)[:max_len]
        if v not in lst:
            lst.append(v)

    for d in docs:
        text = _clean_text_block(d or "")
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


def generate_full_aha(activity: str, collection_name: str, msf_doc_id: str | None = None) -> AhaDoc:
    """Generate a fuller AHA with heuristic extraction of controls/PPE/permits from retrieval context."""
    hazards = hazards_for_activity(activity)
    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)
    # Try MSF collection (dual-source); ignore errors if missing
    try:
        msf_col = get_or_create_collection(client, "msf_index")
    except Exception:
        msf_col = None  # type: ignore

    citations: List[AhaCitation] = []
    gathered_docs: List[str] = []

    for hz in hazards:
        # MSF retrieval first (project-specific)
        if msf_col is not None and msf_doc_id:
            try:
                where = {"$and": [{"doc_id": {"$eq": msf_doc_id}}, {"source_type": {"$eq": "MSF"}}]}
                print(f"[aha-msf] where={where} activity={activity} hazard={hz}")
                q_msf = f"{activity} {hz} safety procedure ppe training"
                msf_res = msf_col.query(query_texts=[q_msf], n_results=10, where=where, include=["documents", "metadatas", "distances"])  # type: ignore
                msf_docs = msf_res.get("documents", [[]])[0]
                msf_metas = msf_res.get("metadatas", [[]])[0]
                gathered_docs[:0] = msf_docs[:8]  # prepend
                for m in msf_metas[:2]:
                    if not m:
                        continue
                    sp = m.get("section_code") or m.get("section_title") or m.get("headings") or "MSF Section"
                    msf_cit = AhaCitation(
                        section_path=str(sp),
                        page_label=str(m.get("page_start") or ""),
                        page_number=None,
                        quote_anchor=_normalize_quote_anchor(str(m.get("section_title") or m.get("headings") or "")),
                        source_url=str(m.get("source_url") or ""),
                    )
                    if _is_relevant_quote(activity, hz, msf_cit.quote_anchor):
                        citations.append(msf_cit)
            except Exception:
                pass

        # EM 385 retrieval next (baseline controls)
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
        cits = _best_citations_from_results(merged, limit=2)
        normd: List[AhaCitation] = []
        for c in cits:
            c.quote_anchor = _normalize_quote_anchor(c.quote_anchor)
            if _is_relevant_quote(activity, hz, c.quote_anchor):
                normd.append(c)
        citations.extend(normd)

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


