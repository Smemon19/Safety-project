from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_credentials_path() -> str:
    return os.path.join(_project_root(), "firebase-admin.json")


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        def load_dotenv(*args: Any, **kwargs: Any) -> bool:  # type: ignore
            return False
    # Load local .env if present without overriding env that may already be set
    load_dotenv(override=False)
    load_dotenv(override=True)


def _init_firebase() -> "firestore.Client":
    _load_env()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path:
        creds_path = _default_credentials_path()
    if not os.path.isabs(creds_path):
        creds_path = os.path.abspath(creds_path)

    project_id = (
        os.getenv("FIREBASE_PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Missing firebase dependency: {e}")

    try:
        try:
            firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(creds_path)
            if project_id:
                firebase_admin.initialize_app(cred, {"projectId": project_id})
            else:
                firebase_admin.initialize_app(cred)
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Failed to initialize Firebase Admin: {e}")

    return firestore.client()


def _read_text_file(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return p.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""


_CODE_TOKEN_RE = re.compile(r"\b(\d{3,4}(?:\.\d+)+)\b")


def _extract_ufgs_token_from_name(name: str) -> str:
    """Extract a UFGS-style section token from a filename stem.

    - Captures ALL two-digit groups to preserve uniqueness (e.g., "46 51 00.00 10" ->
      UFGS-46-51-00-00-10)
    - Falls back to empty string if no two-digit groups are found.
    """
    digits = re.findall(r"\d{2}", name or "")
    if not digits:
        return ""
    return "UFGS-" + "-".join(digits)


def _extract_code_token(text: str) -> str:
    m = _CODE_TOKEN_RE.search(text or "")
    return m.group(1) if m else ""


def _text_hash(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


_YES_PATTERNS = [
    r"\bAHA\b",
    r"\bJHA\b",
    r"hazard\s+analysis\s+required",
    r"\bexcavat(ion|e)\b",
    r"confined\s+space",
    r"\bcrane(s)?\b",
    r"fall\s+protection",
    r"\bdemolition\b",
    r"hot\s+work",
]
_NO_PATTERNS = [
    r"\bdefinitions?\b",
    r"\bpurpose\b",
    r"\breferences?\b",
    r"\badministration\b",
]


def _rules_decide_requires_aha(text: str) -> Tuple[Optional[bool], float, str]:
    s = (text or "").lower()
    for pat in _YES_PATTERNS:
        if re.search(pat, s, re.IGNORECASE):
            return True, 0.95, "Rule-based positive trigger"
    for pat in _NO_PATTERNS:
        if re.search(pat, s, re.IGNORECASE):
            return False, 0.9, "Rule-based administrative/definitions section"
    return None, 0.0, "Ambiguous"


def _rag_citations_for(text: str, collection_name: Optional[str]) -> List[Dict[str, Any]]:
    from utils import (
        get_chroma_client,
        get_default_chroma_dir,
        get_or_create_collection,
        query_collection,
        keyword_search_collection,
        build_section_search_terms,
    )

    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)

    # Build a broad query using detected terms
    terms = build_section_search_terms(text or "")
    q = " ".join(terms[:20]) or (text[:200] if text else "EM 385 requirements")
    res_vec = query_collection(col, q, n_results=10)
    res_kw = keyword_search_collection(col, terms[:20], max_results=10)

    ids = [*res_vec.get("ids", [[]])[0]]
    metas = [*res_vec.get("metadatas", [[]])[0]]
    seen = set(ids)
    for i, id_ in enumerate(res_kw.get("ids", [[]])[0]):
        if id_ not in seen:
            ids.append(id_)
            metas.append(res_kw.get("metadatas", [[]])[0][i])
            seen.add(id_)

    citations: List[Dict[str, Any]] = []
    for m in metas[:5]:
        m = m or {}
        citations.append({
            "section_path": str(m.get("section_path") or m.get("headers") or m.get("title") or ""),
            "page_label": str(m.get("page_label") or ""),
            "page_number": m.get("page_number") if isinstance(m.get("page_number"), int) else None,
            "quote_anchor": str(m.get("quote_anchor") or ""),
            "source_url": str(m.get("source_url") or ""),
        })
    return citations


def _rag_decide_requires_aha(text: str, collection_name: Optional[str]) -> Tuple[bool, float, str, List[Dict[str, Any]]]:
    # Lightweight RAG heuristic using retrieved context only (no LLM in v1)
    citations = _rag_citations_for(text, collection_name)
    s = (text or "").lower()
    positive = any(k in s for k in ["aha", "jha", "hazard analysis", "permit-required confined space", "hot work", "excavation", "crane", "fall protection", "demolition"])  # heuristic
    if positive:
        return True, 0.7, "RAG-backed heuristic positive", citations
    return False, 0.6, "RAG-backed heuristic negative", citations


def _upsert_code_doc(db: "firestore.Client", code_token: str, title: str, text_hash: str, source_path: str) -> None:
    doc_ref = db.collection("codes").document(code_token)
    payload = {
        "code_token": code_token,
        "title": title,
        "text_hash": text_hash,
        "source_path": source_path,
        "inserted_at": datetime.now(timezone.utc).isoformat(),
    }
    doc_ref.set(payload, merge=True)


def _write_decision(db: "firestore.Client", code_token: str, decision: Dict[str, Any]) -> None:
    doc_ref = db.collection("decisions").document(code_token)
    doc_ref.set(decision, merge=True)


def _guess_title(text: str) -> str:
    for line in (text or "").splitlines():
        l = line.strip()
        if len(l) >= 6:
            return l[:200]
    return ""


def process_codes(input_dir: str, em385_version: str, model_version: str, codepack_version: str, collection_name: Optional[str]) -> None:
    db = _init_firebase()
    base = Path(input_dir)
    if not base.exists():
        print(f"[codes] Input directory not found: {base}")
        return

    files: List[Path] = []
    for ext in (".SEC", ".sec", ".txt"):
        files.extend(base.rglob(f"*{ext}"))

    print(f"[codes] Found {len(files)} files")

    for p in sorted(files):
        raw = _read_text_file(p)
        if not raw:
            continue
        # Determine token:
        # - For UFGS .SEC files, ALWAYS use filename-derived UFGS token to ensure one doc per section
        # - Otherwise (e.g., EM385 split text), use first dotted token as EM385 code
        section_type = "UFGS" if p.suffix.lower() == ".sec" else "EM385"
        ufgs_token = _extract_ufgs_token_from_name(p.stem) if section_type == "UFGS" else ""
        em_code = _extract_code_token(raw) if section_type != "UFGS" else ""
        if section_type == "UFGS" and ufgs_token:
            code_token = ufgs_token
        elif em_code:
            code_token = f"385-{em_code}"
            section_type = "EM385"
        else:
            print(f"[codes] Skip {p.name}: no section token found")
            continue

        text_hash = _text_hash(raw)
        title = _guess_title(raw)

        _upsert_code_doc(db, code_token, title, text_hash, str(p))
        # Store full text for auditability
        try:
            db.collection("codes").document(code_token).set({
                "text": raw,
                "section_type": section_type,
            }, merge=True)
        except Exception:
            pass

        # Decide requires AHA
        requires: Optional[bool]
        requires, confidence, rationale = _rules_decide_requires_aha(raw)
        citations: List[Dict[str, Any]] = []
        if requires is None:
            req, conf, rat, cits = _rag_decide_requires_aha(raw, collection_name)
            requires = req
            confidence = conf
            rationale = rat
            citations = cits

        decision_doc = {
            "requiresAha": bool(requires),
            "confidence": float(confidence),
            "rationale": rationale,
            "citations": citations,
            "em385_version": em385_version,
            "model_version": model_version,
            "codepack_version": codepack_version,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_decision(db, code_token, decision_doc)
        print(f"[codes] {code_token}: requiresAha={decision_doc['requiresAha']} conf={decision_doc['confidence']:.2f}")


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process EM 385 codes into Firestore decisions.")
    ap.add_argument("--input", required=True, help="Path to inputs/codes directory")
    ap.add_argument("--em385-version", required=True, help="EM 385 edition/version label, e.g., 2024")
    ap.add_argument("--model-version", required=True, help="Model version label, e.g., gpt-4o-mini")
    ap.add_argument("--codepack", dest="codepack_version", required=True, help="Codepack version label, e.g., v1")
    ap.add_argument("--collection", dest="collection_name", default=None, help="Chroma collection name (optional)")
    return ap.parse_args(argv)


def main() -> int:
    args = _parse_args()
    process_codes(
        input_dir=args.input,
        em385_version=args.em385_version,
        model_version=args.model_version,
        codepack_version=args.codepack_version,
        collection_name=args.collection_name,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())


