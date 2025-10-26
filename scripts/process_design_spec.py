from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
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


def _read_pdf_text_with_ocr_fallback(pdf_path: Path, ocr_threshold: int = 100, diag_dir: Optional[Path] = None) -> str:
    # Primary: extract visible text (and tables) fast
    from pdf_loader.pdf_text import extract_text
    pages = extract_text(pdf_path, include_tables=True, diagnostic_dir=(diag_dir / "text") if diag_dir else None)
    combined = "\n\n".join([pages[k] for k in sorted(pages.keys())]) if pages else ""
    if len(combined) >= max(0, ocr_threshold):
        return combined
    # Fallback: OCR full document via orchestrator
    from pdf_loader import process_pdf
    tmp_json = (diag_dir / "chunks.json") if diag_dir else (pdf_path.with_suffix(".ocr.json"))
    tmp_imgdir = (diag_dir / "images") if diag_dir else (pdf_path.parent / (pdf_path.stem + "_images"))
    chunks = process_pdf(pdf_path, tmp_json, tmp_imgdir, diagnostic_dir=diag_dir)
    texts = [str(c.get("text", "")) for c in chunks]
    return "\n\n".join([t for t in texts if t])


CODE_RE = re.compile(r"385[-\s]?(\d+(?:\.\d+)*)")
RANGE_RE = re.compile(r"\b(\d{3,4})\s*[–-]\s*(\d{3,4})\b")


def _expand_ranges(text: str) -> List[str]:
    out: List[str] = []
    for m in RANGE_RE.finditer(text or ""):
        a = int(m.group(1))
        b = int(m.group(2))
        if a <= b:
            for v in range(a, b + 1):
                out.append(f"385-{v}")
        else:
            for v in range(b, a + 1):
                out.append(f"385-{v}")
    return out


def _extract_codes(text: str) -> List[str]:
    codes: List[str] = []
    # direct tokens
    for m in CODE_RE.finditer(text or ""):
        codes.append(f"385-{m.group(1)}")
    # ranges like 1012–1016 (no 385 prefix required)
    codes.extend(_expand_ranges(text or ""))
    # stable unique
    seen = set()
    out: List[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-\_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "item"


def _load_plans_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({k: (v or "").strip() for k, v in row.items()})
    return rows


def _infer_activity_for_code(code_token: str, design_text: str, collection_name: Optional[str]) -> Optional[str]:
    # Heuristic: match activities by keyword presence in design text + retrieved snippets near the code
    try:
        from mappings.activities import ACTIVITY_KEYWORDS  # type: ignore
    except Exception:
        # Fallback: load JSON mapping activity -> keywords
        try:
            import json as _json
            mp = _json.loads(Path(_project_root()).joinpath("mappings", "activities.json").read_text(encoding="utf-8"))
        except Exception:
            mp = {}
        ACTIVITY_KEYWORDS = {k: [str(x).lower() for x in (v or [])] for k, v in mp.items()}

    from utils import (
        get_chroma_client,
        get_default_chroma_dir,
        get_or_create_collection,
        query_collection,
        build_section_search_terms,
    )
    retrieved_text = ""
    try:
        client = get_chroma_client(get_default_chroma_dir())
        col = get_or_create_collection(client, collection_name)
        terms = build_section_search_terms(code_token)
        q = " ".join(terms[:6]) or code_token
        res = query_collection(col, q, n_results=8)
        docs = res.get("documents", [[]])[0]
        retrieved_text = "\n".join(docs[:6])
    except Exception:
        retrieved_text = ""
    hay = f"{design_text}\n{retrieved_text}".lower()
    best_act: Optional[str] = None
    best_hits = 0
    for act, kws in ACTIVITY_KEYWORDS.items():
        hits = 0
        for kw in kws:
            if kw and kw in hay:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_act = act
    return best_act


def _generate_ahas(activities: List[str], out_dir: Path, collection_name: Optional[str]) -> List[str]:
    from generators.aha import generate_full_aha
    from export.markdown_writer import write_aha_single_md
    out_paths: List[str] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    for act in activities:
        aha = generate_full_aha(act, collection_name or "")
        slug = _slug(act)
        out_path = out_dir / f"{slug}.md"
        write_aha_single_md(aha, str(out_path))
        out_paths.append(str(out_path))
    return out_paths


def _trigger_plans(rows: List[Dict[str, str]], design_text: str, activities: List[str], codes_found: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    s = (design_text or "").lower()
    acts = {a.lower() for a in activities}
    codes = set(codes_found)
    for row in rows:
        name = (row.get("plan_name") or "").strip()
        ttype = (row.get("trigger_type") or "").strip().lower()
        patt = (row.get("pattern") or "").strip().lower()
        req_act = (row.get("requires_aha_activity") or "").strip().lower()
        req_code = (row.get("requires_code") or "").strip()
        why = None
        if not name:
            continue
        if ttype == "contains_text" and patt and patt in s:
            why = f"contains text: {patt}"
        elif ttype == "aha_activity" and patt and patt in acts:
            why = f"activity present: {patt}"
        elif ttype == "code_present" and patt and patt in codes:
            why = f"code present: {patt}"
        if why:
            if req_act and req_act not in acts:
                continue
            if req_code and req_code not in codes:
                continue
            out.append((name, why))
    return out


def _write_plans(plans: List[Tuple[str, str]], out_dir: Path) -> List[str]:
    out_paths: List[str] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, why in plans:
        slug = _slug(name)
        p = out_dir / f"{slug}.txt"
        body = [
            f"Plan: {name}",
            f"Triggered: {why}",
            "",
            "This is a placeholder. Populate with project-specific procedures and references to EM 385 citations in AHAs.",
        ]
        p.write_text("\n".join(body), encoding="utf-8")
        out_paths.append(str(p))
    return out_paths


@dataclass
class RunResult:
    run_id: str
    input_file: str
    codes_found: List[str]
    decisions: Dict[str, Dict[str, Any]]
    activities: List[str]
    aha_files: List[str]
    plan_files: List[str]
    manifest_path: str


def _write_decision(db: "firestore.Client", code_token: str, decision: Dict[str, Any]) -> None:
    doc_ref = db.collection("decisions").document(code_token)
    doc_ref.set(decision, merge=True)


def _rag_decide_requires_aha_for_code(code_token: str, collection_name: Optional[str]) -> Tuple[bool, float, str, List[Dict[str, Any]]]:
    # Heuristic classification using retrieval focused on the EM 385 section token
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
    # Build focused query on the section number
    terms = build_section_search_terms(code_token)
    q = " ".join(terms[:10]) or code_token
    res_vec = query_collection(col, q, n_results=12)
    base_terms = list(set(terms[:10] + ["AHA", "JHA", "hazard analysis", "activity hazard analysis", "shall", "must"]))
    res_kw = keyword_search_collection(col, base_terms, max_results=12)

    ids = [*res_vec.get("ids", [[]])[0]]
    docs = [*res_vec.get("documents", [[]])[0]]
    metas = [*res_vec.get("metadatas", [[]])[0]]
    seen = set(ids)
    for i, id_ in enumerate(res_kw.get("ids", [[]])[0]):
        if id_ not in seen:
            ids.append(id_)
            metas.append(res_kw.get("metadatas", [[]])[0][i])
            docs.append(res_kw.get("documents", [[]])[0][i])
            seen.add(id_)

    # Simple heuristic: positive if AHA/JHA wording appears in top docs
    hay = "\n".join(docs[:10]).lower()
    positive = any(k in hay for k in ["activity hazard analysis", "aha", "jha", "job hazard analysis"]) or ("requires" in hay and "analysis" in hay)
    confidence = 0.75 if positive else 0.6
    rationale = "RAG-backed heuristic based on EM 385 section context"
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
    return positive, confidence, rationale, citations


def process_design_spec(input_path: str, collection_name: Optional[str], ocr_threshold: int = 100, classify_only: bool = False) -> RunResult:
    db = _init_firebase()
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = Path(_project_root()) / "outputs" / "runs" / run_id
    ahas_dir = run_dir / "ahas"
    plans_dir = run_dir / "plans"
    if not classify_only:
        run_dir.mkdir(parents=True, exist_ok=True)

    # Read text depending on type
    text = ""
    if p.suffix.lower() in {".txt", ".spec", ".sec"}:
        text = _read_text_file(p)
    elif p.suffix.lower() == ".pdf":
        diag = run_dir / "pdf_diag"
        text = _read_pdf_text_with_ocr_fallback(p, ocr_threshold=ocr_threshold, diag_dir=diag)
    else:
        raise ValueError(f"Unsupported file type: {p.suffix}")

    # Extract codes and fetch decisions
    codes_found = _extract_codes(text)
    decisions: Dict[str, Dict[str, Any]] = {}
    for code in codes_found:
        try:
            doc = db.collection("decisions").document(code).get()
            if doc.exists:
                decisions[code] = doc.to_dict() or {}
            else:
                decisions[code] = {"status": "unknown"}
        except Exception:
            decisions[code] = {"status": "error"}

    # Determine which activities to generate AHAs for
    # 1) All codes requiring AHA → infer activity per code
    activities: List[str] = []
    for code, dec in decisions.items():
        if bool((dec or {}).get("requiresAha")):
            act = _infer_activity_for_code(code, text, collection_name)
            if act:
                activities.append(act)
    # 2) Also include activities detected from the overall design text
    try:
        from generators.activity_detect import detect_activities
        detected = detect_activities(text)
        activities.extend(detected)
    except Exception:
        pass
    # stable unique preserve order
    seen = set()
    uniq_acts: List[str] = []
    for a in activities:
        if a and a not in seen:
            seen.add(a)
            uniq_acts.append(a)

    aha_files: List[str] = []
    if not classify_only:
        aha_files = _generate_ahas(uniq_acts, ahas_dir, collection_name)

    # Plans via CSV rules
    plan_triggers: List[Tuple[str, str]] = []
    plan_files: List[str] = []
    if not classify_only:
        plans_csv = Path(_project_root()) / "rules" / "safety_plans.csv"
        plan_rows = _load_plans_csv(plans_csv)
        plan_triggers = _trigger_plans(plan_rows, text, uniq_acts, codes_found)
        plan_files = _write_plans(plan_triggers, plans_dir)

    # Manifest
    manifest_path = run_dir / "manifest.json"
    if not classify_only:
        manifest = {
            "run_id": run_id,
            "input_file": str(p),
            "codes_found": codes_found,
            "decisions": decisions,
            "activities": uniq_acts,
            "ahas": aha_files,
            "plans": plan_files,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Log run summary to Firestore
    try:
        if not classify_only:
            db.collection("runs").document(run_id).set({
                "run_id": run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "input_file": str(p),
                "codes_found": codes_found,
                "ahas_generated": uniq_acts,
                "plans_triggered": [n for n, _ in plan_triggers],
                "manifest_path": str(manifest_path),
            })
    except Exception:
        pass

    # If classify-only and a code decision is missing, attempt to classify and store
    if classify_only:
        for code, dec in decisions.items():
            if not dec or dec.get("status") in {"unknown", None}:
                try:
                    req, conf, rat, cits = _rag_decide_requires_aha_for_code(code, collection_name)
                    _write_decision(db, code, {
                        "requiresAha": bool(req),
                        "confidence": float(conf),
                        "rationale": rat,
                        "citations": cits,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    pass

    return RunResult(
        run_id=run_id,
        input_file=str(p),
        codes_found=codes_found,
        decisions=decisions,
        activities=uniq_acts,
        aha_files=aha_files,
        plan_files=plan_files,
        manifest_path=str(manifest_path),
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process a design/spec document and generate AHAs and plans.")
    ap.add_argument("--input", required=True, help="Path to design spec file (.txt, .spec, .pdf)")
    ap.add_argument("--collection", dest="collection_name", default=None, help="Chroma collection name (optional)")
    ap.add_argument("--ocr-threshold", type=int, default=100, help="Min chars to skip OCR fallback for PDFs")
    ap.add_argument("--classify-only", action="store_true", help="Only classify codes and update decisions; no AHA/plan outputs")
    return ap.parse_args(argv)


def main() -> int:
    args = _parse_args()
    _ = process_design_spec(input_path=args.input, collection_name=args.collection_name, ocr_threshold=args.ocr_threshold, classify_only=bool(args.classify_only))
    if args.classify_only:
        print("[design] Classification-only completed.")
    else:
        print("[design] Completed run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())


