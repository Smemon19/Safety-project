from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass, field
import time
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


def _read_docx_text(docx_path: Path) -> str:
    """Extract text from a DOCX, including paragraphs and tables.

    Falls back gracefully if python-docx is missing.
    """
    try:
        from docx import Document  # type: ignore
    except Exception:
        # Advise installing python-docx in logs; return empty to avoid crash
        print("[docx] python-docx not installed; please add python-docx to requirements.txt")
        return ""
    try:
        doc = Document(str(docx_path))
    except Exception as e:
        print(f"[docx] Failed to open {docx_path}: {e}")
        return ""
    parts: List[str] = []
    # paragraphs
    for p in getattr(doc, "paragraphs", []) or []:
        t = (p.text or "").strip()
        if t:
            parts.append(t)
    # tables
    try:
        for table in getattr(doc, "tables", []) or []:
            for row in table.rows:
                for cell in row.cells:
                    ct = (cell.text or "").strip()
                    if ct:
                        parts.append(ct)
    except Exception:
        pass
    return "\n".join(parts)


CODE_RE = re.compile(r"385[-\s]?(\d+(?:\.\d+)*)")
RANGE_RE = re.compile(r"\b(\d{3,4})\s*[–-]\s*(\d{3,4})\b")

# UFGS section like "07 84 00" (optionally with trailing .xx groups we ignore for token)
# UFGS section like "07 84 00" (accept only at line start or after 'SECTION ')
UFGS_LINE_START_RE = re.compile(r"^(?:SECTION\s+)?(\d{2})\s+(\d{2})\s+(\d{2})(?:\b|\.|\s)")


def _expand_ranges(text: str) -> List[str]:
    """Expand numeric ranges only when clearly in EM 385 context to avoid explosion.

    We require the token '385' to appear in the same line as the range to treat it as an EM 385 range.
    """
    out: List[str] = []
    for raw in (text or "").splitlines():
        if "385" not in raw:
            continue
        for m in RANGE_RE.finditer(raw):
            a = int(m.group(1))
            b = int(m.group(2))
            lo, hi = (a, b) if a <= b else (b, a)
            # Clamp to plausible EM385 numeric window to avoid degenerate expansions
            if 1 <= lo <= 9999 and 1 <= hi <= 9999 and (hi - lo) <= 200:
                for v in range(lo, hi + 1):
                    out.append(f"385-{v}")
    return out


def _extract_codes(text: str) -> List[str]:
    """Extract EM 385 codes conservatively using line-level context.

    - Only consider lines that mention '385' to reduce false positives.
    - Extract explicit tokens like 385-1016 and expand ranges on those lines.
    """
    codes: List[str] = []
    for raw in (text or "").splitlines():
        if "385" not in raw:
            continue
        for m in CODE_RE.finditer(raw):
            codes.append(f"385-{m.group(1)}")
    # ranges like 1012–1016 but only on lines that had '385'
    codes.extend(_expand_ranges(text or ""))
    # stable unique
    seen = set()
    out: List[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _extract_ufgs_codes(text: str, include_admin: bool = False) -> List[str]:
    """Extract UFGS MasterFormat codes like '07 84 00' and normalize to 'UFGS-07-84-00'.

    Heuristics:
    - Work line-by-line; require at least some alpha text later in the line to indicate a title/section name.
    - Ignore obvious date fragments (e.g., lines dominated by MM/YY or change markers) by requiring >= 6 alpha characters in line.
    """
    out: List[str] = []
    seen = set()
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        # require some letters to avoid matching random date triplets
        alpha_count = sum(1 for c in line if c.isalpha())
        if alpha_count < 6:
            continue
        m = UFGS_LINE_START_RE.search(line)
        if not m:
            continue
        a, b, c = m.group(1), m.group(2), m.group(3)
        # Filter admin divisions unless explicitly included
        if not include_admin and a in {"00", "01"}:
            continue
        # Normalize token and record
        token = f"UFGS-{a}-{b}-{c}"
        if token not in seen:
            seen.add(token)
            out.append(token)
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


def _extract_project_meta(text: str) -> Dict[str, str]:
    """Heuristic extraction of project metadata from free text.

    Looks for lines like:
      Project: NAME
      Project Name: NAME
      Project Number: 1234
      Location: CITY, STATE
      Owner: X
      GC: Y
    """
    meta = {
        "project_name": "",
        "project_number": "",
        "location": "",
        "owner": "",
        "gc": "",
    }
    lines = (text or "").splitlines()
    for raw in lines:
        line = raw.strip()
        low = line.lower()
        def _val(prefix: str) -> str:
            return line.split(":", 1)[1].strip() if ":" in line else line[len(prefix):].strip()
        if low.startswith("project name:") or low.startswith("project:"):
            meta["project_name"] = _val("project name:") if ":" in line.lower() else _val("project:")
        elif low.startswith("project number:") or low.startswith("project #:") or low.startswith("project no:"):
            meta["project_number"] = _val("project number:")
        elif low.startswith("location:"):
            meta["location"] = _val("location:")
        elif low.startswith("owner:"):
            meta["owner"] = _val("owner:")
        elif low.startswith("gc:") or low.startswith("general contractor:"):
            meta["gc"] = _val("gc:") if low.startswith("gc:") else _val("general contractor:")
    return meta


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


def _finalize_aha_doc(
    aha: Any,
    label: str,
    out_dir: Path,
    metrics: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    from export.docx_writer import write_aha_single
    from export.markdown_writer import write_aha_single_md

    metrics.setdefault("per_activity", {})
    detail = metrics["per_activity"].setdefault(label, {})

    dedup: List[Any] = []
    seen_keys = set()
    before_cits = len(getattr(aha, "citations", []) or [])
    for c in list(getattr(aha, "citations", []) or []):
        key = (
            (c.section_path or "").strip().lower(),
            (c.page_label or str(c.page_number) or "").strip().lower(),
            (c.quote_anchor or "").strip().lower(),
        )
        if key in seen_keys:
            metrics["citations_dropped"] += 1
            continue
        seen_keys.add(key)
        dedup.append(c)
    kept = len(dedup)
    dropped = max(0, before_cits - kept)
    metrics["citations_kept"] += kept
    metrics["citations_dropped"] += dropped
    aha.citations = dedup
    detail.update({
        "citations_kept": kept,
        "citations_dropped": dropped,
    })

    def _collect_lists(a: Any) -> tuple[list[str], list[str]]:
        all_ppe: list[str] = []
        all_permits: list[str] = []
        for it in getattr(a, "items", []) or []:
            all_ppe.extend(getattr(it, "ppe", []) or [])
            all_permits.extend(getattr(it, "permits_training", []) or [])
        return all_ppe, all_permits

    all_ppe, all_permits = _collect_lists(aha)
    low = "\n".join(all_ppe).lower()
    ppe_ok = ("eye" in low or "z87" in low) and ("hard hat" in low or "z89" in low) and ("glove" in low or "cut" in low)
    if "electrical" in (getattr(aha, "activity", "") or "").lower():
        ppe_ok = ppe_ok and ("arc" in low or "flash" in low)
    pt_low = "\n".join(all_permits).lower()
    permits_ok = any(k in pt_low for k in ["loto", "confined space", "permit", "lift plan", "hot work"])
    roles_ok = any(k in pt_low for k in ["competent person", "qualified", "supervisor"])
    detail.update({
        "ppe_ok": bool(ppe_ok),
        "permits_ok": bool(permits_ok),
        "roles_ok": bool(roles_ok),
    })

    out_dir.mkdir(parents=True, exist_ok=True)
    slug = _slug(label or getattr(aha, "activity", "") or "aha") or "aha"
    docx_path = out_dir / f"{slug}.docx"
    md_path = out_dir / f"{slug}.md"
    docx_written: Optional[str] = None
    md_written: Optional[str] = None
    try:
        docx_written = write_aha_single(aha, str(docx_path))
    except Exception as exc:
        print(f"[aha] Failed to write DOCX for {label}: {exc}")
    try:
        md_written = write_aha_single_md(aha, str(md_path))
    except Exception as exc:
        print(f"[aha] Failed to write markdown for {label}: {exc}")
    return docx_written, md_written


def _merge_metrics(dest: Dict[str, Any], src: Dict[str, Any]) -> None:
    if not src:
        return
    dest.setdefault("citations_kept", 0)
    dest.setdefault("citations_dropped", 0)
    dest.setdefault("cleanup_removed_lines", 0)
    dest.setdefault("per_activity", {})

    dest["citations_kept"] += int(src.get("citations_kept", 0))
    dest["citations_dropped"] += int(src.get("citations_dropped", 0))
    dest["cleanup_removed_lines"] += int(src.get("cleanup_removed_lines", 0))

    per_dest = dest.setdefault("per_activity", {})
    for label, data in (src.get("per_activity", {}) or {}).items():
        existing = per_dest.setdefault(label, {})
        for key, value in (data or {}).items():
            if key in {"citations_kept", "citations_dropped"}:
                existing[key] = int(existing.get(key, 0)) + int(value)
            elif key in {"ppe_ok", "permits_ok", "roles_ok"}:
                existing[key] = bool(existing.get(key, False)) or bool(value)
            else:
                existing[key] = value


def _generate_ahas(
    activities: List[str],
    out_dir: Path,
    collection_name: Optional[str],
    msf_doc_id: Optional[str],
) -> Tuple[List[str], List[str], List[Any], Dict[str, Any]]:
    from generators.aha import generate_full_aha

    out_docx: List[str] = []
    out_markdown: List[str] = []
    aha_docs: List[Any] = []
    metrics: Dict[str, Any] = {
        "citations_kept": 0,
        "citations_dropped": 0,
        "cleanup_removed_lines": 0,
        "per_activity": {}
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    for act in activities:
        print(f"[aha] EM385 min-sim ~0.40; MSF min-sim 0.35 (informational)")
        aha = generate_full_aha(act, collection_name or "", msf_doc_id=msf_doc_id)
        aha_docs.append(aha)
        docx_written, md_written = _finalize_aha_doc(aha, act, out_dir, metrics)
        if docx_written:
            out_docx.append(docx_written)
        if md_written:
            out_markdown.append(md_written)
    return out_docx, out_markdown, aha_docs, metrics


def _trigger_plans(rows: List[Dict[str, str]], design_text: str, activities: List[str], codes_found: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    s = (design_text or "").lower()
    # Expand synonyms to improve matching
    synonyms = {
        "prcs": "confined space",
        "permit-required confined space": "confined space",
        "energized work": "electrical",
        "loto": "electrical",
        "rigging": "crane",
        "lifting": "crane",
        "hot work": "welding",
        "cutting": "welding",
    }
    s_aug = s
    for k, v in synonyms.items():
        if k in s:
            s_aug += f" {v}"
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
        if ttype == "contains_text" and patt and (patt in s_aug):
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
    aha_markdown_files: List[str]
    plan_files: List[str]
    manifest_path: str
    msf_doc_id: Optional[str] = None
    auto_classified_codes: List[str] = field(default_factory=list)
    code_decisions: List[Dict[str, Any]] = field(default_factory=list)


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


def process_design_spec(
    input_path: str,
    collection_name: Optional[str],
    ocr_threshold: int = 100,
    classify_only: bool = False,
    aha_mode: str = "activity",
    coverage_enforce: str = "warn",
    fs_batch_size: int = 100,
    fs_max_retries: int = 5,
    fs_backoff_base: float = 0.5,
    fs_between_batches_sleep: float = 0.5,
    write_partials: bool = True,
    include_admin_ufgs: bool = False,
    msf_doc_id: Optional[str] = None,
    auto_classify_unknown: bool = True,
) -> RunResult:
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

    def _write_partial_manifest(stage: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if not write_partials or classify_only:
            return
        try:
            payload = {
                "run_id": run_id,
                "stage": stage,
                "ts": datetime.now(timezone.utc).isoformat(),
                "input_file": str(p),
            }
            if extra:
                payload.update(extra)
            (run_dir / "manifest.partial.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass

    # Read text depending on type
    text = ""
    if p.suffix.lower() in {".txt", ".spec", ".sec"}:
        text = _read_text_file(p)
    elif p.suffix.lower() == ".pdf":
        diag = run_dir / "pdf_diag"
        text = _read_pdf_text_with_ocr_fallback(p, ocr_threshold=ocr_threshold, diag_dir=diag)
    elif p.suffix.lower() == ".docx":
        text = _read_docx_text(p)
    else:
        raise ValueError(f"Unsupported file type: {p.suffix}")

    # Extract codes and fetch decisions (batched for speed + reliability)
    codes_em385 = _extract_codes(text)
    # Exclude admin divisions 00/01 by default to reduce false positives in TOC
    codes_ufgs = _extract_ufgs_codes(text, include_admin=include_admin_ufgs)
    codes_found = codes_em385 + codes_ufgs
    print(f"[design] extracted codes: em385={len(codes_em385)}, ufgs={len(codes_ufgs)}, total={len(codes_found)}", flush=True)
    decisions: Dict[str, Dict[str, Any]] = {}
    decisions_mapped: Dict[str, bool] = {}
    if codes_found:
        from math import ceil
        total = len(codes_found)
        bs = max(1, int(fs_batch_size))
        batches = ceil(total / bs)
        print(f"[design] fetching decisions in {batches} batch(es) (batch_size={bs})…", flush=True)
        for i in range(batches):
            start = i * bs
            end = min(total, start + bs)
            refs = [db.collection("decisions").document(code) for code in codes_found[start:end]]
            # retry with backoff on quota/timeouts
            attempt = 0
            t0 = time.perf_counter()
            while True:
                try:
                    snaps = db.get_all(refs)
                    took = time.perf_counter() - t0
                    ok = 0
                    unk = 0
                    for code, snap in zip(codes_found[start:end], snaps):
                        if getattr(snap, "exists", False):
                            val = dict(snap.to_dict() or {})
                            val.setdefault("status", "firestore")
                            decisions[code] = val
                            ok += 1
                            if "requiresAha" in val:
                                decisions_mapped[code] = bool(val["requiresAha"])
                        else:
                            decisions[code] = {"status": "unknown"}
                            unk += 1
                    print(f"[design] batch {i+1}/{batches} fetched {ok} known, {unk} unknown in {took:.2f}s", flush=True)
                    _write_partial_manifest("decisions_batch", {"batch_index": i+1, "batches": batches, "fetched_known": ok, "fetched_unknown": unk})
                    break
                except Exception as e:
                    attempt += 1
                    if attempt > max(0, int(fs_max_retries)):
                        print(f"[design] batch {i+1}/{batches} failed after {attempt-1} retries: {e}; falling back to per-doc", flush=True)
                        # per-doc fallback with tiny delay
                        ok = 0
                        unk = 0
                        err = 0
                        for code in codes_found[start:end]:
                            try:
                                snap = db.collection("decisions").document(code).get()
                                if getattr(snap, "exists", False):
                                    val = dict(snap.to_dict() or {})
                                    val.setdefault("status", "firestore")
                                    decisions[code] = val
                                    ok += 1
                                    if "requiresAha" in val:
                                        decisions_mapped[code] = bool(val["requiresAha"])
                                else:
                                    decisions[code] = {"status": "unknown"}
                                    unk += 1
                            except Exception:
                                decisions[code] = {"status": "error"}
                                err += 1
                            time.sleep(0.05)
                        print(f"[design] per-doc fallback batch {i+1}/{batches}: ok={ok}, unknown={unk}, error={err}", flush=True)
                        _write_partial_manifest("decisions_perdoc_batch", {"batch_index": i+1, "batches": batches, "ok": ok, "unknown": unk, "error": err})
                        break
                    sleep_s = (fs_backoff_base or 0.5) * (2 ** (attempt - 1))
                    sleep_s = min(8.0, sleep_s)
                    print(f"[design] batch {i+1}/{batches} retry {attempt} in {sleep_s:.2f}s due to: {e}", flush=True)
                    time.sleep(sleep_s)
            # gentle pacing between batches
            if fs_between_batches_sleep and i < (batches - 1):
                time.sleep(max(0.0, float(fs_between_batches_sleep)))

    auto_classified_codes: List[str] = []
    inferred_activity_by_code: Dict[str, str] = {}

    def _cache_inferred_activity(code_token: str) -> Optional[str]:
        if code_token in inferred_activity_by_code:
            return inferred_activity_by_code[code_token]
        act_val = _infer_activity_for_code(code_token, text, collection_name)
        if act_val:
            inferred_activity_by_code[code_token] = act_val
        return act_val

    if auto_classify_unknown and not classify_only and codes_found:
        for code in codes_found:
            dec = decisions.get(code, {}) or {}
            if "requiresAha" in dec and dec.get("status") not in {"unknown", None}:
                continue
            try:
                req, conf, rat, cits = _rag_decide_requires_aha_for_code(code, collection_name)
            except Exception as exc:
                print(f"[design] auto-classify failed for {code}: {exc}")
                continue
            updated = dict(dec)
            updated.update({
                "requiresAha": bool(req),
                "confidence": float(conf),
                "rationale": rat,
                "citations": cits,
                "status": "auto",
            })
            decisions[code] = updated
            if "requiresAha" in updated:
                decisions_mapped[code] = bool(updated["requiresAha"])
                if bool(updated["requiresAha"]):
                    _cache_inferred_activity(code)
            auto_classified_codes.append(code)
        auto_classified_codes = sorted({c for c in auto_classified_codes if c})

    # Determine which activities to generate AHAs for
    # 1) All codes requiring AHA → infer activity per code
    activities: List[str] = []
    for code, dec in decisions.items():
        if bool((dec or {}).get("requiresAha")):
            act = _cache_inferred_activity(code)
            if act:
                activities.append(act)
    # 2) Also include activities detected from the overall design text
    try:
        from generators.activity_detect import detect_activities
        detected = detect_activities(text)
        activities.extend(detected)
    except Exception:
        pass
    # 3) MSF-grounded activity detection (limit activities to those found in MSF index for this doc, if available)
    msf_map: Dict[str, List[Dict[str, Any]]] = {}
    msf_doc_id_effective = msf_doc_id or Path(input_path).stem
    try:
        from generators.activity_msf import detect_activities_from_msf
        msf_map = detect_activities_from_msf(doc_id=msf_doc_id_effective)
    except Exception:
        msf_map = {}
    # stable unique preserve order
    seen = set()
    uniq_acts: List[str] = []
    # Prefer MSF-detected activities when available
    if msf_map:
        activities = list(msf_map.keys())
    for a in activities:
        if a and a not in seen:
            seen.add(a)
            uniq_acts.append(a)

    # Coverage metrics for codes requiring an AHA
    codes_requiring = {c for c, d in decisions.items() if bool((d or {}).get("requiresAha"))}
    # Map activities to covered codes via simple inference
    act_to_codes: Dict[str, List[str]] = {a: [] for a in uniq_acts}
    for c in list(codes_requiring):
        a = _cache_inferred_activity(c)
        if a and a in act_to_codes:
            act_to_codes[a].append(c)
    codes_covered = set()
    for lst in act_to_codes.values():
        codes_covered.update(lst)
    code_to_activity: Dict[str, str] = {}
    for act_name, code_list in act_to_codes.items():
        for code_token in code_list:
            if code_token not in code_to_activity:
                code_to_activity[code_token] = act_name
    codes_uncovered = sorted(list(codes_requiring - codes_covered))

    warnings: List[str] = []
    if auto_classified_codes:
        warnings.append(f"Auto-classified {len(auto_classified_codes)} code(s) via RAG heuristic (review recommended).")
    aha_files: List[str] = []
    aha_markdown_files: List[str] = []
    aha_docs: List[Any] = []
    aha_metrics: Dict[str, Any] = {"citations_kept": 0, "citations_dropped": 0, "cleanup_removed_lines": 0, "per_activity": {}}
    if not classify_only:
        if aha_mode == "code":
            # One AHA per code requiring AHA
            from generators.aha import generate_full_aha
            msf_id = msf_doc_id_effective if msf_doc_id_effective else None
            for code in sorted(codes_requiring):
                act = _infer_activity_for_code(code, text, collection_name) or "General"
                aha = generate_full_aha(act, collection_name or "", msf_doc_id=msf_id)
                aha.name = f"AHA - {code} ({act})"
                try:
                    aha.codes_covered = [code]  # type: ignore[attr-defined]
                except Exception:
                    pass
                aha_docs.append(aha)
                label = f"{code} {act}".strip()
                docx_written, md_written = _finalize_aha_doc(aha, label, ahas_dir, aha_metrics)
                if docx_written:
                    aha_files.append(docx_written)
                if md_written:
                    aha_markdown_files.append(md_written)
            if not aha_docs and uniq_acts:
                fallback_docx, fallback_md, fallback_docs, fallback_metrics = _generate_ahas(
                    uniq_acts,
                    ahas_dir,
                    collection_name,
                    msf_doc_id_effective if msf_doc_id_effective else None,
                )
                aha_docs.extend(fallback_docs)
                aha_files.extend(fallback_docx)
                aha_markdown_files.extend(fallback_md)
                _merge_metrics(aha_metrics, fallback_metrics)
                warnings.append("No curated AHA decisions found; generated AHAs from detected activities as fallback.")
        else:
            aha_files, aha_markdown_files, aha_docs, aha_metrics = _generate_ahas(uniq_acts, ahas_dir, collection_name, msf_doc_id_effective if msf_doc_id_effective else None)

    code_decision_summary: List[Dict[str, Any]] = []
    for code in codes_found:
        dec = decisions.get(code, {}) or {}
        requires_val = dec.get("requiresAha")
        requires_bool: Optional[bool]
        if isinstance(requires_val, bool):
            requires_bool = requires_val
        elif requires_val is None:
            requires_bool = None
        else:
            requires_bool = bool(requires_val)
        status_raw = dec.get("status") or ("auto" if code in auto_classified_codes else ("firestore" if "requiresAha" in dec else "unknown"))
        status = str(status_raw)
        aha_generated = bool(requires_bool) and (((aha_mode == "code") and (code in codes_requiring)) or (code in codes_covered))

        activity_label = code_to_activity.get(code, "")
        activity_source = "coverage_map" if activity_label else ""
        if not activity_label:
            for key in ("activity", "activityLabel", "ahaActivity"):
                val = dec.get(key)
                if val:
                    activity_label = str(val)
                    activity_source = f"decision:{key}"
                    break
        if not activity_label and isinstance(dec.get("activities"), list):
            if dec.get("activities"):
                activity_label = " / ".join(str(x) for x in dec["activities"] if x)
                if activity_label:
                    activity_source = "decision:activities"
        if not activity_label and code in inferred_activity_by_code:
            activity_label = inferred_activity_by_code.get(code, "") or ""
            if activity_label:
                activity_source = "inferred"
        if not activity_label:
            activity_label = ""

        summary_entry: Dict[str, Any] = {
            "code": code,
            "requires_aha": requires_bool,
            "decision_source": status,
            "confidence": dec.get("confidence"),
            "activity": activity_label,
            "activity_source": activity_source,
            "aha_generated": aha_generated,
        }
        if "rationale" in dec and dec.get("rationale"):
            summary_entry["rationale"] = dec.get("rationale")
        code_decision_summary.append(summary_entry)

    # Plans are disabled for now; only CSP and AHA Book are produced
    plan_triggers: List[Tuple[str, str]] = []
    plan_files: List[str] = []

    # Build CSP and AHA Book artifacts (optional but enabled here)
    csp_docx_path = ""
    csp_md_path = ""
    aha_book_docx_path = ""
    aha_book_md_path = ""
    project_meta = _extract_project_meta(text)
    for k in ["project_name", "location", "owner", "gc"]:
        if not (project_meta.get(k) or "").strip():
            warnings.append(f"Missing {k} in spec text; using default placeholder.")
    def _is_present(val: str) -> bool:
        v = (val or "").strip()
        return bool(v) and v.lower() not in {"project", "owner", "gc"}
    placeholders_filled = sum(1 for k in ["project_name","location","owner","gc"] if _is_present(project_meta.get(k, "")))
    incomplete = placeholders_filled < 4
    if not classify_only and (uniq_acts or aha_docs):
        try:
            from generators.csp import generate_csp
            from export.docx_writer import write_aha_book, write_csp_docx
            from export.markdown_writer import write_aha_book_md, write_csp_md
            # Lightweight spec metadata placeholder; UI can pass richer JSON later
            acts_for_csp: List[str] = []
            if uniq_acts:
                acts_for_csp = list(uniq_acts)
            elif aha_docs:
                seen_act: set[str] = set()
                for doc in aha_docs:
                    act_label = (getattr(doc, "activity", "") or "General Activity").strip()
                    if act_label not in seen_act:
                        seen_act.add(act_label)
                        acts_for_csp.append(act_label)

            spec_obj = {
                "project_name": project_meta.get("project_name") or "Project",
                "project_number": project_meta.get("project_number") or "",
                "location": project_meta.get("location") or "",
                "owner": project_meta.get("owner") or "",
                "gc": project_meta.get("gc") or "",
                "work_packages": ([{"title": "Detected Activities", "activities": acts_for_csp}] if acts_for_csp else []),
                "deliverables": [],
                "assumptions": [],
            }
            csp = generate_csp(spec_obj, collection_name or "")
            csp_docx_path = str(run_dir / "CSP.docx")
            csp_md_path = str(run_dir / "CSP.md")
            write_csp_docx(csp, csp_docx_path)
            write_csp_md(csp, csp_md_path)
            # AHA Book
            if aha_docs:
                aha_book_docx_path = str(run_dir / "AHA_Book.docx")
                aha_book_md_path = str(run_dir / "AHA_Book.md")
                try:
                    write_aha_book(aha_docs, aha_book_docx_path)
                except Exception:
                    pass
                try:
                    write_aha_book_md(aha_docs, aha_book_md_path)
                except Exception:
                    pass
        except Exception:
            pass

    # Manifest
    manifest_path = run_dir / "manifest.json"
    if not classify_only and (incomplete or not uniq_acts):
        warnings.append("Incomplete project header (Project/Owner/GC/Location missing). Skipping CSP/AHA Book exports.")

    if not classify_only:
        manifest = {
            "run_id": run_id,
            "input_file": str(p),
            "codes_found": codes_found,
            "codes_found_em385": codes_em385,
            "codes_found_ufgs": codes_ufgs,
            "decisions": decisions,
            "decisions_mapped": decisions_mapped,
            "activities": uniq_acts,
            "msf_activity_sections": msf_map,
            "ahas": aha_files,
            "aha_markdown": aha_markdown_files,
            "plans": plan_files,
            "csp_docx": csp_docx_path,
            "csp_md": csp_md_path,
            "aha_book_docx": aha_book_docx_path,
            "aha_book_md": aha_book_md_path,
            "project_meta": project_meta,
            "msf_doc_id": msf_doc_id_effective,
            "auto_classified_codes": auto_classified_codes,
            "code_decisions": code_decision_summary,
            "warnings": warnings,
            "metrics": {
                "placeholders_filled": placeholders_filled,
                "plans_triggered": len(plan_triggers),
                "activities_detected": len(uniq_acts),
                "ahas_generated": len(aha_docs) if aha_docs else 0,
                "citations_kept": aha_metrics.get("citations_kept", 0),
                "citations_dropped": aha_metrics.get("citations_dropped", 0),
                "cleanup_removed_lines": aha_metrics.get("cleanup_removed_lines", 0),
                "citations_per_activity": aha_metrics.get("per_activity", {}),
                "codes_requiring_aha_total": len(codes_requiring),
                "codes_covered_in_ahas": len(codes_covered),
                "codes_uncovered": codes_uncovered,
            },
        }
        if codes_uncovered:
            warnings.append(f"Codes uncovered: {len(codes_uncovered)}")
        if msf_map and len(aha_docs) != len(msf_map.keys()):
            warnings.append("AHA/MSF activity parity mismatch: generated != MSF activities.")
        if coverage_enforce == "fail" and codes_uncovered:
            warnings.append("Coverage enforcement: fail. Skipping CSP/AHA outputs due to uncovered codes.")
            # remove paths so UI won't offer downloads
            manifest["ahas"] = []
            manifest["aha_markdown"] = []
            manifest["csp_docx"] = ""
            manifest["csp_md"] = ""
            manifest["aha_book_docx"] = ""
            manifest["aha_book_md"] = ""
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Optional: Upload outputs to Firebase Storage if configured
    storage_links: Dict[str, Any] = {}
    if not classify_only:
        bucket_name = (os.getenv("FIREBASE_STORAGE_BUCKET") or "").strip()
        if bucket_name:
            try:
                from firebase_admin import storage
                b = storage.bucket(bucket_name)
                def _upload(path_str: str, dest_prefix: str) -> Optional[str]:
                    if not path_str:
                        return None
                    try:
                        path = Path(path_str)
                        if not path.exists():
                            return None
                        blob = b.blob(f"runs/{run_id}/{dest_prefix}/{path.name}")
                        blob.upload_from_filename(str(path))
                        return f"gs://{bucket_name}/{blob.name}"
                    except Exception:
                        return None
                # Upload main artifacts
                storage_links["manifest"] = _upload(str(manifest_path), "") or ""
                storage_links["csp_docx"] = _upload(csp_docx_path, "") or ""
                storage_links["csp_md"] = _upload(csp_md_path, "") or ""
                storage_links["aha_book_docx"] = _upload(aha_book_docx_path, "") or ""
                storage_links["aha_book_md"] = _upload(aha_book_md_path, "") or ""
                # Upload AHAs
                for f in aha_files:
                    link = _upload(f, "ahas")
                    if link:
                        storage_links.setdefault("ahas", []).append(link)
                for f in aha_markdown_files:
                    link = _upload(f, "ahas_markdown")
                    if link:
                        storage_links.setdefault("ahas_markdown", []).append(link)
            except Exception:
                pass

    # Log run summary to Firestore
    try:
        if not classify_only:
            doc_payload = {
                "run_id": run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "input_file": str(p),
                "codes_found": codes_found,
                "ahas_generated": uniq_acts,
                "plans_triggered": [n for n, _ in plan_triggers],
                "manifest_path": str(manifest_path),
                "csp_docx": csp_docx_path,
                "csp_md": csp_md_path,
                "aha_book_docx": aha_book_docx_path,
                "aha_book_md": aha_book_md_path,
                "aha_files": aha_files,
                "aha_markdown_files": aha_markdown_files,
                "msf_doc_id": msf_doc_id_effective,
                "auto_classified_codes": auto_classified_codes,
                "code_decisions": code_decision_summary,
                "project_meta": project_meta,
                "decisions_mapped": decisions_mapped,
                "metrics": {
                    "placeholders_filled": placeholders_filled,
                    "plans_triggered": len(plan_triggers),
                    "activities_detected": len(uniq_acts),
                    "ahas_generated": len(aha_docs) if aha_docs else 0,
                    "citations_kept": aha_metrics.get("citations_kept", 0),
                    "citations_dropped": aha_metrics.get("citations_dropped", 0),
                    "cleanup_removed_lines": aha_metrics.get("cleanup_removed_lines", 0),
                    "citations_per_activity": aha_metrics.get("per_activity", {}),
                },
            }
            if storage_links:
                doc_payload["storage_links"] = storage_links
            db.collection("runs").document(run_id).set(doc_payload)
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
        aha_markdown_files=aha_markdown_files,
        plan_files=plan_files,
        manifest_path=str(manifest_path),
        msf_doc_id=msf_doc_id_effective,
        auto_classified_codes=auto_classified_codes,
        code_decisions=code_decision_summary,
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process a design/spec document and generate AHAs and plans.")
    ap.add_argument("--input", required=True, help="Path to design spec file (.txt, .spec, .pdf)")
    ap.add_argument("--collection", dest="collection_name", default=None, help="Chroma collection name (optional)")
    ap.add_argument("--ocr-threshold", type=int, default=100, help="Min chars to skip OCR fallback for PDFs")
    ap.add_argument("--classify-only", action="store_true", help="Only classify codes and update decisions; no AHA/plan outputs")
    ap.add_argument("--aha-mode", choices=["activity", "code"], default="activity", help="AHA generation mode: activity (default) or code")
    ap.add_argument("--coverage-enforce", choices=["warn", "fail"], default="warn", help="Warn or fail when some requiring-AHA codes are uncovered")
    ap.add_argument("--include-admin-ufgs", action="store_true", help="Include Division 00/01 UFGS codes in extraction")
    ap.add_argument("--msf-doc-id", default=None, help="Explicit MSF doc_id to use for project-specific retrieval")
    ap.add_argument("--fs-batch-size", type=int, default=100, help="Firestore decision read batch size")
    ap.add_argument("--fs-max-retries", type=int, default=5, help="Max retries per batch on Firestore errors")
    ap.add_argument("--fs-backoff-base", type=float, default=0.5, help="Base seconds for exponential backoff between retries")
    ap.add_argument("--fs-between-batches-sleep", type=float, default=0.5, help="Sleep seconds between batches to avoid 429s")
    ap.add_argument("--write-partials", action="store_true", help="Write partial manifest updates while running")
    ap.add_argument("--no-auto-classify", dest="auto_classify_unknown", action="store_false", help="Disable automatic RAG classification for unknown codes")
    ap.set_defaults(auto_classify_unknown=True)
    return ap.parse_args(argv)


def main() -> int:
    args = _parse_args()
    _ = process_design_spec(
        input_path=args.input,
        collection_name=args.collection_name,
        ocr_threshold=args.ocr_threshold,
        classify_only=bool(args.classify_only),
        aha_mode=args.aha_mode,
        coverage_enforce=args.coverage_enforce,
        fs_batch_size=args.fs_batch_size,
        fs_max_retries=args.fs_max_retries,
        fs_backoff_base=args.fs_backoff_base,
        fs_between_batches_sleep=args.fs_between_batches_sleep,
        write_partials=bool(args.write_partials),
        include_admin_ufgs=bool(args.include_admin_ufgs),
        msf_doc_id=args.msf_doc_id,
        auto_classify_unknown=bool(args.auto_classify_unknown),
    )
    if args.classify_only:
        print("[design] Classification-only completed.")
    else:
        print("[design] Completed run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())


