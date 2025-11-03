from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

from utils import (
    get_chroma_client,
    get_or_create_collection,
    add_documents_to_collection,
    get_default_chroma_dir,
)


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _read_docx(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Read DOCX and return list of paragraph-like strings and a list of table cell strings.

    Falls back gracefully if python-docx is missing.
    """
    try:
        from docx import Document  # type: ignore
    except Exception as e:
        raise RuntimeError("python-docx is required to ingest DOCX (pip install python-docx)") from e
    doc = Document(str(path))
    paras = [(p.text or "").strip() for p in getattr(doc, "paragraphs", []) or []]
    tables: List[Dict[str, Any]] = []
    for table in getattr(doc, "tables", []) or []:
        for row in table.rows:
            for cell in row.cells:
                t = (cell.text or "").strip()
                if t:
                    tables.append({"text": t})
    return paras, tables


_SEC_CODE_RE = re.compile(r"^(?P<div>\d{2})\s+(?P<s1>\d{2})\s+(?P<s2>\d{2})(?:\.(?P<s3>\d{2}))?(?:\s+[-–—]?\s*(?P<title>.*))?$")


def _detect_section_header(s: str) -> Optional[Dict[str, str]]:
    m = _SEC_CODE_RE.match((s or "").strip())
    if not m:
        return None
    div = m.group("div")
    s1 = m.group("s1")
    s2 = m.group("s2")
    s3 = m.group("s3") or None
    title = (m.group("title") or "").strip()
    section_code = f"{div} {s1} {s2}" + (f".{s3}" if s3 else "")
    return {"division": div, "section_code": section_code, "section_title": title}


def _chunk_text_with_overlap(lines: List[str], chunk_chars: int, overlap_chars: int) -> List[str]:
    chunks: List[str] = []
    buf = ""
    for line in lines:
        if buf:
            buf += "\n"
        buf += line
        if len(buf) >= chunk_chars:
            chunks.append(buf)
            # build overlap
            if overlap_chars > 0 and len(buf) > overlap_chars:
                buf = buf[-overlap_chars:]
            else:
                buf = ""
    if buf.strip():
        chunks.append(buf)
    return chunks


def ingest_msf_docx(
    docx_path: str,
    *,
    collection_name: str = "msf_index",
    chunk_chars: int = 6000,
    overlap_chars: int = 1200,
    doc_id: Optional[str] = None,
) -> int:
    """Ingest an MSF DOCX file into Chroma with rich metadata.

    Returns number of chunks inserted.
    """
    path = Path(docx_path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    paras, tables = _read_docx(path)
    # Build section-wise chunks: detect headers and accumulate lines under each section
    sections: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {"division": "", "section_code": "", "section_title": "", "lines": []}

    def _flush_current():
        if current["lines"]:
            sections.append({
                "division": current["division"],
                "section_code": current["section_code"],
                "section_title": current["section_title"],
                "lines": list(current["lines"]),
            })
            current["lines"] = []

    for p in paras:
        if not p:
            continue
        hdr = _detect_section_header(p)
        if hdr:
            # flush previous section
            _flush_current()
            current["division"] = hdr.get("division", "")
            current["section_code"] = hdr.get("section_code", "")
            current["section_title"] = hdr.get("section_title", "") or current.get("section_title", "")
            # also store the header line
            current["lines"].append(p)
        else:
            current["lines"].append(p)
    # add table text to the last section
    for t in tables:
        tt = (t.get("text") or "").strip()
        if tt:
            current["lines"].append(tt)
    # flush last
    _flush_current()

    # If no headers detected, treat entire document as one section
    if not sections:
        raw_text = "\n".join([(p or "").strip() for p in paras if (p or "").strip()])
        doc_hash = _sha256(raw_text)
        chunks = _chunk_text_with_overlap(raw_text.splitlines(), chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        sections = [{
            "division": "",
            "section_code": "",
            "section_title": "",
            "lines": chunks,
            "_pre_chunked": True,
            "_hash": doc_hash,
        }]

    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)

    ids: List[str] = []
    docs: List[str] = []
    metas: List[Dict[str, Any]] = []
    base_doc_id = doc_id or path.stem
    global_idx = 0
    for sec in sections:
        sec_lines = sec["lines"]
        if sec.get("_pre_chunked"):
            sec_chunks = sec_lines
            doc_hash = sec["_hash"]
        else:
            raw = "\n".join(sec_lines)
            doc_hash = _sha256(raw)
            sec_chunks = _chunk_text_with_overlap(sec_lines, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        for ch in sec_chunks:
            cid = f"msf_{base_doc_id}_{doc_hash[:8]}_{global_idx:05d}"
            global_idx += 1
            ids.append(cid)
            docs.append(ch)
            metas.append({
                "doc_id": base_doc_id,
                "division": sec.get("division", ""),
                "section_code": sec.get("section_code", ""),
                "section_title": sec.get("section_title", ""),
                "page_start": "",
                "page_end": "",
                "headings": "",
                "source_type": "MSF",
                "hash": doc_hash,
            })

    if ids:
        add_documents_to_collection(col, ids, docs, metas, batch_size=100)
    return len(ids)


def ingest_msf_pdf(
    pdf_path: str,
    *,
    collection_name: str = "msf_index",
    doc_id: Optional[str] = None,
    render_dpi: Optional[int] = 300,
) -> int:
    """Ingest an MSF PDF using the pdf_loader, preserving page labels and numbers.

    Returns number of chunks inserted.
    """
    from pathlib import Path as _Path
    from pdf_loader import process_pdf as _process_pdf

    p = _Path(pdf_path)
    out_json = p.with_suffix(".chunks.json")
    img_dir = p.parent / f"{p.stem}_images"
    # Use smaller chunk size to improve recall
    chunks = _process_pdf(p, out_json, img_dir, chunk_size=400, render_pages_dpi=render_dpi)

    client = get_chroma_client(get_default_chroma_dir())
    col = get_or_create_collection(client, collection_name)

    ids: List[str] = []
    docs: List[str] = []
    metas: List[Dict[str, Any]] = []
    base_doc_id = doc_id or p.stem

    for idx, ch in enumerate(chunks):
        text = (ch.get("text") or "").strip()
        if not text:
            continue
        page = ch.get("page")
        page_label = ch.get("page_label") or ""
        title = ch.get("title") or ""
        cid = f"msf_{base_doc_id}_{idx:05d}"
        ids.append(cid)
        docs.append(text)
        metas.append({
            "doc_id": base_doc_id,
            "division": "",
            "section_code": "",
            "section_title": title,
            "page_start": page,
            "page_end": page,
            "headings": title,
            "page_label": page_label,
            "source_type": "MSF",
            "hash": _sha256(text),
        })

    if ids:
        add_documents_to_collection(col, ids, docs, metas, batch_size=100)
    return len(ids)


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Ingest MSF DOCX into Chroma index with rich metadata")
    ap.add_argument("--input", required=True, help="Path to MSF .docx or .pdf file")
    ap.add_argument("--collection", default="msf_index", help="Chroma collection name")
    ap.add_argument("--chunk-chars", type=int, default=6000)
    ap.add_argument("--overlap-chars", type=int, default=1200)
    ap.add_argument("--doc-id", default=None)
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    path = Path(args.input)
    if path.suffix.lower() == ".pdf":
        n = ingest_msf_pdf(str(path), collection_name=args.collection, doc_id=args.doc_id)
        print(f"[msf-ingest] inserted {n} PDF chunks into '{args.collection}'")
    else:
        n = ingest_msf_docx(
            str(path),
            collection_name=args.collection,
            chunk_chars=max(1000, args.chunk_chars),
            overlap_chars=max(0, args.overlap_chars),
            doc_id=args.doc_id,
        )
        print(f"[msf-ingest] inserted {n} DOCX chunks into '{args.collection}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


