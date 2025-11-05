"""Document parsing utilities for the Section 11 Generator."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, List

from pdf_loader.pdf_text import extract_text

from section11.constants import (
    DEFAULT_CATEGORY_BY_KEYWORD,
    HAZARD_KEYWORDS,
    SCOPE_LINE_KEYWORDS,
)
from section11.models import ParsedCode, ParsedSpec, SpecSourceHit


CODE_RE = re.compile(r"385[-\s]?(\d+(?:\.\d+)*)")
RANGE_RE = re.compile(r"\b(\d{3,4})\s*[–-]\s*(\d{3,4})\b")
UFGS_LINE_START_RE = re.compile(r"^(?:SECTION\s+)?(\d{2})\s+(\d{2})\s+(\d{2})(?:\b|\.|\s)")


def _expand_ranges(line: str) -> List[str]:
    tokens: List[str] = []
    if "385" not in line:
        return tokens
    for match in RANGE_RE.finditer(line):
        a = int(match.group(1))
        b = int(match.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if lo < 1 or hi > 9999 or (hi - lo) > 200:
            continue
        for value in range(lo, hi + 1):
            tokens.append(f"385-{value}")
    return tokens


def _extract_codes_with_sources(text: str) -> List[ParsedCode]:
    codes: dict[str, ParsedCode] = {}
    for idx, raw_line in enumerate((text or "").splitlines()):
        if "385" not in raw_line:
            continue
        line = raw_line.strip()
        for match in CODE_RE.finditer(line):
            code_token = f"385-{match.group(1)}"
            codes.setdefault(code_token, ParsedCode(code=code_token))
            snippet = line[:240]
            codes[code_token].sources.append(
                SpecSourceHit(page=None, heading="", excerpt=snippet)
            )
        for token in _expand_ranges(line):
            codes.setdefault(token, ParsedCode(code=token))
            codes[token].sources.append(SpecSourceHit(page=None, heading="", excerpt=line[:240]))
    return list(codes.values())


def _extract_ufgs_codes(text: str) -> Iterable[str]:
    seen: set[str] = set()
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        alpha = sum(1 for char in line if char.isalpha())
        if alpha < 6:
            continue
        match = UFGS_LINE_START_RE.search(line)
        if not match:
            continue
        a, b, c = match.group(1), match.group(2), match.group(3)
        if a in {"00", "01"}:
            continue
        token = f"UFGS-{a}-{b}-{c}"
        if token in seen:
            continue
        seen.add(token)
        yield token


def _extract_scope_lines(text: str) -> List[str]:
    summary: List[str] = []
    lowered = text.lower()
    for keyword in SCOPE_LINE_KEYWORDS:
        if keyword not in lowered:
            continue
        for line in text.splitlines():
            if keyword in line.lower():
                cleaned = line.strip()
                if cleaned and cleaned not in summary:
                    summary.append(cleaned)
    if not summary:
        parts = [line.strip() for line in text.splitlines() if line.strip()]
        summary = parts[:5]
    return summary[:10]


def _extract_hazard_phrases(text: str) -> List[str]:
    phrases: List[str] = []
    lowered = text.lower()
    for keyword in HAZARD_KEYWORDS:
        if keyword not in lowered:
            continue
        for line in text.splitlines():
            if keyword in line.lower():
                cleaned = re.sub(r"\s+", " ", line.strip())
                if cleaned and cleaned not in phrases:
                    phrases.append(cleaned)
    return phrases[:25]


def _suggest_category_from_context(code: str, snippet: str) -> str:
    haystack = f"{code} {snippet}".lower()
    for needle, category in DEFAULT_CATEGORY_BY_KEYWORD.items():
        if needle in haystack:
            return category
    return ""


def parse_document_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        pages = extract_text(path, include_tables=True)
        return "\n\n".join(pages[k] for k in sorted(pages.keys())) if pages else ""
    if suffix == ".docx":
        from docx import Document  # type: ignore

        document = Document(str(path))
        parts: List[str] = []
        for paragraph in document.paragraphs:
            text = paragraph.text.strip()
            if text:
                parts.append(text)
        for table in getattr(document, "tables", []):
            for row in table.rows:
                for cell in row.cells:
                    cell_text = cell.text.strip()
                    if cell_text:
                        parts.append(cell_text)
        return "\n".join(parts)
    return path.read_text(encoding="utf-8", errors="ignore")


def parse_spec(path: Path, work_dir: Path) -> ParsedSpec:
    work_dir.mkdir(parents=True, exist_ok=True)
    text = parse_document_text(path)
    raw_dump = work_dir / f"{path.stem}.text.json"
    raw_dump.write_text(json.dumps({"text": text}, indent=2), encoding="utf-8")

    codes = _extract_codes_with_sources(text)
    ufgs = list(_extract_ufgs_codes(text))
    for token in ufgs:
        codes.append(ParsedCode(code=token))

    for parsed in codes:
        if parsed.sources:
            parsed.suggested_category = _suggest_category_from_context(parsed.code, parsed.sources[0].excerpt)
        else:
            parsed.suggested_category = _suggest_category_from_context(parsed.code, parsed.code)

    scope_lines = _extract_scope_lines(text)
    hazard_phrases = _extract_hazard_phrases(text)

    return ParsedSpec(
        scope_summary=scope_lines,
        codes=codes,
        hazard_phrases=hazard_phrases,
        raw_text_path=raw_dump,
    )

