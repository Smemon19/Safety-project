"""Structured extraction of title block metadata from sanitized documents."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


BANNED_SUBSTRINGS = [
    "table of contents",
    "for receipt by the contracting officer",
    "quality control approval. submit the following",
    "update the table of contents",
    "add absorbent material",
]


@dataclass(slots=True)
class FieldPattern:
    key: str
    labels: Iterable[str]
    max_distance: int = 1  # how many lines we may look ahead for the value


FIELD_PATTERNS: List[FieldPattern] = [
    FieldPattern("project_name", [r"project\s+(?:name|title)", r"project"], max_distance=2),
    FieldPattern("project_number", [r"project\s+(?:number|no\.?|#)", r"contract\s+(?:number|no\.?|#)"], max_distance=2),
    FieldPattern("location", [r"project\s+location", r"location", r"located\s+at"], max_distance=2),
    FieldPattern("owner", [r"owner", r"prepared\s+for", r"client", r"agency"], max_distance=2),
    FieldPattern("prime_contractor", [r"prime\s+contractor", r"general\s+contractor", r"prepared\s+by"], max_distance=2),
]


def _clean_value(raw: str) -> str:
    value = raw.strip().strip("-:—")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _is_banned(value: str) -> bool:
    low = value.lower()
    return any(term in low for term in BANNED_SUBSTRINGS)


def _extract_from_line(line: str, label_pattern: str) -> Optional[str]:
    match = re.search(rf"{label_pattern}\s*[:\-–—]?\s*(.+)$", line, flags=re.IGNORECASE)
    if match:
        candidate = _clean_value(match.group(1))
        if candidate and not _is_banned(candidate):
            return candidate
    return None


def _find_next_value(lines: List[str], start_idx: int, max_distance: int) -> Optional[str]:
    for offset in range(1, max_distance + 1):
        if start_idx + offset >= len(lines):
            break
        candidate = _clean_value(lines[start_idx + offset])
        if candidate and not _is_banned(candidate):
            return candidate
    return None


def extract_title_block_fields(text: str) -> Dict[str, str]:
    """Extract project metadata using conservative heuristics."""

    lines = [
        ln.strip()
        for ln in (text or "").splitlines()
        if ln.strip() and not ln.strip().startswith("[[PAGE_BREAK_")
    ]
    # Focus on the first ~120 lines (cover and executive summary)
    lines = lines[:120]
    results: Dict[str, str] = {}

    for idx, line in enumerate(lines):
        normalized = line.lower()
        for field in FIELD_PATTERNS:
            if results.get(field.key):
                continue
            if any(re.search(pattern, normalized) for pattern in field.labels):
                # Attempt same-line extraction
                for pattern in field.labels:
                    value = _extract_from_line(line, pattern)
                    if value:
                        results[field.key] = value
                        break
                if results.get(field.key):
                    continue
                # Look ahead if label only
                value = _find_next_value(lines, idx, field.max_distance)
                if value:
                    results[field.key] = value
    return results


__all__ = ["extract_title_block_fields", "BANNED_SUBSTRINGS"]

