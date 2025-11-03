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
    FieldPattern(
        "project_name",
        [
            r"project\s+(?:name|title)",
            r"project\s+information",
            r"project",
        ],
        max_distance=4,
    ),
    FieldPattern(
        "project_number",
        [
            r"project\s+(?:number|no\.?|#)",
            r"contract\s+(?:number|no\.?|#)",
            r"contract\s+id",
        ],
        max_distance=4,
    ),
    FieldPattern(
        "location",
        [
            r"project\s+location",
            r"location",
            r"located\s+at",
            r"job\s+site",
        ],
        max_distance=4,
    ),
    FieldPattern(
        "owner",
        [
            r"owner",
            r"prepared\s+for",
            r"client",
            r"agency",
            r"customer",
        ],
        max_distance=4,
    ),
    FieldPattern(
        "prime_contractor",
        [
            r"prime\s+contractor",
            r"general\s+contractor",
            r"prepared\s+by",
            r"contractor",
        ],
        max_distance=4,
    ),
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
        if not candidate:
            continue
        # Skip lines that appear to still be labels
        if re.match(r"^[A-Z\s]+:?$", lines[start_idx + offset].strip()):
            continue
        if not _is_banned(candidate):
            return candidate
    return None


def _scan_lines(text: str, limit: int = 200) -> List[str]:
    lines = [
        ln.strip()
        for ln in (text or "").splitlines()
        if ln.strip() and not ln.strip().startswith("[[PAGE_BREAK_")
    ]
    return lines[:limit]


def _extract_candidates(lines: List[str]) -> Dict[str, str]:
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


def _fallback_from_uppercase(lines: List[str], results: Dict[str, str]) -> None:
    """Use prominent uppercase blocks as last-resort candidates."""

    def looks_like_value(block: str) -> bool:
        if not block:
            return False
        if len(block) < 4:
            return False
        letters = sum(1 for ch in block if ch.isalpha())
        if letters and sum(1 for ch in block if ch.isupper()) / letters < 0.6:
            return False
        return True

    for idx, line in enumerate(lines):
        if line in results.values():
            continue
        if not looks_like_value(line):
            continue
        prev = lines[idx - 1].lower() if idx - 1 >= 0 else ""
        if not prev:
            continue
        for field in FIELD_PATTERNS:
            if results.get(field.key):
                continue
            if any(re.search(pattern, prev) for pattern in field.labels):
                candidate = _clean_value(line)
                if candidate and not _is_banned(candidate):
                    results[field.key] = candidate


def extract_title_block_fields(text: str) -> Dict[str, str]:
    """Extract project metadata using conservative heuristics."""

    lines = _scan_lines(text, limit=240)
    results = _extract_candidates(lines)

    # If any required fields are missing, attempt uppercase fallback scanning
    if len(results) < len(FIELD_PATTERNS):
        _fallback_from_uppercase(lines, results)

    return results


__all__ = ["extract_title_block_fields", "BANNED_SUBSTRINGS"]

