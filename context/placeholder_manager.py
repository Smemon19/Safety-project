from __future__ import annotations

"""Helpers for managing placeholder text across CSP outputs."""

import re
from typing import Iterable, List, Tuple


PLACEHOLDER_PREFIX = "«PLACEHOLDER:"
PLACEHOLDER_SUFFIX = "»"


def format_placeholder(raw: str) -> str:
    cleaned = (raw or "").strip()
    if not cleaned:
        return f"{PLACEHOLDER_PREFIX} VALUE REQUIRED {PLACEHOLDER_SUFFIX}"
    if cleaned.startswith(PLACEHOLDER_PREFIX) and cleaned.endswith(PLACEHOLDER_SUFFIX):
        return cleaned
    cleaned = cleaned.strip("[]")
    return f"{PLACEHOLDER_PREFIX} {cleaned} {PLACEHOLDER_SUFFIX}"


def contains_placeholder(text: str) -> bool:
    return PLACEHOLDER_PREFIX in (text or "")


def split_placeholder_segments(text: str) -> List[Tuple[str, bool]]:
    if not text:
        return [("", False)]
    segments: List[Tuple[str, bool]] = []
    remaining = text
    while remaining:
        start = remaining.find(PLACEHOLDER_PREFIX)
        if start == -1:
            segments.append((remaining, False))
            break
        if start > 0:
            segments.append((remaining[:start], False))
        remaining = remaining[start:]
        end = remaining.find(PLACEHOLDER_SUFFIX)
        if end == -1:
            segments.append((remaining, True))
            break
        placeholder_text = remaining[: end + len(PLACEHOLDER_SUFFIX)]
        segments.append((placeholder_text, True))
        remaining = remaining[end + len(PLACEHOLDER_SUFFIX):]
    return segments or [("", False)]


def count_placeholders(texts: Iterable[str]) -> int:
    return sum(1 for text in texts if contains_placeholder(text))


def find_unresolved_tokens(text: str) -> List[Tuple[str, int]]:
    """Find all unresolved placeholder patterns in text.
    
    Returns list of (token_pattern, position) tuples.
    Patterns checked:
    - «PLACEHOLDER: ... »
    - {{ ... }}
    - { ... } (single braces)
    - {project_*
    - {ssho*
    - {pm*
    - {quality*
    - _ _ _ _ (4+ underscores)
    """
    unresolved: List[Tuple[str, int]] = []
    if not text:
        return unresolved
    
    # Check for «PLACEHOLDER: pattern
    start = 0
    while True:
        pos = text.find(PLACEHOLDER_PREFIX, start)
        if pos == -1:
            break
        unresolved.append(("«PLACEHOLDER:", pos))
        start = pos + 1
    
    # Check for {{ pattern (double braces)
    start = 0
    while True:
        pos = text.find("{{", start)
        if pos == -1:
            break
        unresolved.append(("{{", pos))
        start = pos + 1
    
    # Check for {project_, {ssho, {pm, {quality patterns
    tracked_patterns = [
        (r'\{project_[^\}]*\}', "{project_*"),
        (r'\{ssho[^\}]*\}', "{ssho*"),
        (r'\{pm[^\}]*\}', "{pm*"),
        (r'\{quality[^\}]*\}', "{quality*"),
    ]
    for pattern, label in tracked_patterns:
        for match in re.finditer(pattern, text):
            unresolved.append((label, match.start()))
    
    # Check for } and { patterns (stray braces), but exclude PLACEHOLDER syntax and tracked patterns
    i = 0
    while i < len(text):
        if text[i] == '{':
            # Check if it's part of PLACEHOLDER syntax
            if i + len(PLACEHOLDER_PREFIX) <= len(text) and text[i:i+len(PLACEHOLDER_PREFIX)] == PLACEHOLDER_PREFIX:
                # Skip past the placeholder
                end = text.find(PLACEHOLDER_SUFFIX, i)
                if end != -1:
                    i = end + len(PLACEHOLDER_SUFFIX)
                    continue
            # Check if it's a pattern we already track (e.g., {project_, {ssho)
            is_tracked = False
            for pattern, _ in tracked_patterns:
                match = re.match(pattern, text[i:])
                if match:
                    is_tracked = True
                    break
            if not is_tracked and i + 1 < len(text) and text[i+1] != '{':
                # Only flag if not double brace (already handled) and not tracked pattern
                unresolved.append(('{', i))
        elif text[i] == '}':
            # Check if it's part of PLACEHOLDER syntax
            if i >= len(PLACEHOLDER_SUFFIX) and text[i-len(PLACEHOLDER_SUFFIX)+1:i+1] == PLACEHOLDER_SUFFIX:
                pass  # Skip, it's part of placeholder
            else:
                unresolved.append(('}', i))
        i += 1
    
    # Check for 4+ consecutive underscores
    for match in re.finditer(r'_{4,}', text):
        unresolved.append(("____", match.start()))
    
    return unresolved


__all__ = [
    "format_placeholder",
    "contains_placeholder",
    "split_placeholder_segments",
    "count_placeholders",
    "find_unresolved_tokens",
    "PLACEHOLDER_PREFIX",
    "PLACEHOLDER_SUFFIX",
]

