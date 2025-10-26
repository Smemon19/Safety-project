from __future__ import annotations

from typing import List, Dict, Tuple
import re


_ACTIVITY_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("Diving Operations", re.compile(r"\b(div(ing|e)|underwater|subsea)\b", re.IGNORECASE)),
    ("Welding & Cutting", re.compile(r"\b(weld(ing)?|cutting|hot\s+work|oxy(-|\s*)fuel)\b", re.IGNORECASE)),
    ("Electrical Systems", re.compile(r"\b(electrical|energized|lockout|tagout|LOTO|panel|switchgear)\b", re.IGNORECASE)),
    ("Excavation & Trenching", re.compile(r"\b(excavat(ion|e)|trench(ing)?|shoring|shielding)\b", re.IGNORECASE)),
    ("Cranes & Rigging", re.compile(r"\b(crane|rigging|hoist|lift plan|signal(person|er))\b", re.IGNORECASE)),
    ("Confined Space Entry", re.compile(r"\b(confined\s+space|permit-required\s+confined\s+space|PRCS)\b", re.IGNORECASE)),
    ("Demolition", re.compile(r"\b(demolition|demo|structure\s+removal)\b", re.IGNORECASE)),
]


def detect_activities(scope_text: str) -> List[str]:
    """Detect likely activities from a scope/spec text via keyword rules.

    Returns a deduplicated, stable-order list of activity labels.
    """
    found: List[str] = []
    s = scope_text or ""
    for label, pattern in _ACTIVITY_PATTERNS:
        if pattern.search(s):
            found.append(label)
    # stable unique
    seen = set()
    out: List[str] = []
    for a in found:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


