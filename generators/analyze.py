from __future__ import annotations

from typing import Dict, List

from .activity_detect import detect_activities
from .hazard_map import hazards_for_activity


def analyze_scope(scope_text: str) -> Dict[str, List[str]]:
    """Analyze a scope/spec text and return activities and hazards.

    Returns a dict with keys:
    - activities: List[str]
    - hazards: De-duplicated list of hazards inferred from activities
    - by_activity: Flattened mapping-like list ("Activity: hazard") for quick display
    """
    activities = detect_activities(scope_text or "")
    hazards: List[str] = []
    by_activity_pairs: List[str] = []

    for a in activities:
        hs = hazards_for_activity(a)
        for h in hs:
            by_activity_pairs.append(f"{a}: {h}")
        hazards.extend(hs)

    # de-duplicate hazards preserving order
    seen = set()
    hazards_unique: List[str] = []
    for h in hazards:
        if h not in seen:
            seen.add(h)
            hazards_unique.append(h)

    return {
        "activities": activities,
        "hazards": hazards_unique,
        "by_activity": by_activity_pairs,
    }


