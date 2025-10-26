from __future__ import annotations

from typing import Dict, List


# Minimal initial mapping; we will extend as we wire to EM 385 sections
ACTIVITY_TO_HAZARDS: Dict[str, List[str]] = {
    "Diving Operations": ["Drowning", "Decompression sickness", "Entanglement"],
    "Welding & Cutting": ["Burns", "Fumes", "Fire", "Eye injury"],
    "Electrical Systems": ["Electrical shock", "Arc flash", "LOTO failure"],
    "Excavation & Trenching": ["Cave-in", "Struck-by", "Hazardous atmosphere"],
    "Cranes & Rigging": ["Crane tip-over", "Load drop", "Struck-by"],
    "Confined Space Entry": ["Asphyxiation", "Toxic exposure", "Engulfment"],
    "Demolition": ["Uncontrolled collapse", "Falling debris", "Dust"],
}


def hazards_for_activity(activity: str) -> List[str]:
    return ACTIVITY_TO_HAZARDS.get(activity, [])


