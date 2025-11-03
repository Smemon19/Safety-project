from __future__ import annotations

"""Mapping heuristics that link DFOWs to EM 385 sub-plan requirements."""

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class PlanDefinition:
    name: str
    em385_refs: List[str]
    dfow_keywords: List[str]
    hazard_keywords: List[str]
    always_required: bool = False


DEFAULT_PLAN_DATA: List[Dict[str, object]] = [
    {
        "name": "Fall Protection and Prevention Plan",
        "em385_refs": ["§21-7.a"],
        "dfow_keywords": ["fall", "roof", "steel", "scaffold", "tower", "ladder", "elevated"],
        "hazard_keywords": ["fall", "elevation", "unprotected edge", "leading edge"],
        "always_required": False,
    },
    {
        "name": "Rescue Plan",
        "em385_refs": ["§21-7.b"],
        "dfow_keywords": ["confined space", "tower", "vertical", "climbing"],
        "hazard_keywords": ["rescue", "retrieval", "suspension"],
        "always_required": False,
    },
    {
        "name": "Scaffolding Work Plan",
        "em385_refs": ["§22-7"],
        "dfow_keywords": ["scaffold", "scaffolding", "suspended platform"],
        "hazard_keywords": ["scaffold", "platform collapse"],
        "always_required": False,
    },
    {
        "name": "Confined Space Plan",
        "em385_refs": ["§34-7.b"],
        "dfow_keywords": ["confined space", "tank", "vault", "manhole", "tunnel"],
        "hazard_keywords": ["confined space", "oxygen deficiency", "toxic atmosphere"],
        "always_required": False,
    },
    {
        "name": "Excavation and Trenching Plan",
        "em385_refs": ["§25-7"],
        "dfow_keywords": ["excavation", "trench", "earthwork", "shoring"],
        "hazard_keywords": ["cave-in", "trench", "shoring"],
        "always_required": False,
    },
    {
        "name": "Demolition Plan",
        "em385_refs": ["§17-7"],
        "dfow_keywords": ["demolition", "structure removal"],
        "hazard_keywords": ["demolition", "implosion"],
        "always_required": False,
    },
    {
        "name": "Fire Prevention Plan",
        "em385_refs": ["§9-7"],
        "dfow_keywords": ["hot work", "welding", "cutting"],
        "hazard_keywords": ["fire", "hot work", "combustible"],
        "always_required": True,
    },
    {
        "name": "Electrical Safety / Energy Control Plan",
        "em385_refs": ["§11-7", "§12-7"],
        "dfow_keywords": ["electrical", "energized", "loto", "temporary power"],
        "hazard_keywords": ["electrical", "arc flash", "lockout"],
        "always_required": True,
    },
    {
        "name": "Traffic Control Plan",
        "em385_refs": ["§8-7"],
        "dfow_keywords": ["traffic", "roadway", "vehicle", "hauling"],
        "hazard_keywords": ["traffic", "vehicle impact", "flagging"],
        "always_required": False,
    },
    {
        "name": "Silica Compliance Plan",
        "em385_refs": ["§6-7.j"],
        "dfow_keywords": ["concrete cutting", "masonry cutting", "concrete grinding", "masonry grinding", "concrete drilling", "masonry drilling", "abrasive blasting"],
        "hazard_keywords": ["silica", "respirable crystalline silica", "respirable dust"],
        "always_required": False,
    },
    {
        "name": "Hearing Conservation Plan",
        "em385_refs": ["§5-7.a"],
        "dfow_keywords": ["pile driving", "demolition", "drilling"],
        "hazard_keywords": ["noise", "hearing"],
        "always_required": False,
    },
    {
        "name": "Respiratory Protection Plan",
        "em385_refs": ["§5-7.b"],
        "dfow_keywords": ["painting", "coating", "abrasive blasting", "chemical handling"],
        "hazard_keywords": ["respiratory", "air monitoring", "vapors"],
        "always_required": False,
    },
    {
        "name": "Emergency Response Plan",
        "em385_refs": ["§36-7.c"],
        "dfow_keywords": ["emergency", "hazmat", "medical"],
        "hazard_keywords": ["emergency", "evacuation", "severe weather"],
        "always_required": True,
    },
    {
        "name": "Housekeeping Plan",
        "em385_refs": ["§10-7"],
        "dfow_keywords": ["housekeeping", "cleanup", "waste"],
        "hazard_keywords": ["debris", "slip"],
        "always_required": True,
    },
    {
        "name": "Site Layout Plan",
        "em385_refs": ["§28-7.b"],
        "dfow_keywords": ["site layout", "staging", "laydown", "logistics"],
        "hazard_keywords": ["traffic", "material storage", "logistics"],
        "always_required": True,
    }
]


def _normalize_keywords(entries: List[str]) -> List[str]:
    return [str(entry).lower() for entry in entries if entry]


@lru_cache(maxsize=1)
def _load_plan_definitions() -> List[PlanDefinition]:
    path = Path("mappings/safety_plans.json")
    dataset = DEFAULT_PLAN_DATA
    try:
        dataset_from_file = json.loads(path.read_text(encoding="utf-8"))
        if dataset_from_file:
            dataset = dataset_from_file
    except Exception:
        dataset = DEFAULT_PLAN_DATA

    definitions: List[PlanDefinition] = []
    for entry in dataset:
        definitions.append(
            PlanDefinition(
                name=str(entry.get("name", "Plan")),
                em385_refs=[str(ref) for ref in entry.get("em385_refs", [])],
                dfow_keywords=_normalize_keywords(entry.get("dfow_keywords", [])),
                hazard_keywords=_normalize_keywords(entry.get("hazard_keywords", [])),
                always_required=bool(entry.get("always_required", False)),
            )
        )
    return definitions


def get_plan_definitions() -> List[PlanDefinition]:
    return list(_load_plan_definitions())


def map_dfow_to_plans(dfow: List[object], hazards: List[object] | None = None) -> Dict[str, Dict[str, object]]:
    """Return applicability matrix for Section 11 sub-plans."""

    dfow = dfow or []
    hazards = hazards or []

    def _extract(entry: object) -> tuple[str, List[str]]:
        if isinstance(entry, dict):
            text = str(entry.get("text") or entry.get("value") or "")
            chunk_data = entry.get("chunk_ids") or entry.get("chunk_id") or []
            if isinstance(chunk_data, str):
                chunk_ids = [chunk_data]
            else:
                chunk_ids = list(chunk_data)
            return text, chunk_ids
        return str(entry), []

    dfow_texts: List[str] = []
    dfow_chunks: List[List[str]] = []
    for entry in dfow:
        text, chunk_ids = _extract(entry)
        dfow_texts.append(text)
        dfow_chunks.append(chunk_ids)

    hazards_texts: List[str] = []
    hazards_chunks: List[List[str]] = []
    for entry in hazards:
        text, chunk_ids = _extract(entry)
        hazards_texts.append(text)
        hazards_chunks.append(chunk_ids)

    dfow_lower = [text.lower() for text in dfow_texts]
    hazards_lower = [text.lower() for text in hazards_texts]
    planning_matrix: Dict[str, Dict[str, object]] = {}

    for plan in _load_plan_definitions():
        matched_dfow = [
            dfow_texts[idx]
            for idx, low in enumerate(dfow_lower)
            if any(keyword in low for keyword in plan.dfow_keywords)
        ]
        matched_dfow_chunks = [
            dfow_chunks[idx]
            for idx, low in enumerate(dfow_lower)
            if any(keyword in low for keyword in plan.dfow_keywords)
        ]
        matched_hazards = [
            hazards_texts[idx]
            for idx, low in enumerate(hazards_lower)
            if any(keyword in low for keyword in plan.hazard_keywords)
        ]
        matched_hazard_chunks = [
            hazards_chunks[idx]
            for idx, low in enumerate(hazards_lower)
            if any(keyword in low for keyword in plan.hazard_keywords)
        ]

        applicable = plan.always_required or bool(matched_dfow) or bool(matched_hazards)
        if applicable:
            status = "Pending"
            if plan.always_required and not (matched_dfow or matched_hazards):
                justification = "Baseline requirement per EM 385"
            elif matched_dfow:
                evidence_chunks = sorted({cid for group in matched_dfow_chunks for cid in group if cid})
                evidence_suffix = f" (evidence: {', '.join(evidence_chunks)})" if evidence_chunks else ""
                justification = f"Triggered by DFOW: {', '.join(matched_dfow)}{evidence_suffix}"
            else:
                evidence_chunks = sorted({cid for group in matched_hazard_chunks for cid in group if cid})
                evidence_suffix = f" (evidence: {', '.join(evidence_chunks)})" if evidence_chunks else ""
                justification = f"Triggered by hazards: {', '.join(matched_hazards)}{evidence_suffix}"
        else:
            status = "Not Applicable"
            justification = "Scope does not invoke this plan"

        planning_matrix[plan.name] = {
            "status": status,
            "justification": justification,
            "em385_refs": plan.em385_refs,
            "matched_dfow": matched_dfow,
            "matched_hazards": matched_hazards,
            "action_required": applicable,
        }

    return planning_matrix


__all__ = ["map_dfow_to_plans", "get_plan_definitions", "PlanDefinition"]

