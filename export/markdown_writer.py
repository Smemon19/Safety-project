from __future__ import annotations

from typing import Dict, List
from pathlib import Path
from models.aha import AhaDoc
from models.csp import CspDoc


SECTION_CROSSWALK: Dict[str, str] = {
    "Diving Program": "Diving Operations",
    "Welding & Cutting Program": "Welding & Cutting",
    "Electrical Safety & LOTO": "Electrical Systems",
    "Excavation & Trenching Safety": "Excavation & Trenching",
    "Cranes & Rigging": "Cranes & Rigging",
    "Confined Space Program": "Confined Space Entry",
    "Demolition Plan": "Demolition",
}


def write_aha_book_md(docs: List[AhaDoc], output_path: str) -> str:
    parts: List[str] = []
    parts.append("# Activity Hazard Analysis (AHA) Book")
    parts.append("")
    parts.append("## Index")
    for aha in docs:
        parts.append(f"- [{aha.name}](#{aha.name.lower().replace(' ', '-')})")
    parts.append("")

    for aha in docs:
        parts.append("---")
        parts.append(f"## {aha.name}")
        parts.append("")
        parts.append(f"**Activity:** {aha.activity}")
        parts.append("")
        if aha.hazards:
            parts.append("**Hazards**:")
            for h in aha.hazards:
                parts.append(f"- {h}")
            parts.append("")

        parts.append("**Work Sequence, Hazards, Controls, PPE, Permits/Training**")
        parts.append("")
        parts.append("| Step | Hazards | Controls | PPE | Permits/Training |")
        parts.append("| --- | --- | --- | --- | --- |")
        for it in aha.items:
            hz = "<br>".join(it.hazards)
            ct = "<br>".join(it.controls)
            pp = "<br>".join(it.ppe)
            pm = "<br>".join(it.permits_training)
            parts.append(f"| {it.step} | {hz} | {ct} | {pp} | {pm} |")
        parts.append("")

        if aha.citations:
            parts.append("**Citations**:")
            for c in aha.citations:
                pg = c.page_label or c.page_number or ""
                parts.append(f"1. § {c.section_path} | page {pg} — \"{c.quote_anchor}\"")
            parts.append("")

        # PPE / Permits sections
        ppe_items: list[str] = []
        permits_items: list[str] = []
        for it in aha.items:
            ppe_items.extend(it.ppe)
            permits_items.extend(it.permits_training)
        if ppe_items:
            parts.append("**PPE**:")
            for p in ppe_items:
                parts.append(f"- {p}")
            parts.append("")
        if permits_items:
            parts.append("**Permits & Training**:")
            for p in permits_items:
                parts.append(f"- {p}")
            parts.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


def write_csp_md(csp: CspDoc, output_path: str) -> str:
    parts: List[str] = []
    parts.append("# Construction Safety Plan (CSP)")
    parts.append("")
    pname = (csp.project_name or "").strip()
    pn = (csp.project_number or "").strip()
    if pname and pn:
        parts.append(f"**Project:** {pname} — {pn}")
    elif pname:
        parts.append(f"**Project:** {pname}")
    elif pn:
        parts.append(f"**Project:** {pn}")
    if csp.location:
        parts.append(f"**Location:** {csp.location}")
    own = f"**Owner:** {csp.owner}" if csp.owner else ""
    gc = f"**GC:** {csp.general_contractor}" if csp.general_contractor else ""
    if own or gc:
        parts.append(" ".join([s for s in [own, gc] if s]))
    parts.append("")
    # Crosswalk
    try:
        rev = {v: k for k, v in SECTION_CROSSWALK.items()}
        parts.append("## AHA Crosswalk")
        parts.append("")
        parts.append("| CSP Section | AHA | Link |")
        parts.append("| --- | --- | --- |")
        for sec in csp.sections:
            aha_activity = rev.get(sec.name)
            if not aha_activity:
                continue
            slug = aha_activity.lower().replace(' ', '_')
            link = f"outputs/ahas/{slug}.md"
            parts.append(f"| {sec.name} | {aha_activity} | {link} |")
        parts.append("")
    except Exception:
        pass
    for sec in csp.sections:
        parts.append("---")
        parts.append(f"## {sec.name}")
        for p in sec.paragraphs:
            parts.append(p)
        if sec.citations:
            parts.append("")
            parts.append("**Citations**:")
            for c in sec.citations:
                pg = c.page_label or c.page_number or ""
                parts.append(f"1. § {c.section_path} | page {pg} — \"{c.quote_anchor}\"")
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


def write_aha_single_md(aha: AhaDoc, output_path: str) -> str:
    parts: List[str] = []
    parts.append(f"# {aha.name}")
    parts.append("")
    parts.append(f"**Activity:** {aha.activity}")
    parts.append("")
    if aha.hazards:
        parts.append("**Hazards**:")
        for h in aha.hazards:
            parts.append(f"- {h}")
        parts.append("")

    parts.append("**Work Sequence, Hazards, Controls, PPE, Permits/Training**")
    parts.append("")
    parts.append("| Step | Hazards | Controls | PPE | Permits/Training |")
    parts.append("| --- | --- | --- | --- | --- |")
    for it in aha.items:
        hz = "<br>".join(it.hazards)
        ct = "<br>".join(it.controls)
        pp = "<br>".join(it.ppe)
        pm = "<br>".join(it.permits_training)
        parts.append(f"| {it.step} | {hz} | {ct} | {pp} | {pm} |")
    parts.append("")

    if aha.citations:
        parts.append("**Citations**:")
        for c in aha.citations:
            pg = c.page_label or c.page_number or ""
            parts.append(f"1. § {c.section_path} | page {pg} — \"{c.quote_anchor}\"")
        parts.append("")

    # Codes Covered
    if getattr(aha, 'codes_covered', []):
        parts.append("**Codes Covered**:")
        for code in aha.codes_covered:
            parts.append(f"- {code}")
        parts.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)

