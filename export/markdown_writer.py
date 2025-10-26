from __future__ import annotations

from typing import List
from pathlib import Path
from models.aha import AhaDoc
from models.csp import CspDoc
from generators.csp import SECTION_MAP


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

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


def write_csp_md(csp: CspDoc, output_path: str) -> str:
    parts: List[str] = []
    parts.append("# Construction Safety Plan (CSP)")
    parts.append("")
    parts.append(f"**Project:** {csp.project_name} — {csp.project_number}")
    parts.append(f"**Location:** {csp.location}")
    parts.append(f"**Owner:** {csp.owner} | **GC:** {csp.general_contractor}")
    parts.append("")
    # Crosswalk
    try:
        rev = {v: k for k, v in SECTION_MAP.items()}
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

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)

