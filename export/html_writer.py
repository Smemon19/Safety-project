from __future__ import annotations

from typing import Dict, List
from pathlib import Path
from html import escape
from models.aha import AhaDoc
from models.csp import CspDoc


SECTION_CROSSWALK: Dict[str, str] = {
    # Legacy mapping retained for cross-reference table when available.
    "Diving Program": "Diving Operations",
    "Welding & Cutting Program": "Welding & Cutting",
    "Electrical Safety & LOTO": "Electrical Systems",
    "Excavation & Trenching Safety": "Excavation & Trenching",
    "Cranes & Rigging": "Cranes & Rigging",
    "Confined Space Program": "Confined Space Entry",
    "Demolition Plan": "Demolition",
}


def write_aha_book_html(docs: List[AhaDoc], output_path: str) -> str:
    parts: List[str] = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html><head><meta charset=\"utf-8\"><title>AHA Book</title>")
    parts.append("<style>\nbody{font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:1000px;margin:24px auto;padding:0 16px;line-height:1.5} \nheader{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:12px} \n.kv{display:grid;grid-template-columns:200px 1fr;gap:4px 12px;margin:8px 0 20px} \n.kv div{padding:2px 0} \nhr{border:none;border-top:1px solid #e5e7eb;margin:16px 0} \nsection{margin:18px 0 28px} \n.table-wrap{overflow-x:auto} \ntable{width:100%;border-collapse:collapse;margin:12px 0;background:#fff} \nth,td{border:1px solid #e5e7eb;padding:8px 10px;vertical-align:top} \nth{background:#f8fafc;text-align:left} \n.badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px;margin-right:6px} \nsmall.muted{color:#6b7280} \nblockquote{margin:8px 0;padding:8px 12px;border-left:3px solid #93c5fd;background:#f0f9ff} \n</style>")
    parts.append("</head><body>")
    parts.append("<h1>Activity Hazard Analysis (AHA) Book</h1>")

    # Index
    parts.append("<h2>Index</h2><ul>")
    for aha in docs:
        parts.append(f"<li><a href=\"#{escape(aha.name)}\">{escape(aha.name)}</a></li>")
    parts.append("</ul>")

    for aha in docs:
        parts.append(f"<hr><h2 id=\"{escape(aha.name)}\">{escape(aha.name)}</h2>")
        parts.append(f"<p><strong>Activity:</strong> {escape(aha.activity)}</p>")
        if aha.hazards:
            parts.append("<h3>Hazards</h3><ul>")
            for h in aha.hazards:
                parts.append(f"<li>{escape(h)}</li>")
            parts.append("</ul>")

        parts.append("<h3>Work Sequence, Hazards, Controls, PPE, Permits/Training</h3>")
        parts.append("<table><thead><tr><th>Step</th><th>Hazards</th><th>Controls</th><th>PPE</th><th>Permits/Training</th></tr></thead><tbody>")
        for it in aha.items:
            hz = "<br>".join(escape(x) for x in it.hazards)
            ct = "<br>".join(escape(x) for x in it.controls)
            pp = "<br>".join(escape(x) for x in it.ppe)
            pm = "<br>".join(escape(x) for x in it.permits_training)
            parts.append(f"<tr><td>{escape(it.step)}</td><td>{hz}</td><td>{ct}</td><td>{pp}</td><td>{pm}</td></tr>")
        parts.append("</tbody></table>")

        if aha.citations:
            parts.append("<h3>Citations</h3><ol>")
            for c in aha.citations:
                pg = escape(str(c.page_label or c.page_number or ""))
                sp = escape(c.section_path or "")
                qa = escape(c.quote_anchor or "")
                parts.append(f"<li><span class=\"muted\">§ {sp} | page {pg}</span><br>\"{qa}\"</li>")
            parts.append("</ol>")

    parts.append("</body></html>")
    html = "\n".join(parts)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return str(out)


def write_csp_html(csp: CspDoc, output_path: str) -> str:
    parts: List[str] = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html><head><meta charset=\"utf-8\"><title>CSP</title>")
    parts.append("<style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:900px;margin:24px auto;padding:0 12px} h1,h2{margin:16px 0 8px} .muted{color:#666;font-size:0.95em}</style>")
    parts.append("</head><body>")
    parts.append("<h1>Construction Safety Plan (CSP)</h1>")
    parts.append("<header>")
    parts.append("<h1>Construction Safety Plan (CSP)</h1>")
    parts.append("</header>")
    parts.append("<div class=\"kv\">")
    parts.append(f"<div><strong>Project</strong></div><div>{escape(csp.project_name)} — {escape(csp.project_number)}</div>")
    parts.append(f"<div><strong>Location</strong></div><div>{escape(csp.location)}</div>")
    parts.append(f"<div><strong>Owner</strong></div><div>{escape(csp.owner)}</div>")
    parts.append(f"<div><strong>General Contractor</strong></div><div>{escape(csp.general_contractor)}</div>")
    parts.append("</div>")

    # Crosswalk (Section -> AHA link)
    try:
        rev = {v: k for k, v in SECTION_CROSSWALK.items()}
        parts.append("<h2>AHA Crosswalk</h2>")
        parts.append("<table><thead><tr><th>CSP Section</th><th>AHA</th><th>Link</th></tr></thead><tbody>")
        for sec in csp.sections:
            aha_activity = rev.get(sec.name)
            if not aha_activity:
                continue
            slug = aha_activity.lower().replace(' ', '_')
            link = f"outputs/ahas/{escape(slug)}.html"
            parts.append(f"<tr><td>{escape(sec.name)}</td><td>{escape(aha_activity)}</td><td><a href=\"{link}\">{link}</a></td></tr>")
        parts.append("</tbody></table>")
    except Exception:
        pass

    for sec in csp.sections:
        parts.append(f"<hr><h2>{escape(sec.name)}</h2>")
        for p in sec.paragraphs:
            parts.append(f"<p>{escape(p)}</p>")
        if sec.citations:
            parts.append("<h3>Citations</h3><ol>")
            for c in sec.citations:
                pg = escape(str(c.page_label or c.page_number or ""))
                sp = escape(c.section_path or "")
                qa = escape(c.quote_anchor or "")
                parts.append(f"<li><small class=\"muted\">§ {sp} | page {pg}</small><blockquote>\"{qa}\"</blockquote></li>")
            parts.append("</ol>")

    parts.append("</body></html>")
    html = "\n".join(parts)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return str(out)


def write_aha_single_html(aha: AhaDoc, output_path: str) -> str:
    parts: List[str] = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html><head><meta charset=\"utf-8\"><title>AHA</title>")
    parts.append("<style>\nbody{font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:1000px;margin:24px auto;padding:0 16px;line-height:1.5} \nheader{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:12px} \n.kv{display:grid;grid-template-columns:200px 1fr;gap:4px 12px;margin:8px 0 20px} \n.kv div{padding:2px 0} \nhr{border:none;border-top:1px solid #e5e7eb;margin:16px 0} \nsection{margin:18px 0 28px} \n.table-wrap{overflow-x:auto} \ntable{width:100%;border-collapse:collapse;margin:12px 0;background:#fff} \nth,td{border:1px solid #e5e7eb;padding:8px 10px;vertical-align:top} \nth{background:#f8fafc;text-align:left} \n.badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px;margin-right:6px} \nsmall.muted{color:#6b7280} \nblockquote{margin:8px 0;padding:8px 12px;border-left:3px solid #93c5fd;background:#f0f9ff} \n</style>")
    parts.append("</head><body>")
    parts.append(f"<h1>{escape(aha.name)}</h1>")
    parts.append("<div class=\"kv\">")
    parts.append(f"<div><strong>Activity</strong></div><div>{escape(aha.activity)}</div>")
    parts.append("</div>")
    if aha.hazards:
        parts.append("<h3>Hazards</h3><ul>")
        for h in aha.hazards:
            parts.append(f"<li>{escape(h)}</li>")
        parts.append("</ul>")

    parts.append("<h3>Work Sequence, Hazards, Controls, PPE, Permits/Training</h3>")
    parts.append("<table><thead><tr><th>Step</th><th>Hazards</th><th>Controls</th><th>PPE</th><th>Permits/Training</th></tr></thead><tbody>")
    for it in aha.items:
        hz = "<br>".join(escape(x) for x in it.hazards)
        ct = "<br>".join(escape(x) for x in it.controls)
        pp = "<br>".join(escape(x) for x in it.ppe)
        pm = "<br>".join(escape(x) for x in it.permits_training)
        parts.append(f"<tr><td>{escape(it.step)}</td><td>{hz}</td><td>{ct}</td><td>{pp}</td><td>{pm}</td></tr>")
    parts.append("</tbody></table>")

    if aha.citations:
        parts.append("<h3>Citations</h3><ol>")
        for c in aha.citations:
            pg = escape(str(c.page_label or c.page_number or ""))
            sp = escape(c.section_path or "")
            qa = escape(c.quote_anchor or "")
            parts.append(f"<li><span class=\"muted\">§ {sp} | page {pg}</span><br>\"{qa}\"</li>")
        parts.append("</ol>")

    parts.append("</body></html>")
    html = "\n".join(parts)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return str(out)


