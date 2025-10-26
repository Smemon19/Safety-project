from __future__ import annotations

from typing import List
from pathlib import Path
from docx import Document
from docx.shared import Pt
from models.aha import AhaDoc
from models.csp import CspDoc


def write_aha_book(docs: List[AhaDoc], output_path: str) -> str:
    """Write a simple AHA Book DOCX with headings, tables, and citations."""
    doc = Document()

    # Title
    doc.add_heading('Activity Hazard Analysis (AHA) Book', level=0)

    # Index
    doc.add_heading('Index', level=1)
    for aha in docs:
        p = doc.add_paragraph()
        run = p.add_run(aha.name)
        run.bold = True

    for aha in docs:
        doc.add_page_break()
        doc.add_heading(aha.name, level=1)
        doc.add_paragraph(f"Activity: {aha.activity}")
        if aha.hazards:
            doc.add_paragraph("Hazards:")
            for h in aha.hazards:
                doc.add_paragraph(h, style='List Bullet')

        # Steps table
        doc.add_heading('Work Sequence, Hazards, Controls, PPE, Permits/Training', level=2)
        table = doc.add_table(rows=1, cols=5)
        hdr = table.rows[0].cells
        hdr[0].text = 'Step'
        hdr[1].text = 'Hazards'
        hdr[2].text = 'Controls'
        hdr[3].text = 'PPE'
        hdr[4].text = 'Permits/Training'
        for it in aha.items:
            row = table.add_row().cells
            row[0].text = it.step
            row[1].text = "\n".join(it.hazards)
            row[2].text = "\n".join(it.controls)
            row[3].text = "\n".join(it.ppe)
            row[4].text = "\n".join(it.permits_training)

        # Citations
        if aha.citations:
            doc.add_heading('Citations', level=2)
            for c in aha.citations:
                line = f"§ {c.section_path} | page {c.page_label or c.page_number or ''} — \"{c.quote_anchor}\""
                doc.add_paragraph(line, style='List Number')

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out))
    return str(out)


def write_aha_single(aha: AhaDoc, output_path: str) -> str:
    doc = Document()
    doc.add_heading(aha.name, level=0)
    doc.add_paragraph(f"Activity: {aha.activity}")
    if aha.hazards:
        doc.add_paragraph("Hazards:")
        for h in aha.hazards:
            doc.add_paragraph(h, style='List Bullet')

    doc.add_heading('Work Sequence, Hazards, Controls, PPE, Permits/Training', level=2)
    table = doc.add_table(rows=1, cols=5)
    hdr = table.rows[0].cells
    hdr[0].text = 'Step'
    hdr[1].text = 'Hazards'
    hdr[2].text = 'Controls'
    hdr[3].text = 'PPE'
    hdr[4].text = 'Permits/Training'
    for it in aha.items:
        row = table.add_row().cells
        row[0].text = it.step
        row[1].text = "\n".join(it.hazards)
        row[2].text = "\n".join(it.controls)
        row[3].text = "\n".join(it.ppe)
        row[4].text = "\n".join(it.permits_training)

    if aha.citations:
        doc.add_heading('Citations', level=2)
        for c in aha.citations:
            line = f"§ {c.section_path} | page {c.page_label or c.page_number or ''} — \"{c.quote_anchor}\""
            doc.add_paragraph(line, style='List Number')

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out))
    return str(out)


def write_csp_docx(csp: CspDoc, output_path: str) -> str:
    doc = Document()
    doc.add_heading('Construction Safety Plan (CSP)', level=0)
    doc.add_paragraph(f"Project: {csp.project_name} — {csp.project_number}")
    doc.add_paragraph(f"Location: {csp.location}")
    doc.add_paragraph(f"Owner: {csp.owner} | GC: {csp.general_contractor}")

    for sec in csp.sections:
        doc.add_page_break()
        doc.add_heading(sec.name, level=1)
        for p in sec.paragraphs:
            doc.add_paragraph(p)
        if sec.citations:
            doc.add_heading('Citations', level=2)
            for c in sec.citations:
                pg = c.page_label or c.page_number or ''
                doc.add_paragraph(f"§ {c.section_path} | page {pg} — \"{c.quote_anchor}\"", style='List Number')

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out))
    return str(out)


