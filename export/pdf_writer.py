from __future__ import annotations

"""PDF export utilities for the CSP deliverable."""

from pathlib import Path
import textwrap

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from context.placeholder_manager import split_placeholder_segments
from models.csp import CspDoc


def _draw_text_with_placeholders(pdf: canvas.Canvas, x: float, y: float, text: str, base_font: str = "Helvetica", base_size: int = 10) -> None:
    current_x = x
    for segment, is_placeholder in split_placeholder_segments(text or ""):
        if not segment:
            continue
        font_name = base_font
        if is_placeholder:
            font_name = "Helvetica-Bold"
            pdf.setFillColor(colors.red)
        else:
            pdf.setFillColor(colors.black)
        pdf.setFont(font_name, base_size)
        pdf.drawString(current_x, y, segment)
        current_x += pdf.stringWidth(segment, font_name, base_size)
    pdf.setFillColor(colors.black)
    pdf.setFont(base_font, base_size)


def _draw_page_header(pdf: canvas.Canvas, csp: CspDoc, width: float, height: float) -> None:
    header = csp.project_name or "Construction Safety Plan"
    if csp.location:
        header = f"{header} | {csp.location}"
    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.grey)
    pdf.drawString(72, height - 40, header)
    pdf.setFillColor(colors.black)
    pdf.setFont("Helvetica", 10)


def _write_cover_page(pdf: canvas.Canvas, csp: CspDoc) -> None:
    width, height = letter
    y = height - 72
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(72, y, "Comprehensive Site-Specific Construction Safety and Health Plan")
    pdf.setFont("Helvetica", 12)
    y -= 32
    _draw_text_with_placeholders(pdf, 72, y, f"Project: {csp.project_name or '[Insert Project Name]'}", base_font="Helvetica", base_size=12)
    y -= 18
    if csp.project_number:
        _draw_text_with_placeholders(pdf, 72, y, f"Project Number: {csp.project_number}", base_font="Helvetica", base_size=12)
        y -= 18
    if csp.location:
        _draw_text_with_placeholders(pdf, 72, y, f"Location: {csp.location}", base_font="Helvetica", base_size=12)
        y -= 18
    if csp.owner:
        _draw_text_with_placeholders(pdf, 72, y, f"Owner: {csp.owner}", base_font="Helvetica", base_size=12)
        y -= 18
    if csp.general_contractor:
        _draw_text_with_placeholders(pdf, 72, y, f"Prime Contractor: {csp.general_contractor}", base_font="Helvetica", base_size=12)
        y -= 18
    _draw_text_with_placeholders(pdf, 72, y, "Prepared for submission to the U.S. Army Corps of Engineers", base_font="Helvetica", base_size=12)
    pdf.showPage()


def write_csp_pdf(csp: CspDoc, output_path: str) -> str:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pdf = canvas.Canvas(str(out), pagesize=letter)
    width, height = letter

    _write_cover_page(pdf, csp)
    _draw_page_header(pdf, csp, width, height)

    for index, section in enumerate(csp.sections):
        y = height - 72
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawString(72, y, section.name)
        y -= 24
        pdf.setFont("Helvetica", 10)
        for paragraph in section.paragraphs:
            text = (paragraph or '').strip()
            if not text:
                continue
            lines = textwrap.wrap(text, 95)
            for line in lines:
                if y < 72:
                    pdf.showPage()
                    _draw_page_header(pdf, csp, width, height)
                    y = height - 72
                    pdf.setFont("Helvetica", 10)
                _draw_text_with_placeholders(pdf, 72, y, line)
                y -= 14
            if y < 86:
                pdf.showPage()
                _draw_page_header(pdf, csp, width, height)
                y = height - 72
                pdf.setFont("Helvetica", 10)
        if section.citations:
            if y < 100:
                pdf.showPage()
                _draw_page_header(pdf, csp, width, height)
                y = height - 72
                pdf.setFont("Helvetica", 10)
            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawString(72, y, "Citations:")
            y -= 16
            pdf.setFont("Helvetica", 10)
            for citation in section.citations:
                text = citation.section_path
                if citation.page_label:
                    text += f" | page {citation.page_label}"
                elif citation.page_number:
                    text += f" | page {citation.page_number}"
                if citation.quote_anchor:
                    text += f" — \"{citation.quote_anchor}\""
                lines = textwrap.wrap(text, 95)
                for line in lines:
                    if y < 72:
                        pdf.showPage()
                        _draw_page_header(pdf, csp, width, height)
                        y = height - 72
                        pdf.setFont("Helvetica", 10)
                    _draw_text_with_placeholders(pdf, 90, y, f"- {line}")
                    y -= 14
        if index < len(csp.sections) - 1:
            pdf.showPage()
            _draw_page_header(pdf, csp, width, height)

    pdf.save()
    return str(out)

