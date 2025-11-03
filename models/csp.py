from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CspCitation(BaseModel):
    section_path: str = Field(default="")
    page_label: str = Field(default="")
    page_number: Optional[int] = Field(default=None)
    quote_anchor: str = Field(default="")
    source_url: str = Field(default="")


class EvidenceSnippet(BaseModel):
    """Atomic evidence unit surfaced for a CSP section."""

    tag: str
    text: str
    source: str
    page_ref: Optional[str] = Field(default=None)
    section_ref: Optional[str] = Field(default=None)
    is_project: bool = Field(default=True)
    topic_tags: List[str] = Field(default_factory=list)


class SectionContextPacket(BaseModel):
    """Evidence-grounded packet that upstream LLMs consume."""

    section_identifier: str
    intent: str
    must_answer: List[str] = Field(default_factory=list)
    project_evidence: List[EvidenceSnippet] = Field(default_factory=list)
    em385_evidence: List[EvidenceSnippet] = Field(default_factory=list)
    dfow_detected: List[str] = Field(default_factory=list)
    hazards_detected: List[str] = Field(default_factory=list)
    dfow_hazard_pairs: List[str] = Field(default_factory=list)
    constraints: Dict[str, Any] = Field(default_factory=dict)
    selection_plan: Dict[str, List[str]] = Field(default_factory=dict)
    insufficient_reasons: List[str] = Field(default_factory=list)


class CspSection(BaseModel):
    name: str
    paragraphs: List[str] = Field(default_factory=list)
    citations: List[CspCitation] = Field(default_factory=list)
    context_packet: Optional[SectionContextPacket] = Field(default=None)


class CspDoc(BaseModel):
    project_name: str
    project_number: str = ""
    location: str = ""
    owner: str = ""
    general_contractor: str = ""
    sections: List[CspSection]


