from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class CspCitation(BaseModel):
    section_path: str = Field(default="")
    page_label: str = Field(default="")
    page_number: Optional[int] = Field(default=None)
    quote_anchor: str = Field(default="")
    source_url: str = Field(default="")


class CspSection(BaseModel):
    name: str
    paragraphs: List[str] = []
    citations: List[CspCitation] = []


class CspDoc(BaseModel):
    project_name: str
    project_number: str = ""
    location: str = ""
    owner: str = ""
    general_contractor: str = ""
    sections: List[CspSection]


