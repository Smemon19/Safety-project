from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class AhaCitation(BaseModel):
    section_path: str = Field(default="", description="Human-readable section hierarchy")
    page_label: str = Field(default="", description="PDF page label if available")
    page_number: Optional[int] = Field(default=None, description="PDF 1-based page number")
    quote_anchor: str = Field(default="", description="Short confirming quote")
    source_url: str = Field(default="", description="Source URL or file path")


class AhaItem(BaseModel):
    step: str
    hazards: List[str] = []
    controls: List[str] = []
    ppe: List[str] = []
    permits_training: List[str] = []


class AhaDoc(BaseModel):
    name: str
    activity: str
    hazards: List[str]
    items: List[AhaItem]
    citations: List[AhaCitation]
    codes_covered: List[str] = []


