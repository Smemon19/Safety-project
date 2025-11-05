"""Pydantic models for the Section 11 Generator pipeline."""

from __future__ import annotations

import enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SpecSourceHit(BaseModel):
    """Location metadata for a code or phrase detected in the spec."""

    page: Optional[int] = Field(default=None, description="1-based page number if available")
    heading: str = Field(default="", description="Nearest heading or section title")
    excerpt: str = Field(default="", description="Short snippet showing the hit")


class ParsedCode(BaseModel):
    code: str
    title: str = ""
    requires_aha: Optional[bool] = None
    suggested_category: str = ""
    notes: str = ""
    sources: List[SpecSourceHit] = Field(default_factory=list)
    decision_source: str = ""
    confidence: Optional[float] = None
    rationale: str = ""


class ParsedSpec(BaseModel):
    scope_summary: List[str] = Field(default_factory=list)
    codes: List[ParsedCode] = Field(default_factory=list)
    hazard_phrases: List[str] = Field(default_factory=list)
    raw_text_path: Optional[Path] = None


class CategoryStatus(enum.Enum):
    pending = "Pending"
    required = "Required"
    not_required = "Not Required"
    insufficient = "Pending – Insufficient Evidence"


class CategoryAssignment(BaseModel):
    code: str
    suggested_category: str = ""
    override: Optional[str] = None
    why: str = ""

    @property
    def effective_category(self) -> str:
        return self.override or self.suggested_category or "Unmapped"


class AhaEvidence(BaseModel):
    citations: List[Dict[str, str]] = Field(default_factory=list)
    narrative: List[str] = Field(default_factory=list)
    hazards: List[str] = Field(default_factory=list)
    status: CategoryStatus = CategoryStatus.pending
    pending_reason: str = ""


class SafetyPlanEvidence(BaseModel):
    controls: List[str] = Field(default_factory=list)
    ppe: List[str] = Field(default_factory=list)
    permits: List[str] = Field(default_factory=list)
    citations: List[Dict[str, str]] = Field(default_factory=list)
    project_evidence: List[str] = Field(default_factory=list)
    em_evidence: List[str] = Field(default_factory=list)
    status: CategoryStatus = CategoryStatus.pending
    pending_reason: str = ""

    @property
    def evidence_counts(self) -> Dict[str, int]:
        return {
            "project": len(self.project_evidence),
            "em": len(self.em_evidence),
        }


class CategoryBundle(BaseModel):
    category: str
    codes: List[str] = Field(default_factory=list)
    aha: AhaEvidence = Field(default_factory=AhaEvidence)
    plan: SafetyPlanEvidence = Field(default_factory=SafetyPlanEvidence)


class ComplianceMatrixRow(BaseModel):
    category: str
    codes: List[str]
    aha_status: CategoryStatus
    plan_status: CategoryStatus
    project_evidence_count: int = 0
    em_evidence_count: int = 0
    aha_link: str = ""
    plan_link: str = ""


class Section11Artifacts(BaseModel):
    base_dir: Path
    manifest_path: Path
    markdown_path: Path
    docx_path: Path
    json_report_path: Path
    aha_markdown_paths: Dict[str, Path] = Field(default_factory=dict)
    plan_markdown_paths: Dict[str, Path] = Field(default_factory=dict)


class RunDiagnostics(BaseModel):
    run_id: str
    summary: Dict[str, int] = Field(default_factory=dict)
    overrides: List[Dict[str, str]] = Field(default_factory=list)
    codes: Dict[str, Dict[str, object]] = Field(default_factory=dict)
    categories: Dict[str, Dict[str, object]] = Field(default_factory=dict)


class Section11Run(BaseModel):
    run_id: str
    source_file: Path
    parsed: ParsedSpec
    assignments: List[CategoryAssignment]
    bundles: List[CategoryBundle]
    matrix: List[ComplianceMatrixRow]
    artifacts: Section11Artifacts
    diagnostics: RunDiagnostics

