"""Section-level orchestrators that assemble context packets without templates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from context.context_builder import SectionDefinition
from generators.evidence_generator import (
    ContextPacketBuildResult,
    EvidenceBasedSectionGenerator,
)
from models.csp import CspCitation, CspSection


@dataclass(slots=True)
class SectionRunResult:
    """Outcome for a single section orchestration run."""

    section: CspSection
    evidence_entries: List[Dict[str, Any]]
    insufficient_reasons: List[str]


class SectionOrchestrator:
    """Coordinates evidence gathering and packaging for a single section."""

    def __init__(self, definition: SectionDefinition, generator: EvidenceBasedSectionGenerator) -> None:
        self.definition = definition
        self.generator = generator

    async def run(self, context_pack: Dict[str, Any]) -> SectionRunResult:
        project_context = self._build_project_context(context_pack)
        result = await self.generator.build_context_packet(self.definition, project_context)

        section = self._build_section(context_pack, result)
        evidence_entries = self._build_manifest_entries(result)

        return SectionRunResult(
            section=section,
            evidence_entries=evidence_entries,
            insufficient_reasons=result.insufficient_reasons,
        )

    def _build_project_context(self, context_pack: Dict[str, Any]) -> Dict[str, Any]:
        metadata = context_pack.get("metadata", {}) or {}
        return {
            "project_name": metadata.get("project_name", ""),
            "location": metadata.get("location", ""),
            "owner": metadata.get("owner", ""),
            "dfow": context_pack.get("dfow", []) or [],
            "hazards": context_pack.get("hazards", []) or [],
            "topic_tags": context_pack.get("topic_tags", []) or [],
        }

    def _build_section(
        self,
        context_pack: Dict[str, Any],
        result: ContextPacketBuildResult,
    ) -> CspSection:
        packet = result.packet

        paragraphs: List[str] = []
        if packet.selection_plan:
            project_tags = ", ".join(packet.selection_plan.get("project", [])) or "none"
            em_tags = ", ".join(packet.selection_plan.get("em385", [])) or "none"
            optional_tags = ", ".join(packet.selection_plan.get("optional", [])) or "none"
            paragraphs.append(
                "Step A | project:" + project_tags + " | em385:" + em_tags + " | optional:" + optional_tags
            )

        for snippet in packet.project_evidence:
            paragraphs.append(f"[{snippet.tag}] {snippet.text}")

        for snippet in packet.em385_evidence:
            paragraphs.append(f"[{snippet.tag}] {snippet.text}")

        paragraphs.append("Checklist:" + " | ".join(packet.must_answer))

        citations: List[CspCitation] = []
        for snippet in packet.project_evidence + packet.em385_evidence:
            citations.append(
                CspCitation(
                    section_path=snippet.section_ref or "",
                    page_label=snippet.page_ref or "",
                    quote_anchor=snippet.tag,
                    source_url=str(snippet.source),
                )
            )

        return CspSection(
            name=context_pack.get("title", self.definition.title),
            paragraphs=paragraphs,
            citations=citations,
            context_packet=packet,
        )

    def _build_manifest_entries(self, result: ContextPacketBuildResult) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for snippet in result.packet.project_evidence + result.packet.em385_evidence:
            entries.append(
                {
                    "tag": snippet.tag,
                    "text": snippet.text,
                    "source": snippet.source,
                    "page_ref": snippet.page_ref,
                    "section_ref": snippet.section_ref,
                    "is_project": snippet.is_project,
                }
            )
        return entries


__all__ = ["SectionOrchestrator", "SectionRunResult"]

