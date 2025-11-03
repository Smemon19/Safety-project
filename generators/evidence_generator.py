"""Evidence collection layer for context-driven CSP authoring."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from context.context_builder import SectionDefinition
from context.section_retriever import EvidenceChunk, SectionScopedRetriever
from models.csp import EvidenceSnippet, SectionContextPacket


@dataclass(slots=True)
class ExtractedEvidence:
    """Normalized evidence instance used for context packet construction."""

    tag: str
    text: str
    chunk_id: str
    source: str
    page_ref: Optional[str]
    section_ref: Optional[str]
    is_project: bool
    topic_tags: List[str]


@dataclass(slots=True)
class ContextPacketBuildResult:
    """Container returned by the evidence generator for each section."""

    packet: SectionContextPacket
    insufficient_reasons: List[str]
    raw_chunks: List[EvidenceChunk]


class EvidenceBasedSectionGenerator:
    """Performs dual-stage retrieval and evidence selection for each section."""

    def __init__(
        self,
        collection_name: str,
        chroma_client=None,
        embedding_model: str = "all-MiniLM-L6-v2",
        stage_one_limit: int = 150,
        stage_two_limit: int = 48,
    ) -> None:
        self.collection_name = collection_name
        self.retriever = SectionScopedRetriever(
            collection_name=collection_name,
            chroma_client=chroma_client,
            embedding_model=embedding_model,
        )
        self.stage_one_limit = stage_one_limit
        self.stage_two_limit = stage_two_limit

    async def build_context_packet(
        self,
        definition: SectionDefinition,
        project_context: Dict[str, Any],
    ) -> ContextPacketBuildResult:
        """Collect project and EM 385 evidence, enforcing strict quotas."""

        # Stage 1: BM25/keyword prefilter via retriever keyword search
        stage_one_chunks = await self._stage_one_prefilter(definition, project_context)

        # Stage 2: embedding rerank + diversity selection
        candidate_chunks = await self._stage_two_rerank(
            definition,
            project_context,
            stage_one_chunks,
        )

        project_snippets, em_snippets, insufficient = self._extract_snippets(
            definition,
            candidate_chunks,
        )

        selection_plan = self._plan_selection(definition, project_snippets, em_snippets)

        constraints = {
            "composition": "Use only the evidence above. Do not introduce new facts.",
            "paragraph_target": "Compose 2–4 paragraphs grounded strictly in the selected bullets.",
            "citation_rule": "End with ≤5 EM 385 citations drawn exclusively from the listed EM 385 bullets.",
        }

        dfow = project_context.get("dfow", []) or []
        hazards = project_context.get("hazards", []) or []
        dfow_hazard_pairs = self._pair_dfow_hazards(dfow, hazards)

        packet = SectionContextPacket(
            section_identifier=definition.identifier,
            intent=definition.intent,
            must_answer=list(definition.must_answer),
            project_evidence=[self._to_snippet(ev) for ev in project_snippets],
            em385_evidence=[self._to_snippet(ev) for ev in em_snippets],
            dfow_detected=dfow,
            hazards_detected=hazards,
            dfow_hazard_pairs=dfow_hazard_pairs,
            constraints=constraints,
            selection_plan=selection_plan,
            insufficient_reasons=insufficient,
        )

        return ContextPacketBuildResult(
            packet=packet,
            insufficient_reasons=insufficient,
            raw_chunks=candidate_chunks,
        )

    async def _stage_one_prefilter(
        self,
        definition: SectionDefinition,
        project_context: Dict[str, Any],
    ) -> List[EvidenceChunk]:
        """Stage one: gather a broad pool using keyword/BM25 style retrieval."""

        initial_chunks: List[EvidenceChunk] = []
        if not self.retriever.chroma_client:
            return initial_chunks

        query = self.retriever.build_section_query(definition.identifier, project_context)
        if not query:
            return initial_chunks

        # We reuse the keyword search path by requesting more than needed and
        # trimming later. The retriever already guards against boilerplate.
        chunks = await self.retriever.retrieve_for_section(
            definition.identifier,
            project_context,
            top_k=min(self.stage_one_limit, 150),
            use_mmr=False,
        )

        if not chunks:
            return []

        # Deduplicate by chunk id while preserving order.
        seen = set()
        for chunk in chunks:
            if chunk.chunk_id in seen:
                continue
            seen.add(chunk.chunk_id)
            initial_chunks.append(chunk)
            if len(initial_chunks) >= self.stage_one_limit:
                break

        return initial_chunks

    async def _stage_two_rerank(
        self,
        definition: SectionDefinition,
        project_context: Dict[str, Any],
        stage_one_chunks: Sequence[EvidenceChunk],
    ) -> List[EvidenceChunk]:
        """Stage two: apply embedding search + MMR reranking for diversity."""

        if not self.retriever.chroma_client:
            return list(stage_one_chunks)[: self.stage_two_limit]

        # For stage two we leverage the retriever's built-in MMR pipeline.
        reranked = await self.retriever.retrieve_for_section(
            definition.identifier,
            project_context,
            top_k=self.stage_two_limit,
            use_mmr=True,
            mmr_diversity=0.6,
        )

        if reranked:
            return reranked

        return list(stage_one_chunks)[: self.stage_two_limit]

    def _extract_snippets(
        self,
        definition: SectionDefinition,
        chunks: Sequence[EvidenceChunk],
    ) -> tuple[List[ExtractedEvidence], List[ExtractedEvidence], List[str]]:
        """Split evidence into project vs EM 385 pools respecting quotas."""

        project_limit = max(definition.project_evidence_quota, definition.max_project_evidence)
        em_limit = max(definition.em_evidence_quota, definition.max_em_evidence)

        project_snippets: List[ExtractedEvidence] = []
        em_snippets: List[ExtractedEvidence] = []
        insufficient: List[str] = []
        seen_phrases: set[str] = set()

        for chunk in chunks:
            meta = chunk.metadata or {}
            if self._should_skip_chunk(meta):
                continue

            is_em = self._is_em385_chunk(chunk)
            if is_em and len(em_snippets) >= em_limit:
                continue
            if not is_em and len(project_snippets) >= project_limit:
                continue

            sentences = self._extract_key_sentences(chunk.text)
            for sentence in sentences:
                normalized = self._normalize(sentence)
                if normalized in seen_phrases:
                    continue
                seen_phrases.add(normalized)

                tag_prefix = "EM" if is_em else "P"
                tag = f"{tag_prefix}:{chunk.chunk_id}"
                page_ref = self._page_reference(chunk)
                snippet = ExtractedEvidence(
                    tag=tag,
                    text=sentence,
                    chunk_id=chunk.chunk_id,
                    source=chunk.source,
                    page_ref=page_ref,
                    section_ref=chunk.section_path,
                    is_project=not is_em,
                    topic_tags=self._topic_tags_from_meta(meta),
                )

                if is_em:
                    em_snippets.append(snippet)
                else:
                    project_snippets.append(snippet)

                break  # Only take first qualifying sentence per chunk

            if len(project_snippets) >= project_limit and len(em_snippets) >= em_limit:
                break

        if len(project_snippets) < definition.project_evidence_quota:
            insufficient.append(
                f"Expected ≥{definition.project_evidence_quota} project snippets, found {len(project_snippets)}."
            )
        if len(em_snippets) < definition.em_evidence_quota:
            insufficient.append(
                f"Expected ≥{definition.em_evidence_quota} EM 385 snippets, found {len(em_snippets)}."
            )

        if not os.getenv("OPENAI_API_KEY"):
            insufficient.append("Model offline: skip abstractive composition and block export.")

        return project_snippets, em_snippets, insufficient

    def _plan_selection(
        self,
        definition: SectionDefinition,
        project_snippets: Sequence[ExtractedEvidence],
        em_snippets: Sequence[ExtractedEvidence],
    ) -> Dict[str, List[str]]:
        """Decide which evidence bullets must be used during composition."""

        project_required = [
            ev.tag for ev in project_snippets[: definition.project_evidence_quota]
        ]
        em_required = [ev.tag for ev in em_snippets[: definition.em_evidence_quota]]

        return {
            "project": project_required,
            "em385": em_required,
            "optional": [ev.tag for ev in project_snippets[definition.project_evidence_quota :]]
            + [ev.tag for ev in em_snippets[definition.em_evidence_quota :]],
        }

    def _to_snippet(self, evidence: ExtractedEvidence) -> EvidenceSnippet:
        return EvidenceSnippet(
            tag=evidence.tag,
            text=evidence.text,
            source=evidence.source,
            page_ref=evidence.page_ref,
            section_ref=evidence.section_ref,
            is_project=evidence.is_project,
            topic_tags=evidence.topic_tags,
        )

    def _extract_key_sentences(self, text: str) -> List[str]:
        import re

        statements = re.split(r"(?<=[.!?])\s+", text or "")
        snippets: List[str] = []
        for sentence in statements:
            cleaned = sentence.strip()
            if not cleaned:
                continue
            word_count = len(cleaned.split())
            if word_count < 6:
                continue
            if word_count > 40:
                cleaned = " ".join(cleaned.split()[:40])
            if self._contains_banned_phrase(cleaned):
                continue
            snippets.append(cleaned)
        return snippets[:3]

    def _normalize(self, text: str) -> str:
        import re

        lowered = text.lower().strip()
        return re.sub(r"\s+", " ", lowered)

    def _page_reference(self, chunk: EvidenceChunk) -> Optional[str]:
        if chunk.page_label:
            return str(chunk.page_label)
        if chunk.page_number is not None:
            return str(chunk.page_number)
        meta = chunk.metadata or {}
        if meta.get("page_range"):
            return str(meta["page_range"])
        return None

    def _contains_banned_phrase(self, text: str) -> bool:
        banned = (
            "table of contents",
            "for receipt by the contracting officer",
            "quality control approval. submit the following",
            "update the table of contents",
            "add absorbent material to absorb residue oil remaining after draining",
        )
        lower = text.lower()
        return any(phrase in lower for phrase in banned)

    def _should_skip_chunk(self, metadata: Dict[str, Any]) -> bool:
        flags = {key: metadata.get(key) for key in ("is_toc", "is_header", "is_footer", "is_boilerplate")}
        return any(bool(value) for value in flags.values())

    def _is_em385_chunk(self, chunk: EvidenceChunk) -> bool:
        source_lower = (chunk.source or "").lower()
        if "em 385" in source_lower or "em385" in source_lower:
            return True
        meta = chunk.metadata or {}
        source_type = (meta.get("source_type") or "").lower()
        if "em385" in source_type:
            return True
        heading_path = (meta.get("heading_path") or "").lower()
        return heading_path.startswith("em 385")

    def _topic_tags_from_meta(self, metadata: Dict[str, Any]) -> List[str]:
        tags: List[str] = []
        for key in ("topic_tags", "topics", "labels"):
            value = metadata.get(key)
            if isinstance(value, str):
                tags.extend([v.strip() for v in value.split(",") if v.strip()])
            elif isinstance(value, Iterable):
                tags.extend([str(v).strip() for v in value if str(v).strip()])
        return tags[:5]

    def _pair_dfow_hazards(self, dfow: Sequence[str], hazards: Sequence[str]) -> List[str]:
        pairs: List[str] = []
        upper_bound = min(len(dfow), len(hazards))
        for idx in range(upper_bound):
            pairs.append(f"{dfow[idx]} — {hazards[idx]}")
        if not pairs and dfow:
            pairs = [f"{item} — (hazard pending)" for item in dfow[:3]]
        if not pairs and hazards:
            pairs = [f"(dfow pending) — {hazards[idx]}" for idx in range(min(3, len(hazards)))]
        return pairs[:5]


__all__ = [
    "EvidenceBasedSectionGenerator",
    "ExtractedEvidence",
    "ContextPacketBuildResult",
]

