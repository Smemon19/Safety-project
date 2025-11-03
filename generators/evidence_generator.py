from __future__ import annotations

"""Evidence-based CSP section generation with two-step process."""

import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from context.section_retriever import SectionScopedRetriever, EvidenceChunk
from context.contamination_guard import filter_contaminated_content, BANNED_PHRASES
from generators.csp import _normalize_em_ref


@dataclass
class ExtractedEvidence:
    """Evidence extracted from documents."""
    chunk_id: str
    bullet_text: str
    source: str
    page_ref: Optional[str] = None
    section_ref: Optional[str] = None


@dataclass
class SectionGenerationResult:
    """Result of section generation."""
    section_text: str
    evidence_bullets: List[ExtractedEvidence]
    citations: List[Dict[str, Any]]
    has_insufficient_evidence: bool = False
    contamination_removed: int = 0


class EvidenceBasedSectionGenerator:
    """Generates CSP sections using two-step evidence-based approach."""
    
    def __init__(
        self,
        collection_name: str,
        chroma_client=None,
        embedding_model: str = "all-MiniLM-L6-v2",
        min_evidence_count: int = 3,
        max_evidence_count: int = 6,
    ):
        self.retriever = SectionScopedRetriever(
            collection_name=collection_name,
            chroma_client=chroma_client,
            embedding_model=embedding_model,
        )
        self.min_evidence_count = min_evidence_count
        self.max_evidence_count = max_evidence_count
        self.collection_name = collection_name
    
    async def extract_evidence(
        self,
        section_identifier: str,
        project_context: Dict[str, Any],
    ) -> List[ExtractedEvidence]:
        """Step A: Extract 3-6 verbatim evidence bullets with chunk_id.
        
        Returns:
            List of extracted evidence bullets with provenance
        """
        # Retrieve evidence chunks
        evidence_chunks = await self.retriever.retrieve_for_section(
            section_identifier,
            project_context,
            top_k=self.max_evidence_count,
        )
        
        # Extract verbatim bullets from chunks
        extracted: List[ExtractedEvidence] = []
        seen_bullets = set()

        for chunk in evidence_chunks[:self.max_evidence_count]:
            # Extract key sentences/statements from chunk
            sentences = self._extract_key_sentences(chunk.text)

            for sentence in sentences:
                # Normalize and check for duplicates
                normalized = self._normalize_text(sentence)
                if normalized in seen_bullets or len(normalized) < 20:
                    continue
                seen_bullets.add(normalized)
                
                # Build page reference
                page_ref = None
                if chunk.page_label:
                    page_ref = f"page {chunk.page_label}"
                elif chunk.page_number:
                    page_ref = f"page {chunk.page_number}"
                
                words = sentence.strip().split()
                trimmed_sentence = " ".join(words[:40])
                extracted.append(
                    ExtractedEvidence(
                        chunk_id=chunk.chunk_id,
                        bullet_text=trimmed_sentence,
                        source=chunk.source,
                        page_ref=page_ref,
                        section_ref=chunk.section_path,
                    )
                )

                if len(extracted) >= self.max_evidence_count:
                    break

            if len(extracted) >= self.max_evidence_count:
                break
        
        return extracted
    
    def _extract_key_sentences(self, text: str) -> List[str]:
        """Extract key sentences from chunk text."""
        import re
        # Split into sentences
        sentences = re.split(r'[.!?]+\s+', text)
        key_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence or len(sentence) < 20:
                continue
            
            # Filter out sentences that are too generic
            generic_patterns = [
                r'^(this|that|these|those)\s+',
                r'^(see|refer to|see also)',
                r'^note\s+that',
            ]
            is_generic = any(re.match(p, sentence.lower()) for p in generic_patterns)
            
            if not is_generic and len(sentence) >= 20 and len(sentence.split()) <= 50:
                if any(re.search(pattern, sentence, re.IGNORECASE) for pattern in BANNED_PHRASES):
                    continue
                key_sentences.append(sentence)
        
        # Return top sentences (prioritize longer, more specific ones)
        key_sentences.sort(key=lambda s: len(s), reverse=True)
        return key_sentences[:3]  # Top 3 per chunk
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for duplicate detection."""
        import re
        normalized = re.sub(r'\s+', ' ', text.lower().strip())
        normalized = re.sub(r'[^\w\s]', '', normalized)
        return normalized
    
    async def compose_section(
        self,
        section_identifier: str,
        evidence_bullets: List[ExtractedEvidence],
        project_context: Dict[str, Any],
        em385_refs: List[str],
    ) -> str:
        """Step B: Compose section from evidence bullets + EM 385 only.
        
        Args:
            section_identifier: Section ID
            evidence_bullets: Extracted evidence
            project_context: Project metadata
            em385_refs: EM 385 references for this section
            
        Returns:
            Composed section text, or "INSUFFICIENT EVIDENCE" if insufficient
        """
        if len(evidence_bullets) < self.min_evidence_count:
            return "INSUFFICIENT EVIDENCE"

        if not os.getenv("OPENAI_API_KEY"):
            return "INSUFFICIENT EVIDENCE"

        project_supported = [
            ev for ev in evidence_bullets if not self._is_em385_source(ev.source)
        ]
        if len(project_supported) < 2:
            return "INSUFFICIENT EVIDENCE"

        paragraphs = self._compose_structured_paragraphs(evidence_bullets, em385_refs)
        return "\n\n".join(paragraphs)

    def _simple_compose(self, evidence_bullets: List[ExtractedEvidence], em385_refs: List[str]) -> str:
        """Legacy helper retained for compatibility (unused)."""
        return "INSUFFICIENT EVIDENCE"

    def _compose_structured_paragraphs(
        self,
        evidence_bullets: List[ExtractedEvidence],
        em385_refs: List[str],
    ) -> List[str]:
        def fmt(ev: ExtractedEvidence) -> str:
            citation = f" [{ev.chunk_id}]"
            if ev.page_ref:
                citation += f" ({ev.page_ref})"
            return f"{ev.bullet_text}{citation}"

        paragraphs: List[str] = []
        paragraphs.append(f"Purpose: {fmt(evidence_bullets[0])}")

        procedure_evidence = evidence_bullets[1:3]
        if procedure_evidence:
            paragraphs.append(
                "Procedures / Policy / Requirements: "
                + " ".join(fmt(ev) for ev in procedure_evidence)
            )

        responsibility_evidence = evidence_bullets[3:5]
        if responsibility_evidence:
            paragraphs.append(
                "Responsibilities: " + " ".join(fmt(ev) for ev in responsibility_evidence)
            )

        remaining = evidence_bullets[5:]
        if remaining:
            paragraphs.append(
                "Forms, Logs, or Records: " + " ".join(fmt(ev) for ev in remaining)
            )

        normalized_refs = []
        seen_refs = set()
        for ref in em385_refs:
            norm = _normalize_em_ref(ref)
            if norm and norm not in seen_refs:
                seen_refs.add(norm)
                normalized_refs.append(norm)
            if len(normalized_refs) >= 5:
                break

        em_text = ", ".join(normalized_refs) if normalized_refs else "relevant EM 385 clauses"
        chunk_refs = ", ".join(ev.chunk_id for ev in evidence_bullets)
        paragraphs.append(f"References: EM 385-1-1 {em_text}; Evidence: {chunk_refs}.")
        return paragraphs

    def _is_em385_source(self, source: Optional[str]) -> bool:
        if not source:
            return False
        return "em 385" in source.lower()

    async def generate_section(
        self,
        section_identifier: str,
        project_context: Dict[str, Any],
        em385_refs: List[str],
    ) -> SectionGenerationResult:
        """Full two-step generation process."""
        # Step A: Extract evidence
        evidence_bullets = await self.extract_evidence(section_identifier, project_context)
        
        if len(evidence_bullets) < self.min_evidence_count:
            return SectionGenerationResult(
                section_text="INSUFFICIENT EVIDENCE",
                evidence_bullets=evidence_bullets,
                citations=[],
                has_insufficient_evidence=True,
            )

        project_supported = [
            ev for ev in evidence_bullets if not self._is_em385_source(ev.source)
        ]
        if len(project_supported) < 2:
            return SectionGenerationResult(
                section_text="INSUFFICIENT EVIDENCE",
                evidence_bullets=evidence_bullets,
                citations=[],
                has_insufficient_evidence=True,
            )

        # Step B: Compose from evidence
        composed_text = await self.compose_section(
            section_identifier,
            evidence_bullets,
            project_context,
            em385_refs,
        )

        if composed_text.strip() == "INSUFFICIENT EVIDENCE":
            return SectionGenerationResult(
                section_text="INSUFFICIENT EVIDENCE",
                evidence_bullets=evidence_bullets,
                citations=[],
                has_insufficient_evidence=True,
            )

        # Apply contamination guard
        evidence_texts = [ev.bullet_text for ev in evidence_bullets]
        cleaned_text, contamination_count = filter_contaminated_content(
            composed_text, evidence_texts=evidence_texts
        )

        if not cleaned_text:
            return SectionGenerationResult(
                section_text="INSUFFICIENT EVIDENCE",
                evidence_bullets=evidence_bullets,
                citations=[],
                has_insufficient_evidence=True,
                contamination_removed=contamination_count,
            )

        # Build citations
        citations = []
        for evidence in evidence_bullets:
            citations.append({
                "section_path": evidence.section_ref or "",
                "page_label": evidence.page_ref or "",
                "source_url": evidence.source,
                "chunk_id": evidence.chunk_id,
            })

        return SectionGenerationResult(
            section_text=cleaned_text,
            evidence_bullets=evidence_bullets,
            citations=citations,
            has_insufficient_evidence=False,
            contamination_removed=contamination_count,
        )


__all__ = ["EvidenceBasedSectionGenerator", "ExtractedEvidence", "SectionGenerationResult"]

