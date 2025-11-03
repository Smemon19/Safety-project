from __future__ import annotations

"""Evidence-based CSP section generation with two-step process."""

import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from context.section_retriever import SectionScopedRetriever, EvidenceChunk
from context.contamination_guard import filter_contaminated_content, BANNED_PHRASES


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
        
        if len(evidence_chunks) < self.min_evidence_count:
            # Return what we have, but mark as insufficient
            pass
        
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
                
                extracted.append(
                    ExtractedEvidence(
                        chunk_id=chunk.chunk_id,
                        bullet_text=sentence.strip(),
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
            
            if not is_generic and len(sentence) >= 20 and len(sentence) <= 300:
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
        
        # Get section definition
        from context.context_builder import SECTION_DEFINITIONS
        section_def = next(
            (s for s in SECTION_DEFINITIONS if s.identifier == section_identifier),
            None
        )
        if not section_def:
            return "INSUFFICIENT EVIDENCE"
        
        # Build evidence text with chunk IDs
        evidence_text = "\n".join(
            f"• [{e.chunk_id}] {e.bullet_text}" +
            (f" (from {e.source}" if e.source else "") +
            (f", {e.page_ref}" if e.page_ref else "") +
            (f", {e.section_ref}" if e.section_ref else "") +
            (")" if e.source else "")
            for e in evidence_bullets
        )
        
        em385_text = ", ".join(em385_refs) if em385_refs else "relevant sections"
        
        # Try LLM-based composition with controlled parameters
        try:
            from pydantic_ai import Agent
            from pydantic_ai.models.openai import OpenAIModel
            import openai
            
            model_name = os.getenv("OPENAI_API_MODEL", "gpt-4o-mini")
            
            # Build prompt
            prompt = f"""Compose a CSP section for "{section_def.title}" based ONLY on the following evidence and EM 385-1-1 requirements.

Evidence (use verbatim where possible):
{evidence_text}

EM 385-1-1 References: {em385_text}

Requirements:
1. Write in standard CSP format: Purpose, Procedures/Policy/Requirements, Responsibilities, Forms/Logs/Records
2. Use ONLY information from the evidence above - cite chunk IDs [chunk_id] where used
3. Include specific details from the evidence
4. Reference EM 385 sections where applicable
5. Cite sources using format: (source, page X) or (EM 385-1-1 §XX.YY)
6. NO generic filler, templates, or LLM guidance phrases
7. If there's insufficient specific information, write "INSUFFICIENT EVIDENCE" instead of generic text

Output the section text:"""
            
            # Use OpenAI directly with controlled parameters
            client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            response = await client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "You are a CSP generator that writes sections based ONLY on provided evidence. Never add generic filler or templates."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.25,  # Low temperature for factual content
                top_p=0.9,
                stop=[
                    "\n\nThis document",
                    "\n\nNote:",
                    "\n\nDisclaimer:",
                    "\n\nFor more information",
                    "\n\nLLM guidance",
                    "\n\nbest practice",
                ],
                max_tokens=2000,
            )
            
            composed = response.choices[0].message.content.strip()
            return composed
            
        except Exception as e:
            # Fallback to simple composition
            return self._simple_compose(evidence_bullets, em385_refs)
    
    def _simple_compose(self, evidence_bullets: List[ExtractedEvidence], em385_refs: List[str]) -> str:
        """Simple composition fallback."""
        paragraphs = [
            "Purpose: (Derived from evidence)",
            "Procedures / Policy / Requirements:",
        ]
        
        for evidence in evidence_bullets[:4]:  # Use top 4
            paragraphs.append(f"- {evidence.bullet_text}")
            if evidence.page_ref:
                paragraphs[-1] += f" ({evidence.source}, {evidence.page_ref})"
        
        paragraphs.append(
            f"References: EM 385-1-1 {', '.join(em385_refs) if em385_refs else 'relevant sections'}."
        )
        
        return "\n\n".join(paragraphs)
    
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
        
        # Step B: Compose from evidence
        composed_text = await self.compose_section(
            section_identifier,
            evidence_bullets,
            project_context,
            em385_refs,
        )
        
        # Apply contamination guard
        cleaned_text, contamination_count = filter_contaminated_content(composed_text)
        
        # Build citations
        citations = []
        for evidence in evidence_bullets:
            citations.append({
                "section_path": evidence.section_ref or "",
                "page_label": evidence.page_ref or "",
                "source_url": evidence.source,
            })
        
        return SectionGenerationResult(
            section_text=cleaned_text,
            evidence_bullets=evidence_bullets,
            citations=citations,
            has_insufficient_evidence=False,
            contamination_removed=contamination_count,
        )


__all__ = ["EvidenceBasedSectionGenerator", "ExtractedEvidence", "SectionGenerationResult"]

