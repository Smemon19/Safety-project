from __future__ import annotations

"""Section-scoped retrieval system for evidence-based CSP generation."""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from context.context_builder import SECTION_DEFINITIONS, SectionDefinition
from context.document_sanitizer import tag_chunk_for_exclusion


@dataclass
class EvidenceChunk:
    """A retrieved evidence chunk with provenance."""
    chunk_id: str
    text: str
    source: str  # Document name or "EM 385-1-1"
    page_number: Optional[int] = None
    page_label: Optional[str] = None
    section_path: Optional[str] = None  # EM 385 section reference
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SectionQuery:
    """Query template for a CSP section."""
    section_identifier: str
    base_query: str
    keywords: List[str]
    em385_refs: List[str]
    filters: Dict[str, Any]


class SectionScopedRetriever:
    """Retriever that performs section-specific queries with filtering."""
    
    def __init__(
        self,
        collection_name: str,
        chroma_client=None,
        embedding_model: str = "all-MiniLM-L6-v2",
    ):
        self.collection_name = collection_name
        self.chroma_client = chroma_client
        self.embedding_model = embedding_model
        self._section_queries: Dict[str, SectionQuery] = {}
        self._build_query_templates()
    
    def _build_query_templates(self):
        """Build per-section query templates from SECTION_DEFINITIONS."""
        for section_def in SECTION_DEFINITIONS:
            # Build query from section description + keywords
            query_parts = [section_def.description]
            query_parts.extend(section_def.keywords[:3])  # Top 3 keywords
            base_query = " ".join(query_parts)
            
            self._section_queries[section_def.identifier] = SectionQuery(
                section_identifier=section_def.identifier,
                base_query=base_query,
                keywords=section_def.keywords,
                em385_refs=section_def.em385_refs,
                filters={},
            )
    
    def build_section_query(
        self,
        section_identifier: str,
        project_context: Dict[str, Any],
    ) -> str:
        """Build enhanced query for a section with project-specific context."""
        base = self._section_queries.get(section_identifier)
        if not base:
            return ""
        
        # Enhance with project-specific terms
        query_parts = [base.base_query]
        
        # Add DFOW if available
        dfow = project_context.get("dfow", [])
        if dfow:
            query_parts.append(" ".join(dfow[:3]))
        
        # Add hazards if available
        hazards = project_context.get("hazards", [])
        if hazards:
            query_parts.append(" ".join(hazards[:3]))
        
        # Add project name/location for context
        project_name = project_context.get("project_name", "")
        if project_name:
            query_parts.append(project_name)
        
        return " ".join(query_parts)
    
    async def retrieve_for_section(
        self,
        section_identifier: str,
        project_context: Dict[str, Any],
        top_k: int = 6,
        use_mmr: bool = True,
        mmr_diversity: float = 0.7,
    ) -> List[EvidenceChunk]:
        """Retrieve evidence chunks for a specific section.
        
        Args:
            section_identifier: Section ID (e.g., "section_01")
            project_context: Project metadata, DFOW, hazards
            top_k: Number of chunks to retrieve
            use_mmr: Use Maximal Marginal Relevance for diversity
            mmr_diversity: MMR diversity parameter (0.0-1.0)
            
        Returns:
            List of evidence chunks with provenance
        """
        try:
            from utils import get_or_create_collection, query_collection, keyword_search_collection
            from utils import rerank_results
        except ImportError:
            # Fallback if utils not available
            return []
        
        if not self.chroma_client:
            return []
        
        # Get collection
        collection = get_or_create_collection(
            self.chroma_client,
            self.collection_name,
            embedding_model=self.embedding_model,
        )
        
        # Build query
        query = self.build_section_query(section_identifier, project_context)
        section_query = self._section_queries.get(section_identifier)
        
        if not query or not section_query:
            return []
        
        # Domain filtering: prefer project docs + EM 385
        # Get initial pool (larger for MMR)
        initial_pool = top_k * 4 if use_mmr else top_k
        
        # Vector search
        vector_results = query_collection(collection, query, n_results=initial_pool)
        
        # Keyword search for EM 385 refs
        keyword_results = None
        if section_query.em385_refs:
            em385_terms = []
            for ref in section_query.em385_refs:
                # Extract section numbers (e.g., "§01.A.13" -> "01", "A", "13")
                em385_terms.extend(ref.replace("§", "").split("."))
            if em385_terms:
                keyword_results = keyword_search_collection(
                    collection,
                    em385_terms,
                    max_results=top_k,
                )
        
        # Merge results
        vec_ids = vector_results.get("ids", [[]])[0] if vector_results else []
        vec_docs = vector_results.get("documents", [[]])[0] if vector_results else []
        vec_metas = vector_results.get("metadatas", [[]])[0] if vector_results else []
        
        if keyword_results:
            kw_ids = keyword_results.get("ids", [[]])[0]
            kw_docs = keyword_results.get("documents", [[]])[0]
            kw_metas = keyword_results.get("metadatas", [[]])[0]
            
            # Merge, preferring vector order
            seen = set(vec_ids)
            for i, kw_id in enumerate(kw_ids):
                if kw_id not in seen:
                    vec_ids.append(kw_id)
                    vec_docs.append(kw_docs[i])
                    vec_metas.append(kw_metas[i])
                    seen.add(kw_id)
        
        # Filter out excluded chunks (TOC/boilerplate)
        filtered_ids = []
        filtered_docs = []
        filtered_metas = []

        for chunk_id, doc, meta in zip(vec_ids, vec_docs, vec_metas):
            meta = meta or {}
            # Check if chunk should be excluded
            if tag_chunk_for_exclusion(doc):
                continue

            source = meta.get("source", "") or meta.get("file", "") or ""
            source_type = (meta.get("source_type") or "").lower()
            is_project_doc = source_type == "project_document" or "project" in source_type
            is_em385 = source_type == "em385" or "em 385" in source.lower()
            if not is_project_doc and not is_em385:
                if "em385" in source.lower() or "em 385" in source.lower():
                    is_em385 = True
                elif source.lower().endswith(".pdf") or source.lower().endswith(".docx"):
                    is_project_doc = True

            if not (is_project_doc or is_em385):
                continue

            # Ensure section titles exist for domain enforcement
            if not meta.get("section_title"):
                continue

            filtered_ids.append(chunk_id)
            filtered_docs.append(doc)
            filtered_metas.append(meta)
        
        # Apply MMR if requested
        if use_mmr and len(filtered_docs) > top_k:
            # Rerank with diversity
            reranked_ids, reranked_docs, reranked_metas = rerank_results(
                query,
                filtered_ids[:top_k * 2],  # Start with 2x for diversity
                filtered_docs[:top_k * 2],
                filtered_metas[:top_k * 2],
                top_k,
            )
            filtered_ids = reranked_ids
            filtered_docs = reranked_docs
            filtered_metas = reranked_metas
        else:
            # Just take top_k
            filtered_ids = filtered_ids[:top_k]
            filtered_docs = filtered_docs[:top_k]
            filtered_metas = filtered_metas[:top_k]
        
        # Ensure chunks align to dominant section domain
        filtered_ids, filtered_docs, filtered_metas = self._enforce_section_domain(
            filtered_ids, filtered_docs, filtered_metas
        )

        # Build EvidenceChunk objects
        evidence_chunks = []
        for chunk_id, doc, meta in zip(filtered_ids, filtered_docs, filtered_metas):
            meta_dict = meta or {}
            source = meta_dict.get("source") or meta_dict.get("file") or "Unknown"
            
            # Extract page info
            page_num = meta_dict.get("page_number")
            if isinstance(page_num, str):
                try:
                    page_num = int(page_num)
                except ValueError:
                    page_num = None

            page_label = meta_dict.get("page_label") or meta_dict.get("page_range")

            evidence_chunks.append(
                EvidenceChunk(
                    chunk_id=chunk_id,
                    text=doc,
                    source=source,
                    page_number=page_num,
                    page_label=page_label,
                    section_path=meta_dict.get("section_path") or meta_dict.get("section_title"),
                    metadata=meta_dict,
                )
            )
        
        return evidence_chunks


    def _enforce_section_domain(
        self,
        ids: List[str],
        docs: List[str],
        metas: List[Dict[str, Any]],
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        """Keep only chunks that share the dominant section_title."""

        if not metas:
            return ids, docs, metas

        section_counts: Dict[str, int] = {}
        for meta in metas:
            title = (meta or {}).get("section_title") or ""
            if title:
                section_counts[title] = section_counts.get(title, 0) + 1

        if not section_counts:
            return ids, docs, metas

        dominant = max(section_counts.items(), key=lambda item: item[1])[0]
        filtered = [
            (cid, doc, meta)
            for cid, doc, meta in zip(ids, docs, metas)
            if (meta or {}).get("section_title") == dominant
        ]
        if not filtered:
            return ids, docs, metas

        new_ids, new_docs, new_metas = zip(*filtered)
        return list(new_ids), list(new_docs), list(new_metas)


__all__ = ["SectionScopedRetriever", "EvidenceChunk", "SectionQuery"]

