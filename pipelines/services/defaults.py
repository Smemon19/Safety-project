from __future__ import annotations

"""Default (placeholder) service implementations for the CSP pipeline.

These classes provide a minimal, non-destructive implementation that allows the
pipeline to be wired into different entry points while the richer functionality
is developed in subsequent tasks. Each class is designed to be replaced or
extended with production-ready logic once document parsing, metadata
management, context pack creation, and export features are implemented.
"""

import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..csp_pipeline import (
    CSPValidator,
    DecisionProvider,
    DocumentIngestionResult,
    DocumentSourceChoice,
    MetadataSourceChoice,
    MetadataState,
    OutputState,
    PipelineDependencies,
    ProcessingState,
    ValidationState,
)
from ..csp_pipeline import (
    DocumentIngestionService,
    ProcessingEngine,
    ProjectMetadataManager,
    OutputAssembler,
    PostProcessor,
)
from context.context_builder import SECTION_DEFINITIONS, build_context_packs
from context.document_ingestion import DocumentIngestionEngine
from context.dfow_mapping import map_dfow_to_plans
from context.placeholder_manager import format_placeholder, contains_placeholder
from export.docx_writer import write_csp_docx
from export.pdf_writer import write_csp_pdf
from generators.csp import assemble_csp_doc, build_csp_sections


def _ensure_output_dir(base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)


def _generate_appendices(base_dir: Path, processing: ProcessingState) -> List[str]:
    """Generate appendix stub files A1-A6.
    
    Returns list of created appendix file paths.
    """
    appendices_dir = base_dir / "appendices"
    appendices_dir.mkdir(parents=True, exist_ok=True)
    created: List[str] = []
    
    # A1: Project Map (placeholder PDF/MD)
    a1_path = appendices_dir / "A1_Project_Map.md"
    a1_path.write_text("# Appendix 1: Project Map\n\n*Insert site map here*\n", encoding="utf-8")
    created.append(str(a1_path))
    
    # A2: Subcontractor Roster (table scaffold)
    a2_path = appendices_dir / "A2_Subcontractor_Roster.md"
    a2_content = """# Appendix 2: Subcontractor Roster

| Company Name | Contact Person | Phone | Email | Insurance Expiry | Certificates |
| --- | --- | --- | --- | --- | --- |
| *Insert subcontractors* | | | | | |

"""
    a2_path.write_text(a2_content, encoding="utf-8")
    created.append(str(a2_path))
    
    # A3: Personnel Qualifications (matrix scaffold)
    a3_path = appendices_dir / "A3_Personnel_Qualifications.md"
    a3_content = """# Appendix 3: Personnel Qualifications

| Name | Role | Qualification | Issue Date | Expiry Date | Certificate # |
| --- | --- | --- | --- | --- | --- |
| *Insert personnel* | | | | | |

"""
    a3_path.write_text(a3_content, encoding="utf-8")
    created.append(str(a3_path))
    
    # A4: AHA Index (table with DFOW, residual risk, approval dates)
    a4_path = appendices_dir / "A4_AHA_Index.md"
    dfow_list = processing.context_packs.get("section_02", {}).get("dfow", [])
    a4_content = "# Appendix 4: Activity Hazard Analyses Index\n\n"
    a4_content += "| DFOW | AHA Title | Residual Risk | Approval Date | Approved By | Status |\n"
    a4_content += "| --- | --- | --- | --- | --- | --- |\n"
    for dfow_item in dfow_list[:10]:  # Limit to first 10 for scaffold
        a4_content += f"| {dfow_item} | *AHA pending* | | | | Pending |\n"
    a4_path.write_text(a4_content, encoding="utf-8")
    created.append(str(a4_path))
    
    # A5: Site-Specific Plans Register
    a5_path = appendices_dir / "A5_Site_Specific_Plans_Register.md"
    a5_content = "# Appendix 5: Site-Specific Plans Register\n\n"
    a5_content += "| Plan Name | Status | Owner | Due Date | Approval Date | File Reference |\n"
    a5_content += "| --- | --- | --- | --- | --- | --- |\n"
    for plan_name, details in processing.sub_plan_matrix.items():
        status = details.get("status", "Not Applicable")
        justification = details.get("justification", "")
        a5_content += f"| {plan_name} | {status} | *Assign* | | | {justification} |\n"
    a5_path.write_text(a5_content, encoding="utf-8")
    created.append(str(a5_path))
    
    # A6: Revision Log
    a6_path = appendices_dir / "A6_Revision_Log.md"
    a6_content = """# Appendix 6: Revision Log

| Revision | Date | Change Description | Author | Approved By |
| --- | --- | --- | --- | --- |
| 0 | | Initial issue | | |

"""
    a6_path.write_text(a6_content, encoding="utf-8")
    created.append(str(a6_path))
    
    return created


class DefaultDocumentIngestionService(DocumentIngestionService):
    """Ingestion service leveraging context document parsing helpers with ChromaDB indexing."""

    def __init__(self, ocr_threshold: int = 200) -> None:
        self.engine = DocumentIngestionEngine(ocr_threshold=ocr_threshold)

    def ingest(
        self,
        choice: DocumentSourceChoice,
        decision_provider: DecisionProvider,
        run_id: str,
        config: Dict[str, Any],
    ) -> DocumentIngestionResult:
        logs: List[str] = []
        documents: List[str] = []

        if choice is DocumentSourceChoice.PLACEHOLDER:
            logs.append("Using placeholders; no project documents ingested.")
            return DocumentIngestionResult(
                documents=[],
                extracted_text="",
                metadata_candidates={},
                metadata_files=[],
                dfow=[],
                hazards=[],
                citations=[],
                logs=logs,
            )

        if choice is DocumentSourceChoice.EXISTING:
            documents = [
                str(Path(p).resolve())
                for p in config.get("existing_document_paths", [])
                if p
            ]
            logs.append(
                f"Loaded {len(documents)} document(s) from configured existing inputs."
            )
        elif choice is DocumentSourceChoice.UPLOAD:
            documents = [
                str(Path(p).resolve())
                for p in decision_provider.provide_upload_paths()
                if p
            ]
            logs.append(f"Received {len(documents)} uploaded document(s) for run {run_id}.")

        if not documents:
            logs.append("No documents provided; continuing with placeholders.")
            return DocumentIngestionResult(
                documents=[],
                extracted_text="",
                metadata_candidates={},
                metadata_files=[],
                dfow=[],
                hazards=[],
                citations=[],
                logs=logs,
            )

        result = self.engine.ingest(documents, run_id=run_id)
        
        # Index documents into ChromaDB for evidence-based retrieval
        collection_name = config.get("collection_name", "csp_documents")
        if documents and collection_name:
            try:
                from utils import (
                    get_chroma_client,
                    get_default_chroma_dir,
                    add_documents_to_collection,
                    get_or_create_collection,
                )
                from context.heading_aware_chunking import chunk_by_headings, create_chunk_metadata
                from context.document_sanitizer import tag_chunk_for_exclusion
                
                chroma_client = get_chroma_client(get_default_chroma_dir())
                collection = get_or_create_collection(chroma_client, collection_name)
                
                all_chunk_ids = []
                all_chunk_docs = []
                all_chunk_metas = []
                
                # Use the already-sanitized text from ingestion result
                # The result has extracted_text which is combined, but we need per-doc
                # For now, use combined text or re-read per document
                for doc_path in documents:
                    path = Path(doc_path)
                    doc_id = path.stem
                    
                    # Get sanitized text from ingestion (it's already sanitized in engine)
                    # Re-read and sanitize to ensure consistency
                    text = ""
                    if path.suffix.lower() == ".pdf":
                        from context.document_ingestion import _read_pdf_text
                        text = _read_pdf_text(path, self.engine.ocr_threshold, None)
                    elif path.suffix.lower() == ".docx":
                        from context.document_ingestion import _read_docx_text
                        text = _read_docx_text(path)
                    else:
                        from context.document_ingestion import _read_text_file
                        text = _read_text_file(path)
                    
                    # Sanitize (remove TOC, headers, boilerplate)
                    from context.document_sanitizer import sanitize_document_text
                    sanitized = sanitize_document_text(text)
                    
                    # Chunk by headings
                    chunks = chunk_by_headings(sanitized, max_chunk_size=2000, overlap=200)
                    
                    # Index chunks
                    for idx, chunk in enumerate(chunks):
                        # Skip excluded chunks
                        if tag_chunk_for_exclusion(chunk.text):
                            continue
                        
                        metadata = create_chunk_metadata(chunk, doc_id, idx)
                        metadata["source"] = str(path)
                        metadata["file"] = path.name
                        
                        all_chunk_ids.append(metadata["chunk_id"])
                        all_chunk_docs.append(chunk.text)
                        all_chunk_metas.append(metadata)
                
                # Add to collection in batches
                if all_chunk_ids:
                    add_documents_to_collection(
                        collection,
                        all_chunk_ids,
                        all_chunk_docs,
                        all_chunk_metas,
                        batch_size=100,
                    )
                    logs.append(f"Indexed {len(all_chunk_ids)} chunks into ChromaDB collection '{collection_name}'")
            except Exception as e:
                logs.append(f"Warning: Failed to index documents to ChromaDB: {e}")
        
        result.logs = logs + result.logs
        return result


@dataclass(slots=True)
class DefaultProjectMetadataManager(ProjectMetadataManager):
    """Collects metadata inputs or falls back to placeholders."""

    REQUIRED_FIELDS: Iterable[str] = field(
        default_factory=lambda: (
            # Core project info - always required
            "project_name",
            "location",
            "owner",
            "prime_contractor",
            # Key personnel - names preferred but role titles acceptable
            "ssho",
            "project_manager",
            # Additional personnel - optional, role titles used if not found
            # These are marked as required but validation allows role titles
            "corporate_safety_officer",
            "quality_control_manager",
            "superintendent",
        )
    )

    def resolve(
        self,
        choice: MetadataSourceChoice,
        ingestion: DocumentIngestionResult,
        decision_provider: DecisionProvider,
        run_id: str,
        config: Dict[str, Any],
    ) -> MetadataState:
        data: Dict[str, Any] = {
            k: v for k, v in (ingestion.metadata_candidates or {}).items() if v
        }
        placeholders: Dict[str, str] = {}
        warnings: List[str] = []
        metadata_sources: List[str] = []

        if choice is MetadataSourceChoice.FILE:
            candidate_paths: List[str] = []
            candidate_paths.extend(ingestion.metadata_files or [])
            config_paths = config.get("metadata_paths")
            if isinstance(config_paths, str):
                candidate_paths.append(config_paths)
            elif isinstance(config_paths, (list, tuple)):
                candidate_paths.extend([str(p) for p in config_paths])

            seen_paths = set()
            for path_str in candidate_paths:
                if not path_str:
                    continue
                path = Path(path_str).expanduser().resolve()
                if path in seen_paths:
                    continue
                seen_paths.add(path)
                if not path.exists():
                    warnings.append(f"Metadata file not found: {path}")
                    continue
                metadata_sources.append(str(path))
                if path.suffix.lower() == ".json":
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                    except Exception as exc:
                        warnings.append(f"Failed to load metadata file {path}: {exc}")
                        continue
                else:
                    warnings.append(f"Unsupported metadata format: {path.name}")
                    continue

                if isinstance(payload, dict):
                    if isinstance(payload.get("project_meta"), dict):
                        payload = payload["project_meta"]
                    for key, value in payload.items():
                        if isinstance(value, str) and value and not data.get(key):
                            data[key] = value

            if not metadata_sources and not data:
                warnings.append("No metadata file detected; falling back to placeholders.")
        elif choice is MetadataSourceChoice.MANUAL:
            manual = decision_provider.provide_metadata_overrides()
            data.update({k: v for k, v in manual.items() if v})
            if manual:
                metadata_sources.append("manual-entry")
        elif choice is MetadataSourceChoice.PLACEHOLDER:
            warnings.append("Using placeholder metadata; validation may flag missing fields.")

        # Only create placeholders if extraction was attempted but failed
        # If no extraction was attempted (placeholder mode), still create placeholders
        extraction_attempted = (
            choice is MetadataSourceChoice.FILE 
            or (choice is MetadataSourceChoice.PLACEHOLDER and not ingestion.metadata_candidates)
        )
        
        for field in self.REQUIRED_FIELDS:
            if not data.get(field):
                label = field.replace('_', ' ').title()
                # Only add placeholder if extraction was attempted or we're in placeholder mode
                if extraction_attempted or choice is MetadataSourceChoice.PLACEHOLDER:
                    placeholders[field] = format_placeholder(f"Insert {label}")
                else:
                    # In manual mode, don't create placeholders - user should provide
                    pass

        if data and not metadata_sources and ingestion.metadata_candidates:
            metadata_sources.append("document-extraction")

        risk_role_defaults = {
            "residual_risk_extreme": ("corporate_safety_officer", "Insert Corporate Safety Officer Name"),
            "residual_risk_high": ("project_manager", "Insert Project Manager Name"),
            "residual_risk_medium": ("ssho", "Insert SSHO Name"),
            "residual_risk_low": ("foreman", "Insert Foreman or Field Supervisor Name"),
        }

        for field, (fallback_key, placeholder_label) in risk_role_defaults.items():
            current_value = data.get(field)
            if current_value and not contains_placeholder(str(current_value)):
                continue
            fallback_value = data.get(fallback_key)
            if fallback_key and not fallback_value:
                placeholder_value = format_placeholder(placeholder_label)
                data[fallback_key] = placeholder_value
                placeholders.setdefault(fallback_key, placeholder_value)
                fallback_value = placeholder_value
            if fallback_value and not contains_placeholder(str(fallback_value)):
                data[field] = fallback_value
            else:
                placeholder_value = format_placeholder(placeholder_label)
                data[field] = placeholder_value
                placeholders[field] = placeholder_value
            if contains_placeholder(str(data[field])):
                placeholders.setdefault(field, data[field])

        return MetadataState(
            data=data,
            source=choice,
            sources=metadata_sources,
            placeholders=placeholders,
            warnings=warnings,
        )


class DefaultProcessingEngine(ProcessingEngine):
    """Evidence-based processing engine using two-step generation."""

    def process(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        run_id: str,
        config: Dict[str, Any],
    ) -> ProcessingState:
        import asyncio
        
        sub_plan_matrix = map_dfow_to_plans(ingestion.dfow or [], ingestion.hazards or [])
        
        # Check if we should use evidence-based generation
        collection_name = config.get("collection_name")
        use_evidence_based = config.get("use_evidence_based_generation", True)
        
        if use_evidence_based and collection_name:
            # Use evidence-based generation
            try:
                from utils import get_chroma_client, get_default_chroma_dir
                from generators.evidence_generator import EvidenceBasedSectionGenerator
                
                chroma_client = get_chroma_client(get_default_chroma_dir())
                generator = EvidenceBasedSectionGenerator(
                    collection_name=collection_name,
                    chroma_client=chroma_client,
                )
                
                # Build project context
                project_context = {
                    "dfow": ingestion.dfow or [],
                    "hazards": ingestion.hazards or [],
                    "project_name": metadata.data.get("project_name", ""),
                    "location": metadata.data.get("location", ""),
                    "owner": metadata.data.get("owner", ""),
                }
                
                # Generate sections using evidence-based approach
                from context.context_builder import SECTION_DEFINITIONS
                from models.csp import CspSection, CspCitation
                
                evidence_sections = []
                logs = [f"Using evidence-based generation for {len(SECTION_DEFINITIONS)} sections."]
                
                async def generate_all_sections():
                    generated = []
                    for section_def in SECTION_DEFINITIONS:
                        result = await generator.generate_section(
                            section_def.identifier,
                            project_context,
                            section_def.em385_refs,
                        )
                        
                        if result.has_insufficient_evidence:
                            logs.append(f"⚠️ {section_def.title}: INSUFFICIENT EVIDENCE")
                            # Fallback to template-based for this section
                            continue
                        
                        # Convert to CspSection
                        paragraphs = result.section_text.split("\n\n")
                        citations = [
                            CspCitation(
                                section_path=cit.get("section_path", ""),
                                page_label=cit.get("page_label", ""),
                                source_url=cit.get("source_url", ""),
                            )
                            for cit in result.citations
                        ]
                        
                        generated.append(CspSection(
                            name=section_def.title,
                            paragraphs=paragraphs,
                            citations=citations,
                        ))
                        
                        logs.append(
                            f"✅ {section_def.title}: {len(result.evidence_bullets)} evidence bullets, "
                            f"{result.contamination_removed} contaminated sentences removed"
                        )
                    
                    return generated
                
                # Run async generation
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                evidence_sections = loop.run_until_complete(generate_all_sections())
                
                # Build context packs for remaining sections (for compatibility)
                context_packs = build_context_packs(ingestion, metadata, sub_plan_matrix)
                
                # Use evidence sections if we got any, otherwise fall back
                if evidence_sections:
                    csp_sections = evidence_sections
                    logs.append(f"Generated {len(evidence_sections)} evidence-based sections.")
                else:
                    # Fallback to template-based
                    logs.append("Falling back to template-based generation.")
                    csp_sections = build_csp_sections(context_packs)
            except Exception as e:
                # Fallback to template-based on error
                logs.append(f"Evidence-based generation failed: {e}. Using template-based fallback.")
                context_packs = build_context_packs(ingestion, metadata, sub_plan_matrix)
                csp_sections = build_csp_sections(context_packs)
        else:
            # Template-based (legacy)
            context_packs = build_context_packs(ingestion, metadata, sub_plan_matrix)
            csp_sections = build_csp_sections(context_packs)
            logs = [
                f"Detected {len(ingestion.dfow or [])} definable feature(s) of work.",
                f"Sub-plan applicability matrix generated for {len(sub_plan_matrix)} plan(s).",
                f"Assembled context packs for {len(context_packs)} CSP section(s).",
            ]
        
        pending_plans = sum(1 for details in sub_plan_matrix.values() if details.get("status") != "Not Applicable")
        logs.append(f"{pending_plans} plan(s) require review or development.")
        
        return ProcessingState(
            context_packs=context_packs if 'context_packs' in locals() else build_context_packs(ingestion, metadata, sub_plan_matrix),
            sections=csp_sections,
            sub_plan_matrix=sub_plan_matrix,
            manifest_fragments={},
            logs=logs,
        )


class DefaultValidator(CSPValidator):
    """Permissive validator until full validation rules are implemented."""

    def validate(
        self,
        metadata: MetadataState,
        processing: ProcessingState,
    ) -> ValidationState:
        errors: List[str] = []
        warnings: List[str] = []

        missing_required = [
            field for field, placeholder in metadata.placeholders.items() if placeholder
        ]
        if missing_required:
            warnings.append(
                "Required metadata fields rely on placeholders: " + ", ".join(missing_required)
            )

        expected_titles = {definition.title for definition in SECTION_DEFINITIONS}
        produced_titles = {section.name for section in (processing.sections or [])}
        missing_sections = sorted(expected_titles - produced_titles)
        if missing_sections:
            errors.append("Missing CSP sections: " + ", ".join(missing_sections))

        name_to_identifier = {definition.title: definition.identifier for definition in SECTION_DEFINITIONS}

        # Fail-fast: scan for unresolved placeholders/tokens
        from context.placeholder_manager import find_unresolved_tokens
        unresolved_tokens_by_section: Dict[str, List[Tuple[str, int]]] = {}
        total_unresolved = 0
        
        for section in processing.sections or []:
            combined_text = " ".join(section.paragraphs)
            tokens = find_unresolved_tokens(combined_text)
            if tokens:
                unresolved_tokens_by_section[section.name] = tokens
                total_unresolved += len(tokens)
            combined = combined_text.lower()
            for token in ["purpose:", "procedures", "responsibilities", "forms", "references:"]:
                if token not in combined:
                    errors.append(f"Section '{section.name}' is missing subsection '{token}'.")
                    break
            if "em 385" not in combined:
                warnings.append(f"Section '{section.name}' does not explicitly reference EM 385-1-1 in text.")
            if any(contains_placeholder(par) for par in section.paragraphs):
                errors.append(f"Section '{section.name}' contains unresolved placeholders. Export blocked.")
            
            # Check context for warnings
            identifier = name_to_identifier.get(section.name)
            context = processing.context_packs.get(identifier, {}) if identifier else {}
            if context is not None and not context.get("documents"):
                warnings.append(f"Section '{section.name}' generated without user document context (LLM guidance only).")
            if context is not None and not context.get("snippets"):
                warnings.append(f"Section '{section.name}' has no supporting document snippets; review for accuracy.")
            if not section.citations:
                warnings.append(f"Section '{section.name}' produced without formal citations.")
        
        # Check title block fields
        required_title_fields = ["project_name", "location", "owner", "prime_contractor"]
        for field in required_title_fields:
            value = metadata.data.get(field, "")
            if not value or contains_placeholder(str(value)):
                errors.append(f"Required title block field '{field}' is missing or placeholder. Export blocked.")
        
        # Fail if placeholders found
        if total_unresolved > 0:
            error_details = []
            for section_name, tokens in unresolved_tokens_by_section.items():
                token_patterns = [token[0] for token in tokens]
                unique_patterns = sorted(set(token_patterns))
                error_details.append(f"  {section_name}: {', '.join(unique_patterns)}")
            errors.append(
                f"Export blocked: {total_unresolved} unresolved placeholder(s) found:\n" + "\n".join(error_details)
            )

        if not processing.sub_plan_matrix:
            warnings.append("Sub-plan applicability matrix is empty; verify DFOW mapping.")
        
        # Validate at least one plan is marked Required
        required_plans = sum(
            1 for details in processing.sub_plan_matrix.values()
            if details.get("status") in ("Required", "Pending")
        )
        if required_plans == 0:
            warnings.append("No site-specific plans marked as Required or Pending. Verify DFOW mapping.")
        
        # Validate evidence-based sections (if used)
        from context.contamination_guard import detect_contamination, BANNED_PHRASES
        insufficient_evidence_count = 0
        contaminated_sections = []
        
        for section in processing.sections or []:
            combined_text = " ".join(section.paragraphs)
            
            # Check for "INSUFFICIENT EVIDENCE"
            if "INSUFFICIENT EVIDENCE" in combined_text:
                insufficient_evidence_count += 1
                errors.append(f"Section '{section.name}' has insufficient evidence. Export blocked.")
            
            # Check for banned phrases (contamination)
            banned_detected = detect_contamination(combined_text)
            if banned_detected:
                contaminated_sections.append(section.name)
                errors.append(
                    f"Section '{section.name}' contains banned phrases: {', '.join(banned_detected[:3])}. Export blocked."
                )
            
            # Check evidence count (for evidence-based sections)
            # If section has citations, verify minimum count
            if section.citations and len(section.citations) < 3:
                warnings.append(
                    f"Section '{section.name}' has only {len(section.citations)} citations. "
                    "Minimum 3 citations recommended for evidence-based sections."
                )

        can_proceed = not errors

        return ValidationState(
            errors=errors,
            warnings=warnings,
            placeholders_required=metadata.placeholders,
            can_proceed=can_proceed,
        )


class DefaultOutputAssembler(OutputAssembler):
    """Creates placeholder outputs to satisfy pipeline structure."""

    def assemble(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        processing: ProcessingState,
        validation: ValidationState,
        run_id: str,
        config: Dict[str, Any],
    ) -> OutputState:
        base_dir = Path(config.get("output_dir", "outputs/Compiled_CSP_Final"))
        _ensure_output_dir(base_dir)

        csp_doc = assemble_csp_doc(metadata.data, processing.context_packs)
        docx_path = base_dir / "Compiled_CSP_Final.docx"
        pdf_path = base_dir / "Compiled_CSP_Final.pdf"
        manifest_path = base_dir / "manifest.json"

        # Generate appendix stubs before export
        appendices_created = _generate_appendices(base_dir, processing)
        
        write_csp_docx(csp_doc, str(docx_path))
        write_csp_pdf(csp_doc, str(pdf_path))

        name_to_identifier = {definition.title: definition.identifier for definition in SECTION_DEFINITIONS}
        
        # Count placeholders and unresolved tokens
        from context.placeholder_manager import find_unresolved_tokens
        unresolved_tokens: Dict[str, List[str]] = {}
        placeholders_remaining_count = 0
        for section in csp_doc.sections:
            combined_text = " ".join(section.paragraphs)
            tokens = find_unresolved_tokens(combined_text)
            if tokens:
                unresolved_tokens[section.name] = sorted(set([t[0] for t in tokens]))
                placeholders_remaining_count += len(tokens)
        sections_payload = []
        unique_documents: set[str] = set()
        for section in csp_doc.sections:
            identifier = name_to_identifier.get(section.name)
            context = processing.context_packs.get(identifier, {}) if identifier else {}
            context_docs = [Path(doc).name for doc in (context.get("documents", []) or []) if doc]
            unique_documents.update(context_docs)
            sections_payload.append({
                "name": section.name,
                "identifier": identifier,
                "em385_refs": context.get("em385_refs", []),
                "documents": context_docs,
                "has_references": any((par or "").strip().startswith("References:") for par in section.paragraphs),
                "placeholder_flags": bool(context.get("placeholders")),
                "placeholder_count": sum(1 for par in section.paragraphs if contains_placeholder(par)),
                "citation_count": len(section.citations or []),
                "document_count": len(context_docs),
                "llm_guidance_only": len(context_docs) == 0,
            })

        placeholder_totals = {
            "sections": sum(item.get("placeholder_count", 0) for item in sections_payload),
            "metadata_fields": len(metadata.placeholders),
        }
        
        # Calculate site plans status
        required_plans = [name for name, d in processing.sub_plan_matrix.items() if d.get("status") in ("Required", "Pending")]
        pending_plans = [name for name, d in processing.sub_plan_matrix.items() if d.get("status") == "Pending"]
        na_plans = [name for name, d in processing.sub_plan_matrix.items() if d.get("status") == "Not Applicable"]
        
        # Citations per section
        citations_per_section_count = {item["name"]: item.get("citation_count", 0) for item in sections_payload}
        
        manifest_metrics = {
            "sections_total": len(sections_payload),
            "citations_total": sum(item.get("citation_count", 0) for item in sections_payload),
            "citations_per_section_count": citations_per_section_count,
            "llm_guidance_sections": [item["name"] for item in sections_payload if item.get("llm_guidance_only")],
            "documents_referenced": sorted(unique_documents),
            "dfow_detected": processing.context_packs.get("section_02", {}).get("dfow", []),
            "placeholders_remaining_count": placeholders_remaining_count,
            "unresolved_tokens": unresolved_tokens,
            "export_blocked_due_to_placeholders": placeholders_remaining_count > 0,
            "site_plans_required": [{"name": name, "justification": processing.sub_plan_matrix[name].get("justification", "")} for name in required_plans],
            "site_plans_pending": [{"name": name, "justification": processing.sub_plan_matrix[name].get("justification", "")} for name in pending_plans],
            "site_plans_na": [{"name": name, "justification": processing.sub_plan_matrix[name].get("justification", "")} for name in na_plans],
            "appendices_created": [Path(p).name for p in appendices_created],
        }

        manifest = {
            "run_id": run_id,
            "project": {
                "name": csp_doc.project_name,
                "number": csp_doc.project_number,
                "location": csp_doc.location,
                "owner": csp_doc.owner,
                "prime_contractor": csp_doc.general_contractor,
            },
            "metadata_sources": metadata.sources,
            "placeholders": metadata.placeholders,
            "placeholder_totals": placeholder_totals,
            "metrics": manifest_metrics,
            "documents": [str(Path(p)) for p in ingestion.documents],
            "sections": sections_payload,
            "sub_plans": processing.sub_plan_matrix,
            "warnings": validation.warnings,
            "outputs": {
                "docx": str(docx_path),
                "pdf": str(pdf_path),
            },
            "residual_risk_approval": {
                "extremely_high": metadata.data.get("residual_risk_extreme"),
                "high": metadata.data.get("residual_risk_high"),
                "medium": metadata.data.get("residual_risk_medium"),
                "low": metadata.data.get("residual_risk_low"),
            },
        }

        package_path: Path | None = base_dir / "Compiled_CSP_Final_package.zip"
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        except Exception:
            validation.warnings.append("Failed to write manifest.json; check file permissions.")
        try:
            with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
                for artifact in (docx_path, pdf_path, manifest_path):
                    if Path(artifact).exists():
                        bundle.write(Path(artifact), arcname=Path(artifact).name)
                # Add appendices to bundle
                appendices_dir = base_dir / "appendices"
                if appendices_dir.exists():
                    for appendix_file in appendices_dir.glob("*.md"):
                        bundle.write(appendix_file, arcname=f"appendices/{appendix_file.name}")
                snapshot_path = base_dir / "context" / "snapshot.json"
                if snapshot_path.exists():
                    bundle.write(snapshot_path, arcname=f"context/{snapshot_path.name}")
        except Exception:
            validation.warnings.append("Failed to build CSP package archive.")
            package_path = None

        if package_path and Path(package_path).exists():
            manifest.setdefault("outputs", {})["package"] = str(package_path)

        return OutputState(
            docx_path=str(docx_path),
            pdf_path=str(pdf_path),
            manifest_path=str(manifest_path),
            logs_path=None,
            extra={
                "section_count": len(csp_doc.sections),
                "metadata_sources": metadata.sources,
                "dfow_count": len(processing.context_packs.get("section_02", {}).get("dfow", [])),
                "residual_risk_approval": manifest["residual_risk_approval"],
                "citations_total": manifest_metrics["citations_total"],
                "llm_guidance_sections": manifest_metrics["llm_guidance_sections"],
                "documents_referenced": manifest_metrics["documents_referenced"],
                "package_path": str(package_path) if package_path else "",
            },
        )


class DefaultPostProcessor(PostProcessor):
    """No-op post processor placeholder."""

    def finalize(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        processing: ProcessingState,
        outputs: OutputState,
        run_id: str,
        config: Dict[str, Any],
    ) -> None:
        base_dir = Path(config.get("output_dir", "outputs/Compiled_CSP_Final"))
        context_dir = base_dir / "context"
        context_dir.mkdir(parents=True, exist_ok=True)

        snapshot = {
            "run_id": run_id,
            "documents": ingestion.documents,
            "dfow": ingestion.dfow,
            "hazards": ingestion.hazards,
            "metadata": metadata.data,
            "placeholders": metadata.placeholders,
            "metadata_sources": metadata.sources,
            "sub_plans": processing.sub_plan_matrix,
            "logs": processing.logs,
        }
        try:
            (context_dir / "snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        except Exception:
            pass


def build_placeholder_dependencies(
    decision_provider: DecisionProvider,
) -> PipelineDependencies:
    """Construct pipeline dependencies using the default placeholder services."""

    return PipelineDependencies(
        decision_provider=decision_provider,
        document_ingestion=DefaultDocumentIngestionService(),
        metadata_manager=DefaultProjectMetadataManager(),
        processing_engine=DefaultProcessingEngine(),
        validator=DefaultValidator(),
        output_assembler=DefaultOutputAssembler(),
        post_processor=DefaultPostProcessor(),
    )


__all__ = [
    "DefaultDocumentIngestionService",
    "DefaultProjectMetadataManager",
    "DefaultProcessingEngine",
    "DefaultValidator",
    "DefaultOutputAssembler",
    "DefaultPostProcessor",
    "build_placeholder_dependencies",
]

