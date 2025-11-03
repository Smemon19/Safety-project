from __future__ import annotations

"""End-to-end CSP generation pipeline orchestrator.

This module coordinates the five high-level phases required to build a
Construction Safety Plan (CSP) compliant with ENG Form 6293 (Aug 2023):

1. Document ingestion and context setup.
2. Metadata verification / retrieval.
3. Automated processing after ingestion.
4. Fail-fast validation.
5. Optional post-processing and persistence.

The orchestrator itself stays implementation-agnostic by delegating the heavy
lifting to pluggable services defined via lightweight protocols. Subsequent
implementation steps populate those services with concrete logic for
document parsing, metadata management, context pack construction, CSP section
generation, output exporting, and manifest maintenance.

The goal for this scaffold is to provide a single entry point that guides the
control flow, enforces required checkpoints, and exposes structured results to
downstream callers (Streamlit UI, CLI utilities, automated tests).
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol


class DocumentSourceChoice(str, Enum):
    """User-facing choice of where project documents originate."""

    EXISTING = "existing"
    UPLOAD = "upload"
    PLACEHOLDER = "placeholder"


class MetadataSourceChoice(str, Enum):
    """User-facing choice for how required project metadata is populated."""

    FILE = "file"
    MANUAL = "manual"
    PLACEHOLDER = "placeholder"


@dataclass(slots=True)
class DocumentIngestionResult:
    """Structured payload returned by the document ingestion phase."""

    documents: List[str] = field(default_factory=list)
    extracted_text: str = ""
    metadata_candidates: Dict[str, Any] = field(default_factory=dict)
    metadata_files: List[str] = field(default_factory=list)
    dfow: List[str] = field(default_factory=list)
    hazards: List[str] = field(default_factory=list)
    citations: List[str] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)


@dataclass(slots=True)
class MetadataState:
    """Represents the resolved project metadata after Phase 2."""

    data: Dict[str, Any] = field(default_factory=dict)
    source: MetadataSourceChoice = MetadataSourceChoice.PLACEHOLDER
    sources: List[str] = field(default_factory=list)
    placeholders: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


@dataclass(slots=True)
class ProcessingState:
    """Holds artifacts from automated processing after ingestion."""

    context_packs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    sections: List[Any] = field(default_factory=list)
    sub_plan_matrix: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    manifest_fragments: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)


@dataclass(slots=True)
class ValidationState:
    """Captures validation outcomes prior to output compilation."""

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    placeholders_required: Dict[str, str] = field(default_factory=dict)
    can_proceed: bool = True


@dataclass(slots=True)
class OutputState:
    """Represents the final output artifact paths and manifest details."""

    docx_path: Optional[str] = None
    pdf_path: Optional[str] = None
    manifest_path: Optional[str] = None
    logs_path: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CSPPipelineResult:
    """Top-level container summarizing the full pipeline run."""

    ingestion: DocumentIngestionResult
    metadata: MetadataState
    processing: ProcessingState
    validation: ValidationState
    outputs: OutputState


class DecisionProvider(Protocol):
    """Abstraction for obtaining user decisions throughout the pipeline."""

    def choose_document_source(self) -> DocumentSourceChoice:
        ...

    def choose_metadata_source(self) -> MetadataSourceChoice:
        ...

    def confirm_placeholders(self, missing_fields: Iterable[str]) -> bool:
        ...

    def provide_upload_paths(self) -> List[str]:
        ...

    def provide_metadata_overrides(self) -> Dict[str, Any]:
        ...


class DocumentIngestionService(Protocol):
    """Handles Phase 1 document ingestion and context extraction."""

    def ingest(
        self,
        choice: DocumentSourceChoice,
        decision_provider: DecisionProvider,
        run_id: str,
        config: Dict[str, Any],
    ) -> DocumentIngestionResult:
        ...


class ProjectMetadataManager(Protocol):
    """Resolves project metadata and manages placeholder tracking."""

    REQUIRED_FIELDS: Iterable[str]

    def resolve(
        self,
        choice: MetadataSourceChoice,
        ingestion: DocumentIngestionResult,
        decision_provider: DecisionProvider,
        run_id: str,
        config: Dict[str, Any],
    ) -> MetadataState:
        ...


class ProcessingEngine(Protocol):
    """Performs Phase 3 automated processing once inputs are ready."""

    def process(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        run_id: str,
        config: Dict[str, Any],
    ) -> ProcessingState:
        ...


class CSPValidator(Protocol):
    """Validates readiness of CSP content before compilation."""

    def validate(
        self,
        metadata: MetadataState,
        processing: ProcessingState,
    ) -> ValidationState:
        ...


class OutputAssembler(Protocol):
    """Compiles final artifacts (DOCX, PDF, manifest, logs)."""

    def assemble(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        processing: ProcessingState,
        validation: ValidationState,
        run_id: str,
        config: Dict[str, Any],
    ) -> OutputState:
        ...


class PostProcessor(Protocol):
    """Optional persistence and incremental update handler for Phase 5."""

    def finalize(
        self,
        ingestion: DocumentIngestionResult,
        metadata: MetadataState,
        processing: ProcessingState,
        outputs: OutputState,
        run_id: str,
        config: Dict[str, Any],
    ) -> None:
        ...


@dataclass(slots=True)
class PipelineDependencies:
    """Container for injectable services used by the orchestrator."""

    decision_provider: DecisionProvider
    document_ingestion: DocumentIngestionService
    metadata_manager: ProjectMetadataManager
    processing_engine: ProcessingEngine
    validator: CSPValidator
    output_assembler: OutputAssembler
    post_processor: Optional[PostProcessor] = None


class CSPPipelineError(RuntimeError):
    """Base wrapper for pipeline-related runtime issues."""


class ValidationError(CSPPipelineError):
    """Raised when Phase 4 validation blocks compilation."""


@dataclass(slots=True)
class CSPPipeline:
    """High-level orchestrator tying all CSP generation phases together."""

    run_id: str
    deps: PipelineDependencies
    config: Dict[str, Any] = field(default_factory=dict)

    def run(self) -> CSPPipelineResult:
        """Execute the CSP generation workflow end-to-end."""

        # Phase 1 — Document Ingestion & Context Setup
        document_choice = self.deps.decision_provider.choose_document_source()
        ingestion = self.deps.document_ingestion.ingest(
            document_choice,
            self.deps.decision_provider,
            self.run_id,
            self.config,
        )

        # Phase 2 — Metadata Verification / Retrieval
        metadata_choice = self.deps.decision_provider.choose_metadata_source()
        metadata = self.deps.metadata_manager.resolve(
            metadata_choice,
            ingestion,
            self.deps.decision_provider,
            self.run_id,
            self.config,
        )

        # Fail-fast check for required metadata when placeholders are not allowed
        missing_required = [
            field
            for field in self.deps.metadata_manager.REQUIRED_FIELDS
            if not (metadata.data.get(field) or metadata.placeholders.get(field))
        ]
        if missing_required:
            allow_placeholders = self.deps.decision_provider.confirm_placeholders(
                missing_required
            )
            if not allow_placeholders:
                raise ValidationError(
                    "Pipeline halted: required metadata missing and placeholders rejected."  # noqa: E231
                )

        # Phase 3 — Automated Processing After Ingestion
        processing = self.deps.processing_engine.process(
            ingestion,
            metadata,
            self.run_id,
            self.config,
        )

        # Phase 4 — Fail-Fast Validation
        validation = self.deps.validator.validate(metadata, processing)
        if not validation.can_proceed:
            raise ValidationError(
                "Pipeline halted: validation errors prevent compilation: "
                + "; ".join(validation.errors)
            )

        # Phase 5 — Output Assembly & Manifesting
        outputs = self.deps.output_assembler.assemble(
            ingestion,
            metadata,
            processing,
            validation,
            self.run_id,
            self.config,
        )

        if self.deps.post_processor:
            self.deps.post_processor.finalize(
                ingestion,
                metadata,
                processing,
                outputs,
                self.run_id,
                self.config,
            )

        return CSPPipelineResult(
            ingestion=ingestion,
            metadata=metadata,
            processing=processing,
            validation=validation,
            outputs=outputs,
        )


__all__ = [
    "CSPPipeline",
    "CSPPipelineError",
    "ValidationError",
    "PipelineDependencies",
    "DocumentSourceChoice",
    "MetadataSourceChoice",
    "CSPPipelineResult",
    "DocumentIngestionResult",
    "MetadataState",
    "ProcessingState",
    "ValidationState",
    "OutputState",
]

