from __future__ import annotations

"""Decision provider implementations for CSP pipeline entry points."""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List

from .csp_pipeline import (
    DecisionProvider,
    DocumentSourceChoice,
    MetadataSourceChoice,
)


@dataclass(slots=True)
class StaticDecisionProvider(DecisionProvider):
    """Returns predetermined answers for all pipeline prompts."""

    document_choice: DocumentSourceChoice
    metadata_choice: MetadataSourceChoice
    upload_paths: List[str] = field(default_factory=list)
    metadata_overrides: Dict[str, Any] = field(default_factory=dict)
    allow_placeholder_confirmation: bool = True

    def choose_document_source(self) -> DocumentSourceChoice:
        return self.document_choice

    def choose_metadata_source(self) -> MetadataSourceChoice:
        return self.metadata_choice

    def confirm_placeholders(self, missing_fields: Iterable[str]) -> bool:
        return self.allow_placeholder_confirmation

    def provide_upload_paths(self) -> List[str]:
        return list(self.upload_paths)

    def provide_metadata_overrides(self) -> Dict[str, Any]:
        return dict(self.metadata_overrides)


@dataclass(slots=True)
class CLIDecisionProvider(DecisionProvider):
    """Interactive decision provider driven by CLI arguments."""

    args: Any

    def choose_document_source(self) -> DocumentSourceChoice:
        return DocumentSourceChoice(self.args.document_source)

    def choose_metadata_source(self) -> MetadataSourceChoice:
        return MetadataSourceChoice(self.args.metadata_source)

    def confirm_placeholders(self, missing_fields: Iterable[str]) -> bool:
        if getattr(self.args, "reject_placeholders", False):
            return False
        return True

    def provide_upload_paths(self) -> List[str]:
        paths = getattr(self.args, "upload", []) or []
        if isinstance(paths, str):
            return [paths]
        return list(paths)

    def provide_metadata_overrides(self) -> Dict[str, Any]:
        overrides: Dict[str, Any] = {}
        for key in getattr(self.args, "metadata", []) or []:
            if "=" not in key:
                continue
            k, v = key.split("=", 1)
            overrides[k.strip()] = v.strip()
        return overrides


@dataclass(slots=True)
class StreamlitDecisionProvider(DecisionProvider):
    """Decision provider backed by prior Streamlit UI selections."""

    document_choice: DocumentSourceChoice
    metadata_choice: MetadataSourceChoice
    upload_paths: List[str] = field(default_factory=list)
    metadata_overrides: Dict[str, Any] = field(default_factory=dict)
    allow_placeholders: bool = True

    def choose_document_source(self) -> DocumentSourceChoice:
        return self.document_choice

    def choose_metadata_source(self) -> MetadataSourceChoice:
        return self.metadata_choice

    def confirm_placeholders(self, missing_fields: Iterable[str]) -> bool:
        return self.allow_placeholders

    def provide_upload_paths(self) -> List[str]:
        return list(self.upload_paths)

    def provide_metadata_overrides(self) -> Dict[str, Any]:
        return dict(self.metadata_overrides)


__all__ = [
    "StaticDecisionProvider",
    "CLIDecisionProvider",
    "StreamlitDecisionProvider",
]

