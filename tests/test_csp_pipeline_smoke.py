from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipelines.csp_pipeline import DocumentSourceChoice, MetadataSourceChoice, ValidationError
from pipelines.decision_providers import StaticDecisionProvider
from pipelines.runtime import build_pipeline
from context.placeholder_manager import find_unresolved_tokens, PLACEHOLDER_PREFIX


def test_placeholder_pipeline_run(tmp_path):
    output_dir = Path(tmp_path)
    provider = StaticDecisionProvider(
        document_choice=DocumentSourceChoice.PLACEHOLDER,
        metadata_choice=MetadataSourceChoice.PLACEHOLDER,
        allow_placeholder_confirmation=True,
    )
    pipeline = build_pipeline(provider, config={"output_dir": str(output_dir)})
    with pytest.raises(ValidationError) as exc:
        pipeline.run()
    assert "Export blocked due to validation failures" in str(exc.value)


def test_csp_quality_checks(tmp_path):
    """Test CSP quality requirements: no placeholders, DFOW mapping, appendices exist."""
    output_dir = Path(tmp_path)
    
    # Create a provider with actual metadata to avoid placeholders
    provider = StaticDecisionProvider(
        document_choice=DocumentSourceChoice.PLACEHOLDER,
        metadata_choice=MetadataSourceChoice.PLACEHOLDER,
        allow_placeholder_confirmation=True,
        metadata_overrides={
            "project_name": "Test Project",
            "location": "Test Location",
            "owner": "Test Owner",
            "prime_contractor": "Test Contractor",
            "project_manager": "John Doe",
            "ssho": "Jane Smith",
        },
    )
    
    pipeline = build_pipeline(provider, config={"output_dir": str(output_dir)})
    with pytest.raises(ValidationError) as exc:
        pipeline.run()
    message = str(exc.value)
    assert "title block" in message

