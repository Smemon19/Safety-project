from __future__ import annotations

"""Utility helpers for constructing CSP pipeline instances."""

import datetime as _dt
import uuid
from typing import Callable, Dict, Optional

from .csp_pipeline import CSPPipeline, PipelineDependencies
from .decision_providers import DecisionProvider
from .services.defaults import build_placeholder_dependencies


def generate_run_id(prefix: str = "csp") -> str:
    timestamp = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{prefix}-{timestamp}-{short_uuid}"


def build_pipeline(
    decision_provider: DecisionProvider,
    config: Optional[Dict[str, object]] = None,
    deps_factory: Optional[Callable[[DecisionProvider], PipelineDependencies]] = None,
    run_id: Optional[str] = None,
) -> CSPPipeline:
    """Construct a CSPPipeline with either default or provided dependencies."""

    deps_builder = deps_factory or build_placeholder_dependencies
    pipeline_deps = deps_builder(decision_provider)
    config = dict(config or {})
    run_id = run_id or generate_run_id()
    return CSPPipeline(run_id=run_id, deps=pipeline_deps, config=config)


__all__ = ["build_pipeline", "generate_run_id"]

