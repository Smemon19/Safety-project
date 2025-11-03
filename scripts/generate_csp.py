#!/usr/bin/env python3
"""CLI entry point for running the CSP generation pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipelines.csp_pipeline import ValidationError
from pipelines.decision_providers import CLIDecisionProvider
from pipelines.runtime import build_pipeline, generate_run_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the ENG Form 6293 CSP generation pipeline end-to-end.",
    )
    parser.add_argument(
        "--document-source",
        choices=["existing", "upload", "placeholder"],
        default="placeholder",
        help="Where project documents should be sourced from for ingestion.",
    )
    parser.add_argument(
        "--metadata-source",
        choices=["file", "manual", "placeholder"],
        default="placeholder",
        help="How required project metadata should be supplied.",
    )
    parser.add_argument(
        "--upload",
        nargs="*",
        default=[],
        metavar="PATH",
        help="File path(s) to newly uploaded project documents (for --document-source upload).",
    )
    parser.add_argument(
        "--existing",
        nargs="*",
        default=[],
        metavar="PATH",
        help="File path(s) to existing project documents for reuse.",
    )
    parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Manual metadata overrides when using --metadata-source manual.",
    )
    parser.add_argument(
        "--reject-placeholders",
        action="store_true",
        help="Fail fast if required metadata fields would rely on placeholders.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/Compiled_CSP_Final",
        help="Directory where compiled CSP artifacts should be written.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run identifier for diagnostics and logs.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    run_id = args.run_id or generate_run_id("csp-cli")
    provider = CLIDecisionProvider(args=args)

    config = {
        "existing_document_paths": [str(Path(p).resolve()) for p in args.existing],
        "output_dir": str(Path(args.output_dir).resolve()),
        "run_mode": "cli",
    }

    pipeline = build_pipeline(
        decision_provider=provider,
        config=config,
        run_id=run_id,
    )

    try:
        result = pipeline.run()
    except ValidationError as exc:
        print(f"[csp-cli] Validation failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[csp-cli] Pipeline error: {exc}", file=sys.stderr)
        return 1

    summary = {
        "run_id": run_id,
        "documents": result.ingestion.documents,
        "metadata_source": result.metadata.source.value,
        "missing_placeholders": result.validation.placeholders_required,
        "outputs": {
            "docx": result.outputs.docx_path,
            "pdf": result.outputs.pdf_path,
            "manifest": result.outputs.manifest_path,
        },
        "warnings": result.validation.warnings,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

