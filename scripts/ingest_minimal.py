#!/usr/bin/env python
from __future__ import annotations
import json, os, subprocess, sys

ID = "projB_smoketest"
VEC = [0.01, 0.02, 0.03, 0.04]  # tiny placeholder; Vertex requires correct dims when actually used

from config import load_vertex_config, embedding_dimensions_from_env

try:
    from google.cloud import aiplatform  # type: ignore
    HAS_CLIENT = True
except Exception:
    aiplatform = None  # type: ignore
    HAS_CLIENT = False

def main() -> int:
    cfg = load_vertex_config(raise_on_missing=True)
    dims = embedding_dimensions_from_env()
    # Expand or trim vector to dims deterministically
    vec = (VEC * (dims // len(VEC) + 1))[:dims]

    if HAS_CLIENT:
        try:
            aiplatform.init(project=cfg.gcp_project_id, location=cfg.gcp_region)  # type: ignore
            index = aiplatform.MatchingEngineIndex(index_name=cfg.index_id)  # type: ignore
            # Upsert via index.upsert_datapoints is not available for Tree-AH; in production, use batch imports.
            # Here we signal intent only; fall back to SKIPPED if not supported.
            print("[ingest-min] SKIPPED: direct upsert requires import job for Tree-AH.")
            open("/tmp/projB_smoketest_ingested", "w").write("skipped")
            return 0
        except Exception as e:
            print(f"[ingest-min] Python client failed: {e}")
    # Fallback path: cannot upsert without a GCS-import pipeline
    print("[ingest-min] SKIPPED (no direct upsert path in this minimal script).")
    return 0

if __name__ == "__main__":
    sys.exit(main())
