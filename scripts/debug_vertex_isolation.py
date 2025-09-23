#!/usr/bin/env python
"""Quick isolation check for Vertex AI Vector Search configuration.

Prints active config values and executes a trivial neighbor query to verify that
results are filtered by the configured namespace. Warns if any results are found
when the namespace is expected to be empty.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Optional

try:
    from google.cloud import aiplatform  # type: ignore
    _HAS_CLIENT = True
except Exception:
    aiplatform = None  # type: ignore
    _HAS_CLIENT = False

from config import load_vertex_config, log_active_config


def read_endpoint_deployments_via_gcloud(endpoint_id: str, region: str) -> list[str]:
    try:
        out = subprocess.check_output(
            [
                "gcloud",
                "ai",
                "index-endpoints",
                "describe",
                endpoint_id,
                "--region",
                region,
                "--format=json",
            ],
            text=True,
        )
        j = json.loads(out)
        return [d.get("id") for d in (j.get("deployedIndexes") or [])]
    except Exception as e:
        print(f"[debug] gcloud describe failed: {e}")
        return []


def run_smoke_query() -> int:
    log_active_config(prefix="[debug-config]", allow_missing=False)
    cfg = load_vertex_config(raise_on_missing=True)

    deployed_ids: list[str] = []

    if _HAS_CLIENT:
        try:
            aiplatform.init(project=cfg.gcp_project_id, location=cfg.gcp_region)  # type: ignore
            endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=cfg.endpoint_id)  # type: ignore
            res = endpoint.gca_resource
            deployed_ids = [d.id for d in (getattr(res, "deployed_indexes", []) or [])]
        except Exception as e:
            print(f"[debug] Python client failed, falling back to gcloud: {e}")
            deployed_ids = read_endpoint_deployments_via_gcloud(cfg.endpoint_id, cfg.gcp_region)
    else:
        print("[debug] Python client unavailable, using gcloud fallback")
        deployed_ids = read_endpoint_deployments_via_gcloud(cfg.endpoint_id, cfg.gcp_region)

    print(f"[debug] deployedIndexes: {deployed_ids}")

    # Namespace empty check: skip actual neighbor query if client unavailable
    if not _HAS_CLIENT:
        print("[debug] Neighbor check: SKIPPED (client not available)")
        return 0

    try:
        # Build zero vector using inferred dims
        from config import embedding_dimensions_from_env
        dims = embedding_dimensions_from_env()
        vec = [0.0] * dims
        endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=cfg.endpoint_id)  # type: ignore
        did = cfg.deployed_index_id or (deployed_ids[0] if deployed_ids else None)
        if not did:
            print("[debug] No deployed index id resolved; skipping neighbor check")
            return 0
        resp = endpoint.find_neighbors(
            deployed_index_id=did,
            queries=[{"datapoint": {"feature_vector": vec}, "neighbor_count": 5, "filter": {"namespace": cfg.namespace}}],
        )
        n = 0
        try:
            n = len(resp[0].neighbors)
        except Exception:
            n = 0
        print(f"[debug] Neighbor count in namespace '{cfg.namespace}': {n}")
        if n > 0:
            print("[warn] Expected zero for fresh namespace.")
        return 0
    except Exception as e:
        print(f"[debug] Neighbor check failed: {e}")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(run_smoke_query())
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(2)


