#!/usr/bin/env python
"""Create and optionally deploy a Vertex AI Vector Search index.

This script is invoked by ops/vertex/create_index.sh and can be run directly.
It prints out the created Index ID, Endpoint ID (existing or newly created), and
the Deployed Index ID after deployment.
"""

import argparse
import sys
from typing import Optional

from google.cloud import aiplatform


def create_index(
    project: str,
    region: str,
    index_name: str,
    dimensions: int,
    *,
    distance: str = "COSINE_DISTANCE",
    approximate_neighbors_count: int = 150,
    leaf_node_embedding_count: int = 1000,
    leaf_nodes_to_search_percent: int = 7,
) -> aiplatform.MatchingEngineIndex:
    aiplatform.init(project=project, location=region)
    index = aiplatform.MatchingEngineIndex.create_tree_ah_index(
        display_name=index_name,
        dimensions=dimensions,
        distance_measure_type=distance,
        approximate_neighbors_count=approximate_neighbors_count,
        leaf_node_embedding_count=leaf_node_embedding_count,
        leaf_nodes_to_search_percent=leaf_nodes_to_search_percent,
    )
    index.wait()
    return index


def get_or_create_endpoint(
    project: str,
    region: str,
    endpoint_id: Optional[str] = None,
    endpoint_name: Optional[str] = None,
    *,
    public_endpoint_enabled: bool = True,
) -> aiplatform.MatchingEngineIndexEndpoint:
    aiplatform.init(project=project, location=region)
    if endpoint_id:
        return aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=endpoint_id)
    if endpoint_name:
        return aiplatform.MatchingEngineIndexEndpoint.create(
            display_name=endpoint_name,
            public_endpoint_enabled=public_endpoint_enabled,
        )
    # Default create a new endpoint with a derived name
    return aiplatform.MatchingEngineIndexEndpoint.create(
        display_name=f"{project}-{region}-vs-endpoint",
        public_endpoint_enabled=public_endpoint_enabled,
    )


def deploy_index(
    index: aiplatform.MatchingEngineIndex,
    endpoint: aiplatform.MatchingEngineIndexEndpoint,
    *,
    deployed_index_id: Optional[str] = None,
) -> str:
    # If no id provided, derive a simple one from index display name
    if not deployed_index_id:
        try:
            dn = index.display_name or "vs_index"
        except Exception:
            dn = "vs_index"
        # Sanitize: start with a letter; only letters, numbers, underscores
        import re
        s = re.sub(r"[^A-Za-z0-9_]", "_", dn)
        if not re.match(r"^[A-Za-z]", s):
            s = "d_" + s
        deployed_index_id = s[:63]
    endpoint.deploy_index(index=index, deployed_index_id=deployed_index_id)
    endpoint.wait()
    return deployed_index_id


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--index-name", required=True)
    p.add_argument("--dimensions", type=int, required=True)
    p.add_argument("--endpoint-id", default=None)
    p.add_argument("--endpoint-name", default=None)
    p.add_argument("--public-endpoint-enabled", action="store_true", default=True)
    p.add_argument("--distance", default="COSINE_DISTANCE")
    p.add_argument("--approximate-neighbors-count", type=int, default=150)
    p.add_argument("--leaf-node-embedding-count", type=int, default=1000)
    p.add_argument("--leaf-nodes-to-search-percent", type=int, default=7)
    args = p.parse_args()

    print(f"[vertex] project={args.project} region={args.region} index_name={args.index_name} dims={args.dimensions}")
    index = create_index(
        args.project,
        args.region,
        args.index_name,
        args.dimensions,
        distance=args.distance,
        approximate_neighbors_count=args.approximate_neighbors_count,
        leaf_node_embedding_count=args.leaf_node_embedding_count,
        leaf_nodes_to_search_percent=args.leaf_nodes_to_search_percent,
    )
    print(f"[vertex] Created index: {index.resource_name}")

    endpoint = get_or_create_endpoint(
        args.project,
        args.region,
        args.endpoint_id,
        args.endpoint_name,
        public_endpoint_enabled=args.public_endpoint_enabled,
    )
    print(f"[vertex] Using endpoint: {endpoint.resource_name}")

    deployed_index_id = deploy_index(index, endpoint)
    print("\n=== Vertex AI Vector Search IDs ===")
    print(f"VECTOR_SEARCH_INDEX_ID={index.resource_name}")
    print(f"VECTOR_SEARCH_ENDPOINT_ID={endpoint.resource_name}")
    print(f"VECTOR_SEARCH_DEPLOYED_INDEX_ID={deployed_index_id}")
    print("=================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())


