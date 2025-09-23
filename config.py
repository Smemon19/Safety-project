"""Centralized configuration loader.

Loads and validates all environment variables related to Google Vertex AI Vector Search
and application-level isolation (namespace). Other modules should import from here
instead of reading os.environ directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple

try:
    from dotenv import load_dotenv
except Exception:  # tolerate missing python-dotenv
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

try:
    # Prefer the app's persisted env (created by utils.ensure_appdata_scaffold)
    from utils import get_env_file_path
    _ENV_PATH = get_env_file_path()
except Exception:
    _ENV_PATH = None


def _load_env_once() -> None:
    """Idempotently load .env so downstream imports see variables."""
    # Load from persisted appdata path first when available
    if _ENV_PATH:
        load_dotenv(dotenv_path=_ENV_PATH, override=False)
    # Then load from repo-local .env if present (does not override existing)
    load_dotenv(override=False)


_load_env_once()


@dataclass(frozen=True)
class VertexConfig:
    gcp_project_id: str
    gcp_region: str
    index_id: str
    endpoint_id: str
    namespace: str
    # Optional: deployed index id (helpful for find_neighbors)
    deployed_index_id: Optional[str] = None


REQUIRED_KEYS = [
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "VECTOR_SEARCH_INDEX_ID",
    "VECTOR_SEARCH_ENDPOINT_ID",
    "NAMESPACE",
]


def _get(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(key, default if default is not None else None)
    if val is None:
        return None
    s = str(val).strip()
    return s if s else (default if default is not None else "")


def load_vertex_config(raise_on_missing: bool = True) -> VertexConfig:
    """Load and optionally validate core Vertex config from the environment."""
    missing = [k for k in REQUIRED_KEYS if not _get(k)]
    if missing and raise_on_missing:
        keys = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {keys}.\n"
            f"Please update your .env with these keys. See .env.example for reference."
        )
    cfg = VertexConfig(
        gcp_project_id=_get("GCP_PROJECT_ID", ""),
        gcp_region=_get("GCP_REGION", ""),
        index_id=_get("VECTOR_SEARCH_INDEX_ID", ""),
        endpoint_id=_get("VECTOR_SEARCH_ENDPOINT_ID", ""),
        namespace=_get("NAMESPACE", ""),
        deployed_index_id=_get("VECTOR_SEARCH_DEPLOYED_INDEX_ID", None),
    )
    return cfg


def log_active_config(prefix: str = "[config]", allow_missing: bool = True) -> None:
    """Print a concise summary of active Vertex config values at startup."""
    try:
        cfg = load_vertex_config(raise_on_missing=not allow_missing)
    except Exception as e:
        print(f"{prefix} {e}")
        return
    # Mask nothing here; these are resource identifiers, not secrets
    print(
        f"{prefix} project='{cfg.gcp_project_id}' region='{cfg.gcp_region}' index_id='{cfg.index_id}' "
        f"endpoint_id='{cfg.endpoint_id}' namespace='{cfg.namespace}' deployed_index_id='{cfg.deployed_index_id or ''}'"
    )


def get_namespace() -> str:
    """Return the configured namespace (empty string if unset)."""
    return _get("NAMESPACE", "") or ""


def embedding_dimensions_from_env() -> int:
    """Best-effort inference of embedding vector dimension from environment.

    Falls back to 384 if unknown. This is used by ops/ scripts to create indexes with
    the correct dimensionality.
    """
    # Explicit override
    explicit = _get("EMBEDDING_DIMENSIONS", None)
    if explicit:
        try:
            dim = int(explicit)
            if dim > 0:
                return dim
        except Exception:
            pass

    backend = (_get("EMBEDDING_BACKEND", "sentence") or "sentence").lower()
    if backend == "openai":
        model = _get("OPENAI_EMBED_MODEL", "text-embedding-3-large").lower()
        if "text-embedding-3-large" in model:
            return 3072
        if "text-embedding-3-small" in model:
            return 1536
        # Reasonable default for OpenAI unknowns
        return 1536

    # sentence-transformers defaults
    model = _get("SENTENCE_MODEL", "all-MiniLM-L6-v2").lower()
    # Common mappings
    if "all-minilm-l6-v2" in model:
        return 384
    if "all-minilm-l12-v2" in model:
        return 384
    if "mpnet-base" in model or "all-mpnet-base" in model:
        return 768
    # Default safe choice
    return 384


def resolve_project_location() -> Tuple[str, str]:
    """Return (project_id, region) even when validation is not enforced."""
    return (
        _get("GCP_PROJECT_ID", ""),
        _get("GCP_REGION", ""),
    )


