#!/usr/bin/env bash
set -euo pipefail

# Create and deploy a new Vertex AI Vector Search index for this project.
#
# Usage:
#   ops/vertex/create_index.sh \
#     --project my-gcp-project \
#     --region us-central1 \
#     --index-name cal-vs-project-b \
#     [--dimensions 384] \
#     [--endpoint-id EXISTING_ENDPOINT_ID | --endpoint-name cal-vs-endpoint-b]
#
# Prints: Index ID, Endpoint ID, and Deployed Index ID.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PROJECT=""
REGION="us-central1"
INDEX_NAME=""
DIMENSIONS=""
ENDPOINT_ID=""
ENDPOINT_NAME=""

function usage() {
  echo "Usage: $0 --project <id> --region <region> --index-name <name> [--dimensions <n>] [--endpoint-id <id> | --endpoint-name <name>]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT="$2"; shift 2;;
    --region)
      REGION="$2"; shift 2;;
    --index-name)
      INDEX_NAME="$2"; shift 2;;
    --dimensions)
      DIMENSIONS="$2"; shift 2;;
    --endpoint-id)
      ENDPOINT_ID="$2"; shift 2;;
    --endpoint-name)
      ENDPOINT_NAME="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown flag: $1" >&2; usage; exit 1;;
  esac
done

if [[ -z "$PROJECT" || -z "$INDEX_NAME" ]]; then
  echo "--project and --index-name are required" >&2
  usage
  exit 1
fi

# Resolve default dimensions from config if not provided
if [[ -z "$DIMENSIONS" ]]; then
  DIMENSIONS=$(python - <<'PY'
from config import embedding_dimensions_from_env
print(embedding_dimensions_from_env())
PY
)
fi

echo "[create-index] Project=$PROJECT Region=$REGION IndexName=$INDEX_NAME Dimensions=$DIMENSIONS"

PYTHON_BIN=${PYTHON_BIN:-python}

CMD=("$PYTHON_BIN" "$REPO_ROOT/ops/vertex/create_index_py.py" \
  --project "$PROJECT" \
  --region "$REGION" \
  --index-name "$INDEX_NAME" \
  --dimensions "$DIMENSIONS")

if [[ -n "$ENDPOINT_ID" ]]; then
  CMD+=(--endpoint-id "$ENDPOINT_ID")
fi
if [[ -n "$ENDPOINT_NAME" ]]; then
  CMD+=(--endpoint-name "$ENDPOINT_NAME")
fi

"${CMD[@]}"


