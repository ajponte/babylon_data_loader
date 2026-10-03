#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:?REGISTRY is required}"
ECR_REPOSITORY="${ECR_REPOSITORY:?ECR_REPOSITORY is required}"
RAW_TAGS="${RAW_TAGS:?RAW_TAGS is required}"

FORMATTED_TAGS=""
IFS=',' read -ra TAG_ARRAY <<< "$RAW_TAGS"
for tag in "${TAG_ARRAY[@]}"; do
  TRIMMED_TAG=$(echo "$tag" | xargs)
  if [ -n "$TRIMMED_TAG" ]; then
    FULL_IMAGE="${REGISTRY}/${ECR_REPOSITORY}:${TRIMMED_TAG}"
    if [ -z "$FORMATTED_TAGS" ]; then
      FORMATTED_TAGS="${FULL_IMAGE}"
    else
      FORMATTED_TAGS="${FORMATTED_TAGS},${FULL_IMAGE}"
    fi
  fi
done

echo "Pushed Image Tags:"
echo "${FORMATTED_TAGS}" | tr ',' '\n'

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "tags=${FORMATTED_TAGS}" >> "$GITHUB_OUTPUT"
fi
