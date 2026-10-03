#!/usr/bin/env bash
set -euo pipefail

EVENT_NAME="${EVENT_NAME:-}"
REF_NAME="${REF_NAME:-}"
CUSTOM_VERSION="${CUSTOM_VERSION:-}"
IS_TEST_IMAGE="${IS_TEST_IMAGE:-false}"

SHORT_SHA=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "0000000")

# Determine test run status
IS_TEST="false"
if [ "${EVENT_NAME}" = "workflow_dispatch" ] && [ "${IS_TEST_IMAGE}" = "true" ]; then
  IS_TEST="true"
elif [ -n "${REF_NAME}" ] && [ "${REF_NAME}" != "main" ]; then
  IS_TEST="true"
fi

# Resolve SemVer version
if [ -n "${CUSTOM_VERSION}" ]; then
  VERSION="${CUSTOM_VERSION}"
else
  LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.1.0")
  VERSION="${LATEST_TAG}"
  if [ "${IS_TEST}" = "true" ]; then
    VERSION="${LATEST_TAG}-test.${SHORT_SHA}"
  fi
fi

# Format ECR tags list
CLEAN_REF=$(echo "${REF_NAME:-unknown}" | tr '/' '-' | tr '_' '-')
if [ "${IS_TEST}" = "true" ]; then
  ECR_TAGS="test-${CLEAN_REF}-${SHORT_SHA},test-latest"
else
  ECR_TAGS="latest,${VERSION},sha-${SHORT_SHA}"
fi

echo "=== Computed Metadata ==="
echo "Resolved Version: ${VERSION}"
echo "Resolved ECR Tags: ${ECR_TAGS}"
echo "Is Test Run: ${IS_TEST}"
echo "Short SHA: ${SHORT_SHA}"

# Emit outputs if GITHUB_OUTPUT is set
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "short_sha=${SHORT_SHA}"
    echo "is_test=${IS_TEST}"
    echo "version=${VERSION}"
    echo "ecr_tags=${ECR_TAGS}"
  } >> "$GITHUB_OUTPUT"
fi
