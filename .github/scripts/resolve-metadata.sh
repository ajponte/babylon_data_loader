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
  # Check if HEAD is directly tagged
  EXACT_TAG=$(git describe --tags --exact-match 2>/dev/null || true)
  if [ -n "${EXACT_TAG}" ]; then
    VERSION="${EXACT_TAG}"
    if [ "${IS_TEST}" = "true" ]; then
      VERSION="${EXACT_TAG}-test.${SHORT_SHA}"
    fi
  else
    LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || true)
    if [ -z "${LATEST_TAG}" ]; then
      VERSION="v0.1.0"
      if [ "${IS_TEST}" = "true" ]; then
        VERSION="v0.1.0-test.${SHORT_SHA}"
      fi
    elif [ "${IS_TEST}" = "true" ]; then
      VERSION="${LATEST_TAG}-test.${SHORT_SHA}"
    else
      # When on main and HEAD is not an exact tag match, auto-increment patch version
      if [[ "${LATEST_TAG}" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        MAJOR="${BASH_REMATCH[1]}"
        MINOR="${BASH_REMATCH[2]}"
        PATCH="${BASH_REMATCH[3]}"
        NEXT_PATCH=$((PATCH + 1))
        VERSION="v${MAJOR}.${MINOR}.${NEXT_PATCH}"
      else
        VERSION="${LATEST_TAG}"
      fi
    fi
  fi
fi

# Format ECR tags list for shared ajp/babylon repository
CLEAN_REF=$(echo "${REF_NAME:-unknown}" | tr '/' '-' | tr '_' '-')
if [ "${IS_TEST}" = "true" ]; then
  ECR_TAGS="data-loader-test-${CLEAN_REF}-${SHORT_SHA},data-loader-test-latest"
else
  ECR_TAGS="data-loader-latest,data-loader-${VERSION},data-loader-sha-${SHORT_SHA}"
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
