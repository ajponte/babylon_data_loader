#!/usr/bin/env bash
set -euo pipefail

BINARY_NAME="${BINARY_NAME:-data-loader}"
VERSION="${VERSION:?VERSION is required}"
WORKING_DIR="${WORKING_DIR:-release-artifacts}"

echo "==> Aggregating checksums in ${WORKING_DIR}..."
cd "${WORKING_DIR}"
rm -f *.sha256

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum babylon-${BINARY_NAME}_*.tar.gz > checksums.txt
else
  shasum -a 256 babylon-${BINARY_NAME}_*.tar.gz > checksums.txt
fi

echo "=== Consolidated SHA256 Checksums ==="
cat checksums.txt
cd - >/dev/null

echo "==> Ensuring Git tag ${VERSION} exists..."
git fetch --tags origin 2>/dev/null || true

if git ls-remote --tags origin "refs/tags/${VERSION}" | grep -q "${VERSION}"; then
  echo "Tag ${VERSION} already exists on remote origin. Skipping tag creation."
else
  echo "Tag ${VERSION} not found on remote origin. Creating tag ${VERSION}..."
  git config user.name "github-actions[bot]"
  git config user.email "github-actions[bot]@users.noreply.github.com"
  git tag -a "${VERSION}" -m "Release ${VERSION}"
  git push origin "${VERSION}" || echo "Warning: failed to push tag ${VERSION}; may have been pushed concurrently."
fi
