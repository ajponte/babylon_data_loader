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
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
if ! git rev-parse "${VERSION}" >/dev/null 2>&1; then
  echo "Creating tag ${VERSION}..."
  git tag -a "${VERSION}" -m "Release ${VERSION}"
  git push origin "${VERSION}"
else
  echo "Tag ${VERSION} already exists."
fi
