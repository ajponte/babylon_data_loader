#!/usr/bin/env bash
set -euo pipefail

GOOS="${GOOS:?GOOS is required}"
GOARCH="${GOARCH:?GOARCH is required}"
BINARY_NAME="${BINARY_NAME:-data-loader}"
VERSION="${VERSION:?VERSION is required}"
COMMIT_SHA="${COMMIT_SHA:-$(git rev-parse HEAD 2>/dev/null || echo "unknown")}"

mkdir -p dist
OUTPUT_PATH="dist/${BINARY_NAME}"

echo "==> Compiling ${BINARY_NAME} for ${GOOS}/${GOARCH} (version=${VERSION}, commit=${COMMIT_SHA})..."
CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" go build \
  -trimpath \
  -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT_SHA}" \
  -o "${OUTPUT_PATH}" \
  main.go
chmod +x "${OUTPUT_PATH}"

ARCHIVE_NAME="babylon-${BINARY_NAME}_${VERSION}_${GOOS}_${GOARCH}.tar.gz"
echo "==> Packaging into ${ARCHIVE_NAME}..."
tar -czvf "${ARCHIVE_NAME}" -C dist "${BINARY_NAME}"

echo "==> Computing SHA256 checksum..."
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
else
  shasum -a 256 "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
fi

cat "${ARCHIVE_NAME}.sha256"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "archive_name=${ARCHIVE_NAME}" >> "$GITHUB_OUTPUT"
fi
