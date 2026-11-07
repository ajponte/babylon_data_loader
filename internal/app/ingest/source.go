package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"babylon/dataloader/internal/domain/model"
)

const (
	// SourceChase identifies Chase bank and credit card statement sources.
	SourceChase = "chase"
	// SourceFidelity identifies Fidelity brokerage statement sources.
	SourceFidelity = "fidelity"
	// SourceSynthetic identifies synthetic transaction sources.
	SourceSynthetic = "synthetic"

	// DefaultMaxInputBytes is the default maximum size (32 MiB) allowed for input streams.
	DefaultMaxInputBytes int64 = 32 << 20
)

var (
	// ErrUnresolvableSource indicates that the source type could not be determined from the file name.
	ErrUnresolvableSource = errors.New("ingest: cannot resolve source from file name")
	// ErrInputTooLarge indicates that the input stream exceeds the maximum allowed size.
	ErrInputTooLarge = errors.New("ingest: input exceeds maximum size")
	// ErrNilReader indicates that the provided reader is nil.
	ErrNilReader = errors.New("ingest: reader cannot be nil")
)

var chaseAccountRegex = regexp.MustCompile(`chase(\d{4})`)

// SourceRef identifies the resolved data source and account.
type SourceRef struct {
	SourceType string
	AccountID  string
}

// ResolveSource determines the source type and account ID from a file or object name.
func ResolveSource(name string) (SourceRef, error) {
	cleanPath := sanitizePath(name)
	baseName := cleanBaseName(cleanPath)
	if baseName == "" {
		return SourceRef{}, ErrUnresolvableSource
	}

	lowerBase := strings.ToLower(baseName)
	if ref, ok := matchChase(lowerBase); ok {
		return ref, nil
	}

	lowerPath := strings.ToLower(cleanPath)
	if ref, ok := matchFidelity(lowerBase, lowerPath); ok {
		return ref, nil
	}

	if ref, ok := matchSynthetic(lowerBase); ok {
		return ref, nil
	}

	return SourceRef{}, ErrUnresolvableSource
}

func matchChase(lowerBase string) (SourceRef, bool) {
	if !strings.Contains(lowerBase, "chase") {
		return SourceRef{}, false
	}
	matches := chaseAccountRegex.FindStringSubmatch(lowerBase)
	const minSubmatches = 2
	if len(matches) < minSubmatches {
		return SourceRef{}, false
	}
	return SourceRef{
		SourceType: SourceChase,
		AccountID:  matches[1],
	}, true
}

func matchFidelity(lowerBase, lowerPath string) (SourceRef, bool) {
	isFidelityPath := strings.Contains(lowerPath, "fidelity")
	isPositionsExport := strings.HasPrefix(lowerBase, "portfolio_positions") ||
		strings.HasPrefix(lowerBase, "portfolio positions")
	if !isFidelityPath && !isPositionsExport {
		return SourceRef{}, false
	}
	return SourceRef{
		SourceType: SourceFidelity,
		AccountID:  "portfolio",
	}, true
}

func matchSynthetic(lowerBase string) (SourceRef, bool) {
	if !strings.Contains(lowerBase, "synthetic") {
		return SourceRef{}, false
	}
	return SourceRef{
		SourceType: SourceSynthetic,
		AccountID:  "0000",
	}, true
}

// Fingerprint reads up to maxBytes from r, computes its SHA-256 digest, and returns a re-readable body.
func Fingerprint(r io.Reader, maxBytes int64) (*bytes.Reader, string, error) {
	if r == nil {
		return nil, "", ErrNilReader
	}

	limit := resolveMaxBytes(maxBytes)
	buf, err := readBoundedStream(r, limit)
	if err != nil {
		return nil, "", err
	}

	return bytes.NewReader(buf), computeSHA256Hex(buf), nil
}

func resolveMaxBytes(maxBytes int64) int64 {
	if maxBytes <= 0 {
		return DefaultMaxInputBytes
	}
	return maxBytes
}

func readBoundedStream(r io.Reader, maxBytes int64) ([]byte, error) {
	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}

	buf, err := io.ReadAll(io.LimitReader(r, readLimit))
	if err != nil {
		return nil, fmt.Errorf("ingest: read stream: %w", err)
	}
	if int64(len(buf)) > maxBytes {
		return nil, ErrInputTooLarge
	}

	// Clip excess capacity to prevent memory retention across long-lived readers.
	return buf[:len(buf):len(buf)], nil
}

func computeSHA256Hex(buf []byte) string {
	digest := sha256.Sum256(buf)
	return hex.EncodeToString(digest[:])
}

// NewFileMetadata constructs a FileMetadata model with a UTC timestamp and clean base name.
// Invariant verification must be performed by invoking meta.Validate().
func NewFileMetadata(name string, ref SourceRef, sha256Hex, sourceURI string, ingestedAt time.Time) model.FileMetadata {
	return model.FileMetadata{
		FileName:   extractBaseName(name),
		FileHash:   sha256Hex,
		AccountID:  ref.AccountID,
		IngestedAt: ingestedAt.UTC(),
		SourceURI:  sourceURI,
	}
}

// extractBaseName sanitizes an input path and extracts its terminal base name.
// Returns an empty string if the path represents a directory or relative traversal.
func extractBaseName(rawPath string) string {
	return cleanBaseName(sanitizePath(rawPath))
}

// sanitizePath normalizes path delimiters, strips queries, and decodes URL percent-encoding.
func sanitizePath(raw string) string {
	// Step 1: Strip query parameters and URL fragments.
	if idx := strings.IndexAny(raw, "?#"); idx != -1 {
		raw = raw[:idx]
	}
	// Step 2: Normalize backslashes to standard forward slashes.
	raw = strings.ReplaceAll(raw, "\\", "/")
	// Step 3: Replace '+' with space to support standard form-url encoding.
	raw = strings.ReplaceAll(raw, "+", " ")
	// Step 4: Decode URL percent-encoded characters (%20, etc.).
	if unescaped, err := url.PathUnescape(raw); err == nil {
		raw = unescaped
	}
	return raw
}

func cleanBaseName(cleanPath string) string {
	if isDirectoryOrTraversal(cleanPath) {
		return ""
	}
	return path.Base(cleanPath)
}

// isDirectoryOrTraversal detects directory markers, trailing slashes, or relative traversals.
func isDirectoryOrTraversal(p string) bool {
	if p == "" || p == "." || p == ".." {
		return true
	}
	if strings.HasSuffix(p, "/") || strings.HasSuffix(p, "/..") || strings.HasSuffix(p, "/.") {
		return true
	}
	return false
}
