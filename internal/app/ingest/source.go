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
	"path/filepath"
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
	clean := sanitizePath(name)
	baseName := cleanBaseName(clean)
	if baseName == "" {
		return SourceRef{}, ErrUnresolvableSource
	}

	lowerBase := strings.ToLower(baseName)
	if ref, ok := matchChase(lowerBase); ok {
		return ref, nil
	}

	lowerPath := strings.ToLower(clean)
	if ref, ok := matchFidelity(lowerBase, lowerPath); ok {
		return ref, nil
	}

	if ref, ok := matchSynthetic(lowerBase); ok {
		return ref, nil
	}

	return SourceRef{}, ErrUnresolvableSource
}

func matchChase(lowerBase string) (SourceRef, bool) {
	if strings.Contains(lowerBase, "chase") {
		matches := chaseAccountRegex.FindStringSubmatch(lowerBase)
		if len(matches) > 1 {
			return SourceRef{
				SourceType: SourceChase,
				AccountID:  matches[1],
			}, true
		}
	}
	return SourceRef{}, false
}

func matchFidelity(lowerBase, lowerPath string) (SourceRef, bool) {
	if strings.Contains(lowerPath, "fidelity") ||
		strings.HasPrefix(lowerBase, "portfolio_positions") ||
		strings.HasPrefix(lowerBase, "portfolio positions") {
		return SourceRef{
			SourceType: SourceFidelity,
			AccountID:  "portfolio",
		}, true
	}
	return SourceRef{}, false
}

func matchSynthetic(lowerBase string) (SourceRef, bool) {
	if strings.Contains(lowerBase, "synthetic") {
		return SourceRef{
			SourceType: SourceSynthetic,
			AccountID:  "0000",
		}, true
	}
	return SourceRef{}, false
}

// Fingerprint reads up to maxBytes from r, computes its SHA-256 digest, and returns a re-readable body.
func Fingerprint(r io.Reader, maxBytes int64) (*bytes.Reader, string, error) {
	if r == nil {
		return nil, "", ErrNilReader
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInputBytes
	}

	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}

	buf, err := io.ReadAll(io.LimitReader(r, readLimit))
	if err != nil {
		return nil, "", fmt.Errorf("ingest: read stream: %w", err)
	}
	if int64(len(buf)) > maxBytes {
		return nil, "", ErrInputTooLarge
	}

	buf = buf[:len(buf):len(buf)]
	digest := sha256.Sum256(buf)
	return bytes.NewReader(buf), hex.EncodeToString(digest[:]), nil
}

// NewFileMetadata constructs a FileMetadata model with a UTC timestamp and clean base name.
// Invariant verification must be performed by invoking meta.Validate().
func NewFileMetadata(name string, ref SourceRef, sha256Hex, sourceURI string, ingestedAt time.Time) model.FileMetadata {
	return model.FileMetadata{
		FileName:   cleanBaseName(sanitizePath(name)),
		FileHash:   sha256Hex,
		AccountID:  ref.AccountID,
		IngestedAt: ingestedAt.UTC(),
		SourceURI:  sourceURI,
	}
}

func sanitizePath(raw string) string {
	if idx := strings.IndexAny(raw, "?#"); idx != -1 {
		raw = raw[:idx]
	}
	raw = strings.ReplaceAll(filepath.ToSlash(raw), "\\", "/")
	raw = strings.ReplaceAll(raw, "+", " ")
	if unescaped, err := url.PathUnescape(raw); err == nil {
		raw = unescaped
	}
	return raw
}

func cleanBaseName(cleanPath string) string {
	if cleanPath == "" ||
		strings.HasSuffix(cleanPath, "/") ||
		strings.HasSuffix(cleanPath, "/.") ||
		strings.HasSuffix(cleanPath, "/..") {
		return ""
	}
	base := path.Base(cleanPath)
	if base == "." || base == ".." || base == "/" {
		return ""
	}
	return base
}
