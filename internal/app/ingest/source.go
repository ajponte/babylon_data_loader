package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	baseName := cleanBaseName(name)
	if baseName == "" {
		return SourceRef{}, ErrUnresolvableSource
	}

	lowerBase := strings.ToLower(baseName)
	if strings.Contains(lowerBase, "chase") {
		matches := chaseAccountRegex.FindStringSubmatch(lowerBase)
		if len(matches) > 1 {
			return SourceRef{
				SourceType: SourceChase,
				AccountID:  matches[1],
			}, nil
		}
	}

	normalizedPath := normalizePath(name)
	lowerPath := strings.ToLower(normalizedPath)
	if strings.Contains(lowerPath, "fidelity") || strings.HasPrefix(lowerBase, "portfolio_positions") {
		return SourceRef{
			SourceType: SourceFidelity,
			AccountID:  "portfolio",
		}, nil
	}

	if strings.Contains(lowerBase, "synthetic") {
		return SourceRef{
			SourceType: SourceSynthetic,
			AccountID:  "0000",
		}, nil
	}

	return SourceRef{}, ErrUnresolvableSource
}

// Fingerprint reads up to maxBytes from r, computes its SHA-256 digest, and returns a re-readable body.
func Fingerprint(r io.Reader, maxBytes int64) (*bytes.Reader, string, error) {
	if r == nil {
		return nil, "", ErrNilReader
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInputBytes
	}

	buf, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("ingest: read stream: %w", err)
	}
	if int64(len(buf)) > maxBytes {
		return nil, "", ErrInputTooLarge
	}

	digest := sha256.Sum256(buf)
	return bytes.NewReader(buf), hex.EncodeToString(digest[:]), nil
}

// NewFileMetadata constructs a validated FileMetadata model with a UTC timestamp and clean base name.
func NewFileMetadata(name string, ref SourceRef, sha256Hex, sourceURI string, ingestedAt time.Time) model.FileMetadata {
	return model.FileMetadata{
		FileName:   cleanBaseName(name),
		FileHash:   sha256Hex,
		AccountID:  ref.AccountID,
		IngestedAt: ingestedAt.UTC(),
		SourceURI:  sourceURI,
	}
}

func normalizePath(name string) string {
	return strings.ReplaceAll(filepath.ToSlash(name), "\\", "/")
}

func cleanBaseName(name string) string {
	if name == "" {
		return ""
	}
	base := path.Base(normalizePath(name))
	if base == "." || base == "/" {
		return ""
	}
	return base
}
