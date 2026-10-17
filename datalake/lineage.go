package datalake

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// ComputeFileSHA256 returns the hex-encoded SHA-256 hash of a file for tamper-evident provenance.
func ComputeFileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file for sha256 computation: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, copyErr := io.Copy(hasher, file); copyErr != nil {
		return "", fmt.Errorf("failed to compute sha256 checksum: %w", copyErr)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
