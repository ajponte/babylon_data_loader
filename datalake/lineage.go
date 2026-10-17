package datalake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

const hashBufferSize = 32 * 1024

// ComputeFileSHA256 returns the hex-encoded SHA-256 hash of a file for tamper-evident provenance.
func ComputeFileSHA256(ctx context.Context, filePath string) (string, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", ctxErr
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file for sha256 computation: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	buf := make([]byte, hashBufferSize)

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}

		n, readErr := file.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return "", fmt.Errorf("failed to read file during sha256 computation: %w", readErr)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
