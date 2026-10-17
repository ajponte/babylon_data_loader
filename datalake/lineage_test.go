package datalake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestComputeFileSHA256_KnownContent(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_file.txt")
	content := []byte("The quick brown fox jumps over the lazy dog")

	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHashBytes := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(expectedHashBytes[:])

	actualHash, err := ComputeFileSHA256(ctx, filePath)
	if err != nil {
		t.Fatalf("ComputeFileSHA256 failed: %v", err)
	}

	if actualHash != expectedHash {
		t.Errorf("expected hash %s, got %s", expectedHash, actualHash)
	}
}

func TestComputeFileSHA256_EmptyFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "empty.txt")

	if err := os.WriteFile(filePath, []byte{}, 0o644); err != nil {
		t.Fatalf("failed to write empty file: %v", err)
	}

	const expectedEmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	actualHash, err := ComputeFileSHA256(ctx, filePath)
	if err != nil {
		t.Fatalf("ComputeFileSHA256 failed for empty file: %v", err)
	}

	if actualHash != expectedEmptySHA256 {
		t.Errorf("expected empty hash %s, got %s", expectedEmptySHA256, actualHash)
	}
}

func TestComputeFileSHA256_NonExistentFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "non_existent.txt")

	_, err := ComputeFileSHA256(ctx, filePath)
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestComputeFileSHA256_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_file.txt")
	if err := os.WriteFile(filePath, []byte("data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := ComputeFileSHA256(ctx, filePath)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
}
