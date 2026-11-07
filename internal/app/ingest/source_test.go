package ingest_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"babylon/dataloader/internal/app/ingest"
	"babylon/dataloader/internal/domain/model"
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestResolveSource_LegacyParity(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedRef ingest.SourceRef
	}{
		{name: "synthetic file csv", input: "synthetic_file.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceSynthetic, AccountID: "0000"}},
		{name: "synthetic pdf statement", input: "my_synthetic_statement.pdf", expectedRef: ingest.SourceRef{SourceType: ingest.SourceSynthetic, AccountID: "0000"}},
		{name: "synthetic uppercase xls", input: "SYNTHETIC_transactions.xls", expectedRef: ingest.SourceRef{SourceType: ingest.SourceSynthetic, AccountID: "0000"}},
		{name: "synthetic txt file", input: "another_file_synthetic.txt", expectedRef: ingest.SourceRef{SourceType: ingest.SourceSynthetic, AccountID: "0000"}},
		{name: "fidelity portfolio positions date", input: "Portfolio_Positions_Sep-25-2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity positions year", input: "fidelity_positions_2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity uppercase export", input: "PORTFOLIO_POSITIONS_export.CSV", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity portfolio positions synthetic", input: "Portfolio_Positions_Synthetic.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity unprocessed prefix", input: "unprocessed/Portfolio_Positions_Sep-25-2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity absolute unix path", input: "/tmp/uploads/portfolio_positions.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "chase checking statement", input: "unprocessed/chase9876_statement.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "9876"}},
		{name: "synthetic unix path", input: "/var/data/synthetic_records.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceSynthetic, AccountID: "0000"}},
		{name: "chase credit card activity", input: "chase5432_activity.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "5432"}},
		{name: "chase uppercase with s3 prefix", input: "s3://my-bucket/unprocessed/CHASE9876_STATEMENT.CSV", expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "9876"}},
		{name: "chase windows backslash path", input: `unprocessed\chase9876_statement.csv`, expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "9876"}},
		{name: "fidelity windows backslash path", input: `data\unprocessed\portfolio_positions.csv`, expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity s3 prefix", input: "s3://datalake-bucket/unprocessed/fidelity_positions_2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity percent encoded space", input: "Portfolio%20Positions_Sep-25-2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "fidelity plus encoded space", input: "Portfolio+Positions_Sep-25-2026.csv", expectedRef: ingest.SourceRef{SourceType: ingest.SourceFidelity, AccountID: "portfolio"}},
		{name: "chase query parameter sanitized", input: "chase9876_statement.csv?versionId=3", expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "9876"}},
		{name: "chase fragment sanitized", input: "s3://bucket/unprocessed/chase9876_statement.csv#frag", expectedRef: ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "9876"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := ingest.ResolveSource(tc.input)
			if err != nil {
				t.Fatalf("unexpected error resolving source: %v", err)
			}
			if ref != tc.expectedRef {
				t.Errorf("ResolveSource(%q) = %+v, want %+v", tc.input, ref, tc.expectedRef)
			}
		})
	}
}

func TestResolveSource_Unresolvable(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "unknown generic csv", input: "somefile.csv"},
		{name: "unknown bank statement pdf", input: "bank_statement.pdf"},
		{name: "unknown text file", input: "another_file.txt"},
		{name: "empty string", input: ""},
		{name: "slash only", input: "/"},
		{name: "dot only", input: "."},
		{name: "dot dot only", input: ".."},
		{name: "folder traversal dot dot", input: "folder/.."},
		{name: "folder dot", input: "folder/."},
		{name: "dot slash", input: "./"},
		{name: "dot dot slash", input: "../"},
		{name: "s3 chase folder marker trailing slash", input: "unprocessed/chase1234/"},
		{name: "s3 fidelity folder marker trailing slash", input: "s3://bucket/unprocessed/fidelity/"},
		{name: "chase missing digits", input: "chase_statement.csv"},
		{name: "chase three digits", input: "chase123_statement.csv"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := ingest.ResolveSource(tc.input)
			if !errors.Is(err, ingest.ErrUnresolvableSource) {
				t.Errorf("ResolveSource(%q) error = %v, want %v", tc.input, err, ingest.ErrUnresolvableSource)
			}
			if ref != (ingest.SourceRef{}) {
				t.Errorf("ResolveSource(%q) ref = %+v, want empty ref", tc.input, ref)
			}
		})
	}
}

func TestResolveSource_PrecedenceChaseOverSynthetic(t *testing.T) {
	ref, err := ingest.ResolveSource("chase1234_synthetic.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "1234"}
	if ref != expected {
		t.Errorf("ResolveSource(\"chase1234_synthetic.csv\") = %+v, want %+v", ref, expected)
	}
}

func TestFingerprint_HashMatchesSHA256(t *testing.T) {
	content := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\nDEBIT,09/15/2026,Store,-42.50,Sale,1200.00,\n"
	expectedSum := sha256.Sum256([]byte(content))
	expectedHex := hex.EncodeToString(expectedSum[:])

	reader := strings.NewReader(content)
	body, hash, err := ingest.Fingerprint(reader, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hash != expectedHex {
		t.Errorf("hash = %q, want %q", hash, expectedHex)
	}
	if body == nil {
		t.Fatal("expected non-nil reader body")
	}

	readBack, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("failed to re-read body: %v", err)
	}
	if string(readBack) != content {
		t.Errorf("re-read body = %q, want %q", string(readBack), content)
	}
}

func TestFingerprint_ExactBoundary(t *testing.T) {
	const maxBytes = int64(32)
	exactPayload := strings.Repeat("B", int(maxBytes))
	expectedSum := sha256.Sum256([]byte(exactPayload))
	expectedHex := hex.EncodeToString(expectedSum[:])

	body, hash, err := ingest.Fingerprint(strings.NewReader(exactPayload), maxBytes)
	if err != nil {
		t.Fatalf("unexpected error at exact boundary: %v", err)
	}
	if body == nil {
		t.Fatal("expected non-nil reader at exact boundary")
	}
	if hash != expectedHex {
		t.Errorf("hash = %q, want %q", hash, expectedHex)
	}

	oversizePayload := strings.Repeat("B", int(maxBytes)+1)
	overBody, overHash, overErr := ingest.Fingerprint(strings.NewReader(oversizePayload), maxBytes)
	if !errors.Is(overErr, ingest.ErrInputTooLarge) {
		t.Errorf("error = %v, want %v", overErr, ingest.ErrInputTooLarge)
	}
	if overBody != nil {
		t.Errorf("overBody = %v, want nil", overBody)
	}
	if overHash != "" {
		t.Errorf("overHash = %q, want empty string", overHash)
	}
}

func TestFingerprint_EmptyStream(t *testing.T) {
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	body, hash, err := ingest.Fingerprint(strings.NewReader(""), 1024)
	if err != nil {
		t.Fatalf("unexpected error for empty stream: %v", err)
	}
	if hash != emptySHA256 {
		t.Errorf("hash = %q, want %q", hash, emptySHA256)
	}
	if body == nil {
		t.Fatal("expected non-nil reader body for empty stream")
	}
	if body.Len() != 0 {
		t.Errorf("body.Len() = %d, want 0", body.Len())
	}
}

func TestFingerprint_FallbackLimit(t *testing.T) {
	testLimits := []int64{0, -1, -100}

	for _, limit := range testLimits {
		body, hash, err := ingest.Fingerprint(strings.NewReader("valid content"), limit)
		if err != nil {
			t.Fatalf("unexpected error for limit %d: %v", limit, err)
		}
		if body == nil || hash == "" {
			t.Fatalf("expected non-nil body and non-empty hash for limit %d", limit)
		}
	}

	oversizeReader := io.LimitReader(zeroReader{}, ingest.DefaultMaxInputBytes+1)
	body, hash, err := ingest.Fingerprint(oversizeReader, 0)
	if !errors.Is(err, ingest.ErrInputTooLarge) {
		t.Errorf("expected ErrInputTooLarge for fallback limit overflow, got: %v", err)
	}
	if body != nil || hash != "" {
		t.Errorf("expected nil body and empty hash, got body=%v, hash=%q", body, hash)
	}
}

func TestFingerprint_RejectsOversize(t *testing.T) {
	maxBytes := int64(10)
	payload := strings.Repeat("A", int(maxBytes)+1)
	body, hash, err := ingest.Fingerprint(strings.NewReader(payload), maxBytes)

	if !errors.Is(err, ingest.ErrInputTooLarge) {
		t.Errorf("error = %v, want %v", err, ingest.ErrInputTooLarge)
	}
	if body != nil {
		t.Errorf("body = %v, want nil", body)
	}
	if hash != "" {
		t.Errorf("hash = %q, want empty string", hash)
	}
}

func TestFingerprint_ReaderErrorPropagates(t *testing.T) {
	expectedErr := errors.New("underlying stream read failure")
	body, hash, err := ingest.Fingerprint(iotest.ErrReader(expectedErr), 1024)

	if !errors.Is(err, expectedErr) {
		t.Errorf("error = %v, want wrapped %v", err, expectedErr)
	}
	if body != nil {
		t.Errorf("body = %v, want nil", body)
	}
	if hash != "" {
		t.Errorf("hash = %q, want empty string", hash)
	}
}

func TestFingerprint_NilReader(t *testing.T) {
	body, hash, err := ingest.Fingerprint(nil, 1024)
	if !errors.Is(err, ingest.ErrNilReader) {
		t.Errorf("error = %v, want %v", err, ingest.ErrNilReader)
	}
	if body != nil || hash != "" {
		t.Errorf("expected nil body and empty hash, got body=%v, hash=%q", body, hash)
	}
}

func TestNewFileMetadata_Validates(t *testing.T) {
	ref := ingest.SourceRef{
		SourceType: ingest.SourceChase,
		AccountID:  "9876",
	}
	loc := time.FixedZone("EST", -5*3600)
	ingestedAt := time.Date(2026, 9, 25, 14, 30, 0, 0, loc)
	hash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	sourceURI := "s3://datalake-bucket/unprocessed/chase9876_statement.csv"

	meta := ingest.NewFileMetadata(
		"unprocessed/chase9876_statement.csv",
		ref,
		hash,
		sourceURI,
		ingestedAt,
	)

	if err := meta.Validate(); err != nil {
		t.Fatalf("meta.Validate() returned error: %v", err)
	}
	if meta.FileName != "chase9876_statement.csv" {
		t.Errorf("FileName = %q, want %q", meta.FileName, "chase9876_statement.csv")
	}
	if meta.AccountID != "9876" {
		t.Errorf("AccountID = %q, want %q", meta.AccountID, "9876")
	}
	if meta.FileHash != hash {
		t.Errorf("FileHash = %q, want %q", meta.FileHash, hash)
	}
	if meta.SourceURI != sourceURI {
		t.Errorf("SourceURI = %q, want %q", meta.SourceURI, sourceURI)
	}
	if meta.IngestedAt.Location() != time.UTC {
		t.Errorf("IngestedAt location = %v, want UTC", meta.IngestedAt.Location())
	}
	if !meta.IngestedAt.Equal(ingestedAt) {
		t.Errorf("IngestedAt = %v, want equal to %v", meta.IngestedAt, ingestedAt)
	}
}

func TestNewFileMetadata_ValidationInvariants(t *testing.T) {
	validRef := ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: "1234"}
	validTime := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	const validHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name        string
		filePath    string
		ref         ingest.SourceRef
		ingestedAt  time.Time
		expectedErr error
	}{
		{
			name:        "empty file name produces ErrMissingFileName",
			filePath:    "",
			ref:         validRef,
			ingestedAt:  validTime,
			expectedErr: model.ErrMissingFileName,
		},
		{
			name:        "directory marker path produces ErrMissingFileName",
			filePath:    "unprocessed/chase1234/",
			ref:         validRef,
			ingestedAt:  validTime,
			expectedErr: model.ErrMissingFileName,
		},
		{
			name:        "empty account id produces ErrMissingAccountID",
			filePath:    "chase1234.csv",
			ref:         ingest.SourceRef{SourceType: ingest.SourceChase, AccountID: ""},
			ingestedAt:  validTime,
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name:        "zero ingested at produces ErrMissingIngestedAt",
			filePath:    "chase1234.csv",
			ref:         validRef,
			ingestedAt:  time.Time{},
			expectedErr: model.ErrMissingIngestedAt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := ingest.NewFileMetadata(tc.filePath, tc.ref, validHash, "", tc.ingestedAt)
			err := meta.Validate()
			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("meta.Validate() error = %v, want %v", err, tc.expectedErr)
			}
		})
	}
}
