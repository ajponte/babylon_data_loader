package datalake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TC-CM-01: Maps valid Chase Banking record; verifies all fields, running balance, and 5 lineage fields.
func TestChaseMapper_TC_CM_01_ValidBankingRecord(t *testing.T) {
	ingestedAt := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
	mapper := NewChaseMapperWithMetadata(
		"Chase1234_Activity_20260916.CSV",
		"s3://my-datalake/chase/Chase1234_Activity_20260916.CSV",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"chase1234",
		ingestedAt,
	)

	record := map[string]string{
		"Details":         "DEBIT",
		"Posting Date":    "09/16/2026",
		"Description":     "WHOLE FOODS MARKET",
		"Amount":          "-$75.50",
		"Type":            "DEBIT_CARD",
		"Balance":         "$1,250.25",
		"Check or Slip #": "1001",
		"__row_number":    "2",
	}

	tx, err := mapper.MapRecord(record)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if tx.AccountID != "chase1234" {
		t.Errorf("expected AccountID 'chase1234', got %q", tx.AccountID)
	}
	expectedPostDate := time.Date(2026, time.September, 16, 0, 0, 0, 0, time.UTC)
	if !tx.PostingDate.Equal(expectedPostDate) {
		t.Errorf("expected PostingDate %v, got %v", expectedPostDate, tx.PostingDate)
	}
	if tx.TransactionDate != nil {
		t.Errorf("expected nil TransactionDate for banking record, got %v", tx.TransactionDate)
	}
	if tx.Description != "WHOLE FOODS MARKET" {
		t.Errorf("expected Description 'WHOLE FOODS MARKET', got %q", tx.Description)
	}
	if tx.Amount != -75.50 {
		t.Errorf("expected Amount -75.50, got %f", tx.Amount)
	}
	if tx.TransactionType != "DEBIT_CARD" {
		t.Errorf("expected TransactionType 'DEBIT_CARD', got %q", tx.TransactionType)
	}
	if tx.Balance == nil || *tx.Balance != 1250.25 {
		t.Errorf("expected Balance 1250.25, got %v", tx.Balance)
	}
	if tx.Details != "DEBIT" {
		t.Errorf("expected Details 'DEBIT', got %q", tx.Details)
	}
	if tx.CheckOrSlipNum != "1001" {
		t.Errorf("expected CheckOrSlipNum '1001', got %q", tx.CheckOrSlipNum)
	}
	if tx.Category != "" {
		t.Errorf("expected empty Category, got %q", tx.Category)
	}
	if tx.Memo != "" {
		t.Errorf("expected empty Memo, got %q", tx.Memo)
	}
	if tx.DataSource != "chase" {
		t.Errorf("expected DataSource 'chase', got %q", tx.DataSource)
	}

	// Lineage verification
	if tx.Source.FileName != mapper.FileName() {
		t.Errorf("expected Source.FileName %q, got %q", mapper.FileName(), tx.Source.FileName)
	}
	if tx.Source.S3URI != mapper.S3URI() {
		t.Errorf("expected Source.S3URI %q, got %q", mapper.S3URI(), tx.Source.S3URI)
	}
	if tx.Source.RowNumber != 2 {
		t.Errorf("expected Source.RowNumber 2, got %d", tx.Source.RowNumber)
	}
	if tx.Source.SHA256 != mapper.FileHash() {
		t.Errorf("expected Source.SHA256 %q, got %q", mapper.FileHash(), tx.Source.SHA256)
	}
	if !tx.Source.IngestedAt.Equal(ingestedAt) {
		t.Errorf("expected Source.IngestedAt %v, got %v", ingestedAt, tx.Source.IngestedAt)
	}
}

// TC-CM-02: Maps valid Chase Credit Card record; verifies transaction date, category, memo, and lineage.
func TestChaseMapper_TC_CM_02_ValidCreditCardRecord(t *testing.T) {
	ingestedAt := time.Date(2026, time.September, 16, 14, 30, 0, 0, time.UTC)
	mapper := NewChaseMapperWithMetadata(
		"Chase2472_Activity_20260916.CSV",
		"s3://my-datalake/chase/Chase2472_Activity_20260916.CSV",
		"a1b2c3d4e5f67890123456789abcdef0123456789abcdef0123456789abcdef0",
		"chase2472",
		ingestedAt,
	)

	record := map[string]string{
		"Transaction Date": "09/14/2026",
		"Post Date":        "09/15/2026",
		"Description":      "AMAZON.COM*123",
		"Category":         "Shopping",
		"Type":             "Sale",
		"Amount":           "-34.99",
		"Memo":             "Office supplies",
		"__row_number":     "5",
	}

	tx, err := mapper.MapRecord(record)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if tx.AccountID != "chase2472" {
		t.Errorf("expected AccountID 'chase2472', got %q", tx.AccountID)
	}
	expectedPostDate := time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC)
	if !tx.PostingDate.Equal(expectedPostDate) {
		t.Errorf("expected PostingDate %v, got %v", expectedPostDate, tx.PostingDate)
	}
	if tx.TransactionDate == nil {
		t.Fatalf("expected non-nil TransactionDate for credit card record")
	}
	expectedTxDate := time.Date(2026, time.September, 14, 0, 0, 0, 0, time.UTC)
	if !tx.TransactionDate.Equal(expectedTxDate) {
		t.Errorf("expected TransactionDate %v, got %v", expectedTxDate, *tx.TransactionDate)
	}
	if tx.Description != "AMAZON.COM*123" {
		t.Errorf("expected Description 'AMAZON.COM*123', got %q", tx.Description)
	}
	if tx.Category != "Shopping" {
		t.Errorf("expected Category 'Shopping', got %q", tx.Category)
	}
	if tx.TransactionType != "Sale" {
		t.Errorf("expected TransactionType 'Sale', got %q", tx.TransactionType)
	}
	if tx.Amount != -34.99 {
		t.Errorf("expected Amount -34.99, got %f", tx.Amount)
	}
	if tx.Memo != "Office supplies" {
		t.Errorf("expected Memo 'Office supplies', got %q", tx.Memo)
	}
	if tx.Balance != nil {
		t.Errorf("expected nil Balance for credit card record, got %v", tx.Balance)
	}
	if tx.Source.RowNumber != 5 {
		t.Errorf("expected Source.RowNumber 5, got %d", tx.Source.RowNumber)
	}
}

// TC-CM-03: Maps multi-record slice; verifies ordering, count, and individual rowNumber assignment.
func TestChaseMapper_TC_CM_03_MultiRecordBatch(t *testing.T) {
	mapper := NewChaseMapperWithMetadata(
		"chase_batch.csv",
		"s3://bucket/chase_batch.csv",
		"dummyhash",
		"chase9999",
		time.Now().UTC(),
	)

	records := []map[string]string{
		{
			"Posting Date": "09/01/2026",
			"Description":  "Coffee Shop",
			"Amount":       "-4.50",
			"__row_number": "2",
		},
		{
			"Posting Date": "09/02/2026",
			"Description":  "Gas Station",
			"Amount":       "-45.00",
			"__row_number": "3",
		},
		{
			"Posting Date": "09/03/2026",
			"Description":  "Payroll Deposit",
			"Amount":       "2500.00",
			"Type":         "ACH_CREDIT",
			"__row_number": "4",
		},
	}

	results, err := mapper.MapRecords(records)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 mapped transactions, got %d", len(results))
	}

	for i, expectedRow := range []int64{2, 3, 4} {
		if results[i].Source.RowNumber != expectedRow {
			t.Errorf("record %d: expected RowNumber %d, got %d", i, expectedRow, results[i].Source.RowNumber)
		}
	}

	if results[0].Description != "Coffee Shop" || results[1].Description != "Gas Station" || results[2].Description != "Payroll Deposit" {
		t.Errorf("unexpected record order: %+v", results)
	}
}

// TC-CM-04: Verifies empty raw records slice returns empty slice with nil error.
func TestChaseMapper_TC_CM_04_EmptyRecords(t *testing.T) {
	mapper := NewChaseMapperWithMetadata(
		"chase_empty.csv",
		"s3://bucket/chase_empty.csv",
		"hash",
		"chase1111",
		time.Now().UTC(),
	)

	tests := []struct {
		name    string
		records []map[string]string
	}{
		{"nil slice", nil},
		{"empty slice", []map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := mapper.MapRecords(tt.records)
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if res == nil || len(res) != 0 {
				t.Errorf("expected empty non-nil slice, got %v", res)
			}
		})
	}
}

// TC-CM-05: Table-driven invalid data: missing date, invalid date format, invalid amount, missing description.
func TestChaseMapper_TC_CM_05_InvalidData_TableDriven(t *testing.T) {
	mapper := NewChaseMapperWithMetadata(
		"chase_invalid.csv",
		"s3://bucket/chase_invalid.csv",
		"hash",
		"chase1111",
		time.Now().UTC(),
	)

	tests := []struct {
		name          string
		record        map[string]string
		errSubstrings []string
	}{
		{
			name: "missing posting date",
			record: map[string]string{
				"Description":  "Grocery",
				"Amount":       "-25.00",
				"__row_number": "10",
			},
			errSubstrings: []string{"row 10", "posting date is required"},
		},
		{
			name: "invalid posting date format",
			record: map[string]string{
				"Posting Date": "2026-09-16",
				"Description":  "Grocery",
				"Amount":       "-25.00",
				"__row_number": "11",
			},
			errSubstrings: []string{"row 11", "invalid posting date format"},
		},
		{
			name: "missing amount",
			record: map[string]string{
				"Posting Date": "09/16/2026",
				"Description":  "Grocery",
				"__row_number": "12",
			},
			errSubstrings: []string{"row 12", "amount is required"},
		},
		{
			name: "invalid amount format",
			record: map[string]string{
				"Posting Date": "09/16/2026",
				"Description":  "Grocery",
				"Amount":       "twenty-dollars",
				"__row_number": "13",
			},
			errSubstrings: []string{"row 13", "invalid amount value"},
		},
		{
			name: "missing description",
			record: map[string]string{
				"Posting Date": "09/16/2026",
				"Amount":       "-25.00",
				"__row_number": "14",
			},
			errSubstrings: []string{"row 14", "description is required"},
		},
		{
			name: "whitespace description",
			record: map[string]string{
				"Posting Date": "09/16/2026",
				"Description":  "   ",
				"Amount":       "-25.00",
				"__row_number": "15",
			},
			errSubstrings: []string{"row 15", "description is required"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mapper.MapRecord(tt.record)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, sub := range tt.errSubstrings {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("expected error message to contain %q, got %q", sub, err.Error())
				}
			}
		})
	}
}

// TC-CM-06: Table-driven row number parsing: valid string, empty string, missing key, non-integer.
func TestChaseMapper_TC_CM_06_ParseRowNumber_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		record   map[string]string
		expected int64
	}{
		{"valid positive row", map[string]string{"__row_number": "42"}, 42},
		{"valid row 1", map[string]string{"__row_number": "1"}, 1},
		{"empty row number", map[string]string{"__row_number": ""}, 0},
		{"missing row number key", map[string]string{"description": "test"}, 0},
		{"non-integer string", map[string]string{"__row_number": "abc"}, 0},
		{"float string", map[string]string{"__row_number": "3.14"}, 0},
		{"zero row number", map[string]string{"__row_number": "0"}, 0},
		{"negative row number", map[string]string{"__row_number": "-1"}, 0},
		{"nil record map", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := parseRowNumber(tt.record)
			if actual != tt.expected {
				t.Errorf("expected row number %d, got %d", tt.expected, actual)
			}
		})
	}
}

// TC-CM-07: Verifies SHA-256 calculation and field population from real CSV fixture on disk.
func TestChaseMapper_TC_CM_07_Constructor_WithFileHashing(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "chase_real.csv")
	content := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\nDEBIT,09/16/2026,STORE,-10.00,DEBIT_CARD,500.00,\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHashBytes := sha256.Sum256([]byte(content))
	expectedHash := hex.EncodeToString(expectedHashBytes[:])

	mapper, err := NewChaseMapper(ctx, filePath, "chase_real.csv", "s3://bucket/chase_real.csv", "acct_chase_77")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if mapper.FileName() != "chase_real.csv" {
		t.Errorf("expected FileName 'chase_real.csv', got %q", mapper.FileName())
	}
	if mapper.S3URI() != "s3://bucket/chase_real.csv" {
		t.Errorf("expected S3URI 's3://bucket/chase_real.csv', got %q", mapper.S3URI())
	}
	if mapper.AccountID() != "acct_chase_77" {
		t.Errorf("expected AccountID 'acct_chase_77', got %q", mapper.AccountID())
	}
	if mapper.FileHash() != expectedHash {
		t.Errorf("expected FileHash %q, got %q", expectedHash, mapper.FileHash())
	}
	if mapper.IngestedAt().IsZero() {
		t.Errorf("expected non-zero IngestedAt")
	}
}

// TC-CM-08: Verifies nonexistent file path returns descriptive error wrapping os.ErrNotExist.
func TestChaseMapper_TC_CM_08_Constructor_MissingFile(t *testing.T) {
	ctx := context.Background()
	nonExistentPath := filepath.Join(t.TempDir(), "does_not_exist.csv")

	_, err := NewChaseMapper(ctx, nonExistentPath, "does_not_exist.csv", "s3://bucket/path.csv", "acct123")
	if err == nil {
		t.Fatalf("expected error for nonexistent file, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected error to wrap os.ErrNotExist, got %v", err)
	}
}

// TC-CM-09: Verifies pre-canceled context aborts before disk I/O.
func TestChaseMapper_TC_CM_09_Constructor_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewChaseMapper(ctx, "/any/unreached/path.csv", "file.csv", "s3://bucket/file.csv", "acct123")
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got %v", err)
	}
}

// TC-CM-10: Verifies empty accountID returns validation error.
func TestChaseMapper_TC_CM_10_Constructor_MissingAccountID(t *testing.T) {
	ctx := context.Background()
	tmpFile := filepath.Join(t.TempDir(), "test.csv")
	_ = os.WriteFile(tmpFile, []byte("data"), 0o644)

	tests := []struct {
		name      string
		accountID string
	}{
		{"empty string", ""},
		{"whitespace only", "   \t\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewChaseMapper(ctx, tmpFile, "test.csv", "s3://bucket/test.csv", tt.accountID)
			if err == nil {
				t.Fatalf("expected error for empty accountID, got nil")
			}
			if !strings.Contains(err.Error(), "accountID is required") {
				t.Errorf("expected error message to contain 'accountID is required', got %v", err)
			}
		})
	}
}

// Additional edge cases: Nil receiver and Context Cancellation in MapRecordsWithContext.
func TestChaseMapper_NilReceiverAndContext(t *testing.T) {
	var nilMapper *ChaseMapper

	if nilMapper.FileName() != "" {
		t.Errorf("expected empty string from nilMapper.FileName()")
	}
	if nilMapper.S3URI() != "" {
		t.Errorf("expected empty string from nilMapper.S3URI()")
	}
	if nilMapper.FileHash() != "" {
		t.Errorf("expected empty string from nilMapper.FileHash()")
	}
	if nilMapper.AccountID() != "" {
		t.Errorf("expected empty string from nilMapper.AccountID()")
	}
	if !nilMapper.IngestedAt().IsZero() {
		t.Errorf("expected zero time from nilMapper.IngestedAt()")
	}

	_, err := nilMapper.MapRecords(nil)
	if err == nil {
		t.Errorf("expected error from nilMapper.MapRecords, got nil")
	}
	_, err = nilMapper.MapRecordsWithContext(context.Background(), nil)
	if err == nil {
		t.Errorf("expected error from nilMapper.MapRecordsWithContext, got nil")
	}
	_, err = nilMapper.MapRecord(map[string]string{})
	if err == nil {
		t.Errorf("expected error from nilMapper.MapRecord, got nil")
	}

	// Context cancellation during mapping
	mapper := NewChaseMapperWithMetadata("test.csv", "s3://b/f", "hash", "acct", time.Now().UTC())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records := []map[string]string{
		{"Posting Date": "09/16/2026", "Description": "Test", "Amount": "10.00"},
	}
	_, err = mapper.MapRecordsWithContext(ctx, records)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}
