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

	"babylon/dataloader/datalake/model"
)

// TC-FM-01: Unit MapRecord (Zero disk I/O): Maps 1 valid record map; asserts all 18 fields of model.FidelityPosition and 5 lineage fields.
func TestFidelityMapper_MapRecord_Success(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	ingestedAt := time.Date(2026, time.September, 26, 0, 0, 0, 0, time.UTC)
	fileName := "Portfolio_Positions_Sep-25-2026.csv"
	s3URI := "s3://my-datalake-bucket/fidelity/Portfolio_Positions_Sep-25-2026.csv"
	fileHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	mapper := NewFidelityMapperWithMetadata(fileName, s3URI, fileHash, asOfDate, ingestedAt)

	record := map[string]string{
		"account number":            "123456789",
		"account name":              "INDIVIDUAL - TOD",
		"symbol":                    "FXAIX",
		"description":               "FIDELITY 500 INDEX FUND",
		"quantity":                  "100.500",
		"last price":                "$180.25",
		"last price change":         "+$1.50",
		"current value":             "$18115.13",
		"today's gain/loss dollar":  "+$150.75",
		"today's gain/loss percent": "+0.84%",
		"total gain/loss dollar":    "+$3115.13",
		"total gain/loss percent":   "+20.77%",
		"percent of account":        "45.50%",
		"cost basis total":          "$15000.00",
		"average cost basis":        "$149.25",
		"type":                      "Cash",
		"__row_number":              "12",
	}

	pos, err := mapper.MapRecord(record)
	if err != nil {
		t.Fatalf("unexpected error mapping record: %v", err)
	}

	assertPositionIdentity(t, pos, asOfDate)
	assertPositionPricingAndTotals(t, pos)
	assertPositionCostAndBasis(t, pos)
	assertPositionLineage(t, pos.Source, s3URI, fileName, fileHash, ingestedAt, 12)
}

func assertPositionIdentity(t *testing.T, pos model.FidelityPosition, asOfDate time.Time) {
	t.Helper()
	if pos.AccountNumber != "123456789" {
		t.Errorf("AccountNumber mismatch: expected 123456789, got %q", pos.AccountNumber)
	}
	if pos.AccountName != "INDIVIDUAL - TOD" {
		t.Errorf("AccountName mismatch: expected INDIVIDUAL - TOD, got %q", pos.AccountName)
	}
	if pos.Symbol != "FXAIX" {
		t.Errorf("Symbol mismatch: expected FXAIX, got %q", pos.Symbol)
	}
	if pos.Description != "FIDELITY 500 INDEX FUND" {
		t.Errorf("Description mismatch: expected FIDELITY 500 INDEX FUND, got %q", pos.Description)
	}
	if pos.Type != "Cash" {
		t.Errorf("Type mismatch: expected Cash, got %q", pos.Type)
	}
	if !pos.AsOfDate.Equal(asOfDate) {
		t.Errorf("AsOfDate mismatch: expected %v, got %v", asOfDate, pos.AsOfDate)
	}
	if pos.DataSource != "fidelity" {
		t.Errorf("DataSource mismatch: expected fidelity, got %q", pos.DataSource)
	}
}

func assertPositionPricingAndTotals(t *testing.T, pos model.FidelityPosition) {
	t.Helper()
	if pos.Quantity != 100.5 {
		t.Errorf("Quantity mismatch: expected 100.5, got %f", pos.Quantity)
	}
	if pos.LastPrice != 180.25 {
		t.Errorf("LastPrice mismatch: expected 180.25, got %f", pos.LastPrice)
	}
	if pos.LastPriceChange != 1.50 {
		t.Errorf("LastPriceChange mismatch: expected 1.50, got %f", pos.LastPriceChange)
	}
	if pos.CurrentValue != 18115.13 {
		t.Errorf("CurrentValue mismatch: expected 18115.13, got %f", pos.CurrentValue)
	}
	if pos.TodaysGainLossDollar != 150.75 {
		t.Errorf("TodaysGainLossDollar mismatch: expected 150.75, got %f", pos.TodaysGainLossDollar)
	}
	if pos.TodaysGainLossPct != 0.84 {
		t.Errorf("TodaysGainLossPct mismatch: expected 0.84, got %f", pos.TodaysGainLossPct)
	}
	if pos.TotalGainLossDollar != 3115.13 {
		t.Errorf("TotalGainLossDollar mismatch: expected 3115.13, got %f", pos.TotalGainLossDollar)
	}
	if pos.TotalGainLossPct != 20.77 {
		t.Errorf("TotalGainLossPct mismatch: expected 20.77, got %f", pos.TotalGainLossPct)
	}
}

func assertPositionCostAndBasis(t *testing.T, pos model.FidelityPosition) {
	t.Helper()
	if pos.PercentOfAccount != 45.50 {
		t.Errorf("PercentOfAccount mismatch: expected 45.50, got %f", pos.PercentOfAccount)
	}
	if pos.CostBasisTotal != 15000.00 {
		t.Errorf("CostBasisTotal mismatch: expected 15000.00, got %f", pos.CostBasisTotal)
	}
	if pos.AverageCostBasis != 149.25 {
		t.Errorf("AverageCostBasis mismatch: expected 149.25, got %f", pos.AverageCostBasis)
	}
}

func assertPositionLineage(
	t *testing.T,
	source model.SourceLineage,
	s3URI, fileName, fileHash string,
	ingestedAt time.Time,
	rowNumber int64,
) {
	t.Helper()
	if source.S3URI != s3URI {
		t.Errorf("Source.S3URI mismatch: expected %q, got %q", s3URI, source.S3URI)
	}
	if source.FileName != fileName {
		t.Errorf("Source.FileName mismatch: expected %q, got %q", fileName, source.FileName)
	}
	if source.RowNumber != rowNumber {
		t.Errorf("Source.RowNumber mismatch: expected %d, got %d", rowNumber, source.RowNumber)
	}
	if !source.IngestedAt.Equal(ingestedAt) {
		t.Errorf("Source.IngestedAt mismatch: expected %v, got %v", ingestedAt, source.IngestedAt)
	}
	if source.SHA256 != fileHash {
		t.Errorf("Source.SHA256 mismatch: expected %q, got %q", fileHash, source.SHA256)
	}
}

// TC-FM-02: Unit MapRecords (Zero disk I/O): Multi-record slice; asserts length, order, and RowNumber propagation.
func TestFidelityMapper_MapRecords_MultiRecordOrder(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	ingestedAt := time.Date(2026, time.September, 26, 0, 0, 0, 0, time.UTC)
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		asOfDate,
		ingestedAt,
	)

	rawRecords := []map[string]string{
		{
			"account number": "ACC-1",
			"symbol":         "FXAIX",
			"quantity":       "10.5",
			"current value":  "$1500.00",
			"__row_number":   "2",
		},
		{
			"account number": "ACC-1",
			"symbol":         "FSKAX",
			"quantity":       "20.0",
			"current value":  "$2800.00",
			"__row_number":   "3",
		},
		{
			"account number": "ACC-1",
			"symbol":         "SPAXX**",
			"quantity":       "500.0",
			"current value":  "$500.00",
			"__row_number":   "4",
		},
	}

	positions, err := mapper.MapRecords(rawRecords)
	if err != nil {
		t.Fatalf("unexpected error mapping records: %v", err)
	}

	if len(positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(positions))
	}

	expectedSymbols := []string{"FXAIX", "FSKAX", "SPAXX**"}
	expectedRowNumbers := []int64{2, 3, 4}

	for i := range positions {
		if positions[i].Symbol != expectedSymbols[i] {
			t.Errorf("index %d: expected symbol %s, got %s", i, expectedSymbols[i], positions[i].Symbol)
		}
		if positions[i].Source.RowNumber != expectedRowNumbers[i] {
			t.Errorf("index %d: expected row number %d, got %d", i, expectedRowNumbers[i], positions[i].Source.RowNumber)
		}
		if positions[i].Source.FileName != "test.csv" {
			t.Errorf("index %d: expected fileName test.csv, got %s", i, positions[i].Source.FileName)
		}
		if positions[i].Source.SHA256 != "hash123" {
			t.Errorf("index %d: expected fileHash hash123, got %s", i, positions[i].Source.SHA256)
		}
	}
}

// TC-FM-03: Unit MapRecords (Zero disk I/O): Empty slice produces empty output slice and nil error.
func TestFidelityMapper_MapRecords_EmptyInput(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		asOfDate,
		time.Now().UTC(),
	)

	t.Run("empty slice", func(t *testing.T) {
		positions, err := mapper.MapRecords([]map[string]string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if positions == nil {
			t.Fatal("expected non-nil empty slice, got nil")
		}
		if len(positions) != 0 {
			t.Fatalf("expected 0 positions, got %d", len(positions))
		}
	})

	t.Run("nil slice", func(t *testing.T) {
		positions, err := mapper.MapRecords(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if positions == nil {
			t.Fatal("expected non-nil empty slice, got nil")
		}
		if len(positions) != 0 {
			t.Fatalf("expected 0 positions, got %d", len(positions))
		}
	})
}

// TC-FM-04: Unit MapRecord (Zero disk I/O): Malformed currency or quantity returns error identifying failing row number.
func TestFidelityMapper_MapRecord_ErrorWithRowNumber(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		asOfDate,
		time.Now().UTC(),
	)

	t.Run("malformed quantity returns error identifying failing row number", func(t *testing.T) {
		record := map[string]string{
			"symbol":       "FXAIX",
			"quantity":     "invalid_quantity",
			"__row_number": "42",
		}
		pos, err := mapper.MapRecord(record)
		if err == nil {
			t.Fatal("expected error for malformed quantity, got nil")
		}
		if !strings.Contains(err.Error(), "row 42") {
			t.Errorf("expected error to contain 'row 42', got: %v", err)
		}
		if !strings.Contains(err.Error(), "failed to map raw record to Fidelity position") {
			t.Errorf("expected error to contain context message, got: %v", err)
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got: %+v", pos)
		}
	})

	t.Run("malformed currency returns error identifying failing row number", func(t *testing.T) {
		record := map[string]string{
			"symbol":        "FXAIX",
			"current value": "invalid_currency",
			"__row_number":  "99",
		}
		pos, err := mapper.MapRecord(record)
		if err == nil {
			t.Fatal("expected error for malformed currency, got nil")
		}
		if !strings.Contains(err.Error(), "row 99") {
			t.Errorf("expected error to contain 'row 99', got: %v", err)
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got: %+v", pos)
		}
	})

	t.Run("MapRecords fails fast on record error identifying row number", func(t *testing.T) {
		rawRecords := []map[string]string{
			{
				"symbol":        "FXAIX",
				"quantity":      "10.0",
				"current value": "$1500.00",
				"__row_number":  "2",
			},
			{
				"symbol":        "FSKAX",
				"quantity":      "corrupt_quantity",
				"current value": "$2800.00",
				"__row_number":  "7",
			},
			{
				"symbol":        "SPAXX**",
				"quantity":      "500.0",
				"current value": "$500.00",
				"__row_number":  "8",
			},
		}
		positions, err := mapper.MapRecords(rawRecords)
		if err == nil {
			t.Fatal("expected error from MapRecords, got nil")
		}
		if positions != nil {
			t.Errorf("expected nil positions on error, got: %+v", positions)
		}
		if !strings.Contains(err.Error(), "row 7") {
			t.Errorf("expected error to mention 'row 7', got: %v", err)
		}
	})
}

// TC-FM-05: Unit parseRowNumber (Zero disk I/O): Table-driven coverage (valid string, missing key, empty string, non-integer).
func TestFidelityMapper_ParseRowNumber(t *testing.T) {
	tests := []struct {
		name     string
		record   map[string]string
		expected int64
	}{
		{
			name:     "valid positive row number",
			record:   map[string]string{"__row_number": "42"},
			expected: 42,
		},
		{
			name:     "valid row number 1",
			record:   map[string]string{"__row_number": "1"},
			expected: 1,
		},
		{
			name:     "missing key",
			record:   map[string]string{"symbol": "FXAIX"},
			expected: 0,
		},
		{
			name:     "empty string",
			record:   map[string]string{"__row_number": ""},
			expected: 0,
		},
		{
			name:     "non-integer string",
			record:   map[string]string{"__row_number": "abc"},
			expected: 0,
		},
		{
			name:     "floating point string",
			record:   map[string]string{"__row_number": "12.34"},
			expected: 0,
		},
		{
			name:     "nil record map",
			record:   nil,
			expected: 0,
		},
		{
			name:     "negative integer string",
			record:   map[string]string{"__row_number": "-10"},
			expected: 0,
		},
		{
			name:     "zero integer string",
			record:   map[string]string{"__row_number": "0"},
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseRowNumber(tc.record)
			if got != tc.expected {
				t.Errorf("parseRowNumber(%+v) = %d; expected %d", tc.record, got, tc.expected)
			}
		})
	}
}

// TC-FM-06: Unit/IO NewFidelityMapper (Yes - t.TempDir()): Valid fixture; asserts SHA-256 and parsed asOfDate.
func TestNewFidelityMapper_Success(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_Valid.csv")
	content := "Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,Percent Of Account,Cost Basis Total,Average Cost Basis,Type\n" +
		"SYNTH-ROTH-101,Roth IRA,SPAXX**,HELD IN MONEY MARKET,3250.000,$1.00,$0.00,$3250.00,$0.00,0.00%,$0.00,0.00%,25.00%,$3250.00,$1.00,Cash\n\n" +
		"\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n"

	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHashBytes := sha256.Sum256([]byte(content))
	expectedHash := hex.EncodeToString(expectedHashBytes[:])

	parsedDate, parseErr := model.ParseFidelityDateDownloaded("\"Date downloaded Sep-25-2026 9:50 p.m ET\"")
	if parseErr != nil {
		t.Fatalf("failed to parse reference date: %v", parseErr)
	}
	expectedAsOfDate := parsedDate.UTC()

	fileName := "Portfolio_Positions_Valid.csv"
	s3URI := "s3://my-datalake/Portfolio_Positions_Valid.csv"

	mapper, err := NewFidelityMapper(context.Background(), filePath, fileName, s3URI)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	if mapper == nil {
		t.Fatal("expected non-nil FidelityMapper instance")
	}
	if mapper.fileName != fileName {
		t.Errorf("fileName mismatch: expected %q, got %q", fileName, mapper.fileName)
	}
	if mapper.s3URI != s3URI {
		t.Errorf("s3URI mismatch: expected %q, got %q", s3URI, mapper.s3URI)
	}
	if mapper.fileHash != expectedHash {
		t.Errorf("fileHash mismatch: expected %q, got %q", expectedHash, mapper.fileHash)
	}
	if !mapper.asOfDate.Equal(expectedAsOfDate) {
		t.Errorf("asOfDate mismatch: expected %v, got %v", expectedAsOfDate, mapper.asOfDate)
	}
	if mapper.ingestedAt.IsZero() {
		t.Error("expected non-zero ingestedAt timestamp")
	}
}

// TC-FM-07: Unit/IO NewFidelityMapper (No - stat fail): Nonexistent path returns wrapped failed to compute sha256 error.
func TestNewFidelityMapper_NonexistentFile(t *testing.T) {
	nonExistentPath := filepath.Join(t.TempDir(), "nonexistent_statement.csv")

	mapper, err := NewFidelityMapper(
		context.Background(),
		nonExistentPath,
		"nonexistent_statement.csv",
		"s3://bucket/nonexistent_statement.csv",
	)
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
	if mapper != nil {
		t.Errorf("expected nil mapper on error, got: %+v", mapper)
	}
	if !strings.Contains(err.Error(), "failed to compute sha256 for file") {
		t.Errorf("expected error to contain 'failed to compute sha256 for file', got: %v", err)
	}
}

// TC-FM-08: Unit/IO NewFidelityMapper (Yes - t.TempDir()): File without Date downloaded footer returns descriptive error.
func TestNewFidelityMapper_MissingDateFooter(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_NoFooter.csv")
	content := "Account Number,Symbol,Current Value\nACC123,FXAIX,$1000.00\n"

	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	mapper, err := NewFidelityMapper(
		context.Background(),
		filePath,
		"Portfolio_Positions_NoFooter.csv",
		"s3://bucket/Portfolio_Positions_NoFooter.csv",
	)
	if err == nil {
		t.Fatal("expected error for file missing footer, got nil")
	}
	if mapper != nil {
		t.Errorf("expected nil mapper on error, got: %+v", mapper)
	}
	if !errors.Is(err, ErrMissingDateFooter) {
		t.Errorf("expected errors.Is(err, ErrMissingDateFooter), got: %v", err)
	}
	if !strings.Contains(err.Error(), "no valid Fidelity date downloaded") {
		t.Errorf("expected error to contain 'no valid Fidelity date downloaded', got: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to extract Fidelity asOfDate for file") {
		t.Errorf("expected error to contain 'failed to extract Fidelity asOfDate for file', got: %v", err)
	}
}

// TC-FM-09: Unit/IO NewFidelityMapper (Yes - t.TempDir()): File with unparseable date footer returns wrapped parse error.
func TestNewFidelityMapper_UnparseableDateFooter(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_BadDate.csv")
	content := "Account Number,Symbol,Current Value\nACC123,FXAIX,$1000.00\n\n\"Date downloaded Not-A-Real-Date 9:50 p.m ET\"\n"

	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	mapper, err := NewFidelityMapper(
		context.Background(),
		filePath,
		"Portfolio_Positions_BadDate.csv",
		"s3://bucket/Portfolio_Positions_BadDate.csv",
	)
	if err == nil {
		t.Fatal("expected error for file with unparseable date, got nil")
	}
	if mapper != nil {
		t.Errorf("expected nil mapper on error, got: %+v", mapper)
	}
	if !strings.Contains(err.Error(), "failed to parse Fidelity date downloaded") {
		t.Errorf("expected error to contain 'failed to parse Fidelity date downloaded', got: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to extract Fidelity asOfDate for file") {
		t.Errorf("expected error to contain 'failed to extract Fidelity asOfDate for file', got: %v", err)
	}
}

// TC-FM-10: Unit/IO NewFidelityMapper (Zero - cancelled context): Cancelled context aborts before disk I/O.
func TestNewFidelityMapper_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nonExistentPath := filepath.Join(t.TempDir(), "should_never_be_touched.csv")

	mapper, err := NewFidelityMapper(
		ctx,
		nonExistentPath,
		"should_never_be_touched.csv",
		"s3://bucket/should_never_be_touched.csv",
	)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if mapper != nil {
		t.Errorf("expected nil mapper on error, got: %+v", mapper)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
	}
}

// TC-FM-11: Nil receiver guards for MapRecord, MapRecords, and MapRecordsWithContext.
func TestFidelityMapper_NilReceiver_MapMethods(t *testing.T) {
	var mapper *FidelityMapper

	t.Run("MapRecord returns error on nil receiver", func(t *testing.T) {
		_, err := mapper.MapRecord(map[string]string{"symbol": "FXAIX"})
		if err == nil {
			t.Fatal("expected error on nil receiver, got nil")
		}
		if !strings.Contains(err.Error(), "cannot map record: FidelityMapper instance is nil") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("MapRecords returns error on nil receiver", func(t *testing.T) {
		_, err := mapper.MapRecords([]map[string]string{{"symbol": "FXAIX"}})
		if err == nil {
			t.Fatal("expected error on nil receiver, got nil")
		}
		if !strings.Contains(err.Error(), "cannot map records: FidelityMapper instance is nil") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("MapRecordsWithContext returns error on nil receiver", func(t *testing.T) {
		_, err := mapper.MapRecordsWithContext(context.Background(), []map[string]string{{"symbol": "FXAIX"}})
		if err == nil {
			t.Fatal("expected error on nil receiver, got nil")
		}
		if !strings.Contains(err.Error(), "cannot map records: FidelityMapper instance is nil") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

// TC-FM-12: Nil receiver guards for exported getters.
func TestFidelityMapper_NilReceiver_Getters(t *testing.T) {
	var mapper *FidelityMapper

	if mapper.FileName() != "" {
		t.Errorf("expected empty FileName, got %q", mapper.FileName())
	}
	if mapper.S3URI() != "" {
		t.Errorf("expected empty S3URI, got %q", mapper.S3URI())
	}
	if mapper.FileHash() != "" {
		t.Errorf("expected empty FileHash, got %q", mapper.FileHash())
	}
	if !mapper.AsOfDate().IsZero() {
		t.Errorf("expected zero AsOfDate, got %v", mapper.AsOfDate())
	}
	if !mapper.IngestedAt().IsZero() {
		t.Errorf("expected zero IngestedAt, got %v", mapper.IngestedAt())
	}
}

// TC-FM-12: Exported getters return expected provenance metadata.
func TestFidelityMapper_Getters(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	ingestedAt := time.Date(2026, time.September, 26, 0, 0, 0, 0, time.UTC)
	fileName := "Portfolio_Positions_Sep-25-2026.csv"
	s3URI := "s3://bucket/fidelity/Portfolio_Positions_Sep-25-2026.csv"
	fileHash := "hash123456"

	mapper := NewFidelityMapperWithMetadata(fileName, s3URI, fileHash, asOfDate, ingestedAt)

	if mapper.FileName() != fileName {
		t.Errorf("FileName() = %q, expected %q", mapper.FileName(), fileName)
	}
	if mapper.S3URI() != s3URI {
		t.Errorf("S3URI() = %q, expected %q", mapper.S3URI(), s3URI)
	}
	if mapper.FileHash() != fileHash {
		t.Errorf("FileHash() = %q, expected %q", mapper.FileHash(), fileHash)
	}
	if !mapper.AsOfDate().Equal(asOfDate) {
		t.Errorf("AsOfDate() = %v, expected %v", mapper.AsOfDate(), asOfDate)
	}
	if !mapper.IngestedAt().Equal(ingestedAt) {
		t.Errorf("IngestedAt() = %v, expected %v", mapper.IngestedAt(), ingestedAt)
	}
}

// TC-FM-13: Symbol validation requires non-empty, non-whitespace symbol.
func TestFidelityMapper_SymbolValidation(t *testing.T) {
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		time.Now().UTC(),
		time.Now().UTC(),
	)

	t.Run("missing symbol with row number returns row qualified error", func(t *testing.T) {
		record := map[string]string{
			"quantity":     "10.0",
			"__row_number": "5",
		}
		pos, err := mapper.MapRecord(record)
		if err == nil {
			t.Fatal("expected error for missing symbol, got nil")
		}
		expectedErr := "invalid Fidelity position (row 5): symbol is required"
		if err.Error() != expectedErr {
			t.Errorf("expected %q, got %q", expectedErr, err.Error())
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got %+v", pos)
		}
	})

	t.Run("whitespace symbol with row number 0 returns unqualified error", func(t *testing.T) {
		record := map[string]string{
			"symbol":       "   ",
			"quantity":     "10.0",
			"__row_number": "0",
		}
		pos, err := mapper.MapRecord(record)
		if err == nil {
			t.Fatal("expected error for whitespace symbol, got nil")
		}
		expectedErr := "invalid Fidelity position: symbol is required"
		if err.Error() != expectedErr {
			t.Errorf("expected %q, got %q", expectedErr, err.Error())
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got %+v", pos)
		}
	})
}

// TC-FM-14: Clean row formatting in errors omits (row 0) when rowNumber <= 0.
func TestFidelityMapper_ErrorFormatting_RowZero(t *testing.T) {
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		time.Now().UTC(),
		time.Now().UTC(),
	)

	record := map[string]string{
		"symbol":   "FXAIX",
		"quantity": "not-a-number",
	}

	_, err := mapper.MapRecord(record)
	if err == nil {
		t.Fatal("expected error for malformed quantity, got nil")
	}
	if strings.Contains(err.Error(), "row 0") {
		t.Errorf("expected error to omit '(row 0)', got: %v", err)
	}
	if !strings.HasPrefix(err.Error(), "failed to map raw record to Fidelity position: ") {
		t.Errorf("expected error prefix 'failed to map raw record to Fidelity position: ', got: %v", err)
	}
}

// TC-FM-15: MapRecordsWithContext respects context cancellation before and during mapping.
func TestFidelityMapper_MapRecordsWithContext(t *testing.T) {
	mapper := NewFidelityMapperWithMetadata(
		"test.csv",
		"s3://bucket/test.csv",
		"hash123",
		time.Now().UTC(),
		time.Now().UTC(),
	)

	rawRecords := []map[string]string{
		{"symbol": "FXAIX", "quantity": "10.0", "__row_number": "2"},
		{"symbol": "FSKAX", "quantity": "20.0", "__row_number": "3"},
	}

	t.Run("succeeds with active context", func(t *testing.T) {
		positions, err := mapper.MapRecordsWithContext(context.Background(), rawRecords)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(positions) != 2 {
			t.Fatalf("expected 2 positions, got %d", len(positions))
		}
	})

	t.Run("pre-canceled context returns context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		positions, err := mapper.MapRecordsWithContext(ctx, rawRecords)
		if err == nil {
			t.Fatal("expected error with canceled context, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
		}
		if positions != nil {
			t.Errorf("expected nil positions on error, got %+v", positions)
		}
	})
}

// TC-FM-16: Trailing delimiters and commas in date downloaded footer parse successfully.
func TestNewFidelityMapper_TrailingDelimiters(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_Trailing.csv")
	content := "Account Number,Symbol,Current Value\nACC123,FXAIX,$1000.00\n\n\"Date downloaded Sep-25-2026 9:50 p.m ET\",,,,,\n"

	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	mapper, err := NewFidelityMapper(
		context.Background(),
		filePath,
		"Portfolio_Positions_Trailing.csv",
		"s3://bucket/Portfolio_Positions_Trailing.csv",
	)
	if err != nil {
		t.Fatalf("unexpected error parsing date with trailing delimiters: %v", err)
	}
	if mapper == nil {
		t.Fatal("expected non-nil mapper")
	}

	parsedDate, _ := model.ParseFidelityDateDownloaded("Date downloaded Sep-25-2026 9:50 p.m ET")
	if !mapper.AsOfDate().Equal(parsedDate.UTC()) {
		t.Errorf("expected AsOfDate %v, got %v", parsedDate.UTC(), mapper.AsOfDate())
	}
}

// TC-FM-17: Large file (>64KB) tail seeking finds footer at end in O(1).
func TestNewFidelityMapper_LargeFileTailSeek(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_Large.csv")

	// Generate a file larger than 64KB (e.g. ~80KB)
	var sb strings.Builder
	sb.WriteString("Account Number,Symbol,Quantity,Current Value\n")
	paddingRow := "ACC123,FXAIX,10.0,$1500.00\n"
	for sb.Len() < 80*1024 {
		sb.WriteString(paddingRow)
	}
	sb.WriteString("\n\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n")

	if err := os.WriteFile(filePath, []byte(sb.String()), 0o600); err != nil {
		t.Fatalf("failed to write large test file: %v", err)
	}

	mapper, err := NewFidelityMapper(
		context.Background(),
		filePath,
		"Portfolio_Positions_Large.csv",
		"s3://bucket/Portfolio_Positions_Large.csv",
	)
	if err != nil {
		t.Fatalf("unexpected error from tail seek on large file: %v", err)
	}
	if mapper == nil {
		t.Fatal("expected non-nil mapper")
	}

	parsedDate, _ := model.ParseFidelityDateDownloaded("Date downloaded Sep-25-2026 9:50 p.m ET")
	if !mapper.AsOfDate().Equal(parsedDate.UTC()) {
		t.Errorf("expected AsOfDate %v, got %v", parsedDate.UTC(), mapper.AsOfDate())
	}
}

// TC-FM-18: Large file (>64KB) fallback rewinds and finds date header at start of file.
func TestNewFidelityMapper_LargeFileHeaderFallback(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_Fallback.csv")

	// Place date at top, followed by >64KB of padding
	var sb strings.Builder
	sb.WriteString("\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n")
	sb.WriteString("Account Number,Symbol,Quantity,Current Value\n")
	paddingRow := "ACC123,FXAIX,10.0,$1500.00\n"
	for sb.Len() < 80*1024 {
		sb.WriteString(paddingRow)
	}

	if err := os.WriteFile(filePath, []byte(sb.String()), 0o600); err != nil {
		t.Fatalf("failed to write large fallback test file: %v", err)
	}

	mapper, err := NewFidelityMapper(
		context.Background(),
		filePath,
		"Portfolio_Positions_Fallback.csv",
		"s3://bucket/Portfolio_Positions_Fallback.csv",
	)
	if err != nil {
		t.Fatalf("unexpected error from rewind fallback on large file: %v", err)
	}
	if mapper == nil {
		t.Fatal("expected non-nil mapper")
	}

	parsedDate, _ := model.ParseFidelityDateDownloaded("Date downloaded Sep-25-2026 9:50 p.m ET")
	if !mapper.AsOfDate().Equal(parsedDate.UTC()) {
		t.Errorf("expected AsOfDate %v, got %v", parsedDate.UTC(), mapper.AsOfDate())
	}
}
