package synthetic_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"strings"
	"testing"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/infra/sources/synthetic"
)

func newValidTestGeneratorOptions() synthetic.GeneratorOptions {
	return synthetic.GeneratorOptions{
		RowCount: 10,
		Seed:     42,
	}
}

type errWriter struct {
	err         error
	failAfterN  int
	writesCount int
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.writesCount >= w.failAfterN {
		return 0, w.err
	}
	w.writesCount++
	return len(p), nil
}

func TestWriteCSV_HeaderGolden(t *testing.T) {
	var buf bytes.Buffer
	opts := synthetic.GeneratorOptions{RowCount: 0}

	if err := synthetic.WriteCSV(&buf, opts); err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	got := strings.TrimRight(buf.String(), "\r\n")
	if got != legacySyntheticHeader {
		t.Errorf("header mismatch:\ngot:  %q\nwant: %q", got, legacySyntheticHeader)
	}
}

func TestWriteCSV_RowCounts(t *testing.T) {
	tests := []struct {
		name         string
		rowCount     int
		expectedRows int
	}{
		{
			name:         "zero rows produces header only",
			rowCount:     0,
			expectedRows: 0,
		},
		{
			name:         "single row produces header plus one record",
			rowCount:     1,
			expectedRows: 1,
		},
		{
			name:         "multiple rows produces header plus 25 records",
			rowCount:     25,
			expectedRows: 25,
		},
		{
			name:         "negative row count is clamped to zero rows",
			rowCount:     -10,
			expectedRows: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			opts := synthetic.GeneratorOptions{
				RowCount: tc.rowCount,
				Seed:     100,
			}

			if err := synthetic.WriteCSV(&buf, opts); err != nil {
				t.Fatalf("WriteCSV failed: %v", err)
			}

			reader := csv.NewReader(&buf)
			records, err := reader.ReadAll()
			if err != nil {
				t.Fatalf("failed to read generated CSV: %v", err)
			}

			// records[0] is the header row
			expectedTotal := tc.expectedRows + 1
			if len(records) != expectedTotal {
				t.Fatalf("expected %d total records (including header), got %d", expectedTotal, len(records))
			}

			// Validate record column count on all data rows
			for i, row := range records[1:] {
				if len(row) != 8 {
					t.Errorf("row %d: expected 8 columns, got %d", i+1, len(row))
				}
			}
		})
	}
}

func TestWriteCSV_DeterministicOutput(t *testing.T) {
	opts1 := synthetic.GeneratorOptions{RowCount: 15, Seed: 999}
	opts2 := synthetic.GeneratorOptions{RowCount: 15, Seed: 999}
	optsDefault := synthetic.GeneratorOptions{RowCount: 15, Seed: 0}
	optsDefaultSeed := synthetic.GeneratorOptions{RowCount: 15, Seed: 42}

	var buf1, buf2, bufDef, bufDefSeed bytes.Buffer
	if err := synthetic.WriteCSV(&buf1, opts1); err != nil {
		t.Fatalf("buf1 WriteCSV failed: %v", err)
	}
	if err := synthetic.WriteCSV(&buf2, opts2); err != nil {
		t.Fatalf("buf2 WriteCSV failed: %v", err)
	}
	if err := synthetic.WriteCSV(&bufDef, optsDefault); err != nil {
		t.Fatalf("bufDef WriteCSV failed: %v", err)
	}
	if err := synthetic.WriteCSV(&bufDefSeed, optsDefaultSeed); err != nil {
		t.Fatalf("bufDefSeed WriteCSV failed: %v", err)
	}

	if buf1.String() != buf2.String() {
		t.Error("expected identical output for same seed (999)")
	}

	if bufDef.String() != bufDefSeed.String() {
		t.Error("expected seed: 0 to default to seed: 42 producing identical output")
	}
}

func assertRoundTripTransaction(t *testing.T, i int, tx model.ChaseTransaction) bool {
	t.Helper()
	if valErr := tx.Validate(); valErr != nil {
		t.Errorf("tx %d failed validation: %v", i, valErr)
	}
	if tx.Details == "" {
		t.Errorf("tx %d: expected non-empty Details", i)
	}
	if tx.Balance == nil {
		t.Errorf("tx %d: expected non-nil Balance", i)
	}
	if tx.Type != "DEBIT" && tx.Type != "CREDIT" {
		t.Errorf("tx %d: unexpected Type %q", i, tx.Type)
	}
	if tx.CheckOrSlipNum != "" {
		if tx.Details != "CHECK" {
			t.Errorf("tx %d: expected Details 'CHECK' when CheckOrSlipNum is present, got %q", i, tx.Details)
		}
		return true
	}
	return false
}

func TestWriteCSV_RoundTripWithCSVExtractor(t *testing.T) {
	const count = 25
	opts := synthetic.GeneratorOptions{
		RowCount: count,
		Seed:     42,
	}

	var buf bytes.Buffer
	if err := synthetic.WriteCSV(&buf, opts); err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	batch, err := extractor.Extract(context.Background(), &buf, meta)
	if err != nil {
		t.Fatalf("CSVExtractor.Extract failed: %v", err)
	}

	if batch.RecordCount != count {
		t.Errorf("expected batch record count %d, got %d", count, batch.RecordCount)
	}

	payload, ok := batch.Payload.([]model.ChaseTransaction)
	if !ok {
		t.Fatalf("expected batch.Payload to be []model.ChaseTransaction, got %T", batch.Payload)
	}

	if len(payload) != count {
		t.Fatalf("expected %d transactions in payload, got %d", count, len(payload))
	}

	hasCheck := false
	for i, tx := range payload {
		if assertRoundTripTransaction(t, i, tx) {
			hasCheck = true
		}
	}

	if !hasCheck {
		t.Error("expected at least one transaction with populated CheckOrSlipNum")
	}
}

func TestWriteCSV_ValidationAndErrors(t *testing.T) {
	t.Run("nil writer returns ErrNilWriter", func(t *testing.T) {
		err := synthetic.WriteCSV(nil, newValidTestGeneratorOptions())
		if !errors.Is(err, synthetic.ErrNilWriter) {
			t.Errorf("expected ErrNilWriter, got: %v", err)
		}
	})

	t.Run("rowCount exceeding maximum allowable returns error", func(t *testing.T) {
		var buf bytes.Buffer
		opts := synthetic.GeneratorOptions{
			RowCount: synthetic.MaxSyntheticRowCount + 1,
		}
		err := synthetic.WriteCSV(&buf, opts)
		if err == nil {
			t.Fatal("expected error for excessive row count, got nil")
		}
		if !strings.Contains(err.Error(), "exceeds maximum allowable limit") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("writer failure on flush returns wrapped error", func(t *testing.T) {
		writeErr := errors.New("simulated disk full")
		w := &errWriter{err: writeErr, failAfterN: 0}
		err := synthetic.WriteCSV(w, synthetic.GeneratorOptions{RowCount: 0})
		if err == nil {
			t.Fatal("expected error from failing writer on flush, got nil")
		}
		if !errors.Is(err, writeErr) {
			t.Errorf("expected error to wrap writeErr, got: %v", err)
		}
	})

	t.Run("writer failure during records returns wrapped error", func(t *testing.T) {
		writeErr := errors.New("simulated pipe closed")
		// Fail after first buffer write; with 100 rows (> 7KB), it exceeds 4KB buffer
		w := &errWriter{err: writeErr, failAfterN: 0}
		opts := synthetic.GeneratorOptions{RowCount: 100}
		err := synthetic.WriteCSV(w, opts)
		if err == nil {
			t.Fatal("expected error from failing writer, got nil")
		}
		if !errors.Is(err, writeErr) {
			t.Errorf("expected error to wrap writeErr, got: %v", err)
		}
	})
}
