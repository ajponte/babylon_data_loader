package model_test

import (
	"errors"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestDestination_Constants(t *testing.T) {
	tests := []struct {
		name     string
		dest     model.Destination
		expected string
	}{
		{
			name:     "Chase Checking destination",
			dest:     model.DestChaseChecking,
			expected: "transactions_chase",
		},
		{
			name:     "Chase Credit Card destination",
			dest:     model.DestChaseCreditCard,
			expected: "transactions_chase",
		},
		{
			name:     "Fidelity Position destination",
			dest:     model.DestFidelityPosition,
			expected: "positions_fidelity",
		},
		{
			name:     "Synthetic destination",
			dest:     model.DestSynthetic,
			expected: "transactions_synthetic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.dest) != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, string(tt.dest))
			}
		})
	}

	if model.DestChaseChecking != model.DestChaseCreditCard {
		t.Errorf("expected Chase checking and credit card to share collection destination, got %q != %q",
			model.DestChaseChecking, model.DestChaseCreditCard)
	}
}

func TestDestination_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		dest     model.Destination
		expected bool
	}{
		{name: "DestChaseChecking", dest: model.DestChaseChecking, expected: true},
		{name: "DestChaseCreditCard", dest: model.DestChaseCreditCard, expected: true},
		{name: "DestFidelityPosition", dest: model.DestFidelityPosition, expected: true},
		{name: "DestSynthetic", dest: model.DestSynthetic, expected: true},
		{name: "Empty string destination", dest: model.Destination(""), expected: false},
		{name: "Unknown destination", dest: model.Destination("transactions_unknown"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dest.IsValid(); got != tt.expected {
				t.Errorf("Destination(%q).IsValid() = %v; want %v", tt.dest, got, tt.expected)
			}
		})
	}
}

func TestIngestionBatch_Instantiation(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	batch := model.IngestionBatch{
		Destination: model.DestChaseChecking,
		AccountID:   "chk-1234",
		SourceFile:  "statements/chase_checking.csv",
		FileHash:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		IngestedAt:  now,
		RecordCount: 15,
		Payload:     []string{"rec1", "rec2"},
	}

	if batch.Destination != model.DestChaseChecking {
		t.Errorf("expected Destination %v, got %v", model.DestChaseChecking, batch.Destination)
	}
	if batch.AccountID != "chk-1234" {
		t.Errorf("expected AccountID 'chk-1234', got %q", batch.AccountID)
	}
	if batch.SourceFile != "statements/chase_checking.csv" {
		t.Errorf("expected SourceFile 'statements/chase_checking.csv', got %q", batch.SourceFile)
	}
	if batch.FileHash != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("expected FileHash matching SHA-256, got %q", batch.FileHash)
	}
	if !batch.IngestedAt.Equal(now) {
		t.Errorf("expected IngestedAt %v, got %v", now, batch.IngestedAt)
	}
	if batch.RecordCount != 15 {
		t.Errorf("expected RecordCount 15, got %d", batch.RecordCount)
	}
	if batch.Payload == nil {
		t.Fatal("expected non-nil Payload")
	}
}

func TestIngestionBatch_ZeroValue(t *testing.T) {
	var batch model.IngestionBatch

	if batch.Destination != "" {
		t.Errorf("expected empty Destination, got %q", batch.Destination)
	}
	if batch.AccountID != "" {
		t.Errorf("expected empty AccountID, got %q", batch.AccountID)
	}
	if batch.SourceFile != "" {
		t.Errorf("expected empty SourceFile, got %q", batch.SourceFile)
	}
	if batch.FileHash != "" {
		t.Errorf("expected empty FileHash, got %q", batch.FileHash)
	}
	if !batch.IngestedAt.IsZero() {
		t.Errorf("expected zero IngestedAt, got %v", batch.IngestedAt)
	}
	if batch.RecordCount != 0 {
		t.Errorf("expected 0 RecordCount, got %d", batch.RecordCount)
	}
	if batch.Payload != nil {
		t.Errorf("expected nil Payload, got %v", batch.Payload)
	}
}

func TestIngestionBatch_AsChaseTransactions(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

	t.Run("success", func(t *testing.T) {
		chaseTxs := []model.ChaseTransaction{
			{
				AccountID:   "chk-100",
				PostingDate: now,
				Description: "GROCERY STORE",
				Amount:      -54.20,
			},
		}

		batch := model.IngestionBatch{
			Destination: model.DestChaseChecking,
			AccountID:   "chk-100",
			RecordCount: len(chaseTxs),
			Payload:     chaseTxs,
		}

		txs, err := batch.AsChaseTransactions()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(txs) != 1 {
			t.Fatalf("expected 1 transaction, got %d", len(txs))
		}
		if txs[0].Description != "GROCERY STORE" {
			t.Errorf("expected description 'GROCERY STORE', got %q", txs[0].Description)
		}
	})

	t.Run("nil receiver", func(t *testing.T) {
		var nilBatch *model.IngestionBatch
		txs, err := nilBatch.AsChaseTransactions()
		if !errors.Is(err, model.ErrNilBatch) {
			t.Errorf("expected ErrNilBatch, got %v", err)
		}
		if txs != nil {
			t.Errorf("expected nil txs, got %v", txs)
		}
	})

	t.Run("zero count nil payload", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 0,
			Payload:     nil,
		}
		txs, err := batch.AsChaseTransactions()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if txs == nil || len(txs) != 0 {
			t.Errorf("expected empty slice, got %v", txs)
		}
	})

	t.Run("positive count nil payload", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 5,
			Payload:     nil,
		}
		_, err := batch.AsChaseTransactions()
		if !errors.Is(err, model.ErrNilPayload) {
			t.Errorf("expected ErrNilPayload, got %v", err)
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 1,
			Payload:     []string{"wrong_type"},
		}
		_, err := batch.AsChaseTransactions()
		if !errors.Is(err, model.ErrPayloadTypeMismatch) {
			t.Errorf("expected ErrPayloadTypeMismatch, got %v", err)
		}
	})
}

func TestIngestionBatch_AsFidelityPositions(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		positions := []model.FidelityPosition{
			{
				AccountID:    "fid-200",
				Symbol:       "FXAIX",
				Quantity:     50.0,
				CurrentValue: 9000.0,
			},
		}

		batch := model.IngestionBatch{
			Destination: model.DestFidelityPosition,
			AccountID:   "fid-200",
			RecordCount: len(positions),
			Payload:     positions,
		}

		pos, err := batch.AsFidelityPositions()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pos) != 1 {
			t.Fatalf("expected 1 position, got %d", len(pos))
		}
		if pos[0].Symbol != "FXAIX" {
			t.Errorf("expected symbol 'FXAIX', got %q", pos[0].Symbol)
		}
	})

	t.Run("nil receiver", func(t *testing.T) {
		var nilBatch *model.IngestionBatch
		pos, err := nilBatch.AsFidelityPositions()
		if !errors.Is(err, model.ErrNilBatch) {
			t.Errorf("expected ErrNilBatch, got %v", err)
		}
		if pos != nil {
			t.Errorf("expected nil pos, got %v", pos)
		}
	})

	t.Run("zero count nil payload", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 0,
			Payload:     nil,
		}
		pos, err := batch.AsFidelityPositions()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pos == nil || len(pos) != 0 {
			t.Errorf("expected empty slice, got %v", pos)
		}
	})

	t.Run("positive count nil payload", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 5,
			Payload:     nil,
		}
		_, err := batch.AsFidelityPositions()
		if !errors.Is(err, model.ErrNilPayload) {
			t.Errorf("expected ErrNilPayload, got %v", err)
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		batch := &model.IngestionBatch{
			RecordCount: 1,
			Payload:     12345,
		}
		_, err := batch.AsFidelityPositions()
		if !errors.Is(err, model.ErrPayloadTypeMismatch) {
			t.Errorf("expected ErrPayloadTypeMismatch, got %v", err)
		}
	})
}

func TestIngestionBatch_Validate(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		batch       *model.IngestionBatch
		expectedErr error
	}{
		{
			name:        "nil batch",
			batch:       nil,
			expectedErr: model.ErrNilBatch,
		},
		{
			name: "valid batch",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseCreditCard,
				AccountID:   "act-123",
				SourceFile:  "statements.csv",
				IngestedAt:  now,
				RecordCount: 10,
				Payload:     []model.ChaseTransaction{},
			},
			expectedErr: nil,
		},
		{
			name: "invalid destination",
			batch: &model.IngestionBatch{
				Destination: model.Destination("invalid_dest"),
				AccountID:   "act-123",
				SourceFile:  "statements.csv",
				IngestedAt:  now,
				RecordCount: 5,
				Payload:     []string{"data"},
			},
			expectedErr: model.ErrInvalidDestination,
		},
		{
			name: "missing account ID",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking,
				AccountID:   "",
				SourceFile:  "statements.csv",
				IngestedAt:  now,
				RecordCount: 5,
				Payload:     []string{"data"},
			},
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name: "missing source file",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking,
				AccountID:   "act-123",
				SourceFile:  "",
				IngestedAt:  now,
				RecordCount: 5,
				Payload:     []string{"data"},
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "missing ingested at",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking,
				AccountID:   "act-123",
				SourceFile:  "statements.csv",
				RecordCount: 5,
				Payload:     []string{"data"},
			},
			expectedErr: model.ErrMissingIngestedAt,
		},
		{
			name: "negative record count",
			batch: &model.IngestionBatch{
				Destination: model.DestFidelityPosition,
				AccountID:   "act-123",
				SourceFile:  "statements.csv",
				IngestedAt:  now,
				RecordCount: -1,
			},
			expectedErr: model.ErrNegativeRecordCount,
		},
		{
			name: "positive count with nil payload",
			batch: &model.IngestionBatch{
				Destination: model.DestFidelityPosition,
				AccountID:   "act-123",
				SourceFile:  "statements.csv",
				IngestedAt:  now,
				RecordCount: 10,
				Payload:     nil,
			},
			expectedErr: model.ErrNilPayload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.batch.Validate()
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}

func TestFileMetadata(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

	t.Run("Instantiation and zero value", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			FileHash:   "hash123",
			AccountID:  "acc-999",
			IngestedAt: now,
		}

		if meta.FileName != "test.csv" || meta.FileHash != "hash123" || meta.AccountID != "acc-999" || !meta.IngestedAt.Equal(now) {
			t.Errorf("unexpected populated metadata: %+v", meta)
		}

		var zeroMeta model.FileMetadata
		if zeroMeta.FileName != "" || zeroMeta.FileHash != "" || zeroMeta.AccountID != "" || !zeroMeta.IngestedAt.IsZero() {
			t.Errorf("unexpected zero metadata: %+v", zeroMeta)
		}
	})

	t.Run("Validate", func(t *testing.T) {
		tests := []struct {
			name        string
			meta        *model.FileMetadata
			expectedErr error
		}{
			{
				name:        "nil metadata",
				meta:        nil,
				expectedErr: model.ErrNilFileMetadata,
			},
			{
				name: "valid metadata",
				meta: &model.FileMetadata{
					FileName:   "test.csv",
					FileHash:   "hash123",
					AccountID:  "acc-123",
					IngestedAt: now,
				},
				expectedErr: nil,
			},
			{
				name: "missing file name",
				meta: &model.FileMetadata{
					FileName:   "",
					AccountID:  "acc-123",
					IngestedAt: now,
				},
				expectedErr: model.ErrMissingFileName,
			},
			{
				name: "missing account ID",
				meta: &model.FileMetadata{
					FileName:   "test.csv",
					AccountID:  "",
					IngestedAt: now,
				},
				expectedErr: model.ErrMissingAccountID,
			},
			{
				name: "missing ingested at",
				meta: &model.FileMetadata{
					FileName:  "test.csv",
					AccountID: "acc-123",
				},
				expectedErr: model.ErrMissingIngestedAt,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := tt.meta.Validate()
				if !errors.Is(err, tt.expectedErr) {
					t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
				}
			})
		}
	})
}

func TestSourceLineage_Instantiation(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	lineage := model.SourceLineage{
		FileName:   "data.csv",
		FileHash:   "abc123hash",
		RowNumber:  42,
		IngestedAt: now,
	}

	if lineage.FileName != "data.csv" {
		t.Errorf("expected FileName 'data.csv', got %q", lineage.FileName)
	}
	if lineage.FileHash != "abc123hash" {
		t.Errorf("expected FileHash 'abc123hash', got %q", lineage.FileHash)
	}
	if lineage.RowNumber != 42 {
		t.Errorf("expected RowNumber 42, got %d", lineage.RowNumber)
	}
	if !lineage.IngestedAt.Equal(now) {
		t.Errorf("expected IngestedAt %v, got %v", now, lineage.IngestedAt)
	}
}

func TestSourceLineage_ZeroValue(t *testing.T) {
	var lineage model.SourceLineage

	if lineage.FileName != "" {
		t.Errorf("expected empty FileName, got %q", lineage.FileName)
	}
	if lineage.FileHash != "" {
		t.Errorf("expected empty FileHash, got %q", lineage.FileHash)
	}
	if lineage.RowNumber != 0 {
		t.Errorf("expected 0 RowNumber, got %d", lineage.RowNumber)
	}
	if !lineage.IngestedAt.IsZero() {
		t.Errorf("expected zero IngestedAt, got %v", lineage.IngestedAt)
	}
}

func TestSourceLineage_Validate(t *testing.T) {
	tests := []struct {
		name        string
		lineage     *model.SourceLineage
		expectedErr error
	}{
		{
			name:        "nil lineage",
			lineage:     nil,
			expectedErr: model.ErrNilLineage,
		},
		{
			name: "valid lineage",
			lineage: &model.SourceLineage{
				FileName:  "statement.csv",
				RowNumber: 1,
			},
			expectedErr: nil,
		},
		{
			name: "missing file name",
			lineage: &model.SourceLineage{
				FileName:  "",
				RowNumber: 1,
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "negative row number",
			lineage: &model.SourceLineage{
				FileName:  "statement.csv",
				RowNumber: -1,
			},
			expectedErr: model.ErrNegativeRowNumber,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.lineage.Validate()
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}
