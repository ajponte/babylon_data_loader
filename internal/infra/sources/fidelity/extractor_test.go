package fidelity_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/infra/sources/fidelity"
)

func TestFidelityExtractor_SourceType(t *testing.T) {
	extractor := fidelity.NewExtractor()
	if extractor.SourceType() != "fidelity" {
		t.Errorf("expected SourceType 'fidelity', got %q", extractor.SourceType())
	}
}

func TestFidelityExtractor_Extract_Fixture(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "Portfolio_Positions_Synthetic.csv",
		FileHash:   "hash_fid_synthetic",
		AccountID:  "SYNTH-DEFAULT",
		IngestedAt: now,
	}

	fixturePath := filepath.Join("..", "..", "..", "..", "testdata", "fidelity", "Portfolio_Positions_Synthetic.csv")
	file, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture file %s: %v", fixturePath, err)
	}
	defer file.Close()

	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, file, meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	if batch.Destination != model.DestFidelityPosition {
		t.Errorf("expected destination %q, got %q", model.DestFidelityPosition, batch.Destination)
	}
	if batch.RecordCount != 5 {
		t.Fatalf("expected 5 records extracted from fixture, got %d", batch.RecordCount)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(positions) != 5 {
		t.Fatalf("expected 5 positions, got %d", len(positions))
	}

	// 1. SPAXX** (Cash market holding)
	spaxx := positions[0]
	if spaxx.AccountID != "SYNTH-ROTH-101" {
		t.Errorf("expected AccountID 'SYNTH-ROTH-101', got %q", spaxx.AccountID)
	}
	if spaxx.Symbol != "SPAXX**" {
		t.Errorf("expected Symbol 'SPAXX**', got %q", spaxx.Symbol)
	}
	if spaxx.Description != "HELD IN MONEY MARKET" {
		t.Errorf("expected Description 'HELD IN MONEY MARKET', got %q", spaxx.Description)
	}
	if spaxx.CurrentValue != 3250.00 {
		t.Errorf("expected CurrentValue 3250.00, got %f", spaxx.CurrentValue)
	}
	if spaxx.Quantity != 0.0 {
		t.Errorf("expected Quantity 0.0, got %f", spaxx.Quantity)
	}

	// 2. FSKAX
	fskax := positions[1]
	if fskax.Symbol != "FSKAX" {
		t.Errorf("expected Symbol 'FSKAX', got %q", fskax.Symbol)
	}
	if fskax.Quantity != 45.250 {
		t.Errorf("expected Quantity 45.250, got %f", fskax.Quantity)
	}
	if fskax.LastPrice != 150.00 {
		t.Errorf("expected LastPrice 150.00, got %f", fskax.LastPrice)
	}
	if fskax.CurrentValue != 6787.50 {
		t.Errorf("expected CurrentValue 6787.50, got %f", fskax.CurrentValue)
	}
	if fskax.CostBasis != 6000.00 {
		t.Errorf("expected CostBasis 6000.00, got %f", fskax.CostBasis)
	}

	// 5. Zero / Cash Position (**)
	zeroPos := positions[4]
	if zeroPos.AccountID != "SYNTH-TAX-303" {
		t.Errorf("expected AccountID 'SYNTH-TAX-303', got %q", zeroPos.AccountID)
	}
	if zeroPos.Symbol != "**" {
		t.Errorf("expected Symbol '**', got %q", zeroPos.Symbol)
	}
	if zeroPos.CurrentValue != 0.0 {
		t.Errorf("expected CurrentValue 0.0, got %f", zeroPos.CurrentValue)
	}
}

func TestFidelityExtractor_Errors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := fidelity.NewExtractor()

	t.Run("invalid metadata", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, strings.NewReader(""), meta)
		if err == nil {
			t.Error("expected error with missing account ID in metadata")
		}
	})

	t.Run("nil reader", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, nil, meta)
		if err == nil {
			t.Error("expected error with nil reader")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		meta := model.FileMetadata{
			FileName:   "test.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(canceledCtx, strings.NewReader("Account number,Symbol\n"), meta)
		if err == nil {
			t.Error("expected error with cancelled context")
		}
	})

	t.Run("empty stream", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "empty.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		batch, err := extractor.Extract(ctx, strings.NewReader(""), meta)
		if err != nil {
			t.Fatalf("unexpected error on empty stream: %v", err)
		}
		if batch.RecordCount != 0 {
			t.Errorf("expected 0 records for empty stream, got %d", batch.RecordCount)
		}
	})
}

func TestFidelityExtractor_RejectArbitraryCSV(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "arbitrary.csv",
		FileHash:   "hash_arb",
		AccountID:  "acc-arb",
		IngestedAt: now,
	}

	// Arbitrary Chase CSV lacking Fidelity signature columns
	chaseCSV := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,10/15/2026,"GROCERY STORE",-85.50,DEBIT,2414.50,
`
	extractor := fidelity.NewExtractor()
	_, err := extractor.Extract(ctx, strings.NewReader(chaseCSV), meta)
	if err == nil {
		t.Fatal("expected Extract error for arbitrary CSV missing Fidelity columns, got nil")
	}

	if !strings.Contains(err.Error(), "fidelity CSV missing required header column") {
		t.Errorf("expected error to mention missing required header column, got: %v", err)
	}
}

func TestFidelityExtractor_FilterSummaryTotals(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "positions_with_totals.csv",
		FileHash:   "hash_totals",
		AccountID:  "FID-123",
		IngestedAt: now,
	}

	csvData := `Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
FID-123,Individual,AAPL,APPLE INC,10,150.00,$1500.00,$1200.00
FID-123,Individual,,HELD IN MONEY MARKET,,,$500.00,$500.00
FID-123,Individual,Total,Account Total,,, $2000.00, $1700.00
Total Market Value,,,,,,$2000.00,
Account Total,,,,,,$2000.00,
"The data and information is proprietary",,,,,,,
`
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	// Only AAPL and the CASH row should be extracted; summary totals and disclaimers excluded
	if len(positions) != 2 {
		t.Fatalf("expected exactly 2 positions (excluding totals and disclaimer), got %d", len(positions))
	}

	if positions[0].Symbol != "AAPL" {
		t.Errorf("expected first position symbol 'AAPL', got %q", positions[0].Symbol)
	}
	if positions[1].Symbol != "CASH" {
		t.Errorf("expected second position symbol 'CASH', got %q", positions[1].Symbol)
	}
}
