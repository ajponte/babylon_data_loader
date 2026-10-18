package synthetic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/infra/sources/synthetic"
)

func TestSyntheticExtractor_SourceType(t *testing.T) {
	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 10, Seed: 42})
	if extractor.SourceType() != "synthetic" {
		t.Errorf("expected SourceType 'synthetic', got %q", extractor.SourceType())
	}
}

func TestSyntheticExtractor_Extract_RowCount(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "synthetic_test.csv",
		FileHash:   "hash_synth",
		AccountID:  "synth-acc-1",
		IngestedAt: now,
	}

	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{
		RowCount: 25,
		Seed:     12345,
	})

	batch, err := extractor.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	if batch.Destination != model.DestSynthetic {
		t.Errorf("expected Destination %q, got %q", model.DestSynthetic, batch.Destination)
	}
	if batch.RecordCount != 25 {
		t.Errorf("expected RecordCount 25, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 25 {
		t.Fatalf("expected 25 transactions, got %d", len(txs))
	}

	for i, tx := range txs {
		if tx.AccountID != "synth-acc-1" {
			t.Errorf("tx[%d] expected AccountID 'synth-acc-1', got %q", i, tx.AccountID)
		}
		if tx.Lineage.RowNumber != i+1 {
			t.Errorf("tx[%d] expected RowNumber %d, got %d", i, i+1, tx.Lineage.RowNumber)
		}
		if tx.Balance == nil {
			t.Errorf("tx[%d] expected non-nil Balance", i)
		}
		if tx.Amount == 0 {
			t.Errorf("tx[%d] expected non-zero Amount", i)
		}
	}
}

func TestSyntheticExtractor_DeterministicReproducibility(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "synthetic_test.csv",
		FileHash:   "hash_synth",
		AccountID:  "synth-acc-1",
		IngestedAt: now,
	}

	const fixedSeed = int64(987654)
	ext1 := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 10, Seed: fixedSeed})
	ext2 := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 10, Seed: fixedSeed})
	ext3 := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 10, Seed: 111111})

	batch1, err := ext1.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected ext1 Extract error: %v", err)
	}
	batch2, err := ext2.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected ext2 Extract error: %v", err)
	}
	batch3, err := ext3.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected ext3 Extract error: %v", err)
	}

	txs1, _ := batch1.AsChaseTransactions()
	txs2, _ := batch2.AsChaseTransactions()
	txs3, _ := batch3.AsChaseTransactions()

	for i := range txs1 {
		if txs1[i].Amount != txs2[i].Amount {
			t.Errorf("tx[%d] amount mismatch between identical seeds: %f != %f", i, txs1[i].Amount, txs2[i].Amount)
		}
		if *txs1[i].Balance != *txs2[i].Balance {
			t.Errorf("tx[%d] balance mismatch between identical seeds: %f != %f", i, *txs1[i].Balance, *txs2[i].Balance)
		}
	}

	// Verify that different seed produces different values
	identicalToSeed3 := true
	for i := range txs1 {
		if txs1[i].Amount != txs3[i].Amount {
			identicalToSeed3 = false
			break
		}
	}
	if identicalToSeed3 {
		t.Error("expected different seeds to produce different transactions, but amounts matched")
	}
}

func TestSyntheticExtractor_ZeroCount(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "synthetic_zero.csv",
		FileHash:   "hash_zero",
		AccountID:  "acc-zero",
		IngestedAt: now,
	}

	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 0, Seed: 42})
	batch, err := extractor.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected error with RowCount 0: %v", err)
	}

	if batch.RecordCount != 0 {
		t.Errorf("expected 0 RecordCount, got %d", batch.RecordCount)
	}
	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert empty payload: %v", err)
	}
	if len(txs) != 0 {
		t.Errorf("expected 0 transactions, got %d", len(txs))
	}
}

func TestSyntheticExtractor_Errors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 5, Seed: 42})

	t.Run("invalid metadata", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, nil, meta)
		if err == nil {
			t.Error("expected error with missing account ID")
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
		_, err := extractor.Extract(canceledCtx, nil, meta)
		if err == nil {
			t.Error("expected error with cancelled context")
		}
	})
}

func TestSyntheticExtractor_RNGContinuity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "synthetic_continuity.csv",
		FileHash:   "hash_cont",
		AccountID:  "synth-cont-1",
		IngestedAt: now,
	}

	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: 10, Seed: 42})

	batch1, err := extractor.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected Extract batch 1 error: %v", err)
	}

	batch2, err := extractor.Extract(ctx, nil, meta)
	if err != nil {
		t.Fatalf("unexpected Extract batch 2 error: %v", err)
	}

	txs1, err := batch1.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast batch 1: %v", err)
	}
	txs2, err := batch2.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast batch 2: %v", err)
	}

	// Batches generated sequentially by the same extractor must NOT be identical
	identical := true
	for i := range txs1 {
		if txs1[i].Amount != txs2[i].Amount {
			identical = false
			break
		}
	}
	if identical {
		t.Error("expected consecutive Extract() calls to yield non-identical transaction sequences")
	}
}

func TestSyntheticExtractor_RowCountExceeded(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "synthetic_limit.csv",
		FileHash:   "hash_limit",
		AccountID:  "synth-limit-1",
		IngestedAt: now,
	}

	extractor := synthetic.NewExtractor(synthetic.GeneratorOptions{
		RowCount: synthetic.MaxSyntheticRowCount + 1,
		Seed:     42,
	})

	_, err := extractor.Extract(ctx, nil, meta)
	if err == nil {
		t.Fatalf("expected error when RowCount exceeds %d, got nil", synthetic.MaxSyntheticRowCount)
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowable limit") {
		t.Errorf("expected error to mention maximum allowable limit, got: %v", err)
	}
}
