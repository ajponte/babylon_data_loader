package synthetic

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

const (
	defaultSeed = 42
	maxAmount   = 500.0
	baseBalance = 5000.0
	centsFactor = 100.0

	// MaxSyntheticRowCount defines the upper safety bound on synthetic generation.
	MaxSyntheticRowCount = 100_000

	// maxInitialBatchCapacity bounds the upfront slice pre-allocation to prevent large memory spikes.
	maxInitialBatchCapacity = 10_000
)

// GeneratorOptions configures the generation of synthetic transactions.
type GeneratorOptions struct {
	RowCount int
	Seed     int64
}

// Extractor implements port.Extractor by generating mock ChaseTransaction batches.
type Extractor struct {
	opts GeneratorOptions
	mu   sync.Mutex
	rng  *rand.Rand
}

// NewExtractor constructs a new synthetic Extractor with the provided generation options.
func NewExtractor(opts GeneratorOptions) port.Extractor {
	seed := opts.Seed
	if seed == 0 {
		seed = defaultSeed
	}

	//nolint:gosec // G404: Use of math/rand/v2 is acceptable for non-cryptographic synthetic test data.
	rng := rand.New(rand.NewPCG(uint64(seed), 1))
	return &Extractor{
		opts: opts,
		rng:  rng,
	}
}

// SourceType returns the unique source identifier for synthetic data.
func (e *Extractor) SourceType() string {
	return "synthetic"
}

// Extract generates synthetic ChaseTransaction records wrapped in an IngestionBatch.
func (e *Extractor) Extract(
	ctx context.Context,
	_ io.Reader,
	meta model.FileMetadata,
) (*model.IngestionBatch, error) {
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	count := e.opts.RowCount
	if count < 0 {
		count = 0
	}
	if count > MaxSyntheticRowCount {
		return nil, fmt.Errorf("row count %d exceeds maximum allowable limit of %d", count, MaxSyntheticRowCount)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	records := make([]model.ChaseTransaction, 0, min(count, maxInitialBatchCapacity))
	currentBalance := baseBalance

	for i := range count {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		rowNumber := i + 1
		amt := math.Round((e.rng.Float64()*maxAmount-maxAmount/2.0)*centsFactor) / centsFactor
		if amt == 0 {
			amt = 10.00
		}
		currentBalance += amt
		balanceCopy := math.Round(currentBalance*centsFactor) / centsFactor

		txType := "DEBIT"
		details := "DEBIT"
		if amt > 0 {
			txType = "CREDIT"
			details = "CREDIT"
		}

		postingDate := time.Date(
			meta.IngestedAt.Year(),
			meta.IngestedAt.Month(),
			meta.IngestedAt.Day(),
			0, 0, 0, 0, time.UTC,
		).AddDate(0, 0, -i)

		tx := model.ChaseTransaction{
			Lineage: model.SourceLineage{
				FileName:   meta.FileName,
				FileHash:   meta.FileHash,
				RowNumber:  rowNumber,
				IngestedAt: meta.IngestedAt,
			},
			AccountID:      meta.AccountID,
			PostingDate:    postingDate,
			Description:    fmt.Sprintf("SYNTHETIC TRANSACTION %04d", rowNumber),
			Details:        details,
			Amount:         amt,
			Type:           txType,
			Balance:        &balanceCopy,
			CheckOrSlipNum: "",
			Category:       "Synthetic",
			Memo:           "Programmatically generated test transaction",
		}

		if valErr := tx.Validate(); valErr != nil {
			return nil, fmt.Errorf("synthetic transaction validation failed (row %d): %w", rowNumber, valErr)
		}

		records = append(records, tx)
	}

	batch := &model.IngestionBatch{
		Destination: model.DestSynthetic,
		AccountID:   meta.AccountID,
		SourceFile:  meta.FileName,
		FileHash:    meta.FileHash,
		IngestedAt:  meta.IngestedAt,
		RecordCount: len(records),
		Payload:     records,
	}

	if valErr := batch.Validate(); valErr != nil {
		return nil, fmt.Errorf("generated synthetic batch failed validation: %w", valErr)
	}

	return batch, nil
}
