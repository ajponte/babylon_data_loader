package chase

import (
	"context"
	"errors"
	"fmt"
	"io"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

var (
	// ErrNilReader indicates that a nil io.Reader was supplied.
	ErrNilReader = errors.New("reader cannot be nil")

	// ErrUnknownDialect indicates that the Chase statement header did not match a known dialect.
	ErrUnknownDialect = errors.New("unsupported or unknown Chase statement dialect")

	// ErrMissingRequiredColumns indicates that required header columns are missing from the statement.
	ErrMissingRequiredColumns = errors.New("chase CSV missing required header columns")
)

// Extractor implements port.Extractor for Chase checking and credit card statements.
type Extractor struct {
	classifier Classifier
}

// NewExtractor constructs a new Chase Extractor.
// If classifier is nil, DefaultClassifier is used.
func NewExtractor(c Classifier) port.Extractor {
	if c == nil {
		c = NewDefaultClassifier()
	}
	return &Extractor{classifier: c}
}

// SourceType returns the unique source identifier for Chase statements.
func (e *Extractor) SourceType() string {
	return "chase"
}

// Extract parses raw CSV data from r, classifies dialect, and returns an IngestionBatch.
func (e *Extractor) Extract(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) (*model.IngestionBatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if r == nil {
		return nil, ErrNilReader
	}

	dialect, stream, err := e.classifier.Classify(ctx, r)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to classify chase statement dialect: %w", err)
	}

	var (
		records []model.ChaseTransaction
		dest    model.Destination
	)

	switch dialect {
	case DialectChecking:
		dest = model.DestChaseChecking
		records, err = parseChecking(ctx, stream, meta)
	case DialectCreditCard:
		dest = model.DestChaseCreditCard
		records, err = parseCreditCard(ctx, stream, meta)
	case DialectUnknown:
		return nil, fmt.Errorf("%w: %s", ErrUnknownDialect, dialect)
	default:
		return nil, fmt.Errorf("%w: unrecognized dialect %q", ErrUnknownDialect, dialect)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, err
	}

	batch := &model.IngestionBatch{
		Destination: dest,
		AccountID:   meta.AccountID,
		SourceFile:  meta.FileName,
		FileHash:    meta.FileHash,
		IngestedAt:  meta.IngestedAt,
		RecordCount: len(records),
		Payload:     records,
	}

	if valErr := batch.Validate(); valErr != nil {
		return nil, fmt.Errorf("generated batch failed validation: %w", valErr)
	}

	return batch, nil
}
