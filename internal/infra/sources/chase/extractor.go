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
	ctx, err := validateExtractParams(ctx, r, meta)
	if err != nil {
		return nil, err
	}

	dialect, stream, err := e.classifyStream(ctx, r)
	if err != nil {
		return nil, err
	}

	dest, records, err := parseDialect(ctx, dialect, stream, meta)
	if err != nil {
		return nil, err
	}

	return buildBatch(dest, meta, records)
}

func validateExtractParams(ctx context.Context, r io.Reader, meta model.FileMetadata) (context.Context, error) {
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
	return ctx, nil
}

func (e *Extractor) classifyStream(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
	dialect, stream, err := e.classifier.Classify(ctx, r)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return DialectUnknown, nil, err
		}
		return DialectUnknown, nil, fmt.Errorf("failed to classify chase statement dialect: %w", err)
	}
	return dialect, stream, nil
}

type dialectParser func(ctx context.Context, r io.Reader, meta model.FileMetadata) ([]model.ChaseTransaction, error)

type dialectRoute struct {
	destination model.Destination
	parser      dialectParser
}

//nolint:gochecknoglobals,exhaustive // immutable strategy route table initialized at package load; DialectUnknown handled dynamically in parseDialect
var dialectRoutes = map[DialectType]dialectRoute{
	DialectChecking: {
		destination: model.DestChaseChecking,
		parser:      parseChecking,
	},
	DialectCreditCard: {
		destination: model.DestChaseCreditCard,
		parser:      parseCreditCard,
	},
}

func parseDialect(
	ctx context.Context,
	dialect DialectType,
	stream io.Reader,
	meta model.FileMetadata,
) (model.Destination, []model.ChaseTransaction, error) {
	route, ok := dialectRoutes[dialect]
	if !ok {
		if dialect == DialectUnknown {
			return "", nil, fmt.Errorf("%w: %s", ErrUnknownDialect, dialect)
		}
		return "", nil, fmt.Errorf("%w: unrecognized dialect %q", ErrUnknownDialect, dialect)
	}

	records, err := route.parser(ctx, stream, meta)
	if err != nil {
		return "", nil, err
	}

	return route.destination, records, nil
}

func buildBatch(
	dest model.Destination,
	meta model.FileMetadata,
	records []model.ChaseTransaction,
) (*model.IngestionBatch, error) {
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
