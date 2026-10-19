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

	// ErrMalformedHeader indicates that the header row is structurally invalid, for example because
	// a column contains an embedded line break produced by CR-only line endings.
	ErrMalformedHeader = errors.New("chase CSV header is malformed")

	// ErrDuplicateColumn indicates that a required column appears more than once in the header.
	ErrDuplicateColumn = errors.New("chase CSV header contains a duplicate required column")

	// ErrMalformedCSV indicates a CSV syntax error. It wraps the underlying *csv.ParseError,
	// which carries the line and column of the failure.
	ErrMalformedCSV = errors.New("chase CSV is malformed")

	// ErrInvalidRow indicates that a data row was rejected. It wraps the row-level cause, which
	// matches the domain sentinels (model.ErrInvalidAmount, model.ErrMissingDescription, ...)
	// where one applies. Callers can use it to tell bad input data apart from I/O failures.
	ErrInvalidRow = errors.New("chase CSV row is invalid")
)

var _ port.Extractor = (*Extractor)(nil)

// Extractor implements port.Extractor for Chase checking and credit card statements.
// The zero value and a nil *Extractor are usable and fall back to DefaultClassifier.
type Extractor struct {
	classifier Classifier
}

// NewExtractor constructs a new Chase Extractor.
// If classifier is nil, DefaultClassifier is used.
func NewExtractor(c Classifier) *Extractor {
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
// A nil ctx is treated as context.Background(). This is the only place a nil context is
// normalized, so classifiers and parsers always receive a non-nil context.
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
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if r == nil {
		return nil, ErrNilReader
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	return ctx, nil
}

func (e *Extractor) classifyStream(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return DialectUnknown, nil, ctxErr
	}

	dialect, stream, err := e.classifierOrDefault().Classify(ctx, r)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return DialectUnknown, nil, ctxErr
		}
		if errors.Is(err, context.Canceled) {
			return DialectUnknown, nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return DialectUnknown, nil, context.DeadlineExceeded
		}
		return DialectUnknown, nil, fmt.Errorf("failed to classify chase statement dialect: %w", err)
	}

	if stream == nil {
		return DialectUnknown, nil, fmt.Errorf("classifier returned nil reader stream for dialect %s", dialect)
	}

	return dialect, stream, nil
}

// classifierOrDefault returns the configured Classifier, falling back to DefaultClassifier so
// that a nil or zero-value *Extractor does not panic.
func (e *Extractor) classifierOrDefault() Classifier {
	if e == nil || e.classifier == nil {
		return &DefaultClassifier{}
	}
	return e.classifier
}

type dialectParser func(ctx context.Context, r io.Reader, meta model.FileMetadata) ([]model.ChaseTransaction, error)

// dialectRoute pairs a dialect with its parser and destination. Both Chase dialects share one
// datalake collection (model.DestChaseChecking == model.DestChaseCreditCard), so the dialect is
// observable only through the payload fields each parser populates.
type dialectRoute struct {
	destination model.Destination
	parser      dialectParser
}

func resolveDialectRoute(dialect DialectType) (dialectRoute, bool) {
	switch dialect {
	case DialectChecking:
		return dialectRoute{
			destination: model.DestChaseChecking,
			parser:      parseChecking,
		}, true
	case DialectCreditCard:
		return dialectRoute{
			destination: model.DestChaseCreditCard,
			parser:      parseCreditCard,
		}, true
	case DialectUnknown:
		return dialectRoute{}, false
	default:
		return dialectRoute{}, false
	}
}

func parseDialect(
	ctx context.Context,
	dialect DialectType,
	stream io.Reader,
	meta model.FileMetadata,
) (model.Destination, []model.ChaseTransaction, error) {
	route, ok := resolveDialectRoute(dialect)
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
