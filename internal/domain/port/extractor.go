package port

import (
	"context"
	"io"

	"babylon/dataloader/internal/domain/model"
)

// Extractor is the driving port for statement parsers and data generators.
type Extractor interface {
	// SourceType returns the unique identifier for the extractor source type.
	SourceType() string
	// Extract parses raw data from the reader and returns a normalized IngestionBatch.
	Extract(ctx context.Context, r io.Reader, meta model.SourceLineage) (*model.IngestionBatch, error)
}
