package port

import (
	"context"

	"babylon/dataloader/internal/domain/model"
)

// Repository is the driven port for persisting batches and audit logs to storage.
type Repository interface {
	// SaveBatch persists an ingestion batch to the appropriate storage collection.
	SaveBatch(ctx context.Context, batch *model.IngestionBatch) (*model.SyncLog, error)
	// LogSync records an audit log entry for an ingestion run.
	LogSync(ctx context.Context, log *model.SyncLog) error
}
