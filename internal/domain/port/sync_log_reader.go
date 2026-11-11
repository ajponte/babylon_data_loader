package port

import (
	"context"

	"babylon/dataloader/internal/domain/model"
)

// SyncLogReader is the driven read port for ingestion audit history.
// Concrete implementations (such as mongo.Repository) retrieve audit records
// from the dataSync datalake collection.
type SyncLogReader interface {
	// ListSyncLogs returns up to limit audit records, newest first.
	// A non-positive limit (limit <= 0) defaults to the adapter default (100).
	ListSyncLogs(ctx context.Context, limit int64) ([]model.SyncLog, error)
}
