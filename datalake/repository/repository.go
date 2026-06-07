package repository

import (
	"context"

	"babylon/dataloader/datalake/model"
)

type UpsertStats struct {
	UpsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
}

// Repository defines the interface for data storage operations.
type Repository interface {
	BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) (UpsertStats, error)
	GetSyncLogs(ctx context.Context) ([]model.SyncLog, error)
}
