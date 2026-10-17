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

// Repository defines the interface for data storage and audit operations.
type Repository interface {
	BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) (UpsertStats, error)
	BulkUpsertChaseTransactions(ctx context.Context, transactions []model.ChaseTransaction) (UpsertStats, error)
	BulkUpsertFidelityPositions(ctx context.Context, positions []model.FidelityPosition) (UpsertStats, error)
	RecordSyncLog(ctx context.Context, log model.SyncLog) error
	GetSyncLogs(ctx context.Context) ([]model.SyncLog, error)
}
