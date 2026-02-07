package repository

import (
	"context"

	"babylon/dataloader/datalake/model"
)

// Repository defines the interface for data storage operations.
type Repository interface {
	BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) error
}
