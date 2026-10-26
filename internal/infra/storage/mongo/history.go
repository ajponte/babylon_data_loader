package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"babylon/dataloader/internal/domain/model"
)

// DefaultSyncLogLimit defines the default maximum number of audit records returned.
const DefaultSyncLogLimit int64 = 100

// ListSyncLogs returns up to limit audit records sorted by syncedAt descending.
// limit <= 0 uses DefaultSyncLogLimit (legacy desktop behavior; unbounded reads are not offered).
// Reads do not mutate storage and do not trigger index creation.
func (r *Repository) ListSyncLogs(ctx context.Context, limit int64) ([]model.SyncLog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultSyncLogLimit
	}

	opts := options.Find().SetSort(bson.D{{Key: fieldSyncedAt, Value: -1}}).SetLimit(limit)
	cursor, err := r.db.Collection(dataSyncCollection).Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo: find sync logs in %s: %w", dataSyncCollection, err)
	}
	if cursor != nil {
		defer cursor.Close(ctx)
	}

	var docs []syncLogDoc
	if decodeErr := cursor.All(ctx, &docs); decodeErr != nil {
		return nil, fmt.Errorf("mongo: decode sync logs from %s: %w", dataSyncCollection, decodeErr)
	}

	logs := make([]model.SyncLog, len(docs))
	for i, d := range docs {
		logs[i] = fromSyncLogDoc(d)
	}
	return logs, nil
}

func fromSyncLogDoc(d syncLogDoc) model.SyncLog {
	syncedAt := d.SyncedAt
	if syncedAt.IsZero() && !d.DualSyncTimestamp.IsZero() {
		syncedAt = d.DualSyncTimestamp.UTC()
	} else if !syncedAt.IsZero() {
		syncedAt = syncedAt.UTC()
	}

	collectionName := d.CollectionName
	if collectionName == "" && d.DualCollectionName != "" {
		collectionName = d.DualCollectionName
	}

	recordCount := d.RecordCount
	upsertedCount := d.UpsertedCount
	if recordCount == 0 && d.DualRecordsUploaded > 0 {
		recordCount = d.DualRecordsUploaded
		upsertedCount = d.DualRecordsUploaded
	}

	status := model.SyncStatus(d.Status)
	if status == "" {
		status = model.SyncStatusSuccess
	}

	return model.SyncLog{
		FileName:       d.FileName,
		FileHash:       d.FileHash,
		CollectionName: collectionName,
		RecordCount:    recordCount,
		UpsertedCount:  upsertedCount,
		MatchedCount:   d.MatchedCount,
		Status:         status,
		ErrorMessage:   d.ErrorMessage,
		SyncedAt:       syncedAt,
		DurationMs:     d.DurationMs,
	}
}
