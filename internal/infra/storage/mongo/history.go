package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
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

	opts := buildSyncLogFindOptions(limit)
	cursor, err := r.db.Collection(dataSyncCollection).Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo: find sync logs in %s: %w", dataSyncCollection, err)
	}

	return decodeSyncLogs(ctx, cursor)
}

// syncLogSortDesc returns the find sort specification for descending chronological order.
func syncLogSortDesc() bson.D {
	return bson.D{{Key: fieldSyncedAt, Value: -1}}
}

// buildSyncLogFindOptions builds find options for sync log query with default limit clamping and descending sort by syncedAt.
func buildSyncLogFindOptions(limit int64) *options.FindOptions {
	if limit <= 0 {
		limit = DefaultSyncLogLimit
	}
	return options.Find().SetSort(syncLogSortDesc()).SetLimit(limit)
}

// decodeSyncLogs drains the given cursor and decodes documents into a slice of model.SyncLog.
func decodeSyncLogs(ctx context.Context, cursor *mongodrv.Cursor) ([]model.SyncLog, error) {
	if cursor == nil {
		return []model.SyncLog{}, nil
	}
	// Safely close cursor with context.Background() to release server resources even if ctx was cancelled.
	defer func() {
		_ = cursor.Close(context.Background())
	}()

	var docs []syncLogDoc
	if decodeErr := cursor.All(ctx, &docs); decodeErr != nil {
		return nil, fmt.Errorf("mongo: decode sync logs from %s: %w", dataSyncCollection, decodeErr)
	}

	return transformSyncLogs(docs), nil
}

// transformSyncLogs projects a slice of persistence DTOs into domain model.SyncLog entities.
func transformSyncLogs(docs []syncLogDoc) []model.SyncLog {
	logs := make([]model.SyncLog, len(docs))
	for i, d := range docs {
		logs[i] = fromSyncLogDoc(d)
	}
	return logs
}

// fromSyncLogDoc translates a persistence DTO to a domain SyncLog, normalizing legacy
// desktop fields with canonical-first precedence: canonical fields override legacy dual fields,
// missing status defaults to SUCCESS, and non-UTC timestamps are converted to UTC.
func fromSyncLogDoc(d syncLogDoc) model.SyncLog {
	recordCount, upsertedCount := normalizeRecordCounts(d.RecordCount, d.UpsertedCount, d.DualRecordsUploaded)
	return model.SyncLog{
		FileName:       d.FileName,
		FileHash:       d.FileHash,
		CollectionName: normalizeCollectionName(d.CollectionName, d.DualCollectionName),
		RecordCount:    recordCount,
		UpsertedCount:  upsertedCount,
		MatchedCount:   d.MatchedCount,
		Status:         normalizeSyncStatus(d.Status),
		ErrorMessage:   d.ErrorMessage,
		SyncedAt:       normalizeSyncedAt(d.SyncedAt, d.DualSyncTimestamp),
		DurationMs:     d.DurationMs,
	}
}

// normalizeRecordCounts returns canonical (recordCount, upsertedCount), falling back
// to dual records uploaded when canonical record count is zero and legacy records uploaded > 0.
// In legacy desktop datalake records, records_uploaded represented both total and upserted counts.
func normalizeRecordCounts(primaryRecord, primaryUpserted, legacyUploaded int64) (int64, int64) {
	if primaryRecord == 0 && legacyUploaded > 0 {
		return legacyUploaded, legacyUploaded
	}
	return primaryRecord, primaryUpserted
}

// normalizeCollectionName returns primary collection name if non-empty, otherwise legacy.
func normalizeCollectionName(primary, legacy string) string {
	if primary != "" {
		return primary
	}
	return legacy
}

// normalizeSyncStatus returns SyncStatus from raw string, defaulting to SyncStatusSuccess when empty.
func normalizeSyncStatus(raw string) model.SyncStatus {
	if raw == "" {
		return model.SyncStatusSuccess
	}
	return model.SyncStatus(raw)
}

// normalizeSyncedAt returns the UTC timestamp from primary, falling back to legacy UTC timestamp if primary is zero.
func normalizeSyncedAt(primary, legacy time.Time) time.Time {
	if !primary.IsZero() {
		return primary.UTC()
	}
	if !legacy.IsZero() {
		return legacy.UTC()
	}
	return time.Time{}
}
