package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"babylon/dataloader/internal/domain/model"
)

func dataSyncIndexes() []indexSpec {
	return []indexSpec{
		{name: "idx_datasync_synced_at", keys: bson.D{{Key: "syncedAt", Value: -1}}},
		{
			name: "idx_datasync_filename_synced_at",
			keys: bson.D{{Key: "fileName", Value: 1}, {Key: "syncedAt", Value: -1}},
		},
	}
}

type syncLogDoc struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	FileName       string             `bson:"fileName"`
	FileHash       string             `bson:"fileHash,omitempty"`
	CollectionName string             `bson:"collectionName,omitempty"`
	RecordCount    int64              `bson:"recordCount"`
	UpsertedCount  int64              `bson:"upsertedCount"`
	MatchedCount   int64              `bson:"matchedCount"`
	Status         string             `bson:"status"`
	ErrorMessage   string             `bson:"errorMessage,omitempty"`
	SyncedAt       time.Time          `bson:"syncedAt"`
	DurationMs     int64              `bson:"durationMs"`

	DualCollectionName  string    `bson:"collection_name,omitempty"`
	DualSyncTimestamp   time.Time `bson:"sync_timestamp,omitempty"`
	DualRecordsUploaded int64     `bson:"records_uploaded,omitempty"`
}

func toSyncLogDoc(log *model.SyncLog) syncLogDoc {
	utcSyncedAt := log.SyncedAt.UTC()
	return syncLogDoc{
		FileName:            log.FileName,
		FileHash:            log.FileHash,
		CollectionName:      log.CollectionName,
		RecordCount:         log.RecordCount,
		UpsertedCount:       log.UpsertedCount,
		MatchedCount:        log.MatchedCount,
		Status:              string(log.Status),
		ErrorMessage:        log.ErrorMessage,
		SyncedAt:            utcSyncedAt,
		DurationMs:          log.DurationMs,
		DualCollectionName:  log.CollectionName,
		DualSyncTimestamp:   utcSyncedAt,
		DualRecordsUploaded: log.UpsertedCount,
	}
}

// LogSync writes a synchronization audit log to the dataSync collection.
func (r *Repository) LogSync(ctx context.Context, log *model.SyncLog) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if log == nil {
		return model.ErrNilSyncLog
	}
	if err := log.Validate(); err != nil {
		return fmt.Errorf("mongo: invalid sync log: %w", err)
	}
	if err := r.ensureIndexes(ctx, dataSyncCollection); err != nil {
		return err
	}
	doc := toSyncLogDoc(log)
	if _, err := r.db.Collection(dataSyncCollection).InsertOne(ctx, doc); err != nil {
		return fmt.Errorf("mongo: log sync to %s: %w", dataSyncCollection, err)
	}
	return nil
}
