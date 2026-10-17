package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SyncStatus represents the operational execution state of an ingestion task.
type SyncStatus string

const (
	SyncStatusSuccess SyncStatus = "SUCCESS"
	SyncStatusFailed  SyncStatus = "FAILED"
)

// SyncLog captures the definitive ingestion audit record in the dataSync collection.
// It complies with the authoritative specification in docs/specs/mongodb-atlas-datalake.md
// while providing dual-field backward compatibility for legacy BSON readers and UI bindings.
type SyncLog struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"            json:"id,omitempty"`
	FileName       string             `bson:"fileName"                 json:"fileName"`
	FileHash       string             `bson:"fileHash,omitempty"       json:"fileHash,omitempty"`
	CollectionName string             `bson:"collectionName,omitempty" json:"collectionName"`
	RecordCount    int64              `bson:"recordCount"              json:"recordCount"`
	UpsertedCount  int64              `bson:"upsertedCount"            json:"upsertedCount"`
	MatchedCount   int64              `bson:"matchedCount"             json:"matchedCount"`
	Status         string             `bson:"status"                   json:"status"` // "SUCCESS" | "FAILED"
	ErrorMessage   string             `bson:"errorMessage,omitempty"   json:"errorMessage,omitempty"`
	SyncedAt       time.Time          `bson:"syncedAt"                 json:"syncedAt"`
	DurationMs     int64              `bson:"durationMs"               json:"durationMs"`

	// Legacy backward compatibility fields for historical documents and frontend bindings
	LegacyCollectionName  string    `bson:"collection_name,omitempty"  json:"-"`
	LegacySyncTimestamp   time.Time `bson:"sync_timestamp,omitempty"   json:"syncTimestamp"`
	LegacyRecordsUploaded int64     `bson:"records_uploaded,omitempty" json:"recordsUploaded"`
}

// NewSuccessSyncLog constructs a complete audit log entry for a successful file ingestion.
func NewSuccessSyncLog(
	fileName string,
	fileHash string,
	collectionName string,
	recordCount int64,
	upsertedCount int64,
	matchedCount int64,
	syncedAt time.Time,
	durationMs int64,
) SyncLog {
	utcSyncedAt := syncedAt.UTC()
	return SyncLog{
		FileName:              fileName,
		FileHash:              fileHash,
		CollectionName:        collectionName,
		RecordCount:           recordCount,
		UpsertedCount:         upsertedCount,
		MatchedCount:          matchedCount,
		Status:                string(SyncStatusSuccess),
		SyncedAt:              utcSyncedAt,
		DurationMs:            durationMs,
		LegacyCollectionName:  collectionName,
		LegacySyncTimestamp:   utcSyncedAt,
		LegacyRecordsUploaded: upsertedCount,
	}
}

// NewFailedSyncLog constructs an audit log entry for a failed file ingestion.
func NewFailedSyncLog(
	fileName string,
	fileHash string,
	collectionName string,
	errorMessage string,
	recordCount int64,
	syncedAt time.Time,
	durationMs int64,
) SyncLog {
	utcSyncedAt := syncedAt.UTC()
	return SyncLog{
		FileName:              fileName,
		FileHash:              fileHash,
		CollectionName:        collectionName,
		RecordCount:           recordCount,
		UpsertedCount:         0,
		MatchedCount:          0,
		Status:                string(SyncStatusFailed),
		ErrorMessage:          errorMessage,
		SyncedAt:              utcSyncedAt,
		DurationMs:            durationMs,
		LegacyCollectionName:  collectionName,
		LegacySyncTimestamp:   utcSyncedAt,
		LegacyRecordsUploaded: 0,
	}
}

// Normalize populates canonical and legacy fields bidirectionally when reading existing records from MongoDB.
func (l *SyncLog) Normalize() {
	if l == nil {
		return
	}
	// Normalize timestamps
	if l.SyncedAt.IsZero() && !l.LegacySyncTimestamp.IsZero() {
		l.SyncedAt = l.LegacySyncTimestamp.UTC()
	} else if !l.SyncedAt.IsZero() && l.LegacySyncTimestamp.IsZero() {
		l.LegacySyncTimestamp = l.SyncedAt.UTC()
	}

	// Normalize collection name
	if l.CollectionName == "" && l.LegacyCollectionName != "" {
		l.CollectionName = l.LegacyCollectionName
	} else if l.CollectionName != "" && l.LegacyCollectionName == "" {
		l.LegacyCollectionName = l.CollectionName
	}

	// Normalize counts
	if l.RecordCount == 0 && l.LegacyRecordsUploaded > 0 {
		l.RecordCount = l.LegacyRecordsUploaded
		l.UpsertedCount = l.LegacyRecordsUploaded
	}
	if l.LegacyRecordsUploaded == 0 && l.UpsertedCount > 0 {
		l.LegacyRecordsUploaded = l.UpsertedCount
	}

	// Default status if reading legacy document
	if l.Status == "" {
		l.Status = string(SyncStatusSuccess)
	}
}
