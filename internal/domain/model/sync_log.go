package model

import (
	"errors"
	"time"
)

var (
	// ErrNilSyncLog indicates that a SyncLog is nil.
	ErrNilSyncLog = errors.New("sync log is nil")
	// ErrInvalidSyncStatus indicates that the sync status is unrecognized.
	ErrInvalidSyncStatus = errors.New("invalid sync status")
	// ErrNegativeDuration indicates that the duration is negative.
	ErrNegativeDuration = errors.New("duration cannot be negative")
	// ErrNegativeUpsertedCount indicates that the upserted count is negative.
	ErrNegativeUpsertedCount = errors.New("upserted count cannot be negative")
	// ErrNegativeMatchedCount indicates that the matched count is negative.
	ErrNegativeMatchedCount = errors.New("matched count cannot be negative")
	// ErrMissingCollectionName indicates that the target collection name is empty.
	ErrMissingCollectionName = errors.New("collection name is required")
	// ErrMissingSyncTime indicates that the synchronization timestamp is zero.
	ErrMissingSyncTime = errors.New("sync timestamp is required")
	// ErrMissingErrorMessage indicates that an error message is required for failed syncs.
	ErrMissingErrorMessage = errors.New("error message is required for failed sync")
)

// SyncStatus represents the completion status of an ingestion sync operation.
type SyncStatus string

const (
	// SyncStatusSuccess indicates that the ingestion succeeded.
	SyncStatusSuccess SyncStatus = "SUCCESS"
	// SyncStatusFailed indicates that the ingestion failed.
	SyncStatusFailed SyncStatus = "FAILED"
)

// IsValid reports whether the status is a recognized SyncStatus.
func (s SyncStatus) IsValid() bool {
	return s == SyncStatusSuccess || s == SyncStatusFailed
}

// IsSuccess reports whether the status is SyncStatusSuccess.
func (s SyncStatus) IsSuccess() bool {
	return s == SyncStatusSuccess
}

// IsFailed reports whether the status is SyncStatusFailed.
func (s SyncStatus) IsFailed() bool {
	return s == SyncStatusFailed
}

// SyncLog captures the audit log record for an ingestion synchronization operation.
type SyncLog struct {
	FileName       string
	FileHash       string
	CollectionName string
	RecordCount    int64
	UpsertedCount  int64
	MatchedCount   int64
	Status         SyncStatus
	ErrorMessage   string
	SyncedAt       time.Time
	DurationMs     int64
}

// NewSuccessSyncLog constructs a SyncLog representing a successful sync operation.
func NewSuccessSyncLog(
	fileName string,
	fileHash string,
	collectionName string,
	recordCount int64,
	upsertedCount int64,
	matchedCount int64,
	syncedAt time.Time,
	durationMs int64,
) *SyncLog {
	return &SyncLog{
		FileName:       fileName,
		FileHash:       fileHash,
		CollectionName: collectionName,
		RecordCount:    recordCount,
		UpsertedCount:  upsertedCount,
		MatchedCount:   matchedCount,
		Status:         SyncStatusSuccess,
		ErrorMessage:   "",
		SyncedAt:       syncedAt.UTC(),
		DurationMs:     durationMs,
	}
}

// NewFailedSyncLog constructs a SyncLog representing a failed sync operation.
func NewFailedSyncLog(
	fileName string,
	fileHash string,
	collectionName string,
	errorMessage string,
	recordCount int64,
	syncedAt time.Time,
	durationMs int64,
) *SyncLog {
	return &SyncLog{
		FileName:       fileName,
		FileHash:       fileHash,
		CollectionName: collectionName,
		RecordCount:    recordCount,
		UpsertedCount:  0,
		MatchedCount:   0,
		Status:         SyncStatusFailed,
		ErrorMessage:   errorMessage,
		SyncedAt:       syncedAt.UTC(),
		DurationMs:     durationMs,
	}
}

// IsSuccess reports whether the sync log represents a successful sync operation.
func (l *SyncLog) IsSuccess() bool {
	return l != nil && l.Status.IsSuccess()
}

// IsFailed reports whether the sync log represents a failed sync operation.
func (l *SyncLog) IsFailed() bool {
	return l != nil && l.Status.IsFailed()
}

// Validate validates the core integrity of the SyncLog.
func (l *SyncLog) Validate() error {
	if l == nil {
		return ErrNilSyncLog
	}
	if !l.Status.IsValid() {
		return ErrInvalidSyncStatus
	}
	if l.RecordCount < 0 {
		return ErrNegativeRecordCount
	}
	if l.UpsertedCount < 0 {
		return ErrNegativeUpsertedCount
	}
	if l.MatchedCount < 0 {
		return ErrNegativeMatchedCount
	}
	if l.DurationMs < 0 {
		return ErrNegativeDuration
	}
	if l.FileName == "" {
		return ErrMissingFileName
	}
	if l.CollectionName == "" {
		return ErrMissingCollectionName
	}
	if l.SyncedAt.IsZero() {
		return ErrMissingSyncTime
	}
	if l.Status == SyncStatusFailed && l.ErrorMessage == "" {
		return ErrMissingErrorMessage
	}
	return nil
}
