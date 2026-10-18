package model_test

import (
	"errors"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestSyncStatus_Constants(t *testing.T) {
	if model.SyncStatusSuccess != "SUCCESS" {
		t.Errorf("expected SyncStatusSuccess to be 'SUCCESS', got %q", model.SyncStatusSuccess)
	}
	if model.SyncStatusFailed != "FAILED" {
		t.Errorf("expected SyncStatusFailed to be 'FAILED', got %q", model.SyncStatusFailed)
	}
}

func TestSyncStatus_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		status   model.SyncStatus
		expected bool
	}{
		{name: "SyncStatusSuccess", status: model.SyncStatusSuccess, expected: true},
		{name: "SyncStatusFailed", status: model.SyncStatusFailed, expected: true},
		{name: "Empty status", status: model.SyncStatus(""), expected: false},
		{name: "Pending status", status: model.SyncStatus("PENDING"), expected: false},
		{name: "Random status", status: model.SyncStatus("UNKNOWN"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsValid(); got != tt.expected {
				t.Errorf("SyncStatus(%q).IsValid() = %v; want %v", tt.status, got, tt.expected)
			}
		})
	}
}

func TestSyncStatus_StatusChecks(t *testing.T) {
	if !model.SyncStatusSuccess.IsSuccess() {
		t.Error("expected SyncStatusSuccess.IsSuccess() to be true")
	}
	if model.SyncStatusSuccess.IsFailed() {
		t.Error("expected SyncStatusSuccess.IsFailed() to be false")
	}

	if model.SyncStatusFailed.IsSuccess() {
		t.Error("expected SyncStatusFailed.IsSuccess() to be false")
	}
	if !model.SyncStatusFailed.IsFailed() {
		t.Error("expected SyncStatusFailed.IsFailed() to be true")
	}

	other := model.SyncStatus("OTHER")
	if other.IsSuccess() {
		t.Error("expected other.IsSuccess() to be false")
	}
	if other.IsFailed() {
		t.Error("expected other.IsFailed() to be false")
	}
}

func TestSyncLog_Instantiation(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 30, 0, 0, time.UTC)
	log := model.SyncLog{
		FileName:       "chase_checking.csv",
		FileHash:       "hash987654",
		CollectionName: "transactions_chase",
		RecordCount:    200,
		UpsertedCount:  190,
		MatchedCount:   10,
		Status:         model.SyncStatusSuccess,
		ErrorMessage:   "",
		SyncedAt:       now,
		DurationMs:     340,
	}

	if log.FileName != "chase_checking.csv" {
		t.Errorf("expected FileName 'chase_checking.csv', got %q", log.FileName)
	}
	if log.FileHash != "hash987654" {
		t.Errorf("expected FileHash 'hash987654', got %q", log.FileHash)
	}
	if log.CollectionName != "transactions_chase" {
		t.Errorf("expected CollectionName 'transactions_chase', got %q", log.CollectionName)
	}
	if log.RecordCount != 200 {
		t.Errorf("expected RecordCount 200, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 190 {
		t.Errorf("expected UpsertedCount 190, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 10 {
		t.Errorf("expected MatchedCount 10, got %d", log.MatchedCount)
	}
	if log.Status != model.SyncStatusSuccess {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.ErrorMessage != "" {
		t.Errorf("expected empty ErrorMessage, got %q", log.ErrorMessage)
	}
	if !log.SyncedAt.Equal(now) {
		t.Errorf("expected SyncedAt %v, got %v", now, log.SyncedAt)
	}
	if log.DurationMs != 340 {
		t.Errorf("expected DurationMs 340, got %d", log.DurationMs)
	}
}

func TestSyncLog_ZeroValue(t *testing.T) {
	var log model.SyncLog

	if log.FileName != "" {
		t.Errorf("expected empty FileName, got %q", log.FileName)
	}
	if log.FileHash != "" {
		t.Errorf("expected empty FileHash, got %q", log.FileHash)
	}
	if log.CollectionName != "" {
		t.Errorf("expected empty CollectionName, got %q", log.CollectionName)
	}
	if log.RecordCount != 0 {
		t.Errorf("expected RecordCount 0, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 0 {
		t.Errorf("expected MatchedCount 0, got %d", log.MatchedCount)
	}
	if log.Status != "" {
		t.Errorf("expected empty Status, got %q", log.Status)
	}
	if log.ErrorMessage != "" {
		t.Errorf("expected empty ErrorMessage, got %q", log.ErrorMessage)
	}
	if !log.SyncedAt.IsZero() {
		t.Errorf("expected zero SyncedAt, got %v", log.SyncedAt)
	}
	if log.DurationMs != 0 {
		t.Errorf("expected DurationMs 0, got %d", log.DurationMs)
	}
}

func TestSyncLog_NewSuccessSyncLog(t *testing.T) {
	syncedAt := time.Date(2026, 10, 18, 2, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	log := model.NewSuccessSyncLog(
		"fidelity_statement.csv",
		"fidhash123",
		"positions_fidelity",
		50,
		45,
		5,
		syncedAt,
		180,
	)

	if log == nil {
		t.Fatal("expected non-nil SyncLog")
	}
	if log.FileName != "fidelity_statement.csv" {
		t.Errorf("expected FileName 'fidelity_statement.csv', got %q", log.FileName)
	}
	if log.FileHash != "fidhash123" {
		t.Errorf("expected FileHash 'fidhash123', got %q", log.FileHash)
	}
	if log.CollectionName != "positions_fidelity" {
		t.Errorf("expected CollectionName 'positions_fidelity', got %q", log.CollectionName)
	}
	if log.RecordCount != 50 {
		t.Errorf("expected RecordCount 50, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 45 {
		t.Errorf("expected UpsertedCount 45, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 5 {
		t.Errorf("expected MatchedCount 5, got %d", log.MatchedCount)
	}
	if log.Status != model.SyncStatusSuccess {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.ErrorMessage != "" {
		t.Errorf("expected empty ErrorMessage, got %q", log.ErrorMessage)
	}
	if log.SyncedAt.Location() != time.UTC {
		t.Errorf("expected SyncedAt location UTC, got %v", log.SyncedAt.Location())
	}
	if !log.SyncedAt.Equal(syncedAt) {
		t.Errorf("expected SyncedAt equal to %v, got %v", syncedAt, log.SyncedAt)
	}
	if log.DurationMs != 180 {
		t.Errorf("expected DurationMs 180, got %d", log.DurationMs)
	}
	if !log.IsSuccess() {
		t.Error("expected IsSuccess() to be true")
	}
	if log.IsFailed() {
		t.Error("expected IsFailed() to be false")
	}

	if err := log.Validate(); err != nil {
		t.Errorf("expected valid SyncLog from NewSuccessSyncLog, got %v", err)
	}
}

func TestSyncLog_NewFailedSyncLog(t *testing.T) {
	syncedAt := time.Date(2026, 10, 18, 2, 15, 0, 0, time.UTC)
	log := model.NewFailedSyncLog(
		"corrupt_file.csv",
		"corrupthash456",
		"transactions_chase",
		"failed to parse CSV header: unexpected column count",
		30,
		syncedAt,
		45,
	)

	if log == nil {
		t.Fatal("expected non-nil SyncLog")
	}
	if log.FileName != "corrupt_file.csv" {
		t.Errorf("expected FileName 'corrupt_file.csv', got %q", log.FileName)
	}
	if log.FileHash != "corrupthash456" {
		t.Errorf("expected FileHash 'corrupthash456', got %q", log.FileHash)
	}
	if log.CollectionName != "transactions_chase" {
		t.Errorf("expected CollectionName 'transactions_chase', got %q", log.CollectionName)
	}
	if log.RecordCount != 30 {
		t.Errorf("expected RecordCount 30, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 0 {
		t.Errorf("expected MatchedCount 0, got %d", log.MatchedCount)
	}
	if log.Status != model.SyncStatusFailed {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if log.ErrorMessage != "failed to parse CSV header: unexpected column count" {
		t.Errorf("expected ErrorMessage match, got %q", log.ErrorMessage)
	}
	if !log.SyncedAt.Equal(syncedAt) {
		t.Errorf("expected SyncedAt equal to %v, got %v", syncedAt, log.SyncedAt)
	}
	if log.DurationMs != 45 {
		t.Errorf("expected DurationMs 45, got %d", log.DurationMs)
	}
	if log.IsSuccess() {
		t.Error("expected IsSuccess() to be false")
	}
	if !log.IsFailed() {
		t.Error("expected IsFailed() to be true")
	}

	if err := log.Validate(); err != nil {
		t.Errorf("expected valid SyncLog from NewFailedSyncLog, got %v", err)
	}
}

func TestSyncLog_NilReceiver(t *testing.T) {
	var nilLog *model.SyncLog

	if nilLog.IsSuccess() {
		t.Error("expected nil.IsSuccess() to be false")
	}
	if nilLog.IsFailed() {
		t.Error("expected nil.IsFailed() to be false")
	}
	if err := nilLog.Validate(); !errors.Is(err, model.ErrNilSyncLog) {
		t.Errorf("expected ErrNilSyncLog on nil receiver, got %v", err)
	}
}

func TestSyncLog_Validate(t *testing.T) {
	validTime := time.Date(2026, 10, 18, 2, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		log         *model.SyncLog
		expectedErr error
	}{
		{
			name:        "nil sync log",
			log:         nil,
			expectedErr: model.ErrNilSyncLog,
		},
		{
			name: "valid success log",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				RecordCount:    10,
				UpsertedCount:  10,
				MatchedCount:   0,
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     120,
			},
			expectedErr: nil,
		},
		{
			name: "valid failed log",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				RecordCount:    0,
				UpsertedCount:  0,
				MatchedCount:   0,
				Status:         model.SyncStatusFailed,
				ErrorMessage:   "persistence timed out",
				SyncedAt:       validTime,
				DurationMs:     500,
			},
			expectedErr: nil,
		},
		{
			name: "invalid sync status",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				Status:         model.SyncStatus("INVALID"),
				SyncedAt:       validTime,
				DurationMs:     100,
			},
			expectedErr: model.ErrInvalidSyncStatus,
		},
		{
			name: "negative record count",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				RecordCount:    -5,
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     10,
			},
			expectedErr: model.ErrNegativeRecordCount,
		},
		{
			name: "negative upserted count",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				RecordCount:    10,
				UpsertedCount:  -1,
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     10,
			},
			expectedErr: model.ErrNegativeUpsertedCount,
		},
		{
			name: "negative matched count",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				RecordCount:    10,
				MatchedCount:   -1,
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     10,
			},
			expectedErr: model.ErrNegativeMatchedCount,
		},
		{
			name: "negative duration",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     -10,
			},
			expectedErr: model.ErrNegativeDuration,
		},
		{
			name: "missing file name",
			log: &model.SyncLog{
				FileName:       "",
				CollectionName: "transactions_chase",
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     100,
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "missing collection name",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "",
				Status:         model.SyncStatusSuccess,
				SyncedAt:       validTime,
				DurationMs:     100,
			},
			expectedErr: model.ErrMissingCollectionName,
		},
		{
			name: "missing synced at",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				Status:         model.SyncStatusSuccess,
				DurationMs:     100,
			},
			expectedErr: model.ErrMissingSyncTime,
		},
		{
			name: "failed status with missing error message",
			log: &model.SyncLog{
				FileName:       "test.csv",
				CollectionName: "transactions_chase",
				Status:         model.SyncStatusFailed,
				ErrorMessage:   "",
				SyncedAt:       validTime,
				DurationMs:     100,
			},
			expectedErr: model.ErrMissingErrorMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.log.Validate()
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}
