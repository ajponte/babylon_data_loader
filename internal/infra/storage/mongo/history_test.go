package mongo

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"babylon/dataloader/internal/domain/model"
)

func TestListSyncLogs_SortAndLimit(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)

	const requestedLimit int64 = 25
	logs, err := repo.ListSyncLogs(context.Background(), requestedLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logs == nil {
		t.Fatal("expected non-nil logs slice")
	}

	coll := fakeDB.dataSyncColl()
	if coll.FindCallCount() != 1 {
		t.Fatalf("expected 1 Find call, got %d", coll.FindCallCount())
	}

	opts := coll.FindOpts()
	if len(opts) != 1 {
		t.Fatalf("expected 1 FindOpts, got %d", len(opts))
	}
	if opts[0].Limit == nil || *opts[0].Limit != requestedLimit {
		t.Errorf("expected limit %d, got %v", requestedLimit, opts[0].Limit)
	}

	wantSort := bson.D{{Key: fieldSyncedAt, Value: -1}}
	if !reflect.DeepEqual(opts[0].Sort, wantSort) {
		t.Errorf("sort mismatch: got %+v, want %+v", opts[0].Sort, wantSort)
	}

	wantFilter := bson.D{}
	if !reflect.DeepEqual(coll.FindFilter(), wantFilter) {
		t.Errorf("filter mismatch: got %+v, want %+v", coll.FindFilter(), wantFilter)
	}
}

func TestListSyncLogs_LimitSemantics(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected int64
	}{
		{name: "large negative defaults", input: -100, expected: DefaultSyncLogLimit},
		{name: "small negative defaults", input: -5, expected: DefaultSyncLogLimit},
		{name: "boundary negative one defaults", input: -1, expected: DefaultSyncLogLimit},
		{name: "zero limit defaults", input: 0, expected: DefaultSyncLogLimit},
		{name: "boundary positive one accepted", input: 1, expected: 1},
		{name: "typical positive limit accepted", input: 25, expected: 25},
		{name: "default-matching positive limit accepted", input: 100, expected: 100},
		{name: "large positive limit accepted", input: 500, expected: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeDB := newFakeDatabase()
			repo := newRepository(fakeDB)

			if _, err := repo.ListSyncLogs(context.Background(), tt.input); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			opts := fakeDB.dataSyncColl().FindOpts()
			if len(opts) != 1 || opts[0].Limit == nil || *opts[0].Limit != tt.expected {
				t.Errorf("expected limit %d, got %v", tt.expected, opts[0].Limit)
			}
		})
	}
}

func TestListSyncLogs_DecodesCanonicalDocument(t *testing.T) {
	fakeDB := newFakeDatabase()
	now := time.Date(2026, 10, 25, 14, 30, 0, 0, time.UTC)
	canonicalDoc := syncLogDoc{
		FileName:       "chase_checking.csv",
		FileHash:       "hash123",
		CollectionName: "transactions_chase",
		RecordCount:    50,
		UpsertedCount:  40,
		MatchedCount:   10,
		Status:         "SUCCESS",
		ErrorMessage:   "",
		SyncedAt:       now,
		DurationMs:     125,
	}

	cursor, err := mongodrv.NewCursorFromDocuments([]any{canonicalDoc}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}
	fakeDB.dataSyncColl().findCursor = cursor
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	assertSyncLogParity(t, logs[0], model.SyncLog{
		FileName:       "chase_checking.csv",
		FileHash:       "hash123",
		CollectionName: "transactions_chase",
		RecordCount:    50,
		UpsertedCount:  40,
		MatchedCount:   10,
		Status:         model.SyncStatusSuccess,
		ErrorMessage:   "",
		SyncedAt:       now,
		DurationMs:     125,
	})
}

func TestListSyncLogs_DecodesLegacyOnlyDocument(t *testing.T) {
	fakeDB := newFakeDatabase()
	legacyTime := time.Date(2025, 5, 20, 8, 30, 0, 0, time.UTC)
	legacyDoc := bson.D{
		{Key: "fileName", Value: "legacy_fidelity.csv"},
		{Key: "fileHash", Value: "hash456"},
		{Key: "collection_name", Value: "positions_fidelity"},
		{Key: "sync_timestamp", Value: legacyTime},
		{Key: "records_uploaded", Value: int64(42)},
	}

	cursor, err := mongodrv.NewCursorFromDocuments([]any{legacyDoc}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}
	fakeDB.dataSyncColl().findCursor = cursor
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	assertSyncLogParity(t, logs[0], model.SyncLog{
		FileName:       "legacy_fidelity.csv",
		FileHash:       "hash456",
		CollectionName: "positions_fidelity",
		RecordCount:    42,
		UpsertedCount:  42,
		Status:         model.SyncStatusSuccess,
		SyncedAt:       legacyTime,
	})
}

func TestListSyncLogs_DecodesMultipleDocuments(t *testing.T) {
	fakeDB := newFakeDatabase()
	now := time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)
	earlier := now.Add(-1 * time.Hour)

	docs := []any{
		syncLogDoc{
			FileName:       "batch1.csv",
			FileHash:       "hash1",
			CollectionName: "transactions_chase",
			RecordCount:    20,
			UpsertedCount:  20,
			Status:         "SUCCESS",
			SyncedAt:       now,
			DurationMs:     50,
		},
		bson.D{
			{Key: "fileName", Value: "batch2.csv"},
			{Key: "fileHash", Value: "hash2"},
			{Key: "collection_name", Value: "positions_fidelity"},
			{Key: "sync_timestamp", Value: earlier},
			{Key: "records_uploaded", Value: int64(15)},
		},
	}

	cursor, err := mongodrv.NewCursorFromDocuments(docs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}
	fakeDB.dataSyncColl().findCursor = cursor
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(logs))
	}

	assertSyncLogParity(t, logs[0], model.SyncLog{
		FileName:       "batch1.csv",
		FileHash:       "hash1",
		CollectionName: "transactions_chase",
		RecordCount:    20,
		UpsertedCount:  20,
		Status:         model.SyncStatusSuccess,
		SyncedAt:       now,
		DurationMs:     50,
	})

	assertSyncLogParity(t, logs[1], model.SyncLog{
		FileName:       "batch2.csv",
		FileHash:       "hash2",
		CollectionName: "positions_fidelity",
		RecordCount:    15,
		UpsertedCount:  15,
		Status:         model.SyncStatusSuccess,
		SyncedAt:       earlier,
	})
}

func TestFromSyncLogDoc_NormalizeParity(t *testing.T) {
	legacyTime := time.Date(2025, 5, 20, 8, 30, 0, 0, time.UTC)
	canonicalTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	customLoc := time.FixedZone("EDT", -4*3600)
	edtTime := time.Date(2026, 6, 15, 10, 0, 0, 0, customLoc)

	tests := []struct {
		name string
		in   syncLogDoc
		want model.SyncLog
	}{
		{
			name: "legacy-only record normalized",
			in: syncLogDoc{
				FileName:            "fidelity.csv",
				DualCollectionName:  "positions_fidelity",
				DualSyncTimestamp:   legacyTime,
				DualRecordsUploaded: 42,
			},
			want: model.SyncLog{
				FileName:       "fidelity.csv",
				CollectionName: "positions_fidelity",
				SyncedAt:       legacyTime,
				RecordCount:    42,
				UpsertedCount:  42,
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "canonical-only record preserved",
			in: syncLogDoc{
				FileName:       "chase.csv",
				CollectionName: "transactions_chase",
				RecordCount:    100,
				UpsertedCount:  90,
				MatchedCount:   10,
				Status:         "SUCCESS",
				SyncedAt:       canonicalTime,
				DurationMs:     50,
			},
			want: model.SyncLog{
				FileName:       "chase.csv",
				CollectionName: "transactions_chase",
				RecordCount:    100,
				UpsertedCount:  90,
				MatchedCount:   10,
				Status:         model.SyncStatusSuccess,
				SyncedAt:       canonicalTime,
				DurationMs:     50,
			},
		},
		{
			name: "canonical fields take precedence over dual fields",
			in: syncLogDoc{
				FileName:            "chase.csv",
				CollectionName:      "canonical_coll",
				DualCollectionName:  "legacy_coll",
				RecordCount:         15,
				UpsertedCount:       12,
				DualRecordsUploaded: 5,
				Status:              "FAILED",
				ErrorMessage:        "timeout",
				SyncedAt:            canonicalTime,
				DualSyncTimestamp:   legacyTime,
				DurationMs:          80,
			},
			want: model.SyncLog{
				FileName:       "chase.csv",
				CollectionName: "canonical_coll",
				RecordCount:    15,
				UpsertedCount:  12,
				Status:         model.SyncStatusFailed,
				ErrorMessage:   "timeout",
				SyncedAt:       canonicalTime,
				DurationMs:     80,
			},
		},
		{
			name: "empty status defaults to SUCCESS",
			in: syncLogDoc{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       canonicalTime,
				Status:         "",
			},
			want: model.SyncLog{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       canonicalTime,
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "non-UTC SyncedAt converted to UTC",
			in: syncLogDoc{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       edtTime,
				Status:         "SUCCESS",
			},
			want: model.SyncLog{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       edtTime.UTC(),
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "non-UTC DualSyncTimestamp converted to UTC",
			in: syncLogDoc{
				FileName:           "file.csv",
				DualCollectionName: "coll",
				DualSyncTimestamp:  edtTime,
			},
			want: model.SyncLog{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       edtTime.UTC(),
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "zero timestamps remain zero",
			in: syncLogDoc{
				FileName:       "file.csv",
				CollectionName: "coll",
				Status:         "SUCCESS",
			},
			want: model.SyncLog{
				FileName:       "file.csv",
				CollectionName: "coll",
				SyncedAt:       time.Time{},
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "both collection names empty remains empty",
			in: syncLogDoc{
				FileName:           "unknown.csv",
				CollectionName:     "",
				DualCollectionName: "",
				SyncedAt:           canonicalTime,
			},
			want: model.SyncLog{
				FileName:       "unknown.csv",
				CollectionName: "",
				SyncedAt:       canonicalTime,
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "dual records uploaded zero does not overwrite zero recordCount",
			in: syncLogDoc{
				FileName:            "empty.csv",
				CollectionName:      "coll",
				RecordCount:         0,
				UpsertedCount:       0,
				DualRecordsUploaded: 0,
				SyncedAt:            canonicalTime,
			},
			want: model.SyncLog{
				FileName:       "empty.csv",
				CollectionName: "coll",
				RecordCount:    0,
				UpsertedCount:  0,
				SyncedAt:       canonicalTime,
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "negative dual records uploaded ignored",
			in: syncLogDoc{
				FileName:            "neg.csv",
				CollectionName:      "coll",
				RecordCount:         0,
				UpsertedCount:       0,
				DualRecordsUploaded: -5,
				SyncedAt:            canonicalTime,
			},
			want: model.SyncLog{
				FileName:       "neg.csv",
				CollectionName: "coll",
				RecordCount:    0,
				UpsertedCount:  0,
				SyncedAt:       canonicalTime,
				Status:         model.SyncStatusSuccess,
			},
		},
		{
			name: "status failed preserved even with empty error message",
			in: syncLogDoc{
				FileName:       "failed_no_err.csv",
				CollectionName: "coll",
				Status:         "FAILED",
				ErrorMessage:   "",
				SyncedAt:       canonicalTime,
			},
			want: model.SyncLog{
				FileName:       "failed_no_err.csv",
				CollectionName: "coll",
				Status:         model.SyncStatusFailed,
				ErrorMessage:   "",
				SyncedAt:       canonicalTime,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertSyncLogParity(t, fromSyncLogDoc(tt.in), tt.want)
		})
	}
}

func assertSyncLogParity(t *testing.T, got, want model.SyncLog) {
	t.Helper()
	if got.FileName != want.FileName {
		t.Errorf("FileName: got %q, want %q", got.FileName, want.FileName)
	}
	if got.FileHash != want.FileHash {
		t.Errorf("FileHash: got %q, want %q", got.FileHash, want.FileHash)
	}
	if got.CollectionName != want.CollectionName {
		t.Errorf("CollectionName: got %q, want %q", got.CollectionName, want.CollectionName)
	}
	if got.RecordCount != want.RecordCount || got.UpsertedCount != want.UpsertedCount || got.MatchedCount != want.MatchedCount {
		t.Errorf("Counts (rec/ups/mat): got (%d/%d/%d), want (%d/%d/%d)",
			got.RecordCount, got.UpsertedCount, got.MatchedCount,
			want.RecordCount, want.UpsertedCount, want.MatchedCount)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %q, want %q", got.Status, want.Status)
	}
	if got.ErrorMessage != want.ErrorMessage {
		t.Errorf("ErrorMessage: got %q, want %q", got.ErrorMessage, want.ErrorMessage)
	}
	if !got.SyncedAt.Equal(want.SyncedAt) {
		t.Errorf("SyncedAt: got %v, want %v", got.SyncedAt, want.SyncedAt)
	}
	if !got.SyncedAt.IsZero() && got.SyncedAt.Location() != time.UTC {
		t.Errorf("SyncedAt location is not UTC: %v", got.SyncedAt.Location())
	}
	if got.DurationMs != want.DurationMs {
		t.Errorf("DurationMs: got %d, want %d", got.DurationMs, want.DurationMs)
	}
}

func TestListSyncLogs_EmptyResult(t *testing.T) {
	fakeDB := newFakeDatabase()
	cursor, err := mongodrv.NewCursorFromDocuments([]any{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}
	fakeDB.dataSyncColl().findCursor = cursor
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logs == nil {
		t.Fatal("expected non-nil slice on empty result")
	}
	if len(logs) != 0 {
		t.Fatalf("expected 0 logs, got %d", len(logs))
	}
}

func TestListSyncLogs_FindErrorWrapped(t *testing.T) {
	fakeDB := newFakeDatabase()
	driverErr := errors.New("underlying network error")
	fakeDB.dataSyncColl().findErr = driverErr
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if logs != nil {
		t.Fatalf("expected nil logs on error, got %+v", logs)
	}
	if !errors.Is(err, driverErr) {
		t.Fatalf("expected driverErr wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), dataSyncCollection) {
		t.Fatalf("expected error message to contain %q, got %q", dataSyncCollection, err.Error())
	}
}

func TestListSyncLogs_DecodeErrorWrapped(t *testing.T) {
	fakeDB := newFakeDatabase()
	corruptedDocs := []any{
		bson.D{{Key: "syncedAt", Value: "invalid-timestamp-type"}},
	}
	cursor, err := mongodrv.NewCursorFromDocuments(corruptedDocs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}
	fakeDB.dataSyncColl().findCursor = cursor
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(context.Background(), 10)
	if logs != nil {
		t.Fatalf("expected nil logs on decode error, got %+v", logs)
	}
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), dataSyncCollection) {
		t.Fatalf("expected error message to contain %q, got %q", dataSyncCollection, err.Error())
	}
}

func TestListSyncLogs_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)

	logs, err := repo.ListSyncLogs(ctx, 10)
	if logs != nil {
		t.Fatalf("expected nil logs on canceled context, got %+v", logs)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if fakeDB.dataSyncColl().FindCallCount() != 0 {
		t.Fatalf("expected 0 Find calls on canceled context, got %d", fakeDB.dataSyncColl().FindCallCount())
	}
}

func TestListSyncLogs_NoIndexCreation(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)

	if _, err := repo.ListSyncLogs(context.Background(), 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	coll := fakeDB.dataSyncColl()
	// RF-02: Strict AC-3G-3 verification: 0 indexes, 0 bulk writes, 0 single inserts
	if coll.IdxCallCount() != 0 {
		t.Fatalf("expected 0 CreateIndexes calls on read path, got %d", coll.IdxCallCount())
	}
	if len(coll.createdIndex) != 0 {
		t.Fatalf("expected 0 created index batches, got %d", len(coll.createdIndex))
	}
	if coll.BulkCallCount() != 0 {
		t.Fatalf("expected 0 BulkWrite calls on read path, got %d", coll.BulkCallCount())
	}
	if coll.InsertCallCount() != 0 {
		t.Fatalf("expected 0 InsertOne calls on read path, got %d", coll.InsertCallCount())
	}
}
