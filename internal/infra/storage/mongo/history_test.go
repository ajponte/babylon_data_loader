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

func TestListSyncLogs_DefaultLimit(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected int64
	}{
		{name: "zero limit defaults", input: 0, expected: DefaultSyncLogLimit},
		{name: "negative limit defaults", input: -5, expected: DefaultSyncLogLimit},
		{name: "large negative limit defaults", input: -100, expected: DefaultSyncLogLimit},
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

	got := logs[0]
	if got.FileName != "chase_checking.csv" ||
		got.FileHash != "hash123" ||
		got.CollectionName != "transactions_chase" ||
		got.RecordCount != 50 ||
		got.UpsertedCount != 40 ||
		got.MatchedCount != 10 ||
		got.Status != model.SyncStatusSuccess ||
		got.ErrorMessage != "" ||
		!got.SyncedAt.Equal(now) ||
		got.DurationMs != 125 {
		t.Errorf("decoded canonical log mismatch: %+v", got)
	}
}

func TestListSyncLogs_DecodesLegacyOnlyDocument(t *testing.T) {
	fakeDB := newFakeDatabase()
	legacyTime := time.Date(2025, 5, 20, 8, 30, 0, 0, time.UTC)
	legacyDoc := bson.D{
		{Key: "fileName", Value: "legacy_fidelity.csv"},
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

	got := logs[0]
	if got.FileName != "legacy_fidelity.csv" {
		t.Errorf("expected FileName 'legacy_fidelity.csv', got %q", got.FileName)
	}
	if got.CollectionName != "positions_fidelity" {
		t.Errorf("expected CollectionName backfilled to 'positions_fidelity', got %q", got.CollectionName)
	}
	if !got.SyncedAt.Equal(legacyTime) {
		t.Errorf("expected SyncedAt backfilled to %v, got %v", legacyTime, got.SyncedAt)
	}
	if got.RecordCount != 42 {
		t.Errorf("expected RecordCount backfilled to 42, got %d", got.RecordCount)
	}
	if got.UpsertedCount != 42 {
		t.Errorf("expected UpsertedCount backfilled to 42, got %d", got.UpsertedCount)
	}
	if got.Status != model.SyncStatusSuccess {
		t.Errorf("expected Status defaulted to %q, got %q", model.SyncStatusSuccess, got.Status)
	}
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
	if coll.IdxCallCount() != 0 {
		t.Fatalf("expected 0 CreateIndexes calls on read path, got %d", coll.IdxCallCount())
	}
	if len(coll.createdIndex) != 0 {
		t.Fatalf("expected 0 created index batches, got %d", len(coll.createdIndex))
	}
}
