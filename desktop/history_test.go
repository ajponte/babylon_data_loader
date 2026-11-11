package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

// mockHistoryReader implements port.SyncLogReader for unit tests.
type mockHistoryReader struct {
	logs      []model.SyncLog
	err       error
	lastLimit int64
	called    bool
}

var _ port.SyncLogReader = (*mockHistoryReader)(nil)

func (m *mockHistoryReader) ListSyncLogs(ctx context.Context, limit int64) ([]model.SyncLog, error) {
	m.called = true
	m.lastLimit = limit
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.logs, nil
}

// assertNoDatabaseID asserts that the serialized JSON contains neither "id" nor "_id" fields (Divergence 10).
func assertNoDatabaseID(t *testing.T, jsonStr string) {
	t.Helper()
	if strings.Contains(jsonStr, `"id"`) || strings.Contains(jsonStr, `"_id"`) {
		t.Errorf("expected JSON to omit database ID, found: %s", jsonStr)
	}
}

// assertDualFieldsEqual asserts that legacy dual aliases match canonical domain values.
func assertDualFieldsEqual(t *testing.T, entries []HistoryEntry) {
	t.Helper()
	if len(entries) == 0 {
		return
	}
	entry := entries[0]
	if !entry.SyncTimestamp.Equal(entry.SyncedAt) {
		t.Errorf("syncTimestamp (%v) must equal syncedAt (%v)", entry.SyncTimestamp, entry.SyncedAt)
	}
	if entry.RecordsUploaded != entry.UpsertedCount {
		t.Errorf("recordsUploaded (%d) must equal upsertedCount (%d)", entry.RecordsUploaded, entry.UpsertedCount)
	}
}

func TestToHistoryEntries_LegacyJSONGolden(t *testing.T) {
	syncedTime := time.Date(2026, 11, 11, 15, 30, 0, 0, time.UTC)
	estZone := time.FixedZone("EST", -5*3600)
	syncedTimeEST := time.Date(2026, 11, 11, 10, 30, 0, 0, estZone)

	tests := []struct {
		name       string
		input      []model.SyncLog
		expected   string
		checkNoID  bool
		dualFields bool
	}{
		{
			name: "single successful log matches golden legacy JSON",
			input: []model.SyncLog{
				{
					FileName:       "chase_sample.csv",
					FileHash:       "abcd1234ef5678",
					CollectionName: "transactions_chase",
					RecordCount:    25,
					UpsertedCount:  20,
					MatchedCount:   5,
					Status:         model.SyncStatusSuccess,
					ErrorMessage:   "",
					SyncedAt:       syncedTime,
					DurationMs:     342,
				},
			},
			expected:   `[{"fileName":"chase_sample.csv","fileHash":"abcd1234ef5678","collectionName":"transactions_chase","recordCount":25,"upsertedCount":20,"matchedCount":5,"status":"SUCCESS","syncedAt":"2026-11-11T15:30:00Z","durationMs":342,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":20}]`,
			checkNoID:  true,
			dualFields: true,
		},
		{
			name: "non-UTC timestamp normalized to UTC Zulu",
			input: []model.SyncLog{
				{
					FileName:       "tz_test.csv",
					CollectionName: "transactions_chase",
					RecordCount:    1,
					UpsertedCount:  1,
					Status:         model.SyncStatusSuccess,
					SyncedAt:       syncedTimeEST,
					DurationMs:     50,
				},
			},
			expected:   `[{"fileName":"tz_test.csv","collectionName":"transactions_chase","recordCount":1,"upsertedCount":1,"matchedCount":0,"status":"SUCCESS","syncedAt":"2026-11-11T15:30:00Z","durationMs":50,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":1}]`,
			checkNoID:  true,
			dualFields: true,
		},
		{
			name: "failed log omits empty fileHash and includes errorMessage",
			input: []model.SyncLog{
				{
					FileName:       "corrupt.csv",
					FileHash:       "",
					CollectionName: "transactions_chase",
					RecordCount:    0,
					UpsertedCount:  0,
					MatchedCount:   0,
					Status:         model.SyncStatusFailed,
					ErrorMessage:   "parse error: unexpected EOF",
					SyncedAt:       syncedTime,
					DurationMs:     12,
				},
			},
			expected:   `[{"fileName":"corrupt.csv","collectionName":"transactions_chase","recordCount":0,"upsertedCount":0,"matchedCount":0,"status":"FAILED","errorMessage":"parse error: unexpected EOF","syncedAt":"2026-11-11T15:30:00Z","durationMs":12,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":0}]`,
			checkNoID:  true,
			dualFields: true,
		},
		{
			name: "multi-entry ordering and mixed status preserved",
			input: []model.SyncLog{
				{
					FileName:       "first.csv",
					FileHash:       "hash1",
					CollectionName: "transactions_chase",
					RecordCount:    10,
					UpsertedCount:  10,
					Status:         model.SyncStatusSuccess,
					SyncedAt:       syncedTime,
					DurationMs:     100,
				},
				{
					FileName:       "second.csv",
					CollectionName: "positions_fidelity",
					RecordCount:    0,
					UpsertedCount:  0,
					Status:         model.SyncStatusFailed,
					ErrorMessage:   "empty file",
					SyncedAt:       syncedTime,
					DurationMs:     15,
				},
			},
			expected:   `[{"fileName":"first.csv","fileHash":"hash1","collectionName":"transactions_chase","recordCount":10,"upsertedCount":10,"matchedCount":0,"status":"SUCCESS","syncedAt":"2026-11-11T15:30:00Z","durationMs":100,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":10},{"fileName":"second.csv","collectionName":"positions_fidelity","recordCount":0,"upsertedCount":0,"matchedCount":0,"status":"FAILED","errorMessage":"empty file","syncedAt":"2026-11-11T15:30:00Z","durationMs":15,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":0}]`,
			checkNoID:  true,
			dualFields: true,
		},
		{
			name:       "nil logs slice serializes to empty JSON array",
			input:      nil,
			expected:   `[]`,
			checkNoID:  false,
			dualFields: false,
		},
		{
			name:       "empty logs slice serializes to empty JSON array",
			input:      []model.SyncLog{},
			expected:   `[]`,
			checkNoID:  false,
			dualFields: false,
		},
		{
			name: "empty status defaults to SUCCESS",
			input: []model.SyncLog{
				{
					FileName:       "default_status.csv",
					CollectionName: "transactions_synthetic",
					RecordCount:    5,
					UpsertedCount:  5,
					Status:         "",
					SyncedAt:       syncedTime,
					DurationMs:     50,
				},
			},
			expected:   `[{"fileName":"default_status.csv","collectionName":"transactions_synthetic","recordCount":5,"upsertedCount":5,"matchedCount":0,"status":"SUCCESS","syncedAt":"2026-11-11T15:30:00Z","durationMs":50,"syncTimestamp":"2026-11-11T15:30:00Z","recordsUploaded":5}]`,
			checkNoID:  true,
			dualFields: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := ToHistoryEntries(tc.input)
			rawJSON, err := json.Marshal(entries)
			if err != nil {
				t.Fatalf("unexpected json.Marshal error: %v", err)
			}
			actual := string(rawJSON)

			if actual != tc.expected {
				t.Errorf("\nexpected: %s\nactual:   %s", tc.expected, actual)
			}

			if tc.checkNoID {
				assertNoDatabaseID(t, actual)
			}

			if tc.dualFields {
				assertDualFieldsEqual(t, entries)
			}
		})
	}
}

func execGetHistory(ctx context.Context, svc *HistoryService, limits []int64) ([]HistoryEntry, error) {
	if len(limits) == 0 {
		return svc.GetHistory(ctx)
	}
	return svc.GetHistory(ctx, limits...)
}

func TestHistoryService_UsesDefaultLimit(t *testing.T) {
	ctx := context.Background()
	sampleLogs := []model.SyncLog{
		{
			FileName:       "sample.csv",
			CollectionName: "transactions_chase",
			RecordCount:    10,
			UpsertedCount:  10,
			Status:         model.SyncStatusSuccess,
			SyncedAt:       time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC),
			DurationMs:     100,
		},
	}

	tests := []struct {
		name          string
		callWithLimit []int64
		expectedLimit int64
		structLiteral bool
	}{
		{
			name:          "no limit argument uses DefaultHistoryLimit (100)",
			callWithLimit: nil,
			expectedLimit: DefaultHistoryLimit,
		},
		{
			name:          "zero limit uses DefaultHistoryLimit (100)",
			callWithLimit: []int64{0},
			expectedLimit: DefaultHistoryLimit,
		},
		{
			name:          "negative limit uses DefaultHistoryLimit (100)",
			callWithLimit: []int64{-5},
			expectedLimit: DefaultHistoryLimit,
		},
		{
			name:          "explicit positive limit is respected",
			callWithLimit: []int64{42},
			expectedLimit: 42,
		},
		{
			name:          "custom limit 25 is passed to reader",
			callWithLimit: []int64{25},
			expectedLimit: 25,
		},
		{
			name:          "struct literal with zero defaultLimit falls back to DefaultHistoryLimit",
			callWithLimit: nil,
			expectedLimit: DefaultHistoryLimit,
			structLiteral: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockHistoryReader{logs: sampleLogs}
			svc := NewHistoryService(reader)
			if tc.structLiteral {
				svc = &HistoryService{reader: reader}
			}

			entries, err := execGetHistory(ctx, svc, tc.callWithLimit)
			if err != nil {
				t.Fatalf("unexpected GetHistory error: %v", err)
			}
			if !reader.called {
				t.Fatal("expected reader.ListSyncLogs to be called")
			}
			if reader.lastLimit != tc.expectedLimit {
				t.Errorf("expected limit %d, got %d", tc.expectedLimit, reader.lastLimit)
			}
			if len(entries) != 1 || entries[0].FileName != "sample.csv" {
				t.Errorf("unexpected entries returned: %+v", entries)
			}
		})
	}
}

func TestHistoryService_PropagatesError(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("database connection refused")

	t.Run("reader error is propagated", func(t *testing.T) {
		reader := &mockHistoryReader{err: sentinelErr}
		svc := NewHistoryService(reader)

		entries, err := svc.GetHistory(ctx)
		if !errors.Is(err, sentinelErr) {
			t.Fatalf("expected error %v, got %v", sentinelErr, err)
		}
		if entries != nil {
			t.Errorf("expected nil entries on error, got: %+v", entries)
		}
	})

	t.Run("cancelled context returns context error", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		svc := NewHistoryService(&mockHistoryReader{})
		_, err := svc.GetHistory(canceledCtx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})
}

func TestHistoryService_NilGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("nil context returns error safely without panic", func(t *testing.T) {
		svc := NewHistoryService(&mockHistoryReader{})
		var nilCtx context.Context
		entries, err := svc.GetHistory(nilCtx)
		if err == nil || !strings.Contains(err.Error(), "nil context") {
			t.Fatalf("expected nil context error, got: %v", err)
		}
		if entries != nil {
			t.Errorf("expected nil entries, got: %+v", entries)
		}
	})

	t.Run("nil receiver returns error safely without panic", func(t *testing.T) {
		var svc *HistoryService
		entries, err := svc.GetHistory(ctx)
		if err == nil || !strings.Contains(err.Error(), "uninitialized or nil") {
			t.Fatalf("expected uninitialized error, got: %v", err)
		}
		if entries != nil {
			t.Errorf("expected nil entries, got: %+v", entries)
		}
	})

	t.Run("nil reader in service returns error safely without panic", func(t *testing.T) {
		svc := NewHistoryService(nil)
		entries, err := svc.GetHistory(ctx)
		if err == nil || !strings.Contains(err.Error(), "uninitialized or nil") {
			t.Fatalf("expected nil sync log reader error, got: %v", err)
		}
		if entries != nil {
			t.Errorf("expected nil entries, got: %+v", entries)
		}
	})
}
