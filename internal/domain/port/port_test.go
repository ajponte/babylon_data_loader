package port_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

// mockExtractor implements port.Extractor for testing contract compliance.
type mockExtractor struct {
	sourceType string
	batch      *model.IngestionBatch
	err        error
}

func (m *mockExtractor) SourceType() string {
	return m.sourceType
}

func (m *mockExtractor) Extract(
	_ context.Context,
	_ io.Reader,
	_ model.SourceLineage,
) (*model.IngestionBatch, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.batch, nil
}

// mockRepository implements port.Repository for testing contract compliance.
type mockRepository struct {
	syncLog *model.SyncLog
	saveErr error
	logErr  error
}

func (m *mockRepository) SaveBatch(
	_ context.Context,
	_ *model.IngestionBatch,
) (*model.SyncLog, error) {
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	return m.syncLog, nil
}

func (m *mockRepository) LogSync(_ context.Context, _ *model.SyncLog) error {
	return m.logErr
}

// mockNotifier implements port.ProgressNotifier for testing contract compliance.
type mockNotifier struct {
	stages   []string
	currents []int
	totals   []int
}

func (m *mockNotifier) NotifyProgress(stage string, current int, total int) {
	m.stages = append(m.stages, stage)
	m.currents = append(m.currents, current)
	m.totals = append(m.totals, total)
}

func TestPortContracts(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

	t.Run("Extractor port contract", func(t *testing.T) {
		expectedBatch := &model.IngestionBatch{
			Destination: model.DestChaseChecking,
			AccountID:   "chk-1",
			RecordCount: 1,
			IngestedAt:  now,
		}
		var ext port.Extractor = &mockExtractor{
			sourceType: "chase_checking",
			batch:      expectedBatch,
		}

		if ext.SourceType() != "chase_checking" {
			t.Errorf("expected SourceType 'chase_checking', got %q", ext.SourceType())
		}

		meta := model.SourceLineage{
			FileName:   "test.csv",
			RowNumber:  1,
			IngestedAt: now,
		}
		reader := strings.NewReader("col1,col2\nval1,val2")
		batch, err := ext.Extract(ctx, reader, meta)
		if err != nil {
			t.Fatalf("unexpected extract error: %v", err)
		}
		if batch.AccountID != "chk-1" {
			t.Errorf("expected AccountID 'chk-1', got %q", batch.AccountID)
		}
	})

	t.Run("Repository port contract", func(t *testing.T) {
		expectedLog := &model.SyncLog{
			FileName:       "test.csv",
			CollectionName: "transactions_chase",
			RecordCount:    10,
			Status:         model.SyncStatusSuccess,
			SyncedAt:       now,
		}
		var repo port.Repository = &mockRepository{
			syncLog: expectedLog,
		}

		batch := &model.IngestionBatch{
			Destination: model.DestChaseChecking,
			RecordCount: 10,
		}
		log, err := repo.SaveBatch(ctx, batch)
		if err != nil {
			t.Fatalf("unexpected SaveBatch error: %v", err)
		}
		if log.Status != model.SyncStatusSuccess {
			t.Errorf("expected status 'SUCCESS', got %q", log.Status)
		}

		if logErr := repo.LogSync(ctx, log); logErr != nil {
			t.Fatalf("unexpected LogSync error: %v", logErr)
		}
	})

	t.Run("ProgressNotifier port contract", func(t *testing.T) {
		notifier := &mockNotifier{}
		var p port.ProgressNotifier = notifier

		p.NotifyProgress("PARSING", 0, 100)
		p.NotifyProgress("SAVING", 50, 100)
		p.NotifyProgress("COMPLETE", 100, 100)

		if len(notifier.stages) != 3 {
			t.Fatalf("expected 3 stage notifications, got %d", len(notifier.stages))
		}
		if notifier.stages[0] != "PARSING" || notifier.stages[1] != "SAVING" || notifier.stages[2] != "COMPLETE" {
			t.Errorf("unexpected stages: %v", notifier.stages)
		}
		if notifier.currents[1] != 50 || notifier.totals[1] != 100 {
			t.Errorf("unexpected progress at step 1: %d/%d", notifier.currents[1], notifier.totals[1])
		}
	})
}
