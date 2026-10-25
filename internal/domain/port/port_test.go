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
	_ model.FileMetadata,
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
	stages   []port.ProgressStage
	currents []int
	totals   []int
}

func (m *mockNotifier) NotifyProgress(
	_ context.Context,
	stage port.ProgressStage,
	current int,
	total int,
) {
	m.stages = append(m.stages, stage)
	m.currents = append(m.currents, current)
	m.totals = append(m.totals, total)
}

// mockObjectStore implements port.ObjectStore for testing contract compliance.
type mockObjectStore struct {
	body       io.ReadCloser
	getErr     error
	archiveErr error
	lastGet    string
	lastSrc    string
	lastDest   string
}

func (m *mockObjectStore) DownloadStream(
	_ context.Context,
	key string,
) (io.ReadCloser, error) {
	m.lastGet = key
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.body, nil
}

func (m *mockObjectStore) ArchiveFile(
	_ context.Context,
	srcKey, destKey string,
) error {
	m.lastSrc = srcKey
	m.lastDest = destKey
	return m.archiveErr
}

func TestPortContracts_Extractor(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

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

	meta := model.FileMetadata{
		FileName:   "test.csv",
		FileHash:   "hash123",
		AccountID:  "chk-1",
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
}

func TestPortContracts_Repository(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

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
}

func TestPortContracts_ProgressNotifier(t *testing.T) {
	ctx := context.Background()

	if port.StageParsing != "PARSING" {
		t.Errorf("expected StageParsing 'PARSING', got %q", port.StageParsing)
	}
	if port.StageSaving != "SAVING" {
		t.Errorf("expected StageSaving 'SAVING', got %q", port.StageSaving)
	}
	if port.StageComplete != "COMPLETE" {
		t.Errorf("expected StageComplete 'COMPLETE', got %q", port.StageComplete)
	}

	notifier := &mockNotifier{}
	var p port.ProgressNotifier = notifier

	p.NotifyProgress(ctx, port.StageParsing, 0, 100)
	p.NotifyProgress(ctx, port.StageSaving, 50, 100)
	p.NotifyProgress(ctx, port.StageComplete, 100, 100)

	if len(notifier.stages) != 3 {
		t.Fatalf("expected 3 stage notifications, got %d", len(notifier.stages))
	}
	if notifier.stages[0] != port.StageParsing ||
		notifier.stages[1] != port.StageSaving ||
		notifier.stages[2] != port.StageComplete {
		t.Errorf("unexpected stages: %v", notifier.stages)
	}
	if notifier.currents[1] != 50 || notifier.totals[1] != 100 {
		t.Errorf("unexpected progress at step 1: %d/%d", notifier.currents[1], notifier.totals[1])
	}
}

func TestPortContracts_ObjectStore(t *testing.T) {
	ctx := context.Background()
	content := "statement-date,amount,memo\n2026-01-15,100.00,test"
	body := io.NopCloser(strings.NewReader(content))

	mock := &mockObjectStore{
		body: body,
	}
	var store port.ObjectStore = mock

	rc, err := store.DownloadStream(ctx, "unprocessed/data.csv")
	if err != nil {
		t.Fatalf("unexpected DownloadStream error: %v", err)
	}
	defer rc.Close()

	if mock.lastGet != "unprocessed/data.csv" {
		t.Errorf("expected lastGet 'unprocessed/data.csv', got %q", mock.lastGet)
	}

	data, readErr := io.ReadAll(rc)
	if readErr != nil {
		t.Fatalf("unexpected read error: %v", readErr)
	}
	if string(data) != content {
		t.Errorf("expected content %q, got %q", content, string(data))
	}

	if archiveErr := store.ArchiveFile(ctx, "unprocessed/data.csv", "archive/data.csv"); archiveErr != nil {
		t.Fatalf("unexpected ArchiveFile error: %v", archiveErr)
	}
	if mock.lastSrc != "unprocessed/data.csv" || mock.lastDest != "archive/data.csv" {
		t.Errorf("expected archive keys unprocessed/data.csv -> archive/data.csv, got %s -> %s", mock.lastSrc, mock.lastDest)
	}

	if port.ErrObjectNotFound == nil || port.ErrObjectNotFound.Error() != "object not found" {
		t.Errorf("expected port.ErrObjectNotFound to have message 'object not found'")
	}
}
