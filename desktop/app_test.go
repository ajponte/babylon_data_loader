package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/config"
	"babylon/dataloader/internal/app/ingest"
	domainmodel "babylon/dataloader/internal/domain/model"
)

type mockRepo struct {
	syncLogs   []domainmodel.SyncLog
	retLog     *domainmodel.SyncLog
	saveErr    error
	listErr    error
	logErr     error
	closeErr   error
	closed     bool
	closeCalls int
	closeCtx   context.Context
	mu         sync.Mutex
}

var _ repository = (*mockRepo)(nil)

func (m *mockRepo) SaveBatch(ctx context.Context, batch *domainmodel.IngestionBatch) (*domainmodel.SyncLog, error) {
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	if m.retLog != nil {
		return m.retLog, nil
	}
	count := int64(0)
	collection := ""
	if batch != nil {
		count = int64(batch.RecordCount)
		collection = string(batch.Destination)
	}
	return &domainmodel.SyncLog{
		CollectionName: collection,
		RecordCount:    count,
		UpsertedCount:  count,
		Status:         domainmodel.SyncStatusSuccess,
	}, nil
}

func (m *mockRepo) LogSync(ctx context.Context, log *domainmodel.SyncLog) error {
	if log != nil {
		m.syncLogs = append(m.syncLogs, *log)
	}
	return m.logErr
}

func (m *mockRepo) ListSyncLogs(ctx context.Context, limit int64) ([]domainmodel.SyncLog, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.syncLogs, nil
}

type recordedContext struct {
	context.Context
	errDuringClose error
}

func (r recordedContext) Err() error {
	return r.errDuringClose
}

func (m *mockRepo) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.closeCalls++
	m.closeCtx = recordedContext{
		Context:        ctx,
		errDuringClose: ctx.Err(),
	}
	return m.closeErr
}

func newTestApp(repo repository) *App {
	app := NewApp()
	app.ctx = context.Background()
	app.cfg = &config.Config{MongoURI: "mongodb://localhost:27017"}
	if repo != nil {
		app.repo = repo
		app.historyService = NewHistoryService(repo)
		notifier := NewWailsProgressNotifier(app.ctx)
		app.notifier = notifier
		pipe, _ := newDesktopPipeline(repo, notifier)
		app.pipeline = pipe
		app.dbConn = true
	}
	return app
}

func createTempCSV(t *testing.T, filename string, content string) string {
	t.Helper()
	dir := t.TempDir()
	filePath := filepath.Join(dir, filename)
	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to create temp CSV: %v", err)
	}
	return filePath
}

const validSyntheticCSV = "Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #\n" +
	"DEBIT,10/31/2026,Coffee Shop,Food & Drink,-4.50,Debit Card,1000.00,\n"

func TestApp_OnStartup_Success(t *testing.T) {
	app := NewApp()
	mock := &mockRepo{}
	app.connectRepo = func(ctx context.Context, uri string) (repository, error) {
		return mock, nil
	}

	ctx := context.Background()
	app.OnStartup(ctx)

	if !app.dbConn {
		t.Error("expected dbConn to be true on successful startup connection")
	}
	if app.repo == nil {
		t.Error("expected repo to be initialized")
	}
	if app.pipeline == nil {
		t.Error("expected pipeline to be initialized")
	}
	if app.historyService == nil {
		t.Error("expected historyService to be initialized")
	}
	if app.notifier == nil {
		t.Error("expected notifier to be initialized")
	}
}

func TestApp_OnStartup_Failure(t *testing.T) {
	app := NewApp()
	app.connectRepo = func(ctx context.Context, uri string) (repository, error) {
		return nil, errors.New("connection timeout")
	}

	ctx := context.Background()
	app.OnStartup(ctx)

	if app.dbConn {
		t.Error("expected dbConn to be false on startup failure")
	}
	if app.repo != nil {
		t.Error("expected repo to be nil on failure")
	}
	if app.pipeline != nil {
		t.Error("expected pipeline to be nil on failure")
	}
}

func TestApp_RetryConnectDB(t *testing.T) {
	t.Run("success after failure", func(t *testing.T) {
		app := NewApp()
		app.connectRepo = func(ctx context.Context, uri string) (repository, error) {
			return nil, errors.New("initial connection error")
		}
		ctx := context.Background()
		app.OnStartup(ctx)

		if app.dbConn {
			t.Fatal("expected connection to fail initially")
		}

		mock := &mockRepo{}
		app.connectRepo = func(ctx context.Context, uri string) (repository, error) {
			return mock, nil
		}

		success := app.RetryConnectDB()
		if !success {
			t.Error("expected RetryConnectDB to return true on success")
		}
		if !app.dbConn {
			t.Error("expected dbConn to be true after successful retry")
		}
	})

	t.Run("re-failure", func(t *testing.T) {
		app := NewApp()
		app.connectRepo = func(ctx context.Context, uri string) (repository, error) {
			return nil, errors.New("error")
		}
		ctx := context.Background()
		app.OnStartup(ctx)

		success := app.RetryConnectDB()
		if success {
			t.Error("expected RetryConnectDB to return false on connection failure")
		}
		if app.dbConn {
			t.Error("expected dbConn to remain false")
		}
	})

	t.Run("already connected", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		success := app.RetryConnectDB()
		if !success {
			t.Error("expected RetryConnectDB to return true when already connected")
		}
	})

	t.Run("called before startup", func(t *testing.T) {
		app := NewApp()
		success := app.RetryConnectDB()
		if success {
			t.Error("expected RetryConnectDB to return false when ctx is nil")
		}
	})
}

func TestApp_IngestFile_Guards(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		app := NewApp()
		_, err := app.IngestFile("file.csv", "synthetic", "0000")
		if err == nil {
			t.Error("expected error when app is not initialized")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		app.dbConn = false

		_, err := app.IngestFile("file.csv", "synthetic", "0000")
		if err == nil {
			t.Error("expected error when database is not connected")
		}
	})

	t.Run("empty file path", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)

		_, err := app.IngestFile("", "synthetic", "0000")
		if err == nil {
			t.Error("expected error on empty file path")
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		app.ctx = ctx

		_, err := app.IngestFile("file.csv", "synthetic", "0000")
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("non-csv extension", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)

		_, err := app.IngestFile("file.txt", "synthetic", "0000")
		if err == nil || !errors.Is(err, ingest.ErrNotCSV) {
			t.Errorf("expected ErrNotCSV, got %v", err)
		}
	})
}

func TestApp_ProcessFile_Success(t *testing.T) {
	mock := &mockRepo{}
	app := newTestApp(mock)

	csvPath := createTempCSV(t, "synthetic-0000.csv", validSyntheticCSV)

	stats, err := app.IngestFile(csvPath, "synthetic", "0000")
	if err != nil {
		t.Fatalf("IngestFile failed: %v", err)
	}

	if stats.TotalFiles != 1 {
		t.Errorf("expected TotalFiles 1, got %d", stats.TotalFiles)
	}
	if stats.ProcessedFiles != 1 {
		t.Errorf("expected ProcessedFiles 1, got %d", stats.ProcessedFiles)
	}
	if stats.FailedFiles != 0 {
		t.Errorf("expected FailedFiles 0, got %d", stats.FailedFiles)
	}
	if len(stats.Failures) != 0 {
		t.Errorf("expected 0 failures, got %v", stats.Failures)
	}

	// Decision U3: verify the file is read in-place and still exists at original path.
	if _, statErr := os.Stat(csvPath); os.IsNotExist(statErr) {
		t.Errorf("expected original file to remain on disk (Decision U3), but was deleted")
	}
}

func TestApp_IngestFile_Error(t *testing.T) {
	t.Run("non-existent file", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)

		stats, err := app.IngestFile("/tmp/does-not-exist-synthetic.csv", "synthetic", "0000")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
		if stats.FailedFiles != 1 {
			t.Errorf("expected FailedFiles 1, got %d", stats.FailedFiles)
		}
		if stats.ProcessedFiles != 0 {
			t.Errorf("expected ProcessedFiles 0, got %d", stats.ProcessedFiles)
		}
	})

	t.Run("pipeline save failure", func(t *testing.T) {
		mock := &mockRepo{
			saveErr: errors.New("mongo disk full"),
		}
		app := newTestApp(mock)

		csvPath := createTempCSV(t, "synthetic-0000.csv", validSyntheticCSV)
		stats, err := app.IngestFile(csvPath, "synthetic", "0000")
		if err == nil {
			t.Fatal("expected error on save failure")
		}
		if stats.FailedFiles != 1 {
			t.Errorf("expected FailedFiles 1, got %d", stats.FailedFiles)
		}
	})
}

func TestApp_GetHistory_Parity(t *testing.T) {
	mock := &mockRepo{
		syncLogs: []domainmodel.SyncLog{
			{
				FileName:       "synthetic-0000.csv",
				FileHash:       "abc123hash",
				CollectionName: "transactions_synthetic",
				RecordCount:    10,
				UpsertedCount:  8,
				MatchedCount:   2,
				Status:         domainmodel.SyncStatusSuccess,
				SyncedAt:       time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC),
				DurationMs:     42,
			},
		},
	}
	app := newTestApp(mock)

	history, err := app.GetHistory()
	if err != nil {
		t.Fatalf("GetHistory failed: %v", err)
	}

	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}

	entry := history[0]
	if entry.FileName != "synthetic-0000.csv" {
		t.Errorf("expected FileName 'synthetic-0000.csv', got '%s'", entry.FileName)
	}
	if entry.FileHash != "abc123hash" {
		t.Errorf("expected FileHash 'abc123hash', got '%s'", entry.FileHash)
	}
	if entry.CollectionName != "transactions_synthetic" {
		t.Errorf("expected CollectionName 'transactions_synthetic', got '%s'", entry.CollectionName)
	}
	if entry.RecordCount != 10 {
		t.Errorf("expected RecordCount 10, got %d", entry.RecordCount)
	}
	if entry.UpsertedCount != 8 {
		t.Errorf("expected UpsertedCount 8, got %d", entry.UpsertedCount)
	}
	if entry.MatchedCount != 2 {
		t.Errorf("expected MatchedCount 2, got %d", entry.MatchedCount)
	}
	if entry.Status != "SUCCESS" {
		t.Errorf("expected Status 'SUCCESS', got '%s'", entry.Status)
	}
	if entry.DurationMs != 42 {
		t.Errorf("expected DurationMs 42, got %d", entry.DurationMs)
	}
	// Verify legacy dual-aliased fields (Divergence 10)
	if entry.RecordsUploaded != 8 {
		t.Errorf("expected RecordsUploaded 8, got %d", entry.RecordsUploaded)
	}
	if !entry.SyncTimestamp.Equal(entry.SyncedAt) {
		t.Errorf("expected SyncTimestamp to match SyncedAt, got %v vs %v", entry.SyncTimestamp, entry.SyncedAt)
	}
}

func TestApp_GetHistory_Failure(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		app := NewApp()
		_, err := app.GetHistory()
		if err == nil {
			t.Error("expected error when context is nil")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		app.dbConn = false

		_, err := app.GetHistory()
		if err == nil {
			t.Error("expected error when database is not connected")
		}
	})

	t.Run("repo error", func(t *testing.T) {
		expectedErr := errors.New("read error from mongo")
		mock := &mockRepo{
			listErr: expectedErr,
		}
		app := newTestApp(mock)

		_, err := app.GetHistory()
		if err == nil || !errors.Is(err, expectedErr) {
			t.Errorf("expected repo error %v, got %v", expectedErr, err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		app.ctx = ctx

		_, err := app.GetHistory()
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("nil receiver", func(t *testing.T) {
		var nilApp *App
		_, err := nilApp.GetHistory()
		if err == nil {
			t.Error("expected error on nil receiver")
		}
	})
}

func TestApp_OnShutdown(t *testing.T) {
	t.Run("closes initialized repository", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		if !app.dbConn {
			t.Fatal("expected app to be connected initially")
		}

		app.OnShutdown(context.Background())

		if !mock.closed {
			t.Error("expected repository to be closed on shutdown")
		}
		if mock.closeCalls != 1 {
			t.Errorf("expected 1 close call, got %d", mock.closeCalls)
		}
		if mock.closeCtx == nil {
			t.Fatal("expected closeCtx to be recorded")
		}
		deadline, ok := mock.closeCtx.Deadline()
		if !ok {
			t.Error("expected closeCtx to have an active deadline")
		} else if time.Until(deadline) <= 0 || time.Until(deadline) > defaultShutdownTimeout {
			t.Errorf("unexpected close deadline: %v", time.Until(deadline))
		}
		if app.dbConn {
			t.Error("expected dbConn to be false after shutdown")
		}
		if app.repo != nil {
			t.Error("expected repo pointer to be cleared after shutdown")
		}
		if app.pipeline != nil {
			t.Error("expected pipeline to be cleared after shutdown")
		}
		if app.historyService != nil {
			t.Error("expected historyService to be cleared after shutdown")
		}
		if app.ctx != nil {
			t.Error("expected ctx to be cleared after shutdown")
		}
	})

	t.Run("nil receiver is safe", func(t *testing.T) {
		var nilApp *App
		nilApp.OnShutdown(context.Background())
	})

	t.Run("nil repository is safe", func(t *testing.T) {
		app := newTestApp(nil)
		app.OnShutdown(context.Background())
		if app.repo != nil {
			t.Error("expected repo to remain nil")
		}
	})

	t.Run("nil context is safe", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		var nilCtx context.Context
		app.OnShutdown(nilCtx)
		if !mock.closed {
			t.Error("expected repository to be closed when ctx is nil")
		}
		if app.repo != nil {
			t.Error("expected repo pointer to be cleared after shutdown")
		}
	})

	t.Run("handles close error without panic", func(t *testing.T) {
		mock := &mockRepo{closeErr: errors.New("simulated close failure")}
		app := newTestApp(mock)
		app.OnShutdown(context.Background())
		if !mock.closed {
			t.Error("expected Close to be called despite error")
		}
		if app.repo != nil {
			t.Error("expected repo pointer to be cleared after shutdown")
		}
	})

	t.Run("idempotent when called repeatedly", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		app.OnShutdown(context.Background())
		app.OnShutdown(context.Background())
		if !mock.closed {
			t.Error("expected mock repo to remain closed")
		}
		if mock.closeCalls != 1 {
			t.Errorf("expected exactly 1 Close call across repeated shutdowns, got %d", mock.closeCalls)
		}
	})

	t.Run("pre-canceled context still permits graceful close", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Pre-cancel

		app.OnShutdown(canceledCtx)

		if !mock.closed {
			t.Error("expected repository to be closed even with pre-canceled context")
		}
		if mock.closeCtx == nil {
			t.Fatal("expected closeCtx to be recorded")
		}
		if mock.closeCtx.Err() != nil {
			t.Errorf("expected detached context without cancellation, got %v", mock.closeCtx.Err())
		}
		deadline, ok := mock.closeCtx.Deadline()
		if !ok {
			t.Error("expected detached context to retain an active timeout deadline")
		} else if time.Until(deadline) <= 0 || time.Until(deadline) > defaultShutdownTimeout {
			t.Errorf("unexpected close deadline on detached context: %v", time.Until(deadline))
		}
	})

	t.Run("concurrent calls are race-free", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				app.OnShutdown(context.Background())
			}()
		}
		wg.Wait()
		mock.mu.Lock()
		calls := mock.closeCalls
		mock.mu.Unlock()
		if calls != 1 {
			t.Errorf("expected exactly 1 Close call, got %d", calls)
		}
	})

	t.Run("post-shutdown operations fail predictably", func(t *testing.T) {
		mock := &mockRepo{}
		app := newTestApp(mock)
		app.OnShutdown(context.Background())

		_, ingestErr := app.IngestFile("some_file.csv", "synthetic", "0000")
		if ingestErr == nil {
			t.Error("expected IngestFile to fail after shutdown")
		}

		_, historyErr := app.GetHistory()
		if historyErr == nil {
			t.Error("expected GetHistory to fail after shutdown")
		}

		connected := app.RetryConnectDB()
		if connected {
			t.Error("expected RetryConnectDB to return false after shutdown")
		}
	})
}

const validChaseCheckingCSV = "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
	"DEBIT,10/31/2026,Coffee Shop,-4.50,Debit Card,1000.00,\n"

func TestApp_IngestFile_AutomaticSourceResolution(t *testing.T) {
	mock := &mockRepo{}
	app := newTestApp(mock)

	csvPath := createTempCSV(t, "Chase1234_Activity_20261112.csv", validChaseCheckingCSV)

	// Pass empty dataSource and accountID to exercise automatic ingest.ResolveSource fallback
	stats, err := app.IngestFile(csvPath, "", "")
	if err != nil {
		t.Fatalf("IngestFile with auto source resolution failed: %v", err)
	}

	if stats.TotalFiles != 1 {
		t.Errorf("expected TotalFiles 1, got %d", stats.TotalFiles)
	}
	if stats.ProcessedFiles != 1 {
		t.Errorf("expected ProcessedFiles 1, got %d", stats.ProcessedFiles)
	}
	if stats.FailedFiles != 0 {
		t.Errorf("expected FailedFiles 0, got %d", stats.FailedFiles)
	}
}

func TestApp_ConcurrentAccess(t *testing.T) {
	mock := &mockRepo{}
	app := newTestApp(mock)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = app.RetryConnectDB()
		}()
		go func() {
			defer wg.Done()
			_, _ = app.GetHistory()
		}()
	}

	wg.Wait()
}
