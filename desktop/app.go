package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/config"
	"babylon/dataloader/desktop/model"
	"babylon/dataloader/internal/app/ingest"
	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/chase"
	"babylon/dataloader/internal/infra/sources/fidelity"
	"babylon/dataloader/internal/infra/sources/synthetic"
	"babylon/dataloader/internal/infra/storage/localfs"
	"babylon/dataloader/internal/infra/storage/mongo"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// repository defines the composite persistence and sync log read contract required by desktop App.
type repository interface {
	port.Repository
	port.SyncLogReader
	Close(ctx context.Context) error
}

func defaultConnectRepo(ctx context.Context, uri string) (repository, error) {
	return mongo.NewRepository(ctx, mongo.Config{URI: uri})
}

func defaultNewStore(root string) (port.ObjectStore, error) {
	return localfs.New(root)
}

// App represents the desktop application driving adapter for Wails.
type App struct {
	ctx            context.Context
	cfg            *config.Config
	repo           repository
	pipeline       *pipeline.Pipeline
	historyService *HistoryService
	notifier       *WailsProgressNotifier
	connectRepo    func(ctx context.Context, uri string) (repository, error)
	newStore       func(root string) (port.ObjectStore, error)
	mu             sync.RWMutex
	dbConn         bool
	retrying       bool
}

// NewApp creates a new App application struct with production default dependencies.
func NewApp() *App {
	return &App{
		connectRepo: defaultConnectRepo,
		newStore:    defaultNewStore,
	}
}

func (a *App) getConnectRepo() func(context.Context, string) (repository, error) {
	if a != nil && a.connectRepo != nil {
		return a.connectRepo
	}
	return defaultConnectRepo
}

func (a *App) getNewStore() func(string) (port.ObjectStore, error) {
	if a != nil && a.newStore != nil {
		return a.newStore
	}
	return defaultNewStore
}

func newDesktopPipeline(repo port.Repository, notifier port.ProgressNotifier) (*pipeline.Pipeline, error) {
	p := pipeline.New(repo, notifier)
	if err := ingest.RegisterDefaults(
		p,
		chase.NewExtractor(nil),
		fidelity.NewExtractor(),
		synthetic.NewCSVExtractor(),
	); err != nil {
		return nil, fmt.Errorf("failed to register extractors: %w", err)
	}
	return p, nil
}

// OnStartup is called when the app starts. The context is saved so runtime methods can be invoked.
func (a *App) OnStartup(ctx context.Context) {
	a.mu.Lock()
	a.ctx = ctx
	a.mu.Unlock()

	// Update the Wails context in the global log handler if it is set.
	if h, ok := slog.Default().Handler().(*WailsLogHandler); ok {
		h.SetWailsCtx(ctx)
	}

	a.mu.Lock()
	a.cfg = config.LoadConfig(bcontext.WithLogger(ctx, slog.Default()))
	a.notifier = NewWailsProgressNotifier(ctx)
	a.mu.Unlock()

	repo, err := a.getConnectRepo()(ctx, a.cfg.MongoURI)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to MongoDB on startup", "error", err)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return
	}

	pipe, err := newDesktopPipeline(repo, a.notifier)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize pipeline on startup", "error", err)
		_ = repo.Close(ctx)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return
	}

	a.mu.Lock()
	a.repo = repo
	a.pipeline = pipe
	a.historyService = NewHistoryService(repo)
	a.dbConn = true
	a.mu.Unlock()

	slog.InfoContext(ctx, "Successfully connected to MongoDB on startup")
	emitEvent(ctx, "db:status", map[string]any{
		"connected": true,
	})
}

// OnShutdown is called during application teardown to release database connection pools.
func (a *App) OnShutdown(ctx context.Context) {
	if a == nil {
		return
	}

	a.mu.Lock()
	repo := a.repo
	a.repo = nil
	a.pipeline = nil
	a.historyService = nil
	a.dbConn = false
	a.mu.Unlock()

	if repo != nil {
		_ = repo.Close(ctx)
	}
}

// IngestFile processes a single CSV file in-place (Decision U3) without copying or mutating local disk state.
func (a *App) IngestFile(filePath string, dataSource string, accountID string) (ingest.RunStats, error) {
	a.mu.RLock()
	ctx := a.ctx
	cfg := a.cfg
	pipe := a.pipeline
	notifier := a.notifier
	connected := a.dbConn
	a.mu.RUnlock()

	if ctx == nil || cfg == nil {
		return ingest.RunStats{}, errors.New("application is not initialized")
	}

	if err := ctx.Err(); err != nil {
		return ingest.RunStats{}, err
	}

	if !connected || pipe == nil {
		return ingest.RunStats{}, errors.New("database not connected; please connect to MongoDB first")
	}

	if filePath == "" {
		return ingest.RunStats{}, errors.New("file path cannot be empty")
	}

	if !strings.EqualFold(filepath.Ext(filePath), ".csv") {
		return ingest.RunStats{}, ingest.ErrNotCSV
	}

	dir := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)

	store, err := a.getNewStore()(dir)
	if err != nil {
		return ingest.RunStats{}, fmt.Errorf("failed to initialize local store: %w", err)
	}

	if notifier != nil {
		notifier.SetFileName(baseName)
	}

	var override *ingest.SourceRef
	if dataSource != "" || accountID != "" {
		override = &ingest.SourceRef{
			SourceType: dataSource,
			AccountID:  accountID,
		}
	}

	ingestor := ingest.NewIngestor(pipe, store)
	req := ingest.ObjectRequest{
		Key:        baseName,
		ArchiveKey: "", // Decision U3: zero archival/deletion for local desktop files
		Override:   override,
		Notifier:   notifier,
	}

	stats := ingest.RunStats{
		TotalFiles: 1,
		Failures:   make(map[string]string),
	}

	_, ingestErr := ingestor.IngestObject(ctx, req)
	if ingestErr != nil {
		stats.FailedFiles = 1
		stats.Failures[baseName] = ingestErr.Error()
		return stats, ingestErr
	}

	stats.ProcessedFiles = 1
	return stats, nil
}

// RetryConnectDB attempts to reconnect to MongoDB if it is currently in a degraded state.
func (a *App) RetryConnectDB() bool {
	if a == nil {
		return false
	}

	a.mu.Lock()
	if a.retrying {
		a.mu.Unlock()
		return false
	}
	a.retrying = true
	ctx := a.ctx
	cfg := a.cfg
	connected := a.dbConn
	notifier := a.notifier
	existingRepo := a.repo
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.retrying = false
		a.mu.Unlock()
	}()

	if ctx == nil {
		slog.Error("RetryConnectDB called before startup")
		return false
	}

	if connected {
		return true
	}

	if cfg == nil {
		cfg = config.LoadConfig(bcontext.WithLogger(ctx, slog.Default()))
		a.mu.Lock()
		a.cfg = cfg
		a.mu.Unlock()
	}

	if notifier == nil {
		notifier = NewWailsProgressNotifier(ctx)
		a.mu.Lock()
		a.notifier = notifier
		a.mu.Unlock()
	}

	slog.InfoContext(ctx, "Attempting to reconnect to MongoDB...")
	repo, err := a.getConnectRepo()(ctx, cfg.MongoURI)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to reconnect to MongoDB", "error", err)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return false
	}

	pipe, err := newDesktopPipeline(repo, notifier)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize pipeline on reconnect", "error", err)
		_ = repo.Close(ctx)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return false
	}

	if existingRepo != nil {
		_ = existingRepo.Close(ctx)
	}

	a.mu.Lock()
	a.repo = repo
	a.pipeline = pipe
	a.historyService = NewHistoryService(repo)
	a.dbConn = true
	a.mu.Unlock()

	slog.InfoContext(ctx, "Successfully reconnected to MongoDB")
	emitEvent(ctx, "db:status", map[string]any{
		"connected": true,
	})
	return true
}

// emitEvent wraps runtime.EventsEmit to avoid test-environment panics.
var emitEvent = func(ctx context.Context, name string, optionalData ...interface{}) {
	if ctx == nil {
		return
	}
	if flag.Lookup("test.v") != nil {
		return
	}
	runtime.EventsEmit(ctx, name, optionalData...)
}

// GetHistory retrieves the history of completed ingestion runs.
func (a *App) GetHistory() ([]model.SyncLog, error) {
	if a == nil {
		return nil, errors.New("application is not initialized")
	}

	a.mu.RLock()
	ctx := a.ctx
	historySvc := a.historyService
	connected := a.dbConn
	a.mu.RUnlock()

	if ctx == nil {
		return nil, errors.New("application is not initialized")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !connected || historySvc == nil {
		return nil, errors.New("database not connected")
	}

	return historySvc.GetHistory(ctx)
}
