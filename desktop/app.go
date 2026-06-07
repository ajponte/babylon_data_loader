package main

import (
	"babylon/dataloader/config"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"
	"babylon/dataloader/storage"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"sync"

	bcontext "babylon/dataloader/appcontext"

	csvparser "babylon/dataloader/csv"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct
type App struct {
	ctx      context.Context
	cfg      *config.Config
	mongo    storage.MongoClient
	repo     repository.Repository
	parser   csvparser.Parser
	datalake datalake.Client
	reporter *WailsProgressReporter
	mu       sync.RWMutex
	dbConn   bool
}

// NewApp creates a new App application struct
func NewApp() *App {
	return &App{}
}

// OnStartup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) OnStartup(ctx context.Context) {
	a.mu.Lock()
	a.ctx = ctx
	a.mu.Unlock()

	// Update the Wails context in the global log handler if it's set
	if h, ok := slog.Default().Handler().(*WailsLogHandler); ok {
		h.SetWailsCtx(ctx)
	}

	a.mu.Lock()
	a.cfg = config.LoadConfig(bcontext.WithLogger(ctx, slog.Default()))
	a.parser = csvparser.NewDefaultParser()
	a.datalake = datalake.NewClient()
	a.reporter = NewWailsProgressReporter(ctx)
	a.mu.Unlock()

	// Connect to MongoDB
	client, err := storage.ConnectToMongoDBFunc(ctx, a.cfg.MongoURI)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to MongoDB on startup", "error", err)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return
	}

	a.mu.Lock()
	a.mongo = client
	a.repo = storage.NewMongoRepository(storage.NewMongoProvider(client))
	a.dbConn = true
	a.mu.Unlock()

	slog.InfoContext(ctx, "Successfully connected to MongoDB on startup")
	emitEvent(ctx, "db:status", map[string]any{
		"connected": true,
	})
}

// IngestFile processes a single CSV file, copying it to UnprocessedDir first, and then uploading it to MongoDB.
func (a *App) IngestFile(filePath string, dataSource string, accountID string) (datalake.Stats, error) {
	a.mu.RLock()
	ctx := a.ctx
	repo := a.repo
	parser := a.parser
	dlClient := a.datalake
	cfg := a.cfg
	connected := a.dbConn
	a.mu.RUnlock()

	if ctx == nil || cfg == nil {
		return datalake.Stats{}, fmt.Errorf("application is not initialized")
	}

	if !connected || repo == nil {
		return datalake.Stats{}, fmt.Errorf("database not connected; please connect to MongoDB first")
	}

	opts := datalake.IngestFileOptions{
		UnprocessedDir:     cfg.UnprocessedDir,
		MoveProcessedFiles: cfg.MoveProcessedFiles,
		ProcessedDir:       cfg.ProcessedDir,
		Reporter:           a.reporter,
	}

	ctxWithLog := bcontext.WithLogger(ctx, slog.Default())

	stats, err := dlClient.IngestCSVFile(ctxWithLog, repo, parser, filePath, dataSource, accountID, opts)
	if err != nil {
		if stats != nil {
			return *stats, err
		}
		return datalake.Stats{}, err
	}

	if stats == nil {
		return datalake.Stats{}, fmt.Errorf("nil stats returned from ingestion")
	}

	return *stats, nil
}

// RetryConnectDB attempts to reconnect to MongoDB if it is currently in a degraded state.
func (a *App) RetryConnectDB() bool {
	a.mu.Lock()
	ctx := a.ctx
	cfg := a.cfg
	connected := a.dbConn
	a.mu.Unlock()

	if ctx == nil {
		slog.Error("RetryConnectDB called before startup")
		return false
	}

	if connected {
		return true
	}

	slog.InfoContext(ctx, "Attempting to reconnect to MongoDB...")
	client, err := storage.ConnectToMongoDBFunc(ctx, cfg.MongoURI)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to reconnect to MongoDB", "error", err)
		emitEvent(ctx, "db:status", map[string]any{
			"connected": false,
			"error":     err.Error(),
		})
		return false
	}

	a.mu.Lock()
	a.mongo = client
	a.repo = storage.NewMongoRepository(storage.NewMongoProvider(client))
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
	a.mu.RLock()
	ctx := a.ctx
	repo := a.repo
	connected := a.dbConn
	a.mu.RUnlock()

	if ctx == nil {
		return nil, fmt.Errorf("application is not initialized")
	}

	if !connected || repo == nil {
		return nil, fmt.Errorf("database not connected")
	}

	return repo.GetSyncLogs(ctx)
}
