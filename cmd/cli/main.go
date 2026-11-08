// Package main provides the command-line interface entrypoint for the
// babylon data-loader application, supporting batch file ingestion and
// synthetic data generation/persistence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/config"
	"babylon/dataloader/internal/app/ingest"
	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/chase"
	"babylon/dataloader/internal/infra/sources/fidelity"
	"babylon/dataloader/internal/infra/sources/synthetic"
	"babylon/dataloader/internal/infra/storage/localfs"
	"babylon/dataloader/internal/infra/storage/mongo"
	legacySynthetic "babylon/dataloader/synthetic"
)

const (
	minArgs              = 2
	statsHeaderMarker    = "--- Ingestion Stats (JSON) ---"
	statsFooterMarker    = "------------------------------"
	syntheticAccountID   = "0000"
	syntheticFilePattern = "synthetic-%d.csv"
	closeTimeout         = 5 * time.Second
)

var (
	// ErrUsage indicates missing or invalid command-line invocation syntax.
	ErrUsage          = errors.New("usage: data-loader <command> [options]")
	uriCredentialMask = regexp.MustCompile(`://([^:@\s]+):([^@\s]+)@`)
)

type repository interface {
	port.Repository
	Close(ctx context.Context) error
}

type objectStore interface {
	port.ObjectStore
	List(ctx context.Context, prefix string) ([]string, error)
}

type deps struct {
	connectRepo func(ctx context.Context, uri string) (repository, error)
	newStore    func(root string) (objectStore, error)
	now         func() time.Time
	logger      *slog.Logger
}

func (d deps) withDefaults() deps {
	if d.connectRepo == nil {
		d.connectRepo = func(ctx context.Context, uri string) (repository, error) {
			return mongo.NewRepository(ctx, mongo.Config{URI: uri})
		}
	}
	if d.newStore == nil {
		d.newStore = func(root string) (objectStore, error) {
			return localfs.New(root)
		}
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.logger == nil {
		d.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}
	return d
}

type slogNotifier struct {
	logger *slog.Logger
}

func (n *slogNotifier) NotifyProgress(ctx context.Context, stage port.ProgressStage, current, total int) {
	if n == nil || n.logger == nil {
		return
	}
	n.logger.InfoContext(ctx, "pipeline progress",
		"stage", string(stage),
		"current", current,
		"total", total,
	)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	ctx := context.Background()

	if len(os.Args) < minArgs {
		logger.ErrorContext(ctx, "Usage: data-loader <command> [options]")
		os.Exit(1)
	}

	d := deps{logger: logger}
	if err := run(ctx, os.Args[1:], d); err != nil {
		logger.ErrorContext(ctx, "Application terminated with an error", "error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, d deps) error {
	if len(args) < 1 {
		return ErrUsage
	}

	d = d.withDefaults()
	runCtx := bcontext.WithLogger(ctx, d.logger)
	cfg := config.LoadConfig(runCtx)

	timeoutCtx, cancel := context.WithTimeout(runCtx, cfg.Timeout)
	defer cancel()

	command := args[0]
	cmdArgs := args[1:]

	switch command {
	case "ingest":
		return runIngest(timeoutCtx, cfg, d)
	case "generate-synthetic-data":
		return runGenerateSyntheticData(timeoutCtx, cmdArgs, cfg, d)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func closeRepoWithTimeout(ctx context.Context, repo repository, logger *slog.Logger) {
	if repo == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	if closeErr := repo.Close(closeCtx); closeErr != nil {
		logger.ErrorContext(closeCtx, "Error disconnecting from repository", "error", closeErr)
	}
}

func runIngest(ctx context.Context, cfg *config.Config, d deps) error {
	repo, err := d.connectRepo(ctx, cfg.MongoURI)
	if err != nil {
		sanitizedErr := sanitizeError(err, cfg.MongoURI)
		d.logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", sanitizedErr)
		return fmt.Errorf("connection to repository failed: %w", sanitizedErr)
	}
	defer closeRepoWithTimeout(ctx, repo, d.logger)

	notifier := &slogNotifier{logger: d.logger}
	p := pipeline.New(repo, notifier, pipeline.WithClock(d.now))
	if regErr := ingest.RegisterDefaults(
		p,
		chase.NewExtractor(nil),
		fidelity.NewExtractor(),
		synthetic.NewCSVExtractor(),
	); regErr != nil {
		return fmt.Errorf("failed to register extractors: %w", regErr)
	}

	// In the local filesystem object store abstraction, the parent directory of
	// UnprocessedDir acts as the storage root, and the base directory name (e.g., "unprocessed")
	// serves as the object key prefix to list and retrieve.
	storeRoot := filepath.Dir(cfg.UnprocessedDir)
	store, err := d.newStore(storeRoot)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}

	prefix := filepath.Base(cfg.UnprocessedDir)
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list unprocessed files: %w", err)
	}

	archivePrefix := filepath.Base(cfg.ProcessedDir)
	reqs := buildObjectRequests(keys, prefix, archivePrefix, cfg.MoveProcessedFiles, notifier)

	ingestor := ingest.NewIngestor(p, store, ingest.WithClock(d.now))
	stats := ingestor.IngestAll(ctx, reqs)
	logStats(ctx, d.logger, stats)
	return nil
}

func buildObjectRequests(
	keys []string,
	prefix, archivePrefix string,
	moveProcessed bool,
	notifier port.ProgressNotifier,
) []ingest.ObjectRequest {
	reqs := make([]ingest.ObjectRequest, 0, len(keys))
	for _, key := range keys {
		archiveKey := ""
		if moveProcessed {
			if ak, ok := ingest.ArchiveKeyFor(key, prefix, archivePrefix); ok {
				archiveKey = ak
			}
		}
		reqs = append(reqs, ingest.ObjectRequest{
			Key:        key,
			ArchiveKey: archiveKey,
			Notifier:   notifier,
		})
	}
	return reqs
}

func runGenerateSyntheticData(ctx context.Context, args []string, cfg *config.Config, d deps) error {
	fs := flag.NewFlagSet("generate-synthetic-data", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	rows := fs.Int("rows", cfg.SyntheticDataRows, "Number of rows to generate")
	dir := fs.String("dir", cfg.SyntheticDataDir, "Directory to write synthetic data to")
	persistToMongo := fs.Bool("persist-to-mongo", false, "Persist synthetic data to MongoDB")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *persistToMongo {
		return runPersistSyntheticData(ctx, *rows, cfg, d)
	}

	d.logger.InfoContext(ctx, "Generating synthetic data")
	if err := legacySynthetic.GenerateSyntheticData(*rows, *dir); err != nil {
		return fmt.Errorf("failed to generate synthetic data: %w", err)
	}
	d.logger.InfoContext(ctx, "Synthetic data generated successfully")
	return nil
}

func runPersistSyntheticData(ctx context.Context, rows int, cfg *config.Config, d deps) error {
	repo, err := d.connectRepo(ctx, cfg.MongoURI)
	if err != nil {
		sanitizedErr := sanitizeError(err, cfg.MongoURI)
		d.logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", sanitizedErr)
		return fmt.Errorf("connection to repository failed: %w", sanitizedErr)
	}
	defer closeRepoWithTimeout(ctx, repo, d.logger)

	notifier := &slogNotifier{logger: d.logger}
	p := pipeline.New(repo, notifier, pipeline.WithClock(d.now))
	synthExt := synthetic.NewExtractor(synthetic.GeneratorOptions{RowCount: rows})
	if regErr := p.Register(ingest.SourceSynthetic, synthExt, model.DestSynthetic); regErr != nil {
		return fmt.Errorf("failed to register synthetic extractor: %w", regErr)
	}

	now := d.now()
	// AccountID "0000" is the fixed synthetic fixture identifier specified in D4 domain invariants.
	meta := model.FileMetadata{
		FileName:   fmt.Sprintf(syntheticFilePattern, now.Unix()),
		AccountID:  syntheticAccountID,
		IngestedAt: now,
	}

	req := pipeline.Request{
		Reader:     strings.NewReader(""),
		Meta:       meta,
		SourceType: ingest.SourceSynthetic,
		Notifier:   notifier,
	}

	if _, runErr := p.Run(ctx, req); runErr != nil {
		return fmt.Errorf("failed to generate and persist synthetic data: %w", runErr)
	}

	d.logger.InfoContext(ctx, "Synthetic data generated and persisted successfully")
	return nil
}

func logStats(ctx context.Context, logger *slog.Logger, stats ingest.RunStats) {
	jsonData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		logger.ErrorContext(ctx, "Failed to marshal stats to JSON", "error", err)
		return
	}
	logger.InfoContext(ctx, statsHeaderMarker)
	logger.InfoContext(ctx, string(jsonData))
	logger.InfoContext(ctx, statsFooterMarker)
}

type sanitizedError struct {
	msg  string
	orig error
}

func (e *sanitizedError) Error() string {
	return e.msg
}

func (e *sanitizedError) Is(target error) bool {
	return errors.Is(e.orig, target)
}

func sanitizeError(err error, rawURI string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if rawURI != "" {
		msg = strings.ReplaceAll(msg, rawURI, "[REDACTED_URI]")
	}
	msg = uriCredentialMask.ReplaceAllString(msg, "://[REDACTED]:[REDACTED]@")
	return &sanitizedError{
		msg:  msg,
		orig: err,
	}
}
