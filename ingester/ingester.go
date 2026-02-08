package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"babylon/dataloader/config"
	"babylon/dataloader/csv"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/repository"
	"babylon/dataloader/storage"
)

// Sink orchestrates the data ingestion process by calling datalake.IngestCSVFiles.
// It holds all the necessary dependencies and configuration for that call.
type Sink struct {
	Logger             *slog.Logger
	Config             *config.Config
	Repo               repository.Repository
	Extractor          datasource.InfoExtractor
	Parser             csv.Parser
	UnprocessedDir     string
	ProcessedDir       string
	MoveProcessedFiles bool
}

// NewSink creates a new Sink instance.
func NewSink(
	logger *slog.Logger,
	cfg *config.Config,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csv.Parser,
) *Sink {
	return &Sink{
		Logger:             logger,
		Config:             cfg,
		Repo:               repo,
		Extractor:          extractor,
		Parser:             parser,
		UnprocessedDir:     cfg.UnprocessedDir,
		ProcessedDir:       cfg.ProcessedDir,
		MoveProcessedFiles: cfg.MoveProcessedFiles,
	}
}

// Ingest handles the main data ingestion process.
func (s *Sink) Ingest(ctx context.Context) error {
	s.Logger.DebugContext(ctx, "Starting data ingestion process")

	// Directory existence check
	if _, err := os.Stat(s.Config.UnprocessedDir); err != nil || os.IsNotExist(err) {
		s.Logger.ErrorContext(
			ctx,
			"The directory does not exist. Please create it and place your CSV files inside.",
			"dir", s.Config.UnprocessedDir,
			"error", err,
		)
		return fmt.Errorf("stat check for directory %s: %w", s.Config.UnprocessedDir, err)
	}

	// MongoDB connection
	client, err := storage.ConnectToMongoDB(ctx, s.Config.MongoURI)
	if err != nil {
		s.Logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", err)
		return fmt.Errorf("connection to MongoDB failed: %w", err)
	}
	defer func() {
		if deferErr := client.Disconnect(ctx); deferErr != nil {
			s.Logger.ErrorContext(ctx, "Error disconnecting from MongoDB", "error", deferErr)
		}
	}()
	s.Logger.InfoContext(ctx, "Successfully connected to MongoDB.")

	// Call datalake.IngestCSVFiles directly
	stats, err := datalake.IngestCSVFiles(
		ctx,
		s.Repo,
		s.Extractor,
		s.Parser,
		s.UnprocessedDir,
		s.ProcessedDir,
		s.MoveProcessedFiles,
	)
	if err != nil {
		s.Logger.ErrorContext(ctx, "Error ingesting CSV files", "error", err)
		return fmt.Errorf("ingestion of CSV files failed: %w", err)
	}

	s.Logger.InfoContext(ctx, "Data ingestion process completed successfully.")
	stats.Log(s.Logger)

	return nil
}
