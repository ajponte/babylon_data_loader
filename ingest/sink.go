package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"babylon/dataloader/config"
	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/repository"
	"babylon/dataloader/storage"
)

// SinkDependencies holds all the dependencies for the Sink.
type SinkDependencies struct {
	Logger         *slog.Logger
	Config         *config.Config
	Repo           repository.Repository
	Extractor      datasource.InfoExtractor
	Parser         csvparser.Parser
	DatalakeClient datalake.Client
}

// Sink orchestrates the data ingestion process by calling datalake.IngestCSVFiles.
// It holds all the necessary dependencies and configuration for that call.
type Sink struct {
	deps               SinkDependencies
	UnprocessedDir     string
	ProcessedDir       string
	MoveProcessedFiles bool
}

// NewSink creates a new Sink instance.
func NewSink(deps SinkDependencies) *Sink {
	return &Sink{
		deps:               deps,
		UnprocessedDir:     deps.Config.UnprocessedDir,
		ProcessedDir:       deps.Config.ProcessedDir,
		MoveProcessedFiles: deps.Config.MoveProcessedFiles,
	}
}

// Ingest handles the main data ingestion process.
func (s *Sink) Ingest(ctx context.Context) error {
	s.deps.Logger.DebugContext(ctx, "Starting data ingestion process")

	// Directory existence check
	if _, err := os.Stat(s.UnprocessedDir); err != nil || os.IsNotExist(err) {
		s.deps.Logger.ErrorContext(
			ctx,
			"The directory does not exist. Please create it and place your CSV files inside.",
			"dir", s.UnprocessedDir,
			"error", err,
		)
		return fmt.Errorf("stat check for directory %s: %w", s.UnprocessedDir, err)
	}

	// MongoDB connection
	client, err := storage.ConnectToMongoDBFunc(ctx, s.deps.Config.MongoURI)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", err)
		return fmt.Errorf("connection to MongoDB failed: %w", err)
	}
	defer func() {
		if deferErr := client.Disconnect(ctx); deferErr != nil {
			s.deps.Logger.ErrorContext(ctx, "Error disconnecting from MongoDB", "error", deferErr)
		}
	}()
	s.deps.Logger.InfoContext(ctx, "Successfully connected to MongoDB.")

	// Call datalake.IngestCSVFiles directly
	stats, err := s.deps.DatalakeClient.IngestCSVFiles(
		ctx,
		s.deps.Repo,
		s.deps.Extractor,
		s.deps.Parser,
		s.UnprocessedDir,
		s.ProcessedDir,
		s.MoveProcessedFiles,
	)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "Error ingesting CSV files", "error", err)
		return fmt.Errorf("ingestion of CSV files failed: %w", err)
	}

	s.deps.Logger.InfoContext(ctx, "Data ingestion process completed successfully.")
	stats.Log(s.deps.Logger)

	return nil
}
