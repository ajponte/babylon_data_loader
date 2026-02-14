package datalake

import (
	"context"
	"fmt"
	"os"

	bcontext "babylon/dataloader/appcontext"
	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/repository"
)

// Client defines the interface for the datalake client.
type Client interface {
	IngestCSVFiles(
		ctx context.Context,
		repo repository.Repository,
		extractor datasource.InfoExtractor,
		parser csvparser.Parser,
		unprocessedDir string,
		processedDir string,
		moveProcessedFiles bool,
	) (*Stats, error)
}

// client is the concrete implementation of the Client interface.
type client struct{}

// NewClient creates a new Client.
func NewClient() Client {
	return &client{}
}

// IngestCSVFiles processes all CSV files in a given directory and uploads them to MongoDB.
func (c *client) IngestCSVFiles(
	ctx context.Context,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csvparser.Parser,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) (*Stats, error) {
	logger := bcontext.LoggerFromContext(ctx)
	logger.InfoContext(ctx, "Reading data from sink", "sink", unprocessedDir)

	files, err := os.ReadDir(unprocessedDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	stats := NewStats()
	stats.TotalFiles = len(files)

	logger.InfoContext(ctx, "looping through files", "files", files)

	// Create a new CSVFileProcessor instance.
	processor := NewCSVFileProcessor(
		repo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		moveProcessedFiles,
		stats,
		*logger,
	)

	// Ingest all files.
	for _, file := range files {
		err = processor.ingestCSVFile( // Now calling the method on the processor
			ctx,
			file)
		if err != nil {
			logger.ErrorContext(ctx, "failed to ingest CSV file", "file", file.Name(), "error", err)
			stats.AddFailure(file.Name(), err.Error())
		}
	}

	return stats, nil
}
