package datalake

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	bcontext "babylon/dataloader/appcontext"
	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/repository"
)

type IngestFileOptions struct {
	UnprocessedDir     string
	MoveProcessedFiles bool
	ProcessedDir       string
	Reporter           ProgressReporter
}

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

	IngestCSVFile(
		ctx context.Context,
		repo repository.Repository,
		parser csvparser.Parser,
		filePath string,
		dataSource string,
		accountID string,
		opts IngestFileOptions,
	) (*Stats, error)
}

type client struct{}

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

// IngestCSVFile processes a single CSV file, copying it to UnprocessedDir first, and then uploading it to MongoDB.
func (c *client) IngestCSVFile(
	ctx context.Context,
	repo repository.Repository,
	parser csvparser.Parser,
	filePath string,
	dataSource string,
	accountID string,
	opts IngestFileOptions,
) (*Stats, error) {
	logger := bcontext.LoggerFromContext(ctx)
	fileName := filepath.Base(filePath)

	stats := NewStats()
	stats.TotalFiles = 1

	if opts.Reporter != nil {
		opts.Reporter.Report(ProgressEvent{
			Phase:    PhaseValidating,
			FileName: fileName,
			Message:  "Starting file ingestion validation",
		})
	}

	// 1. Copy the file to UnprocessedDir
	if err := os.MkdirAll(opts.UnprocessedDir, 0o750); err != nil {
		errStr := fmt.Sprintf("failed to create unprocessed directory: %v", err)
		if opts.Reporter != nil {
			opts.Reporter.Report(ProgressEvent{
				Phase:    PhaseFailed,
				FileName: fileName,
				Message:  errStr,
			})
		}
		stats.AddFailure(fileName, errStr)
		return stats, fmt.Errorf("%s: %w", errStr, err)
	}

	destPath := filepath.Join(opts.UnprocessedDir, fileName)

	// Copy utility
	srcFile, err := os.Open(filePath)
	if err != nil {
		errStr := fmt.Sprintf("failed to open source file for copy: %v", err)
		if opts.Reporter != nil {
			opts.Reporter.Report(ProgressEvent{
				Phase:    PhaseFailed,
				FileName: fileName,
				Message:  errStr,
			})
		}
		stats.AddFailure(fileName, errStr)
		return stats, fmt.Errorf("%s: %w", errStr, err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		errStr := fmt.Sprintf("failed to create destination file: %v", err)
		if opts.Reporter != nil {
			opts.Reporter.Report(ProgressEvent{
				Phase:    PhaseFailed,
				FileName: fileName,
				Message:  errStr,
			})
		}
		stats.AddFailure(fileName, errStr)
		return stats, fmt.Errorf("%s: %w", errStr, err)
	}
	defer dstFile.Close()

	if _, copyErr := io.Copy(dstFile, srcFile); copyErr != nil {
		errStr := fmt.Sprintf("failed to copy file contents: %v", copyErr)
		if opts.Reporter != nil {
			opts.Reporter.Report(ProgressEvent{
				Phase:    PhaseFailed,
				FileName: fileName,
				Message:  errStr,
			})
		}
		stats.AddFailure(fileName, errStr)
		return stats, fmt.Errorf("%s: %w", errStr, copyErr)
	}

	processor := NewCSVFileProcessor(
		repo,
		nil, // No extractor needed since metadata is explicitly passed
		parser,
		opts.UnprocessedDir,
		opts.ProcessedDir,
		opts.MoveProcessedFiles,
		stats,
		*logger,
	)

	err = processor.processSingleFile(ctx, fileName, dataSource, accountID, opts.Reporter)
	if err != nil {
		logger.ErrorContext(ctx, "failed to ingest CSV file", "file", fileName, "error", err)
		return stats, err
	}

	return stats, nil
}
