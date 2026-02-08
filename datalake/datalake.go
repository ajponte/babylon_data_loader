package datalake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/csv"

	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"
)

var (
	errTargetFileNotFound = errors.New("the valid directory target was not found")
	errCreateDirectory    = errors.New("the valid directory target was not found")
	errMoveFile           = errors.New("failed to move file")
)

func ValidFileNotFoundError(filePath string) error {
	return fmt.Errorf("%w, %s", errTargetFileNotFound, filePath)
}

func CreateDirectoryError(directoryPath string) error {
	return fmt.Errorf("%w, %s", errCreateDirectory, directoryPath)
}

func MoveFileError(source string, target string) error {
	return fmt.Errorf("%w, %s, %s", errMoveFile, source, target)
}

// IngestCSVFiles processes all CSV files in a given directory and uploads them to MongoDB.
func IngestCSVFiles(
	ctx context.Context,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csv.Parser,
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

	// Initialize new stats counter with the number of files to process.
	stats := NewStats()
	stats.TotalFiles = len(files)

	logger.InfoContext(ctx, "looping through files", "files", files)

	// Ingest all files.
	for _, file := range files {
		err = ingestCSVFile( // Fix 1: changed := to =
			ctx,
			repo,
			extractor,
			parser,
			file,
			unprocessedDir,
			processedDir,
			moveProcessedFiles,
			*stats,
			*logger)
		if err != nil {
			logger.ErrorContext(ctx, "failed to ingest CSV file", "file", file.Name(), "error", err)
			stats.AddFailure(file.Name(), err.Error())
		}
	}

	return stats, nil
}

// Ingest a CSV file into the datalake.
func ingestCSVFile(
	ctx context.Context,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csv.Parser,
	file os.DirEntry,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
	stats Stats,
	logger slog.Logger,
) error {
	if !validateCSVFile(file) {
		reason := "Not a valid CSV file"
		stats.AddFailure(file.Name(), reason)
		logger.WarnContext(ctx, "file was not processed", "fileName", file.Name(), "reason", reason)
		return fmt.Errorf("file %s is not a valid CSV file", file.Name()) // Fix 4
	}

	// Process the file.
	err := processFile(
		ctx,
		logger,
		repo,
		extractor,
		parser,
		file,
		unprocessedDir,
		processedDir,
		moveProcessedFiles)
	if err != nil {
		stats.AddFailure(file.Name(), err.Error())
		logger.ErrorContext(ctx, "failed to process file", "file", file.Name(), "error", err)
		return fmt.Errorf("failed to process file %s: %w", file.Name(), err) // Fix 4
	}
	// Fix 5: Removed else block
	stats.IncrementProcessed()
	return nil
}

// Process the file in the directory.
// This function will:
//   - Parse the unprocessedFile csv in unprocessedDir row by row.
//   - Map each row to mongo datalake models.
//   - Upsert the models to appropriate collections.
//   - Move the file to the unprocessedDir, only if the moveProcessedFiles
//     flag is enabled.
func processFile(
	ctx context.Context,
	logger slog.Logger,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csv.Parser,
	unprocessedFile os.DirEntry,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) error {
	sourceInfo, err := extractor.ExtractInfo(unprocessedFile.Name())
	if err != nil {
		return fmt.Errorf("failed to extract source info: %w", err)
	}
	dataSource := sourceInfo.DataSource
	accountID := sourceInfo.AccountID

	unprocessedFilePath := sanitizeFilePath(unprocessedFile, unprocessedDir)

	// Parse raw records.
	rawRecords, _, err := parser.Parse(ctx, unprocessedFilePath, dataSource, accountID)
	if err != nil {
		return err
	}

	// Create transaction mappings.
	transactions, err := mapRawRecordsToTransactions(ctx, rawRecords, dataSource, accountID)
	if err != nil {
		return err
	}

	// Upsert documents to datalake collection.
	if err = repo.BulkUpsertTransactions(ctx, transactions); err != nil {
		return fmt.Errorf("failed to bulk upsert transactions: %w", err)
	}

	// Move the file, only if moveProcessedFiles is enabled.
	if moveProcessedFiles {
		err = moveFile(ctx, unprocessedFilePath, processedDir, logger)
		if err != nil {
			return fmt.Errorf("failed to move file: %w", err)
		}
	}

	return nil
}

// Return a sanitized path to the file.
func sanitizeFilePath(file os.DirEntry, dir string) string {
	// Return the shortest path name to the file.
	sanitizedFileName := filepath.Clean(file.Name())

	// Build unprocessed file path.
	filePath := filepath.Join(dir, sanitizedFileName)

	return filePath
}

func getPostingDate(record map[string]string, validHeaders []string) string {
	for _, header := range validHeaders {
		if date, ok := record[header]; ok && date != "" {
			return date
		}
	}
	return ""
}

func mapRawRecordsToTransactions(
	ctx context.Context,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
) ([]model.Transaction, error) {
	logger := bcontext.LoggerFromContext(ctx)
	var transactions []model.Transaction

	// ValidPostingDateHeaders is a list of valid header names for the posting date column.
	validPostingDateHeaders := []string{
		"Post Date",
		"Posting Date",
		"post date",
		"posting date",
	}

	for _, record := range rawRecords {
		postingDateStr := getPostingDate(record, validPostingDateHeaders)
		if postingDateStr == "" {
			logger.WarnContext(ctx, "Skipping record with empty posting date", "record", record)
			continue
		}

		parsedDate, parseErr := time.Parse("01/02/2006", postingDateStr)
		if parseErr != nil {
			logger.WarnContext(
				ctx,
				"Skipping record with invalid date format",
				"date", postingDateStr,
				"error", parseErr,
			)
			continue
		}

		amountStr := record["amount"]
		amount, convErr := strconv.ParseFloat(amountStr, 64)
		if convErr != nil {
			logger.WarnContext(ctx, "Skipping record with invalid amount format", "amount", amountStr, "error", convErr)
			continue
		}

		balance := 0.0
		if balanceStr, ok := record["balance"]; ok && balanceStr != "" {
			parsedBalance, balanceConvErr := strconv.ParseFloat(balanceStr, 64)
			if balanceConvErr != nil {
				logger.WarnContext(
					ctx,
					"Skipping record with invalid balance format",
					"balance", balanceStr,
					"error", balanceConvErr,
				)
			} else {
				balance = parsedBalance
			}
		}

		transactions = append(transactions, model.Transaction{
			Details:        record["details"],
			PostingDate:    parsedDate.Format("01/02/2006"), // Store as formatted string
			Description:    record["description"],
			Amount:         amount,
			Category:       record["category"],
			Type:           record["type"],
			Balance:        balance,
			CheckOrSlipNum: record["check or slip #"],
			DataSource:     dataSource,
			AccountID:      accountID,
		})
	}

	if len(rawRecords) > 0 && len(transactions) == 0 {
		return nil, fmt.Errorf("no valid transactions could be processed from %d raw records", len(rawRecords))
	}

	return transactions, nil
}

// Move the file from processedFilePath to processedDir.
func moveFile(
	ctx context.Context,
	processedFilePath string,
	processedDir string,
	logger slog.Logger,
) error {
	if err := checkProcessedDir(ctx, processedDir, logger); err != nil { // Fix 2
		return fmt.Errorf("failed to check/create processed directory: %w", err)
	}

	// Make sure we're at the base-path of the source directory.
	fileName := filepath.Base(processedFilePath)

	// Build processed file path.
	newPath := filepath.Join(processedDir, fileName)

	// Cleanup the processed file.
	moveErr := moveProcessedFile(fileName, newPath) // Fix 3: Renamed 'error' to 'moveErr'
	if moveErr != nil {
		return MoveFileError(fileName, processedDir)
	}

	return nil
}

// Attempt to permanently move the moveProcessedFile file by renaming it.
func moveProcessedFile(processedFilePath string, newPath string) error {
	var err error
	if err = os.Rename(processedFilePath, newPath); err != nil {
		return CreateDirectoryError(err.Error())
	}

	return nil
}

func checkProcessedDir(ctx context.Context, processedDir string, logger slog.Logger) error {
	var err error
	if _, err = os.Stat(processedDir); os.IsNotExist(err) {
		if err = os.MkdirAll(processedDir, 0o750); err != nil {
			logger.DebugContext(ctx, "failed to create processed directory")
			return ValidFileNotFoundError(processedDir)
		}
	}
	logger.DebugContext(ctx, "Processed Directory already exists.")
	return nil
}

// Return true only if the entry is a valid csv.
func validateCSVFile(
	file os.DirEntry,
) bool {
	if file.IsDir() || (!strings.HasSuffix(file.Name(), ".csv") && !strings.HasSuffix(file.Name(), ".CSV")) {
		return false
	}
	return true
}
