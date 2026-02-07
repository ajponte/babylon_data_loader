package datalake

import (
	"context"
	"fmt"
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

	stats := NewStats()
	stats.TotalFiles = len(files)

	logger.InfoContext(ctx, "looping through files", "files", files)

	for _, file := range files {
		// Validate that's it's a real CSV file.
		if !validateFile(file) {
			reason := "Not a valid CSV file"
			stats.AddFailure(file.Name(), reason)
			logger.WarnContext(ctx, "file was not processed", "fileName", file.Name(), "reason", reason)
		}
		// Process the file.
		err = processFile(ctx, repo, extractor, parser, file, unprocessedDir, processedDir, moveProcessedFiles)
		if err != nil {
			stats.AddFailure(file.Name(), err.Error())
			logger.ErrorContext(ctx, "failed to process file", "file", file.Name(), "error", err)
		} else {
			stats.IncrementProcessed()
		}
	}

	return stats, nil
}

// Return true only if the entry pointed to by FILE is valid.
func validateFile(
	file os.DirEntry,
) bool {
	if file.IsDir() || (!strings.HasSuffix(file.Name(), ".csv") && !strings.HasSuffix(file.Name(), ".CSV")) {
		return false
	}
	return true
}

// Process the file.
func processFile(
	ctx context.Context,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csv.Parser,
	file os.DirEntry,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) error {
	sourceInfo, err := extractor.ExtractInfo(file.Name())
	if err != nil {
		return fmt.Errorf("failed to extract source info: %w", err)
	}
	dataSource := sourceInfo.DataSource
	accountID := sourceInfo.AccountID

	cleanFileName := filepath.Clean(file.Name())
	if strings.HasPrefix(cleanFileName, "../") {
		return csv.ValidFileNotFoundError(file.Name())
	}

	filePath := filepath.Join(unprocessedDir, cleanFileName)
	rawRecords, _, err := parser.Parse(ctx, filePath, dataSource, accountID)
	if err != nil {
		return err
	}

	transactions, err := mapRawRecordsToTransactions(ctx, rawRecords, dataSource, accountID)
	if err != nil {
		return err
	}

	if err = repo.BulkUpsertTransactions(ctx, transactions); err != nil {
		return fmt.Errorf("failed to bulk upsert transactions: %w", err)
	}

	if moveProcessedFiles {
		err = moveFile(filePath, processedDir)
		if err != nil {
			return fmt.Errorf("failed to move file: %w", err)
		}
	}

	return nil
}

// mapRawRecordsToTransactions converts a slice of raw CSV records (map[string]string)
// into a slice of model.Transaction structs, performing necessary type conversions and validations.
//
//nolint:unparam // error is always nil for now, but may be used later
func mapRawRecordsToTransactions(
	ctx context.Context,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
) ([]model.Transaction, error) {
	logger := bcontext.LoggerFromContext(ctx)
	var transactions []model.Transaction

	for _, record := range rawRecords {
		postingDateStr := record["posting date"]
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

	return transactions, nil
}

func moveFile(filePath, processedDir string) error {
	var err error
	if _, err = os.Stat(processedDir); os.IsNotExist(err) {
		if err = os.MkdirAll(processedDir, 0o750); err != nil {
			return fmt.Errorf("failed to create processed directory '%s': %w", processedDir, err)
		}
	}

	fileName := filepath.Base(filePath)
	newPath := filepath.Join(processedDir, fileName)

	if err = os.Rename(filePath, newPath); err != nil {
		return fmt.Errorf("failed to move file from '%s' to '%s': %w", filePath, newPath, err)
	}

	return nil
}
