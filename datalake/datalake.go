package datalake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
	csvparser "babylon/dataloader/csv"

	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"
)

var (
	errTargetFileNotFound = errors.New("the valid directory target was not found")
	errCreateDirectory    = errors.New("failed to create directory")
	errMoveFile           = errors.New("failed to move file")
)

const (
	msgUpsertingToDB = "Upserting to database"
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

// CSVFileProcessor encapsulates dependencies and configuration for processing CSV files.
type CSVFileProcessor struct {
	Repo               repository.Repository
	Extractor          datasource.InfoExtractor
	Parser             csvparser.Parser
	UnprocessedDir     string
	ProcessedDir       string
	MoveProcessedFiles bool
	Stats              *Stats
	Logger             slog.Logger
	S3URI              string
}

// NewCSVFileProcessor creates a new CSVFileProcessor instance.
func NewCSVFileProcessor(
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csvparser.Parser,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
	stats *Stats,
	logger slog.Logger,
) *CSVFileProcessor {
	return &CSVFileProcessor{
		Repo:               repo,
		Extractor:          extractor,
		Parser:             parser,
		UnprocessedDir:     unprocessedDir,
		ProcessedDir:       processedDir,
		MoveProcessedFiles: moveProcessedFiles,
		Stats:              stats,
		Logger:             logger,
	}
}

// Ingest a CSV file into the datalake.

func (p *CSVFileProcessor) ingestCSVFile(
	ctx context.Context,

	file os.DirEntry,
) error {
	if !validateCSVFile(file) {
		reason := "Not a valid CSV file"
		p.Stats.AddFailure(file.Name(), reason)
		p.Logger.WarnContext(ctx, "file was not processed", "fileName", file.Name(), "reason", reason)

		return fmt.Errorf("file %s is not a valid CSV file", file.Name())

	}

	// Process the file.
	err := p.processFile(
		ctx,
		file)
	if err != nil {
		p.Stats.AddFailure(file.Name(), err.Error())
		p.Logger.ErrorContext(ctx, "failed to process file", "file", file.Name(), "error", err)

		return fmt.Errorf("failed to process file %s: %w", file.Name(), err)

	}

	p.Stats.IncrementProcessed()

	return nil
}

// Process the file in the directory.
// This function will:
//   - Parse the unprocessedFile csv in unprocessedDir row by row.
//   - Map each row to mongo datalake models.
//   - Upsert the models to appropriate collections.
//   - Move the file to the unprocessedDir, only if the moveProcessedFiles
//     flag is enabled.
func (p *CSVFileProcessor) processFile(
	ctx context.Context,
	unprocessedFile os.DirEntry,
) error {
	sourceInfo, err := p.Extractor.ExtractInfo(unprocessedFile.Name())
	if err != nil {
		return fmt.Errorf("failed to extract source info: %w", err)
	}

	unprocessedFilePath := sanitizeFilePath(unprocessedFile, p.UnprocessedDir)

	// Parse raw records.
	rawRecords, _, err := p.Parser.Parse(ctx, unprocessedFilePath, sourceInfo.DataSource, sourceInfo.AccountID)
	if err != nil {
		return err
	}

	if err = p.routeAndUpsertRecords(
		ctx,
		sourceInfo,
		unprocessedFilePath,
		unprocessedFile.Name(),
		rawRecords,
	); err != nil {
		return err
	}

	// Move the file, only if moveProcessedFiles is enabled.
	if p.MoveProcessedFiles {
		err = p.moveFile(ctx, unprocessedFilePath)
		if err != nil {
			return fmt.Errorf("failed to move file: %w", err)
		}
	}

	return nil
}

func (p *CSVFileProcessor) routeAndUpsertRecords(
	ctx context.Context,
	sourceInfo *datasource.SourceInfo,
	unprocessedFilePath, fileName string,
	rawRecords []map[string]string,
) error {
	switch datasource.DataSource(sourceInfo.DataSource) {
	case datasource.Fidelity:
		return p.processFidelityRecords(ctx, unprocessedFilePath, fileName, rawRecords)
	case datasource.Chase:
		return p.processChaseRecords(ctx, unprocessedFilePath, fileName, sourceInfo.AccountID, rawRecords)
	case datasource.Generic, datasource.Synthetic:
		fallthrough
	default:
		return p.processGenericRecords(ctx, sourceInfo.DataSource, sourceInfo.AccountID, rawRecords)
	}
}

func (p *CSVFileProcessor) processFidelityRecords(
	ctx context.Context,
	filePath, fileName string,
	rawRecords []map[string]string,
) error {
	mapper, mapInitErr := NewFidelityMapper(ctx, filePath, fileName, p.S3URI)
	if mapInitErr != nil {
		return fmt.Errorf("failed to initialize Fidelity mapper: %w", mapInitErr)
	}
	positions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
	if mapErr != nil {
		return fmt.Errorf("failed to map Fidelity positions: %w", mapErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if _, err := p.Repo.BulkUpsertFidelityPositions(ctx, positions); err != nil {
		return fmt.Errorf("failed to bulk upsert Fidelity positions: %w", err)
	}
	return nil
}

func (p *CSVFileProcessor) processChaseRecords(
	ctx context.Context,
	filePath, fileName, accountID string,
	rawRecords []map[string]string,
) error {
	transactions, err := p.mapChaseTransactions(ctx, filePath, fileName, accountID, rawRecords)
	if err != nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if _, upsertErr := p.Repo.BulkUpsertChaseTransactions(ctx, transactions); upsertErr != nil {
		return fmt.Errorf("failed to bulk upsert Chase transactions: %w", upsertErr)
	}
	return nil
}

func (p *CSVFileProcessor) mapChaseTransactions(
	ctx context.Context,
	filePath, fileName, accountID string,
	rawRecords []map[string]string,
) ([]model.ChaseTransaction, error) {
	mapper, err := NewChaseMapper(ctx, filePath, fileName, p.S3URI, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Chase mapper: %w", err)
	}
	transactions, err := mapper.MapRecordsWithContext(ctx, rawRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to map Chase transactions: %w", err)
	}
	return transactions, nil
}

func (p *CSVFileProcessor) processGenericRecords(
	ctx context.Context,
	dataSource, accountID string,
	rawRecords []map[string]string,
) error {
	transactions, mapErr := mapGenericTransactions(ctx, rawRecords, dataSource, accountID)
	if mapErr != nil {
		return mapErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if _, err := p.Repo.BulkUpsertTransactions(ctx, transactions); err != nil {
		return fmt.Errorf("failed to bulk upsert transactions: %w", err)
	}
	return nil
}

// processSingleFile processes a single file with explicit dataSource, accountID, and progress updates.
func (p *CSVFileProcessor) processSingleFile(
	ctx context.Context,
	fileName string,
	dataSource string,
	accountID string,
	reporter ProgressReporter,
) error {
	unprocessedFilePath := filepath.Join(p.UnprocessedDir, fileName)

	p.reportProgress(reporter, ProgressEvent{
		Phase:    PhaseValidating,
		FileName: fileName,
		Message:  "Validating file format",
	})

	if !strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		return p.handleFailure(fileName, fmt.Errorf("file %s is not a valid CSV file", fileName), reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:    PhaseParsing,
		FileName: fileName,
		Message:  "Parsing CSV rows",
	})

	rawRecords, _, err := p.parseCSVFile(
		ctx,
		unprocessedFilePath,
		fileName,
		dataSource,
		accountID,
		reporter,
	)
	if err != nil {
		return p.handleFailure(fileName, err, reporter)
	}

	var upsertStats repository.UpsertStats

	switch datasource.DataSource(dataSource) {
	case datasource.Fidelity:
		stats, upsertErr := p.upsertFidelityPositions(ctx, fileName, rawRecords, unprocessedFilePath, reporter)
		if upsertErr != nil {
			return upsertErr
		}
		upsertStats = stats

	case datasource.Chase:
		stats, upsertErr := p.upsertChaseTransactions(
			ctx,
			fileName,
			rawRecords,
			unprocessedFilePath,
			accountID,
			reporter,
		)
		if upsertErr != nil {
			return upsertErr
		}
		upsertStats = stats

	case datasource.Generic, datasource.Synthetic:
		fallthrough
	default:
		stats, upsertErr := p.upsertTransactions(ctx, fileName, rawRecords, dataSource, accountID, reporter)
		if upsertErr != nil {
			return upsertErr
		}
		upsertStats = stats
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:          PhaseUpserting,
		FileName:       fileName,
		UpsertedCount:  upsertStats.UpsertedCount,
		DuplicateCount: upsertStats.MatchedCount,
	})

	if p.MoveProcessedFiles {
		p.reportProgress(reporter, ProgressEvent{
			Phase:    PhaseMoving,
			FileName: fileName,
			Message:  "Moving processed file",
		})
		err = p.moveFile(ctx, unprocessedFilePath)
		if err != nil {
			return p.handleFailure(fileName, fmt.Errorf("failed to move file: %w", err), reporter)
		}
	}

	p.Stats.IncrementProcessed()
	p.reportProgress(reporter, ProgressEvent{
		Phase:          PhaseDone,
		FileName:       fileName,
		Message:        "Ingestion completed successfully",
		UpsertedCount:  upsertStats.UpsertedCount,
		DuplicateCount: upsertStats.MatchedCount,
	})

	return nil
}

func (p *CSVFileProcessor) upsertFidelityPositions(
	ctx context.Context,
	fileName string,
	rawRecords []map[string]string,
	unprocessedFilePath string,
	reporter ProgressReporter,
) (repository.UpsertStats, error) {
	mapper, mapInitErr := NewFidelityMapper(
		ctx,
		unprocessedFilePath,
		fileName,
		p.S3URI,
	)
	if mapInitErr != nil {
		return repository.UpsertStats{}, p.handleFailure(
			fileName,
			fmt.Errorf("failed to initialize Fidelity mapper: %w", mapInitErr),
			reporter,
		)
	}

	positions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
	if mapErr != nil {
		return repository.UpsertStats{}, p.handleFailure(
			fileName,
			fmt.Errorf("failed to map Fidelity positions: %w", mapErr),
			reporter,
		)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, ctxErr, reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(positions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertFidelityPositions(ctx, positions)
	if upsertErr != nil {
		return repository.UpsertStats{}, p.handleFailure(
			fileName,
			fmt.Errorf("failed to bulk upsert Fidelity positions: %w", upsertErr),
			reporter,
		)
	}

	return stats, nil
}

func (p *CSVFileProcessor) upsertChaseTransactions(
	ctx context.Context,
	fileName string,
	rawRecords []map[string]string,
	unprocessedFilePath string,
	accountID string,
	reporter ProgressReporter,
) (repository.UpsertStats, error) {
	transactions, err := p.mapChaseTransactions(ctx, unprocessedFilePath, fileName, accountID, rawRecords)
	if err != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, err, reporter)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, ctxErr, reporter)
	}

	return p.executeChaseUpsert(ctx, fileName, transactions, reporter)
}

func (p *CSVFileProcessor) executeChaseUpsert(
	ctx context.Context,
	fileName string,
	transactions []model.ChaseTransaction,
	reporter ProgressReporter,
) (repository.UpsertStats, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, ctxErr, reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(transactions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertChaseTransactions(ctx, transactions)
	if upsertErr != nil {
		return repository.UpsertStats{}, p.handleFailure(
			fileName,
			fmt.Errorf("failed to bulk upsert Chase transactions: %w", upsertErr),
			reporter,
		)
	}

	return stats, nil
}

func (p *CSVFileProcessor) upsertTransactions(
	ctx context.Context,
	fileName string,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
	reporter ProgressReporter,
) (repository.UpsertStats, error) {
	transactions, mapErr := mapGenericTransactions(ctx, rawRecords, dataSource, accountID)
	if mapErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, mapErr, reporter)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, ctxErr, reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(transactions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertTransactions(ctx, transactions)
	if upsertErr != nil {
		return repository.UpsertStats{}, p.handleFailure(
			fileName,
			fmt.Errorf("failed to bulk upsert transactions: %w", upsertErr),
			reporter,
		)
	}

	return stats, nil
}

func (p *CSVFileProcessor) parseCSVFile(
	ctx context.Context,
	filePath string,
	fileName string,
	dataSource string,
	accountID string,
	reporter ProgressReporter,
) ([]map[string]string, int64, error) {
	if streamingParser, ok := p.Parser.(csvparser.StreamingParser); ok {
		return streamingParser.ParseStream(
			ctx,
			filePath,
			dataSource,
			accountID,
			func(progress csvparser.RowProgress) {
				p.reportProgress(reporter, ProgressEvent{
					Phase:         PhaseParsing,
					FileName:      fileName,
					CurrentRecord: progress.CurrentRecord,
					TotalRecords:  progress.TotalRecords,
				})
			},
		)
	}
	return p.Parser.Parse(ctx, filePath, dataSource, accountID)
}

func (p *CSVFileProcessor) reportProgress(reporter ProgressReporter, event ProgressEvent) {
	if reporter != nil {
		reporter.Report(event)
	}
}

func (p *CSVFileProcessor) handleFailure(fileName string, err error, reporter ProgressReporter) error {
	p.Stats.AddFailure(fileName, err.Error())
	p.reportProgress(reporter, ProgressEvent{
		Phase:    PhaseFailed,
		FileName: fileName,
		Message:  err.Error(),
	})
	return err
}

// Return a sanitized path to the file.
func sanitizeFilePath(file os.DirEntry, dir string) string {
	// Return the shortest path name to the file.
	sanitizedFileName := filepath.Clean(file.Name())

	// Build unprocessed file path.
	filePath := filepath.Join(dir, sanitizedFileName)

	return filePath
}

func parseGenericPostingDate(record map[string]string) (time.Time, bool, error) {
	dateHeaders := []string{
		"Post Date",
		"Posting Date",
		"post date",
		"posting date",
	}

	for _, header := range dateHeaders {
		if dateStr, ok := record[header]; ok && dateStr != "" {
			parsed, err := time.Parse("01/02/2006", dateStr)
			if err != nil {
				return time.Time{}, false, fmt.Errorf("invalid date %q: %w", dateStr, err)
			}
			return parsed, true, nil
		}
	}
	return time.Time{}, false, nil
}

func parseGenericBalance(ctx context.Context, record map[string]string, logger *slog.Logger) float64 {
	balanceStr, ok := record["balance"]
	if !ok || balanceStr == "" {
		return 0.0
	}
	parsed, err := strconv.ParseFloat(balanceStr, 64)
	if err != nil {
		logger.WarnContext(ctx, "Skipping invalid balance format", "balance", balanceStr, "error", err)
		return 0.0
	}
	return parsed
}

func parseGenericRecord(
	ctx context.Context,
	record map[string]string,
	dataSource, accountID string,
	logger *slog.Logger,
) (model.Transaction, bool) {
	parsedDate, found, err := parseGenericPostingDate(record)
	if err != nil {
		logger.WarnContext(ctx, "Skipping record with invalid date format", "error", err)
		return model.Transaction{}, false
	}
	if !found {
		logger.WarnContext(ctx, "Skipping record with empty posting date", "record", record)
		return model.Transaction{}, false
	}

	amountStr := record["amount"]
	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil {
		logger.WarnContext(ctx, "Skipping record with invalid amount format", "amount", amountStr, "error", err)
		return model.Transaction{}, false
	}

	balance := parseGenericBalance(ctx, record, logger)

	return model.Transaction{
		Details:        record["details"],
		PostingDate:    parsedDate.Format("01/02/2006"),
		Description:    record["description"],
		Amount:         amount,
		Category:       record["category"],
		Type:           record["type"],
		Balance:        balance,
		CheckOrSlipNum: record["check or slip #"],
		DataSource:     dataSource,
		AccountID:      accountID,
	}, true
}

func mapGenericTransactions(
	ctx context.Context,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
) ([]model.Transaction, error) {
	logger := bcontext.LoggerFromContext(ctx)
	transactions := make([]model.Transaction, 0, len(rawRecords))

	for _, record := range rawRecords {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		if tx, ok := parseGenericRecord(ctx, record, dataSource, accountID, logger); ok {
			transactions = append(transactions, tx)
		}
	}

	if len(rawRecords) > 0 && len(transactions) == 0 {
		return nil, fmt.Errorf("no valid transactions could be processed from %d raw records", len(rawRecords))
	}

	return transactions, nil
}

// Move the file from processedFilePath to processedDir.
func (p *CSVFileProcessor) moveFile(
	ctx context.Context,
	processedFilePath string,
) error {
	if err := checkProcessedDir(ctx, p.ProcessedDir); err != nil {
		return fmt.Errorf("failed to check/create processed directory: %w", err)
	}

	// Make sure we're at the base-path of the source directory.
	fileName := filepath.Base(processedFilePath)

	// Build processed file path.
	newPath := filepath.Join(p.ProcessedDir, fileName)

	// Cleanup the processed file.
	moveErr := moveProcessedFile(processedFilePath, newPath)
	if moveErr != nil {
		return fmt.Errorf("failed to move file %s to %s: %w", fileName, p.ProcessedDir, moveErr)
	}

	return nil
}

// Attempt to move the processed file by renaming it, falling back to copy-and-remove on cross-device link errors.
func moveProcessedFile(processedFilePath string, newPath string) error {
	if err := os.Rename(processedFilePath, newPath); err != nil {
		if copyErr := copyAndRemoveFile(processedFilePath, newPath); copyErr != nil {
			return fmt.Errorf("failed to move file from %s to %s: %w", processedFilePath, newPath, copyErr)
		}
	}

	return nil
}

func copyAndRemoveFile(srcPath, dstPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source for copy fallback: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open destination for copy fallback: %w", err)
	}
	defer dstFile.Close()

	if _, copyErr := io.Copy(dstFile, srcFile); copyErr != nil {
		return fmt.Errorf("failed to copy file contents: %w", copyErr)
	}

	if closeErr := srcFile.Close(); closeErr != nil {
		return fmt.Errorf("failed to close source file: %w", closeErr)
	}
	if closeErr := dstFile.Close(); closeErr != nil {
		return fmt.Errorf("failed to close destination file: %w", closeErr)
	}

	if removeErr := os.Remove(srcPath); removeErr != nil {
		return fmt.Errorf("failed to remove original file after copy: %w", removeErr)
	}

	return nil
}

func checkProcessedDir(ctx context.Context, processedDir string) error {
	logger := bcontext.LoggerFromContext(ctx)
	var err error
	if _, err = os.Stat(processedDir); os.IsNotExist(err) {
		if err = os.MkdirAll(processedDir, 0o750); err != nil {
			logger.DebugContext(ctx, "failed to create processed directory")
			return fmt.Errorf("%w: %w", CreateDirectoryError(processedDir), err)
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
