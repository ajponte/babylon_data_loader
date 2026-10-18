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
	TransactionsCollection      = "transactions"
	ChaseTransactionsCollection = "transactions_chase"
	FidelityPositionsCollection = "positions_fidelity"
	msgUpsertingToDB            = "Upserting to database"
	auditFailureTimeout         = 5 * time.Second
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

// recordAuditFailure persists a failed sync log using a detached context with a 5-second timeout,
// ensuring the audit record is saved even if the parent context was canceled or timed out.
func (p *CSVFileProcessor) recordAuditFailure(
	ctx context.Context,
	fileName, fileHash, collectionName, errMsg string,
	recordCount int64,
	startTime time.Time,
) {
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditFailureTimeout)
	defer cancel()

	durationMs := time.Since(startTime).Milliseconds()
	failLog := model.NewFailedSyncLog(
		fileName,
		fileHash,
		collectionName,
		errMsg,
		recordCount,
		startTime,
		durationMs,
	)

	if err := p.Repo.RecordSyncLog(auditCtx, failLog); err != nil {
		p.Logger.WarnContext(auditCtx, "failed to record failure sync log", "fileName", fileName, "error", err)
	}
}

// Ingest a CSV file into the datalake with start-to-finish lifecycle timing and audit logging.
func (p *CSVFileProcessor) ingestCSVFile(
	ctx context.Context,
	file os.DirEntry,
) error {
	startTime := time.Now().UTC()
	fileName := file.Name()

	if !validateCSVFile(file) {
		reason := "Not a valid CSV file"
		p.Stats.AddFailure(fileName, reason)
		p.Logger.WarnContext(ctx, "file was not processed", "fileName", fileName, "reason", reason)

		p.recordAuditFailure(ctx, fileName, "", "", reason, 0, startTime)

		return fmt.Errorf("file %s is not a valid CSV file", fileName)
	}

	err := p.processFileWithAudit(ctx, file, startTime)
	if err != nil {
		p.Stats.AddFailure(fileName, err.Error())
		p.Logger.ErrorContext(ctx, "failed to process file", "file", fileName, "error", err)
		return fmt.Errorf("failed to process file %s: %w", fileName, err)
	}

	p.Stats.IncrementProcessed()
	return nil
}

// processFile processes a CSV file, delegating to processFileWithAudit with the current timestamp.
func (p *CSVFileProcessor) processFile(
	ctx context.Context,
	unprocessedFile os.DirEntry,
) error {
	return p.processFileWithAudit(ctx, unprocessedFile, time.Now().UTC())
}

// processFileWithAudit coordinates the end-to-end ingestion lifecycle for a file:
// source info extraction, CSV parsing, routing/transformation, repository upsert, and audit logging.
func (p *CSVFileProcessor) processFileWithAudit(
	ctx context.Context,
	unprocessedFile os.DirEntry,
	startTime time.Time,
) error {
	fileName := unprocessedFile.Name()
	unprocessedFilePath := sanitizeFilePath(unprocessedFile, p.UnprocessedDir)

	// 1. Source Info Extraction
	sourceInfo, err := p.Extractor.ExtractInfo(fileName)
	if err != nil {
		p.recordAuditFailure(ctx, fileName, "", "", err.Error(), 0, startTime)
		return fmt.Errorf("failed to extract source info: %w", err)
	}

	// 2. CSV Parsing
	rawRecords, _, err := p.Parser.Parse(ctx, unprocessedFilePath, sourceInfo.DataSource, sourceInfo.AccountID)
	if err != nil {
		p.recordAuditFailure(ctx, fileName, "", sourceInfo.DataSource, err.Error(), 0, startTime)
		return err
	}

	// 3. Routing, Transformation & Storage
	upsertStats, collectionName, fileHash, err := p.routeAndUpsertRecordsWithStats(
		ctx,
		sourceInfo,
		unprocessedFilePath,
		fileName,
		rawRecords,
	)
	if err != nil {
		p.recordAuditFailure(
			ctx,
			fileName,
			fileHash,
			collectionName,
			err.Error(),
			int64(len(rawRecords)),
			startTime,
		)
		return err
	}

	// 4. Move Processed File
	if p.MoveProcessedFiles {
		if moveErr := p.moveFile(ctx, unprocessedFilePath); moveErr != nil {
			errMsg := fmt.Sprintf("storage succeeded but file archival failed: %v", moveErr)
			p.recordAuditFailure(
				ctx,
				fileName,
				fileHash,
				collectionName,
				errMsg,
				int64(len(rawRecords)),
				startTime,
			)
			return fmt.Errorf("failed to move file: %w", moveErr)
		}
	}

	// 5. Record Successful Audit Log
	durationMs := time.Since(startTime).Milliseconds()
	successLog := model.NewSuccessSyncLog(
		fileName,
		fileHash,
		collectionName,
		int64(len(rawRecords)),
		upsertStats.UpsertedCount,
		upsertStats.MatchedCount,
		startTime,
		durationMs,
	)
	if logErr := p.Repo.RecordSyncLog(ctx, successLog); logErr != nil {
		p.Logger.WarnContext(ctx, "failed to record success sync log", "fileName", fileName, "error", logErr)
	}

	return nil
}

// routeAndUpsertRecordsWithStats routes raw records to their dedicated mapper and repository upsert,
// returning the resulting UpsertStats, collection name, file hash, and any error encountered.
func (p *CSVFileProcessor) routeAndUpsertRecordsWithStats(
	ctx context.Context,
	sourceInfo *datasource.SourceInfo,
	unprocessedFilePath, fileName string,
	rawRecords []map[string]string,
) (repository.UpsertStats, string, string, error) {
	switch datasource.DataSource(sourceInfo.DataSource) {
	case datasource.Fidelity:
		return p.processFidelityRecords(ctx, unprocessedFilePath, fileName, rawRecords, nil)
	case datasource.Chase:
		return p.processChaseRecords(ctx, unprocessedFilePath, fileName, sourceInfo.AccountID, rawRecords, nil)
	case datasource.Generic, datasource.Synthetic:
		fallthrough
	default:
		return p.processGenericRecords(ctx, sourceInfo.DataSource, sourceInfo.AccountID, fileName, rawRecords, nil)
	}
}

// processFidelityRecords initializes the Fidelity mapper, transforms raw records into portfolio positions,
// reports upsert progress if a reporter is provided, and performs a bulk upsert into the Fidelity positions repository.
func (p *CSVFileProcessor) processFidelityRecords(
	ctx context.Context,
	unprocessedFilePath, fileName string,
	rawRecords []map[string]string,
	reporter ProgressReporter,
) (repository.UpsertStats, string, string, error) {
	mapper, err := NewFidelityMapper(ctx, unprocessedFilePath, fileName, p.S3URI)
	if err != nil {
		return repository.UpsertStats{}, FidelityPositionsCollection, "", fmt.Errorf(
			"failed to initialize Fidelity mapper: %w",
			err,
		)
	}
	positions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
	if mapErr != nil {
		return repository.UpsertStats{}, FidelityPositionsCollection, mapper.FileHash(), fmt.Errorf(
			"failed to map Fidelity positions: %w",
			mapErr,
		)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, FidelityPositionsCollection, mapper.FileHash(), ctxErr
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(positions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertFidelityPositions(ctx, positions)
	if upsertErr != nil {
		return repository.UpsertStats{}, FidelityPositionsCollection, mapper.FileHash(), fmt.Errorf(
			"failed to bulk upsert Fidelity positions: %w",
			upsertErr,
		)
	}
	return stats, FidelityPositionsCollection, mapper.FileHash(), nil
}

// processChaseRecords initializes the Chase mapper, transforms raw records into transactions,
// reports upsert progress if a reporter is provided, and performs a bulk upsert into the Chase transactions repository.
func (p *CSVFileProcessor) processChaseRecords(
	ctx context.Context,
	unprocessedFilePath, fileName, accountID string,
	rawRecords []map[string]string,
	reporter ProgressReporter,
) (repository.UpsertStats, string, string, error) {
	mapper, err := NewChaseMapper(ctx, unprocessedFilePath, fileName, p.S3URI, accountID)
	if err != nil {
		return repository.UpsertStats{}, ChaseTransactionsCollection, "", fmt.Errorf(
			"failed to initialize Chase mapper: %w",
			err,
		)
	}
	transactions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
	if mapErr != nil {
		return repository.UpsertStats{}, ChaseTransactionsCollection, mapper.FileHash(), fmt.Errorf(
			"failed to map Chase transactions: %w",
			mapErr,
		)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, ChaseTransactionsCollection, mapper.FileHash(), ctxErr
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(transactions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertChaseTransactions(ctx, transactions)
	if upsertErr != nil {
		return repository.UpsertStats{}, ChaseTransactionsCollection, mapper.FileHash(), fmt.Errorf(
			"failed to bulk upsert Chase transactions: %w",
			upsertErr,
		)
	}
	return stats, ChaseTransactionsCollection, mapper.FileHash(), nil
}

// processGenericRecords transforms raw records into generic transactions,
// reports upsert progress if a reporter is provided, and performs a bulk upsert into the generic transactions repository.
func (p *CSVFileProcessor) processGenericRecords(
	ctx context.Context,
	dataSource, accountID, fileName string,
	rawRecords []map[string]string,
	reporter ProgressReporter,
) (repository.UpsertStats, string, string, error) {
	collectionName := fmt.Sprintf("%s_%s", TransactionsCollection, dataSource)
	transactions, mapErr := mapGenericTransactions(ctx, rawRecords, dataSource, accountID)
	if mapErr != nil {
		return repository.UpsertStats{}, collectionName, "", mapErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, collectionName, "", ctxErr
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      msgUpsertingToDB,
		TotalRecords: int64(len(transactions)),
	})

	stats, upsertErr := p.Repo.BulkUpsertTransactions(ctx, transactions)
	if upsertErr != nil {
		return repository.UpsertStats{}, collectionName, "", fmt.Errorf(
			"failed to bulk upsert transactions: %w",
			upsertErr,
		)
	}
	return stats, collectionName, "", nil
}

// upsertSingleFileRecords delegates record mapping and database upserting by data source.
func (p *CSVFileProcessor) upsertSingleFileRecords(
	ctx context.Context,
	fileName, dataSource, accountID, unprocessedFilePath string,
	rawRecords []map[string]string,
	reporter ProgressReporter,
) (repository.UpsertStats, string, string, error) {
	switch datasource.DataSource(dataSource) {
	case datasource.Fidelity:
		return p.processFidelityRecords(ctx, unprocessedFilePath, fileName, rawRecords, reporter)
	case datasource.Chase:
		return p.processChaseRecords(ctx, unprocessedFilePath, fileName, accountID, rawRecords, reporter)
	case datasource.Generic, datasource.Synthetic:
		fallthrough
	default:
		return p.processGenericRecords(ctx, dataSource, accountID, fileName, rawRecords, reporter)
	}
}

// processSingleFile processes a single file with explicit dataSource, accountID, and progress updates.
func (p *CSVFileProcessor) processSingleFile(
	ctx context.Context,
	fileName string,
	dataSource string,
	accountID string,
	reporter ProgressReporter,
) error {
	startTime := time.Now().UTC()
	unprocessedFilePath := filepath.Join(p.UnprocessedDir, fileName)

	p.reportProgress(reporter, ProgressEvent{
		Phase:    PhaseValidating,
		FileName: fileName,
		Message:  "Validating file format",
	})

	if !strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		valErr := fmt.Errorf("file %s is not a valid CSV file", fileName)
		p.recordAuditFailure(ctx, fileName, "", "", valErr.Error(), 0, startTime)
		return p.handleFailure(fileName, valErr, reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:    PhaseParsing,
		FileName: fileName,
		Message:  "Parsing CSV rows",
	})

	rawRecords, _, err := p.parseCSVFile(ctx, unprocessedFilePath, fileName, dataSource, accountID, reporter)
	if err != nil {
		p.recordAuditFailure(ctx, fileName, "", dataSource, err.Error(), 0, startTime)
		return p.handleFailure(fileName, err, reporter)
	}

	upsertStats, collectionName, fileHash, upsertErr := p.upsertSingleFileRecords(
		ctx,
		fileName,
		dataSource,
		accountID,
		unprocessedFilePath,
		rawRecords,
		reporter,
	)
	if upsertErr != nil {
		p.recordAuditFailure(
			ctx,
			fileName,
			fileHash,
			collectionName,
			upsertErr.Error(),
			int64(len(rawRecords)),
			startTime,
		)
		return p.handleFailure(fileName, upsertErr, reporter)
	}

	p.reportProgress(reporter, ProgressEvent{
		Phase:          PhaseUpserting,
		FileName:       fileName,
		UpsertedCount:  upsertStats.UpsertedCount,
		DuplicateCount: upsertStats.MatchedCount,
	})

	if finalizeErr := p.finalizeSingleFileSuccess(
		ctx,
		fileName,
		fileHash,
		collectionName,
		unprocessedFilePath,
		rawRecords,
		upsertStats,
		startTime,
		reporter,
	); finalizeErr != nil {
		return p.handleFailure(fileName, finalizeErr, reporter)
	}

	return nil
}

// finalizeSingleFileSuccess handles file archival, success logging, and final progress reporting.
func (p *CSVFileProcessor) finalizeSingleFileSuccess(
	ctx context.Context,
	fileName, fileHash, collectionName, unprocessedFilePath string,
	rawRecords []map[string]string,
	upsertStats repository.UpsertStats,
	startTime time.Time,
	reporter ProgressReporter,
) error {
	if p.MoveProcessedFiles {
		p.reportProgress(reporter, ProgressEvent{
			Phase:    PhaseMoving,
			FileName: fileName,
			Message:  "Moving processed file",
		})
		if err := p.moveFile(ctx, unprocessedFilePath); err != nil {
			errMsg := fmt.Sprintf("storage succeeded but file archival failed: %v", err)
			p.recordAuditFailure(
				ctx,
				fileName,
				fileHash,
				collectionName,
				errMsg,
				int64(len(rawRecords)),
				startTime,
			)
			return fmt.Errorf("failed to move file: %w", err)
		}
	}

	durationMs := time.Since(startTime).Milliseconds()
	successLog := model.NewSuccessSyncLog(
		fileName,
		fileHash,
		collectionName,
		int64(len(rawRecords)),
		upsertStats.UpsertedCount,
		upsertStats.MatchedCount,
		startTime,
		durationMs,
	)
	if logErr := p.Repo.RecordSyncLog(ctx, successLog); logErr != nil {
		p.Logger.WarnContext(ctx, "failed to record success sync log", "fileName", fileName, "error", logErr)
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
		_ = dstFile.Close()
		_ = os.Remove(dstPath)
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
