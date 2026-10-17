package datalake

import (
	"bufio"
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
	csvparser "babylon/dataloader/csv"

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
	dataSource := sourceInfo.DataSource
	accountID := sourceInfo.AccountID

	unprocessedFilePath := sanitizeFilePath(unprocessedFile, p.UnprocessedDir)

	// Parse raw records.
	rawRecords, _, err := p.Parser.Parse(ctx, unprocessedFilePath, dataSource, accountID)
	if err != nil {
		return err
	}

	switch datasource.DataSource(sourceInfo.DataSource) {
	case datasource.Fidelity:
		positions, mapErr := mapRawRecordsToFidelityPositions(
			ctx,
			rawRecords,
			unprocessedFilePath,
			unprocessedFile.Name(),
		)
		if mapErr != nil {
			return fmt.Errorf("failed to map Fidelity positions: %w", mapErr)
		}
		if _, err = p.Repo.BulkUpsertFidelityPositions(ctx, positions); err != nil {
			return fmt.Errorf("failed to bulk upsert Fidelity positions: %w", err)
		}
	case datasource.Generic, datasource.Chase, datasource.Synthetic:
		fallthrough
	default:
		// Create transaction mappings.
		transactions, mapErr := mapRawRecordsToTransactions(ctx, rawRecords, dataSource, accountID)
		if mapErr != nil {
			return mapErr
		}

		// Upsert documents to datalake collection.
		if _, err = p.Repo.BulkUpsertTransactions(ctx, transactions); err != nil {
			return fmt.Errorf("failed to bulk upsert transactions: %w", err)
		}
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

	rawRecords, totalRecords, err := p.parseCSVFile(
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

	p.reportProgress(reporter, ProgressEvent{
		Phase:        PhaseUpserting,
		FileName:     fileName,
		Message:      "Upserting to database",
		TotalRecords: totalRecords,
	})

	var upsertStats repository.UpsertStats

	switch datasource.DataSource(dataSource) {
	case datasource.Fidelity:
		stats, upsertErr := p.upsertFidelityPositions(ctx, fileName, rawRecords, unprocessedFilePath, reporter)
		if upsertErr != nil {
			return upsertErr
		}
		upsertStats = stats

	case datasource.Generic, datasource.Chase, datasource.Synthetic:
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
	positions, mapErr := mapRawRecordsToFidelityPositions(ctx, rawRecords, unprocessedFilePath, fileName)
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

func (p *CSVFileProcessor) upsertTransactions(
	ctx context.Context,
	fileName string,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
	reporter ProgressReporter,
) (repository.UpsertStats, error) {
	transactions, mapErr := mapRawRecordsToTransactions(ctx, rawRecords, dataSource, accountID)
	if mapErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, mapErr, reporter)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return repository.UpsertStats{}, p.handleFailure(fileName, ctxErr, reporter)
	}

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

func getPostingDate(record map[string]string, validHeaders []string) string {
	for _, header := range validHeaders {
		if date, ok := record[header]; ok && date != "" {
			return date
		}
	}
	return ""
}

func mapRawRecordsToFidelityPositions(
	ctx context.Context,
	rawRecords []map[string]string,
	filePath string,
	fileName string,
) ([]model.FidelityPosition, error) {
	if len(rawRecords) == 0 {
		return []model.FidelityPosition{}, nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	fileHash, err := ComputeFileSHA256(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to compute sha256 for file %s: %w", filePath, err)
	}

	asOfDate := extractFidelityAsOfDate(filePath)
	ingestedAt := time.Now().UTC()

	positions := make([]model.FidelityPosition, 0, len(rawRecords))
	for _, record := range rawRecords {
		var rowNumber int64
		if rowStr, ok := record["__row_number"]; ok && rowStr != "" {
			if parsedRow, parseErr := strconv.ParseInt(rowStr, 10, 64); parseErr == nil {
				rowNumber = parsedRow
			}
		}

		source := model.SourceLineage{
			FileName:   fileName,
			RowNumber:  rowNumber,
			IngestedAt: ingestedAt,
			SHA256:     fileHash,
		}

		pos, mapErr := model.MapRawRecordToFidelityPosition(record, asOfDate, source)
		if mapErr != nil {
			return nil, fmt.Errorf("failed to map raw record to Fidelity position (row %d): %w", rowNumber, mapErr)
		}
		positions = append(positions, pos)
	}

	return positions, nil
}

func extractFidelityAsOfDate(filePath string) time.Time {
	file, err := os.Open(filePath)
	if err != nil {
		return time.Now().UTC()
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		clean := strings.Trim(line, `" `)
		if strings.HasPrefix(clean, "Date downloaded ") {
			if parsedDate, parseErr := model.ParseFidelityDateDownloaded(line); parseErr == nil {
				return parsedDate.UTC()
			}
		}
	}

	return time.Now().UTC()
}

func mapRawRecordsToTransactions(
	ctx context.Context,
	rawRecords []map[string]string,
	dataSource string,
	accountID string,
) ([]model.Transaction, error) {
	logger := bcontext.LoggerFromContext(ctx)

	validPostingDateHeaders := []string{
		"Post Date",
		"Posting Date",
		"post date",
		"posting date",
	}

	transactions := fromRecords(
		ctx,
		dataSource,
		accountID,
		rawRecords,
		validPostingDateHeaders,
		*logger,
	)

	if len(rawRecords) > 0 && len(transactions) == 0 {
		return nil, fmt.Errorf("no valid transactions could be processed from %d raw records", len(rawRecords))
	}

	return transactions, nil
}

// Map raw records to transaction DTOs.
func fromRecords(
	ctx context.Context,
	dataSource string,
	accountID string,
	rawRecords []map[string]string,
	validPostingDateHeaders []string,
	logger slog.Logger,
) []model.Transaction {
	var transactions []model.Transaction
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
	return transactions
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
		return MoveFileError(fileName, p.ProcessedDir)
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

func checkProcessedDir(ctx context.Context, processedDir string) error {
	logger := bcontext.LoggerFromContext(ctx)
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
