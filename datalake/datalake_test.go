package datalake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"
)

// ---- Mocks ----

// mockRepository implements repository.Repository for testing.
type mockRepository struct {
	bulkUpsertTransactionsCalled      bool
	transactions                      []model.Transaction
	bulkUpsertChaseTransactionsCalled bool
	chaseTransactions                 []model.ChaseTransaction
	bulkUpsertFidelityPositionsCalled bool
	fidelityPositions                 []model.FidelityPosition
	recordSyncLogCalled               bool
	recordedSyncLog                   model.SyncLog
	recordedSyncLogCtx                context.Context
	recordedSyncLogCtxErr             error
	syncLogs                          []model.SyncLog
	err                               error
	upsertErr                         error
	recordSyncLogErr                  error
}

func (m *mockRepository) BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) (repository.UpsertStats, error) {
	m.bulkUpsertTransactionsCalled = true
	m.transactions = transactions
	err := m.err
	if m.upsertErr != nil {
		err = m.upsertErr
	}
	return repository.UpsertStats{
		UpsertedCount: int64(len(transactions)),
	}, err
}

func (m *mockRepository) BulkUpsertChaseTransactions(ctx context.Context, transactions []model.ChaseTransaction) (repository.UpsertStats, error) {
	m.bulkUpsertChaseTransactionsCalled = true
	m.chaseTransactions = transactions
	err := m.err
	if m.upsertErr != nil {
		err = m.upsertErr
	}
	return repository.UpsertStats{
		UpsertedCount: int64(len(transactions)),
	}, err
}

func (m *mockRepository) BulkUpsertFidelityPositions(ctx context.Context, positions []model.FidelityPosition) (repository.UpsertStats, error) {
	m.bulkUpsertFidelityPositionsCalled = true
	m.fidelityPositions = positions
	err := m.err
	if m.upsertErr != nil {
		err = m.upsertErr
	}
	return repository.UpsertStats{
		UpsertedCount: int64(len(positions)),
	}, err
}

func (m *mockRepository) RecordSyncLog(ctx context.Context, log model.SyncLog) error {
	m.recordSyncLogCalled = true
	m.recordedSyncLog = log
	m.recordedSyncLogCtx = ctx
	m.recordedSyncLogCtxErr = ctx.Err()
	m.syncLogs = append(m.syncLogs, log)
	if m.recordSyncLogErr != nil {
		return m.recordSyncLogErr
	}
	return nil
}

func (m *mockRepository) GetSyncLogs(ctx context.Context) ([]model.SyncLog, error) {
	return nil, m.err
}

// mockInfoExtractor implements datasource.InfoExtractor for testing.
type mockInfoExtractor struct {
	extractInfoCalled bool
	info              *datasource.SourceInfo
	err               error
}

func (m *mockInfoExtractor) ExtractInfo(filename string) (*datasource.SourceInfo, error) {
	m.extractInfoCalled = true
	return m.info, m.err
}

// mockCSVParser implements csv.Parser for testing.
type mockCSVParser struct {
	parseCalled bool
	records     []map[string]string
	err         error
}

func (m *mockCSVParser) Parse(ctx context.Context, filePath string, dataSource string, accountID string) ([]map[string]string, int64, error) {
	m.parseCalled = true
	return m.records, int64(len(m.records)), m.err
}

// ---- Tests ----

func TestProcessFile(t *testing.T) {
	ctx := context.Background()

	// Create a temporary CSV file with one valid record
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "generic_test.csv")

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2023,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}

	// Setup Mocks
	mockRepo := &mockRepository{}
	mockExtractor := &mockInfoExtractor{
		info: &datasource.SourceInfo{
			DataSource: string(datasource.Generic),
			AccountID:  "1234",
		},
	}
	mockParser := &mockCSVParser{
		records: []map[string]string{
			{
				"details":         "DEBIT",
				"posting date":    "01/31/2023",
				"description":     "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
				"category":        "", // Assuming Category is empty in the CSV content
				"amount":          "-75.77",
				"type":            "DEBIT_CARD",
				"balance":         "11190.76",
				"check or slip #": "",
			},
		},
	}
	mockStats := NewStats()
	mockLogger := *slog.New(slog.DiscardHandler)

	// Create CSVFileProcessor
	processor := NewCSVFileProcessor(
		mockRepo,
		mockExtractor,
		mockParser,
		tmpDir, // unprocessedDir
		"",     // processedDir
		false,  // moveProcessedFiles
		mockStats,
		mockLogger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to get file info: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	// Call processFile with mocks
	if processErr := processor.processFile(ctx, dirEntry); processErr != nil { // Call method on processor
		t.Fatalf("processFile failed: %v", processErr)
	}

	// Assertions
	if !mockRepo.bulkUpsertTransactionsCalled {
		t.Errorf("expected BulkUpsertTransactions to be called, but it wasn't")
	}
	if !mockExtractor.extractInfoCalled {
		t.Errorf("expected ExtractInfo to be called, but it wasn't")
	}
	if !mockParser.parseCalled {
		t.Errorf("expected Parse to be called, but it wasn't")
	}

	if len(mockRepo.transactions) != 1 {
		t.Errorf("Expected 1 transaction to be upserted, got %d", len(mockRepo.transactions))
	}
	expectedTransaction := model.Transaction{
		Details:        "DEBIT",
		PostingDate:    "01/31/2023",
		Description:    "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
		Amount:         -75.77,
		Type:           "DEBIT_CARD",
		Balance:        11190.76,
		CheckOrSlipNum: "",
		DataSource:     string(datasource.Generic),
		AccountID:      "1234",
	}

	if mockRepo.transactions[0].Details != expectedTransaction.Details ||
		mockRepo.transactions[0].PostingDate != expectedTransaction.PostingDate ||
		mockRepo.transactions[0].Description != expectedTransaction.Description ||
		mockRepo.transactions[0].Amount != expectedTransaction.Amount ||
		mockRepo.transactions[0].Type != expectedTransaction.Type ||
		mockRepo.transactions[0].Balance != expectedTransaction.Balance ||
		mockRepo.transactions[0].CheckOrSlipNum != expectedTransaction.CheckOrSlipNum ||
		mockRepo.transactions[0].DataSource != expectedTransaction.DataSource ||
		mockRepo.transactions[0].AccountID != expectedTransaction.AccountID {
		t.Errorf("Expected transaction %+v, got %+v", expectedTransaction, mockRepo.transactions[0])
	}
}

func TestProcessFile_PostDateHeader(t *testing.T) {
	ctx := context.Background()

	// Create a temporary CSV file with one valid record using "Post Date"
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Bank_test.csv")

	csvContent := `Details,Post Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2023,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}

	// Setup Mocks
	mockRepo := &mockRepository{}
	mockExtractor := &mockInfoExtractor{
		info: &datasource.SourceInfo{
			DataSource: "bank",
			AccountID:  "5678",
		},
	}
	mockParser := &mockCSVParser{
		records: []map[string]string{
			{
				"details":         "DEBIT",
				"Post Date":       "01/31/2023",
				"description":     "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
				"category":        "",
				"amount":          "-75.77",
				"type":            "DEBIT_CARD",
				"balance":         "11190.76",
				"check or slip #": "",
			},
		},
	}
	mockStats := NewStats()
	mockLogger := *slog.New(slog.DiscardHandler)

	// Create CSVFileProcessor
	processor := NewCSVFileProcessor(
		mockRepo,
		mockExtractor,
		mockParser,
		tmpDir, // unprocessedDir
		"",     // processedDir
		false,  // moveProcessedFiles
		mockStats,
		mockLogger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to get file info: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	// Call processFile with mocks
	if processErr := processor.processFile(ctx, dirEntry); processErr != nil { // Call method on processor
		t.Fatalf("processFile failed: %v", processErr)
	}

	// Assertions
	if !mockRepo.bulkUpsertTransactionsCalled {
		t.Errorf("expected BulkUpsertTransactions to be called, but it wasn't")
	}
	if !mockExtractor.extractInfoCalled {
		t.Errorf("expected ExtractInfo to be called, but it wasn't")
	}
	if !mockParser.parseCalled {
		t.Errorf("expected Parse to be called, but it wasn't")
	}

	if len(mockRepo.transactions) != 1 {
		t.Errorf("Expected 1 transaction to be upserted, got %d", len(mockRepo.transactions))
	}
	expectedTransaction := model.Transaction{
		Details:        "DEBIT",
		PostingDate:    "01/31/2023",
		Description:    "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
		Amount:         -75.77,
		Type:           "DEBIT_CARD",
		Balance:        11190.76,
		CheckOrSlipNum: "",
		DataSource:     "bank",
		AccountID:      "5678",
	}

	if mockRepo.transactions[0].Details != expectedTransaction.Details ||
		mockRepo.transactions[0].PostingDate != expectedTransaction.PostingDate ||
		mockRepo.transactions[0].Description != expectedTransaction.Description ||
		mockRepo.transactions[0].Amount != expectedTransaction.Amount ||
		mockRepo.transactions[0].Type != expectedTransaction.Type ||
		mockRepo.transactions[0].Balance != expectedTransaction.Balance ||
		mockRepo.transactions[0].CheckOrSlipNum != expectedTransaction.CheckOrSlipNum ||
		mockRepo.transactions[0].DataSource != expectedTransaction.DataSource ||
		mockRepo.transactions[0].AccountID != expectedTransaction.AccountID {
		t.Errorf("Expected transaction %+v, got %+v", expectedTransaction, mockRepo.transactions[0])
	}
}

// mockDirEntry implements fs.DirEntry for testing.
type mockDirEntry struct {
	os.FileInfo
}

func newMockDirEntry(info os.FileInfo) mockDirEntry {
	return mockDirEntry{info}
}

func (m mockDirEntry) Type() os.FileMode {
	return m.Mode().Type()
}

func (m mockDirEntry) Info() (os.FileInfo, error) {
	return m, nil
}

type mockProgressReporter struct {
	events []ProgressEvent
}

func (m *mockProgressReporter) Report(ev ProgressEvent) {
	m.events = append(m.events, ev)
}

func TestIngestCSVFile_Success(t *testing.T) {
	ctx := context.Background()

	// Create temporary folders
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")

	if err := os.MkdirAll(sourceDir, 0o750); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2023,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",-75.77,DEBIT_CARD,11190.76,`
	srcFilePath := filepath.Join(sourceDir, "generic_test.csv")
	if err := os.WriteFile(srcFilePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}

	mockRepo := &mockRepository{}
	parser := csvparser.NewDefaultParser()
	client := NewClient()

	reporter := &mockProgressReporter{}

	opts := IngestFileOptions{
		UnprocessedDir:     unprocessedDir,
		ProcessedDir:       processedDir,
		MoveProcessedFiles: true,
		Reporter:           reporter,
	}

	stats, err := client.IngestCSVFile(ctx, mockRepo, parser, srcFilePath, "chase", "1234", opts)
	if err != nil {
		t.Fatalf("IngestCSVFile failed: %v", err)
	}

	// Verify stats
	if stats.TotalFiles != 1 || stats.ProcessedFiles != 1 || stats.FailedFiles != 0 {
		t.Errorf("Expected stats to have 1 total and processed, 0 failed, got %+v", stats)
	}

	// Verify progress reporter events
	if len(reporter.events) == 0 {
		t.Errorf("Expected progress events, got 0")
	}

	// Verify events are in order of phases
	var phases []Phase
	for _, ev := range reporter.events {
		phases = append(phases, ev.Phase)
	}

	if len(phases) < 5 {
		t.Errorf("Expected at least 5 progress events, got %d: %v", len(phases), phases)
	}

	// Check if validating came first
	if phases[0] != PhaseValidating {
		t.Errorf("Expected first phase to be validating, got %s", phases[0])
	}
	// Check if done came last
	if phases[len(phases)-1] != PhaseDone {
		t.Errorf("Expected last phase to be done, got %s", phases[len(phases)-1])
	}

	// Check if the file was copied to unprocessedDir and then moved to processedDir
	// Since MoveProcessedFiles is true, it should now be in processedDir and NOT in unprocessedDir
	copiedPath := filepath.Join(unprocessedDir, "generic_test.csv")
	if _, statErr := os.Stat(copiedPath); !os.IsNotExist(statErr) {
		t.Errorf("Expected copied file to be removed from unprocessedDir, but it exists")
	}

	movedPath := filepath.Join(processedDir, "generic_test.csv")
	if _, statErr := os.Stat(movedPath); os.IsNotExist(statErr) {
		t.Errorf("Expected moved file to exist in processedDir, but it doesn't")
	}
}

func TestDatalake_ProcessFidelityFile(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Portfolio_Positions_20260925.csv"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash
SYNTH-401K-202,TRADITIONAL 401K,FXAIX,FIDELITY 500 INDEX,25.500,$180.20,-$0.80,$4595.10,-$20.40,-0.44%,+$595.10,+14.88%,100.00%,$4000.00,$156.86,Cash

"The data and information in this spreadsheet is provided to you solely for your use."

"Date downloaded Sep-25-2026 9:50 p.m ET"
`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Fidelity CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true, // moveProcessedFiles
		stats,
		logger,
	)
	processor.S3URI = "s3://test-bucket/datalake/fidelity.csv"

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat test file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	if processErr := processor.processFile(ctx, dirEntry); processErr != nil {
		t.Fatalf("processFile failed: %v", processErr)
	}

	// Verify routing assertions
	if !mockRepo.bulkUpsertFidelityPositionsCalled {
		t.Errorf("expected BulkUpsertFidelityPositions to be called")
	}
	if mockRepo.bulkUpsertTransactionsCalled {
		t.Errorf("expected BulkUpsertTransactions NOT to be called")
	}
	if len(mockRepo.fidelityPositions) != 2 {
		t.Fatalf("expected 2 Fidelity positions upserted, got %d", len(mockRepo.fidelityPositions))
	}

	// Verify first position details and lineage
	pos1 := mockRepo.fidelityPositions[0]
	if pos1.AccountNumber != "SYNTH-ROTH-101" || pos1.Symbol != "FSKAX" {
		t.Errorf("unexpected position data: %+v", pos1)
	}
	if pos1.CurrentValue != 6787.50 {
		t.Errorf("expected currentValue 6787.50, got %f", pos1.CurrentValue)
	}
	if pos1.Source.S3URI != processor.S3URI {
		t.Errorf("expected Source.S3URI %s, got %s", processor.S3URI, pos1.Source.S3URI)
	}
	if pos1.Source.FileName != fileName {
		t.Errorf("expected Source.FileName %s, got %s", fileName, pos1.Source.FileName)
	}
	if pos1.Source.RowNumber != 2 {
		t.Errorf("expected Source.RowNumber 2, got %d", pos1.Source.RowNumber)
	}
	expectedHashBytes := sha256.Sum256([]byte(csvContent))
	expectedSHA256 := hex.EncodeToString(expectedHashBytes[:])
	if pos1.Source.SHA256 != expectedSHA256 {
		t.Errorf("expected Source.SHA256 %s, got %s", expectedSHA256, pos1.Source.SHA256)
	}
	if pos1.Source.IngestedAt.IsZero() {
		t.Errorf("expected non-zero Source.IngestedAt")
	}

	nyLoc, _ := time.LoadLocation("America/New_York")
	expectedAsOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, nyLoc).UTC()
	if !pos1.AsOfDate.Equal(expectedAsOfDate) {
		t.Errorf("expected AsOfDate %v, got %v", expectedAsOfDate, pos1.AsOfDate)
	}

	// Verify second position row number
	pos2 := mockRepo.fidelityPositions[1]
	if pos2.Source.RowNumber != 3 {
		t.Errorf("expected Source.RowNumber 3, got %d", pos2.Source.RowNumber)
	}

	// Verify file movement to processedDir
	if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
		t.Errorf("expected source file to be removed from unprocessedDir, but it still exists")
	}
	destPath := filepath.Join(processedDir, fileName)
	if _, statErr := os.Stat(destPath); os.IsNotExist(statErr) {
		t.Errorf("expected moved file to exist in processedDir, but it does not")
	}
}

func TestDatalake_ProcessSingleFile_Fidelity(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Portfolio_Positions_20260925.csv"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash

"Date downloaded Sep-25-2026 9:50 p.m ET"
`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Fidelity CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		false,
		stats,
		logger,
	)
	processor.S3URI = "s3://test-bucket/datalake/fidelity.csv"

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Fidelity), "portfolio", reporter)
	if err != nil {
		t.Fatalf("processSingleFile failed: %v", err)
	}

	if !mockRepo.bulkUpsertFidelityPositionsCalled {
		t.Errorf("expected BulkUpsertFidelityPositions to be called")
	}
	if len(mockRepo.fidelityPositions) != 1 {
		t.Errorf("expected 1 Fidelity position, got %d", len(mockRepo.fidelityPositions))
	}
	if mockRepo.fidelityPositions[0].Source.S3URI != processor.S3URI {
		t.Errorf("expected Source.S3URI %s, got %s", processor.S3URI, mockRepo.fidelityPositions[0].Source.S3URI)
	}
}

func TestDatalake_ProcessFidelityFile_MissingDateFooter(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Portfolio_Positions_20260925.csv"
	filePath := filepath.Join(unprocessedDir, fileName)

	// CSV content without the "Date downloaded ..." footer
	csvContent := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash
`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true, // moveProcessedFiles
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	// Ingest file through ingestCSVFile so stats and file retention are tested
	ingestErr := processor.ingestCSVFile(ctx, dirEntry)
	if ingestErr == nil {
		t.Fatalf("expected ingestCSVFile to return error for missing date footer, got nil")
	}

	if stats.FailedFiles != 1 {
		t.Errorf("expected FailedFiles to be 1, got %d", stats.FailedFiles)
	}
	if stats.ProcessedFiles != 0 {
		t.Errorf("expected ProcessedFiles to be 0, got %d", stats.ProcessedFiles)
	}

	// Verify file is left in unprocessed directory and NOT moved to processed directory
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir, but it was removed")
	}
	destPath := filepath.Join(processedDir, fileName)
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file NOT to exist in processedDir, but it was found")
	}

	if mockRepo.bulkUpsertFidelityPositionsCalled {
		t.Errorf("expected BulkUpsertFidelityPositions NOT to be called")
	}
}

func TestDatalake_ProcessFidelityFile_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context immediately before calling

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Portfolio_Positions_20260925.csv"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash

"Date downloaded Sep-25-2026 9:50 p.m ET"
`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	processErr := processor.processFile(ctx, dirEntry)
	if processErr == nil {
		t.Fatalf("expected error due to canceled context, got nil")
	}

	if mockRepo.bulkUpsertFidelityPositionsCalled {
		t.Errorf("expected BulkUpsertFidelityPositions NOT to be called on canceled context")
	}

	// Verify file was NOT moved
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir on canceled context")
	}
}

func TestDatalake_ProcessFidelityFile_RepositoryError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Portfolio_Positions_20260925.csv"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash

"Date downloaded Sep-25-2026 9:50 p.m ET"
`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	dbErr := errors.New("mongodb cluster unreachable")
	mockRepo := &mockRepository{err: dbErr}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	processErr := processor.processFile(ctx, dirEntry)
	if processErr == nil {
		t.Fatalf("expected error from repository failure, got nil")
	}
	if !errors.Is(processErr, dbErr) && !strings.Contains(processErr.Error(), dbErr.Error()) {
		t.Errorf("expected error to contain repository error message, got %v", processErr)
	}

	// Verify file was NOT moved
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir on repository error")
	}

	// Also test processSingleFile error propagation
	reporter := &mockProgressReporter{}
	singleErr := processor.processSingleFile(ctx, fileName, string(datasource.Fidelity), "portfolio", reporter)
	if singleErr == nil {
		t.Fatalf("expected processSingleFile to return repository error, got nil")
	}
	if !errors.Is(singleErr, dbErr) && !strings.Contains(singleErr.Error(), dbErr.Error()) {
		t.Errorf("expected error to wrap db error, got %v", singleErr)
	}

	// Ensure reporter recorded PhaseFailed
	hasFailedPhase := false
	for _, ev := range reporter.events {
		if ev.Phase == PhaseFailed {
			hasFailedPhase = true
			break
		}
	}
	if !hasFailedPhase {
		t.Errorf("expected reporter to record PhaseFailed event")
	}
}

func assertChaseTransaction(
	t *testing.T,
	actual model.ChaseTransaction,
	expectedAccountID, expectedDesc string,
	expectedAmount float64,
	expectedType string,
	expectedRowNumber int64,
	expectedS3URI, expectedFileName, expectedSHA string,
	expectedDate time.Time,
) {
	t.Helper()
	if actual.AccountID != expectedAccountID {
		t.Errorf("expected AccountID %s, got %s", expectedAccountID, actual.AccountID)
	}
	if actual.Description != expectedDesc {
		t.Errorf("expected Description %s, got %s", expectedDesc, actual.Description)
	}
	if actual.Amount != expectedAmount {
		t.Errorf("expected Amount %f, got %f", expectedAmount, actual.Amount)
	}
	if actual.TransactionType != expectedType {
		t.Errorf("expected TransactionType %s, got %s", expectedType, actual.TransactionType)
	}
	if actual.Source.RowNumber != expectedRowNumber {
		t.Errorf("expected Source.RowNumber %d, got %d", expectedRowNumber, actual.Source.RowNumber)
	}
	if actual.Source.S3URI != expectedS3URI {
		t.Errorf("expected Source.S3URI %s, got %s", expectedS3URI, actual.Source.S3URI)
	}
	if actual.Source.FileName != expectedFileName {
		t.Errorf("expected Source.FileName %s, got %s", expectedFileName, actual.Source.FileName)
	}
	if expectedSHA != "" && actual.Source.SHA256 != expectedSHA {
		t.Errorf("expected Source.SHA256 %s, got %s", expectedSHA, actual.Source.SHA256)
	}
	if !expectedDate.IsZero() && !actual.PostingDate.Equal(expectedDate) {
		t.Errorf("expected PostingDate %v, got %v", expectedDate, actual.PostingDate)
	}
}

// TC-DL-01: End-to-end ingestion test verifying ChaseMapper invocation, repository write, and file movement.
func TestProcessFile_Chase(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,
CREDIT,02/01/2026,"PAYROLL DIRECT DEP",2500.00,ACH,13690.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true, // moveProcessedFiles
		stats,
		logger,
	)
	processor.S3URI = "s3://test-bucket/datalake/chase.csv"

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat test file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	if processErr := processor.processFile(ctx, dirEntry); processErr != nil {
		t.Fatalf("processFile failed: %v", processErr)
	}

	// Verify routing assertions
	if !mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions to be called")
	}
	if mockRepo.bulkUpsertTransactionsCalled {
		t.Errorf("expected legacy BulkUpsertTransactions NOT to be called")
	}
	if mockRepo.bulkUpsertFidelityPositionsCalled {
		t.Errorf("expected BulkUpsertFidelityPositions NOT to be called")
	}
	if len(mockRepo.chaseTransactions) != 2 {
		t.Fatalf("expected 2 Chase transactions upserted, got %d", len(mockRepo.chaseTransactions))
	}

	expectedPostingDate := time.Date(2026, time.January, 31, 0, 0, 0, 0, time.UTC)
	expectedHashBytes := sha256.Sum256([]byte(csvContent))
	expectedSHA256 := hex.EncodeToString(expectedHashBytes[:])

	// Verify first transaction details and lineage
	tx1 := mockRepo.chaseTransactions[0]
	assertChaseTransaction(
		t, tx1, "6896", "WHOLE FOODS GROCERY", -75.77, "DEBIT_CARD", 2,
		processor.S3URI, fileName, expectedSHA256, expectedPostingDate,
	)
	if tx1.Balance == nil || *tx1.Balance != 11190.76 {
		t.Errorf("expected Balance 11190.76, got %v", tx1.Balance)
	}
	if tx1.Source.IngestedAt.IsZero() {
		t.Errorf("expected non-zero Source.IngestedAt")
	}

	// Verify second transaction
	tx2 := mockRepo.chaseTransactions[1]
	assertChaseTransaction(
		t, tx2, "6896", "PAYROLL DIRECT DEP", 2500.00, "ACH", 3,
		processor.S3URI, fileName, "", time.Time{},
	)

	// Verify file movement to processedDir
	if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
		t.Errorf("expected source file to be removed from unprocessedDir, but it still exists")
	}
	destPath := filepath.Join(processedDir, fileName)
	if _, statErr := os.Stat(destPath); os.IsNotExist(statErr) {
		t.Errorf("expected moved file to exist in processedDir, but it does not")
	}
}

// TC-DL-02: Verifies single-file progress reporting across all phases (PhaseValidating, PhaseParsing, PhaseUpserting, PhaseDone).
func TestDatalake_ProcessSingleFile_Chase(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		false,
		stats,
		logger,
	)
	processor.S3URI = "s3://test-bucket/datalake/chase.csv"

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "6896", reporter)
	if err != nil {
		t.Fatalf("processSingleFile failed: %v", err)
	}

	if !mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions to be called")
	}
	if len(mockRepo.chaseTransactions) != 1 {
		t.Fatalf("expected 1 Chase transaction, got %d", len(mockRepo.chaseTransactions))
	}
	if mockRepo.chaseTransactions[0].Source.S3URI != processor.S3URI {
		t.Errorf("expected Source.S3URI %s, got %s", processor.S3URI, mockRepo.chaseTransactions[0].Source.S3URI)
	}

	// Verify events are in order of phases: validating -> parsing -> upserting -> done
	var phases []Phase
	for _, ev := range reporter.events {
		phases = append(phases, ev.Phase)
	}
	if len(phases) < 4 {
		t.Errorf("expected at least 4 progress events, got %d: %v", len(phases), phases)
	}
	if phases[0] != PhaseValidating {
		t.Errorf("expected first phase to be PhaseValidating, got %s", phases[0])
	}
	if phases[len(phases)-1] != PhaseDone {
		t.Errorf("expected last phase to be PhaseDone, got %s", phases[len(phases)-1])
	}
}

func TestDatalake_ProcessChaseFile_RepositoryError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	dbErr := errors.New("mongodb cluster unreachable")
	mockRepo := &mockRepository{err: dbErr}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	processErr := processor.processFile(ctx, dirEntry)
	if processErr == nil {
		t.Fatalf("expected error from repository failure, got nil")
	}
	if !errors.Is(processErr, dbErr) && !strings.Contains(processErr.Error(), dbErr.Error()) {
		t.Errorf("expected error to contain repository error message, got %v", processErr)
	}

	// Verify file was NOT moved
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir on repository error")
	}

	// Also test processSingleFile error propagation
	reporter := &mockProgressReporter{}
	singleErr := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "6896", reporter)
	if singleErr == nil {
		t.Fatalf("expected processSingleFile to return repository error, got nil")
	}
	if !errors.Is(singleErr, dbErr) && !strings.Contains(singleErr.Error(), dbErr.Error()) {
		t.Errorf("expected error to wrap db error, got %v", singleErr)
	}

	// Ensure reporter recorded PhaseFailed
	hasFailedPhase := false
	for _, ev := range reporter.events {
		if ev.Phase == PhaseFailed {
			hasFailedPhase = true
			break
		}
	}
	if !hasFailedPhase {
		t.Errorf("expected reporter to record PhaseFailed event")
	}
}

func TestDatalake_ProcessChaseFile_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	processErr := processor.processFile(ctx, dirEntry)
	if processErr == nil {
		t.Fatalf("expected error due to canceled context, got nil")
	}

	if mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions NOT to be called on canceled context")
	}

	// Verify file was NOT moved
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir on canceled context")
	}
}

func TestDatalake_ProcessChaseFile_MappingError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	// CSV content with an invalid posting date
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,invalid-date,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		true, // moveProcessedFiles
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	ingestErr := processor.ingestCSVFile(ctx, dirEntry)
	if ingestErr == nil {
		t.Fatalf("expected ingestCSVFile to return error for mapping failure, got nil")
	}

	if stats.FailedFiles != 1 {
		t.Errorf("expected FailedFiles to be 1, got %d", stats.FailedFiles)
	}
	if stats.ProcessedFiles != 0 {
		t.Errorf("expected ProcessedFiles to be 0, got %d", stats.ProcessedFiles)
	}

	// Verify file is left in unprocessed directory and NOT moved to processed directory
	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		t.Errorf("expected file to remain in unprocessedDir, but it was removed")
	}
	destPath := filepath.Join(processedDir, fileName)
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file NOT to exist in processedDir, but it was found")
	}

	if mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions NOT to be called")
	}
}

func TestDatalake_ProcessSingleFile_Chase_MappingError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	// CSV content with an invalid amount
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",not-a-number,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		false,
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "6896", reporter)
	if err == nil {
		t.Fatalf("expected processSingleFile to return error for invalid amount, got nil")
	}

	if mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions NOT to be called on mapping error")
	}

	if stats.FailedFiles != 1 {
		t.Errorf("expected FailedFiles to be 1, got %d", stats.FailedFiles)
	}

	hasFailedPhase := false
	for _, ev := range reporter.events {
		if ev.Phase == PhaseFailed {
			hasFailedPhase = true
			if !strings.Contains(ev.Message, "failed to map Chase transactions") {
				t.Errorf("expected PhaseFailed message to describe mapping error, got %q", ev.Message)
			}
			break
		}
	}
	if !hasFailedPhase {
		t.Errorf("expected reporter to record PhaseFailed event")
	}
}

func TestDatalake_ProcessSingleFile_Chase_MapperInitError(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase6896_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"WHOLE FOODS GROCERY",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test Chase CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		false,
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	// Pass empty accountID to trigger mapper init failure
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "", reporter)
	if err == nil {
		t.Fatalf("expected processSingleFile to return error for empty accountID, got nil")
	}
	if !strings.Contains(err.Error(), "accountID is required") {
		t.Errorf("expected error to mention accountID is required, got %v", err)
	}

	if mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions NOT to be called on mapper init error")
	}

	if stats.FailedFiles != 1 {
		t.Errorf("expected FailedFiles to be 1, got %d", stats.FailedFiles)
	}

	hasFailedPhase := false
	for _, ev := range reporter.events {
		if ev.Phase == PhaseFailed {
			hasFailedPhase = true
			if !strings.Contains(ev.Message, "accountID is required") {
				t.Errorf("expected PhaseFailed message to mention accountID is required, got %q", ev.Message)
			}
			break
		}
	}
	if !hasFailedPhase {
		t.Errorf("expected reporter to record PhaseFailed event")
	}
}

func TestDatalake_ProcessChaseRecords_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		"",
		"",
		false,
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	rawRecords := []map[string]string{
		{
			"Details":      "DEBIT",
			"Posting Date": "01/31/2026",
			"Description":  "WHOLE FOODS GROCERY",
			"Amount":       "-75.77",
			"Type":         "DEBIT_CARD",
			"Balance":      "11190.76",
		},
	}
	_, _, _, err := processor.processChaseRecords(ctx, "test.csv", "test.csv", "1234", rawRecords, reporter)
	if err == nil {
		t.Fatalf("expected processChaseRecords to return error for canceled context, got nil")
	}

	if mockRepo.bulkUpsertChaseTransactionsCalled {
		t.Errorf("expected BulkUpsertChaseTransactions NOT to be called on canceled context")
	}

	hasUpsertingPhase := false
	for _, ev := range reporter.events {
		if ev.Phase == PhaseUpserting {
			hasUpsertingPhase = true
		}
	}
	if hasUpsertingPhase {
		t.Errorf("expected PhaseUpserting NOT to be emitted when context is canceled")
	}
}

// TC-DL-SYNC-01: Verifies that a successful ingestion invokes RecordSyncLog with Status="SUCCESS", non-zero DurationMs, accurate UpsertedCount, MatchedCount, and FileHash.
func TestTC_DL_SYNC_01_SuccessfulIngestionAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	processedDir := filepath.Join(tmpDir, "processed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"GROCERY STORE",-50.00,DEBIT_CARD,1000.00,
CREDIT,02/01/2026,"SALARY",2000.00,ACH,3000.00,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		processedDir,
		false,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	if ingestErr := processor.ingestCSVFile(ctx, dirEntry); ingestErr != nil {
		t.Fatalf("ingestCSVFile failed: %v", ingestErr)
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.FileName != fileName {
		t.Errorf("expected FileName %q, got %q", fileName, log.FileName)
	}
	if log.CollectionName != ChaseTransactionsCollection {
		t.Errorf("expected CollectionName %q, got %q", ChaseTransactionsCollection, log.CollectionName)
	}
	if log.RecordCount != 2 {
		t.Errorf("expected RecordCount 2, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 2 {
		t.Errorf("expected UpsertedCount 2, got %d", log.UpsertedCount)
	}
	if log.DurationMs < 0 {
		t.Errorf("expected DurationMs >= 0, got %d", log.DurationMs)
	}
	expectedHashBytes := sha256.Sum256([]byte(csvContent))
	expectedHash := hex.EncodeToString(expectedHashBytes[:])
	if log.FileHash != expectedHash {
		t.Errorf("expected FileHash %q, got %q", expectedHash, log.FileHash)
	}
}

// TC-DL-SYNC-02: Verifies that CSV Parse Failure calls RecordSyncLog with Status="FAILED", error details preserved, and UpsertedCount=0.
func TestTC_DL_SYNC_02_CSVParseFailureAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	if err := os.WriteFile(filePath, []byte("invalid-csv-data"), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	parseErr := errors.New("corrupt CSV syntax on line 4")
	mockRepo := &mockRepository{}
	mockParser := &mockCSVParser{err: parseErr}
	extractor := datasource.NewGenericExtractor()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		mockParser,
		unprocessedDir,
		"",
		false,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	err = processor.ingestCSVFile(ctx, dirEntry)
	if err == nil {
		t.Fatal("expected ingestCSVFile to return parse error, got nil")
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called on parse failure")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if !strings.Contains(log.ErrorMessage, parseErr.Error()) {
		t.Errorf("expected ErrorMessage to contain %q, got %q", parseErr.Error(), log.ErrorMessage)
	}
	if log.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", log.UpsertedCount)
	}
}

// TC-DL-SYNC-03: Verifies that Empty File Ingestion calls RecordSyncLog with Status="SUCCESS", RecordCount=0, UpsertedCount=0.
func TestTC_DL_SYNC_03_EmptyFileIngestionAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "generic_empty.csv"
	filePath := filepath.Join(unprocessedDir, fileName)
	headerOnly := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n"
	if err := os.WriteFile(filePath, []byte(headerOnly), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	mockExtractor := &mockInfoExtractor{
		info: &datasource.SourceInfo{
			DataSource: string(datasource.Generic),
			AccountID:  "0000",
		},
	}
	mockParser := &mockCSVParser{records: []map[string]string{}}
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		mockExtractor,
		mockParser,
		unprocessedDir,
		"",
		false,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	if ingestErr := processor.ingestCSVFile(ctx, dirEntry); ingestErr != nil {
		t.Fatalf("ingestCSVFile failed for empty file: %v", ingestErr)
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called for empty file")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.RecordCount != 0 {
		t.Errorf("expected RecordCount 0, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", log.UpsertedCount)
	}
}

// TC-DL-SYNC-04: Verifies that Storage Failure calls RecordSyncLog with Status="FAILED" when database write fails.
func TestTC_DL_SYNC_04_StorageFailureAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"GROCERY STORE",-50.00,DEBIT_CARD,1000.00,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	dbErr := errors.New("mongodb write timeout")
	mockRepo := &mockRepository{upsertErr: dbErr}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		"",
		false,
		stats,
		logger,
	)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	dirEntry := newMockDirEntry(fileInfo)

	err = processor.ingestCSVFile(ctx, dirEntry)
	if err == nil {
		t.Fatal("expected ingestCSVFile to return database error, got nil")
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called on storage failure")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if !strings.Contains(log.ErrorMessage, dbErr.Error()) {
		t.Errorf("expected ErrorMessage to contain %q, got %q", dbErr.Error(), log.ErrorMessage)
	}
}

// TC-DL-SYNC-05: Verifies that Single File Ingestion verifies processSingleFile emits both progress events and final dataSync record.
func TestTC_DL_SYNC_05_SingleFileIngestionProgressAndAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"GROCERY STORE",-50.00,DEBIT_CARD,1000.00,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		"",
		false,
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "1234", reporter)
	if err != nil {
		t.Fatalf("processSingleFile failed: %v", err)
	}

	// Verify progress reporter received events for PhaseValidating, PhaseParsing, PhaseUpserting, PhaseDone
	var phases []Phase
	for _, ev := range reporter.events {
		phases = append(phases, ev.Phase)
	}
	if len(phases) < 4 {
		t.Errorf("expected at least 4 progress phases, got %d: %v", len(phases), phases)
	}
	hasValidating := false
	hasParsing := false
	hasUpserting := false
	hasDone := false
	for _, p := range phases {
		switch p {
		case PhaseValidating:
			hasValidating = true
		case PhaseParsing:
			hasParsing = true
		case PhaseUpserting:
			hasUpserting = true
		case PhaseDone:
			hasDone = true
		case PhaseMoving, PhaseFailed:
			// Non-asserted phases in this specific scenario
		}
	}
	if !hasValidating || !hasParsing || !hasUpserting || !hasDone {
		t.Errorf("missing expected phases: validating=%v, parsing=%v, upserting=%v, done=%v",
			hasValidating, hasParsing, hasUpserting, hasDone)
	}

	// Verify RecordSyncLog was called
	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called for processSingleFile")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.FileName != fileName {
		t.Errorf("expected FileName %q, got %q", fileName, log.FileName)
	}
	if log.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount 1, got %d", log.UpsertedCount)
	}
}

// TestTC_DL_SYNC_MoveFileFailureAudit verifies that when file archival fails after upsert,
// a failure audit log is recorded with descriptive error and the error is returned.
func TestTC_DL_SYNC_MoveFileFailureAudit(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	// Create a regular file where the processed directory is expected, causing directory creation or move to fail.
	invalidProcessedDir := filepath.Join(tmpDir, "invalid_processed_dir")
	if err := os.WriteFile(invalidProcessedDir, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("failed to create blocking file: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"GROCERY STORE",-50.00,DEBIT_CARD,1000.00,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		invalidProcessedDir,
		true, // Enable MoveProcessedFiles
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "1234", reporter)
	if err == nil {
		t.Fatal("expected processSingleFile to return error on move failure, got nil")
	}
	if !strings.Contains(err.Error(), "failed to move file") {
		t.Errorf("expected error to mention 'failed to move file', got: %v", err)
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called on move failure")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if !strings.Contains(log.ErrorMessage, "storage succeeded but file archival failed") {
		t.Errorf("expected ErrorMessage to contain 'storage succeeded but file archival failed', got %q", log.ErrorMessage)
	}
	if log.FileName != fileName {
		t.Errorf("expected FileName %q, got %q", fileName, log.FileName)
	}
}

// TestTC_DL_SYNC_ContextCancellationFailureAudit verifies that context cancellation during ingestion
// still successfully records a failure audit log via the detached context.
func TestTC_DL_SYNC_ContextCancellationFailureAudit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel parent context immediately

	tmpDir := t.TempDir()
	unprocessedDir := filepath.Join(tmpDir, "unprocessed")
	if err := os.MkdirAll(unprocessedDir, 0o755); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}

	fileName := "Chase1234_Activity_20260131.CSV"
	filePath := filepath.Join(unprocessedDir, fileName)
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2026,"GROCERY STORE",-50.00,DEBIT_CARD,1000.00,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	mockRepo := &mockRepository{}
	extractor := datasource.NewGenericExtractor()
	parser := csvparser.NewDefaultParser()
	stats := NewStats()
	logger := *slog.New(slog.DiscardHandler)

	processor := NewCSVFileProcessor(
		mockRepo,
		extractor,
		parser,
		unprocessedDir,
		"",
		false,
		stats,
		logger,
	)

	reporter := &mockProgressReporter{}
	err := processor.processSingleFile(ctx, fileName, string(datasource.Chase), "1234", reporter)
	if err == nil {
		t.Fatal("expected processSingleFile to fail due to canceled context, got nil")
	}

	if !mockRepo.recordSyncLogCalled {
		t.Fatal("expected RecordSyncLog to be called via detached context")
	}

	log := mockRepo.recordedSyncLog
	if log.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if !strings.Contains(log.ErrorMessage, "context canceled") {
		t.Errorf("expected ErrorMessage to contain 'context canceled', got %q", log.ErrorMessage)
	}

	// Verify that the context passed to RecordSyncLog was detached and not canceled during execution
	if mockRepo.recordedSyncLogCtx == nil {
		t.Fatal("expected recordedSyncLogCtx to be non-nil")
	}
	if mockRepo.recordedSyncLogCtxErr != nil {
		t.Errorf("expected detached context for RecordSyncLog to NOT be canceled during invocation, got: %v", mockRepo.recordedSyncLogCtxErr)
	}
}

func TestCopyAndRemoveFile_Success(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.csv")
	dstPath := filepath.Join(tmpDir, "dest.csv")

	if err := os.WriteFile(srcPath, []byte("col1,col2\nval1,val2\n"), 0o644); err != nil {
		t.Fatalf("failed to write test source: %v", err)
	}

	if err := copyAndRemoveFile(srcPath, dstPath); err != nil {
		t.Fatalf("copyAndRemoveFile failed: %v", err)
	}

	if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
		t.Errorf("expected source file to be removed, but it still exists")
	}

	dstData, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read dest file: %v", err)
	}
	if string(dstData) != "col1,col2\nval1,val2\n" {
		t.Errorf("dest content mismatch: got %q", string(dstData))
	}
}

func TestCopyAndRemoveFile_CopyFailureCleansUpDestination(t *testing.T) {
	tmpDir := t.TempDir()
	// Open a directory as the source. Reading from a directory file descriptor will fail during io.Copy on Unix.
	srcDir := filepath.Join(tmpDir, "source_dir")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}
	dstPath := filepath.Join(tmpDir, "dest.csv")

	err := copyAndRemoveFile(srcDir, dstPath)
	if err == nil {
		t.Fatal("expected copyAndRemoveFile to fail when copying a directory, got nil")
	}

	// Verify destination file was deleted upon copy failure
	if _, statErr := os.Stat(dstPath); !os.IsNotExist(statErr) {
		t.Errorf("expected destination file %s to be cleaned up on copy failure, but it exists", dstPath)
	}
}
