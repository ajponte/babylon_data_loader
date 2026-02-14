package datalake

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/model"
	_ "babylon/dataloader/datalake/repository"
)

// ---- Mocks ----

// mockRepository implements repository.Repository for testing.
type mockRepository struct {
	bulkUpsertTransactionsCalled bool
	transactions                 []model.Transaction
	err                          error
}

func (m *mockRepository) BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) error {
	m.bulkUpsertTransactionsCalled = true
	m.transactions = transactions
	return m.err
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
	mockLogger := *slog.New(slog.NewTextHandler(io.Discard, nil))

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
	mockLogger := *slog.New(slog.NewTextHandler(io.Discard, nil))

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
