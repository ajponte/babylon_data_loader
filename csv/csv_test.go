package csv

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// createTempCSV creates a temporary CSV file with the given content.
func createTempCSV(t *testing.T, filename, content string) string {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, filename)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}
	return filePath
}

func TestParseCSV_Success(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,01/01/2024,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",Shopping,-75.77,DEBIT_CARD,11190.76,
CREDIT,01/02/2024,"ONLINE PAYMENT THANK YOU",Payment,1000.00,PAYMENT,10114.36,`
	filePath := createTempCSV(t, "chase_valid.csv", csvContent)
	dataSource := "chase"
	accountID := "1234"

	data, collectionName, recordsProcessed, err := ParseCSV(ctx, filePath, dataSource, accountID)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(data) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(data))
	}
	if collectionName != "chase-data-2024-01-01" {
		t.Errorf("Expected collection name 'chase-data-2024-01-01', got '%s'", collectionName)
	}
	if recordsProcessed != 2 {
		t.Errorf("Expected 2 records processed, got %d", recordsProcessed)
	}

	// Verify first record
	expectedData1 := Data{
		Details:        "DEBIT",
		PostingDate:    "01/01/2024",
		Description:    "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
		Amount:         -75.77,
		Category:       "Shopping",
		Type:           "DEBIT_CARD",
		Balance:        11190.76,
		CheckOrSlipNum: "",
		DataSource:     "chase",
		AccountID:      "1234",
	}
	if data[0] != expectedData1 {
		t.Errorf("Expected first record %+v, got %+v", expectedData1, data[0])
	}

	// Verify second record
	expectedData2 := Data{
		Details:        "CREDIT",
		PostingDate:    "01/02/2024",
		Description:    "ONLINE PAYMENT THANK YOU",
		Amount:         1000.00,
		Category:       "Payment",
		Type:           "PAYMENT",
		Balance:        10114.36,
		CheckOrSlipNum: "",
		DataSource:     "chase",
		AccountID:      "1234",
	}
	if data[1] != expectedData2 {
		t.Errorf("Expected second record %+v, got %+v", expectedData2, data[1])
	}
}

func TestParseCSV_DifferentColumnOrder(t *testing.T) {
	ctx := context.Background()
	csvContent := `Amount,Description,Posting Date,Details,Category,Type,Balance
-75.77,"Transaction 1","01/01/2024","DEBIT","Shopping","DEBIT_CARD","100.00"
100.00,"Transaction 2","01/02/2024","CREDIT","Payment","PAYMENT","200.00"`
	filePath := createTempCSV(t, "chase_reordered.csv", csvContent)
	dataSource := "chase"
	accountID := "5678"

	data, _, _, err := ParseCSV(ctx, filePath, dataSource, accountID)
	if err != nil {
		t.Fatalf("ParseCSV with reordered columns failed: %v", err)
	}

	if len(data) != 2 {
		t.Fatalf("Expected 2 documents, got %d", len(data))
	}

	expected1 := Data{
		Details:     "DEBIT",
		PostingDate: "01/01/2024",
		Description: "Transaction 1",
		Amount:      -75.77,
		Category:    "Shopping",
		Type:        "DEBIT_CARD",
		Balance:     100.00,
		DataSource:  "chase",
		AccountID:   "5678",
	}

	if data[0] != expected1 {
		t.Errorf("Expected first record %+v, got %+v", expected1, data[0])
	}

	expected2 := Data{
		Details:     "CREDIT",
		PostingDate: "01/02/2024",
		Description: "Transaction 2",
		Amount:      100.00,
		Category:    "Payment",
		Type:        "PAYMENT",
		Balance:     200.00,
		DataSource:  "chase",
		AccountID:   "5678",
	}
	if data[1] != expected2 {
		t.Errorf("Expected second record %+v, got %+v", expected2, data[1])
	}
}


func TestParseCSV_InvalidRecord(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,01/01/2024,Test,Shopping,-75.77` // Missing columns
	filePath := createTempCSV(t, "chase_invalid_record.csv", csvContent)
	dataSource := "chase"

	data, _, _, err := ParseCSV(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Expected 0 documents, got %d", len(data))
	}
}

func TestParseCSV_InvalidDateFormat(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,InvalidDate,Test,Shopping,-75.77,DEBIT_CARD,11190.76,`
	filePath := createTempCSV(t, "chase_invalid_date.csv", csvContent)
	dataSource := "chase"

	data, _, _, err := ParseCSV(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("Expected 0 documents, got %d", len(data))
	}
}

func TestParseCSV_EmptyFile(t *testing.T) {
	ctx := context.Background()
	filePath := createTempCSV(t, "chase_empty.csv", "")
	dataSource := "chase"

	_, _, _, err := ParseCSV(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("Expected ParseCSV to succeed for empty file, but got error: %v", err)
	}
}

func TestParseCSV_FileNotFound(t *testing.T) {
	ctx := context.Background()
	filePath := "non_existent_file.csv"
	dataSource := "chase"

	_, _, _, err := ParseCSV(ctx, filePath, dataSource, "0000")
	if err == nil {
		t.Fatalf("Expected ParseCSV to fail for file not found, but got nil error")
	}
	expectedErrorMsg := "failed to open file"
	if !strings.Contains(err.Error(), expectedErrorMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}
