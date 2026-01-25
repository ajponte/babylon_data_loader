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
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/01/2024,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",-75.77,DEBIT_CARD,11190.76,
CREDIT,01/02/2024,"ONLINE PAYMENT THANK YOU",1000.00,PAYMENT,10114.36,`
	filePath := createTempCSV(t, "chase_valid.csv", csvContent)
	dataSource := "chase"

	data, collectionName, recordsProcessed, err := ParseCSV(ctx, filePath, dataSource)
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
		Type:           "DEBIT_CARD",
		Balance:        11190.76,
		CheckOrSlipNum: "",
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
		Type:           "PAYMENT",
		Balance:        10114.36,
		CheckOrSlipNum: "",
	}
	if data[1] != expectedData2 {
		t.Errorf("Expected second record %+v, got %+v", expectedData2, data[1])
	}
}

func TestParseCSV_InvalidRecord(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/01/2024,Test,-75.77` // Missing columns
	filePath := createTempCSV(t, "chase_invalid_record.csv", csvContent)
	dataSource := "chase"

	data, _, _, err := ParseCSV(ctx, filePath, dataSource)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(data) != 1 {
		t.Errorf("Expected 1 document, got %d", len(data))
	}
}

func TestParseCSV_InvalidDateFormat(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,InvalidDate,Test,-75.77,DEBIT_CARD,11190.76,`
	filePath := createTempCSV(t, "chase_invalid_date.csv", csvContent)
	dataSource := "chase"

	data, _, _, err := ParseCSV(ctx, filePath, dataSource)
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

	_, _, _, err := ParseCSV(ctx, filePath, dataSource)
	if err != nil { 
		t.Fatalf("Expected ParseCSV to succeed for empty file, but got error: %v", err)
	}
}

func TestParseCSV_FileNotFound(t *testing.T) {
	ctx := context.Background()
	filePath := "non_existent_file.csv"
	dataSource := "chase"

	_, _, _, err := ParseCSV(ctx, filePath, dataSource)
	if err == nil {
		t.Fatalf("Expected ParseCSV to fail for file not found, but got nil error")
	}
	expectedErrorMsg := "failed to open file"
	if !strings.Contains(err.Error(), expectedErrorMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}
