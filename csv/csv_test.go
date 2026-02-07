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
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
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

	parser := NewDefaultParser()
	data, recordsProcessed, err := parser.Parse(ctx, filePath, dataSource, accountID)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(data) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(data))
	}
	if recordsProcessed != 2 {
		t.Errorf("Expected 2 records processed, got %d", recordsProcessed)
	}

	// Verify first record
	expectedData1 := map[string]string{
		"details":         "DEBIT",
		"posting date":    "01/01/2024",
		"description":     "WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",
		"category":        "Shopping",
		"amount":          "-75.77",
		"type":            "DEBIT_CARD",
		"balance":         "11190.76",
		"check or slip #": "",
	}

	if data[0]["details"] != expectedData1["details"] ||
		data[0]["posting date"] != expectedData1["posting date"] ||
		data[0]["description"] != expectedData1["description"] ||
		data[0]["category"] != expectedData1["category"] ||
		data[0]["amount"] != expectedData1["amount"] ||
		data[0]["type"] != expectedData1["type"] ||
		data[0]["balance"] != expectedData1["balance"] ||
		data[0]["check or slip #"] != expectedData1["check or slip #"] ||
		data[0]["dataSource"] != expectedData1["dataSource"] ||
		data[0]["accountID"] != expectedData1["accountID"] {
		t.Errorf("Expected first record %+v, got %+v", expectedData1, data[0])
	}

	// Verify second record
	expectedData2 := map[string]string{
		"details":         "CREDIT",
		"posting date":    "01/02/2024",
		"description":     "ONLINE PAYMENT THANK YOU",
		"category":        "Payment",
		"amount":          "1000.00",
		"type":            "PAYMENT",
		"balance":         "10114.36",
		"check or slip #": "",
	}

	if data[1]["details"] != expectedData2["details"] ||
		data[1]["posting date"] != expectedData2["posting date"] ||
		data[1]["description"] != expectedData2["description"] ||
		data[1]["category"] != expectedData2["category"] ||
		data[1]["amount"] != expectedData2["amount"] ||
		data[1]["type"] != expectedData2["type"] ||
		data[1]["balance"] != expectedData2["balance"] ||
		data[1]["check or slip #"] != expectedData2["check or slip #"] ||
		data[1]["dataSource"] != expectedData2["dataSource"] ||
		data[1]["accountID"] != expectedData2["accountID"] {
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

	parser := NewDefaultParser()
	data, _, err := parser.Parse(ctx, filePath, dataSource, accountID)
	if err != nil {
		t.Fatalf("Parse with reordered columns failed: %v", err)
	}

	if len(data) != 2 {
		t.Fatalf("Expected 2 documents, got %d", len(data))
	}

	expected1 := map[string]string{
		"amount":       "-75.77",
		"description":  "Transaction 1",
		"posting date": "01/01/2024",
		"details":      "DEBIT",
		"category":     "Shopping",
		"type":         "DEBIT_CARD",
		"balance":      "100.00",
		"dataSource":   "chase",
		"accountID":    "5678",
	}

	if data[0]["details"] != expected1["details"] {
		t.Errorf("Expected details %s, got %s", expected1["details"], data[0]["details"])
	}

	expected2 := map[string]string{
		"amount":       "100.00",
		"description":  "Transaction 2",
		"posting date": "01/02/2024",
		"details":      "CREDIT",
		"category":     "Payment",
		"type":         "PAYMENT",
		"balance":      "200.00",
		"dataSource":   "chase",
		"accountID":    "5678",
	}
	if data[1]["details"] != expected2["details"] {
		t.Errorf("Expected details %s, got %s", expected2["details"], data[1]["details"])
	}
}

func TestParseCSV_InvalidRecord(t *testing.T) {
	ctx := context.Background()
	csvContent := `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,01/01/2024,Test,Shopping,-75.77` // Missing columns
	filePath := createTempCSV(t, "chase_invalid_record.csv", csvContent)
	dataSource := "chase"

	parser := NewDefaultParser()
	data, _, err := parser.Parse(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
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

	parser := NewDefaultParser()
	data, _, err := parser.Parse(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Now that date parsing is moved to datalake, this test should expect 1 record
	// and verify the 'posting date' field contains "InvalidDate"
	if len(data) != 1 {
		t.Errorf("Expected 1 document, got %d", len(data))
	}
	if data[0]["posting date"] != "InvalidDate" {
		t.Errorf("Expected 'posting date' to be 'InvalidDate', got '%s'", data[0]["posting date"])
	}
}

func TestParseCSV_EmptyFile(t *testing.T) {
	ctx := context.Background()
	filePath := createTempCSV(t, "chase_empty.csv", "")
	dataSource := "chase"

	parser := NewDefaultParser()
	data, recordsProcessed, err := parser.Parse(ctx, filePath, dataSource, "0000")
	if err != nil {
		t.Fatalf("Expected Parse to succeed for empty file, but got error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Expected 0 documents, got %d", len(data))
	}
	if recordsProcessed != 0 {
		t.Errorf("Expected 0 records processed, got %d", recordsProcessed)
	}
}

func TestParseCSV_FileNotFound(t *testing.T) {
	ctx := context.Background()
	filePath := "non_existent_file.csv"
	dataSource := "chase"

	parser := NewDefaultParser()
	_, _, err := parser.Parse(ctx, filePath, dataSource, "0000")
	if err == nil {
		t.Fatalf("Expected Parse to fail for file not found, but got nil error")
	}
	expectedErrorMsg := "failed to open file"
	if !strings.Contains(err.Error(), expectedErrorMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}
