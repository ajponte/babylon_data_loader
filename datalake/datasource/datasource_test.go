package datasource_test

import (
	"errors"
	"testing"

	"babylon/dataloader/datalake/datasource"
)

func TestChaseExtractor_NewChaseExtractor(t *testing.T) {
	extractor := datasource.NewChaseExtractor()
	if extractor == nil {
		t.Errorf("NewChaseExtractor() returned nil, expected a ChaseExtractor instance")
	}
}

func TestChaseExtractor_ExtractInfo_Success(t *testing.T) {
	extractor := datasource.NewChaseExtractor()
	tests := []struct {
		filename    string
		expectedDS  string
		expectedAcc string
	}{
		{"chase1234.csv", "chase", "1234"},
		{"my_chase0000_statement.pdf", "chase", "0000"},
		{"CHASE9876_transactions.xls", "chase", "9876"},
		{"another_file_chase5555.txt", "chase", "5555"},
	}

	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			info, err := extractor.ExtractInfo(test.filename)
			if err != nil {
				t.Errorf("ExtractInfo(%s) returned an unexpected error: %v", test.filename, err)
			}
			if info == nil {
				t.Fatalf("ExtractInfo(%s) returned nil info, expected a SourceInfo instance", test.filename)
			}
			if info.DataSource != test.expectedDS {
				t.Errorf("ExtractInfo(%s) DataSource got %s, want %s", test.filename, info.DataSource, test.expectedDS)
			}
			if info.AccountID != test.expectedAcc {
				t.Errorf("ExtractInfo(%s) AccountID got %s, want %s", test.filename, info.AccountID, test.expectedAcc)
			}
		})
	}
}

func TestChaseExtractor_ExtractInfo_NoMatch(t *testing.T) {
	extractor := datasource.NewChaseExtractor()
	tests := []string{
		"somefile.csv",
		"bank_statement.pdf",
		"chase.txt",    // Missing digits
		"chase123.csv", // Not 4 digits
	}

	for _, filename := range tests {
		t.Run(filename, func(t *testing.T) {
			info, err := extractor.ExtractInfo(filename)
			if !errors.Is(err, datasource.ErrUnableToExtractInfo) {
				t.Errorf("ExtractInfo(%s) expected ErrUnableToExtractInfo, got %v", filename, err)
			}
			if info != nil {
				t.Errorf("ExtractInfo(%s) returned info %v, expected nil", filename, info)
			}
		})
	}
}

func TestGenericExtractor_NewGenericExtractor(t *testing.T) {
	extractor := datasource.NewGenericExtractor()
	if extractor == nil {
		t.Errorf("NewGenericExtractor() returned nil, expected a GenericExtractor instance")
	}
}

func TestGenericExtractor_ExtractInfo_Success(t *testing.T) {
	extractor := datasource.NewGenericExtractor()
	tests := []struct {
		filename    string
		expectedDS  string
		expectedAcc string
	}{
		{"test_file.csv", "test", "0000"},
		{"my_test_statement.pdf", "test", "0000"},
		{"TEST_transactions.xls", "test", "0000"},
		{"another_file_test.txt", "test", "0000"},
	}

	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			info, err := extractor.ExtractInfo(test.filename)
			if err != nil {
				t.Errorf("ExtractInfo(%s) returned an unexpected error: %v", test.filename, err)
			}
			if info == nil {
				t.Fatalf("ExtractInfo(%s) returned nil info, expected a SourceInfo instance", test.filename)
			}
			if info.DataSource != test.expectedDS {
				t.Errorf("ExtractInfo(%s) DataSource got %s, want %s", test.filename, info.DataSource, test.expectedDS)
			}
			if info.AccountID != test.expectedAcc {
				t.Errorf("ExtractInfo(%s) AccountID got %s, want %s", test.filename, info.AccountID, test.expectedAcc)
			}
		})
	}
}

func TestGenericExtractor_ExtractInfo_NoMatch(t *testing.T) {
	extractor := datasource.NewGenericExtractor()
	tests := []string{
		"somefile.csv",
		"bank_statement.pdf",
		"chase1234.txt",
		"another_file.txt",
	}

	for _, filename := range tests {
		t.Run(filename, func(t *testing.T) {
			info, err := extractor.ExtractInfo(filename)
			if !errors.Is(err, datasource.ErrUnableToExtractInfo) {
				t.Errorf("ExtractInfo(%s) expected ErrUnableToExtractInfo, got %v", filename, err)
			}
			if info != nil {
				t.Errorf("ExtractInfo(%s) returned info %v, expected nil", filename, info)
			}
		})
	}
}
