package datasource_test

import (
	"errors"
	"testing"

	"babylon/dataloader/datalake/datasource"
)

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
		expectedDS  datasource.DataSource
		expectedAcc string
	}{
		{"synthetic_file.csv", datasource.Synthetic, "0000"},
		{"my_synthetic_statement.pdf", datasource.Synthetic, "0000"},
		{"SYNTHETIC_transactions.xls", datasource.Synthetic, "0000"},
		{"another_file_synthetic.txt", datasource.Synthetic, "0000"},
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
			if info.DataSource != string(test.expectedDS) {
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
