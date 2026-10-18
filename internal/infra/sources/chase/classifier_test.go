package chase_test

import (
	"io"
	"strings"
	"testing"

	"babylon/dataloader/internal/infra/sources/chase"
)

func TestDefaultClassifier_Classify(t *testing.T) {
	classifier := chase.NewDefaultClassifier()

	tests := []struct {
		name            string
		input           string
		expectedDialect chase.DialectType
		expectErr       bool
	}{
		{
			name:            "standard checking header",
			input:           "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\nCREDIT,12/31/2025,INTEREST,0.03,MISC,100.00,,\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
		{
			name:            "checking header with UTF-8 BOM",
			input:           "\xef\xbb\xbfDetails,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
		{
			name:            "checking header containing check or slip num",
			input:           "details,posting date,description,amount,type,balance,check or slip num\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
		{
			name:            "standard credit card header",
			input:           "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n12/21/2025,12/22/2025,LEETCODE,Shopping,Sale,-35.00,\n",
			expectedDialect: chase.DialectCreditCard,
			expectErr:       false,
		},
		{
			name:            "credit card header without transaction date but with category",
			input:           "Post Date,Description,Category,Type,Amount,Memo\n",
			expectedDialect: chase.DialectCreditCard,
			expectErr:       false,
		},
		{
			name:            "credit card header with UTF-8 BOM",
			input:           "\xef\xbb\xbfTransaction Date,Post Date,Description,Category,Type,Amount,Memo\n",
			expectedDialect: chase.DialectCreditCard,
			expectErr:       false,
		},
		{
			name:            "unknown CSV header",
			input:           "ID,Name,Email,Age\n1,Alice,alice@example.com,30\n",
			expectedDialect: chase.DialectUnknown,
			expectErr:       false,
		},
		{
			name:            "empty stream",
			input:           "",
			expectedDialect: chase.DialectUnknown,
			expectErr:       false,
		},
		{
			name:            "whitespace only stream",
			input:           "   \n\n\r\n  \t\n",
			expectedDialect: chase.DialectUnknown,
			expectErr:       false,
		},
		{
			name:            "leading blank lines before checking header",
			input:           "\n\nDetails,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.input)
			dialect, reconstructedReader, err := classifier.Classify(r)

			if (err != nil) != tt.expectErr {
				t.Fatalf("Classify() error = %v, expectErr = %v", err, tt.expectErr)
			}
			if dialect != tt.expectedDialect {
				t.Errorf("expected dialect %q, got %q", tt.expectedDialect, dialect)
			}

			// Verify stream preservation: reconstructedReader must contain all original bytes
			reconstructedBytes, readErr := io.ReadAll(reconstructedReader)
			if readErr != nil {
				t.Fatalf("failed to read reconstructed reader: %v", readErr)
			}
			if string(reconstructedBytes) != tt.input {
				t.Errorf("stream data corrupted:\nexpected: %q\ngot:      %q", tt.input, string(reconstructedBytes))
			}
		})
	}

	t.Run("nil reader returns error", func(t *testing.T) {
		_, _, err := classifier.Classify(nil)
		if err == nil {
			t.Error("expected error when reader is nil")
		}
	})
}
