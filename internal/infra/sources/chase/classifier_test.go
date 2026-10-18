package chase_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"babylon/dataloader/internal/infra/sources/chase"
)

func assertClassifyResult(t *testing.T, dialect, expectedDialect chase.DialectType, reconstructedReader io.Reader, input string) {
	t.Helper()
	if dialect != expectedDialect {
		t.Errorf("expected dialect %q, got %q", expectedDialect, dialect)
	}

	// Verify stream preservation: reconstructedReader must contain all original bytes
	reconstructedBytes, readErr := io.ReadAll(reconstructedReader)
	if readErr != nil {
		t.Fatalf("failed to read reconstructed reader: %v", readErr)
	}
	if string(reconstructedBytes) != input {
		t.Errorf("stream data corrupted:\nexpected: %q\ngot:      %q", input, string(reconstructedBytes))
	}
}

func TestDefaultClassifier_Classify(t *testing.T) {
	t.Parallel()
	classifier := chase.NewDefaultClassifier()
	ctx := context.Background()

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
			name:            "checking header without balance column",
			input:           "Details,Posting Date,Description,Amount,Type\nDEBIT,10/15/2026,Store,-10.00,DEBIT\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
		{
			name:            "checking header with details and posting date",
			input:           "Details,Posting Date\nDEBIT,10/15/2026\n",
			expectedDialect: chase.DialectChecking,
			expectErr:       false,
		},
		{
			name:            "foreign CSV containing balance column is unknown",
			input:           "Employee Name,Leave Balance,Start Date\nAlice,15,2024-01-01\n",
			expectedDialect: chase.DialectUnknown,
			expectErr:       false,
		},
		{
			name:            "standard credit card header",
			input:           "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n12/21/2025,12/22/2025,LEETCODE,Shopping,Sale,-35.00,\n",
			expectedDialect: chase.DialectCreditCard,
			expectErr:       false,
		},
		{
			name:            "credit card header with trans date alias",
			input:           "Trans Date,Post Date,Description,Amount\n10/14/2026,10/15/2026,Store,-10.00\n",
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
			t.Parallel()
			r := strings.NewReader(tt.input)
			dialect, reconstructedReader, err := classifier.Classify(ctx, r)

			if (err != nil) != tt.expectErr {
				t.Fatalf("Classify() error = %v, expectErr = %v", err, tt.expectErr)
			}
			assertClassifyResult(t, dialect, tt.expectedDialect, reconstructedReader, tt.input)
		})
	}
}

func TestDefaultClassifier_Classify_Errors(t *testing.T) {
	t.Parallel()
	classifier := chase.NewDefaultClassifier()
	ctx := context.Background()

	t.Run("nil reader returns error", func(t *testing.T) {
		t.Parallel()
		_, _, err := classifier.Classify(ctx, nil)
		if err == nil {
			t.Error("expected error when reader is nil")
		}
		if !errors.Is(err, chase.ErrNilReader) {
			t.Errorf("expected ErrNilReader, got %v", err)
		}
	})

	t.Run("nil context is safely handled", func(t *testing.T) {
		t.Parallel()
		var nilCtx context.Context
		dialect, _, err := classifier.Classify(nilCtx, strings.NewReader("Details,Posting Date,Balance\n"))
		if err != nil {
			t.Fatalf("unexpected error with nil context: %v", err)
		}
		if dialect != chase.DialectChecking {
			t.Errorf("expected dialect %q, got %q", chase.DialectChecking, dialect)
		}
	})

	t.Run("context cancelled returns error", func(t *testing.T) {
		t.Parallel()
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := classifier.Classify(canceledCtx, strings.NewReader("Details,Posting Date\n"))
		if err == nil {
			t.Error("expected error when context is cancelled")
		}
	})
}

func TestDefaultClassifier_Classify_PeekBounds(t *testing.T) {
	t.Parallel()
	classifier := chase.NewDefaultClassifier()
	ctx := context.Background()

	// 120KB of characters without any newline delimiter
	largeContent := strings.Repeat("A", 120*1024)
	r := strings.NewReader(largeContent)

	dialect, reconstructedReader, err := classifier.Classify(ctx, r)
	if err != nil {
		t.Fatalf("unexpected Classify error on large stream: %v", err)
	}
	if dialect != chase.DialectUnknown {
		t.Errorf("expected DialectUnknown, got %q", dialect)
	}

	// Ensure the entire 120KB content can still be read without corruption
	reconstructedBytes, readErr := io.ReadAll(reconstructedReader)
	if readErr != nil {
		t.Fatalf("failed to read reconstructed reader: %v", readErr)
	}
	if len(reconstructedBytes) != len(largeContent) {
		t.Fatalf("reconstructed size mismatch: expected %d, got %d", len(largeContent), len(reconstructedBytes))
	}
}

func TestDefaultClassifier_Classify_CreditCardWithBalance(t *testing.T) {
	t.Parallel()
	classifier := chase.NewDefaultClassifier()
	ctx := context.Background()

	input := "Card,Transaction Date,Post Date,Description,Category,Type,Amount,Balance,Memo\n1234,10/14/2026,10/15/2026,AMAZON,Shopping,Sale,-50.00,1200.00,Book\n"
	r := strings.NewReader(input)

	dialect, reconstructedReader, err := classifier.Classify(ctx, r)
	if err != nil {
		t.Fatalf("unexpected Classify error: %v", err)
	}
	assertClassifyResult(t, dialect, chase.DialectCreditCard, reconstructedReader, input)
}
