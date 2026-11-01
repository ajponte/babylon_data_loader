package synthetic_test

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/infra/sources/synthetic"
)

// Legacy header verified from synthetic/data.go GenerateSyntheticData.
const legacySyntheticHeader = "Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #"

func newValidTestMeta(t *testing.T) model.FileMetadata {
	t.Helper()
	return model.FileMetadata{
		FileName:   "test-synthetic-data.csv",
		FileHash:   "sha256:synth-hash",
		AccountID:  "0000",
		IngestedAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
	}
}

func assertSyntheticTx(t *testing.T, got, want model.ChaseTransaction) {
	t.Helper()
	if got.AccountID != want.AccountID {
		t.Errorf("AccountID mismatch: got %q, want %q", got.AccountID, want.AccountID)
	}
	if !got.PostingDate.Equal(want.PostingDate) {
		t.Errorf("PostingDate mismatch: got %v, want %v", got.PostingDate, want.PostingDate)
	}
	if got.Description != want.Description {
		t.Errorf("Description mismatch: got %q, want %q", got.Description, want.Description)
	}
	if got.Details != want.Details {
		t.Errorf("Details mismatch: got %q, want %q", got.Details, want.Details)
	}
	if got.Amount != want.Amount {
		t.Errorf("Amount mismatch: got %v, want %v", got.Amount, want.Amount)
	}
	if got.Type != want.Type {
		t.Errorf("Type mismatch: got %q, want %q", got.Type, want.Type)
	}
	if (got.Balance == nil) != (want.Balance == nil) {
		t.Errorf("Balance nil mismatch: got %v, want %v", got.Balance, want.Balance)
	} else if got.Balance != nil && *got.Balance != *want.Balance {
		t.Errorf("Balance mismatch: got %v, want %v", *got.Balance, *want.Balance)
	}
	if got.CheckOrSlipNum != want.CheckOrSlipNum {
		t.Errorf("CheckOrSlipNum mismatch: got %q, want %q", got.CheckOrSlipNum, want.CheckOrSlipNum)
	}
	if got.Category != want.Category {
		t.Errorf("Category mismatch: got %q, want %q", got.Category, want.Category)
	}
	if got.Lineage.RowNumber != want.Lineage.RowNumber {
		t.Errorf("RowNumber mismatch: got %d, want %d", got.Lineage.RowNumber, want.Lineage.RowNumber)
	}
	if got.Lineage.FileName != want.Lineage.FileName {
		t.Errorf("FileName mismatch: got %q, want %q", got.Lineage.FileName, want.Lineage.FileName)
	}
	if got.Lineage.FileHash != want.Lineage.FileHash {
		t.Errorf("FileHash mismatch: got %q, want %q", got.Lineage.FileHash, want.Lineage.FileHash)
	}
}

func TestCSVExtractor_LegacyGeneratorOutput(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	csvData := legacySyntheticHeader + "\n" +
		"SALE,10/31/2026,Synthetic transaction 0,synthetic,123.45,DEBIT,4567.89,\n" +
		"CREDIT,10/30/2026,Synthetic transaction 1,synthetic,-50.00,CREDIT,4617.89,\n" +
		"SALE,10/29/2026,Synthetic transaction 2,synthetic,(75.25),DEBIT,4542.64,\n" +
		"FEE,10/28/2026,Synthetic transaction 3,synthetic,\"+$1,234.56\",DEBIT,3308.08,\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed unexpectedly: %v", err)
	}

	if batch.Destination != model.DestSynthetic {
		t.Errorf("expected destination %q, got %q", model.DestSynthetic, batch.Destination)
	}
	if batch.RecordCount != 4 {
		t.Errorf("expected RecordCount 4, got %d", batch.RecordCount)
	}
	if valErr := batch.Validate(); valErr != nil {
		t.Fatalf("batch validation failed: %v", valErr)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("batch.AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 4 {
		t.Fatalf("expected 4 transactions, got %d", len(txs))
	}

	bal0 := 4567.89
	bal1 := 4617.89
	bal2 := 4542.64
	bal3 := 3308.08
	want0 := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  2,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:      meta.AccountID,
		PostingDate:    time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC),
		Description:    "Synthetic transaction 0",
		Details:        "SALE",
		Amount:         123.45,
		Type:           "DEBIT",
		Balance:        &bal0,
		CheckOrSlipNum: "",
		Category:       "synthetic",
	}
	want1 := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  3,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:      meta.AccountID,
		PostingDate:    time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
		Description:    "Synthetic transaction 1",
		Details:        "CREDIT",
		Amount:         -50.00,
		Type:           "CREDIT",
		Balance:        &bal1,
		CheckOrSlipNum: "",
		Category:       "synthetic",
	}
	want2 := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  4,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:      meta.AccountID,
		PostingDate:    time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC),
		Description:    "Synthetic transaction 2",
		Details:        "SALE",
		Amount:         -75.25,
		Type:           "DEBIT",
		Balance:        &bal2,
		CheckOrSlipNum: "",
		Category:       "synthetic",
	}
	want3 := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  5,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:      meta.AccountID,
		PostingDate:    time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC),
		Description:    "Synthetic transaction 3",
		Details:        "FEE",
		Amount:         1234.56,
		Type:           "DEBIT",
		Balance:        &bal3,
		CheckOrSlipNum: "",
		Category:       "synthetic",
	}

	assertSyntheticTx(t, txs[0], want0)
	assertSyntheticTx(t, txs[1], want1)
	assertSyntheticTx(t, txs[2], want2)
	assertSyntheticTx(t, txs[3], want3)
}

func TestCSVExtractor_KeepsCheckingOnlyFields(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	csvData := legacySyntheticHeader + "\n" +
		"CHECK_ORDER,10/25/2026,ATM Withdrawal,banking,100.00,DEBIT,3500.25,1042\n" +
		"CHECK_PAYMENT,10/26/2026,Check Paid,banking,-250.00,DEBIT,3250.25,CK#1042\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed unexpectedly: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(txs))
	}

	if txs[0].Details != "CHECK_ORDER" {
		t.Errorf("expected Details %q, got %q", "CHECK_ORDER", txs[0].Details)
	}
	if txs[0].Balance == nil || *txs[0].Balance != 3500.25 {
		t.Errorf("expected Balance 3500.25, got %v", txs[0].Balance)
	}
	if txs[0].CheckOrSlipNum != "1042" {
		t.Errorf("expected CheckOrSlipNum %q, got %q", "1042", txs[0].CheckOrSlipNum)
	}

	if txs[1].Details != "CHECK_PAYMENT" {
		t.Errorf("expected Details %q, got %q", "CHECK_PAYMENT", txs[1].Details)
	}
	if txs[1].Balance == nil || *txs[1].Balance != 3250.25 {
		t.Errorf("expected Balance 3250.25, got %v", txs[1].Balance)
	}
	if txs[1].CheckOrSlipNum != "CK#1042" {
		t.Errorf("expected CheckOrSlipNum %q, got %q", "CK#1042", txs[1].CheckOrSlipNum)
	}
}

func TestCSVExtractor_EmptyBalanceIsNil(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	tests := []struct {
		name    string
		balance string
	}{
		{name: "empty field", balance: ""},
		{name: "whitespace only", balance: "   "},
		{name: "dash placeholder", balance: "-"},
		{name: "na placeholder", balance: "N/A"},
		{name: "null placeholder", balance: "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			csvData := fmt.Sprintf("%s\nSALE,10/31/2026,Desc,Cat,10.00,DEBIT,%s,\n", legacySyntheticHeader, tt.balance)
			batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("Extract failed unexpectedly: %v", err)
			}
			txs, err := batch.AsChaseTransactions()
			if err != nil {
				t.Fatalf("AsChaseTransactions failed: %v", err)
			}
			if len(txs) != 1 {
				t.Fatalf("expected 1 transaction, got %d", len(txs))
			}
			if txs[0].Balance != nil {
				t.Errorf("expected nil Balance, got %v", *txs[0].Balance)
			}
		})
	}
}

func TestCSVExtractor_PlaceholderBalances(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	placeholders := []string{
		"-",
		"--",
		"---",
		"n/a",
		"N/A",
		"na",
		"NA",
		"#n/a",
		"#N/A",
		"none",
		"None",
		"NONE",
		"null",
		"Null",
		"NULL",
		"nil",
		"Nil",
		"NIL",
	}

	for _, ph := range placeholders {
		t.Run("placeholder_"+ph, func(t *testing.T) {
			t.Parallel()
			csvData := fmt.Sprintf("%s\nSALE,10/31/2026,Desc,Cat,10.00,DEBIT,%s,\n", legacySyntheticHeader, ph)
			batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("Extract failed unexpectedly for placeholder %q: %v", ph, err)
			}
			txs, err := batch.AsChaseTransactions()
			if err != nil {
				t.Fatalf("AsChaseTransactions failed: %v", err)
			}
			if len(txs) != 1 {
				t.Fatalf("expected 1 transaction, got %d", len(txs))
			}
			if txs[0].Balance != nil {
				t.Errorf("expected nil Balance for placeholder %q, got %v", ph, *txs[0].Balance)
			}
		})
	}
}

func TestCSVExtractor_BlankLinesBeforeHeader(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	// Multiple empty lines (including whitespace) preceding the header row.
	csvData := "\n  \r\n\n" + legacySyntheticHeader + "\n" +
		"SALE,10/31/2026,Synthetic tx,cat,25.00,DEBIT,1000.00,\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed with blank lines before header: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if txs[0].Description != "Synthetic tx" {
		t.Errorf("expected Description 'Synthetic tx', got %q", txs[0].Description)
	}
	// Line 1: \n, Line 2: "  \r\n", Line 3: \n, Line 4: Header, Line 5: Data row
	if txs[0].Lineage.RowNumber != 5 {
		t.Errorf("expected RowNumber 5, got %d", txs[0].Lineage.RowNumber)
	}
}

func TestCSVExtractor_AccountingCurrencyFormats(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	tests := []struct {
		name       string
		amountRaw  string
		wantAmount float64
	}{
		{name: "accounting parentheses outside dollar $(10.00)", amountRaw: "$(10.00)", wantAmount: -10.00},
		{name: "accounting parentheses inside dollar ($10.00)", amountRaw: "($10.00)", wantAmount: -10.00},
		{name: "negative dollar -$10.00", amountRaw: "-$10.00", wantAmount: -10.00},
		{name: "dollar negative $-10.00", amountRaw: "$-10.00", wantAmount: -10.00},
		{name: "negative zero normalized -$0.00", amountRaw: "-$0.00", wantAmount: 0.0},
		{name: "accounting negative zero $(0.00)", amountRaw: "$(0.00)", wantAmount: 0.0},
		{name: "positive dollar +$10.00", amountRaw: "+$10.00", wantAmount: 10.00},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			csvData := fmt.Sprintf("%s\nSALE,10/31/2026,Desc,cat,%s,DEBIT,1000.00,\n", legacySyntheticHeader, tt.amountRaw)
			batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("Extract failed for amount %q: %v", tt.amountRaw, err)
			}
			txs, err := batch.AsChaseTransactions()
			if err != nil {
				t.Fatalf("AsChaseTransactions failed: %v", err)
			}
			if len(txs) != 1 {
				t.Fatalf("expected 1 transaction, got %d", len(txs))
			}
			if txs[0].Amount != tt.wantAmount {
				t.Errorf("Amount mismatch: got %v, want %v", txs[0].Amount, tt.wantAmount)
			}
			if tt.wantAmount == 0.0 && math.Signbit(txs[0].Amount) {
				t.Errorf("Amount %v retained negative sign bit (-0.0)", txs[0].Amount)
			}
		})
	}
}

func TestCSVExtractor_HeaderCaseAndWhitespace(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	header := " dEtAiLs , Posting Date , DeScRiPtIoN , cAtEgOrY , AMOUNT , Type , BaLaNcE , Check or Slip # "
	csvData := header + "\n" +
		"SALE,10/31/2026,Synthetic 0,cat,99.99,DEBIT,1234.56,999\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed with case/whitespace variations: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if txs[0].Amount != 99.99 {
		t.Errorf("expected Amount 99.99, got %v", txs[0].Amount)
	}
	if txs[0].Description != "Synthetic 0" {
		t.Errorf("expected Description 'Synthetic 0', got %q", txs[0].Description)
	}
}

func TestCSVExtractor_ColumnOrderIndependent(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	shuffledHeader := "Check or Slip #,Amount,Posting Date,Details,Balance,Description,Type,Category"
	csvData := shuffledHeader + "\n" +
		"SLIP-1,250.75,10/18/2026,TRANSFER,5000.00,Inter-account transfer,CREDIT,banking\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed on shuffled header: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}

	tx := txs[0]
	if tx.CheckOrSlipNum != "SLIP-1" {
		t.Errorf("expected CheckOrSlipNum 'SLIP-1', got %q", tx.CheckOrSlipNum)
	}
	if tx.Amount != 250.75 {
		t.Errorf("expected Amount 250.75, got %v", tx.Amount)
	}
	if tx.PostingDate != time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) {
		t.Errorf("unexpected PostingDate: %v", tx.PostingDate)
	}
	if tx.Details != "TRANSFER" {
		t.Errorf("expected Details 'TRANSFER', got %q", tx.Details)
	}
	if tx.Balance == nil || *tx.Balance != 5000.00 {
		t.Errorf("expected Balance 5000.00, got %v", tx.Balance)
	}
	if tx.Description != "Inter-account transfer" {
		t.Errorf("expected Description 'Inter-account transfer', got %q", tx.Description)
	}
	if tx.Type != "CREDIT" {
		t.Errorf("expected Type 'CREDIT', got %q", tx.Type)
	}
	if tx.Category != "banking" {
		t.Errorf("expected Category 'banking', got %q", tx.Category)
	}
}

func TestCSVExtractor_UTF8BOM(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	csvData := "\xef\xbb\xbf" + legacySyntheticHeader + "\n" +
		"SALE,10/31/2026,BOM Test,synthetic,42.00,DEBIT,1000.00,\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed with UTF-8 BOM: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if txs[0].Lineage.RowNumber != 2 {
		t.Errorf("expected RowNumber 2 with BOM, got %d", txs[0].Lineage.RowNumber)
	}
	if txs[0].Amount != 42.00 {
		t.Errorf("expected Amount 42.00, got %v", txs[0].Amount)
	}
}

func TestCSVExtractor_MissingRequiredColumns(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	tests := []struct {
		name       string
		header     string
		missingCol string
	}{
		{
			name:       "missing posting date",
			header:     "Details,Description,Category,Amount,Type,Balance,Check or Slip #",
			missingCol: "Posting Date",
		},
		{
			name:       "missing description",
			header:     "Details,Posting Date,Category,Amount,Type,Balance,Check or Slip #",
			missingCol: "Description",
		},
		{
			name:       "missing amount",
			header:     "Details,Posting Date,Description,Category,Type,Balance,Check or Slip #",
			missingCol: "Amount",
		},
		{
			name:       "missing all required",
			header:     "Details,Category,Type,Balance,Check or Slip #",
			missingCol: "Posting Date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			csvData := tt.header + "\nSALE,10/31/2026,Desc,synthetic,10.00,DEBIT,100.00,\n"
			_, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !errors.Is(err, synthetic.ErrMissingRequiredColumns) {
				t.Errorf("expected ErrMissingRequiredColumns, got: %v", err)
			}
			if !strings.Contains(err.Error(), tt.missingCol) {
				t.Errorf("expected error to name missing column %q, got: %v", tt.missingCol, err)
			}
		})
	}
}

func TestCSVExtractor_EmptyStream(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	tests := []struct {
		name string
		data string
	}{
		{name: "empty string", data: ""},
		{name: "newlines only", data: "\n\n\n"},
		{name: "whitespace only", data: "   \t\n  \r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := extractor.Extract(context.Background(), strings.NewReader(tt.data), meta)
			if !errors.Is(err, synthetic.ErrMissingHeader) {
				t.Errorf("expected ErrMissingHeader, got: %v", err)
			}
		})
	}
}

func TestCSVExtractor_HeaderOnly(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	csvData := legacySyntheticHeader + "\n"
	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed on header-only CSV: %v", err)
	}

	if batch.RecordCount != 0 {
		t.Errorf("expected RecordCount 0, got %d", batch.RecordCount)
	}
	if batch.Destination != model.DestSynthetic {
		t.Errorf("expected destination %q, got %q", model.DestSynthetic, batch.Destination)
	}
	if valErr := batch.Validate(); valErr != nil {
		t.Fatalf("batch validation failed: %v", valErr)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 0 {
		t.Errorf("expected 0 transactions, got %d", len(txs))
	}
}

func TestCSVExtractor_InvalidRows(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	tests := []struct {
		name       string
		preamble   string
		row        string
		wantDomain error
		wantLine   int
	}{
		{
			name:       "bad date format",
			row:        "SALE,invalid-date,Synthetic tx,synthetic,10.00,DEBIT,100.00,",
			wantDomain: model.ErrMissingPostingDate,
			wantLine:   2,
		},
		{
			name:       "empty date",
			row:        "SALE,,Synthetic tx,synthetic,10.00,DEBIT,100.00,",
			wantDomain: model.ErrMissingPostingDate,
			wantLine:   2,
		},
		{
			name:       "empty description",
			row:        "SALE,10/31/2026,,synthetic,10.00,DEBIT,100.00,",
			wantDomain: model.ErrMissingDescription,
			wantLine:   2,
		},
		{
			name:       "whitespace description",
			row:        "SALE,10/31/2026,   ,synthetic,10.00,DEBIT,100.00,",
			wantDomain: model.ErrMissingDescription,
			wantLine:   2,
		},
		{
			name:       "nan amount",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,NaN,DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   2,
		},
		{
			name:       "unparseable amount",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,not-an-amount,DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   2,
		},
		{
			name:       "empty amount",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,,DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   2,
		},
		{
			name:       "excess fraction digits amount",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,10.999,DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   2,
		},
		{
			name:       "bad comma grouping amount",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,\"1,23.45\",DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   2,
		},
		{
			name:       "short row missing description",
			row:        "SALE,10/31/2026",
			wantDomain: model.ErrMissingDescription,
			wantLine:   2,
		},
		{
			name:       "bad balance",
			row:        "SALE,10/31/2026,Synthetic tx,synthetic,10.00,DEBIT,not-a-balance,",
			wantDomain: model.ErrInvalidBalance,
			wantLine:   2,
		},
		{
			name:       "bad amount on line 3 after valid row",
			preamble:   "SALE,10/31/2026,Valid tx,synthetic,10.00,DEBIT,100.00,\n",
			row:        "SALE,10/30/2026,Invalid tx,synthetic,not-an-amount,DEBIT,100.00,",
			wantDomain: model.ErrInvalidAmount,
			wantLine:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			csvData := legacySyntheticHeader + "\n" + tt.preamble + tt.row + "\n"
			_, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !errors.Is(err, synthetic.ErrInvalidRow) {
				t.Errorf("expected ErrInvalidRow, got: %v", err)
			}
			if !errors.Is(err, tt.wantDomain) {
				t.Errorf("expected domain error %v, got: %v", tt.wantDomain, err)
			}
			expectedLine := tt.wantLine
			if expectedLine == 0 {
				expectedLine = 2
			}
			expectedLineStr := fmt.Sprintf("line %d", expectedLine)
			if !strings.Contains(err.Error(), expectedLineStr) {
				t.Errorf("expected error to contain %q, got: %v", expectedLineStr, err)
			}
		})
	}
}

func TestCSVExtractor_MalformedCSV(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	// An unclosed quote triggers *csv.ParseError from encoding/csv.
	malformedData := legacySyntheticHeader + "\n" +
		"SALE,10/31/2026,\"unclosed quote description,synthetic,10.00,DEBIT,100.00,\n"

	_, err := extractor.Extract(context.Background(), strings.NewReader(malformedData), meta)
	if err == nil {
		t.Fatal("expected error for malformed CSV, got nil")
	}
	if !errors.Is(err, synthetic.ErrMalformedCSV) {
		t.Errorf("expected ErrMalformedCSV, got: %v", err)
	}

	var parseErr *csv.ParseError
	if !errors.As(err, &parseErr) {
		t.Errorf("expected underlying error to be *csv.ParseError, got: %T (%v)", err, err)
	}
}

type cancelOnSecondReadReader struct {
	header []byte
	row    []byte
	step   int
	cancel context.CancelFunc
}

func (c *cancelOnSecondReadReader) Read(p []byte) (int, error) {
	c.step++
	if c.step == 1 {
		n := copy(p, c.header)
		return n, nil
	}
	c.cancel()
	n := copy(p, c.row)
	return n, nil
}

func TestCSVExtractor_CancelledContextUnwrapped(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	t.Run("pre-cancelled context", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		csvData := legacySyntheticHeader + "\nSALE,10/31/2026,Synthetic 0,cat,10.00,DEBIT,100.00,\n"
		_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		//nolint:errorlint // Proves invariant 4: unwrapped context error identity.
		if err != context.Canceled {
			t.Errorf("expected raw unwrapped context.Canceled, got: %v", err)
		}
	})

	t.Run("mid-stream cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cr := &cancelOnSecondReadReader{
			header: []byte(legacySyntheticHeader + "\n"),
			row:    []byte("SALE,10/31/2026,Synthetic 0,cat,10.00,DEBIT,100.00,\n"),
			cancel: cancel,
		}

		_, err := extractor.Extract(ctx, cr, meta)
		//nolint:errorlint // Proves invariant 4: unwrapped context error identity.
		if err != context.Canceled {
			t.Errorf("expected raw unwrapped context.Canceled, got: %v", err)
		}
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		defer cancel()

		csvData := legacySyntheticHeader + "\nSALE,10/31/2026,Synthetic 0,cat,10.00,DEBIT,100.00,\n"
		_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		//nolint:errorlint // Proves invariant 4: unwrapped context error identity.
		if err != context.DeadlineExceeded {
			t.Errorf("expected raw unwrapped context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("mid-stream deadline exceeded", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cr := &errOnSecondReadReader{
			header: []byte(legacySyntheticHeader + "\n"),
			err:    context.DeadlineExceeded,
		}

		_, err := extractor.Extract(ctx, cr, meta)
		//nolint:errorlint // Proves invariant 4: unwrapped context error identity.
		if err != context.DeadlineExceeded {
			t.Errorf("expected raw unwrapped context.DeadlineExceeded, got: %v", err)
		}
	})
}

type errOnSecondReadReader struct {
	header []byte
	err    error
	step   int
}

func (e *errOnSecondReadReader) Read(p []byte) (int, error) {
	e.step++
	if e.step == 1 {
		return copy(p, e.header), nil
	}
	return 0, e.err
}

func TestCSVExtractor_StreamReadError(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	ioErr := errors.New("simulated network read failure")
	cr := &errOnSecondReadReader{
		header: []byte(legacySyntheticHeader + "\n"),
		err:    ioErr,
	}

	_, err := extractor.Extract(context.Background(), cr, meta)
	if !errors.Is(err, ioErr) {
		t.Errorf("expected %v, got: %v", ioErr, err)
	}
}

func TestCSVExtractor_RowNumbersArePhysicalLines(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	// Line 1: Header
	// Line 2: Blank line
	// Line 3: Row 1
	// Line 4-5: Row 2 with multi-line quoted description
	// Line 6: Row 3
	csvData := legacySyntheticHeader + "\n\n" +
		"SALE,10/31/2026,Line 3 tx,cat,10.00,DEBIT,100.00,\n" +
		"SALE,10/30/2026,\"Multi\nLine\",cat,20.00,DEBIT,80.00,\n" +
		"SALE,10/29/2026,Line 6 tx,cat,30.00,DEBIT,50.00,\n"

	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("Extract failed unexpectedly: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("AsChaseTransactions failed: %v", err)
	}
	if len(txs) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(txs))
	}

	if txs[0].Lineage.RowNumber != 3 {
		t.Errorf("tx[0] expected RowNumber 3, got %d", txs[0].Lineage.RowNumber)
	}
	if txs[1].Lineage.RowNumber != 4 {
		t.Errorf("tx[1] expected RowNumber 4, got %d", txs[1].Lineage.RowNumber)
	}
	if txs[2].Lineage.RowNumber != 6 {
		t.Errorf("tx[2] expected RowNumber 6, got %d", txs[2].Lineage.RowNumber)
	}
}

func TestCSVExtractor_LongPreambleBounded(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	// A 64 KiB junk header line that should return ErrMissingRequiredColumns on record 1
	longJunkHeader := strings.Repeat("A", 64*1024) + "\n"
	data := longJunkHeader + "SHOULD_NOT_BE_READ,10/31/2026,Desc,cat,10.00,DEBIT,100.00,\n"

	_, err := extractor.Extract(context.Background(), strings.NewReader(data), meta)
	if err == nil {
		t.Fatal("expected error on long junk header, got nil")
	}
	if !errors.Is(err, synthetic.ErrMissingRequiredColumns) {
		t.Errorf("expected ErrMissingRequiredColumns, got: %v", err)
	}
}

func TestCSVExtractor_InvalidMetaAndNilReader(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	ctx := context.Background()

	t.Run("nil reader", func(t *testing.T) {
		t.Parallel()
		meta := newValidTestMeta(t)
		_, err := extractor.Extract(ctx, nil, meta)
		if !errors.Is(err, synthetic.ErrNilReader) {
			t.Errorf("expected ErrNilReader, got: %v", err)
		}
	})

	t.Run("nil context treated as Background", func(t *testing.T) {
		t.Parallel()
		meta := newValidTestMeta(t)
		csvData := legacySyntheticHeader + "\n"
		//nolint:staticcheck // SA1012: Testing nil context tolerance per extractor contract.
		batch, err := extractor.Extract(nil, strings.NewReader(csvData), meta)
		if err != nil {
			t.Fatalf("expected nil context to be treated as Background, got error: %v", err)
		}
		if batch.RecordCount != 0 {
			t.Errorf("expected RecordCount 0, got %d", batch.RecordCount)
		}
	})

	t.Run("invalid metadata - empty filename", func(t *testing.T) {
		t.Parallel()
		meta := newValidTestMeta(t)
		meta.FileName = ""
		_, err := extractor.Extract(ctx, strings.NewReader(legacySyntheticHeader+"\n"), meta)
		if !errors.Is(err, model.ErrMissingFileName) {
			t.Errorf("expected ErrMissingFileName, got: %v", err)
		}
	})

	t.Run("invalid metadata - empty account ID", func(t *testing.T) {
		t.Parallel()
		meta := newValidTestMeta(t)
		meta.AccountID = ""
		_, err := extractor.Extract(ctx, strings.NewReader(legacySyntheticHeader+"\n"), meta)
		if !errors.Is(err, model.ErrMissingAccountID) {
			t.Errorf("expected ErrMissingAccountID, got: %v", err)
		}
	})

	t.Run("invalid metadata - zero IngestedAt", func(t *testing.T) {
		t.Parallel()
		meta := newValidTestMeta(t)
		meta.IngestedAt = time.Time{}
		_, err := extractor.Extract(ctx, strings.NewReader(legacySyntheticHeader+"\n"), meta)
		if !errors.Is(err, model.ErrMissingIngestedAt) {
			t.Errorf("expected ErrMissingIngestedAt, got: %v", err)
		}
	})
}

func TestCSVExtractor_ConcurrentExtract(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	meta := newValidTestMeta(t)

	csvData := legacySyntheticHeader + "\n" +
		"SALE,10/31/2026,Concurrent tx 1,synthetic,10.00,DEBIT,1000.00,\n" +
		"CREDIT,10/30/2026,Concurrent tx 2,synthetic,20.00,CREDIT,1020.00,\n"

	const concurrency = 16
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
			if err != nil {
				errCh <- err
				return
			}
			if batch.RecordCount != 2 {
				errCh <- fmt.Errorf("expected RecordCount 2, got %d", batch.RecordCount)
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent extract error: %v", err)
	}
}

func TestCSVExtractor_SourceType(t *testing.T) {
	t.Parallel()
	extractor := synthetic.NewCSVExtractor()
	if got := extractor.SourceType(); got != "synthetic" {
		t.Errorf("expected SourceType %q, got %q", "synthetic", got)
	}
}

func BenchmarkCSVExtractor_Extract_500Rows(b *testing.B) {
	extractor := synthetic.NewCSVExtractor()
	meta := model.FileMetadata{
		FileName:   "benchmark-synthetic.csv",
		FileHash:   "hash-bench",
		AccountID:  "bench-acc",
		IngestedAt: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC),
	}

	var sb strings.Builder
	sb.WriteString(legacySyntheticHeader)
	sb.WriteByte('\n')
	for i := range 500 {
		fmt.Fprintf(&sb, "SALE,10/31/2026,Benchmark tx %d,synthetic,%.2f,DEBIT,%.2f,\n",
			i, float64(i)*1.5+10.0, 5000.0-float64(i)*1.5)
	}
	csvData := sb.String()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}
