package chase_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/infra/sources/chase"
)

func TestChaseExtractor_SourceType(t *testing.T) {
	extractor := chase.NewExtractor(nil)
	if extractor.SourceType() != "chase" {
		t.Errorf("expected SourceType 'chase', got %q", extractor.SourceType())
	}
}

func TestChaseExtractor_Extract_CheckingInline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "checking.csv",
		FileHash:   "hash123",
		AccountID:  "chk-999",
		IngestedAt: now,
	}

	csvData := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,10/15/2026,"GROCERY STORE",-85.50,DEBIT,2414.50,
CREDIT,10/16/2026,"PAYROLL DIRECT DEP",1500.00,ACH,3914.50,
`
	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	if batch.Destination != model.DestChaseChecking {
		t.Errorf("expected destination %q, got %q", model.DestChaseChecking, batch.Destination)
	}
	if batch.AccountID != "chk-999" {
		t.Errorf("expected account ID 'chk-999', got %q", batch.AccountID)
	}
	if batch.RecordCount != 2 {
		t.Fatalf("expected 2 records, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(txs))
	}

	first := txs[0]
	if first.Description != "GROCERY STORE" {
		t.Errorf("expected Description 'GROCERY STORE', got %q", first.Description)
	}
	if first.Amount != -85.50 {
		t.Errorf("expected Amount -85.50, got %f", first.Amount)
	}
	if first.Balance == nil || *first.Balance != 2414.50 {
		t.Errorf("expected Balance 2414.50, got %v", first.Balance)
	}
	if first.Lineage.RowNumber != 2 {
		t.Errorf("expected row number 2, got %d", first.Lineage.RowNumber)
	}
}

func TestChaseExtractor_Extract_CreditCardInline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "credit_card.csv",
		FileHash:   "hash456",
		AccountID:  "card-888",
		IngestedAt: now,
	}

	csvData := `Transaction Date,Post Date,Description,Category,Type,Amount,Memo
10/14/2026,10/15/2026,AMAZON.COM,Shopping,Sale,-45.99,Kindle Book
10/16/2026,10/16/2026,COFFEE SHOP,Food & Drink,Sale,-4.50,Morning latte
`
	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	if batch.Destination != model.DestChaseCreditCard {
		t.Errorf("expected destination %q, got %q", model.DestChaseCreditCard, batch.Destination)
	}
	if batch.RecordCount != 2 {
		t.Fatalf("expected 2 records, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}

	first := txs[0]
	if first.Description != "AMAZON.COM" {
		t.Errorf("expected Description 'AMAZON.COM', got %q", first.Description)
	}
	if first.Category != "Shopping" {
		t.Errorf("expected Category 'Shopping', got %q", first.Category)
	}
	if first.Memo != "Kindle Book" {
		t.Errorf("expected Memo 'Kindle Book', got %q", first.Memo)
	}
	if first.Balance != nil {
		t.Errorf("expected nil Balance for credit card, got %v", first.Balance)
	}
	if first.Amount != -45.99 {
		t.Errorf("expected Amount -45.99, got %f", first.Amount)
	}
	if first.Lineage.RowNumber != 2 {
		t.Errorf("expected row number 2, got %d", first.Lineage.RowNumber)
	}
}

func TestChaseExtractor_Fixtures_Checking(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	fixturePath := filepath.Join("..", "..", "..", "..", "data", "unprocessed", "Chase6921_Activity_20260131.CSV")
	file, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture %s: %v", fixturePath, err)
	}
	defer file.Close()

	meta := model.FileMetadata{
		FileName:   "Chase6921_Activity_20260131.CSV",
		FileHash:   "hash6921",
		AccountID:  "chk-6921",
		IngestedAt: now,
	}

	batch, err := extractor.Extract(ctx, file, meta)
	if err != nil {
		t.Fatalf("failed to extract Chase6921: %v", err)
	}

	if batch.Destination != model.DestChaseChecking {
		t.Errorf("expected destination %q, got %q", model.DestChaseChecking, batch.Destination)
	}
	if batch.RecordCount != 89 {
		t.Errorf("expected 89 records from Chase6921, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 89 {
		t.Fatalf("expected 89 transactions, got %d", len(txs))
	}

	first := txs[0]
	if first.Description != "INTEREST PAYMENT" {
		t.Errorf("expected first Description 'INTEREST PAYMENT', got %q", first.Description)
	}
	if first.Amount != 0.03 {
		t.Errorf("expected first Amount 0.03, got %f", first.Amount)
	}
	if first.Balance == nil || *first.Balance != 1708.32 {
		t.Errorf("expected first Balance 1708.32, got %v", first.Balance)
	}
}

func TestChaseExtractor_Fixtures_CreditCard(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	fixturePath := filepath.Join("..", "..", "..", "..", "data", "unprocessed", "Chase2472_Activity20250101_20251231_20260201.CSV")
	file, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture %s: %v", fixturePath, err)
	}
	defer file.Close()

	meta := model.FileMetadata{
		FileName:   "Chase2472_Activity20250101_20251231_20260201.CSV",
		FileHash:   "hash2472",
		AccountID:  "card-2472",
		IngestedAt: now,
	}

	batch, err := extractor.Extract(ctx, file, meta)
	if err != nil {
		t.Fatalf("failed to extract Chase2472: %v", err)
	}

	if batch.Destination != model.DestChaseCreditCard {
		t.Errorf("expected destination %q, got %q", model.DestChaseCreditCard, batch.Destination)
	}
	if batch.RecordCount != 97 {
		t.Errorf("expected 97 records from Chase2472, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 97 {
		t.Fatalf("expected 97 transactions, got %d", len(txs))
	}

	first := txs[0]
	if first.Description != "LEETCODE.COM" {
		t.Errorf("expected first Description 'LEETCODE.COM', got %q", first.Description)
	}
	if first.Category != "Shopping" {
		t.Errorf("expected first Category 'Shopping', got %q", first.Category)
	}
	if first.Amount != -35.00 {
		t.Errorf("expected first Amount -35.00, got %f", first.Amount)
	}
	if first.Balance != nil {
		t.Errorf("expected nil Balance for credit card, got %v", first.Balance)
	}
}

func TestChaseExtractor_Errors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	t.Run("invalid metadata missing account ID", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, strings.NewReader(""), meta)
		if err == nil {
			t.Error("expected error with invalid metadata")
		}
	})

	t.Run("nil reader", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "test.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, nil, meta)
		if err == nil {
			t.Error("expected error with nil reader")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		meta := model.FileMetadata{
			FileName:   "test.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(canceledCtx, strings.NewReader("Details,Posting Date,Description,Amount\n"), meta)
		if err == nil {
			t.Error("expected error with canceled context")
		}
	})

	t.Run("unknown dialect", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "unknown.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, strings.NewReader("colA,colB,colC\n1,2,3\n"), meta)
		if err == nil {
			t.Error("expected error with unknown dialect")
		}
	})
}

func TestChaseExtractor_LeadingBlankLines(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "blank_lines.csv",
		FileHash:   "hash_blank",
		AccountID:  "chk-blank",
		IngestedAt: now,
	}

	csvData := "\n\r\n   \nDetails,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
		"DEBIT,10/15/2026,\"GROCERY STORE\",-85.50,DEBIT,2414.50,\n" +
		"CREDIT,10/16/2026,\"PAYROLL DIRECT DEP\",1500.00,ACH,3914.50,\n"

	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error on leading blank lines: %v", err)
	}

	if batch.RecordCount != 2 {
		t.Fatalf("expected 2 records, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}
	if len(txs) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(txs))
	}
	// Record 1 starts at physical line 5 (after 3 blank lines and header row)
	if txs[0].Lineage.RowNumber != 5 {
		t.Errorf("expected physical line 5, got %d", txs[0].Lineage.RowNumber)
	}
}

func TestChaseExtractor_PendingCreditCardTransactions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "pending_cc.csv",
		FileHash:   "hash_pending",
		AccountID:  "card-pending",
		IngestedAt: now,
	}

	csvData := `Transaction Date,Post Date,Description,Category,Type,Amount,Memo
10/14/2026,10/15/2026,SETTLED MERCHANT,Shopping,Sale,-45.99,Item
10/16/2026,,PENDING COFFEE,Food & Drink,Sale,-5.25,Latte
`
	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error on pending transaction: %v", err)
	}

	if batch.RecordCount != 2 {
		t.Fatalf("expected 2 records, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	pendingTx := txs[1]
	expectedDate := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	if !pendingTx.PostingDate.Equal(expectedDate) {
		t.Errorf("expected posting date %v from transaction date fallback, got %v", expectedDate, pendingTx.PostingDate)
	}
	if pendingTx.Description != "PENDING COFFEE" {
		t.Errorf("expected Description 'PENDING COFFEE', got %q", pendingTx.Description)
	}
}

func TestChaseExtractor_AccountingFormatAndISODates(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "accounting.csv",
		FileHash:   "hash_acc",
		AccountID:  "chk-acc",
		IngestedAt: now,
	}

	csvData := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,2026-10-18,"UTILITIES","($45.00)",DEBIT,"1,250.75",-
CREDIT,2026/10/19,"REFUND","+$100.50",CREDIT,"($200.00)",--
`
	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error on accounting format: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	if txs[0].Amount != -45.00 {
		t.Errorf("expected Amount -45.00, got %f", txs[0].Amount)
	}
	if txs[0].Balance == nil || *txs[0].Balance != 1250.75 {
		t.Errorf("expected Balance 1250.75, got %v", txs[0].Balance)
	}
	expectedDate0 := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	if !txs[0].PostingDate.Equal(expectedDate0) {
		t.Errorf("expected PostingDate %v, got %v", expectedDate0, txs[0].PostingDate)
	}

	if txs[1].Amount != 100.50 {
		t.Errorf("expected Amount 100.50, got %f", txs[1].Amount)
	}
	if txs[1].Balance == nil || *txs[1].Balance != -200.00 {
		t.Errorf("expected Balance -200.00, got %v", txs[1].Balance)
	}
	expectedDate1 := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	if !txs[1].PostingDate.Equal(expectedDate1) {
		t.Errorf("expected PostingDate %v, got %v", expectedDate1, txs[1].PostingDate)
	}
}

func TestChaseExtractor_CorruptedBalanceFails(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "corrupted_bal.csv",
		FileHash:   "hash_corrupt",
		AccountID:  "chk-corrupt",
		IngestedAt: now,
	}

	csvData := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,10/15/2026,"VALID TX",-10.00,DEBIT,CORRUPT_VAL,
`
	extractor := chase.NewExtractor(nil)
	_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err == nil {
		t.Fatal("expected Extract error for corrupted balance, got nil")
	}

	if !strings.Contains(err.Error(), "invalid balance") {
		t.Errorf("expected error message to contain 'invalid balance', got: %v", err)
	}
}

func TestChaseExtractor_MultilinePhysicalLineage(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "multiline.csv",
		FileHash:   "hash_multi",
		AccountID:  "chk-multi",
		IngestedAt: now,
	}

	// Line 1: Header
	// Line 2: Record 1 starts
	// Line 3: Record 1 description continues (multiline quoted field)
	// Line 4: Record 1 ends
	// Line 5: Record 2 starts
	csvData := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
		"DEBIT,10/15/2026,\"LINE 1 OF DESC\n" +
		"LINE 2 OF DESC\n" +
		"LINE 3 OF DESC\",-20.00,DEBIT,1000.00,\n" +
		"CREDIT,10/16/2026,\"NEXT ROW\",50.00,CREDIT,1050.00,\n"

	extractor := chase.NewExtractor(nil)
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error on multiline CSV: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	if len(txs) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(txs))
	}

	// First record starts at line 2
	if txs[0].Lineage.RowNumber != 2 {
		t.Errorf("expected record 0 Lineage.RowNumber to be 2, got %d", txs[0].Lineage.RowNumber)
	}
	// Second record starts at physical line 5
	if txs[1].Lineage.RowNumber != 5 {
		t.Errorf("expected record 1 Lineage.RowNumber to be 5, got %d", txs[1].Lineage.RowNumber)
	}
}
