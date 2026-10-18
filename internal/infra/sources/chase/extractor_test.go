package chase_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/chase"
)

func TestChaseExtractor_SourceType(t *testing.T) {
	t.Parallel()
	extractor := chase.NewExtractor(nil)
	if extractor.SourceType() != "chase" {
		t.Errorf("expected SourceType 'chase', got %q", extractor.SourceType())
	}
}

func TestChaseExtractor_Extract_CheckingInline(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

const mockCheckingCSVFixture = `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
CREDIT,01/31/2026,"INTEREST PAYMENT",0.03,MISC,1708.32,
DEBIT,01/30/2026,"GROCERY STORE",-85.50,DEBIT,1708.29,
CHECK,01/29/2026,"CHECK PAID",-120.00,CHECK,1793.79,1042
`

const mockCreditCardCSVFixture = `Transaction Date,Post Date,Description,Category,Type,Amount,Memo
12/31/2025,01/01/2026,LEETCODE.COM,Shopping,Sale,-35.00,Annual Subscription
01/02/2026,01/03/2026,COFFEE SHOP,Food & Drink,Sale,-4.50,Morning latte
01/04/2026,,PENDING BOOKSTORE,Shopping,Sale,-22.15,Technical book
`

func TestChaseExtractor_Fixtures_Checking(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	meta := model.FileMetadata{
		FileName:   "Chase6921_Activity_20260131.CSV",
		FileHash:   "hash6921",
		AccountID:  "chk-6921",
		IngestedAt: now,
	}

	batch, err := extractor.Extract(ctx, strings.NewReader(mockCheckingCSVFixture), meta)
	if err != nil {
		t.Fatalf("failed to extract Chase6921 fixture: %v", err)
	}

	if batch.Destination != model.DestChaseChecking {
		t.Errorf("expected destination %q, got %q", model.DestChaseChecking, batch.Destination)
	}
	if batch.RecordCount != 3 {
		t.Errorf("expected 3 records from Chase6921 fixture, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(txs))
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

	last := txs[2]
	if last.CheckOrSlipNum != "1042" {
		t.Errorf("expected check num '1042', got %q", last.CheckOrSlipNum)
	}
}

func TestChaseExtractor_Fixtures_CreditCard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	meta := model.FileMetadata{
		FileName:   "Chase2472_Activity20250101_20251231_20260201.CSV",
		FileHash:   "hash2472",
		AccountID:  "card-2472",
		IngestedAt: now,
	}

	batch, err := extractor.Extract(ctx, strings.NewReader(mockCreditCardCSVFixture), meta)
	if err != nil {
		t.Fatalf("failed to extract Chase2472 fixture: %v", err)
	}

	if batch.Destination != model.DestChaseCreditCard {
		t.Errorf("expected destination %q, got %q", model.DestChaseCreditCard, batch.Destination)
	}
	if batch.RecordCount != 3 {
		t.Errorf("expected 3 records from Chase2472 fixture, got %d", batch.RecordCount)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(txs) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(txs))
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
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := chase.NewExtractor(nil)

	t.Run("invalid metadata missing account ID", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		meta := model.FileMetadata{
			FileName:   "test.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, nil, meta)
		if err == nil {
			t.Error("expected error with nil reader")
		}
		if !errors.Is(err, chase.ErrNilReader) {
			t.Errorf("expected ErrNilReader, got %v", err)
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		meta := model.FileMetadata{
			FileName:   "unknown.csv",
			AccountID:  "acc-1",
			IngestedAt: now,
		}
		_, err := extractor.Extract(ctx, strings.NewReader("colA,colB,colC\n1,2,3\n"), meta)
		if err == nil {
			t.Error("expected error with unknown dialect")
		}
		if !errors.Is(err, chase.ErrUnknownDialect) {
			t.Errorf("expected ErrUnknownDialect, got %v", err)
		}
	})
}

func TestChaseExtractor_LeadingBlankLines(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func assertSingleTransactionDate(t *testing.T, batch *model.IngestionBatch, expectedDate time.Time) {
	t.Helper()
	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast transactions: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if !txs[0].PostingDate.Equal(expectedDate) {
		t.Errorf("expected PostingDate %v, got %v", expectedDate, txs[0].PostingDate)
	}
}

func TestChaseExtractor_CreditCardPendingFormats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	pendingIndicators := []string{"", "Pending", "pending", "-", "--", "n/a", "N/A"}
	for _, indicator := range pendingIndicators {
		indicator := indicator
		t.Run("indicator_"+indicator, func(t *testing.T) {
			t.Parallel()
			csvData := fmt.Sprintf("Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n10/14/2026,%s,COFFEE,Food & Drink,Sale,-4.50,latte\n", indicator)
			meta := testFileMetadata("card-pending", "pending.csv")
			batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("unexpected error for pending indicator %q: %v", indicator, err)
			}
			assertSingleTransactionDate(t, batch, time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
		})
	}
}

func TestChaseExtractor_CheckingWithoutBalance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	csvData := "Details,Posting Date,Description,Amount\nDEBIT,10/15/2026,STORE,-20.00\n"
	meta := testFileMetadata("chk-nobal", "nobal.csv")
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}
	if batch.Destination != model.DestChaseChecking {
		t.Errorf("expected destination %q, got %q", model.DestChaseChecking, batch.Destination)
	}
	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast transactions: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if txs[0].Balance != nil {
		t.Errorf("expected nil Balance, got %v", *txs[0].Balance)
	}
}

func TestChaseExtractor_CreditCardWithBalance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	csvData := "Transaction Date,Post Date,Description,Category,Type,Amount,Balance,Memo\n10/14/2026,10/15/2026,AMAZON,Shopping,Sale,-50.00,1200.00,Book\n"
	meta := testFileMetadata("card-bal", "card_bal.csv")
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}
	if batch.Destination != model.DestChaseCreditCard {
		t.Errorf("expected destination %q, got %q", model.DestChaseCreditCard, batch.Destination)
	}
	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to cast transactions: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	if txs[0].Balance == nil {
		t.Fatal("expected non-nil balance for credit card, got nil")
	}
	if *txs[0].Balance != 1200.00 {
		t.Errorf("expected balance 1200.00, got %f", *txs[0].Balance)
	}
}

func TestChaseExtractor_MissingAmountRejection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	missingAmts := []string{"-", "--", "n/a", "N/A", "   "}
	for _, amt := range missingAmts {
		amt := amt
		t.Run("missing_amount_"+strings.TrimSpace(amt), func(t *testing.T) {
			t.Parallel()
			csvData := fmt.Sprintf("Details,Posting Date,Description,Amount,Type,Balance\nDEBIT,10/15/2026,STORE,%s,DEBIT,100.00\n", amt)
			meta := testFileMetadata("chk-missing-amt", "missing_amt.csv")
			_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err == nil {
				t.Fatalf("expected error for missing amount %q, got nil", amt)
			}
		})
	}
}

func runConcurrentExtract(t *testing.T, ctx context.Context, extractor port.Extractor, id int) {
	t.Helper()
	var (
		csvData string
		meta    model.FileMetadata
	)
	if id%2 == 0 {
		csvData = "Details,Posting Date,Description,Amount,Type,Balance\nDEBIT,10/15/2026,STORE,-10.00,DEBIT,100.00\n"
		meta = testFileMetadata(fmt.Sprintf("chk-%d", id), fmt.Sprintf("chk_%d.csv", id))
	} else {
		csvData = "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n10/14/2026,10/15/2026,STORE,Shopping,Sale,-20.00,memo\n"
		meta = testFileMetadata(fmt.Sprintf("card-%d", id), fmt.Sprintf("card_%d.csv", id))
	}
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Errorf("worker %d: unexpected Extract error: %v", id, err)
		return
	}
	if batch.RecordCount != 1 {
		t.Errorf("worker %d: expected 1 record, got %d", id, batch.RecordCount)
	}
}

func TestChaseExtractor_ConcurrentExecution(t *testing.T) {
	t.Parallel()
	extractor := chase.NewExtractor(nil)
	var wg sync.WaitGroup
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConcurrentExtract(t, ctx, extractor, id)
		}(i)
	}
	wg.Wait()
}

func TestChaseExtractor_AccountingFormatAndISODates(t *testing.T) {
	t.Parallel()
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

func testFileMetadata(accountID, fileName string) model.FileMetadata {
	return model.FileMetadata{
		FileName:   fileName,
		FileHash:   "hash-" + accountID,
		AccountID:  accountID,
		IngestedAt: time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
	}
}

type cancelingReader struct {
	r      io.Reader
	cancel context.CancelFunc
}

func (cr *cancelingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.cancel()
	}
	return n, err
}

func TestChaseExtractor_SentinelErrors(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-sentinel", "test.csv")

	tests := []struct {
		name        string
		csvData     string
		passNilR    bool
		expectedErr error
	}{
		{
			name:        "nil reader returns ErrNilReader",
			passNilR:    true,
			expectedErr: chase.ErrNilReader,
		},
		{
			name:        "unrecognized header returns ErrUnknownDialect",
			csvData:     "Alpha,Beta,Gamma\n1,2,3\n",
			expectedErr: chase.ErrUnknownDialect,
		},
		{
			name:        "checking header missing amount returns ErrMissingRequiredColumns",
			csvData:     "Details,Posting Date,Description,Balance\nDEBIT,10/15/2026,Store,100.00\n",
			expectedErr: chase.ErrMissingRequiredColumns,
		},
		{
			name:        "credit card header missing amount returns ErrMissingRequiredColumns",
			csvData:     "Transaction Date,Post Date,Description,Category\n10/14/2026,10/15/2026,Shop,Retail\n",
			expectedErr: chase.ErrMissingRequiredColumns,
		},
		{
			name:        "checking header missing description returns ErrMissingRequiredColumns",
			csvData:     "Details,Posting Date,Amount,Balance\nDEBIT,10/15/2026,-10.00,100.00\n",
			expectedErr: chase.ErrMissingRequiredColumns,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r io.Reader
			if !tt.passNilR {
				r = strings.NewReader(tt.csvData)
			}
			_, err := extractor.Extract(ctx, r, meta)
			if err == nil {
				t.Fatalf("expected error matching %v, got nil", tt.expectedErr)
			}
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("expected errors.Is(err, %v), got %v", tt.expectedErr, err)
			}
		})
	}
}

func TestChaseExtractor_NonFiniteAmounts(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)

	tests := []struct {
		name    string
		csvData string
	}{
		{
			name: "checking amount NaN",
			csvData: "Details,Posting Date,Description,Amount,Type,Balance\n" +
				"DEBIT,10/15/2026,STORE,NaN,DEBIT,100.00\n",
		},
		{
			name: "checking amount positive Inf",
			csvData: "Details,Posting Date,Description,Amount,Type,Balance\n" +
				"DEBIT,10/15/2026,STORE,Inf,DEBIT,100.00\n",
		},
		{
			name: "checking amount negative Inf",
			csvData: "Details,Posting Date,Description,Amount,Type,Balance\n" +
				"DEBIT,10/15/2026,STORE,-Inf,DEBIT,100.00\n",
		},
		{
			name: "credit card amount NaN",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"10/14/2026,10/15/2026,STORE,Shopping,Sale,NaN,memo\n",
		},
		{
			name: "credit card amount positive Inf",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"10/14/2026,10/15/2026,STORE,Shopping,Sale,Inf,memo\n",
		},
		{
			name: "credit card amount negative Inf",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"10/14/2026,10/15/2026,STORE,Shopping,Sale,-Inf,memo\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := testFileMetadata("acc-nonfinite", "nonfinite.csv")
			_, err := extractor.Extract(ctx, strings.NewReader(tt.csvData), meta)
			if err == nil {
				t.Fatal("expected error for non-finite amount, got nil")
			}
			if !strings.Contains(err.Error(), "non-finite number") {
				t.Errorf("expected error mentioning 'non-finite number', got: %v", err)
			}
		})
	}
}

func TestChaseExtractor_HeaderOnlyCSV(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)

	tests := []struct {
		name        string
		csvData     string
		expectedDst model.Destination
	}{
		{
			name:        "checking header only",
			csvData:     "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n",
			expectedDst: model.DestChaseChecking,
		},
		{
			name:        "credit card header only",
			csvData:     "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n",
			expectedDst: model.DestChaseCreditCard,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := testFileMetadata("acc-empty", "empty.csv")
			batch, err := extractor.Extract(ctx, strings.NewReader(tt.csvData), meta)
			if err != nil {
				t.Fatalf("unexpected error for header-only CSV: %v", err)
			}
			if batch.Destination != tt.expectedDst {
				t.Errorf("expected destination %s, got %s", tt.expectedDst, batch.Destination)
			}
			if batch.RecordCount != 0 {
				t.Errorf("expected 0 RecordCount, got %d", batch.RecordCount)
			}
			txs, err := batch.AsChaseTransactions()
			if err != nil {
				t.Fatalf("unexpected error casting transactions: %v", err)
			}
			if len(txs) != 0 {
				t.Errorf("expected 0 transactions, got %d", len(txs))
			}
		})
	}
}

func TestChaseExtractor_MalformedCSVSyntax(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-malformed", "malformed.csv")

	csvData := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
		"DEBIT,10/15/2026,\"UNCLOSED QUOTE DESCRIPTION,-25.00,DEBIT,100.00,\n"

	_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err == nil {
		t.Fatal("expected error on malformed CSV with unclosed quote, got nil")
	}
}

func TestChaseExtractor_CorruptedDates(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-corrupt-date", "corrupt_date.csv")

	tests := []struct {
		name    string
		csvData string
	}{
		{
			name: "checking invalid calendar date",
			csvData: "Details,Posting Date,Description,Amount,Type,Balance\n" +
				"DEBIT,99/99/9999,STORE,-25.00,DEBIT,100.00\n",
		},
		{
			name: "credit card missing both dates",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				",,STORE,Shopping,Sale,-25.00,\n",
		},
		{
			name: "credit card invalid date format",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"invalid-date,invalid-date,STORE,Shopping,Sale,-25.00,\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractor.Extract(ctx, strings.NewReader(tt.csvData), meta)
			if err == nil {
				t.Fatalf("expected error for corrupted date in %s, got nil", tt.name)
			}
		})
	}
}

func TestChaseExtractor_ContextCancellation_HeaderAndBody(t *testing.T) {
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-cancel", "cancel.csv")

	t.Run("pre-canceled context during extract", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		csvData := "Details,Posting Date,Description,Amount\nDEBIT,10/15/2026,Store,-10.00\n"
		_, err := extractor.Extract(canceledCtx, strings.NewReader(csvData), meta)
		if err == nil {
			t.Fatal("expected error with pre-canceled context, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("cancellation during stream processing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		csvData := "Details,Posting Date,Description,Amount,Type,Balance\n" +
			"DEBIT,10/15/2026,Row 1,-10.00,DEBIT,100.00\n" +
			"DEBIT,10/16/2026,Row 2,-20.00,DEBIT,80.00\n"

		cr := &cancelingReader{
			r:      strings.NewReader(csvData),
			cancel: cancel,
		}

		_, err := extractor.Extract(ctx, cr, meta)
		if err == nil {
			t.Fatal("expected error when context canceled mid-stream, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestChaseExtractor_AccountingNegativesAndUnicodeSpaces(t *testing.T) {
	ctx := context.Background()
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-unicode", "unicode.csv")

	csvData := "Details,Posting Date,Description,Amount,Type,Balance\n" +
		"DEBIT,10/15/2026,\"OUTER DOLLAR PARENS\",\"$(45.00)\",DEBIT,\"$\u00a01,250.75\"\n" +
		"DEBIT,10/16/2026,\"SPACED PARENS\",\"$( 99.50 )\",DEBIT,\"($200.00)\"\n" +
		"CREDIT,10/17/2026,\"NARROW NBSP\",\"+\u202f$50.25\",CREDIT,\"100.00\"\n"

	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected error on accounting/unicode formats: %v", err)
	}

	txs, err := batch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("unexpected error extracting transactions: %v", err)
	}
	if len(txs) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(txs))
	}

	if txs[0].Amount != -45.00 {
		t.Errorf("row 0 expected Amount -45.00, got %f", txs[0].Amount)
	}
	if txs[0].Balance == nil || *txs[0].Balance != 1250.75 {
		t.Errorf("row 0 expected Balance 1250.75, got %v", txs[0].Balance)
	}

	if txs[1].Amount != -99.50 {
		t.Errorf("row 1 expected Amount -99.50, got %f", txs[1].Amount)
	}
	if txs[1].Balance == nil || *txs[1].Balance != -200.00 {
		t.Errorf("row 1 expected Balance -200.00, got %v", txs[1].Balance)
	}

	if txs[2].Amount != 50.25 {
		t.Errorf("row 2 expected Amount 50.25, got %f", txs[2].Amount)
	}
}

func TestChaseExtractor_NilContext(t *testing.T) {
	extractor := chase.NewExtractor(nil)
	meta := testFileMetadata("acc-nil-ctx", "nil_ctx.csv")

	csvData := "Details,Posting Date,Description,Amount,Type,Balance\n" +
		"DEBIT,10/15/2026,Store,-10.00,DEBIT,100.00\n"

	var nilCtx context.Context
	batch, err := extractor.Extract(nilCtx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected error with nil context: %v", err)
	}
	if batch.RecordCount != 1 {
		t.Errorf("expected 1 record, got %d", batch.RecordCount)
	}
}
