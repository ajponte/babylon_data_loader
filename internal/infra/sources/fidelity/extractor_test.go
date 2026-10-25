package fidelity_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/fidelity"
)

const mockPortfolioPositionsFixture = `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,SPAXX**,HELD IN MONEY MARKET,,,,$3250.00,,,,,25.00%,,,Cash,
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash,
SYNTH-ROTH-101,ROTH IRA,FBGRX,FIDELITY BLUE CHIP GROWTH,10.000,$296.25,+$2.10,$2962.50,+$21.00,+0.71%,+$462.50,+18.50%,22.79%,$2500.00,$250.00,Cash,
SYNTH-401K-202,TRADITIONAL 401K,FXAIX,FIDELITY 500 INDEX,25.500,$180.20,-$0.80,$4595.10,-$20.40,-0.44%,+$595.10,+14.88%,100.00%,$4000.00,$156.86,Cash,
SYNTH-TAX-303,INDIVIDUAL TOD,**,,--,--,--,$0.00,--,,--,--,--,--,--,,

"The data and information in this spreadsheet is provided to you solely for your use and is not for distribution. The spreadsheet is provided for informational purposes only, and is not intended to provide advice, nor should it be construed as an offer to sell, a solicitation of an offer to buy or a recommendation for any security by Fidelity or any third party. Data and information shown is based on information known to Fidelity as of the date it was exported and is subject to change. It should not be used in place of your account statements or trade confirmations and is not intended for tax reporting purposes. For more information on the data included in this spreadsheet, including any limitations thereof, go to Fidelity.com."

"Brokerage services are provided by Fidelity Brokerage Services LLC (FBS), 900 Salem Street, Smithfield, RI 02917. Custody and other services provided by National Financial Services LLC (NFS). Both are Fidelity Investment companies and members SIPC, NYSE. Neither FBS nor NFS offer crypto as a direct investment nor provide trading or custody services for such assets."

"Date downloaded Sep-25-2026 9:50 p.m ET"
`

func withFooter(csvData string) string {
	return csvData + "\n\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n"
}

func TestFidelityExtractor_SourceType(t *testing.T) {
	extractor := fidelity.NewExtractor()
	if extractor.SourceType() != "fidelity" {
		t.Errorf("expected SourceType 'fidelity', got %q", extractor.SourceType())
	}
}

func TestFidelityExtractor_Extract_Fixture(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "Portfolio_Positions_Synthetic.csv",
		FileHash:   "hash_fid_synthetic",
		AccountID:  "SYNTH-DEFAULT",
		IngestedAt: now,
	}

	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(mockPortfolioPositionsFixture), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	if batch.Destination != model.DestFidelityPosition {
		t.Errorf("expected destination %q, got %q", model.DestFidelityPosition, batch.Destination)
	}
	if batch.RecordCount != 5 {
		t.Fatalf("expected 5 records extracted from fixture, got %d", batch.RecordCount)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to type-assert payload: %v", err)
	}
	if len(positions) != 5 {
		t.Fatalf("expected 5 positions, got %d", len(positions))
	}

	spaxx := positions[0]
	assertHolding(t, spaxx, "SYNTH-ROTH-101", "SPAXX**", 3250.00)
	if spaxx.Description != "HELD IN MONEY MARKET" {
		t.Errorf("expected Description 'HELD IN MONEY MARKET', got %q", spaxx.Description)
	}

	fskax := positions[1]
	assertHolding(t, fskax, "SYNTH-ROTH-101", "FSKAX", 6787.50)
	if fskax.Quantity != 45.250 || fskax.LastPrice != 150.00 || fskax.CostBasis != 6000.00 {
		t.Errorf("unexpected fskax metrics: %+v", fskax)
	}

	zeroPos := positions[4]
	assertHolding(t, zeroPos, "SYNTH-TAX-303", "CASH", 0.0)

	if spaxx.Lineage.RowNumber != 2 {
		t.Errorf("expected spaxx Lineage.RowNumber 2, got %d", spaxx.Lineage.RowNumber)
	}
	if spaxx.Lineage.FileName != meta.FileName || spaxx.Lineage.FileHash != meta.FileHash {
		t.Errorf("unexpected spaxx Lineage metadata: %+v", spaxx.Lineage)
	}
}

func assertHolding(
	t *testing.T,
	pos model.FidelityPosition,
	expectedAccount, expectedSymbol string,
	expectedVal float64,
) {
	t.Helper()
	if pos.AccountID != expectedAccount {
		t.Errorf("expected AccountID %q, got %q", expectedAccount, pos.AccountID)
	}
	if pos.Symbol != expectedSymbol {
		t.Errorf("expected Symbol %q, got %q", expectedSymbol, pos.Symbol)
	}
	if pos.CurrentValue != expectedVal {
		t.Errorf("expected CurrentValue %f, got %f", expectedVal, pos.CurrentValue)
	}
}

func TestFidelityExtractor_Errors(t *testing.T) {
	baseCtx := context.Background()
	canceledCtx, cancel := context.WithCancel(baseCtx)
	cancel()

	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	validMeta := model.FileMetadata{
		FileName:   "test.csv",
		AccountID:  "acc-1",
		IngestedAt: now,
	}

	tests := []struct {
		name        string
		ctx         context.Context
		reader      io.Reader
		meta        model.FileMetadata
		wantErr     bool
		expectedIs  error
		wantRecords int
	}{
		{
			name:   "invalid metadata missing account ID",
			ctx:    baseCtx,
			reader: strings.NewReader(""),
			meta: model.FileMetadata{
				FileName:   "test.csv",
				IngestedAt: now,
			},
			wantErr: true,
		},
		{
			name:       "nil reader returns ErrNilReader",
			ctx:        baseCtx,
			reader:     nil,
			meta:       validMeta,
			wantErr:    true,
			expectedIs: fidelity.ErrNilReader,
		},
		{
			name:    "nil context returns error",
			ctx:     nil,
			reader:  strings.NewReader(""),
			meta:    validMeta,
			wantErr: true,
		},
		{
			name:       "cancelled context returns context.Canceled",
			ctx:        canceledCtx,
			reader:     strings.NewReader("Account number,Symbol\n"),
			meta:       validMeta,
			wantErr:    true,
			expectedIs: context.Canceled,
		},
		{
			name:        "empty stream returns empty batch without error",
			ctx:         baseCtx,
			reader:      strings.NewReader(""),
			meta:        validMeta,
			wantErr:     false,
			wantRecords: 0,
		},
	}

	extractor := fidelity.NewExtractor()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := extractor.Extract(tc.ctx, tc.reader, tc.meta)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.name)
				}
				if tc.expectedIs != nil && !errors.Is(err, tc.expectedIs) {
					t.Errorf("expected errors.Is(err, %v), got %v", tc.expectedIs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.name, err)
			}
			if batch.RecordCount != tc.wantRecords {
				t.Errorf("expected %d records, got %d", tc.wantRecords, batch.RecordCount)
			}
		})
	}
}

func TestFidelityExtractor_RejectArbitraryCSV(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "arbitrary.csv",
		FileHash:   "hash_arb",
		AccountID:  "acc-arb",
		IngestedAt: now,
	}

	// Arbitrary Chase CSV lacking Fidelity signature columns
	chaseCSV := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,10/15/2026,"GROCERY STORE",-85.50,DEBIT,2414.50,
`
	extractor := fidelity.NewExtractor()
	_, err := extractor.Extract(ctx, strings.NewReader(chaseCSV), meta)
	if err == nil {
		t.Fatal("expected Extract error for arbitrary CSV missing Fidelity columns, got nil")
	}

	if !errors.Is(err, fidelity.ErrMissingRequiredColumns) {
		t.Errorf("expected ErrMissingRequiredColumns, got: %v", err)
	}
	if !strings.Contains(err.Error(), "fidelity CSV missing required header column") {
		t.Errorf("expected error to mention missing required header column, got: %v", err)
	}
}

func TestFidelityExtractor_FilterSummaryTotals_PreserveFundsWithTotal(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "positions_with_totals.csv",
		FileHash:   "hash_totals",
		AccountID:  "FID-123",
		IngestedAt: now,
	}

	csvData := `Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
FID-123,Individual,AAPL,APPLE INC,10,150.00,$1500.00,$1200.00
FID-123,Individual,VTI,Vanguard Total Stock Market Index Fund,25,220.00,$5500.00,$4800.00
FID-123,Individual,TTE,TOTAL ENERGIES SE,15,65.00,$975.00,$900.00
FID-123,Individual,,HELD IN MONEY MARKET,,,$500.00,$500.00
FID-123,Individual,Total,Account Total,,, $8475.00, $7400.00
Total Market Value,,,,,,$8475.00,
Account Total,,,,,,$8475.00,
"The data and information is proprietary",,,,,,,
`
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(withFooter(csvData)), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	// AAPL, VTI, TTE, and CASH should be retained; summary totals and disclaimer excluded
	if len(positions) != 4 {
		t.Fatalf("expected exactly 4 positions (AAPL, VTI, TTE, CASH), got %d", len(positions))
	}

	expectedSymbols := []string{"AAPL", "VTI", "TTE", "CASH"}
	for i, expected := range expectedSymbols {
		if positions[i].Symbol != expected {
			t.Errorf("position %d: expected symbol %q, got %q", i, expected, positions[i].Symbol)
		}
	}
}

type cashNormalizationTestCase struct {
	name         string
	csvRow       string
	expectKept   bool
	expectSymbol string
}

func TestFidelityExtractor_CashSymbolNormalization(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "cash_normalization.csv",
		FileHash:   "hash_cash",
		AccountID:  "FID-CASH-TEST",
		IngestedAt: now,
	}

	tests := []cashNormalizationTestCase{
		{
			name:         "asterisks with empty description normalized to CASH",
			csvRow:       "FID-1,Taxable,**,,0.00,0.00,$250.00,$250.00",
			expectKept:   true,
			expectSymbol: "CASH",
		},
		{
			name:         "asterisks with cash description normalized to CASH",
			csvRow:       "FID-1,Taxable,**,HELD IN MONEY MARKET,0.00,0.00,$250.00,$250.00",
			expectKept:   true,
			expectSymbol: "CASH",
		},
		{
			name:         "empty symbol with fcash description normalized to CASH",
			csvRow:       "FID-1,Taxable,,HELD IN FCASH,0.00,1.00,$1500.00,$1500.00",
			expectKept:   true,
			expectSymbol: "CASH",
		},
		{
			name:         "empty symbol with deposit sweep normalized to CASH",
			csvRow:       "FID-1,Taxable,,FDIC-INSURED DEPOSIT SWEEP,0.00,1.00,$3500.00,$3500.00",
			expectKept:   true,
			expectSymbol: "CASH",
		},
		{
			name:         "empty symbol with core description normalized to CASH",
			csvRow:       "FID-1,Taxable,,CORE CASH,0.00,1.00,$500.00,$500.00",
			expectKept:   true,
			expectSymbol: "CASH",
		},
		{
			name:         "SPAXX footnote asterisks preserved",
			csvRow:       "FID-1,Taxable,SPAXX**,HELD IN MONEY MARKET,10.0,1.00,$10.00,$10.00",
			expectKept:   true,
			expectSymbol: "SPAXX**",
		},
		{
			name:       "empty symbol with non-cash description skipped",
			csvRow:     "FID-1,Taxable,,PENDING EQUITY SETTLEMENT,0.00,0.00,$50.00,$50.00",
			expectKept: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCashNormalizationCase(t, meta, tc)
		})
	}
}

func runCashNormalizationCase(t *testing.T, meta model.FileMetadata, tc cashNormalizationTestCase) {
	t.Helper()
	csvData := withFooter(
		"Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total\n" +
			tc.csvRow + "\n",
	)

	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	if !tc.expectKept {
		if len(positions) != 0 {
			t.Fatalf("expected 0 positions, got %d", len(positions))
		}
		return
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
	if positions[0].Symbol != tc.expectSymbol {
		t.Errorf("expected symbol %q, got %q", tc.expectSymbol, positions[0].Symbol)
	}
}

func TestFidelityExtractor_BOMHandling(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "bom_test.csv",
		FileHash:   "hash_bom",
		AccountID:  "FID-BOM",
		IngestedAt: now,
	}

	t.Run("valid UTF-8 BOM prefix stripped", func(t *testing.T) {
		bomPrefix := "\xef\xbb\xbf"
		csvData := withFooter(bomPrefix + `Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
FID-BOM,Individual,MSFT,MICROSOFT CORP,5,400.00,$2000.00,$1800.00
`)
		extractor := fidelity.NewExtractor()
		batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		if err != nil {
			t.Fatalf("unexpected Extract error with BOM: %v", err)
		}

		positions, err := batch.AsFidelityPositions()
		if err != nil {
			t.Fatalf("failed to cast payload: %v", err)
		}

		if len(positions) != 1 {
			t.Fatalf("expected 1 position, got %d", len(positions))
		}
		if positions[0].Symbol != "MSFT" {
			t.Errorf("expected symbol 'MSFT', got %q", positions[0].Symbol)
		}
	})

	t.Run("non-EOF error in stream peek surfaced", func(t *testing.T) {
		simulatedErr := errors.New("network socket read failure")
		errReader := &testErrorReader{err: simulatedErr}

		extractor := fidelity.NewExtractor()
		_, err := extractor.Extract(ctx, errReader, meta)
		if err == nil {
			t.Fatal("expected error from failing stream peek, got nil")
		}
		if !errors.Is(err, simulatedErr) {
			t.Errorf("expected wrapped simulatedErr, got %v", err)
		}
	})
}

func TestFidelityExtractor_AccountingAndUnicodeParsing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "accounting.csv",
		FileHash:   "hash_accounting",
		AccountID:  "FID-ACCT",
		IngestedAt: now,
	}

	tests := []accountingTestCase{
		{
			name:          "accounting negative parentheses ($50.00)",
			qtyStr:        "10",
			priceStr:      "5.00",
			valStr:        "($50.00)",
			costBasisStr:  "($60.00)",
			expectedQty:   10.0,
			expectedPrice: 5.0,
			expectedVal:   -50.0,
			expectedBasis: -60.0,
		},
		{
			name:          "single dash and double dash zero defaults",
			qtyStr:        "-",
			priceStr:      "--",
			valStr:        "-",
			costBasisStr:  "--",
			expectedQty:   0.0,
			expectedPrice: 0.0,
			expectedVal:   0.0,
			expectedBasis: 0.0,
		},
		{
			name:          "N/A case-insensitive zero defaults",
			qtyStr:        "N/A",
			priceStr:      "n/a",
			valStr:        "N/a",
			costBasisStr:  "n/A",
			expectedQty:   0.0,
			expectedPrice: 0.0,
			expectedVal:   0.0,
			expectedBasis: 0.0,
		},
		{
			name:          "non-breaking space \u00a0 and narrow space \u202f",
			qtyStr:        "\u00a0100.5\u00a0",
			priceStr:      "$\u00a025.00\u00a0",
			valStr:        "$\u202f2,512.50\u202f",
			costBasisStr:  "$\u00a02,000.00\u00a0",
			expectedQty:   100.5,
			expectedPrice: 25.0,
			expectedVal:   2512.50,
			expectedBasis: 2000.0,
		},
		{
			name:          "commas and plus signs",
			qtyStr:        "+1,000.00",
			priceStr:      "+$50.00",
			valStr:        "+$50,000.00",
			costBasisStr:  "$45,000.00",
			expectedQty:   1000.0,
			expectedPrice: 50.0,
			expectedVal:   50000.0,
			expectedBasis: 45000.0,
		},
		{
			name:      "invalid numeric string triggers error with field name and raw value",
			qtyStr:    "bad_quantity",
			priceStr:  "50.00",
			valStr:    "100.00",
			expectErr: true,
		},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAccountingTestCase(t, extractor, ctx, meta, tc)
		})
	}
}

type accountingTestCase struct {
	name          string
	qtyStr        string
	priceStr      string
	valStr        string
	costBasisStr  string
	expectedQty   float64
	expectedPrice float64
	expectedVal   float64
	expectedBasis float64
	expectErr     bool
}

func runAccountingTestCase(
	t *testing.T,
	extractor port.Extractor,
	ctx context.Context,
	meta model.FileMetadata,
	tc accountingTestCase,
) {
	t.Helper()
	quoteCSV := func(s string) string {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}

	csvData := withFooter(fmt.Sprintf(
		"Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total\nFID-ACCT,Individual,XYZ,XYZ HOLDINGS,%s,%s,%s,%s\n",
		quoteCSV(tc.qtyStr), quoteCSV(tc.priceStr), quoteCSV(tc.valStr), quoteCSV(tc.costBasisStr),
	))

	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if tc.expectErr {
		if err == nil {
			t.Fatalf("expected error for %s, got nil", tc.name)
		}
		if !strings.Contains(err.Error(), "quantity") || !strings.Contains(err.Error(), "bad_quantity") {
			t.Errorf("expected error to include field name and raw value, got: %v", err)
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}

	assertPositionMetrics(t, positions[0], tc.expectedQty, tc.expectedPrice, tc.expectedVal, tc.expectedBasis)
}

func assertPositionMetrics(t *testing.T, p model.FidelityPosition, qty, price, val, basis float64) {
	t.Helper()
	if p.Quantity != qty {
		t.Errorf("Quantity: expected %f, got %f", qty, p.Quantity)
	}
	if p.LastPrice != price {
		t.Errorf("LastPrice: expected %f, got %f", price, p.LastPrice)
	}
	if p.CurrentValue != val {
		t.Errorf("CurrentValue: expected %f, got %f", val, p.CurrentValue)
	}
	if p.CostBasis != basis {
		t.Errorf("CostBasis: expected %f, got %f", basis, p.CostBasis)
	}
}

func TestFidelityExtractor_TrailingDisclaimers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "disclaimers.csv",
		FileHash:   "hash_disc",
		AccountID:  "FID-DISC",
		IngestedAt: now,
	}

	csvData := `Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
FID-DISC,Individual,GOOGL,ALPHABET INC,8,175.00,$1400.00,$1200.00
Please note that values are based on market close.,,,,,,,
1. Data is provided for your information only.,,,,,,,
*Real-time quotes are provided by NASDAQ.,,,,,,,
Important Information Regarding Your Portfolio,,,,,,,
Quotes are delayed by at least 15 minutes.,,,,,,,
Security prices are subject to change without notice.,,,,,,,
Total Market Value,,,,,,$1400.00,
Account Total,,,,,,$1400.00,
`
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(withFooter(csvData)), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error with trailing disclaimers: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast payload: %v", err)
	}

	if len(positions) != 1 {
		t.Fatalf("expected exactly 1 position, got %d", len(positions))
	}
	if positions[0].Symbol != "GOOGL" {
		t.Errorf("expected symbol 'GOOGL', got %q", positions[0].Symbol)
	}
}

func TestFidelityExtractor_SingleAccountInference(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	extractor := fidelity.NewExtractor()

	t.Run("all rows share same account -> infer batch AccountID", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "single_account.csv",
			FileHash:   "hash_single",
			AccountID:  "SYNTH-DEFAULT",
			IngestedAt: now,
		}

		csvData := withFooter(`Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
FID-EXPLICIT-999,Brokerage,NVDA,NVIDIA CORP,10,130.00,$1300.00,$1000.00
FID-EXPLICIT-999,Brokerage,AMZN,AMAZON.COM,5,180.00,$900.00,$800.00
`)
		batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		if err != nil {
			t.Fatalf("unexpected Extract error: %v", err)
		}

		if batch.AccountID != "FID-EXPLICIT-999" {
			t.Errorf("expected batch AccountID to resolve to 'FID-EXPLICIT-999', got %q", batch.AccountID)
		}
	})

	t.Run("rows have multiple accounts -> keep meta AccountID", func(t *testing.T) {
		meta := model.FileMetadata{
			FileName:   "multi_account.csv",
			FileHash:   "hash_multi",
			AccountID:  "META-DEFAULT",
			IngestedAt: now,
		}

		csvData := withFooter(`Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total
ACC-111,Brokerage,NVDA,NVIDIA CORP,10,130.00,$1300.00,$1000.00
ACC-222,Brokerage,AMZN,AMAZON.COM,5,180.00,$900.00,$800.00
`)
		batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		if err != nil {
			t.Fatalf("unexpected Extract error: %v", err)
		}

		if batch.AccountID != "META-DEFAULT" {
			t.Errorf("expected batch AccountID to remain 'META-DEFAULT', got %q", batch.AccountID)
		}
	})
}

func TestFidelityExtractor_ContextCancellation_MidStream(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "cancel_midstream.csv",
		FileHash:   "hash_cancel",
		AccountID:  "FID-CANCEL",
		IngestedAt: now,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	header := "Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total\n"
	row1 := "FID-CANCEL,Individual,AAPL,APPLE INC,10,150.00,$1500.00,$1200.00\n"
	row2 := "FID-CANCEL,Individual,MSFT,MICROSOFT,10,400.00,$4000.00,$3500.00\n"

	cr := &midStreamCancelReader{
		chunks:     [][]byte{[]byte(header), []byte(row1), []byte(row2)},
		cancelFunc: cancel,
	}

	extractor := fidelity.NewExtractor()
	_, err := extractor.Extract(ctx, cr, meta)
	if err == nil {
		t.Fatal("expected error due to cancelled context mid-stream, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestFidelityExtractor_SentinelErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "sentinel.csv",
		AccountID:  "FID-1",
		IngestedAt: now,
	}
	extractor := fidelity.NewExtractor()

	t.Run("ErrNilReader verification", func(t *testing.T) {
		_, err := extractor.Extract(ctx, nil, meta)
		if !errors.Is(err, fidelity.ErrNilReader) {
			t.Errorf("expected errors.Is(err, fidelity.ErrNilReader) == true, got %v", err)
		}
	})

	t.Run("ErrMissingRequiredColumns verification", func(t *testing.T) {
		invalidCSV := "ColA,ColB\n1,2\n"
		_, err := extractor.Extract(ctx, strings.NewReader(invalidCSV), meta)
		if !errors.Is(err, fidelity.ErrMissingRequiredColumns) {
			t.Errorf("expected errors.Is(err, fidelity.ErrMissingRequiredColumns) == true, got %v", err)
		}
	})

	t.Run("ErrMissingAsOfDate verification", func(t *testing.T) {
		missingFooterCSV := "Account number,Symbol,Current value\nFID-1,AAPL,$1500.00\n"
		_, err := extractor.Extract(ctx, strings.NewReader(missingFooterCSV), meta)
		if !errors.Is(err, model.ErrMissingAsOfDate) {
			t.Errorf("expected errors.Is(err, model.ErrMissingAsOfDate) == true, got %v", err)
		}
	})

	t.Run("ErrInvalidDateDownloaded verification", func(t *testing.T) {
		invalidFooterCSV := "Account number,Symbol,Current value\nFID-1,AAPL,$1500.00\n\"Date downloaded MALFORMED\"\n"
		_, err := extractor.Extract(ctx, strings.NewReader(invalidFooterCSV), meta)
		if !errors.Is(err, fidelity.ErrInvalidDateDownloaded) {
			t.Errorf("expected errors.Is(err, fidelity.ErrInvalidDateDownloaded) == true, got %v", err)
		}
	})
}

func TestFidelityExtractor_NonFiniteFloats(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "non_finite.csv",
		FileHash:   "hash_non_finite",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	tests := []struct {
		name     string
		quantity string
		value    string
	}{
		{name: "NaN quantity", quantity: "NaN", value: "100.00"},
		{name: "nan lowercase quantity", quantity: "nan", value: "100.00"},
		{name: "+Inf quantity", quantity: "+Inf", value: "100.00"},
		{name: "-Inf quantity", quantity: "-Inf", value: "100.00"},
		{name: "Infinity quantity", quantity: "Infinity", value: "100.00"},
		{name: "NaN value", quantity: "10", value: "NaN"},
		{name: "-Inf value", quantity: "10", value: "-Inf"},
		{name: "+Infinity value", quantity: "10", value: "+Infinity"},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvData := fmt.Sprintf(
				"Account number,Symbol,Quantity,Current value\nFID-1,AAPL,%s,%s\n",
				tc.quantity, tc.value,
			)
			_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), "non-finite float") {
				t.Errorf("expected error to mention 'non-finite float', got: %v", err)
			}
		})
	}
}

func TestFidelityExtractor_StrictValuationColumnRequirements(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "no_valuation.csv",
		FileHash:   "hash_no_val",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	tests := []struct {
		name   string
		header string
	}{
		{name: "symbol and quantity only", header: "Account number,Symbol,Quantity\nFID-1,AAPL,10\n"},
		{name: "symbol and last price only", header: "Account number,Symbol,Last price\nFID-1,AAPL,150.00\n"},
		{name: "symbol and description only", header: "Account number,Symbol,Description\nFID-1,AAPL,Apple Inc\n"},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractor.Extract(ctx, strings.NewReader(tc.header), meta)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !errors.Is(err, fidelity.ErrMissingRequiredColumns) {
				t.Errorf("expected ErrMissingRequiredColumns, got: %v", err)
			}
		})
	}
}

func TestFidelityExtractor_AccountIDPlaceholders(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "placeholders.csv",
		FileHash:   "hash_placeholders",
		AccountID:  "FALLBACK-ACC-99",
		IngestedAt: now,
	}

	tests := []struct {
		name       string
		rawAccount string
	}{
		{name: "empty account id", rawAccount: ""},
		{name: "single dash", rawAccount: "-"},
		{name: "double dash", rawAccount: "--"},
		{name: "N/A uppercase", rawAccount: "N/A"},
		{name: "n/a lowercase", rawAccount: "n/a"},
		{name: "none lowercase", rawAccount: "none"},
		{name: "NONE uppercase", rawAccount: "NONE"},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvData := withFooter(fmt.Sprintf(
				"Account number,Symbol,Current value\n%s,AAPL,1500.00\n",
				tc.rawAccount,
			))
			batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("unexpected Extract error for %s: %v", tc.name, err)
			}
			positions, err := batch.AsFidelityPositions()
			if err != nil {
				t.Fatalf("failed to cast positions: %v", err)
			}
			if len(positions) != 1 {
				t.Fatalf("expected 1 position, got %d", len(positions))
			}
			if positions[0].AccountID != "FALLBACK-ACC-99" {
				t.Errorf("expected fallback AccountID 'FALLBACK-ACC-99', got %q", positions[0].AccountID)
			}
		})
	}
}

func TestFidelityExtractor_DirectCashTickers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "direct_cash.csv",
		FileHash:   "hash_direct_cash",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	tests := []struct {
		name        string
		symbol      string
		description string
	}{
		{name: "FCASH symbol", symbol: "FCASH", description: "HELD IN FCASH"},
		{name: "CORE symbol", symbol: "CORE", description: "CORE MONEY MARKET"},
		{name: "fcash lowercase", symbol: "fcash", description: "FIDELITY CASH SWEEP"},
		{name: "core lowercase", symbol: "core", description: "CORE POSITION"},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvData := withFooter(fmt.Sprintf(
				"Account number,Symbol,Description,Current value\nFID-1,%s,%s,1000.00\n",
				tc.symbol, tc.description,
			))
			batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err != nil {
				t.Fatalf("unexpected Extract error for %s: %v", tc.name, err)
			}
			positions, err := batch.AsFidelityPositions()
			if err != nil {
				t.Fatalf("failed to cast positions: %v", err)
			}
			if len(positions) != 1 {
				t.Fatalf("expected 1 position, got %d", len(positions))
			}
			if positions[0].Symbol != "CASH" {
				t.Errorf("expected normalized symbol 'CASH', got %q", positions[0].Symbol)
			}
		})
	}
}

func TestFidelityExtractor_HeaderOnlyCSV(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "header_only.csv",
		FileHash:   "hash_header_only",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	csvData := "Account number,Symbol,Current value\n"
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error for header-only CSV: %v", err)
	}
	if batch.RecordCount != 0 {
		t.Errorf("expected RecordCount == 0, got %d", batch.RecordCount)
	}
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	if len(positions) != 0 {
		t.Errorf("expected len(positions) == 0, got %d", len(positions))
	}
}

func TestFidelityExtractor_MalformedCSVSyntax(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "malformed.csv",
		FileHash:   "hash_malformed",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	// CSV with unclosed quote in data row
	csvData := "Account number,Symbol,Current value\nFID-1,\"UNCLOSED QUOTE,150.00\n"
	extractor := fidelity.NewExtractor()
	_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err == nil {
		t.Fatal("expected error on malformed CSV syntax with unclosed quote, got nil")
	}
}

func TestFidelityExtractor_LeadingBlankLines(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "leading_blanks.csv",
		FileHash:   "hash_blanks",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	csvData := withFooter("\n   \n\n\t\nAccount number,Symbol,Current value\nFID-1,AAPL,1500.00\n")
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error on leading blank lines: %v", err)
	}
	if batch.RecordCount != 1 {
		t.Fatalf("expected RecordCount == 1, got %d", batch.RecordCount)
	}
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	if len(positions) != 1 || positions[0].Symbol != "AAPL" {
		t.Errorf("expected position AAPL, got %+v", positions)
	}
}

func TestParseDateDownloaded_LegacyParity(t *testing.T) {
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "date_parity.csv",
		FileHash:   "hash_date_parity",
		AccountID:  "FID-DATE",
		IngestedAt: now,
	}

	tests := []struct {
		name        string
		footerRow   string
		expectedUTC time.Time
	}{
		{
			name:        "summer EDT (-04:00) rolls into next UTC day",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 p.m ET"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "winter EST (-05:00) rolls into next UTC day",
			footerRow:   `"Date downloaded Jan-15-2026 9:50 p.m ET"`,
			expectedUTC: time.Date(2026, time.January, 16, 2, 50, 0, 0, time.UTC),
		},
		{
			name:        "single-digit day with a.m. and trailing period",
			footerRow:   `"Date downloaded Sep-5-2026 9:50 a.m. ET"`,
			expectedUTC: time.Date(2026, time.September, 5, 13, 50, 0, 0, time.UTC),
		},
		{
			name:        "p.m. with trailing period",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 p.m. ET"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "unquoted footer line with a.m without period",
			footerRow:   `Date downloaded Jan-05-2026 8:15 a.m ET`,
			expectedUTC: time.Date(2026, time.January, 5, 13, 15, 0, 0, time.UTC),
		},
		{
			name:        "no ET suffix defaults to UTC",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 p.m"`,
			expectedUTC: time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC),
		},
		{
			name:        "12:00 a.m. midnight boundary in EDT",
			footerRow:   `"Date downloaded Sep-25-2026 12:00 a.m. ET"`,
			expectedUTC: time.Date(2026, time.September, 25, 4, 0, 0, 0, time.UTC),
		},
		{
			name:        "12:00 p.m. noon boundary in EDT",
			footerRow:   `"Date downloaded Sep-25-2026 12:00 p.m. ET"`,
			expectedUTC: time.Date(2026, time.September, 25, 16, 0, 0, 0, time.UTC),
		},
		{
			name:        "uppercase PM without periods",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 PM ET"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "uppercase AM without periods",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 AM ET"`,
			expectedUTC: time.Date(2026, time.September, 25, 13, 50, 0, 0, time.UTC),
		},
		{
			name:        "uppercase dotted P.M. and A.M.",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 P.M. ET"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "uppercase single-dot A.M without trailing period",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 A.M ET"`,
			expectedUTC: time.Date(2026, time.September, 25, 13, 50, 0, 0, time.UTC),
		},
		{
			name:        "lowercase pm and am without periods",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 pm ET"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "explicit EDT suffix",
			footerRow:   `"Date downloaded Sep-25-2026 9:50 p.m. EDT"`,
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "explicit EST suffix",
			footerRow:   `"Date downloaded Jan-15-2026 9:50 p.m. EST"`,
			expectedUTC: time.Date(2026, time.January, 16, 2, 50, 0, 0, time.UTC),
		},
		{
			name:        "case-varied prefix with tabs and multiple internal spaces",
			footerRow:   "\"Date Downloaded\tSep-25-2026   9:50  p.m.  ET\"",
			expectedUTC: time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC),
		},
		{
			name:        "valid leap day Feb-29-2024",
			footerRow:   `"Date downloaded Feb-29-2024 9:50 p.m. ET"`,
			expectedUTC: time.Date(2024, time.March, 1, 2, 50, 0, 0, time.UTC),
		},
		{
			name:        "US DST spring-forward day before transition (1:15 a.m. EST -05:00)",
			footerRow:   `"Date downloaded Mar-08-2026 1:15 a.m. ET"`,
			expectedUTC: time.Date(2026, time.March, 8, 6, 15, 0, 0, time.UTC),
		},
		{
			name:        "US DST spring-forward day after transition (3:15 a.m. EDT -04:00)",
			footerRow:   `"Date downloaded Mar-08-2026 3:15 a.m. ET"`,
			expectedUTC: time.Date(2026, time.March, 8, 7, 15, 0, 0, time.UTC),
		},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertExtractedFooterDate(t, extractor, meta, tc.footerRow, tc.expectedUTC)
		})
	}
}

func assertExtractedFooterDate(
	t *testing.T,
	extractor port.Extractor,
	meta model.FileMetadata,
	footerRow string,
	expectedUTC time.Time,
) {
	t.Helper()
	csvData := "Account number,Symbol,Current value\nFID-DATE,AAPL,$1500.00\n" + footerRow + "\n"
	batch, err := extractor.Extract(context.Background(), strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
	got := positions[0].AsOfDate
	if !got.Equal(expectedUTC) {
		t.Errorf("expected AsOfDate %v, got %v", expectedUTC, got)
	}
	if got.Location() != time.UTC {
		t.Errorf("expected AsOfDate location UTC, got %v", got.Location())
	}
}

func TestParseDateDownloaded_Invalid(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "date_invalid.csv",
		FileHash:   "hash_date_invalid",
		AccountID:  "FID-DATE",
		IngestedAt: now,
	}

	invalidFooters := []struct {
		name      string
		footerRow string
		wantInErr string
	}{
		{
			name:      "non-date text",
			footerRow: `"Date downloaded not-a-date"`,
			wantInErr: "not-a-date",
		},
		{
			name:      "ISO-8601 format instead of Fidelity format",
			footerRow: `"Date downloaded 2026-09-25 21:50 ET"`,
			wantInErr: "2026-09-25 21:50",
		},
		{
			name:      "out-of-range day 32",
			footerRow: `"Date downloaded Sep-32-2026 9:50 p.m ET"`,
			wantInErr: "Sep-32-2026",
		},
		{
			name:      "invalid Feb 29 on non-leap year 2026",
			footerRow: `"Date downloaded Feb-29-2026 9:50 p.m. ET"`,
			wantInErr: "Feb-29-2026",
		},
		{
			name:      "single-digit day with invalid meridiem wraps specific errSingle",
			footerRow: `"Date downloaded Sep-5-2026 9:50 BAD ET"`,
			wantInErr: "BAD",
		},
		{
			name:      "truncated footer with trailing space only",
			footerRow: `"Date downloaded "`,
			wantInErr: "empty or missing date",
		},
		{
			name:      "bare Date downloaded keyword with no date",
			footerRow: `"Date downloaded"`,
			wantInErr: "empty or missing date",
		},
		{
			name:      "only timezone suffix after prefix",
			footerRow: `"Date downloaded ET"`,
			wantInErr: "empty date",
		},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range invalidFooters {
		t.Run(tc.name, func(t *testing.T) {
			csvData := "Account number,Symbol,Current value\nFID-DATE,AAPL,$1500.00\n" + tc.footerRow + "\n"
			_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
			if err == nil {
				t.Fatalf("expected error for malformed footer %s, got nil", tc.footerRow)
			}
			if !errors.Is(err, fidelity.ErrInvalidDateDownloaded) {
				t.Errorf("expected errors.Is(err, fidelity.ErrInvalidDateDownloaded) for %s, got: %v", tc.name, err)
			}
			if tc.wantInErr != "" && !strings.Contains(err.Error(), tc.wantInErr) {
				t.Errorf("expected error to contain %q, got: %v", tc.wantInErr, err)
			}
		})
	}
}

func TestFidelityExtractor_AsOfDate_StampedOnAllPositions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "Portfolio_Positions_Synthetic.csv",
		FileHash:   "hash_fid_synthetic",
		AccountID:  "SYNTH-DEFAULT",
		IngestedAt: now,
	}

	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(mockPortfolioPositionsFixture), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	if len(positions) != 5 {
		t.Fatalf("expected 5 positions, got %d", len(positions))
	}

	expectedAsOf := time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC)
	for i, pos := range positions {
		if pos.AsOfDate.IsZero() {
			t.Errorf("position %d (%s) has zero AsOfDate", i, pos.Symbol)
		}
		if !pos.AsOfDate.Equal(expectedAsOf) {
			t.Errorf("position %d (%s): expected AsOfDate %v, got %v", i, pos.Symbol, expectedAsOf, pos.AsOfDate)
		}
		if pos.AsOfDate.Location() != time.UTC {
			t.Errorf("position %d (%s): expected UTC location, got %v", i, pos.Symbol, pos.AsOfDate.Location())
		}
	}
}

func TestFidelityExtractor_AsOfDate_MissingFooter(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "missing_footer.csv",
		FileHash:   "hash_missing_footer",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	csvData := "Account number,Symbol,Current value\nFID-1,AAPL,$1500.00\n"
	extractor := fidelity.NewExtractor()
	_, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err == nil {
		t.Fatal("expected error when positions exist without Date downloaded footer, got nil")
	}
	if !errors.Is(err, model.ErrMissingAsOfDate) {
		t.Errorf("expected errors.Is(err, model.ErrMissingAsOfDate), got: %v", err)
	}
}

func TestFidelityExtractor_AsOfDate_HeaderOnlyNoFooter(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "header_only_no_footer.csv",
		FileHash:   "hash_header_only_no_footer",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	tests := []struct {
		name        string
		csvData     string
		expectedErr error
	}{
		{
			name:        "header only with 0 subsequent rows and no footer",
			csvData:     "Account number,Symbol,Current value\n",
			expectedErr: nil,
		},
		{
			name:        "single disclaimer row with no footer",
			csvData:     "Account number,Symbol,Current value\n\"The data and information is proprietary\",,\n",
			expectedErr: nil,
		},
		{
			name: "multiple disclaimers, Account Total summary row, blank rows, 0 positions, no footer",
			csvData: "Account number,Symbol,Current value\n" +
				"\n" +
				"Account Total,,$0.00\n" +
				"\"The data and information is proprietary\",,\n" +
				"\"Brokerage services are provided by Fidelity\",,\n" +
				"\"Please note quotes are delayed\",,\n",
			expectedErr: nil,
		},
		{
			name: "0 positions with valid Date downloaded footer succeeds",
			csvData: "Account number,Symbol,Current value\n" +
				"\"The data and information is proprietary\",,\n" +
				"\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n",
			expectedErr: nil,
		},
		{
			name: "0 positions with malformed Date downloaded footer fails",
			csvData: "Account number,Symbol,Current value\n" +
				"\"The data and information is proprietary\",,\n" +
				"\"Date downloaded MALFORMED\"\n",
			expectedErr: fidelity.ErrInvalidDateDownloaded,
		},
		{
			name: "0 positions with truncated Date downloaded footer fails",
			csvData: "Account number,Symbol,Current value\n" +
				"\"Date downloaded \"\n",
			expectedErr: fidelity.ErrInvalidDateDownloaded,
		},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runZeroPositionFooterCase(t, extractor, ctx, meta, tc.csvData, tc.expectedErr)
		})
	}
}

func runZeroPositionFooterCase(
	t *testing.T,
	extractor port.Extractor,
	ctx context.Context,
	meta model.FileMetadata,
	csvData string,
	expectedErr error,
) {
	t.Helper()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if expectedErr != nil {
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected errors.Is(err, %v), got: %v", expectedErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("expected zero-position CSV to succeed, got: %v", err)
	}
	if batch.RecordCount != 0 {
		t.Errorf("expected RecordCount == 0, got %d", batch.RecordCount)
	}
}

func TestFidelityExtractor_AsOfDate_UnparseableFooter(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "unparseable_footer.csv",
		FileHash:   "hash_unparseable_footer",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	csvData := "Account number,Symbol,Current value\nFID-1,AAPL,$1500.00\n\"Date downloaded MALFORMED_DATE_STRING\"\n"
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err == nil {
		t.Fatal("expected error for unparseable footer, got nil")
	}
	if batch != nil {
		t.Errorf("expected nil batch on footer parse error, got %+v", batch)
	}
	if !errors.Is(err, fidelity.ErrInvalidDateDownloaded) {
		t.Errorf("expected errors.Is(err, fidelity.ErrInvalidDateDownloaded) == true, got: %v", err)
	}
	if errors.Is(err, model.ErrMissingAsOfDate) {
		t.Errorf("expected errors.Is(err, model.ErrMissingAsOfDate) == false, got: %v", err)
	}
	if !strings.Contains(err.Error(), "row 3 date downloaded footer") {
		t.Errorf("expected error to contain 1-based row context 'row 3 date downloaded footer', got: %v", err)
	}
}

func TestFidelityExtractor_AsOfDate_FooterNotAPosition(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "footer_not_position.csv",
		FileHash:   "hash_footer_not_pos",
		AccountID:  "FID-1",
		IngestedAt: now,
	}

	csvData := `Account number,Symbol,Current value
FID-1,AAPL,$1500.00
FID-1,MSFT,$2000.00
"Date downloaded Sep-25-2026 9:50 p.m ET"
"Date downloaded Jan-15-2026 9:50 p.m ET"
`
	extractor := fidelity.NewExtractor()
	batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}
	if batch.RecordCount != 2 {
		t.Fatalf("expected RecordCount == 2 (footers excluded), got %d", batch.RecordCount)
	}

	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	expectedFirstFooter := time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC)
	for i, pos := range positions {
		if !pos.AsOfDate.Equal(expectedFirstFooter) {
			t.Errorf("position %d: expected first footer date %v, got %v", i, expectedFirstFooter, pos.AsOfDate)
		}
	}
}

type structuralOrderingCase struct {
	name        string
	csvData     string
	wantCount   int
	wantAsOfUTC time.Time
	wantErr     error
}

func TestFidelityExtractor_AsOfDate_StructuralOrderingAndColumnPlacement(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "structural_ordering.csv",
		FileHash:   "hash_structural_ordering",
		AccountID:  "FID-STRUCT",
		IngestedAt: now,
	}
	expectedSep26 := time.Date(2026, time.September, 26, 1, 50, 0, 0, time.UTC)

	tests := []structuralOrderingCase{
		{
			name: "footer above position rows",
			csvData: `Account number,Symbol,Current value
"Date downloaded Sep-25-2026 9:50 p.m ET"
FID-1,AAPL,$1500.00
FID-1,MSFT,$2000.00
`,
			wantCount:   2,
			wantAsOfUTC: expectedSep26,
		},
		{
			name: "multi-account export with footer interleaved between accounts and followed by totals and disclaimers",
			csvData: `Account number,Symbol,Description,Current value
FID-ROTH,AAPL,APPLE INC,$1500.00
"Date downloaded Sep-25-2026 9:50 p.m ET"
FID-401K,FXAIX,FIDELITY 500 INDEX,$4500.00
Account Total,,,$6000.00
"Brokerage services are provided by Fidelity Brokerage Services LLC."
"*"
`,
			wantCount:   2,
			wantAsOfUTC: expectedSep26,
		},
		{
			name: "footer in non-zero column index preceded by empty CSV cells",
			csvData: `Account number,Symbol,Description,Current value
FID-1,AAPL,APPLE INC,$1500.00
,,"Date downloaded Sep-25-2026 9:50 p.m ET",
`,
			wantCount:   1,
			wantAsOfUTC: expectedSep26,
		},
		{
			name: "unquoted comma-split footer across multiple CSV columns",
			csvData: `Account number,Symbol,Description,Current value
FID-1,AAPL,APPLE INC,$1500.00
Date downloaded, Sep-25-2026, 9:50 p.m ET,
`,
			wantCount:   1,
			wantAsOfUTC: expectedSep26,
		},
		{
			name: "valid first footer followed by malformed second footer succeeds (first footer wins)",
			csvData: `Account number,Symbol,Current value
FID-1,AAPL,$1500.00
"Date downloaded Sep-25-2026 9:50 p.m ET"
"Date downloaded CORRUPT_SECOND_FOOTER"
`,
			wantCount:   1,
			wantAsOfUTC: expectedSep26,
		},
		{
			name: "malformed first footer followed by valid second footer fails with ErrInvalidDateDownloaded",
			csvData: `Account number,Symbol,Current value
FID-1,AAPL,$1500.00
"Date downloaded CORRUPT_FIRST_FOOTER"
"Date downloaded Sep-25-2026 9:50 p.m ET"
`,
			wantErr: fidelity.ErrInvalidDateDownloaded,
		},
	}

	extractor := fidelity.NewExtractor()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runStructuralOrderingCase(t, extractor, ctx, meta, tc)
		})
	}
}

func runStructuralOrderingCase(
	t *testing.T,
	extractor port.Extractor,
	ctx context.Context,
	meta model.FileMetadata,
	tc structuralOrderingCase,
) {
	t.Helper()
	batch, err := extractor.Extract(ctx, strings.NewReader(tc.csvData), meta)
	if tc.wantErr != nil {
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("expected errors.Is(err, %v), got: %v", tc.wantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected Extract error: %v", err)
	}
	if batch.RecordCount != tc.wantCount {
		t.Fatalf("expected RecordCount == %d, got %d", tc.wantCount, batch.RecordCount)
	}
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast positions: %v", err)
	}
	for i, pos := range positions {
		if !pos.AsOfDate.Equal(tc.wantAsOfUTC) {
			t.Errorf("position %d (%s): expected AsOfDate %v, got %v", i, pos.Symbol, tc.wantAsOfUTC, pos.AsOfDate)
		}
	}
}

type testErrorReader struct {
	err error
}

func (r *testErrorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

type midStreamCancelReader struct {
	chunks     [][]byte
	index      int
	cancelFunc context.CancelFunc
}

func (r *midStreamCancelReader) Read(p []byte) (int, error) {
	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}
	if r.index == 1 && r.cancelFunc != nil {
		r.cancelFunc()
	}
	chunk := r.chunks[r.index]
	r.index++
	return copy(p, chunk), nil
}

func generateSyntheticFidelityCSV(rows int) string {
	var sb strings.Builder
	sb.WriteString(
		"Account number,Account name,Symbol,Description,Quantity,Last price,Current value,Cost basis total\n",
	)
	for i := range rows {
		fmt.Fprintf(
			&sb,
			"FID-BENCH-%d,INDIVIDUAL,SYM%d,SYNTHETIC HOLDING %d,%d.25,$150.00,$%d.00,$%d.00\n",
			(i%5)+1,
			i,
			i,
			(i%100)+1,
			((i%100)+1)*150,
			((i%100)+1)*120,
		)
	}
	sb.WriteString(
		"\"The data and information in this spreadsheet is provided to you solely for your use.\"\n",
	)
	sb.WriteString("\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n")
	return sb.String()
}

func BenchmarkFidelityExtractor_Extract_Fixture(b *testing.B) {
	extractor := fidelity.NewExtractor()
	ctx := context.Background()
	meta := model.FileMetadata{
		FileName:   "Portfolio_Positions_Synthetic.csv",
		FileHash:   "hash_fid_bench_fixture",
		AccountID:  "SYNTH-DEFAULT",
		IngestedAt: time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
	}

	b.SetBytes(int64(len(mockPortfolioPositionsFixture)))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		batch, err := extractor.Extract(ctx, strings.NewReader(mockPortfolioPositionsFixture), meta)
		if err != nil || batch == nil || batch.RecordCount != 5 {
			b.Fatalf("unexpected extract error in fixture benchmark: %v", err)
		}
	}
}

func BenchmarkFidelityExtractor_Extract_HighVolume_500Rows(b *testing.B) {
	extractor := fidelity.NewExtractor()
	ctx := context.Background()
	meta := model.FileMetadata{
		FileName:   "Portfolio_Positions_500.csv",
		FileHash:   "hash_fid_bench_500",
		AccountID:  "FID-BENCH-DEFAULT",
		IngestedAt: time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
	}
	csvData := generateSyntheticFidelityCSV(500)

	b.SetBytes(int64(len(csvData)))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		batch, err := extractor.Extract(ctx, strings.NewReader(csvData), meta)
		if err != nil || batch == nil || batch.RecordCount != 500 {
			b.Fatalf("unexpected extract error in high volume benchmark: %v", err)
		}
	}
}
