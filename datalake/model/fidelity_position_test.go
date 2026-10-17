package model_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake/model"
)

// createSyntheticFidelityCSV generates a self-contained temporary Fidelity CSV portfolio export file.
func createSyntheticFidelityCSV(t *testing.T) string {
	t.Helper()
	content := `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,SPAXX**,HELD IN MONEY MARKET,,,,$3250.00,,,,,25.00%,,,Cash,
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash,
SYNTH-ROTH-101,ROTH IRA,FBGRX,FIDELITY BLUE CHIP GROWTH,10.000,$296.25,+$2.10,$2962.50,+$21.00,+0.71%,+$462.50,+18.50%,22.79%,$2500.00,$250.00,Cash,
SYNTH-401K-202,TRADITIONAL 401K,FXAIX,FIDELITY 500 INDEX,25.500,$180.20,-$0.80,$4595.10,-$20.40,-0.44%,+$595.10,+14.88%,100.00%,$4000.00,$156.86,Cash,
SYNTH-TAX-303,INDIVIDUAL TOD,**,,--,--,--,$0.00,--,,--,--,--,--,--,,

"The data and information in this spreadsheet is provided to you solely for your use and is not for distribution. The spreadsheet is provided for informational purposes only, and is not intended to provide advice, nor should it be construed as an offer to sell, a solicitation of an offer to buy or a recommendation for any security by Fidelity or any third party. Data and information shown is based on information known to Fidelity as of the date it was exported and is subject to change. It should not be used in place of your account statements or trade confirmations and is not intended for tax reporting purposes. For more information on the data included in this spreadsheet, including any limitations thereof, go to Fidelity.com."

"Brokerage services are provided by Fidelity Brokerage Services LLC (FBS), 900 Salem Street, Smithfield, RI 02917. Custody and other services provided by National Financial Services LLC (NFS). Both are Fidelity Investment companies and members SIPC, NYSE. Neither FBS nor NFS offer crypto as a direct investment nor provide trading or custody services for such assets."

"Date downloaded Sep-25-2026 9:50 p.m ET"
`
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Portfolio_Positions_Synthetic.csv")
	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write synthetic CSV file: %v", err)
	}
	return filePath
}

func TestMapRawRecordToFidelityPosition_SyntheticFixture(t *testing.T) {
	ctx := context.Background()
	parser := csvparser.NewDefaultParser()
	fixturePath := createSyntheticFidelityCSV(t)

	docs, count, err := parser.Parse(ctx, fixturePath, "fidelity", "portfolio")
	if err != nil {
		t.Fatalf("failed to parse synthetic fixture: %v", err)
	}

	if count != 5 {
		t.Fatalf("expected 5 data records from fixture, got %d", count)
	}

	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	positions := make([]model.FidelityPosition, len(docs))
	for i, doc := range docs {
		lineage := model.SourceLineage{
			FileName:  "Portfolio_Positions_Synthetic.csv",
			RowNumber: int64(i + 2), // Header is row 1
		}
		pos, mapErr := model.MapRawRecordToFidelityPosition(doc, asOfDate, lineage)
		if mapErr != nil {
			t.Fatalf("failed to map doc %d (%+v): %v", i, doc, mapErr)
		}
		positions[i] = pos
	}

	t.Run("assert cash position", func(t *testing.T) {
		assertCashPosition(t, positions[0])
	})

	t.Run("assert stock position", func(t *testing.T) {
		assertStockPosition(t, positions[1])
	})

	t.Run("assert zero position", func(t *testing.T) {
		assertZeroPosition(t, positions[4])
	})
}

func assertCashPosition(t *testing.T, spaxx model.FidelityPosition) {
	t.Helper()
	if spaxx.AccountNumber != "SYNTH-ROTH-101" {
		t.Errorf("expected accountNumber SYNTH-ROTH-101, got %s", spaxx.AccountNumber)
	}
	if spaxx.Symbol != "SPAXX**" {
		t.Errorf("expected symbol SPAXX**, got %s", spaxx.Symbol)
	}
	if spaxx.CurrentValue != 3250.00 {
		t.Errorf("expected currentValue 3250.00, got %f", spaxx.CurrentValue)
	}
	if spaxx.PercentOfAccount != 25.00 {
		t.Errorf("expected percentOfAccount 25.00, got %f", spaxx.PercentOfAccount)
	}
	if spaxx.Type != "Cash" {
		t.Errorf("expected type Cash, got %s", spaxx.Type)
	}
}

func assertStockPosition(t *testing.T, fskax model.FidelityPosition) {
	t.Helper()
	if fskax.Symbol != "FSKAX" {
		t.Errorf("expected symbol FSKAX, got %s", fskax.Symbol)
	}
	if fskax.Quantity != 45.250 {
		t.Errorf("expected quantity 45.250, got %f", fskax.Quantity)
	}
	if fskax.LastPrice != 150.00 {
		t.Errorf("expected lastPrice 150.00, got %f", fskax.LastPrice)
	}
	if fskax.LastPriceChange != 1.25 {
		t.Errorf("expected lastPriceChange 1.25, got %f", fskax.LastPriceChange)
	}
	if fskax.CurrentValue != 6787.50 {
		t.Errorf("expected currentValue 6787.50, got %f", fskax.CurrentValue)
	}
	if fskax.TodaysGainLossDollar != 56.56 {
		t.Errorf("expected todaysGainLossDollar 56.56, got %f", fskax.TodaysGainLossDollar)
	}
	if fskax.TodaysGainLossPct != 0.84 {
		t.Errorf("expected todaysGainLossPct 0.84, got %f", fskax.TodaysGainLossPct)
	}
	if fskax.CostBasisTotal != 6000.00 {
		t.Errorf("expected costBasisTotal 6000.00, got %f", fskax.CostBasisTotal)
	}
}

func assertZeroPosition(t *testing.T, zeroPos model.FidelityPosition) {
	t.Helper()
	if zeroPos.AccountNumber != "SYNTH-TAX-303" {
		t.Errorf("expected accountNumber SYNTH-TAX-303, got %s", zeroPos.AccountNumber)
	}
	if zeroPos.Symbol != "**" {
		t.Errorf("expected symbol **, got %s", zeroPos.Symbol)
	}
	if zeroPos.CurrentValue != 0.00 {
		t.Errorf("expected currentValue 0.00, got %f", zeroPos.CurrentValue)
	}
	if zeroPos.Quantity != 0.00 {
		t.Errorf("expected quantity 0.00, got %f", zeroPos.Quantity)
	}
}

func TestMapRawRecordToFidelityPosition_ErrorHandling(t *testing.T) {
	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	lineage := model.SourceLineage{FileName: "test.csv", RowNumber: 2}

	t.Run("invalid quantity error", func(t *testing.T) {
		record := map[string]string{
			"quantity": "not-a-number",
		}
		pos, err := model.MapRawRecordToFidelityPosition(record, asOfDate, lineage)
		if err == nil {
			t.Fatal("expected quantity error, got nil")
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got: %+v", pos)
		}
	})

	t.Run("invalid currency error", func(t *testing.T) {
		record := map[string]string{
			"quantity":   "10",
			"last price": "not-a-price",
		}
		pos, err := model.MapRawRecordToFidelityPosition(record, asOfDate, lineage)
		if err == nil {
			t.Fatal("expected currency error, got nil")
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got: %+v", pos)
		}
	})

	t.Run("invalid percentage error", func(t *testing.T) {
		record := map[string]string{
			"quantity":                  "10",
			"last price":                "$100.00",
			"today's gain/loss percent": "not-a-percent",
		}
		pos, err := model.MapRawRecordToFidelityPosition(record, asOfDate, lineage)
		if err == nil {
			t.Fatal("expected percentage error, got nil")
		}
		if pos != (model.FidelityPosition{}) {
			t.Errorf("expected empty position on error, got: %+v", pos)
		}
	})
}
