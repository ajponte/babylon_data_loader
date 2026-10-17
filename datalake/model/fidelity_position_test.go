package model_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake/model"
)

func TestMapRawRecordToFidelityPosition_SyntheticFixture(t *testing.T) {
	ctx := context.Background()
	parser := csvparser.NewDefaultParser()
	fixturePath := filepath.Join("..", "..", "testdata", "fidelity", "Portfolio_Positions_Synthetic.csv")

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
