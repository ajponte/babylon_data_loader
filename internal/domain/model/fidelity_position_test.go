package model_test

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestFidelityPosition_Instantiation(t *testing.T) {
	ingestedAt := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	asOfDate := time.Date(2026, 9, 26, 1, 50, 0, 0, time.UTC)
	lineage := model.SourceLineage{
		FileName:   "Portfolio_Positions.csv",
		FileHash:   "fidposhash123",
		RowNumber:  5,
		IngestedAt: ingestedAt,
	}

	pos := model.FidelityPosition{
		Lineage:      lineage,
		AccountID:    "FID-ROTH-789",
		AccountName:  "ROTH IRA",
		Symbol:       "FSKAX",
		Description:  "FIDELITY TOTAL MARKET INDEX",
		Quantity:     125.750,
		LastPrice:    152.40,
		CurrentValue: 19164.30,
		CostBasis:    16500.00,
		AsOfDate:     asOfDate,
	}

	assertFidelityPositionEqual(t, pos, model.FidelityPosition{
		Lineage:      lineage,
		AccountID:    "FID-ROTH-789",
		AccountName:  "ROTH IRA",
		Symbol:       "FSKAX",
		Description:  "FIDELITY TOTAL MARKET INDEX",
		Quantity:     125.750,
		LastPrice:    152.40,
		CurrentValue: 19164.30,
		CostBasis:    16500.00,
		AsOfDate:     asOfDate,
	})
}

func assertFidelityPositionEqual(t *testing.T, got, want model.FidelityPosition) {
	t.Helper()
	if got.Lineage.FileName != want.Lineage.FileName {
		t.Errorf("expected lineage FileName %q, got %q", want.Lineage.FileName, got.Lineage.FileName)
	}
	if got.AccountID != want.AccountID {
		t.Errorf("expected AccountID %q, got %q", want.AccountID, got.AccountID)
	}
	if got.AccountName != want.AccountName {
		t.Errorf("expected AccountName %q, got %q", want.AccountName, got.AccountName)
	}
	if got.Symbol != want.Symbol {
		t.Errorf("expected Symbol %q, got %q", want.Symbol, got.Symbol)
	}
	if got.Description != want.Description {
		t.Errorf("expected Description %q, got %q", want.Description, got.Description)
	}
	if got.Quantity != want.Quantity {
		t.Errorf("expected Quantity %f, got %f", want.Quantity, got.Quantity)
	}
	if got.LastPrice != want.LastPrice {
		t.Errorf("expected LastPrice %f, got %f", want.LastPrice, got.LastPrice)
	}
	if got.CurrentValue != want.CurrentValue {
		t.Errorf("expected CurrentValue %f, got %f", want.CurrentValue, got.CurrentValue)
	}
	if got.CostBasis != want.CostBasis {
		t.Errorf("expected CostBasis %f, got %f", want.CostBasis, got.CostBasis)
	}
	if !got.AsOfDate.Equal(want.AsOfDate) {
		t.Errorf("expected AsOfDate %v, got %v", want.AsOfDate, got.AsOfDate)
	}
	if got.AsOfDate.Location() != time.UTC {
		t.Errorf("expected AsOfDate location UTC, got %v", got.AsOfDate.Location())
	}
}

func TestFidelityPosition_NumericPrecision(t *testing.T) {
	tests := []struct {
		name         string
		quantity     float64
		lastPrice    float64
		currentValue float64
		costBasis    float64
	}{
		{
			name:         "fractional shares and penny prices",
			quantity:     0.1234,
			lastPrice:    54.99,
			currentValue: 6.79,
			costBasis:    5.50,
		},
		{
			name:         "zero cost basis (e.g. gift or cash)",
			quantity:     1000.0,
			lastPrice:    1.00,
			currentValue: 1000.0,
			costBasis:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos := model.FidelityPosition{
				Quantity:     tt.quantity,
				LastPrice:    tt.lastPrice,
				CurrentValue: tt.currentValue,
				CostBasis:    tt.costBasis,
			}

			if pos.Quantity != tt.quantity {
				t.Errorf("Quantity = %f; want %f", pos.Quantity, tt.quantity)
			}
			if pos.LastPrice != tt.lastPrice {
				t.Errorf("LastPrice = %f; want %f", pos.LastPrice, tt.lastPrice)
			}
			if pos.CurrentValue != tt.currentValue {
				t.Errorf("CurrentValue = %f; want %f", pos.CurrentValue, tt.currentValue)
			}
			if pos.CostBasis != tt.costBasis {
				t.Errorf("CostBasis = %f; want %f", pos.CostBasis, tt.costBasis)
			}
		})
	}
}

func TestFidelityPosition_ZeroValue(t *testing.T) {
	var pos model.FidelityPosition

	if pos.AccountID != "" {
		t.Errorf("expected empty AccountID, got %q", pos.AccountID)
	}
	if pos.AccountName != "" {
		t.Errorf("expected empty AccountName, got %q", pos.AccountName)
	}
	if pos.Symbol != "" {
		t.Errorf("expected empty Symbol, got %q", pos.Symbol)
	}
	if pos.Description != "" {
		t.Errorf("expected empty Description, got %q", pos.Description)
	}
	if pos.Quantity != 0.0 {
		t.Errorf("expected Quantity 0.0, got %f", pos.Quantity)
	}
	if pos.LastPrice != 0.0 {
		t.Errorf("expected LastPrice 0.0, got %f", pos.LastPrice)
	}
	if pos.CurrentValue != 0.0 {
		t.Errorf("expected CurrentValue 0.0, got %f", pos.CurrentValue)
	}
	if pos.CostBasis != 0.0 {
		t.Errorf("expected CostBasis 0.0, got %f", pos.CostBasis)
	}
	if pos.Lineage.FileName != "" || pos.Lineage.RowNumber != 0 {
		t.Errorf("expected zero Lineage, got %+v", pos.Lineage)
	}
	if !pos.AsOfDate.IsZero() {
		t.Errorf("expected zero AsOfDate, got %v", pos.AsOfDate)
	}
}

func newValidTestPosition(mutate func(*model.FidelityPosition)) *model.FidelityPosition {
	pos := &model.FidelityPosition{
		Lineage: model.SourceLineage{
			FileName:  "positions.csv",
			RowNumber: 2,
		},
		AccountID:    "FID-123",
		Symbol:       "FXAIX",
		Quantity:     10.0,
		LastPrice:    180.50,
		CurrentValue: 1805.00,
		CostBasis:    1500.00,
	}
	if mutate != nil {
		mutate(pos)
	}
	return pos
}

func TestFidelityPosition_Validate(t *testing.T) {
	nanVal := math.NaN()
	posInf := math.Inf(1)
	negInf := math.Inf(-1)

	tests := []struct {
		name        string
		pos         *model.FidelityPosition
		expectedErr error
	}{
		{
			name:        "nil position",
			pos:         nil,
			expectedErr: model.ErrNilFidelityPosition,
		},
		{
			name:        "valid position with zero AsOfDate (pre-footer row validation)",
			pos:         newValidTestPosition(nil),
			expectedErr: nil,
		},
		{
			name: "valid position with populated UTC AsOfDate (post-footer stamping)",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.AsOfDate = time.Date(2026, 9, 26, 1, 50, 0, 0, time.UTC)
			}),
			expectedErr: nil,
		},
		{
			name: "invalid lineage missing filename",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.Lineage.FileName = ""
			}),
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "invalid lineage negative row number",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.Lineage.RowNumber = -2
			}),
			expectedErr: model.ErrNegativeRowNumber,
		},
		{
			name: "missing account ID",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.AccountID = ""
			}),
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name: "missing symbol",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.Symbol = ""
			}),
			expectedErr: model.ErrMissingSymbol,
		},
		{
			name: "NaN quantity",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.Quantity = nanVal
			}),
			expectedErr: model.ErrInvalidQuantity,
		},
		{
			name: "infinity quantity",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.Quantity = posInf
			}),
			expectedErr: model.ErrInvalidQuantity,
		},
		{
			name: "NaN last price",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.LastPrice = nanVal
			}),
			expectedErr: model.ErrInvalidLastPrice,
		},
		{
			name: "infinity last price",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.LastPrice = posInf
			}),
			expectedErr: model.ErrInvalidLastPrice,
		},
		{
			name: "negative last price",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.LastPrice = -1.50
			}),
			expectedErr: model.ErrNegativeLastPrice,
		},
		{
			name: "NaN current value",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.CurrentValue = nanVal
			}),
			expectedErr: model.ErrInvalidCurrentValue,
		},
		{
			name: "infinity current value",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.CurrentValue = negInf
			}),
			expectedErr: model.ErrInvalidCurrentValue,
		},
		{
			name: "NaN cost basis",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.CostBasis = nanVal
			}),
			expectedErr: model.ErrInvalidCostBasis,
		},
		{
			name: "infinity cost basis",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.CostBasis = posInf
			}),
			expectedErr: model.ErrInvalidCostBasis,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pos.Validate()
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}

func TestFidelityPosition_ErrMissingAsOfDateSentinel(t *testing.T) {
	if model.ErrMissingAsOfDate == nil {
		t.Fatal("expected model.ErrMissingAsOfDate to be non-nil")
	}
	if model.ErrMissingAsOfDate.Error() == "" {
		t.Error("expected model.ErrMissingAsOfDate to have a non-empty error message")
	}
	wrapped := fmt.Errorf("batch validation failed: %w", model.ErrMissingAsOfDate)
	if !errors.Is(wrapped, model.ErrMissingAsOfDate) {
		t.Errorf("expected errors.Is(wrapped, model.ErrMissingAsOfDate) == true, got false")
	}
}
