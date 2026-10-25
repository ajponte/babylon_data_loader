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

	if pos.Lineage.FileName != "Portfolio_Positions.csv" {
		t.Errorf("expected lineage FileName 'Portfolio_Positions.csv', got %q", pos.Lineage.FileName)
	}
	if pos.AccountID != "FID-ROTH-789" {
		t.Errorf("expected AccountID 'FID-ROTH-789', got %q", pos.AccountID)
	}
	if pos.AccountName != "ROTH IRA" {
		t.Errorf("expected AccountName 'ROTH IRA', got %q", pos.AccountName)
	}
	if pos.Symbol != "FSKAX" {
		t.Errorf("expected Symbol 'FSKAX', got %q", pos.Symbol)
	}
	if pos.Description != "FIDELITY TOTAL MARKET INDEX" {
		t.Errorf("expected Description 'FIDELITY TOTAL MARKET INDEX', got %q", pos.Description)
	}
	if pos.Quantity != 125.750 {
		t.Errorf("expected Quantity 125.750, got %f", pos.Quantity)
	}
	if pos.LastPrice != 152.40 {
		t.Errorf("expected LastPrice 152.40, got %f", pos.LastPrice)
	}
	if pos.CurrentValue != 19164.30 {
		t.Errorf("expected CurrentValue 19164.30, got %f", pos.CurrentValue)
	}
	if pos.CostBasis != 16500.00 {
		t.Errorf("expected CostBasis 16500.00, got %f", pos.CostBasis)
	}
	if !pos.AsOfDate.Equal(asOfDate) {
		t.Errorf("expected AsOfDate %v, got %v", asOfDate, pos.AsOfDate)
	}
	if pos.AsOfDate.Location() != time.UTC {
		t.Errorf("expected AsOfDate location UTC, got %v", pos.AsOfDate.Location())
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

func TestFidelityPosition_Validate(t *testing.T) {
	validLineage := model.SourceLineage{
		FileName:  "positions.csv",
		RowNumber: 2,
	}
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
			name: "valid position with zero AsOfDate (pre-footer row validation)",
			pos: &model.FidelityPosition{
				Lineage:      validLineage,
				AccountID:    "FID-123",
				Symbol:       "FXAIX",
				Quantity:     10.0,
				LastPrice:    180.50,
				CurrentValue: 1805.00,
				CostBasis:    1500.00,
			},
			expectedErr: nil,
		},
		{
			name: "valid position with populated UTC AsOfDate (post-footer stamping)",
			pos: &model.FidelityPosition{
				Lineage:      validLineage,
				AccountID:    "FID-123",
				Symbol:       "FXAIX",
				Quantity:     10.0,
				LastPrice:    180.50,
				CurrentValue: 1805.00,
				CostBasis:    1500.00,
				AsOfDate:     time.Date(2026, 9, 26, 1, 50, 0, 0, time.UTC),
			},
			expectedErr: nil,
		},
		{
			name: "invalid lineage missing filename",
			pos: &model.FidelityPosition{
				Lineage: model.SourceLineage{
					FileName:  "",
					RowNumber: 1,
				},
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "invalid lineage negative row number",
			pos: &model.FidelityPosition{
				Lineage: model.SourceLineage{
					FileName:  "positions.csv",
					RowNumber: -2,
				},
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
			},
			expectedErr: model.ErrNegativeRowNumber,
		},
		{
			name: "missing account ID",
			pos: &model.FidelityPosition{
				Lineage:  validLineage,
				Symbol:   "FXAIX",
				Quantity: 10.0,
			},
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name: "missing symbol",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Quantity:  10.0,
			},
			expectedErr: model.ErrMissingSymbol,
		},
		{
			name: "NaN quantity",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  nanVal,
			},
			expectedErr: model.ErrInvalidQuantity,
		},
		{
			name: "infinity quantity",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  posInf,
			},
			expectedErr: model.ErrInvalidQuantity,
		},
		{
			name: "NaN last price",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
				LastPrice: nanVal,
			},
			expectedErr: model.ErrInvalidLastPrice,
		},
		{
			name: "infinity last price",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
				LastPrice: posInf,
			},
			expectedErr: model.ErrInvalidLastPrice,
		},
		{
			name: "negative last price",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
				LastPrice: -1.50,
			},
			expectedErr: model.ErrNegativeLastPrice,
		},
		{
			name: "NaN current value",
			pos: &model.FidelityPosition{
				Lineage:      validLineage,
				AccountID:    "FID-123",
				Symbol:       "FXAIX",
				Quantity:     10.0,
				CurrentValue: nanVal,
			},
			expectedErr: model.ErrInvalidCurrentValue,
		},
		{
			name: "infinity current value",
			pos: &model.FidelityPosition{
				Lineage:      validLineage,
				AccountID:    "FID-123",
				Symbol:       "FXAIX",
				Quantity:     10.0,
				CurrentValue: negInf,
			},
			expectedErr: model.ErrInvalidCurrentValue,
		},
		{
			name: "NaN cost basis",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
				CostBasis: nanVal,
			},
			expectedErr: model.ErrInvalidCostBasis,
		},
		{
			name: "infinity cost basis",
			pos: &model.FidelityPosition{
				Lineage:   validLineage,
				AccountID: "FID-123",
				Symbol:    "FXAIX",
				Quantity:  10.0,
				CostBasis: posInf,
			},
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
