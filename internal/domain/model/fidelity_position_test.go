package model_test

import (
	"errors"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestFidelityPosition_Instantiation(t *testing.T) {
	ingestedAt := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
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
				Symbol:       "TEST",
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
}

func TestFidelityPosition_Validate(t *testing.T) {
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
			name: "valid position",
			pos: &model.FidelityPosition{
				Symbol:   "FXAIX",
				Quantity: 10.0,
			},
			expectedErr: nil,
		},
		{
			name: "missing symbol",
			pos: &model.FidelityPosition{
				Quantity: 10.0,
			},
			expectedErr: model.ErrMissingSymbol,
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
