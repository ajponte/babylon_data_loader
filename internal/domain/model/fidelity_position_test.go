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
		Lineage:              lineage,
		AccountID:            "FID-ROTH-789",
		AccountName:          "ROTH IRA",
		Symbol:               "FSKAX",
		Description:          "FIDELITY TOTAL MARKET INDEX",
		Quantity:             125.750,
		LastPrice:            152.40,
		CurrentValue:         19164.30,
		CostBasis:            16500.00,
		AsOfDate:             asOfDate,
		LastPriceChange:      1.25,
		TodaysGainLossDollar: 56.56,
		TodaysGainLossPct:    0.84,
		TotalGainLossDollar:  2664.30,
		TotalGainLossPct:     16.15,
		PercentOfAccount:     52.21,
		AverageCostBasis:     131.21,
		Type:                 "Cash",
	}

	assertFidelityPositionEqual(t, pos, model.FidelityPosition{
		Lineage:              lineage,
		AccountID:            "FID-ROTH-789",
		AccountName:          "ROTH IRA",
		Symbol:               "FSKAX",
		Description:          "FIDELITY TOTAL MARKET INDEX",
		Quantity:             125.750,
		LastPrice:            152.40,
		CurrentValue:         19164.30,
		CostBasis:            16500.00,
		AsOfDate:             asOfDate,
		LastPriceChange:      1.25,
		TodaysGainLossDollar: 56.56,
		TodaysGainLossPct:    0.84,
		TotalGainLossDollar:  2664.30,
		TotalGainLossPct:     16.15,
		PercentOfAccount:     52.21,
		AverageCostBasis:     131.21,
		Type:                 "Cash",
	})
}

func assertFidelityPositionEqual(t *testing.T, got, want model.FidelityPosition) {
	t.Helper()
	assertFidelityIdentityEqual(t, got, want)
	assertFidelityCoreMetricsEqual(t, got, want)
	assertFidelityPerformanceEqual(t, got, want)
}

func assertFidelityIdentityEqual(t *testing.T, got, want model.FidelityPosition) {
	t.Helper()
	if got.Lineage != want.Lineage {
		t.Errorf("expected Lineage %+v, got %+v", want.Lineage, got.Lineage)
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
}

func assertFidelityCoreMetricsEqual(t *testing.T, got, want model.FidelityPosition) {
	t.Helper()
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

func assertFidelityPerformanceEqual(t *testing.T, got, want model.FidelityPosition) {
	t.Helper()
	if got.LastPriceChange != want.LastPriceChange {
		t.Errorf("expected LastPriceChange %f, got %f", want.LastPriceChange, got.LastPriceChange)
	}
	if got.TodaysGainLossDollar != want.TodaysGainLossDollar {
		t.Errorf("expected TodaysGainLossDollar %f, got %f", want.TodaysGainLossDollar, got.TodaysGainLossDollar)
	}
	if got.TodaysGainLossPct != want.TodaysGainLossPct {
		t.Errorf("expected TodaysGainLossPct %f, got %f", want.TodaysGainLossPct, got.TodaysGainLossPct)
	}
	if got.TotalGainLossDollar != want.TotalGainLossDollar {
		t.Errorf("expected TotalGainLossDollar %f, got %f", want.TotalGainLossDollar, got.TotalGainLossDollar)
	}
	if got.TotalGainLossPct != want.TotalGainLossPct {
		t.Errorf("expected TotalGainLossPct %f, got %f", want.TotalGainLossPct, got.TotalGainLossPct)
	}
	if got.PercentOfAccount != want.PercentOfAccount {
		t.Errorf("expected PercentOfAccount %f, got %f", want.PercentOfAccount, got.PercentOfAccount)
	}
	if got.AverageCostBasis != want.AverageCostBasis {
		t.Errorf("expected AverageCostBasis %f, got %f", want.AverageCostBasis, got.AverageCostBasis)
	}
	if got.Type != want.Type {
		t.Errorf("expected Type %q, got %q", want.Type, got.Type)
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
	assertFidelityPositionEqual(t, pos, model.FidelityPosition{})
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
		AccountID:            "FID-123",
		Symbol:               "FXAIX",
		Quantity:             10.0,
		LastPrice:            180.50,
		CurrentValue:         1805.00,
		CostBasis:            1500.00,
		LastPriceChange:      1.25,
		TodaysGainLossDollar: 12.50,
		TodaysGainLossPct:    0.70,
		TotalGainLossDollar:  305.00,
		TotalGainLossPct:     20.33,
		PercentOfAccount:     100.00,
		AverageCostBasis:     150.00,
		Type:                 "Cash",
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
			name: "valid position with negative daily and total gain/loss performance metrics",
			pos: newValidTestPosition(func(p *model.FidelityPosition) {
				p.LastPriceChange = -0.80
				p.TodaysGainLossDollar = -20.40
				p.TodaysGainLossPct = -0.44
				p.TotalGainLossDollar = -595.10
				p.TotalGainLossPct = -14.88
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

func TestFidelityPosition_ErrInvalidPerformanceMetricSentinel(t *testing.T) {
	if model.ErrInvalidPerformanceMetric == nil {
		t.Fatal("expected model.ErrInvalidPerformanceMetric to be non-nil")
	}
	if model.ErrInvalidPerformanceMetric.Error() == "" {
		t.Error("expected model.ErrInvalidPerformanceMetric to have a non-empty error message")
	}
	wrapped := fmt.Errorf("row validation failed: %w", model.ErrInvalidPerformanceMetric)
	if !errors.Is(wrapped, model.ErrInvalidPerformanceMetric) {
		t.Errorf("expected errors.Is(wrapped, model.ErrInvalidPerformanceMetric) == true, got false")
	}
	distinctSentinels := []error{
		model.ErrInvalidQuantity,
		model.ErrInvalidLastPrice,
		model.ErrInvalidCurrentValue,
		model.ErrInvalidCostBasis,
		model.ErrMissingAsOfDate,
	}
	for _, target := range distinctSentinels {
		if errors.Is(wrapped, target) {
			t.Errorf("expected ErrInvalidPerformanceMetric to be distinct from %v", target)
		}
	}
}

func TestFidelityPosition_Validate_PerformanceMetricsFinite(t *testing.T) {
	nonFiniteValues := []struct {
		label string
		val   float64
	}{
		{label: "NaN", val: math.NaN()},
		{label: "+Inf", val: math.Inf(1)},
		{label: "-Inf", val: math.Inf(-1)},
	}

	fields := []struct {
		name string
		set  func(p *model.FidelityPosition, v float64)
	}{
		{name: "LastPriceChange", set: func(p *model.FidelityPosition, v float64) { p.LastPriceChange = v }},
		{name: "TodaysGainLossDollar", set: func(p *model.FidelityPosition, v float64) { p.TodaysGainLossDollar = v }},
		{name: "TodaysGainLossPct", set: func(p *model.FidelityPosition, v float64) { p.TodaysGainLossPct = v }},
		{name: "TotalGainLossDollar", set: func(p *model.FidelityPosition, v float64) { p.TotalGainLossDollar = v }},
		{name: "TotalGainLossPct", set: func(p *model.FidelityPosition, v float64) { p.TotalGainLossPct = v }},
		{name: "PercentOfAccount", set: func(p *model.FidelityPosition, v float64) { p.PercentOfAccount = v }},
		{name: "AverageCostBasis", set: func(p *model.FidelityPosition, v float64) { p.AverageCostBasis = v }},
	}

	for _, field := range fields {
		for _, nf := range nonFiniteValues {
			t.Run(field.name+"_"+nf.label, func(t *testing.T) {
				pos := newValidTestPosition(func(p *model.FidelityPosition) {
					field.set(p, nf.val)
				})
				err := pos.Validate()
				if !errors.Is(err, model.ErrInvalidPerformanceMetric) {
					t.Errorf(
						"expected errors.Is(err, model.ErrInvalidPerformanceMetric) for %s=%s, got: %v",
						field.name,
						nf.label,
						err,
					)
				}
			})
		}
	}
}
