package model

import (
	"errors"
	"math"
	"time"
)

var (
	// ErrNilFidelityPosition indicates that a FidelityPosition is nil.
	ErrNilFidelityPosition = errors.New("fidelity position is nil")
	// ErrMissingSymbol indicates that the position symbol is empty.
	ErrMissingSymbol = errors.New("symbol is required")
	// ErrInvalidQuantity indicates that the quantity is NaN or infinite.
	ErrInvalidQuantity = errors.New("quantity must be a valid finite number")
	// ErrInvalidLastPrice indicates that the last price is NaN or infinite.
	ErrInvalidLastPrice = errors.New("last price must be a valid finite number")
	// ErrNegativeLastPrice indicates that the last price is negative.
	ErrNegativeLastPrice = errors.New("last price cannot be negative")
	// ErrInvalidCurrentValue indicates that the current value is NaN or infinite.
	ErrInvalidCurrentValue = errors.New("current value must be a valid finite number")
	// ErrInvalidCostBasis indicates that the cost basis is NaN or infinite.
	ErrInvalidCostBasis = errors.New("cost basis must be a valid finite number")
	// ErrInvalidPerformanceMetric indicates that a performance or allocation metric is NaN or infinite.
	ErrInvalidPerformanceMetric = errors.New("performance or allocation metric must be a valid finite number")
	// ErrMissingAsOfDate indicates that the portfolio snapshot date (asOfDate) is missing.
	ErrMissingAsOfDate = errors.New("fidelity snapshot date (asOfDate) is required")
)

// FidelityPosition represents a normalized position holding from Fidelity statements.
type FidelityPosition struct {
	Lineage      SourceLineage
	AccountID    string
	AccountName  string
	Symbol       string
	Description  string
	Quantity     float64
	LastPrice    float64
	CurrentValue float64
	CostBasis    float64
	AsOfDate     time.Time // UTC instant parsed from the "Date downloaded" footer.

	// Legacy-parity fields (A1, Option C). Placeholders ("--", "") parse to zero, as in legacy.
	LastPriceChange      float64 // Per-unit dollar change.
	TodaysGainLossDollar float64 // Today's gain/loss in dollars.
	TodaysGainLossPct    float64 // Percentage points: "+0.84%" -> 0.84 (not 0.0084).
	TotalGainLossDollar  float64 // Total gain/loss in dollars.
	TotalGainLossPct     float64 // Percentage points.
	PercentOfAccount     float64 // Percentage points.
	AverageCostBasis     float64 // Per-unit cost basis in dollars.
	Type                 string  // Raw Fidelity "Type" column, trimmed (e.g. "Cash", "Margin").
}

// Validate validates the essential fields of a FidelityPosition.
func (p *FidelityPosition) Validate() error {
	if p == nil {
		return ErrNilFidelityPosition
	}
	if err := p.validateIdentity(); err != nil {
		return err
	}
	if err := p.validateCoreMetrics(); err != nil {
		return err
	}
	return p.validatePerformanceMetrics()
}

func (p *FidelityPosition) validateIdentity() error {
	if err := p.Lineage.Validate(); err != nil {
		return err
	}
	if p.AccountID == "" {
		return ErrMissingAccountID
	}
	if p.Symbol == "" {
		return ErrMissingSymbol
	}
	return nil
}

func (p *FidelityPosition) validateCoreMetrics() error {
	if !isFinite(p.Quantity) {
		return ErrInvalidQuantity
	}
	if !isFinite(p.LastPrice) {
		return ErrInvalidLastPrice
	}
	if p.LastPrice < 0 {
		return ErrNegativeLastPrice
	}
	if !isFinite(p.CurrentValue) {
		return ErrInvalidCurrentValue
	}
	if !isFinite(p.CostBasis) {
		return ErrInvalidCostBasis
	}
	return nil
}

func (p *FidelityPosition) validatePerformanceMetrics() error {
	metrics := [...]float64{
		p.LastPriceChange,
		p.TodaysGainLossDollar,
		p.TodaysGainLossPct,
		p.TotalGainLossDollar,
		p.TotalGainLossPct,
		p.PercentOfAccount,
		p.AverageCostBasis,
	}
	for _, v := range metrics {
		if !isFinite(v) {
			return ErrInvalidPerformanceMetric
		}
	}
	return nil
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
