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
}

// Validate validates the essential fields of a FidelityPosition.
func (p *FidelityPosition) Validate() error {
	if p == nil {
		return ErrNilFidelityPosition
	}
	if err := p.Lineage.Validate(); err != nil {
		return err
	}
	if p.AccountID == "" {
		return ErrMissingAccountID
	}
	if p.Symbol == "" {
		return ErrMissingSymbol
	}
	if math.IsNaN(p.Quantity) || math.IsInf(p.Quantity, 0) {
		return ErrInvalidQuantity
	}
	if math.IsNaN(p.LastPrice) || math.IsInf(p.LastPrice, 0) {
		return ErrInvalidLastPrice
	}
	if p.LastPrice < 0 {
		return ErrNegativeLastPrice
	}
	if math.IsNaN(p.CurrentValue) || math.IsInf(p.CurrentValue, 0) {
		return ErrInvalidCurrentValue
	}
	if math.IsNaN(p.CostBasis) || math.IsInf(p.CostBasis, 0) {
		return ErrInvalidCostBasis
	}
	return nil
}
