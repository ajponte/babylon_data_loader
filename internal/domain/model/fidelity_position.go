package model

import (
	"errors"
)

var (
	// ErrNilFidelityPosition indicates that a FidelityPosition is nil.
	ErrNilFidelityPosition = errors.New("fidelity position is nil")
	// ErrMissingSymbol indicates that the position symbol is empty.
	ErrMissingSymbol = errors.New("symbol is required")
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
}

// Validate validates the essential fields of a FidelityPosition.
func (p *FidelityPosition) Validate() error {
	if p == nil {
		return ErrNilFidelityPosition
	}
	if p.Symbol == "" {
		return ErrMissingSymbol
	}
	return nil
}
