package model

import (
	"errors"
	"time"
)

var (
	// ErrNilChaseTransaction indicates that a ChaseTransaction is nil.
	ErrNilChaseTransaction = errors.New("chase transaction is nil")
	// ErrMissingPostingDate indicates that the posting date is zero.
	ErrMissingPostingDate = errors.New("posting date is required")
	// ErrMissingDescription indicates that the transaction description is empty.
	ErrMissingDescription = errors.New("description is required")
)

// ChaseTransaction represents a normalized transaction record from Chase statements.
type ChaseTransaction struct {
	Lineage        SourceLineage
	AccountID      string
	PostingDate    time.Time
	Description    string
	Details        string
	Amount         float64
	Type           string
	Balance        *float64
	CheckOrSlipNum string
	Category       string
	Memo           string
}

// Validate validates the essential fields of a ChaseTransaction.
func (t *ChaseTransaction) Validate() error {
	if t == nil {
		return ErrNilChaseTransaction
	}
	if t.PostingDate.IsZero() {
		return ErrMissingPostingDate
	}
	if t.Description == "" {
		return ErrMissingDescription
	}
	return nil
}
