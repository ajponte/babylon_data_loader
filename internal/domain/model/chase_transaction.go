package model

import (
	"errors"
	"math"
	"time"
)

var (
	// ErrNilChaseTransaction indicates that a ChaseTransaction is nil.
	ErrNilChaseTransaction = errors.New("chase transaction is nil")
	// ErrMissingPostingDate indicates that the posting date is zero.
	ErrMissingPostingDate = errors.New("posting date is required")
	// ErrMissingDescription indicates that the transaction description is empty.
	ErrMissingDescription = errors.New("description is required")
	// ErrInvalidAmount indicates that the transaction amount is NaN or infinite.
	ErrInvalidAmount = errors.New("amount must be a valid finite number")
	// ErrInvalidBalance indicates that the account balance is NaN or infinite.
	ErrInvalidBalance = errors.New("balance must be a valid finite number")
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
	// TransactionDate is the credit card "Transaction Date" (purchase date) at UTC midnight.
	// It is nil for checking, synthetic rows, and placeholder values (matching legacy omitempty).
	TransactionDate *time.Time
}

// Validate validates the essential fields of a ChaseTransaction.
func (t *ChaseTransaction) Validate() error {
	if t == nil {
		return ErrNilChaseTransaction
	}
	if err := t.Lineage.Validate(); err != nil {
		return err
	}
	if t.AccountID == "" {
		return ErrMissingAccountID
	}
	if t.PostingDate.IsZero() {
		return ErrMissingPostingDate
	}
	if t.Description == "" {
		return ErrMissingDescription
	}
	if math.IsNaN(t.Amount) || math.IsInf(t.Amount, 0) {
		return ErrInvalidAmount
	}
	if t.Balance != nil && (math.IsNaN(*t.Balance) || math.IsInf(*t.Balance, 0)) {
		return ErrInvalidBalance
	}
	return nil
}
