package model_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestChaseTransaction_Instantiation(t *testing.T) {
	postingDate := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	ingestedAt := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	balanceVal := 5432.10

	lineage := model.SourceLineage{
		FileName:   "Chase1234_Activity.CSV",
		FileHash:   "hashchase123",
		RowNumber:  3,
		IngestedAt: ingestedAt,
	}

	tx := model.ChaseTransaction{
		Lineage:        lineage,
		AccountID:      "chase-checking-001",
		PostingDate:    postingDate,
		Description:    "ATM WITHDRAWAL",
		Details:        "DEBIT",
		Amount:         -100.00,
		Type:           "DEBIT",
		Balance:        &balanceVal,
		CheckOrSlipNum: "1001",
		Category:       "Cash & ATM",
		Memo:           "Vacation spending money",
	}

	if tx.Lineage.FileName != "Chase1234_Activity.CSV" {
		t.Errorf("expected lineage FileName 'Chase1234_Activity.CSV', got %q", tx.Lineage.FileName)
	}
	if tx.AccountID != "chase-checking-001" {
		t.Errorf("expected AccountID 'chase-checking-001', got %q", tx.AccountID)
	}
	if !tx.PostingDate.Equal(postingDate) {
		t.Errorf("expected PostingDate %v, got %v", postingDate, tx.PostingDate)
	}
	if tx.Description != "ATM WITHDRAWAL" {
		t.Errorf("expected Description 'ATM WITHDRAWAL', got %q", tx.Description)
	}
	if tx.Details != "DEBIT" {
		t.Errorf("expected Details 'DEBIT', got %q", tx.Details)
	}
	if tx.Amount != -100.00 {
		t.Errorf("expected Amount -100.00, got %f", tx.Amount)
	}
	if tx.Type != "DEBIT" {
		t.Errorf("expected Type 'DEBIT', got %q", tx.Type)
	}
	if tx.Balance == nil || *tx.Balance != 5432.10 {
		t.Errorf("expected Balance 5432.10, got %v", tx.Balance)
	}
	if tx.CheckOrSlipNum != "1001" {
		t.Errorf("expected CheckOrSlipNum '1001', got %q", tx.CheckOrSlipNum)
	}
	if tx.Category != "Cash & ATM" {
		t.Errorf("expected Category 'Cash & ATM', got %q", tx.Category)
	}
	if tx.Memo != "Vacation spending money" {
		t.Errorf("expected Memo 'Vacation spending money', got %q", tx.Memo)
	}
}

func TestChaseTransaction_OptionalPointers(t *testing.T) {
	t.Run("Transaction with non-nil balance", func(t *testing.T) {
		val := 1250.75
		tx := model.ChaseTransaction{
			PostingDate: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC),
			Description: "PAYROLL DIRECT DEPOSIT",
			Amount:      3500.00,
			Balance:     &val,
		}

		if tx.Balance == nil {
			t.Fatal("expected non-nil Balance pointer")
		}
		if *tx.Balance != 1250.75 {
			t.Errorf("expected dereferenced Balance 1250.75, got %f", *tx.Balance)
		}
	})

	t.Run("Transaction with nil balance (e.g. credit card)", func(t *testing.T) {
		tx := model.ChaseTransaction{
			PostingDate: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC),
			Description: "COFFEE SHOP",
			Amount:      -4.50,
			Balance:     nil,
			Category:    "Food & Drink",
		}

		if tx.Balance != nil {
			t.Errorf("expected nil Balance pointer, got %v", tx.Balance)
		}
	})
}

func TestChaseTransaction_ZeroValue(t *testing.T) {
	var tx model.ChaseTransaction

	if tx.AccountID != "" {
		t.Errorf("expected empty AccountID, got %q", tx.AccountID)
	}
	if !tx.PostingDate.IsZero() {
		t.Errorf("expected zero PostingDate, got %v", tx.PostingDate)
	}
	if tx.Description != "" {
		t.Errorf("expected empty Description, got %q", tx.Description)
	}
	if tx.Details != "" {
		t.Errorf("expected empty Details, got %q", tx.Details)
	}
	if tx.Amount != 0.0 {
		t.Errorf("expected Amount 0.0, got %f", tx.Amount)
	}
	if tx.Type != "" {
		t.Errorf("expected empty Type, got %q", tx.Type)
	}
	if tx.Balance != nil {
		t.Errorf("expected nil Balance, got %v", tx.Balance)
	}
	if tx.CheckOrSlipNum != "" {
		t.Errorf("expected empty CheckOrSlipNum, got %q", tx.CheckOrSlipNum)
	}
	if tx.Category != "" {
		t.Errorf("expected empty Category, got %q", tx.Category)
	}
	if tx.Memo != "" {
		t.Errorf("expected empty Memo, got %q", tx.Memo)
	}
	if tx.Lineage.FileName != "" || tx.Lineage.RowNumber != 0 {
		t.Errorf("expected zero Lineage, got %+v", tx.Lineage)
	}
}

func TestChaseTransaction_Validate(t *testing.T) {
	validDate := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	validLineage := model.SourceLineage{
		FileName:  "statement.csv",
		RowNumber: 1,
	}
	validBalance := 100.50
	nanVal := math.NaN()
	posInf := math.Inf(1)
	negInf := math.Inf(-1)

	tests := []struct {
		name        string
		tx          *model.ChaseTransaction
		expectedErr error
	}{
		{
			name:        "nil transaction",
			tx:          nil,
			expectedErr: model.ErrNilChaseTransaction,
		},
		{
			name: "valid checking transaction with balance",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
				Balance:     &validBalance,
			},
			expectedErr: nil,
		},
		{
			name: "valid card transaction without balance",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "cc-456",
				PostingDate: validDate,
				Description: "TARGET",
				Amount:      -42.50,
				Balance:     nil,
			},
			expectedErr: nil,
		},
		{
			name: "invalid lineage missing filename",
			tx: &model.ChaseTransaction{
				Lineage: model.SourceLineage{
					FileName:  "",
					RowNumber: 1,
				},
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "invalid lineage negative row number",
			tx: &model.ChaseTransaction{
				Lineage: model.SourceLineage{
					FileName:  "statement.csv",
					RowNumber: -1,
				},
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
			},
			expectedErr: model.ErrNegativeRowNumber,
		},
		{
			name: "missing account ID",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
			},
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name: "missing posting date",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				Description: "STARBUCKS",
				Amount:      -6.25,
			},
			expectedErr: model.ErrMissingPostingDate,
		},
		{
			name: "missing description",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "",
				Amount:      -6.25,
			},
			expectedErr: model.ErrMissingDescription,
		},
		{
			name: "NaN amount",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      nanVal,
			},
			expectedErr: model.ErrInvalidAmount,
		},
		{
			name: "positive infinity amount",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      posInf,
			},
			expectedErr: model.ErrInvalidAmount,
		},
		{
			name: "negative infinity amount",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      negInf,
			},
			expectedErr: model.ErrInvalidAmount,
		},
		{
			name: "NaN balance",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
				Balance:     &nanVal,
			},
			expectedErr: model.ErrInvalidBalance,
		},
		{
			name: "positive infinity balance",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
				Balance:     &posInf,
			},
			expectedErr: model.ErrInvalidBalance,
		},
		{
			name: "negative infinity balance",
			tx: &model.ChaseTransaction{
				Lineage:     validLineage,
				AccountID:   "chk-123",
				PostingDate: validDate,
				Description: "STARBUCKS",
				Amount:      -6.25,
				Balance:     &negInf,
			},
			expectedErr: model.ErrInvalidBalance,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tx.Validate()
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Validate() err = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}
