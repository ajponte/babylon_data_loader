package chase

import (
	"context"
	"fmt"
	"io"
	"strings"

	"babylon/dataloader/internal/domain/model"
)

func parseCreditCard(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) ([]model.ChaseTransaction, error) {
	required := [][]string{
		{"post date", "posting date", "transaction date", "trans date"},
		{"description"},
		{"amount"},
	}
	return parseChaseRecords(ctx, r, meta, "credit card", required, parseCreditCardRow)
}

func parseCreditCardRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	postDateStr := getFieldValue(row, colIndex, "post date", "posting date")
	if isPlaceholderDate(postDateStr) {
		postDateStr = getFieldValue(row, colIndex, "transaction date", "trans date")
	}
	if isPlaceholderDate(postDateStr) {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: missing both post date and transaction date", lineNum)
	}
	postingDate, err := parseDate(postDateStr)
	if err != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: %w", lineNum, err)
	}

	desc, err := parseDescription(getFieldValue(row, colIndex, "description"), lineNum)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	amount, err := parseAmount(getFieldValue(row, colIndex, "amount"), lineNum)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	category := getFieldValue(row, colIndex, "category")
	txType := getFieldValue(row, colIndex, "type")
	if txType == "" {
		txType = "Sale"
	}
	memo := getFieldValue(row, colIndex, "memo")

	balance, err := parseBalance(getFieldValue(row, colIndex, "balance"), lineNum)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	tx := model.ChaseTransaction{
		Lineage:        buildLineage(meta, lineNum),
		AccountID:      meta.AccountID,
		PostingDate:    postingDate,
		Description:    desc,
		Details:        "",
		Amount:         amount,
		Type:           txType,
		Balance:        balance,
		CheckOrSlipNum: "",
		Category:       category,
		Memo:           memo,
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return tx, nil
}

// isPlaceholderDate checks whether a date string is empty or contains an indicator for pending transactions.
func isPlaceholderDate(val string) bool {
	trimmed := strings.TrimSpace(val)
	return trimmed == "" || trimmed == "-" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") || strings.EqualFold(trimmed, "pending")
}
