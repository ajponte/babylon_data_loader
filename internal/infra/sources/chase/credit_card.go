package chase

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"babylon/dataloader/internal/domain/model"
)

// parseCreditCard parses Chase credit card CSV records from the provided reader.
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

// parseCreditCardRow parses a single row of a Chase credit card CSV into a ChaseTransaction.
func parseCreditCardRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	rr := newRowReader(row, colIndex, lineNum)

	postingDate, err := resolveCreditCardDate(rr)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	desc, err := rr.parseDescription("description")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	amount, err := rr.parseAmount("amount")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	balance, err := rr.parseBalance("balance")
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
		Type:           firstNonEmpty(rr.get("type"), "Sale"),
		Balance:        balance,
		CheckOrSlipNum: "",
		Category:       rr.get("category"),
		Memo:           rr.get("memo"),
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return tx, nil
}

// resolveCreditCardDate extracts the posting date, falling back to transaction date for pending transactions.
func resolveCreditCardDate(rr rowReader) (time.Time, error) {
	postDateStr := rr.get("post date", "posting date")
	if isPlaceholderDate(postDateStr) {
		postDateStr = rr.get("transaction date", "trans date")
	}
	if isPlaceholderDate(postDateStr) {
		return time.Time{}, fmt.Errorf(
			"row %d: missing both post date and transaction date: %w", rr.lineNum, model.ErrMissingPostingDate,
		)
	}
	postingDate, err := parseDate(postDateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("row %d: %w", rr.lineNum, err)
	}
	return postingDate, nil
}

// isPlaceholderDate checks whether a date string is empty, placeholder numeric, or "pending".
func isPlaceholderDate(val string) bool {
	return isPlaceholderNumeric(val) || strings.EqualFold(strings.TrimSpace(val), "pending")
}
