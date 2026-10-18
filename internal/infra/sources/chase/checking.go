package chase

import (
	"context"
	"fmt"
	"io"

	"babylon/dataloader/internal/domain/model"
)

// parseChecking parses Chase checking account CSV records from the provided reader.
func parseChecking(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) ([]model.ChaseTransaction, error) {
	required := [][]string{
		{"posting date", "post date"},
		{"description"},
		{"amount"},
	}
	return parseChaseRecords(ctx, r, meta, "checking", required, parseCheckingRow)
}

// parseCheckingRow parses a single row of a Chase checking CSV into a ChaseTransaction.
func parseCheckingRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	rr := newRowReader(row, colIndex, lineNum)

	postingDate, err := rr.parseDate("posting date", "post date")
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

	details := rr.get("details")
	txType := firstNonEmpty(rr.get("type"), details, "TRANSACTION")
	checkNum := rr.get("check or slip #", "check or slip num")

	tx := model.ChaseTransaction{
		Lineage:        buildLineage(meta, lineNum),
		AccountID:      meta.AccountID,
		PostingDate:    postingDate,
		Description:    desc,
		Details:        details,
		Amount:         amount,
		Type:           txType,
		Balance:        balance,
		CheckOrSlipNum: checkNum,
		Category:       "",
		Memo:           "",
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return tx, nil
}
