package chase

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"babylon/dataloader/internal/domain/model"
)

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

func parseCheckingRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	postDateStr := getFieldValue(row, colIndex, "posting date", "post date")
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

	details := getFieldValue(row, colIndex, "details")
	txType := getFieldValue(row, colIndex, "type")
	if txType == "" {
		txType = details
	}
	if txType == "" {
		txType = "TRANSACTION"
	}

	balance, err := parseBalance(getFieldValue(row, colIndex, "balance"), lineNum)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	checkNum := getFieldValue(row, colIndex, "check or slip #", "check or slip num")

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

//nolint:nilnil // nil balance represents an intentionally absent optional value
func parseBalance(balRaw string, lineNum int) (*float64, error) {
	if balRaw == "" || balRaw == "-" || balRaw == "--" || strings.EqualFold(balRaw, "n/a") {
		return nil, nil
	}
	balStr := cleanNumeric(balRaw)
	bal, balErr := strconv.ParseFloat(balStr, 64)
	if balErr != nil {
		return nil, fmt.Errorf("row %d: invalid balance %q: %w", lineNum, balStr, balErr)
	}
	if math.IsNaN(bal) || math.IsInf(bal, 0) {
		return nil, fmt.Errorf("row %d: invalid balance %q: non-finite number", lineNum, balStr)
	}
	return &bal, nil
}
