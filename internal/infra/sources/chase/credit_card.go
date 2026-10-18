package chase

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"babylon/dataloader/internal/domain/model"
)

func parseCreditCard(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) ([]model.ChaseTransaction, error) {
	required := [][]string{
		{"post date", "posting date"},
		{"description"},
		{"amount"},
	}
	return parseChaseRecords(ctx, r, meta, "credit card", required, parseCreditCardRow)
}

func parseCreditCardRow(
	row []string,
	colIndex map[string]int,
	rowIdx int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	postDateStr := getFieldValue(row, colIndex, "post date", "posting date")
	postingDate, err := parseDate(postDateStr)
	if err != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: %w", rowIdx, err)
	}

	desc := getFieldValue(row, colIndex, "description")
	if strings.TrimSpace(desc) == "" {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: transaction description is required", rowIdx)
	}

	amtStr := cleanNumeric(getFieldValue(row, colIndex, "amount"))
	if amtStr == "" {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: transaction amount is required", rowIdx)
	}
	amount, err := strconv.ParseFloat(amtStr, 64)
	if err != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: invalid amount %q: %w", rowIdx, amtStr, err)
	}

	category := getFieldValue(row, colIndex, "category")
	txType := getFieldValue(row, colIndex, "type")
	if txType == "" {
		txType = "Sale"
	}
	memo := getFieldValue(row, colIndex, "memo")

	tx := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  rowIdx,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:      meta.AccountID,
		PostingDate:    postingDate,
		Description:    desc,
		Details:        "",
		Amount:         amount,
		Type:           txType,
		Balance:        nil,
		CheckOrSlipNum: "",
		Category:       category,
		Memo:           memo,
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", rowIdx, valErr)
	}

	return tx, nil
}
