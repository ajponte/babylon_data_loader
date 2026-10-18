package chase

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"babylon/dataloader/internal/domain/model"
)

type rowParser func(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error)

func parseChaseRecords(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
	dialectName string,
	requiredCols [][]string,
	parseRow rowParser,
) ([]model.ChaseTransaction, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	reader.TrimLeadingSpace = true

	colIndex, err := readChaseHeader(reader, dialectName, requiredCols)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return []model.ChaseTransaction{}, nil
		}
		return nil, err
	}

	var transactions []model.ChaseTransaction

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		row, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read Chase %s CSV row: %w", dialectName, readErr)
		}

		lineNum, _ := reader.FieldPos(0)

		if isRowEmpty(row) {
			continue
		}

		tx, parseErr := parseRow(row, colIndex, lineNum, meta)
		if parseErr != nil {
			return nil, parseErr
		}

		transactions = append(transactions, tx)
	}

	if transactions == nil {
		transactions = []model.ChaseTransaction{}
	}

	return transactions, nil
}

func readChaseHeader(
	reader *csv.Reader,
	dialectName string,
	requiredCols [][]string,
) (map[string]int, error) {
	var header []string
	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read Chase %s CSV header: %w", dialectName, err)
		}
		if !isRowEmpty(record) {
			header = record
			break
		}
	}

	colIndex := make(map[string]int)
	for i, col := range header {
		cleanCol := strings.TrimPrefix(col, "\xef\xbb\xbf")
		colIndex[strings.ToLower(strings.TrimSpace(cleanCol))] = i
	}

	for _, req := range requiredCols {
		if _, ok := findColumn(colIndex, req...); !ok {
			return nil, fmt.Errorf("chase %s CSV missing required header column %q", dialectName, req[0])
		}
	}

	return colIndex, nil
}

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

	desc := getFieldValue(row, colIndex, "description")
	if strings.TrimSpace(desc) == "" {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: transaction description is required", lineNum)
	}

	amtStr := cleanNumeric(getFieldValue(row, colIndex, "amount"))
	if amtStr == "" {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: transaction amount is required", lineNum)
	}
	amount, err := strconv.ParseFloat(amtStr, 64)
	if err != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d: invalid amount %q: %w", lineNum, amtStr, err)
	}

	details := getFieldValue(row, colIndex, "details")
	txType := getFieldValue(row, colIndex, "type")
	if txType == "" {
		txType = details
	}
	if txType == "" {
		txType = "TRANSACTION"
	}

	var balance *float64
	balRaw := getFieldValue(row, colIndex, "balance")
	if balRaw != "" && balRaw != "-" && balRaw != "--" && !strings.EqualFold(balRaw, "n/a") {
		balStr := cleanNumeric(balRaw)
		bal, balErr := strconv.ParseFloat(balStr, 64)
		if balErr != nil {
			return model.ChaseTransaction{}, fmt.Errorf("row %d: invalid balance %q: %w", lineNum, balStr, balErr)
		}
		if math.IsNaN(bal) || math.IsInf(bal, 0) {
			return model.ChaseTransaction{}, fmt.Errorf(
				"row %d: invalid balance %q: non-finite number",
				lineNum,
				balStr,
			)
		}
		balance = &bal
	}

	checkNum := getFieldValue(row, colIndex, "check or slip #", "check or slip num")

	tx := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  lineNum,
			IngestedAt: meta.IngestedAt,
		},
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

func parseDate(val string) (time.Time, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return time.Time{}, errors.New("date value is empty")
	}

	dateLayouts := []string{
		"01/02/2006",
		"1/2/2006",
		"2006-01-02",
		"2006/01/02",
	}

	for _, layout := range dateLayouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format %q: expected MM/DD/YYYY or YYYY-MM-DD", val)
}

func cleanNumeric(val string) string {
	clean := strings.TrimSpace(val)
	if clean == "-" || clean == "--" || strings.EqualFold(clean, "n/a") {
		return "0"
	}
	if strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")") {
		clean = "-" + clean[1:len(clean)-1]
	}
	clean = strings.ReplaceAll(clean, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	return strings.TrimSpace(clean)
}

func findColumn(colIndex map[string]int, aliases ...string) (int, bool) {
	for _, alias := range aliases {
		if idx, ok := colIndex[strings.ToLower(alias)]; ok {
			return idx, true
		}
	}
	return -1, false
}

func getFieldValue(row []string, colIndex map[string]int, aliases ...string) string {
	idx, ok := findColumn(colIndex, aliases...)
	if !ok || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

func isRowEmpty(row []string) bool {
	if len(row) == 0 {
		return true
	}
	for _, field := range row {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}
