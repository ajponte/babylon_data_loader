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

const defaultTransactionsCapacity = 64

// supportedDateLayouts defines the accepted timestamp layouts in Chase statement CSVs.
//
//nolint:gochecknoglobals // immutable format string slice reused across row parsers
var supportedDateLayouts = []string{
	"01/02/2006",
	"1/2/2006",
	"2006-01-02",
	"2006/01/02",
}

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

	colIndex, err := readChaseHeader(ctx, reader, dialectName, requiredCols)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return []model.ChaseTransaction{}, nil
		}
		return nil, err
	}

	transactions := make([]model.ChaseTransaction, 0, defaultTransactionsCapacity)

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

	return transactions, nil
}

func readChaseHeader(
	ctx context.Context,
	reader *csv.Reader,
	dialectName string,
	requiredCols [][]string,
) (map[string]int, error) {
	var header []string
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
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

	colIndex := make(map[string]int, len(header))
	for i, col := range header {
		cleanCol := strings.TrimPrefix(col, "\xef\xbb\xbf")
		colIndex[strings.ToLower(strings.TrimSpace(cleanCol))] = i
	}

	for _, req := range requiredCols {
		if _, ok := findColumn(colIndex, req...); !ok {
			return nil, fmt.Errorf("%w: dialect %s missing column %q", ErrMissingRequiredColumns, dialectName, req[0])
		}
	}

	return colIndex, nil
}

func parseAmount(amtRaw string, lineNum int) (float64, error) {
	amtStr := cleanNumeric(amtRaw)
	if amtStr == "" {
		return 0, fmt.Errorf("row %d: transaction amount is required", lineNum)
	}
	amount, err := strconv.ParseFloat(amtStr, 64)
	if err != nil {
		return 0, fmt.Errorf("row %d: invalid amount %q: %w", lineNum, amtStr, err)
	}
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, fmt.Errorf("row %d: invalid amount %q: non-finite number", lineNum, amtStr)
	}
	return amount, nil
}

func parseDescription(descRaw string, lineNum int) (string, error) {
	desc := strings.TrimSpace(descRaw)
	if desc == "" {
		return "", fmt.Errorf("row %d: transaction description is required", lineNum)
	}
	return desc, nil
}

func parseDate(val string) (time.Time, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return time.Time{}, errors.New("date value is empty")
	}

	for _, layout := range supportedDateLayouts {
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
	clean = strings.ReplaceAll(clean, "\u00a0", "")
	clean = strings.ReplaceAll(clean, "\u202f", "")
	clean = strings.ReplaceAll(clean, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.TrimSpace(clean)

	if strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")") {
		clean = "-" + clean[1:len(clean)-1]
	}
	clean = strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, clean)
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

func buildLineage(meta model.FileMetadata, lineNum int) model.SourceLineage {
	return model.SourceLineage{
		FileName:   meta.FileName,
		FileHash:   meta.FileHash,
		RowNumber:  lineNum,
		IngestedAt: meta.IngestedAt,
	}
}
