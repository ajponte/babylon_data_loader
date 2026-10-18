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
	"unicode"

	"babylon/dataloader/internal/domain/model"
)

const defaultTransactionsCapacity = 64

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

// isPlaceholderNumeric reports whether val represents an absent or non-numeric placeholder value.
// It detects empty fields, tabular null tokens ("n/a", "na", "#n/a", "none", "null", "nil", "-$"),
// and strings consisting entirely of ASCII or Unicode dashes (-, --, ---, –, —, −, ―).
func isPlaceholderNumeric(val string) bool {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return true
	}

	switch strings.ToLower(trimmed) {
	case "-", "--", "---", "n/a", "na", "#n/a", "none", "null", "nil", "-$":
		return true
	}

	hasDash := false
	for _, r := range trimmed {
		if r == '-' || r == '\u2013' || r == '\u2014' || r == '\u2212' || r == '\u2015' {
			hasDash = true
			continue
		}
		return false
	}
	return hasDash
}

// parseBalance parses an optional running balance field.
// It returns (nil, nil) if the field is empty or contains a placeholder dash/NA.
//
//nolint:nilnil // nil balance represents an intentionally absent optional value
func parseBalance(balRaw string, lineNum int) (*float64, error) {
	if isPlaceholderNumeric(balRaw) {
		return nil, nil
	}

	balStr := cleanNumeric(balRaw)
	if isPlaceholderNumeric(balStr) {
		return nil, nil
	}

	bal, err := strconv.ParseFloat(balStr, 64)
	if err != nil {
		return nil, fmt.Errorf("row %d: invalid balance %q: %w", lineNum, balStr, err)
	}
	if math.IsNaN(bal) || math.IsInf(bal, 0) {
		return nil, fmt.Errorf("row %d: invalid balance %q: non-finite number", lineNum, balStr)
	}
	return &bal, nil
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

	layouts := [...]string{
		"01/02/2006",
		"1/2/2006",
		"2006-01-02",
		"2006/01/02",
	}

	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format %q: expected MM/DD/YYYY or YYYY-MM-DD", val)
}

// cleanNumeric normalizes numeric strings by removing currency signs, formatting symbols,
// Unicode whitespace, and accounting parentheses without coercing placeholder values to zero.
func cleanNumeric(val string) string {
	clean := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, val)

	clean = strings.ReplaceAll(clean, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")

	if strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")") {
		clean = "-" + clean[1:len(clean)-1]
	}

	return clean
}

func findColumn(colIndex map[string]int, aliases ...string) (int, bool) {
	for _, alias := range aliases {
		normalized := strings.ToLower(strings.TrimSpace(alias))
		if normalized == "" {
			continue
		}
		if idx, ok := colIndex[normalized]; ok {
			return idx, true
		}
	}
	return -1, false
}

func getFieldValue(row []string, colIndex map[string]int, aliases ...string) string {
	idx, ok := findColumn(colIndex, aliases...)
	if !ok || idx < 0 || idx >= len(row) {
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

// rowReader encapsulates row values, header index, and physical line number
// to provide ergonomic, bounds-checked extraction without heap allocation.
type rowReader struct {
	row      []string
	colIndex map[string]int
	lineNum  int
}

// newRowReader constructs a stack-allocated rowReader value.
func newRowReader(row []string, colIndex map[string]int, lineNum int) rowReader {
	return rowReader{
		row:      row,
		colIndex: colIndex,
		lineNum:  lineNum,
	}
}

// get retrieves the trimmed value for the first matching column alias.
func (r rowReader) get(aliases ...string) string {
	return getFieldValue(r.row, r.colIndex, aliases...)
}

// parseDate extracts and validates a timestamp field from the specified column aliases.
// It returns an error attributed to the physical row number if the date is missing or invalid.
func (r rowReader) parseDate(aliases ...string) (time.Time, error) {
	dateStr := r.get(aliases...)
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("row %d: transaction date is required", r.lineNum)
	}
	t, err := parseDate(dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("row %d: %w", r.lineNum, err)
	}
	return t, nil
}

// parseDescription extracts and validates the description field.
func (r rowReader) parseDescription(aliases ...string) (string, error) {
	return parseDescription(r.get(aliases...), r.lineNum)
}

// parseAmount extracts and validates the transaction amount field.
func (r rowReader) parseAmount(aliases ...string) (float64, error) {
	return parseAmount(r.get(aliases...), r.lineNum)
}

// parseBalance extracts and validates an optional running balance field.
func (r rowReader) parseBalance(aliases ...string) (*float64, error) {
	return parseBalance(r.get(aliases...), r.lineNum)
}

// firstNonEmpty returns the first non-whitespace trimmed value among the supplied candidates.
// If all candidates are blank or whitespace-only, it returns an empty string.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
