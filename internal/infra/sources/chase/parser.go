package chase

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"

	"babylon/dataloader/internal/domain/model"
)

const (
	defaultTransactionsCapacity = 64

	// utf8BOM is the UTF-8 byte order mark that Excel's "CSV UTF-8" export writes.
	utf8BOM = "\xef\xbb\xbf"

	// thousandsGroupSize is the digit count between thousands separators.
	thousandsGroupSize = 3
	// maxFractionDigits is the maximum number of decimal places in a currency amount.
	maxFractionDigits = 2
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
	br, err := skipUTF8BOM(r)
	if err != nil {
		return nil, handleReadError(ctx, dialectName, "header", err)
	}
	reader := newChaseCSVReader(br)

	colIndex, err := readChaseHeader(ctx, reader, dialectName, requiredCols)
	if err != nil {
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
			return nil, handleReadError(ctx, dialectName, "row", readErr)
		}

		lineNum, _ := reader.FieldPos(0)

		if isRowEmpty(row) {
			continue
		}

		tx, parseErr := parseRow(row, colIndex, lineNum, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidRow, parseErr)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// skipUTF8BOM discards a leading UTF-8 byte order mark from the stream. It must run before
// CSV tokenization, because encoding/csv treats a BOM-prefixed quoted field as unquoted and
// rejects it under strict quoting.
func skipUTF8BOM(r io.Reader) (*bufio.Reader, error) {
	br := bufio.NewReader(r)
	prefix, err := br.Peek(len(utf8BOM))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if string(prefix) == utf8BOM {
		_, _ = br.Discard(len(utf8BOM))
	}
	return br, nil
}

// newChaseCSVReader returns a CSV reader with the settings shared by the classifier and the
// parser, so both stages tokenize a statement identically.
func newChaseCSVReader(r io.Reader) *csv.Reader {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	reader.TrimLeadingSpace = true
	return reader
}

// readHeaderRecord returns the first CSV record that has a non-blank field. The classifier and
// the parser both locate the header through this function.
func readHeaderRecord(ctx context.Context, reader *csv.Reader) ([]string, error) {
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		if !isRowEmpty(record) {
			return record, nil
		}
	}
}

func readChaseHeader(
	ctx context.Context,
	reader *csv.Reader,
	dialectName string,
	requiredCols [][]string,
) (map[string]int, error) {
	header, err := readHeaderRecord(ctx, reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: dialect %s has no header row", ErrMissingRequiredColumns, dialectName)
		}
		return nil, handleReadError(ctx, dialectName, "header", err)
	}

	colIndex, err := indexHeader(header, dialectName, requiredCols)
	if err != nil {
		return nil, err
	}

	for _, req := range requiredCols {
		if _, ok := findColumn(colIndex, req...); !ok {
			return nil, fmt.Errorf("%w: dialect %s missing column %q", ErrMissingRequiredColumns, dialectName, req[0])
		}
	}

	return colIndex, nil
}

// indexHeader maps each normalized, non-blank column name to its position. A column containing
// a line break means the file uses CR-only line endings, which encoding/csv does not treat as
// record terminators, so the whole file was read as one header record. A duplicate required
// column would bind ambiguously and is rejected; for other duplicates the first one wins.
func indexHeader(header []string, dialectName string, requiredCols [][]string) (map[string]int, error) {
	colIndex := make(map[string]int, len(header))
	for i, col := range header {
		if strings.ContainsAny(col, "\r\n") {
			return nil, fmt.Errorf(
				"%w: dialect %s column %d contains an embedded line break (CR-only line endings?)",
				ErrMalformedHeader, dialectName, i+1,
			)
		}
		name := normalizeColumn(col)
		if name == "" {
			continue
		}
		if _, dup := colIndex[name]; dup {
			if isRequiredColumn(name, requiredCols) {
				return nil, fmt.Errorf("%w: dialect %s column %q", ErrDuplicateColumn, dialectName, name)
			}
			continue
		}
		colIndex[name] = i
	}
	return colIndex, nil
}

func isRequiredColumn(name string, requiredCols [][]string) bool {
	for _, aliases := range requiredCols {
		for _, alias := range aliases {
			if name == alias {
				return true
			}
		}
	}
	return false
}

// normalizeColumn returns the canonical form used to match header columns and aliases.
func normalizeColumn(col string) string {
	return strings.ToLower(strings.TrimSpace(col))
}

func handleReadError(ctx context.Context, dialectName, target string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var parseErr *csv.ParseError
	if errors.As(err, &parseErr) {
		return fmt.Errorf("%w: dialect %s %s: %w", ErrMalformedCSV, dialectName, target, err)
	}
	return fmt.Errorf("failed to read Chase %s CSV %s: %w", dialectName, target, err)
}

func parseAmount(amtRaw string, lineNum int) (float64, error) {
	if strings.TrimSpace(amtRaw) == "" {
		return 0, fmt.Errorf("row %d: transaction amount is required: %w", lineNum, model.ErrInvalidAmount)
	}
	amount, ok := parseCurrency(amtRaw)
	if !ok {
		return 0, fmt.Errorf("row %d: invalid amount %q: %w", lineNum, amtRaw, model.ErrInvalidAmount)
	}
	return amount, nil
}

// isPlaceholderNumeric reports whether val represents an absent or non-numeric placeholder value.
// It detects empty fields, tabular null tokens ("n/a", "na", "#n/a", "none", "null", "nil", "-$"),
// and strings consisting entirely of ASCII or Unicode dashes (see isDashRune).
func isPlaceholderNumeric(val string) bool {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return true
	}

	switch strings.ToLower(trimmed) {
	case "-", "--", "---", "n/a", "na", "#n/a", "none", "null", "nil", "-$":
		return true
	}

	for _, r := range trimmed {
		if !isDashRune(r) {
			return false
		}
	}
	return true
}

// isDashRune reports whether r is the ASCII hyphen-minus or a Unicode minus or dash that
// spreadsheets and autocorrect substitute for it.
func isDashRune(r rune) bool {
	switch r {
	case '-', '\u2012', '\u2013', '\u2014', '\u2015', '\u2212', '\ufe63', '\uff0d':
		return true
	default:
		return false
	}
}

// parseBalance parses an optional running balance field.
// It returns (nil, nil) if the field is empty or contains a placeholder dash/NA.
//
//nolint:nilnil // nil balance represents an intentionally absent optional value
func parseBalance(balRaw string, lineNum int) (*float64, error) {
	if isPlaceholderNumeric(balRaw) {
		return nil, nil
	}

	bal, ok := parseCurrency(balRaw)
	if !ok {
		if isPlaceholderNumeric(strings.ReplaceAll(balRaw, "$", "")) {
			return nil, nil
		}
		return nil, fmt.Errorf("row %d: invalid balance %q: %w", lineNum, balRaw, model.ErrInvalidBalance)
	}
	return &bal, nil
}

func parseDescription(descRaw string, lineNum int) (string, error) {
	desc := strings.TrimSpace(descRaw)
	if desc == "" {
		return "", fmt.Errorf("row %d: %w", lineNum, model.ErrMissingDescription)
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

// parseCurrency converts a currency-formatted value to a float64. It reports false for any value
// that cleanNumeric rejects, so strconv.ParseFloat never sees Go float syntax such as hex
// mantissas, exponents, digit-separator underscores, NaN, or Inf.
func parseCurrency(raw string) (float64, bool) {
	digits, negative, ok := cleanNumeric(raw)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseFloat(digits, 64)
	if err != nil {
		// Only out-of-range magnitudes reach here; the grammar admits nothing else ParseFloat rejects.
		return 0, false
	}
	if negative {
		value = -value
	}
	return value, true
}

// cleanNumeric normalizes a currency-formatted value into an unsigned, strconv.ParseFloat-safe
// decimal and its sign. It drops Unicode whitespace, maps Unicode minus and dash variants to '-',
// and then accepts only a strict currency shape: an optional leading sign or accounting
// parentheses, an optional '$' on either side of the sign or inside the parentheses, digits with
// commas only between groups of three, and at most two decimal places. ok is false for any other
// shape, including trailing-minus negatives such as "4.50-", rather than coercing placeholder
// values to zero. Returning the sign separately avoids allocating a re-signed string per value.
func cleanNumeric(val string) (string, bool, bool) {
	clean := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		if isDashRune(r) {
			return '-'
		}
		return r
	}, val)

	negative := false
	clean = strings.TrimPrefix(clean, "$")
	switch {
	case strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")"):
		negative = true
		clean = strings.TrimPrefix(clean[1:len(clean)-1], "$")
	case strings.HasPrefix(clean, "-"), strings.HasPrefix(clean, "+"):
		negative = clean[0] == '-'
		clean = strings.TrimPrefix(clean[1:], "$")
	}

	digits, ok := stripThousandsSeparators(clean)
	if !ok {
		return "", false, false
	}
	return digits, negative, true
}

// stripThousandsSeparators validates an unsigned decimal whose integer part is either plain
// digits or comma-separated groups of three, with at most maxFractionDigits decimal places,
// and returns it without the commas.
func stripThousandsSeparators(s string) (string, bool) {
	intPart, frac, hasFrac := strings.Cut(s, ".")
	if hasFrac && (len(frac) > maxFractionDigits || !isASCIIDigits(frac)) {
		return "", false
	}

	lead, rest, grouped := strings.Cut(intPart, ",")
	if !isASCIIDigits(lead) || (grouped && len(lead) > thousandsGroupSize) {
		return "", false
	}
	for grouped {
		var group string
		group, rest, grouped = strings.Cut(rest, ",")
		if len(group) != thousandsGroupSize || !isASCIIDigits(group) {
			return "", false
		}
	}

	if !strings.Contains(intPart, ",") {
		return s, true
	}
	return strings.ReplaceAll(s, ",", ""), true
}

// isASCIIDigits reports whether s is non-empty and consists only of the digits 0-9.
func isASCIIDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func findColumn(colIndex map[string]int, aliases ...string) (int, bool) {
	for _, alias := range aliases {
		normalized := normalizeColumn(alias)
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
		return time.Time{}, fmt.Errorf("row %d: %w", r.lineNum, model.ErrMissingPostingDate)
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
