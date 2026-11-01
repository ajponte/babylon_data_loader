package synthetic

import (
	"bufio"
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
	"babylon/dataloader/internal/domain/port"
)

var (
	// ErrNilReader indicates that a nil io.Reader was supplied.
	ErrNilReader = errors.New("synthetic csv: reader cannot be nil")

	// ErrMissingHeader indicates that the CSV stream contains no header row.
	ErrMissingHeader = errors.New("synthetic csv: header row is missing")

	// ErrMissingRequiredColumns indicates that one or more required columns are missing from the header.
	ErrMissingRequiredColumns = errors.New("synthetic csv: missing required header columns")

	// ErrMalformedCSV indicates a CSV syntax error wrapping the underlying *csv.ParseError.
	ErrMalformedCSV = errors.New("synthetic csv: malformed CSV")

	// ErrInvalidRow indicates that a data row was rejected, wrapping the domain sentinel and line number.
	ErrInvalidRow = errors.New("synthetic csv: row is invalid")
)

const (
	// csvPeekSize defines the bounded inspection buffer size (4 KiB) for BOM detection and header sanity.
	csvPeekSize = 4 << 10

	utf8BOM = "\xef\xbb\xbf"

	initialBatchCapacity = 64
	thousandsGroupSize   = 3
	maxFractionDigits    = 2
)

var _ port.Extractor = (*CSVExtractor)(nil)

// CSVExtractor implements port.Extractor for legacy synthetic statement CSV files.
type CSVExtractor struct{}

// NewCSVExtractor constructs a new synthetic CSVExtractor.
func NewCSVExtractor() *CSVExtractor {
	return &CSVExtractor{}
}

// SourceType returns the unique source identifier for synthetic statements.
func (e *CSVExtractor) SourceType() string {
	return "synthetic"
}

// Extract parses raw CSV synthetic transaction data from r and returns an IngestionBatch.
func (e *CSVExtractor) Extract(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) (*model.IngestionBatch, error) {
	ctx, err := validateExtractParams(ctx, r, meta)
	if err != nil {
		return nil, err
	}

	br := bufio.NewReaderSize(r, csvPeekSize)
	if bomErr := stripBOM(br); bomErr != nil {
		return nil, handleStreamErr(ctx, bomErr)
	}

	reader := csv.NewReader(br)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	reader.TrimLeadingSpace = true

	indices, err := readAndIndexHeader(ctx, reader)
	if err != nil {
		return nil, err
	}

	records, err := parseRows(ctx, reader, indices, meta)
	if err != nil {
		return nil, err
	}

	batch := &model.IngestionBatch{
		Destination: model.DestSynthetic,
		AccountID:   meta.AccountID,
		SourceFile:  meta.FileName,
		FileHash:    meta.FileHash,
		IngestedAt:  meta.IngestedAt,
		RecordCount: len(records),
		Payload:     records,
	}

	if valErr := batch.Validate(); valErr != nil {
		return nil, fmt.Errorf("synthetic batch validation failed: %w", valErr)
	}

	return batch, nil
}

func validateExtractParams(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if r == nil {
		return nil, ErrNilReader
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	return ctx, nil
}

func stripBOM(br *bufio.Reader) error {
	peek, err := br.Peek(len(utf8BOM))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		return err
	}
	if string(peek) == utf8BOM {
		_, _ = br.Discard(len(utf8BOM))
	}
	return nil
}

func handleStreamErr(ctx context.Context, err error) error {
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
		return fmt.Errorf("%w: %w", ErrMalformedCSV, err)
	}
	return err
}

type colIndices struct {
	postingDate    int
	description    int
	amount         int
	details        int
	category       int
	txType         int
	balance        int
	checkOrSlipNum int
}

func newColIndices() colIndices {
	return colIndices{
		postingDate:    -1,
		description:    -1,
		amount:         -1,
		details:        -1,
		category:       -1,
		txType:         -1,
		balance:        -1,
		checkOrSlipNum: -1,
	}
}

func readAndIndexHeader(ctx context.Context, reader *csv.Reader) (colIndices, error) {
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return colIndices{}, ctxErr
		}
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return colIndices{}, ErrMissingHeader
			}
			return colIndices{}, handleStreamErr(ctx, err)
		}
		if isRowEmpty(record) {
			continue
		}
		return indexHeader(record)
	}
}

func indexHeader(record []string) (colIndices, error) {
	indices := newColIndices()
	for i, col := range record {
		switch normalizeHeader(col) {
		case "posting date":
			indices.postingDate = i
		case "description":
			indices.description = i
		case "amount":
			indices.amount = i
		case "details":
			indices.details = i
		case "category":
			indices.category = i
		case "type":
			indices.txType = i
		case "balance":
			indices.balance = i
		case "check or slip #", "check or slip num":
			indices.checkOrSlipNum = i
		}
	}

	var missing []string
	if indices.postingDate == -1 {
		missing = append(missing, "Posting Date")
	}
	if indices.description == -1 {
		missing = append(missing, "Description")
	}
	if indices.amount == -1 {
		missing = append(missing, "Amount")
	}
	if len(missing) > 0 {
		return colIndices{}, fmt.Errorf("%w: %s", ErrMissingRequiredColumns, strings.Join(missing, ", "))
	}

	return indices, nil
}

func parseRows(
	ctx context.Context,
	reader *csv.Reader,
	indices colIndices,
	meta model.FileMetadata,
) ([]model.ChaseTransaction, error) {
	records := make([]model.ChaseTransaction, 0, initialBatchCapacity)

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		row, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, handleStreamErr(ctx, err)
		}

		lineNum, _ := reader.FieldPos(0)
		if isRowEmpty(row) {
			continue
		}

		tx, parseErr := parseSyntheticRow(row, indices, lineNum, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("%w (line %d): %w", ErrInvalidRow, lineNum, parseErr)
		}

		records = append(records, tx)
	}

	return records, nil
}

func parseSyntheticRow(
	row []string,
	indices colIndices,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	postingDate, err := parseDate(getField(row, indices.postingDate))
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	desc, err := parseDescription(getField(row, indices.description))
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	amount, err := parseAmount(getField(row, indices.amount))
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	balance, err := parseBalance(getField(row, indices.balance))
	if err != nil {
		return model.ChaseTransaction{}, err
	}

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
		Details:        strings.TrimSpace(getField(row, indices.details)),
		Amount:         amount,
		Type:           strings.TrimSpace(getField(row, indices.txType)),
		Balance:        balance,
		CheckOrSlipNum: strings.TrimSpace(getField(row, indices.checkOrSlipNum)),
		Category:       strings.TrimSpace(getField(row, indices.category)),
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, valErr
	}

	return tx, nil
}

func parseDate(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, model.ErrMissingPostingDate
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

	return time.Time{}, fmt.Errorf("invalid date format %q: %w", raw, model.ErrMissingPostingDate)
}

func parseDescription(raw string) (string, error) {
	desc := strings.TrimSpace(raw)
	if desc == "" {
		return "", model.ErrMissingDescription
	}
	return desc, nil
}

func parseAmount(raw string) (float64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("amount is required: %w", model.ErrInvalidAmount)
	}
	amt, ok := parseCurrency(trimmed)
	if !ok {
		return 0, fmt.Errorf("invalid amount %q: %w", raw, model.ErrInvalidAmount)
	}
	return amt, nil
}

//nolint:nilnil // nil balance represents an intentionally absent optional value.
func parseBalance(raw string) (*float64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || isPlaceholderNumeric(trimmed) {
		return nil, nil
	}
	bal, ok := parseCurrency(trimmed)
	if !ok {
		return nil, fmt.Errorf("invalid balance %q: %w", raw, model.ErrInvalidBalance)
	}
	return &bal, nil
}

func parseCurrency(raw string) (float64, bool) {
	digits, negative, ok := cleanNumeric(raw)
	if !ok {
		return 0, false
	}
	val, err := strconv.ParseFloat(digits, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, false
	}
	if negative {
		val = -val
	}
	return val, true
}

func getField(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return row[idx]
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

func normalizeHeader(col string) string {
	return strings.ToLower(strings.TrimSpace(col))
}

func isPlaceholderNumeric(val string) bool {
	switch strings.ToLower(val) {
	case "-", "--", "---", "n/a", "na", "#n/a", "none", "null", "nil":
		return true
	default:
		return false
	}
}

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

func stripThousandsSeparators(s string) (string, bool) {
	if s == "" {
		return "", false
	}
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

func isDashRune(r rune) bool {
	switch r {
	case '-', '\u2012', '\u2013', '\u2014', '\u2015', '\u2212', '\ufe63', '\uff0d':
		return true
	default:
		return false
	}
}
