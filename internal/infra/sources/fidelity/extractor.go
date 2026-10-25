// Package fidelity implements the port.Extractor driving adapter for Fidelity portfolio position statement exports.
package fidelity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	_ "time/tzdata" // Embed timezone database to match legacy datalake/model/fidelity_parser.go:8.

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

var (
	// ErrNilReader indicates that a nil io.Reader was provided to Extract.
	ErrNilReader = errors.New("reader cannot be nil")
	// ErrMissingRequiredColumns indicates that the CSV header is missing mandatory columns.
	ErrMissingRequiredColumns = errors.New("fidelity CSV missing required header columns")
	// ErrInvalidDateDownloaded indicates that a Date downloaded footer line could not be parsed.
	ErrInvalidDateDownloaded = errors.New("invalid fidelity Date downloaded footer")
)

const (
	defaultPositionsCapacity = 64
	utf8BOMString            = "\xef\xbb\xbf"
	dateDownloadedKeyword    = "date downloaded"
	dateTrimCutset           = "\", \t\r\n"
	edtOffsetSeconds         = -4 * 3600 // Eastern Daylight Time UTC offset in seconds (satisfies mnd linter).
)

// Extractor implements port.Extractor for Fidelity portfolio positions exports.
type Extractor struct{}

// NewExtractor constructs a new Fidelity Extractor.
func NewExtractor() port.Extractor {
	return &Extractor{}
}

// SourceType returns the unique source identifier for Fidelity portfolio exports.
func (e *Extractor) SourceType() string {
	return "fidelity"
}

// Extract parses raw CSV data from r and returns an IngestionBatch of FidelityPosition entities.
func (e *Extractor) Extract(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) (*model.IngestionBatch, error) {
	if err := validateParams(ctx, r, meta); err != nil {
		return nil, err
	}

	reader, err := newCSVReader(r)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(ctx, reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return makeBatch(meta, []model.FidelityPosition{})
		}
		return nil, err
	}

	colIndex := buildColumnIndex(header)
	if valErr := validateHeaderSignatures(colIndex); valErr != nil {
		return nil, valErr
	}

	positions, err := parsePositions(ctx, reader, colIndex, meta)
	if err != nil {
		return nil, err
	}

	return makeBatch(meta, positions)
}

func validateParams(ctx context.Context, r io.Reader, meta model.FileMetadata) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("invalid file metadata: %w", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if r == nil {
		return ErrNilReader
	}
	return nil
}

func newCSVReader(r io.Reader) (*csv.Reader, error) {
	br := bufio.NewReader(r)
	if err := stripUTF8BOM(br); err != nil {
		return nil, err
	}

	reader := csv.NewReader(br)
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	reader.TrimLeadingSpace = true
	return reader, nil
}

func readHeader(ctx context.Context, reader *csv.Reader) ([]string, error) {
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read Fidelity CSV header: %w", err)
		}
		if !isRowEmpty(record) {
			return record, nil
		}
	}
}

func buildColumnIndex(header []string) map[string]int {
	colIndex := make(map[string]int, len(header))
	for i, col := range header {
		cleanCol := strings.TrimSpace(strings.TrimPrefix(col, utf8BOMString))
		colIndex[strings.ToLower(cleanCol)] = i
	}
	return colIndex
}

func validateHeaderSignatures(colIndex map[string]int) error {
	requiredSignatures := [][]string{
		{"symbol", "ticker"},
		{"current value", "market value", "current balance"},
	}
	for _, sig := range requiredSignatures {
		if _, ok := findColumn(colIndex, sig...); !ok {
			return fmt.Errorf("%w (expected one of %v)", ErrMissingRequiredColumns, sig)
		}
	}
	return nil
}

func parsePositions(
	ctx context.Context,
	reader *csv.Reader,
	colIndex map[string]int,
	meta model.FileMetadata,
) ([]model.FidelityPosition, error) {
	positions := make([]model.FidelityPosition, 0, defaultPositionsCapacity)
	parser := newRowParser(colIndex, meta)
	var asOfDate time.Time

	for {
		row, lineNum, err := readNextRow(ctx, reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		isFooter, footerErr := consumeFooterRow(row, lineNum, &asOfDate)
		if footerErr != nil {
			return nil, footerErr
		}
		if isFooter {
			continue
		}

		pos, ok, parseErr := parser.parse(row, lineNum)
		if parseErr != nil {
			return nil, parseErr
		}
		if ok {
			positions = append(positions, pos)
		}
	}

	return finalizePositions(positions, asOfDate)
}

func consumeFooterRow(row []string, lineNum int, asOfDate *time.Time) (bool, error) {
	footerField, isFooter := matchDateDownloadedField(row)
	if !isFooter {
		return false, nil
	}
	if asOfDate.IsZero() {
		parsedDate, dateErr := parseDateDownloaded(footerField)
		if dateErr != nil {
			return true, fmt.Errorf("row %d date downloaded footer: %w", lineNum, dateErr)
		}
		*asOfDate = parsedDate
	}
	return true, nil
}

func finalizePositions(positions []model.FidelityPosition, asOfDate time.Time) ([]model.FidelityPosition, error) {
	if len(positions) == 0 {
		return positions, nil
	}
	if asOfDate.IsZero() {
		return nil, fmt.Errorf("fidelity CSV missing Date downloaded footer: %w", model.ErrMissingAsOfDate)
	}
	for i := range positions {
		positions[i].AsOfDate = asOfDate
	}
	return positions, nil
}

func matchDateDownloadedField(row []string) (string, bool) {
	for i, field := range row {
		trimmed := strings.Trim(field, dateTrimCutset)
		if trimmed == "" {
			continue
		}
		if !hasDateDownloadedPrefix(trimmed) {
			return "", false
		}
		return joinNonEmptyFooterFields(row[i:]), true
	}
	return "", false
}

func hasDateDownloadedPrefix(s string) bool {
	if len(s) < len(dateDownloadedKeyword) {
		return false
	}
	if !strings.EqualFold(s[:len(dateDownloadedKeyword)], dateDownloadedKeyword) {
		return false
	}
	if len(s) == len(dateDownloadedKeyword) {
		return true
	}
	next := s[len(dateDownloadedKeyword)]
	return next == ' ' || next == '\t' || next == ':' || next == ','
}

func joinNonEmptyFooterFields(fields []string) string {
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		trimmed := strings.Trim(f, dateTrimCutset)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, " ")
}

// parseDateDownloaded is a behavior-identical port of legacy
// datalake/model.ParseFidelityDateDownloaded, returning the instant in UTC.
func parseDateDownloaded(line string) (time.Time, error) {
	rawDate, ok := stripDateDownloadedPrefix(line)
	if !ok || rawDate == "" {
		return time.Time{}, fmt.Errorf("%w: empty or missing date in %q", ErrInvalidDateDownloaded, line)
	}

	dateStr, isET := stripEasternSuffix(rawDate)
	dateStr = normalizeMeridiem(dateStr)
	dateStr = strings.Join(strings.Fields(dateStr), " ")
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("%w: empty date in %q", ErrInvalidDateDownloaded, line)
	}

	loc := resolveDateLocation(isET)
	return parseTimestampInLocation(dateStr, loc)
}

func parseTimestampInLocation(dateStr string, loc *time.Location) (time.Time, error) {
	parsed, err := time.ParseInLocation("Jan-02-2006 3:04 PM", dateStr, loc)
	if err != nil {
		var errSingle error
		parsed, errSingle = time.ParseInLocation("Jan-2-2006 3:04 PM", dateStr, loc)
		if errSingle != nil {
			return time.Time{}, fmt.Errorf("%w %q: %w", ErrInvalidDateDownloaded, dateStr, errSingle)
		}
	}
	return parsed.UTC(), nil
}

func stripDateDownloadedPrefix(line string) (string, bool) {
	clean := strings.Trim(line, dateTrimCutset)
	if !hasDateDownloadedPrefix(clean) {
		return "", false
	}
	rest := clean[len(dateDownloadedKeyword):]
	rest = strings.TrimLeft(rest, " \t:,")
	rest = strings.Trim(rest, dateTrimCutset)
	return rest, true
}

func stripEasternSuffix(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	upper := strings.ToUpper(trimmed)
	for _, suffix := range []string{"EDT", "EST", "ET"} {
		if strings.HasSuffix(upper, suffix) {
			without := strings.TrimSpace(trimmed[:len(trimmed)-len(suffix)])
			return strings.Trim(without, dateTrimCutset), true
		}
	}
	return trimmed, false
}

func normalizeMeridiem(s string) string {
	replacer := strings.NewReplacer(
		"p.m.", "PM",
		"a.m.", "AM",
		"P.M.", "PM",
		"A.M.", "AM",
		"p.m", "PM",
		"a.m", "AM",
		"P.M", "PM",
		"A.M", "AM",
		"pm", "PM",
		"am", "AM",
	)
	return replacer.Replace(s)
}

func resolveDateLocation(isET bool) *time.Location {
	if !isET {
		return time.UTC
	}
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return time.FixedZone("EDT", edtOffsetSeconds)
	}
	return loc
}

func readNextRow(ctx context.Context, reader *csv.Reader) ([]string, int, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, 0, ctxErr
	}

	row, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.EOF
		}
		return nil, 0, fmt.Errorf("failed to read Fidelity CSV row: %w", err)
	}

	lineNum, _ := reader.FieldPos(0)
	return row, lineNum, nil
}

func resolveBatchAccountID(fallbackID string, positions []model.FidelityPosition) string {
	if len(positions) == 0 {
		return fallbackID
	}
	firstID := positions[0].AccountID
	if firstID == "" {
		return fallbackID
	}
	for _, pos := range positions[1:] {
		if pos.AccountID != firstID {
			return fallbackID
		}
	}
	return firstID
}

func makeBatch(meta model.FileMetadata, positions []model.FidelityPosition) (*model.IngestionBatch, error) {
	batch := &model.IngestionBatch{
		Destination: model.DestFidelityPosition,
		AccountID:   resolveBatchAccountID(meta.AccountID, positions),
		SourceFile:  meta.FileName,
		FileHash:    meta.FileHash,
		IngestedAt:  meta.IngestedAt,
		RecordCount: len(positions),
		Payload:     positions,
	}

	if valErr := batch.Validate(); valErr != nil {
		return nil, fmt.Errorf("generated batch failed validation: %w", valErr)
	}

	return batch, nil
}

func stripUTF8BOM(br *bufio.Reader) error {
	bom := []byte(utf8BOMString)
	peekBytes, err := br.Peek(len(bom))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		return fmt.Errorf("failed to peek BOM header: %w", err)
	}
	if bytes.Equal(peekBytes, bom) {
		if _, discardErr := br.Discard(len(bom)); discardErr != nil {
			return fmt.Errorf("failed to discard BOM: %w", discardErr)
		}
	}
	return nil
}

func isRowEmpty(record []string) bool {
	if len(record) == 0 {
		return true
	}
	for _, field := range record {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}
