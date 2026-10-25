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
)

const (
	defaultPositionsCapacity = 64
	utf8BOMString            = "\xef\xbb\xbf"
	dateDownloadedPrefix     = "Date downloaded "
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

		if footerField, isFooter := matchDateDownloadedField(row); isFooter {
			if asOfDate.IsZero() {
				parsedDate, dateErr := parseDateDownloaded(footerField)
				if dateErr != nil {
					return nil, fmt.Errorf("row %d date downloaded footer: %w", lineNum, dateErr)
				}
				asOfDate = parsedDate
			}
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
	for _, field := range row {
		trimmed := strings.Trim(field, dateTrimCutset)
		if trimmed != "" {
			return trimmed, strings.HasPrefix(trimmed, dateDownloadedPrefix)
		}
	}
	return "", false
}

// parseDateDownloaded is a behavior-identical port of legacy
// datalake/model.ParseFidelityDateDownloaded, returning the instant in UTC.
func parseDateDownloaded(line string) (time.Time, error) {
	clean := strings.Trim(line, dateTrimCutset)
	dateStr := strings.TrimPrefix(clean, dateDownloadedPrefix)
	dateStr = strings.Trim(dateStr, dateTrimCutset)
	dateStr = normalizeMeridiem(dateStr)

	isET := strings.HasSuffix(strings.TrimSpace(dateStr), "ET")
	dateStr = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(dateStr), "ET"))

	loc := resolveDateLocation(isET)

	parsed, err := time.ParseInLocation("Jan-02-2006 3:04 PM", dateStr, loc)
	if err != nil {
		var errSingle error
		parsed, errSingle = time.ParseInLocation("Jan-2-2006 3:04 PM", dateStr, loc)
		if errSingle != nil {
			return time.Time{}, fmt.Errorf("failed to parse date %q: %w", dateStr, err)
		}
	}
	return parsed.UTC(), nil
}

func normalizeMeridiem(s string) string {
	s = strings.ReplaceAll(s, "p.m.", "PM")
	s = strings.ReplaceAll(s, "a.m.", "AM")
	s = strings.ReplaceAll(s, "p.m", "PM")
	s = strings.ReplaceAll(s, "a.m", "AM")
	return s
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
