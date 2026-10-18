package fidelity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
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
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if r == nil {
		return nil, errors.New("reader cannot be nil")
	}

	br := bufio.NewReader(r)
	stripUTF8BOM(br)

	reader := csv.NewReader(br)
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return makeBatch(meta, []model.FidelityPosition{})
		}
		return nil, fmt.Errorf("failed to read Fidelity CSV header: %w", err)
	}

	colIndex := make(map[string]int)
	for i, col := range header {
		cleanCol := strings.TrimSpace(strings.TrimPrefix(col, "\xef\xbb\xbf"))
		colIndex[strings.ToLower(cleanCol)] = i
	}

	positions, err := parsePositions(ctx, reader, colIndex, meta)
	if err != nil {
		return nil, err
	}

	return makeBatch(meta, positions)
}

func parsePositions(
	ctx context.Context,
	reader *csv.Reader,
	colIndex map[string]int,
	meta model.FileMetadata,
) ([]model.FidelityPosition, error) {
	var positions []model.FidelityPosition
	rowIdx := 1 // header is row 1

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		row, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read Fidelity CSV row %d: %w", rowIdx+1, readErr)
		}
		rowIdx++

		if isFidelityDisclaimerOrEmpty(row) {
			continue
		}

		pos, parseErr := parseFidelityRow(row, colIndex, rowIdx, meta)
		if parseErr != nil {
			return nil, parseErr
		}

		positions = append(positions, pos)
	}

	if positions == nil {
		positions = []model.FidelityPosition{}
	}

	return positions, nil
}

func parseFidelityRow(
	row []string,
	colIndex map[string]int,
	rowIdx int,
	meta model.FileMetadata,
) (model.FidelityPosition, error) {
	accountID := getFieldValue(row, colIndex, "account number", "account id")
	if accountID == "" {
		accountID = meta.AccountID
	}

	accountName := getFieldValue(row, colIndex, "account name")
	symbol := getFieldValue(row, colIndex, "symbol")
	if symbol == "" {
		symbol = "CASH"
	}
	description := getFieldValue(row, colIndex, "description")

	qty, err := parseQuantity(getFieldValue(row, colIndex, "quantity"))
	if err != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d: %w", rowIdx, err)
	}

	lastPrice, err := parseCurrency(getFieldValue(row, colIndex, "last price"))
	if err != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d: %w", rowIdx, err)
	}

	currentVal, err := parseCurrency(getFieldValue(row, colIndex, "current value"))
	if err != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d: %w", rowIdx, err)
	}

	costBasis, err := parseCurrency(getFieldValue(row, colIndex, "cost basis total", "cost basis"))
	if err != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d: %w", rowIdx, err)
	}

	pos := model.FidelityPosition{
		Lineage: model.SourceLineage{
			FileName:   meta.FileName,
			FileHash:   meta.FileHash,
			RowNumber:  rowIdx,
			IngestedAt: meta.IngestedAt,
		},
		AccountID:    accountID,
		AccountName:  accountName,
		Symbol:       symbol,
		Description:  description,
		Quantity:     qty,
		LastPrice:    lastPrice,
		CurrentValue: currentVal,
		CostBasis:    costBasis,
	}

	if valErr := pos.Validate(); valErr != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d validation error: %w", rowIdx, valErr)
	}

	return pos, nil
}

func makeBatch(meta model.FileMetadata, positions []model.FidelityPosition) (*model.IngestionBatch, error) {
	batch := &model.IngestionBatch{
		Destination: model.DestFidelityPosition,
		AccountID:   meta.AccountID,
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

func stripUTF8BOM(br *bufio.Reader) {
	bom := []byte{0xef, 0xbb, 0xbf}
	peekBytes, err := br.Peek(len(bom))
	if err == nil && bytes.Equal(peekBytes, bom) {
		_, _ = br.Discard(len(bom))
	}
}

func isFidelityDisclaimerOrEmpty(record []string) bool {
	if len(record) == 0 {
		return true
	}
	first := strings.TrimSpace(record[0])
	if first == "" {
		allEmpty := true
		for _, f := range record {
			if strings.TrimSpace(f) != "" {
				allEmpty = false
				break
			}
		}
		return allEmpty
	}

	lower := strings.ToLower(first)
	return strings.HasPrefix(lower, "the data and information") ||
		strings.HasPrefix(lower, "brokerage services") ||
		strings.HasPrefix(lower, "date downloaded")
}

func parseCurrency(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") {
		return 0.0, nil
	}
	clean := strings.ReplaceAll(trimmed, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.ReplaceAll(clean, " ", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid currency value %q: %w", val, err)
	}
	return f, nil
}

func parseQuantity(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") {
		return 0.0, nil
	}
	clean := strings.ReplaceAll(trimmed, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.ReplaceAll(clean, " ", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid quantity value %q: %w", val, err)
	}
	return f, nil
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
