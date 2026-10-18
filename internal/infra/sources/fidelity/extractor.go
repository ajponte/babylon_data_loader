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

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

var (
	// ErrNilReader indicates that a nil io.Reader was provided to Extract.
	ErrNilReader = errors.New("reader cannot be nil")
	// ErrMissingRequiredColumns indicates that the CSV header is missing mandatory columns.
	ErrMissingRequiredColumns = errors.New("fidelity CSV missing required header columns")
)

const defaultPositionsCapacity = 64

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
	if err := e.validateParams(ctx, r, meta); err != nil {
		return nil, err
	}

	reader, err := e.newCSVReader(r)
	if err != nil {
		return nil, err
	}

	header, err := e.readHeader(ctx, reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return e.makeBatch(meta, []model.FidelityPosition{})
		}
		return nil, err
	}

	colIndex, err := e.parseAndValidateHeader(header)
	if err != nil {
		return nil, err
	}

	positions, err := e.parsePositions(ctx, reader, colIndex, meta)
	if err != nil {
		return nil, err
	}

	return e.makeBatch(meta, positions)
}

func (e *Extractor) validateParams(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) error {
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

func (e *Extractor) newCSVReader(r io.Reader) (*csv.Reader, error) {
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

func (e *Extractor) readHeader(
	ctx context.Context,
	reader *csv.Reader,
) ([]string, error) {
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

func (e *Extractor) parseAndValidateHeader(header []string) (map[string]int, error) {
	colIndex := make(map[string]int, len(header))
	for i, col := range header {
		cleanCol := strings.TrimSpace(strings.TrimPrefix(col, "\xef\xbb\xbf"))
		colIndex[strings.ToLower(cleanCol)] = i
	}

	// Enforce required column signatures for Fidelity portfolio positions
	requiredSignatures := [][]string{
		{"symbol", "ticker"},
		{"current value", "market value", "current balance", "last price", "quantity"},
	}

	for _, sig := range requiredSignatures {
		if _, ok := findColumn(colIndex, sig...); !ok {
			return nil, fmt.Errorf("%w (expected one of %v)", ErrMissingRequiredColumns, sig)
		}
	}

	return colIndex, nil
}

func (e *Extractor) parsePositions(
	ctx context.Context,
	reader *csv.Reader,
	colIndex map[string]int,
	meta model.FileMetadata,
) ([]model.FidelityPosition, error) {
	positions := make([]model.FidelityPosition, 0, defaultPositionsCapacity)
	parser := newRowParser(colIndex, meta)

	for {
		row, lineNum, eof, err := e.readNextRow(ctx, reader)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}

		pos, ok, parseErr := parser.parse(row, lineNum)
		if parseErr != nil {
			return nil, parseErr
		}
		if ok {
			positions = append(positions, pos)
		}
	}

	return positions, nil
}

func (e *Extractor) readNextRow(
	ctx context.Context,
	reader *csv.Reader,
) ([]string, int, bool, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, 0, false, ctxErr
	}

	row, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, true, nil
		}
		return nil, 0, false, fmt.Errorf("failed to read Fidelity CSV row: %w", err)
	}

	lineNum, _ := reader.FieldPos(0)
	return row, lineNum, false, nil
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

func (e *Extractor) makeBatch(
	meta model.FileMetadata,
	positions []model.FidelityPosition,
) (*model.IngestionBatch, error) {
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
	bom := []byte{0xef, 0xbb, 0xbf}
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
