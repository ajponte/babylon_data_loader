package datalake

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"babylon/dataloader/datalake/model"
)

const (
	fidelityScanInitialBufferSize = 64 * 1024
	fidelityScanMaxBufferSize     = 10 * 1024 * 1024
)

// FidelityMapper encapsulates file provenance coordinates and provides pure
// in-memory transformation from raw CSV maps to typed FidelityPosition entities.
type FidelityMapper struct {
	fileName   string
	s3URI      string
	fileHash   string
	asOfDate   time.Time
	ingestedAt time.Time
}

// NewFidelityMapper constructs a FidelityMapper by executing required I/O operations
// (SHA-256 calculation and asOfDate footer extraction) against the local file on disk.
func NewFidelityMapper(
	ctx context.Context,
	filePath string,
	fileName string,
	s3URI string,
) (*FidelityMapper, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	fileHash, err := ComputeFileSHA256(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to compute sha256 for file %s: %w", filePath, err)
	}

	asOfDate, err := extractFidelityAsOfDate(ctx, filePath)
	if err != nil {
		return nil, err
	}

	return &FidelityMapper{
		fileName:   fileName,
		s3URI:      s3URI,
		fileHash:   fileHash,
		asOfDate:   asOfDate,
		ingestedAt: time.Now().UTC(),
	}, nil
}

// NewFidelityMapperWithMetadata creates a FidelityMapper with explicit metadata parameters.
// This factory enables hermetic unit testing without requiring physical files on disk.
func NewFidelityMapperWithMetadata(
	fileName string,
	s3URI string,
	fileHash string,
	asOfDate time.Time,
	ingestedAt time.Time,
) *FidelityMapper {
	return &FidelityMapper{
		fileName:   fileName,
		s3URI:      s3URI,
		fileHash:   fileHash,
		asOfDate:   asOfDate,
		ingestedAt: ingestedAt,
	}
}

// MapRecords transforms a slice of raw record maps into typed model.FidelityPosition slices.
// Operates purely in-memory with zero disk or network I/O.
func (m *FidelityMapper) MapRecords(rawRecords []map[string]string) ([]model.FidelityPosition, error) {
	if len(rawRecords) == 0 {
		return []model.FidelityPosition{}, nil
	}

	positions := make([]model.FidelityPosition, 0, len(rawRecords))
	for _, record := range rawRecords {
		pos, err := m.MapRecord(record)
		if err != nil {
			return nil, err
		}
		positions = append(positions, pos)
	}

	return positions, nil
}

// MapRecord transforms a single raw record map into a model.FidelityPosition with lineage.
// Operates purely in-memory with zero disk or network I/O.
func (m *FidelityMapper) MapRecord(record map[string]string) (model.FidelityPosition, error) {
	rowNumber := parseRowNumber(record)

	source := model.SourceLineage{
		S3URI:      m.s3URI,
		FileName:   m.fileName,
		RowNumber:  rowNumber,
		IngestedAt: m.ingestedAt,
		SHA256:     m.fileHash,
	}

	pos, mapErr := model.MapRawRecordToFidelityPosition(record, m.asOfDate, source)
	if mapErr != nil {
		return model.FidelityPosition{}, fmt.Errorf(
			"failed to map raw record to Fidelity position (row %d): %w",
			rowNumber,
			mapErr,
		)
	}

	return pos, nil
}

// parseRowNumber parses the 1-indexed row number injected by the streaming parser.
func parseRowNumber(record map[string]string) int64 {
	if record == nil {
		return 0
	}
	if rowStr, ok := record["__row_number"]; ok && rowStr != "" {
		if parsedRow, parseErr := strconv.ParseInt(rowStr, 10, 64); parseErr == nil {
			return parsedRow
		}
	}
	return 0
}

// extractFidelityAsOfDate scans the file for the "Date downloaded" footer marker.
func extractFidelityAsOfDate(ctx context.Context, filePath string) (time.Time, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return time.Time{}, ctxErr
	}

	file, err := os.Open(filePath)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to open file to extract Fidelity asOfDate: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, fidelityScanInitialBufferSize), fidelityScanMaxBufferSize)

	var foundDate time.Time
	var hasDate bool

	for scanner.Scan() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return time.Time{}, ctxErr
		}
		line := strings.TrimSpace(scanner.Text())
		clean := strings.Trim(line, "\", \t\r\n")
		if strings.HasPrefix(clean, "Date downloaded ") {
			parsedDate, parseErr := model.ParseFidelityDateDownloaded(line)
			if parseErr != nil {
				return time.Time{}, fmt.Errorf("failed to parse Fidelity date downloaded from %q: %w", line, parseErr)
			}
			foundDate = parsedDate.UTC()
			hasDate = true
			break
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return time.Time{}, fmt.Errorf("failed to scan file for Fidelity asOfDate: %w", scanErr)
	}

	if !hasDate {
		return time.Time{}, fmt.Errorf("no valid Fidelity date downloaded header/footer found in file %s", filePath)
	}

	return foundDate, nil
}
