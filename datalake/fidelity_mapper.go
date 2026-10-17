package datalake

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"babylon/dataloader/datalake/model"
)

const (
	fidelityScanInitialBufferSize = 64 * 1024
	fidelityScanMaxBufferSize     = 10 * 1024 * 1024
	fidelityTailSeekSize          = 64 * 1024
)

// ErrMissingDateFooter is returned when a Fidelity CSV file does not contain a recognizable
// "Date downloaded" metadata header or footer.
var ErrMissingDateFooter = errors.New("no valid Fidelity date downloaded header/footer found")

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
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to compute sha256 for file %s: %w", filePath, err)
	}

	asOfDate, err := extractFidelityAsOfDate(ctx, filePath)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to extract Fidelity asOfDate for file %s: %w", filePath, err)
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

// FileName returns the source file name.
func (m *FidelityMapper) FileName() string {
	if m == nil {
		return ""
	}
	return m.fileName
}

// S3URI returns the destination or source S3 URI for datalake storage.
func (m *FidelityMapper) S3URI() string {
	if m == nil {
		return ""
	}
	return m.s3URI
}

// FileHash returns the SHA-256 hex digest of the raw source file.
func (m *FidelityMapper) FileHash() string {
	if m == nil {
		return ""
	}
	return m.fileHash
}

// AsOfDate returns the portfolio snapshot timestamp parsed from the file footer.
func (m *FidelityMapper) AsOfDate() time.Time {
	if m == nil {
		return time.Time{}
	}
	return m.asOfDate
}

// IngestedAt returns the ingestion timestamp applied to mapped positions.
func (m *FidelityMapper) IngestedAt() time.Time {
	if m == nil {
		return time.Time{}
	}
	return m.ingestedAt
}

// MapRecords transforms a slice of raw record maps into typed model.FidelityPosition slices.
// Operates purely in-memory with zero disk or network I/O.
func (m *FidelityMapper) MapRecords(rawRecords []map[string]string) ([]model.FidelityPosition, error) {
	if m == nil {
		return nil, errors.New("cannot map records: FidelityMapper instance is nil")
	}
	return m.MapRecordsWithContext(context.Background(), rawRecords)
}

// MapRecordsWithContext transforms raw record maps into typed model.FidelityPosition slices
// while respecting context cancellation between record mappings.
func (m *FidelityMapper) MapRecordsWithContext(
	ctx context.Context,
	rawRecords []map[string]string,
) ([]model.FidelityPosition, error) {
	if m == nil {
		return nil, errors.New("cannot map records: FidelityMapper instance is nil")
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	if len(rawRecords) == 0 {
		return []model.FidelityPosition{}, nil
	}

	positions := make([]model.FidelityPosition, 0, len(rawRecords))
	for _, record := range rawRecords {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
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
	if m == nil {
		return model.FidelityPosition{}, errors.New("cannot map record: FidelityMapper instance is nil")
	}

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
		if rowNumber > 0 {
			return model.FidelityPosition{}, fmt.Errorf(
				"failed to map raw record to Fidelity position (row %d): %w",
				rowNumber,
				mapErr,
			)
		}
		return model.FidelityPosition{}, fmt.Errorf(
			"failed to map raw record to Fidelity position: %w",
			mapErr,
		)
	}

	if strings.TrimSpace(pos.Symbol) == "" {
		if rowNumber > 0 {
			return model.FidelityPosition{}, fmt.Errorf(
				"invalid Fidelity position (row %d): symbol is required",
				rowNumber,
			)
		}
		return model.FidelityPosition{}, errors.New("invalid Fidelity position: symbol is required")
	}

	return pos, nil
}

// parseRowNumber parses the 1-indexed row number injected by the streaming parser.
// Returns 0 if the row number is missing, non-integer, or <= 0.
func parseRowNumber(record map[string]string) int64 {
	if record == nil {
		return 0
	}
	if rowStr, ok := record["__row_number"]; ok && rowStr != "" {
		if parsedRow, parseErr := strconv.ParseInt(rowStr, 10, 64); parseErr == nil && parsedRow > 0 {
			return parsedRow
		}
	}
	return 0
}

// extractFidelityAsOfDate scans the file for the "Date downloaded" footer marker.
// It seeks to the last 64KB to locate the footer in O(1) time, falling back to a full
// scan from the beginning if not found in the tail.
func extractFidelityAsOfDate(ctx context.Context, filePath string) (time.Time, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return time.Time{}, ctxErr
	}

	file, err := os.Open(filePath)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to open file to extract Fidelity asOfDate: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to stat file %s to extract Fidelity asOfDate: %w", filePath, err)
	}

	fileSize := stat.Size()
	var offset int64
	if fileSize > fidelityTailSeekSize {
		offset = fileSize - fidelityTailSeekSize
	}

	if offset > 0 {
		if _, seekErr := file.Seek(offset, io.SeekStart); seekErr != nil {
			return time.Time{}, fmt.Errorf("failed to seek to tail of file %s: %w", filePath, seekErr)
		}
	}

	foundDate, found, scanErr := scanForFidelityDate(ctx, file)
	if scanErr != nil {
		return time.Time{}, scanErr
	}
	if found {
		return foundDate, nil
	}

	// Fallback: If not found in tail and we started at offset > 0, rewind and scan whole file from start.
	if offset > 0 {
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return time.Time{}, fmt.Errorf("failed to rewind file %s: %w", filePath, seekErr)
		}

		foundDate, found, scanErr = scanForFidelityDate(ctx, file)
		if scanErr != nil {
			return time.Time{}, scanErr
		}
		if found {
			return foundDate, nil
		}
	}

	return time.Time{}, fmt.Errorf("%w in file %s", ErrMissingDateFooter, filePath)
}

func scanForFidelityDate(ctx context.Context, r io.Reader) (time.Time, bool, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, fidelityScanInitialBufferSize), fidelityScanMaxBufferSize)

	prefix := []byte("Date downloaded ")
	for scanner.Scan() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return time.Time{}, false, ctxErr
		}
		cleanBytes := bytes.Trim(scanner.Bytes(), "\", \t\r\n")
		if bytes.HasPrefix(cleanBytes, prefix) {
			clean := string(cleanBytes)
			parsedDate, parseErr := model.ParseFidelityDateDownloaded(clean)
			if parseErr != nil {
				return time.Time{}, false, fmt.Errorf(
					"failed to parse Fidelity date downloaded from %q: %w",
					clean,
					parseErr,
				)
			}
			return parsedDate.UTC(), true, nil
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return time.Time{}, false, fmt.Errorf("failed to scan file for Fidelity asOfDate: %w", scanErr)
	}

	return time.Time{}, false, nil
}
