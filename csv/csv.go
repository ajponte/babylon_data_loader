package csvparser

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

var (
	errTargetFileNotFound = errors.New("the valid target file was not found")
	errInvalidDataSource  = errors.New("data source is not valid")
	errProcessCsv         = errors.New("error while parsing CSV file")
)

func ValidFileNotFoundError(path string) error {
	return fmt.Errorf("%w, %s", errTargetFileNotFound, path)
}

func DataSourceParseError(dataSource string) error {
	return fmt.Errorf("%w, %s", errInvalidDataSource, dataSource)
}

func ProcessCsvError(filename string) error {
	return fmt.Errorf("%s, %w", filename, errProcessCsv)
}

// DefaultParser is a concrete implementation of the Parser interface.
type DefaultParser struct{}

// NewDefaultParser creates a new DefaultParser instance.
func NewDefaultParser() *DefaultParser {
	return &DefaultParser{}
}

// Parse reads a CSV file from a given path and returns the data.
func (p *DefaultParser) Parse(
	ctx context.Context,
	filePath string,
	dataSource string,
	accountID string,
) ([]map[string]string, int64, error) {
	return p.ParseStream(ctx, filePath, dataSource, accountID, nil)
}

// ParseStream reads a CSV file from a given path and executes row-by-row progress callbacks.
func (p *DefaultParser) ParseStream(
	ctx context.Context,
	filePath string,
	_ string,
	_ string,
	onProgress RowProgressCallback,
) ([]map[string]string, int64, error) {
	var totalRecords int64
	if onProgress != nil {
		var err error
		totalRecords, err = countRecords(ctx, filePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "no such file or directory") {
				return nil, 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
			}
			return nil, 0, fmt.Errorf("failed to count CSV records from file %s: %w", filePath, err)
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.Comma = ','

	colIndex, headerLen, err := createHeaderIndex(reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, nil // Handle empty file gracefully
		}
		return nil, 0, fmt.Errorf("failed to read CSV header from file %s: %w", filePath, err)
	}

	return p.readRecords(ctx, reader, filePath, colIndex, headerLen, totalRecords, onProgress)
}

func (p *DefaultParser) readRecords(
	ctx context.Context,
	reader *csv.Reader,
	filePath string,
	colIndex map[string]int,
	headerLen int,
	totalRecords int64,
	onProgress RowProgressCallback,
) ([]map[string]string, int64, error) {
	var documents []map[string]string
	var recordsProcessed int64

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, 0, ctxErr
		}

		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, fmt.Errorf("failed to read record from CSV in file %s: %w", filePath, readErr)
		}

		if isDisclaimerOrEmptyRow(record) {
			continue
		}

		if len(record) < headerLen {
			continue
		}

		line, _ := reader.FieldPos(0)
		doc := make(map[string]string)
		for key, idx := range colIndex {
			doc[key] = safeGet(record, idx)
		}
		doc["__row_number"] = strconv.Itoa(line)

		documents = append(documents, doc)
		recordsProcessed++

		if onProgress != nil {
			onProgress(RowProgress{
				CurrentRecord: recordsProcessed,
				TotalRecords:  totalRecords,
			})
		}
	}

	if len(documents) == 0 {
		return nil, 0, nil
	}

	return documents, recordsProcessed, nil
}

func createHeaderIndex(reader *csv.Reader) (map[string]int, int, error) {
	header, err := reader.Read()
	if err != nil {
		return nil, 0, err
	}
	colIndex := make(map[string]int)
	for i, col := range header {
		cleanCol := strings.TrimPrefix(col, "\xef\xbb\xbf")
		cleanCol = strings.TrimSpace(cleanCol)
		colIndex[strings.ToLower(cleanCol)] = i
	}
	return colIndex, len(header), nil
}

func countRecords(ctx context.Context, filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.Comma = ','

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}

	var count int64
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}

		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return 0, readErr
		}
		if isDisclaimerOrEmptyRow(record) {
			continue
		}
		if len(record) < len(header) {
			continue
		}
		count++
	}
	return count, nil
}

func isDisclaimerOrEmptyRow(record []string) bool {
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
		if allEmpty {
			return true
		}
	}
	lower := strings.ToLower(first)
	return strings.HasPrefix(lower, "the data and information") ||
		strings.HasPrefix(lower, "brokerage services") ||
		strings.HasPrefix(lower, "date downloaded")
}

// safeGet retrieves slice[index] safely.
func safeGet(slice []string, index int) string {
	if index < len(slice) {
		return slice[index]
	}

	return ""
}
