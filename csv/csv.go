package csv

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	errTargetFileNotFound = errors.New("the valid target file was found")
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
	_ context.Context,
	filePath string,
	_ string,
	_ string,
) ([]map[string]string, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.Comma = ','

	// Read header and create column index map
	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, nil // Handle empty file gracefully
		}
		return nil, 0, fmt.Errorf("failed to read CSV header from file %s: %w", filePath, err)
	}
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[strings.ToLower(col)] = i
	}

	var documents []map[string]string

	var recordsProcessed int64

	for {
		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, fmt.Errorf("failed to read record from CSV in file %s: %w", filePath, readErr)
		}

		if len(record) < len(header) {
			continue
		}

		doc := make(map[string]string)
		for key, idx := range colIndex {
			doc[key] = safeGet(record, idx)
		}

		documents = append(documents, doc)
		recordsProcessed++
	}

	if len(documents) == 0 {
		return nil, 0, nil
	}

	return documents, recordsProcessed, nil
}

// safeGet retrieves slice[index] safely.
func safeGet(slice []string, index int) string {
	if index < len(slice) {
		return slice[index]
	}

	return ""
}
