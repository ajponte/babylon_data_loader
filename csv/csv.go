package csv

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	bcontext "babylon/dataloader/appcontext"
)

// Data represents a single row from the CSV file.
type Data struct {
	Details        string  `bson:"Details"`
	PostingDate    string  `bson:"PostingDate"`
	Description    string  `bson:"Description"`
	Amount         float64 `bson:"Amount"`
	Type           string  `bson:"Type"`
	Balance        float64 `bson:"Balance"`
	CheckOrSlipNum string  `bson:"CheckOrSlipNum"`
}

// SyncLog represents a record in the dataSync collection.
type SyncLog struct {
	CollectionName  string    `bson:"collection_name"`
	SyncTimestamp   time.Time `bson:"sync_timestamp"`
	RecordsUploaded int64     `bson:"records_uploaded"`
}

var errTargetFileNotFound = errors.New("the valid target file was found")
var errInvalidDataSource = errors.New("data source is not valid")
var errProcessCsv = errors.New("error while parsing CSV file")

func ValidFileNotFoundError(path string) error {
	return fmt.Errorf("%w, %s", errTargetFileNotFound, path)
}

func DataSourceParseError(dataSource string) error {
	return fmt.Errorf("%w, %s", errInvalidDataSource, dataSource)
}

func ProcessCsvError(filename string) error {
	return fmt.Errorf("%s, %w", filename, errProcessCsv)
}

// ParseCSV reads a CSV file from a given path and returns the data.
//
//nolint:gocognit,funlen
func ParseCSV(
	ctx context.Context,
	filePath string,
	dataSource string,
) ([]Data, string, int64, error) {
	// Retrieve the logger from the context at the start of the function.
	logger := bcontext.LoggerFromContext(ctx)
	logger.InfoContext(
		ctx,
		"Parsing data from csv", "filePath",
		filePath, "dataSource", dataSource,
	)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.Comma = ','

	// Read header
	_, err = reader.Read()
	if err != nil {
		// If there's an error reading the header, it could be an empty file.
		// Return nil documents and nil error if EOF, otherwise return the error.
		if errors.Is(err, io.EOF) {
			return nil, "", 0, nil
		}
		return nil, "", 0, fmt.Errorf("failed to read CSV header from file %s: %w", filePath, err)
	}

	var documents []Data
	var finalCollectionName string // Store the first valid collection name encountered
	var recordsProcessed int64

	// Mak number of columns required per record.
	var maxColumns = 4

	for {
		logger.DebugContext(ctx, "Reading new record from CSV")
		record, readErr := reader.Read() // Renamed err to readErr
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, "", 0, fmt.Errorf("failed to read record from CSV in file %s: %w", filePath, readErr)
		}

		if len(record) < maxColumns {
			logger.WarnContext(ctx, "Skipping invalid record", "reason", "less than 4 columns", "file", filePath)

			continue
		}

		postingDateStr := record[1]

		parsedDate, parseErr := time.Parse("01/02/2006", postingDateStr) // Renamed err to parseErr
		if parseErr != nil {
			logger.WarnContext(ctx, "skipping record with invalid date format '%s': %v", postingDateStr, parseErr)

			continue
		}

		currentCollectionName := fmt.Sprintf("%s-data-%s", dataSource, parsedDate.Format("2006-01-02"))
		if finalCollectionName == "" {
			finalCollectionName = currentCollectionName
		}

		amount, convErr := strconv.ParseFloat(record[3], 64) // Renamed _ to convErr
		if convErr != nil {
			logger.WarnContext(ctx, "skipping record with invalid amount format '%s': %v", record[3], convErr)
			continue
		}

		var minRecords = 5

		balance := 0.0
		if len(record) > minRecords {
			parsedBalance, balanceConvErr := strconv.ParseFloat(record[5], 64) // Renamed convErr to balanceConvErr
			if balanceConvErr != nil {
				logger.WarnContext(
					ctx, "skipping record with invalid balance format '%s': %v",
					record[5], balanceConvErr,
				)
				continue
			}
			balance = parsedBalance // Assign parsedBalance to balance
		}

		var typeColumnPos = 4

		var slipNumColumnPos = 6

		doc := Data{
			Details:        record[0],
			PostingDate:    postingDateStr,
			Description:    record[2],
			Amount:         amount,
			Type:           safeGet(record, typeColumnPos),
			Balance:        balance,
			CheckOrSlipNum: safeGet(record, slipNumColumnPos),
		}

		documents = append(documents, doc)
		recordsProcessed++
	}

	if len(documents) == 0 {
		return nil, "", 0, nil
	}

	return documents, finalCollectionName, recordsProcessed, nil
}

// safeGet retrieves slice[index] safely.
func safeGet(slice []string, index int) string {
	if index < len(slice) {
		return slice[index]
	}

	return ""
}
