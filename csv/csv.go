package csv

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
)

// Data represents a single row from the CSV file.
type Data struct {
	Details        string  `bson:"Details"`
	PostingDate    string  `bson:"PostingDate"`
	Description    string  `bson:"Description"`
	Amount         float64 `bson:"Amount"`
	Category       string  `bson:"category"`   // New field
	Type           string  `bson:"Type"`
	Balance        float64 `bson:"Balance"`
	CheckOrSlipNum string  `bson:"CheckOrSlipNum"`
	DataSource     string  `bson:"dataSource"` // New field
	AccountID      string  `bson:"accountID"`  // New field
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
	accountID string, // New argument
) ([]Data, string, int64, error) {
	// Retrieve the logger from the context at the start of the function.
	logger := bcontext.LoggerFromContext(ctx)
	logger.InfoContext(
		ctx,
		"Parsing data from csv", "filePath",
		filePath, "dataSource", dataSource, "accountID", accountID,
	)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.Comma = ','

	// Read header and create column index map
	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, "", 0, nil // Handle empty file gracefully
		}
		return nil, "", 0, fmt.Errorf("failed to read CSV header from file %s: %w", filePath, err)
	}
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[strings.ToLower(col)] = i
	}

	var documents []Data
	var finalCollectionName string // Store the first valid collection name encountered
	var recordsProcessed int64

	for {
		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, "", 0, fmt.Errorf("failed to read record from CSV in file %s: %w", filePath, readErr)
		}

		if len(record) < len(header) {
			logger.WarnContext(ctx, "Skipping invalid record", "reason", "not enough columns", "file", filePath)
			continue
		}

		postingDateStr := safeGet(record, colIndex["posting date"])
		if postingDateStr == "" {
			logger.WarnContext(ctx, "Skipping record with empty posting date", "file", filePath)
			continue
		}

		parsedDate, parseErr := time.Parse("01/02/2006", postingDateStr)
		if parseErr != nil {
			logger.WarnContext(ctx, "skipping record with invalid date format '%s': %v", postingDateStr, parseErr)
			continue
		}

		currentCollectionName := fmt.Sprintf("%s-data-%s", dataSource, parsedDate.Format("2006-01-02"))
		if finalCollectionName == "" {
			finalCollectionName = currentCollectionName
		}

		amount, convErr := strconv.ParseFloat(safeGet(record, colIndex["amount"]), 64)
		if convErr != nil {
			logger.WarnContext(ctx, "skipping record with invalid amount format", "value", safeGet(record, colIndex["amount"]), "error", convErr)
			continue
		}

		balance := 0.0
		if balanceIndex, ok := colIndex["balance"]; ok {
			if balanceStr := safeGet(record, balanceIndex); balanceStr != "" {
				parsedBalance, balanceConvErr := strconv.ParseFloat(balanceStr, 64)
				if balanceConvErr != nil {
					logger.WarnContext(ctx, "skipping record with invalid balance format", "value", balanceStr, "error", balanceConvErr)
				} else {
					balance = parsedBalance
				}
			}
		}

		doc := Data{
			Details:        safeGet(record, colIndex["details"]),
			PostingDate:    postingDateStr,
			Description:    safeGet(record, colIndex["description"]),
			Category:       safeGet(record, colIndex["category"]),
			Amount:         amount,
			Type:           safeGet(record, colIndex["type"]),
			Balance:        balance,
			DataSource:     dataSource,
			AccountID:      accountID,
		}
		if checkOrSlipNumIndex, ok := colIndex["check or slip #"]; ok {
			doc.CheckOrSlipNum = safeGet(record, checkOrSlipNumIndex)
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
