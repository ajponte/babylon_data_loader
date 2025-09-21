// Package datalake holds methods for pushing new records to the Babylon data lake.
package datalake

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

const (
	dbName        = "babylonDataLake"
	syncTableName = "dataSync"
)

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

// ---- Abstractions for Testability ----

type dataStore interface {
	BulkWrite(
		ctx context.Context,
		models []mongo.WriteModel,
		opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	InsertOne(
		ctx context.Context,
		document interface{},
		opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
}

type collectionProvider interface {
	Collection(name string) dataStore
}

// mongoCollection adapts *mongo.Collection to dataStore.
type mongoCollection struct {
	*mongo.Collection
}

func (c *mongoCollection) BulkWrite(
	ctx context.Context,
	models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	result, err := c.Collection.BulkWrite(ctx, models, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to perform BulkWrite: %w", err)
	}

	return result, nil
}

func (c *mongoCollection) InsertOne(
	ctx context.Context,
	document interface{},
	opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	result, err := c.Collection.InsertOne(ctx, document, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to perform InsertOne: %w", err)
	}

	return result, nil
}

// mongoProvider adapts *mongo.Client to collectionProvider.
type mongoProvider struct {
	client *mongo.Client
}

func (p *mongoProvider) Collection(name string) dataStore {
	return &mongoCollection{p.client.Database(dbName).Collection(name)}
}

// ---- Core Logic ----

// IngestCSVFiles processes all CSV files in a given directory and uploads them to MongoDB.
func IngestCSVFiles(ctx context.Context, client *mongo.Client, dirPath string) error {
	// Retrieve the logger from the context at the start of the function.
	logger := LoggerFromContext(ctx)
	logger.InfoContext(ctx, "Reading data from sink", "sink", dirPath)
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	provider := &mongoProvider{client: client}

	logger.InfoContext(ctx, "looping through files", "files", files)
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			//nolint:govet // We want to stay in the file.
			externalDataSource, err := dataSource(file.Name())
			if err != nil {
				return fmt.Errorf("failed to retrieve data source: %w", err)
			}

			// Sanitize the file name to prevent directory traversal attacks
			cleanFileName := filepath.Clean(file.Name())

			// Optional: Ensure the path is not attempting to go up the directory tree
			if strings.HasPrefix(cleanFileName, "../") {
				return ValidFileNotFoundError(file.Name())
			}

			filePath := filepath.Join(dirPath, cleanFileName)

			err = ProcessCSV(ctx, provider, filePath, externalDataSource)
			if err != nil {
				return ProcessCsvError(file.Name())
			}
		} else {
			logger.WarnContext(ctx, "file was not processed", "fileName", file.Name())
		}
	}

	return nil
}

// ProcessCSV reads a CSV file from a given path and uploads the data to MongoDB.
//
//nolint:funlen // refactor this later
func ProcessCSV(ctx context.Context, provider collectionProvider, filePath string, dataSource string) error {
	// Retrieve the logger from the context at the start of the function.
	logger := LoggerFromContext(ctx)
	logger.InfoContext(
		ctx,
		"Processing data from csv", "filePath",
		filePath, "dataSource", dataSource,
	)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	var documents []mongo.WriteModel

	var collectionName string

	var recordsProcessed int64

	// Mak number of columns required per record.
	var maxColumns = 4

	for {
		logger.DebugContext(ctx, "Reading new record from CSV")
		//nolint:govet // We want to stay in the file.
		record, err := reader.Read()
		//nolint:errorlint // We want to continue if we've reached to the end of a file.
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to read record from CSV: %w", err)
		}

		if len(record) < maxColumns {
			logger.WarnContext(ctx, "Skipping invalid record", "reason", "less than 4 columns", "file", filePath)

			continue
		}

		postingDateStr := record[1]

		parsedDate, err := time.Parse("01/02/2006", postingDateStr)
		if err != nil {
			logger.WarnContext(ctx, "skipping record with invalid date format '%s': %v", postingDateStr, err)

			continue
		}

		collectionName = fmt.Sprintf("%s-data-%s", dataSource, parsedDate.Format("2006-01-02"))

		amount, _ := strconv.ParseFloat(record[3], 64)

		var minRecords = 5

		balance := 0.0
		if len(record) > minRecords {
			balance, _ = strconv.ParseFloat(record[5], 64)
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

		filter := bson.M{"Details": doc.Details, "PostingDate": doc.PostingDate, "Description": doc.Description}
		update := bson.M{"$set": doc}
		upsertModel := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)

		documents = append(documents, upsertModel)
		recordsProcessed++
	}

	if len(documents) == 0 {
		return ValidFileNotFoundError(filePath)
	}

	collection := provider.Collection(collectionName)

	logger.InfoContext(
		ctx,
		"writing documents to collection",
		"collectionName", collectionName,
	)

	result, err := collection.BulkWrite(ctx, documents, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("failed to perform bulk write for collection %s: %w", collectionName, err)
	}

	logger.InfoContext(
		ctx,
		"Successfully upserted documents into collection.",
		slog.Int("upsertedCount", int(result.UpsertedCount)),
		slog.String("collectionName", collectionName),
	)

	syncCollection := provider.Collection(syncTableName)
	syncLog := SyncLog{
		CollectionName:  collectionName,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: recordsProcessed,
	}

	_, err = syncCollection.InsertOne(ctx, syncLog)
	if err != nil {
		return fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	return nil
}

// safeGet retrieves slice[index] safely.
func safeGet(slice []string, index int) string {
	if index < len(slice) {
		return slice[index]
	}

	return ""
}

// ConnectToMongoDB establishes a connection to MongoDB.
func ConnectToMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return client, nil
}

func dataSource(fileName string) (string, error) {
	if strings.Contains(strings.ToLower(fileName), "chase") {
		return "chase", nil
	}

	return "", DataSourceParseError(fileName)
}
