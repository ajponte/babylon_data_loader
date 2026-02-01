package datalake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/csv"
	"babylon/dataloader/storage"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dbName        = "datalake"
	syncTableName = "dataSync" // Moved from csv.go
)

// IngestCSVFiles processes all CSV files in a given directory and uploads them to MongoDB.
func IngestCSVFiles(
	ctx context.Context,
	client *mongo.Client,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) (*Stats, error) {
	logger := bcontext.LoggerFromContext(ctx)
	logger.InfoContext(ctx, "Reading data from sink", "sink", unprocessedDir)

	files, err := os.ReadDir(unprocessedDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	stats := NewStats()
	stats.TotalFiles = len(files)

	provider := storage.NewMongoProvider(client)
	logger.InfoContext(ctx, "looping through files", "files", files)

	for _, file := range files {
		if file.IsDir() || (!strings.HasSuffix(file.Name(), ".csv") && !strings.HasSuffix(file.Name(), ".CSV")) {
			reason := "Not a CSV file"
			if file.IsDir() {
				reason = "Is a directory"
			}
			stats.AddFailure(file.Name(), reason)
			logger.WarnContext(ctx, "file was not processed", "fileName", file.Name(), "reason", reason)
			continue
		}

		err = processFile(ctx, provider, file, unprocessedDir, processedDir, moveProcessedFiles)
		if err != nil {
			stats.AddFailure(file.Name(), err.Error())
			logger.ErrorContext(ctx, "failed to process file", "file", file.Name(), "error", err)
		} else {
			stats.IncrementProcessed()
		}
	}

	return stats, nil
}

func processFile(
	ctx context.Context,
	provider storage.CollectionProvider,
	file os.DirEntry,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) error {
	logger := bcontext.LoggerFromContext(ctx)
	dataSource, accountID, err := parseFileNameForSource(file.Name())
	if err != nil {
		return fmt.Errorf("failed to retrieve data source: %w", err)
	}

	cleanFileName := filepath.Clean(file.Name())
	if strings.HasPrefix(cleanFileName, "../") {
		return csv.ValidFileNotFoundError(file.Name())
	}

	filePath := filepath.Join(unprocessedDir, cleanFileName)
	documents, _, recordsProcessed, err := csv.ParseCSV(ctx, filePath, dataSource, accountID)
	if err != nil {
		return err
	}

	var models []mongo.WriteModel
	for _, doc := range documents {
		// Use dataSource and accountID in the filter for uniqueness
		filter := bson.M{
			"Details":     doc.Details,
			"PostingDate": doc.PostingDate,
			"Description": doc.Description,
			"dataSource":  doc.DataSource,
			"accountID":   doc.AccountID,
		}
		update := bson.M{"$set": doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	collectionName := "transactions" // Hardcode collection name to "transactions"
	collection := provider.Collection(collectionName)

	logger.InfoContext(
		ctx,
		"writing documents to collection",
		"collectionName", collectionName,
	)

	result, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("failed to perform bulk write for collection %s: %w", collectionName, err)
	}

	logger.InfoContext(
		ctx,
		"Successfully upserted documents into collection.",
		"upsertedCount", int(result.UpsertedCount),
		"collectionName", collectionName,
	)

	syncCollection := provider.Collection(syncTableName)
	syncLog := csv.SyncLog{
		CollectionName:  collectionName,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: recordsProcessed,
	}

	_, err = syncCollection.InsertOne(ctx, syncLog)
	if err != nil {
		return fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	if moveProcessedFiles {
		err = moveFile(filePath, processedDir)
		if err != nil {
			return fmt.Errorf("failed to move file: %w", err)
		}
	}

	return nil
}

// parseFileNameForSource extracts the data source and account ID from the filename.
func parseFileNameForSource(fileName string) (string, string, error) {
	lowerFileName := strings.ToLower(fileName)

	// Regex to match "chase" and then a 4-digit number
	re := regexp.MustCompile(`chase(\d{4})`)
	matches := re.FindStringSubmatch(lowerFileName)

	if len(matches) > 1 {
		return "chase", matches[1], nil
	}

	// Fallback for generic "test" data source without account ID
	if strings.Contains(lowerFileName, "test") {
		return "test", "0000", nil // Assign a default account ID for test files
	}

	return "", "", fmt.Errorf("could not parse data source and account ID from filename: %s", fileName)
}

func moveFile(filePath, processedDir string) error {
	var err error
	if _, err = os.Stat(processedDir); os.IsNotExist(err) {
		if err = os.MkdirAll(processedDir, 0750); err != nil {
			return fmt.Errorf("failed to create processed directory '%s': %w", processedDir, err)
		}
	}

	fileName := filepath.Base(filePath)
	newPath := filepath.Join(processedDir, fileName)

	if err = os.Rename(filePath, newPath); err != nil {
		return fmt.Errorf("failed to move file from '%s' to '%s': %w", filePath, newPath, err)
	}

	return nil
}
