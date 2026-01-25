package datalake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	bcontext "babylon/dataloader/context"
	"babylon/dataloader/csv"
	"babylon/dataloader/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	externalDataSource, err := dataSource(file.Name())
	if err != nil {
		return fmt.Errorf("failed to retrieve data source: %w", err)
	}

	cleanFileName := filepath.Clean(file.Name())
	if strings.HasPrefix(cleanFileName, "../") {
		return csv.ValidFileNotFoundError(file.Name())
	}

	filePath := filepath.Join(unprocessedDir, cleanFileName)
	documents, collectionName, recordsProcessed, err := csv.ParseCSV(ctx, filePath, externalDataSource)
	if err != nil {
		return err
	}

	var models []mongo.WriteModel
	for _, doc := range documents {
		filter := bson.M{"Details": doc.Details, "PostingDate": doc.PostingDate, "Description": doc.Description}
		update := bson.M{"$set": doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

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

	syncCollection := provider.Collection("dataSync")
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

func dataSource(fileName string) (string, error) {
	if strings.Contains(strings.ToLower(fileName), "chase") {
		return "chase", nil
	}
	if strings.Contains(strings.ToLower(fileName), "test") {
		return "test", nil
	}

	return "", csv.DataSourceParseError(fileName)
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
