// main.go
package main

import (
	datalake "babylon/dataloader/datalake"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

const (

	defaultTimeoutSeconds   = 30

	defaultMongoURI         = "mongodb://localhost:27017/datalake"

	defaultCSVDir           = "./data"

	defaultProcessedDir     = "processed"

	defaultUnprocessedDir   = "unprocessed"

	defaultMoveProcessedFiles = false

	envMongoURI             = "MONGO_URI"

	envCSVDirectory         = "CSV_DIR"

	envProcessedDirectory   = "PROCESSED_DIR"

	envUnprocessedDirectory = "UNPROCESSED_DIR"

		envMoveProcessedFiles   = "MOVE_PROCESSED_FILES"

		envMongoUser            = "MONGO_USER"

		envMongoPassword        = "MONGO_PASSWORD"

	)

func main() {
	// Create the logger instance at the very beginning.
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Fix for noinlineerr: Separate the assignment and the error check.
	err := run(logger)
	if err != nil {
		logger.Error("Application terminated with an error", "error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	logger.Info("Begin running data loading")
	ctx, cancel := context.WithTimeout(
		datalake.WithLogger(context.Background(), logger),
		defaultTimeoutSeconds*time.Second,
	)
	defer cancel()

	mongoURI := os.Getenv(envMongoURI)
	mongoUser := os.Getenv(envMongoUser)
	mongoPassword := os.Getenv(envMongoPassword)

	if mongoURI == "" {
		if mongoUser != "" && mongoPassword != "" {
			mongoURI = fmt.Sprintf("mongodb://%s:%s@localhost:27017/datalake?authSource=admin", mongoUser, mongoPassword)
		} else {
			mongoURI = defaultMongoURI
			logger.Info(
				"MongoDB URI not found in environment variable, using default",
				"env_var", envMongoURI,
				"uri", mongoURI, // Log the actual URI being used
			)
		}
	}

	csvDirectory := os.Getenv(envCSVDirectory)
	if csvDirectory == "" {
		csvDirectory = defaultCSVDir
		logger.Info(
			"CSV directory not found in environment variable, using default",
			"env_var", envCSVDirectory,
			"dir", defaultCSVDir,
		)
	}

	unprocessedDirName := os.Getenv(envUnprocessedDirectory)
	if unprocessedDirName == "" {
		unprocessedDirName = defaultUnprocessedDir
		logger.Info(
			"Unprocessed directory not found in environment variable, using default",
			"env_var", envUnprocessedDirectory,
			"dir", defaultUnprocessedDir,
		)
	}

	processedDirName := os.Getenv(envProcessedDirectory)
	if processedDirName == "" {
		processedDirName = defaultProcessedDir
		logger.Info(
			"Processed directory not found in environment variable, using default",
			"env_var", envProcessedDirectory,
			"dir", defaultProcessedDir,
		)
	}

	unprocessedDir := fmt.Sprintf("%s/%s", csvDirectory, unprocessedDirName)
	processedDir := fmt.Sprintf("%s/%s", csvDirectory, processedDirName)

	moveProcessedFilesStr := os.Getenv(envMoveProcessedFiles)
	moveProcessedFiles := defaultMoveProcessedFiles
	if moveProcessedFilesStr != "" {
		parsedBool, err := strconv.ParseBool(moveProcessedFilesStr)
		if err != nil {
			logger.Warn(
				"Invalid value for MOVE_PROCESSED_FILES environment variable, using default",
				"env_var", envMoveProcessedFiles,
				"value", moveProcessedFilesStr,
				"error", err,
			)
		} else {
			moveProcessedFiles = parsedBool
		}
	}

	var err error

	// Fix govet shadowing error. Use existing err variable.
	_, err = os.Stat(unprocessedDir)
	if err != nil || os.IsNotExist(err) {
		logger.Error(
			"The directory does not exist. Please create it and place your CSV files inside.",
			"dir", unprocessedDir,
			"error", err,
		)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("stat check for directory %s: %w", unprocessedDir, err)
	}

	// Fix govet shadowing error. Use existing err variable.
	client, err := datalake.ConnectToMongoDB(ctx, mongoURI)
	if err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("connection to MongoDB failed: %w", err)
	}

	defer func() {
		var deferErr = client.Disconnect(ctx)
		// Use a local err variable here to avoid shadowing the function-scoped err.
		if deferErr != nil {
			logger.Error("Error disconnecting from MongoDB", "error", err)
		}
	}()

	logger.Info("Successfully connected to MongoDB.")

	// Fix govet shadowing error. Use existing err variable.
	err = datalake.IngestCSVFiles(ctx, client, unprocessedDir, processedDir, moveProcessedFiles)
	if err != nil {
		logger.Error("Error ingesting CSV files", "error", err)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("ingestion of CSV files failed: %w", err)
	}

	logger.Info("Data ingestion process completed successfully.")

	return nil
}
