// main.go
package main

import (
	datalake "babylon/dataloader/datalake"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

const (
	defaultTimeoutSeconds = 30
	defaultMongoURI       = "mongodb://localhost:27017/datalake"
	defaultCSVDir         = "./data"
	envMongoURI           = "MONGO_URI"
	envCSVDirectory       = "CSV_DIR"
)

func main() {
	// Create the logger instance at the very beginning.
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Fix for noinlineerr: Separate the assignment and the error check.
	err := run(logger)
	if err != nil {
		logger.Error("Application terminated with an error", "error", err)
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
	if mongoURI == "" {
		mongoURI = defaultMongoURI
		logger.Info(
			"MongoDB URI not found in environment variable, using default",
			"env_var", envMongoURI,
			"uri", mongoURI, // Log the actual URI being used
		)
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

	var err error

	// Fix govet shadowing error. Use existing err variable.
	_, err = os.Stat(csvDirectory)
	if err != nil || os.IsNotExist(err) {
		logger.Error(
			"The directory does not exist. Please create it and place your CSV files inside.",
			"dir", csvDirectory,
			"error", err,
		)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("stat check for directory %s: %w", csvDirectory, err)
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
	err = datalake.IngestCSVFiles(ctx, client, csvDirectory)
	if err != nil {
		logger.Error("Error ingesting CSV files", "error", err)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("ingestion of CSV files failed: %w", err)
	}

	logger.Info("Data ingestion process completed successfully.")

	return nil
}
