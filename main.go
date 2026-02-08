package main

// Package main provides the entry point for the data loader application.

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/config"
	"babylon/dataloader/csv"
	"babylon/dataloader/datalake/datasource"
	_ "babylon/dataloader/datalake/repository"
	"babylon/dataloader/ingest"

	"babylon/dataloader/storage"
	"babylon/dataloader/synthetic"
)

const (
	minArgs = 2
)

func main() {
	// Create the logger instance.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	ctx := context.Background()

	if len(os.Args) < minArgs {
		logger.ErrorContext(ctx, "Usage: go run main.go <command> [options]")
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Execute commands.
	if err := run(logger, command, args); err != nil {
		logger.ErrorContext(ctx, "Application terminated with an error", "error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

// Hanlde user input and exectue appropriate behavior.
func run(logger *slog.Logger, command string, args []string) error {
	cfg := config.LoadConfig(context.Background(), logger)
	ctx, cancel := context.WithTimeout(
		bcontext.WithLogger(context.Background(), logger),
		cfg.Timeout,
	)
	defer cancel()
	logger.InfoContext(ctx, "Begin running data loading")

	switch command {
	case "generate-synthetic-data":
		return runGenerateSyntheticData(ctx, logger, args, cfg)
	case "ingest":
		// Instantiate dependencies
		client, err := storage.ConnectToMongoDB(ctx, cfg.MongoURI)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", err)
			return fmt.Errorf("connection to MongoDB failed: %w", err)
		}
		defer func() {
			if deferErr := client.Disconnect(ctx); deferErr != nil {
				logger.ErrorContext(ctx, "Error disconnecting from MongoDB", "error", deferErr)
			}
		}()

		mongoProvider := storage.NewMongoProvider(client)
		repo := storage.NewMongoRepository(mongoProvider)
		chaseExtractor := datasource.NewChaseExtractor()
		csvParser := csv.NewDefaultParser()

		// Create and run sink
		sink := ingest.NewSink(logger, cfg, repo, chaseExtractor, csvParser)
		return sink.Ingest(ctx)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

// Generate synthetic data for testing.
func runGenerateSyntheticData(ctx context.Context, logger *slog.Logger, args []string, cfg *config.Config) error {
	genFlagSet := flag.NewFlagSet("generate-synthetic-data", flag.ExitOnError)
	rows := genFlagSet.Int("rows", cfg.SyntheticDataRows, "Number of rows to generate")
	dir := genFlagSet.String("dir", cfg.SyntheticDataDir, "Directory to write synthetic data to")
	persistToMongo := genFlagSet.Bool("persist-to-mongo", false, "Persist synthetic data to MongoDB")
	if err := genFlagSet.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *persistToMongo {
		client, err := storage.ConnectToMongoDB(ctx, cfg.MongoURI)
		if err != nil {
			return fmt.Errorf("failed to connect to MongoDB: %w", err)
		}
		defer func() {
			if deferErr := client.Disconnect(ctx); deferErr != nil {
				logger.ErrorContext(ctx, "Error disconnecting from MongoDB", "error", deferErr)
			}
		}()

		err = synthetic.GenerateAndPersistSyntheticData(ctx, client, "synthetic-ingest", *rows)
		if err != nil {
			return fmt.Errorf("failed to generate and persist synthetic data: %w", err)
		}
		logger.InfoContext(ctx, "Synthetic data generated and persisted successfully")
		return nil
	}

	logger.InfoContext(ctx, "Generating synthetic data")
	err := synthetic.GenerateSyntheticData(*rows, *dir)
	if err != nil {
		return fmt.Errorf("failed to generate synthetic data: %w", err)
	}
	logger.InfoContext(ctx, "Synthetic data generated successfully")
	return nil
}
