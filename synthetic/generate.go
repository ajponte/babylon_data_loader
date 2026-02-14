package synthetic

import (
	"context"
	"flag"
	"fmt"
	"log/slog"

	"babylon/dataloader/config"
	"babylon/dataloader/storage"
)

// RunGenerateSyntheticData generates synthetic data for testing.
func RunGenerateSyntheticData(ctx context.Context, logger *slog.Logger, args []string, cfg *config.Config) error {
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

		err = GenerateAndPersistSyntheticData(ctx, client, "synthetic-ingest", *rows)
		if err != nil {
			return fmt.Errorf("failed to generate and persist synthetic data: %w", err)
		}
		logger.InfoContext(ctx, "Synthetic data generated and persisted successfully")
		return nil
	}

	logger.InfoContext(ctx, "Generating synthetic data")
	err := GenerateSyntheticData(*rows, *dir)
	if err != nil {
		return fmt.Errorf("failed to generate synthetic data: %w", err)
	}
	logger.InfoContext(ctx, "Synthetic data generated successfully")
	return nil
}
