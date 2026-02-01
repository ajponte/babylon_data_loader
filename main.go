// main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/datalake"
	"babylon/dataloader/storage"
	"babylon/dataloader/synthetic"
)

const (
	defaultTimeoutSeconds     = 30
	defaultMongoURI           = "mongodb://localhost:27017/datalake"
	defaultMongoHost          = "localhost"
	defaultMongoPort          = "27017"
	defaultCSVDir             = "./data"
	defaultProcessedDir       = "processed"
	defaultUnprocessedDir     = "unprocessed"
	defaultMoveProcessedFiles = false
	defaultSyntheticDataDir   = "tmp/synthetic"
	defaultSyntheticDataRows  = 100
	envMongoURI               = "MONGO_URI"
	envMongoHost              = "MONGO_HOST"
	envCSVDirectory           = "CSV_DIR"
	envProcessedDirectory     = "PROCESSED_DIR"
	envUnprocessedDirectory   = "UNPROCESSED_DIR"
	envMoveProcessedFiles     = "MOVE_PROCESSED_FILES"
	envMongoUser              = "MONGO_USER"
	envMongoPassword          = "MONGO_PASSWORD"
)

func main() {
	// Create the logger instance at the very beginning.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	if len(os.Args) < 2 {
		logger.Error("Usage: go run main.go <command> [options]")
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Fix for noinlineerr: Separate the assignment and the error check.
	if err := run(logger, command, args); err != nil {
		logger.Error("Application terminated with an error", "error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger, command string, args []string) error {
	logger.Info("Begin running data loading")
	ctx, cancel := context.WithTimeout(
		bcontext.WithLogger(context.Background(), logger),
		defaultTimeoutSeconds*time.Second,
	)
	defer cancel()

	switch command {
	case "generate-synthetic-data":
		genFlagSet := flag.NewFlagSet("generate-synthetic-data", flag.ExitOnError)
		rows := genFlagSet.Int("rows", defaultSyntheticDataRows, "Number of rows to generate")
		dir := genFlagSet.String("dir", defaultSyntheticDataDir, "Directory to write synthetic data to")
		genFlagSet.Parse(args)

		logger.Info("Generating synthetic data")
		err := synthetic.GenerateSyntheticData(*rows, *dir)
		if err != nil {
			return fmt.Errorf("failed to generate synthetic data: %w", err)
		}
		logger.Info("Synthetic data generated successfully")
		return nil
	case "ingest":
		cfg := loadConfig(logger)

		logger.Debug("im here")

		// Fix govet shadowing error. Use existing err variable.
		if _, err := os.Stat(cfg.unprocessedDir); err != nil || os.IsNotExist(err) {
			logger.Error(
				"The directory does not exist. Please create it and place your CSV files inside.",
				"dir", cfg.unprocessedDir,
				"error", err,
			)
			// Fix wrapcheck error. Wrap the error before returning.
			return fmt.Errorf("stat check for directory %s: %w", cfg.unprocessedDir, err)
		}

		// Fix govet shadowing error. Use existing err variable.
		client, err := storage.ConnectToMongoDB(ctx, cfg.mongoURI)
		if err != nil {
			logger.Error("Failed to connect to MongoDB", "error", err)
			// Fix wrapcheck error. Wrap the error before returning.
			return fmt.Errorf("connection to MongoDB failed: %w", err)
		}

		defer func() {
			if deferErr := client.Disconnect(ctx); deferErr != nil {
				logger.Error("Error disconnecting from MongoDB", "error", deferErr)
			}
		}()

		logger.Info("Successfully connected to MongoDB.")

		stats, err := datalake.IngestCSVFiles(ctx, client, cfg.unprocessedDir, cfg.processedDir, cfg.moveProcessedFiles)
		if err != nil {
			logger.Error("Error ingesting CSV files", "error", err)
			return fmt.Errorf("ingestion of CSV files failed: %w", err)
		}

		logger.Info("Data ingestion process completed successfully.")
		stats.Log(logger)

		return nil
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

type config struct {
	mongoURI           string
	unprocessedDir     string
	processedDir       string
	moveProcessedFiles bool
}

func loadConfig(logger *slog.Logger) *config {
	mongoURI := os.Getenv(envMongoURI)
	mongoURI = formatMongoURI(mongoURI, logger)

	csvDirectory := os.Getenv(envCSVDirectory)
	if csvDirectory == "" {
		csvDirectory = defaultCSVDir
		logger.Debug("Using default CSV directory", "dir", csvDirectory)
	} else {
		logger.Debug("Using CSV directory from environment variable", "dir", csvDirectory)
	}

	unprocessedDirName := os.Getenv(envUnprocessedDirectory)
	if unprocessedDirName == "" {
		unprocessedDirName = defaultUnprocessedDir
		logger.Debug("Using default unprocessed directory name", "dir", unprocessedDirName)
	} else {
		logger.Debug("Using unprocessed directory name from environment variable", "dir", unprocessedDirName)
	}

	processedDirName := os.Getenv(envProcessedDirectory)
	if processedDirName == "" {
		processedDirName = defaultProcessedDir
		logger.Debug("Using default processed directory name", "dir", processedDirName)
	} else {
		logger.Debug("Using processed directory name from environment variable", "dir", processedDirName)
	}

	unprocessedDir := fmt.Sprintf("%s/%s", csvDirectory, unprocessedDirName)
	processedDir := fmt.Sprintf("%s/%s", csvDirectory, processedDirName)
	logger.Debug("Constructed directory paths", "unprocessed", unprocessedDir, "processed", processedDir)

	moveProcessedFilesStr := os.Getenv(envMoveProcessedFiles)
	moveProcessedFiles := defaultMoveProcessedFiles
	if moveProcessedFilesStr != "" {
		parsedBool, err := strconv.ParseBool(moveProcessedFilesStr)
		if err != nil {
			logger.Warn(
				"Invalid value for MOVE_PROCESSED_FILES, using default",
				"value", moveProcessedFilesStr,
				"default", defaultMoveProcessedFiles,
				"error", err,
			)
		} else {
			moveProcessedFiles = parsedBool
			logger.Debug("Set moveProcessedFiles from environment variable", "value", moveProcessedFiles)
		}
	} else {
		logger.Debug("Using default value for moveProcessedFiles", "value", defaultMoveProcessedFiles)
	}

	return &config{
		mongoURI:           mongoURI,
		unprocessedDir:     unprocessedDir,
		processedDir:       processedDir,
		moveProcessedFiles: moveProcessedFiles,
	}
}

/**
 * Format mongo settings to a url and return the result.
 */
func formatMongoURI(
	mongoURI string,
	logger *slog.Logger,
) string {
	if mongoURI != "" {
		logger.Debug("Using MongoDB URI from environment variable", "uri", mongoURI)
		return mongoURI
	}

	mongoHost := os.Getenv(envMongoHost)
	if mongoHost == "" {
		mongoHost = defaultMongoHost
		logger.Debug("Using default MongoDB host", "host", mongoHost)
	} else {
		logger.Debug("Using MongoDB host from environment variable", "host", mongoHost)
	}

	mongoUser := os.Getenv(envMongoUser)
	mongoPassword := os.Getenv(envMongoPassword)

	if mongoUser != "" && mongoPassword != "" {
		hostPort := net.JoinHostPort(mongoHost, defaultMongoPort)
		mongoURI = fmt.Sprintf(
			"mongodb://%s:%s@%s/datalake?authSource=admin",
			mongoUser,
			mongoPassword,
			hostPort,
		)
		logger.Debug("Created MongoDB URI from user, password, and host", "uri", mongoURI)
	} else {
		mongoURI = defaultMongoURI
		logger.Debug("Using default MongoDB URI", "uri", mongoURI)
	}
	return mongoURI
}
