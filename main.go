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
	"babylon/dataloader/csv"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/datasource"
	_ "babylon/dataloader/datalake/repository"
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
	minArgs                   = 2
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

	// Check for application startup.
	if err := run(logger, command, args); err != nil {
		logger.ErrorContext(ctx, "Application terminated with an error", "error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

// Hanlde user input and exectue appropriate behavior.
func run(logger *slog.Logger, command string, args []string) error {
	ctx, cancel := context.WithTimeout(
		bcontext.WithLogger(context.Background(), logger),
		defaultTimeoutSeconds*time.Second,
	)
	defer cancel()
	logger.InfoContext(ctx, "Begin running data loading")

	switch command {
	case "generate-synthetic-data":
		return runGenerateSyntheticData(ctx, logger, args)
	case "ingest":
		return runIngest(ctx, logger)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

// Generate synthetic data for testing.
func runGenerateSyntheticData(ctx context.Context, logger *slog.Logger, args []string) error {
	genFlagSet := flag.NewFlagSet("generate-synthetic-data", flag.ExitOnError)
	rows := genFlagSet.Int("rows", defaultSyntheticDataRows, "Number of rows to generate")
	dir := genFlagSet.String("dir", defaultSyntheticDataDir, "Directory to write synthetic data to")
	persistToMongo := genFlagSet.Bool("persist-to-mongo", false, "Persist synthetic data to MongoDB")
	if err := genFlagSet.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *persistToMongo {
		cfg := loadConfig(ctx, logger)
		client, err := storage.ConnectToMongoDB(ctx, cfg.mongoURI)
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

// Main entry point for data ingestion.
func runIngest(ctx context.Context, logger *slog.Logger) error {
	cfg := loadConfig(ctx, logger)

	logger.DebugContext(ctx, "im here")

	// Fix govet shadowing error. Use existing err variable.
	if _, err := os.Stat(cfg.unprocessedDir); err != nil || os.IsNotExist(err) {
		logger.ErrorContext(
			ctx,
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
		logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", err)
		// Fix wrapcheck error. Wrap the error before returning.
		return fmt.Errorf("connection to MongoDB failed: %w", err)
	}

	defer func() {
		if deferErr := client.Disconnect(ctx); deferErr != nil {
			logger.ErrorContext(ctx, "Error disconnecting from MongoDB", "error", deferErr)
		}
	}()

	logger.InfoContext(ctx, "Successfully connected to MongoDB.")

	// Instantiate dependencies
	mongoProvider := storage.NewMongoProvider(client)
	repo := storage.NewMongoRepository(mongoProvider)
	chaseExtractor := datasource.NewChaseExtractor()
	csvParser := csv.NewDefaultParser()

	stats, err := datalake.IngestCSVFiles(
		ctx,
		repo,
		chaseExtractor,
		csvParser,
		cfg.unprocessedDir,
		cfg.processedDir,
		cfg.moveProcessedFiles,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Error ingesting CSV files", "error", err)
		return fmt.Errorf("ingestion of CSV files failed: %w", err)
	}

	logger.InfoContext(ctx, "Data ingestion process completed successfully.")
	stats.Log(logger)

	return nil
}

// Config map.
type config struct {
	mongoURI           string
	unprocessedDir     string
	processedDir       string
	moveProcessedFiles bool
}

// Load app-specific config.
func loadConfig(ctx context.Context, logger *slog.Logger) *config {
	mongoURI := os.Getenv(envMongoURI)
	mongoURI = formatMongoURI(ctx, mongoURI, logger)

	csvDirectory := os.Getenv(envCSVDirectory)
	if csvDirectory == "" {
		csvDirectory = defaultCSVDir
		logger.DebugContext(ctx, "Using default CSV directory", "dir", csvDirectory)
	} else {
		logger.DebugContext(ctx, "Using CSV directory from environment variable", "dir", csvDirectory)
	}

	unprocessedDirName := os.Getenv(envUnprocessedDirectory)
	if unprocessedDirName == "" {
		unprocessedDirName = defaultUnprocessedDir
		logger.DebugContext(ctx, "Using default unprocessed directory name", "dir", unprocessedDirName)
	} else {
		logger.DebugContext(ctx, "Using unprocessed directory name from environment variable",
			"dir", unprocessedDirName)
	}

	processedDirName := os.Getenv(envProcessedDirectory)
	if processedDirName == "" {
		processedDirName = defaultProcessedDir
		logger.DebugContext(ctx, "Using default processed directory name", "dir", processedDirName)
	} else {
		logger.DebugContext(ctx, "Using processed directory name from environment variable", "dir", processedDirName)
	}

	unprocessedDir := fmt.Sprintf("%s/%s", csvDirectory, unprocessedDirName)
	processedDir := fmt.Sprintf("%s/%s", csvDirectory, processedDirName)
	logger.DebugContext(ctx, "Constructed directory paths", "unprocessed", unprocessedDir, "processed", processedDir)

	moveProcessedFilesStr := os.Getenv(envMoveProcessedFiles)
	moveProcessedFiles := defaultMoveProcessedFiles
	if moveProcessedFilesStr != "" {
		parsedBool, err := strconv.ParseBool(moveProcessedFilesStr)
		if err != nil {
			logger.WarnContext(
				ctx,
				"Invalid value for MOVE_PROCESSED_FILES, using default",
				"value", moveProcessedFilesStr,
				"default", defaultMoveProcessedFiles,
				"error", err,
			)
		} else {
			moveProcessedFiles = parsedBool
			logger.DebugContext(ctx, "Set moveProcessedFiles from environment variable", "value", moveProcessedFiles)
		}
	} else {
		logger.DebugContext(ctx, "Using default value for moveProcessedFiles", "value", defaultMoveProcessedFiles)
	}

	return &config{
		mongoURI:           mongoURI,
		unprocessedDir:     unprocessedDir,
		processedDir:       processedDir,
		moveProcessedFiles: moveProcessedFiles,
	}
}

// Format mongo settings to a url and return the result.
func formatMongoURI(
	ctx context.Context,
	mongoURI string,
	logger *slog.Logger,
) string {
	if mongoURI != "" {
		logger.DebugContext(ctx, "Using MongoDB URI from environment variable", "uri", mongoURI)
		return mongoURI
	}

	mongoHost := os.Getenv(envMongoHost)
	if mongoHost == "" {
		mongoHost = defaultMongoHost
		logger.DebugContext(ctx, "Using default MongoDB host", "host", mongoHost)
	} else {
		logger.DebugContext(ctx, "Using MongoDB host from environment variable", "host", mongoHost)
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
		logger.DebugContext(ctx, "Created MongoDB URI from user, password, and host", "uri", mongoURI)
	} else {
		mongoURI = defaultMongoURI
		logger.DebugContext(ctx, "Using default MongoDB URI", "uri", mongoURI)
	}
	return mongoURI
}
