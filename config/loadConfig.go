package config

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"
)

// Default values for testing.
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
	minArgs                   = 2 // This constant is only used in main.go, so it should not be here.
	envMongoURI               = "MONGO_URI"
	envMongoHost              = "MONGO_HOST"
	envCSVDirectory           = "CSV_DIR"
	envProcessedDirectory     = "PROCESSED_DIR"
	envUnprocessedDirectory   = "UNPROCESSED_DIR"
	envMoveProcessedFiles     = "MOVE_PROCESSED_FILES"
	envMongoUser              = "MONGO_USER"
	envMongoPassword          = "MONGO_PASSWORD"
)

// LoadConfig loads the application configuration from environment variables or uses default values.
func LoadConfig(ctx context.Context, logger *slog.Logger) *Config {
	mongoURI := os.Getenv(envMongoURI)
	mongoURI = formatMongoURI(ctx, mongoURI, logger)

	csvDirectory := setEnvCSVDir(ctx, *logger)

	// Configure the dirs for processed/unprocessed files.
	unprocessedDir := setUnprocessedDir(ctx, csvDirectory, *logger)
	processedDir := setProcessedDir(ctx, csvDirectory, *logger)

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

	syntheticDataDir := defaultSyntheticDataDir
	syntheticDataRows := defaultSyntheticDataRows
	// TODO: Add environment variable parsing for syntheticDataDir and syntheticDataRows if needed

	return &Config{
		MongoURI:           mongoURI,
		UnprocessedDir:     unprocessedDir,
		ProcessedDir:       processedDir,
		MoveProcessedFiles: moveProcessedFiles,
		SyntheticDataDir:   syntheticDataDir,
		SyntheticDataRows:  syntheticDataRows,
		Timeout:            defaultTimeoutSeconds * time.Second,
	}
}

func setEnvCSVDir(ctx context.Context, logger slog.Logger) string {
	csvDirectory := os.Getenv(envCSVDirectory)
	if csvDirectory == "" {
		csvDirectory = defaultCSVDir
		logger.DebugContext(ctx, "Using default CSV directory", "dir", csvDirectory)
	} else {
		logger.DebugContext(ctx, "Using CSV directory from environment variable", "dir", csvDirectory)
	}

	return csvDirectory
}

// Format the directory in which unprocessed data files exist.
func setUnprocessedDir(ctx context.Context, csvDirectory string, logger slog.Logger) string {
	return fmt.Sprintf("%s/%s", csvDirectory, setUnprocessedDirName(ctx, logger))
}

// Format the directory in which processed data files are moved to.
func setProcessedDir(ctx context.Context, csvDirectory string, logger slog.Logger) string {
	return fmt.Sprintf("%s/%s", csvDirectory, getProcessedDirName(ctx, logger))
}

// Fetch the `processedDirName` env var or set to a default value.
func getProcessedDirName(ctx context.Context, logger slog.Logger) string {
	processedDirName := os.Getenv(envProcessedDirectory)
	if processedDirName == "" {
		processedDirName = defaultProcessedDir
		logger.DebugContext(ctx, "Using default processed directory name", "dir", processedDirName)
	} else {
		logger.DebugContext(ctx, "Using processed directory name from environment variable", "dir", processedDirName)
	}

	return processedDirName
}

// Fetch the `unprocessedDirName` env var or set to a default value.
func setUnprocessedDirName(ctx context.Context, logger slog.Logger) string {
	unprocessedDirName := os.Getenv(envUnprocessedDirectory)
	if unprocessedDirName == "" {
		unprocessedDirName = defaultUnprocessedDir
		logger.DebugContext(ctx, "Using default unprocessed directory name", "dir", unprocessedDirName)
	} else {
		logger.DebugContext(ctx, "Using unprocessed directory name from environment variable",
			"dir", unprocessedDirName)
	}

	return unprocessedDirName
}

// formatMongoURI formats mongo settings to a url and return the result.
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
