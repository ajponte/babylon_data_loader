package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/config"
	csvparser "babylon/dataloader/csv"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/ingest"
	"babylon/dataloader/storage"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

const (
	defaultIngestTimeout = 4 * time.Minute
	dirPerms             = 0o750
)

// S3StorageService defines the operations required for S3 handling.
type S3StorageService interface {
	Download(ctx context.Context, bucket, key, localPath string) error
	Copy(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error
	Delete(ctx context.Context, bucket, key string) error
}

// IngestRunner executes the ingestion process given a configuration.
type IngestRunner func(ctx context.Context, cfg *config.Config) error

// Handler coordinates S3 event consumption, downloading, ingestion, and archiving.
type Handler struct {
	s3Storage     S3StorageService
	secretsClient config.SecretsClient
	secretID      string
	tmpDir        string
	ingestRunner  IngestRunner
	logger        *slog.Logger
}

// NewHandler constructs a new Lambda Handler.
func NewHandler(
	s3Storage S3StorageService,
	secretsClient config.SecretsClient,
	secretID string,
	tmpDir string,
	runner IngestRunner,
	logger *slog.Logger,
) *Handler {
	if tmpDir == "" {
		tmpDir = "/tmp"
	}
	if runner == nil {
		runner = DefaultIngestRunner
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		s3Storage:     s3Storage,
		secretsClient: secretsClient,
		secretID:      secretID,
		tmpDir:        tmpDir,
		ingestRunner:  runner,
		logger:        logger,
	}
}

// DefaultIngestRunner wires dependencies and executes Sink.Ingest.
func DefaultIngestRunner(ctx context.Context, cfg *config.Config) error {
	logger := bcontext.LoggerFromContext(ctx)

	client, err := storage.ConnectToMongoDBFunc(ctx, cfg.MongoURI)
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
	genericExtractor := datasource.NewGenericExtractor()
	csvParser := csvparser.NewDefaultParser()
	datalakeClient := datalake.NewClient()

	sink := ingest.NewSink(ingest.SinkDependencies{
		Config:         cfg,
		Repo:           repo,
		Extractor:      genericExtractor,
		Parser:         csvParser,
		DatalakeClient: datalakeClient,
	})

	return sink.Ingest(ctx)
}

func (h *Handler) resolveS3Key(ctx context.Context, record events.S3EventRecord) string {
	key := record.S3.Object.URLDecodedKey
	if key != "" {
		return key
	}

	unescaped, err := url.QueryUnescape(record.S3.Object.Key)
	if err != nil {
		h.logger.WarnContext(
			ctx,
			"Failed to unescape S3 object key, using raw key",
			"key", record.S3.Object.Key,
			"error", err,
		)
		return record.S3.Object.Key
	}
	return unescaped
}

func (h *Handler) processRecord(ctx context.Context, record events.S3EventRecord) error {
	bucket := record.S3.Bucket.Name
	key := h.resolveS3Key(ctx, record)

	lowerKey := strings.ToLower(key)
	if !strings.HasPrefix(key, "unprocessed/") {
		h.logger.InfoContext(ctx, "Skipping object: not in unprocessed/ prefix", "bucket", bucket, "key", key)
		return nil
	}
	if !strings.HasSuffix(lowerKey, ".csv") {
		h.logger.InfoContext(ctx, "Skipping object: not a .csv file", "bucket", bucket, "key", key)
		return nil
	}

	filename := filepath.Base(key)
	if filename == "." || filename == "/" || filename == "" {
		h.logger.WarnContext(ctx, "Invalid filename extracted from key", "key", key)
		return nil
	}

	invDir, tmpErr := os.MkdirTemp(h.tmpDir, "ingest-*")
	if tmpErr != nil {
		return fmt.Errorf("failed to create temp directory: %w", tmpErr)
	}
	defer func() {
		_ = os.RemoveAll(invDir)
	}()

	unprocessedDir := filepath.Join(invDir, "unprocessed")
	processedDir := filepath.Join(invDir, "processed")

	if dirErr := os.MkdirAll(unprocessedDir, dirPerms); dirErr != nil {
		return fmt.Errorf("failed to create directory %s: %w", unprocessedDir, dirErr)
	}
	if dirErr := os.MkdirAll(processedDir, dirPerms); dirErr != nil {
		return fmt.Errorf("failed to create directory %s: %w", processedDir, dirErr)
	}

	localUnprocessed := filepath.Join(unprocessedDir, filename)

	h.logger.InfoContext(ctx, "Downloading S3 file", "bucket", bucket, "key", key, "dest", localUnprocessed)
	if dlErr := h.s3Storage.Download(ctx, bucket, key, localUnprocessed); dlErr != nil {
		return fmt.Errorf("failed to download s3://%s/%s: %w", bucket, key, dlErr)
	}

	mongoURI, secretErr := config.GetMongoURI(ctx, h.secretsClient, h.secretID)
	if secretErr != nil {
		return fmt.Errorf("failed to resolve MongoDB URI: %w", secretErr)
	}

	cfg := &config.Config{
		MongoURI:           mongoURI,
		UnprocessedDir:     unprocessedDir,
		ProcessedDir:       processedDir,
		MoveProcessedFiles: true,
		Timeout:            defaultIngestTimeout,
	}

	h.logger.InfoContext(ctx, "Ingesting file", "filename", filename)
	if ingestErr := h.ingestRunner(ctx, cfg); ingestErr != nil {
		return fmt.Errorf("ingestion failed for %s: %w", filename, ingestErr)
	}

	relPath := strings.TrimPrefix(key, "unprocessed/")
	destKey := "processed/" + relPath
	h.logger.InfoContext(ctx, "Copying S3 file to processed/", "src", key, "dest", destKey)
	if copyErr := h.s3Storage.Copy(ctx, bucket, key, bucket, destKey); copyErr != nil {
		return fmt.Errorf("failed to copy object to s3://%s/%s: %w", bucket, destKey, copyErr)
	}

	h.logger.InfoContext(ctx, "Deleting original S3 file from unprocessed/", "key", key)
	if deleteErr := h.s3Storage.Delete(ctx, bucket, key); deleteErr != nil {
		return fmt.Errorf("failed to delete original object from s3://%s/%s: %w", bucket, key, deleteErr)
	}

	h.logger.InfoContext(ctx, "Successfully processed and archived file", "bucket", bucket, "key", key)
	return nil
}

// HandleS3Event handles S3 event notifications from AWS Lambda.
func (h *Handler) HandleS3Event(ctx context.Context, event events.S3Event) error {
	ctx = bcontext.WithLogger(ctx, h.logger)

	for _, record := range event.Records {
		if err := h.processRecord(ctx, record); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := bcontext.WithLogger(context.Background(), logger)
	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to load AWS SDK config", "error", err)
		os.Exit(1)
	}

	s3Client := s3.NewFromConfig(sdkConfig)
	s3Storage := storage.NewS3Storage(s3Client)
	smClient := secretsmanager.NewFromConfig(sdkConfig)

	secretID := os.Getenv("MONGO_SECRET_ID")
	tmpDir := os.Getenv("LAMBDA_TMP_DIR")
	if tmpDir == "" {
		tmpDir = "/tmp"
	}

	handler := NewHandler(s3Storage, smClient, secretID, tmpDir, DefaultIngestRunner, logger)
	lambda.Start(handler.HandleS3Event)
}
