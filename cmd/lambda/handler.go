// Package main provides the AWS Lambda worker entrypoint and S3 event processing handler
// for streaming bank and investment statements directly into the MongoDB datalake.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/config"
	"babylon/dataloader/internal/app/ingest"
	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/chase"
	"babylon/dataloader/internal/infra/sources/fidelity"
	"babylon/dataloader/internal/infra/sources/synthetic"
	"babylon/dataloader/internal/infra/storage/mongo"
	s3storage "babylon/dataloader/internal/infra/storage/s3"

	"github.com/aws/aws-lambda-go/events"
)

const defaultCloseTimeout = 5 * time.Second

var uriCredentialMask = regexp.MustCompile(`://([^:@\s]+):([^@\s]+)@`)

// repository encapsulates port.Repository and its graceful shutdown.
type repository interface {
	port.Repository
	Close(ctx context.Context) error
}

type (
	repoFactory func(ctx context.Context, uri string) (repository, error)
	s3Factory   func(api s3storage.ObjectAPI, bucket string) (port.ObjectStore, error)
)

// S3EventHandler coordinates S3 event notifications and streaming ingestion into MongoDB.
type S3EventHandler struct {
	api        s3storage.ObjectAPI
	secrets    config.SecretsClient
	secretID   string
	connect    repoFactory
	newS3Store s3Factory
	logger     *slog.Logger
	clock      func() time.Time

	mu       sync.Mutex
	pipeline *pipeline.Pipeline
	repo     repository
}

// HandlerOption configures optional behavior on S3EventHandler.
type HandlerOption func(*S3EventHandler)

// WithClock sets a custom timestamp provider for pipeline runs.
func WithClock(clock func() time.Time) HandlerOption {
	return func(h *S3EventHandler) {
		if clock != nil {
			h.clock = clock
		}
	}
}

// WithS3StoreFactory overrides the S3 store construction seam.
func WithS3StoreFactory(f s3Factory) HandlerOption {
	return func(h *S3EventHandler) {
		if f != nil {
			h.newS3Store = f
		}
	}
}

// NewS3EventHandler constructs a new streaming S3EventHandler for AWS Lambda.
func NewS3EventHandler(
	api s3storage.ObjectAPI,
	secrets config.SecretsClient,
	secretID string,
	connect repoFactory,
	logger *slog.Logger,
	opts ...HandlerOption,
) *S3EventHandler {
	if connect == nil {
		connect = func(ctx context.Context, uri string) (repository, error) {
			return mongo.NewRepository(ctx, mongo.Config{URI: uri})
		}
	}
	if logger == nil {
		logger = slog.Default()
	}

	h := &S3EventHandler{
		api:      api,
		secrets:  secrets,
		secretID: secretID,
		connect:  connect,
		newS3Store: func(a s3storage.ObjectAPI, bucket string) (port.ObjectStore, error) {
			return s3storage.NewClient(a, bucket)
		},
		logger: logger,
		clock:  time.Now,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// ensurePipeline lazily connects to MongoDB and registers default pipeline extractors under a mutex.
// Resets state on failure to ensure subsequent Lambda invocations can retry.
func (h *S3EventHandler) ensurePipeline(ctx context.Context) (*pipeline.Pipeline, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.pipeline != nil {
		return h.pipeline, nil
	}

	mongoURI, err := config.GetMongoURI(ctx, h.secrets, h.secretID)
	if err != nil {
		sanitizedErr := sanitizeError(err, mongoURI)
		h.logger.ErrorContext(ctx, "Failed to resolve MongoDB URI", "error", sanitizedErr)
		return nil, fmt.Errorf("resolve mongo uri: %w", sanitizedErr)
	}

	repo, err := h.connect(ctx, mongoURI)
	if err != nil {
		sanitizedErr := sanitizeError(err, mongoURI)
		h.logger.ErrorContext(ctx, "Failed to connect to MongoDB", "error", sanitizedErr)
		return nil, fmt.Errorf("connect to mongo: %w", sanitizedErr)
	}

	p := pipeline.New(repo, nil, pipeline.WithClock(h.clock))
	if regErr := ingest.RegisterDefaults(
		p,
		chase.NewExtractor(nil),
		fidelity.NewExtractor(),
		synthetic.NewCSVExtractor(),
	); regErr != nil {
		_ = repo.Close(ctx)
		return nil, fmt.Errorf("register default extractors: %w", regErr)
	}

	h.repo = repo
	h.pipeline = p
	return h.pipeline, nil
}

// Close disconnects the repository if initialized and resets cached pipeline state.
// Uses a detached context with a 5-second timeout to ensure clean TCP disconnect.
func (h *S3EventHandler) Close(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.repo != nil {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultCloseTimeout)
		defer cancel()
		err := h.repo.Close(closeCtx)
		h.repo = nil
		h.pipeline = nil
		return err
	}
	return nil
}

func (h *S3EventHandler) resolveS3Key(ctx context.Context, record events.S3EventRecord) string {
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

func (h *S3EventHandler) validateS3Key(ctx context.Context, bucket, key string) (string, bool) {
	if !strings.HasPrefix(key, "unprocessed/") {
		h.logger.InfoContext(ctx, "Skipping object: not in unprocessed/ prefix", "bucket", bucket, "key", key)
		return "", false
	}
	if !strings.HasSuffix(strings.ToLower(key), ".csv") {
		h.logger.InfoContext(ctx, "Skipping object: not a .csv file", "bucket", bucket, "key", key)
		return "", false
	}

	filename := filepath.Base(key)
	if filename == "." || filename == "/" || filename == "" {
		h.logger.WarnContext(ctx, "Invalid filename extracted from key", "key", key)
		return "", false
	}

	return filename, true
}

func (h *S3EventHandler) processRecord(ctx context.Context, record events.S3EventRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	bucket := record.S3.Bucket.Name
	key := h.resolveS3Key(ctx, record)

	filename, valid := h.validateS3Key(ctx, bucket, key)
	if !valid {
		return nil
	}

	p, err := h.ensurePipeline(ctx)
	if err != nil {
		return err
	}

	store, err := h.newS3Store(h.api, bucket)
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to initialize S3 store", "bucket", bucket, "error", err)
		return err
	}

	archiveKey := deriveArchiveKey(key)
	sourceURI := fmt.Sprintf("s3://%s/%s", bucket, key)
	ingestor := ingest.NewIngestor(p, store, ingest.WithClock(h.clock))

	h.logger.InfoContext(ctx, "Ingesting S3 object",
		"bucket", bucket,
		"key", key,
		"filename", filename,
		"sourceURI", sourceURI,
		"archiveKey", archiveKey,
	)

	_, runErr := ingestor.IngestObject(ctx, ingest.ObjectRequest{
		Key:        key,
		ArchiveKey: archiveKey,
		SourceURI:  sourceURI,
	})

	return h.classifyRecordError(ctx, runErr, bucket, key)
}

func deriveArchiveKey(key string) string {
	archiveKey, ok := ingest.ArchiveKeyFor(key, "unprocessed/", "processed/")
	if !ok {
		return "processed/" + strings.TrimPrefix(key, "unprocessed/")
	}
	return archiveKey
}

func (h *S3EventHandler) classifyRecordError(ctx context.Context, err error, bucket, key string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pipeline.ErrAuditWrite) {
		h.logger.WarnContext(ctx, "Audit write failed for record; batch persisted",
			"bucket", bucket,
			"key", key,
			"error", err,
		)
		return nil
	}
	if isTransientError(err) {
		h.logger.ErrorContext(ctx, "Transient error processing S3 record, returning error for retry",
			"bucket", bucket,
			"key", key,
			"error", err,
		)
		return err
	}
	h.logger.WarnContext(ctx, "Deterministic error processing S3 record, leaving object in unprocessed/",
		"bucket", bucket,
		"key", key,
		"error", err,
	)
	return nil
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, mongo.ErrTransient) {
		return true
	}
	if errors.Is(err, port.ErrObjectNotFound) {
		return true
	}
	if strings.Contains(err.Error(), "file archival failed") ||
		strings.Contains(err.Error(), "s3: archive") {
		return true
	}
	if isDeterministicError(err) {
		return false
	}
	return true
}

func isDeterministicError(err error) bool {
	return isIngestOrPipelineError(err) ||
		isSourceDialectError(err) ||
		isModelOrCSVParseError(err)
}

func isIngestOrPipelineError(err error) bool {
	return errors.Is(err, ingest.ErrNotCSV) ||
		errors.Is(err, ingest.ErrUnresolvableSource) ||
		errors.Is(err, ingest.ErrInputTooLarge) ||
		errors.Is(err, pipeline.ErrUnknownSourceType) ||
		errors.Is(err, pipeline.ErrInvalidRegistration) ||
		errors.Is(err, mongo.ErrDuplicateKey)
}

func isSourceDialectError(err error) bool {
	return errors.Is(err, chase.ErrUnknownDialect) ||
		errors.Is(err, chase.ErrMissingRequiredColumns) ||
		errors.Is(err, chase.ErrMalformedHeader) ||
		errors.Is(err, chase.ErrDuplicateColumn) ||
		errors.Is(err, chase.ErrMalformedCSV) ||
		errors.Is(err, chase.ErrInvalidRow) ||
		errors.Is(err, synthetic.ErrMissingHeader) ||
		errors.Is(err, synthetic.ErrMissingRequiredColumns) ||
		errors.Is(err, synthetic.ErrMalformedCSV) ||
		errors.Is(err, synthetic.ErrInvalidRow) ||
		errors.Is(err, fidelity.ErrMissingRequiredColumns) ||
		errors.Is(err, fidelity.ErrInvalidDateDownloaded)
}

func isModelOrCSVParseError(err error) bool {
	if errors.Is(err, model.ErrNilBatch) ||
		errors.Is(err, model.ErrInvalidDestination) ||
		errors.Is(err, model.ErrNilPayload) ||
		errors.Is(err, model.ErrMissingPostingDate) ||
		errors.Is(err, model.ErrInvalidAmount) ||
		errors.Is(err, model.ErrMissingSymbol) ||
		errors.Is(err, model.ErrInvalidQuantity) ||
		errors.Is(err, model.ErrInvalidCostBasis) ||
		errors.Is(err, model.ErrInvalidLastPrice) ||
		errors.Is(err, model.ErrMissingAsOfDate) ||
		errors.Is(err, model.ErrInvalidPerformanceMetric) {
		return true
	}
	var parseErr *csv.ParseError
	return errors.As(err, &parseErr) ||
		errors.Is(err, csv.ErrFieldCount) ||
		errors.Is(err, csv.ErrBareQuote) ||
		errors.Is(err, csv.ErrQuote)
}

// HandleS3Event handles S3 event notifications from AWS Lambda.
func (h *S3EventHandler) HandleS3Event(ctx context.Context, event events.S3Event) error {
	ctx = bcontext.WithLogger(ctx, h.logger)

	for _, record := range event.Records {
		if err := h.processRecord(ctx, record); err != nil {
			return err
		}
	}

	return nil
}

type sanitizedError struct {
	msg  string
	orig error
}

func (e *sanitizedError) Error() string {
	return e.msg
}

func (e *sanitizedError) Is(target error) bool {
	return errors.Is(e.orig, target)
}

func sanitizeError(err error, rawURI string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if rawURI != "" {
		msg = strings.ReplaceAll(msg, rawURI, "[REDACTED_URI]")
	}
	msg = uriCredentialMask.ReplaceAllString(msg, "://[REDACTED]:[REDACTED]@")
	return &sanitizedError{
		msg:  msg,
		orig: err,
	}
}
