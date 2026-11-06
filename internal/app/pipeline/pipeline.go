package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

var (
	ErrUnknownSourceType   = errors.New("unknown source type")
	ErrDuplicateSourceType = errors.New("source type already registered")
	ErrInvalidRegistration = errors.New("invalid extractor registration")
	ErrNilReader           = errors.New("reader cannot be nil")
	ErrAuditWrite          = errors.New("audit log write failed")
	ErrNilBatch            = errors.New("extractor returned nil batch")
	ErrNilSyncLog          = errors.New("repository returned nil sync log on success")
)

// DefaultAuditTimeout is the maximum duration allocated for detached audit logging.
const DefaultAuditTimeout = 5 * time.Second

// Request encapsulates the input stream and metadata for a pipeline ingestion execution.
type Request struct {
	Reader     io.Reader
	Meta       model.FileMetadata
	SourceType string
	Finalize   func(ctx context.Context) error
	Notifier   port.ProgressNotifier
}

type registration struct {
	extractor port.Extractor
	fallback  model.Destination
}

// Option configures pipeline behavior.
type Option func(*Pipeline)

// Pipeline coordinates stream extraction, batch persistence, and audit logging.
type Pipeline struct {
	repo         port.Repository
	notifier     port.ProgressNotifier
	auditTimeout time.Duration
	now          func() time.Time

	mu       sync.RWMutex
	registry map[string]registration
}

// New creates a new application pipeline orchestrator.
func New(repo port.Repository, notifier port.ProgressNotifier, opts ...Option) *Pipeline {
	p := &Pipeline{
		repo:         repo,
		notifier:     notifier,
		auditTimeout: DefaultAuditTimeout,
		now:          time.Now,
		registry:     make(map[string]registration),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WithClock overrides the timestamp generator used for audit durations.
func WithClock(clock func() time.Time) Option {
	return func(p *Pipeline) {
		if clock != nil {
			p.now = clock
		}
	}
}

// WithAuditTimeout configures the timeout for detached failure audit persistence.
func WithAuditTimeout(timeout time.Duration) Option {
	return func(p *Pipeline) {
		if timeout > 0 {
			p.auditTimeout = timeout
		}
	}
}

// Register registers an extractor and fallback destination for a given source type.
func (p *Pipeline) Register(sourceType string, ext port.Extractor, fallback model.Destination) error {
	if sourceType == "" || ext == nil || !fallback.IsValid() {
		return ErrInvalidRegistration
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.registry[sourceType]; exists {
		return ErrDuplicateSourceType
	}
	p.registry[sourceType] = registration{extractor: ext, fallback: fallback}
	return nil
}

func (p *Pipeline) getRegistration(sourceType string) (registration, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	r, ok := p.registry[sourceType]
	return r, ok
}

func (p *Pipeline) timeNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *Pipeline) elapsedMs(start time.Time) int64 {
	return max(0, p.timeNow().Sub(start).Milliseconds())
}

func (p *Pipeline) restamp(log *model.SyncLog, start time.Time) {
	log.SyncedAt = start.UTC()
	log.DurationMs = p.elapsedMs(start)
}

func (p *Pipeline) notify(ctx context.Context, notif port.ProgressNotifier, stage port.ProgressStage, cur, total int) {
	if notif != nil {
		notif.NotifyProgress(ctx, stage, cur, total)
	} else if p.notifier != nil {
		p.notifier.NotifyProgress(ctx, stage, cur, total)
	}
}

func (p *Pipeline) auditDetached(ctx context.Context, log *model.SyncLog) error {
	detachedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.auditTimeout)
	defer cancel()
	return p.repo.LogSync(detachedCtx, log)
}

func (p *Pipeline) logFailure(ctx context.Context, log *model.SyncLog, cause error) (*model.SyncLog, error) {
	if auditErr := p.auditDetached(ctx, log); auditErr != nil {
		return log, errors.Join(cause, fmt.Errorf("%w: %w", ErrAuditWrite, auditErr))
	}
	return log, cause
}

func (p *Pipeline) newFailedLog(
	meta model.FileMetadata,
	dest string,
	err error,
	count int,
	start time.Time,
) *model.SyncLog {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}
	return model.NewFailedSyncLog(
		meta.FileName,
		meta.FileHash,
		dest,
		msg,
		int64(count),
		start.UTC(),
		p.elapsedMs(start),
	)
}

func resolveDestination(batch *model.IngestionBatch, fallback model.Destination) string {
	if batch != nil && batch.Destination != "" {
		return string(batch.Destination)
	}
	return string(fallback)
}

func (p *Pipeline) validateRequest(ctx context.Context, req Request) (registration, error) {
	if err := ctx.Err(); err != nil {
		return registration{}, err
	}
	if req.Reader == nil {
		return registration{}, ErrNilReader
	}
	if err := req.Meta.Validate(); err != nil {
		return registration{}, err
	}
	reg, ok := p.getRegistration(req.SourceType)
	if !ok {
		return registration{}, ErrUnknownSourceType
	}
	return reg, nil
}

// Run executes a complete ingestion request across extract, persist, and audit stages.
func (p *Pipeline) Run(ctx context.Context, req Request) (*model.SyncLog, error) {
	reg, err := p.validateRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	start := p.timeNow().UTC()
	p.notify(ctx, req.Notifier, port.StageParsing, 0, 0)

	batch, err := reg.extractor.Extract(ctx, req.Reader, req.Meta)
	if err != nil {
		return p.logFailure(ctx, p.newFailedLog(req.Meta, string(reg.fallback), err, 0, start), err)
	}
	if batch == nil {
		return p.logFailure(ctx, p.newFailedLog(req.Meta, string(reg.fallback), ErrNilBatch, 0, start), ErrNilBatch)
	}

	batch.SourceURI = req.Meta.SourceURI
	if valErr := batch.Validate(); valErr != nil {
		coll := resolveDestination(batch, reg.fallback)
		return p.logFailure(ctx, p.newFailedLog(req.Meta, coll, valErr, batch.RecordCount, start), valErr)
	}

	p.notify(ctx, req.Notifier, port.StageSaving, 0, batch.RecordCount)

	syncLog, err := p.repo.SaveBatch(ctx, batch)
	if err != nil {
		if syncLog == nil {
			syncLog = p.newFailedLog(req.Meta, resolveDestination(batch, reg.fallback), err, batch.RecordCount, start)
		} else {
			p.restamp(syncLog, start)
		}
		return p.logFailure(ctx, syncLog, err)
	}
	if syncLog == nil {
		coll := resolveDestination(batch, reg.fallback)
		return p.logFailure(ctx, p.newFailedLog(req.Meta, coll, ErrNilSyncLog, batch.RecordCount, start), ErrNilSyncLog)
	}

	if req.Finalize != nil {
		if finErr := req.Finalize(ctx); finErr != nil {
			cause := fmt.Errorf("storage succeeded but file archival failed: %w", finErr)
			coll := resolveDestination(batch, reg.fallback)
			return p.logFailure(ctx, p.newFailedLog(req.Meta, coll, cause, batch.RecordCount, start), cause)
		}
	}

	p.restamp(syncLog, start)
	if auditErr := p.repo.LogSync(ctx, syncLog); auditErr != nil {
		return syncLog, fmt.Errorf("%w: %w", ErrAuditWrite, auditErr)
	}

	p.notify(ctx, req.Notifier, port.StageComplete, batch.RecordCount, batch.RecordCount)
	return syncLog, nil
}

// RecordFailure writes a failure audit record for errors occurring prior to batch creation.
func (p *Pipeline) RecordFailure(
	ctx context.Context,
	src string,
	meta model.FileMetadata,
	cause error,
) (*model.SyncLog, error) {
	reg, ok := p.getRegistration(src)
	if !ok {
		return nil, ErrUnknownSourceType
	}
	meta.FileHash = ""
	failedLog := p.newFailedLog(meta, string(reg.fallback), cause, 0, p.timeNow().UTC())
	if auditErr := p.auditDetached(ctx, failedLog); auditErr != nil {
		return failedLog, errors.Join(cause, fmt.Errorf("%w: %w", ErrAuditWrite, auditErr))
	}
	return failedLog, nil
}
