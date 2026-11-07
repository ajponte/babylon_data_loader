package ingest

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"

	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

// runner abstracts the pipeline orchestration seam for ingestion requests.
type runner interface {
	Run(ctx context.Context, req pipeline.Request) (*model.SyncLog, error)
	RecordFailure(ctx context.Context, sourceType string, meta model.FileMetadata, cause error) (*model.SyncLog, error)
}

// ErrNotCSV indicates that the target object is not a .csv file.
var ErrNotCSV = errors.New("ingest: object is not a .csv file")

// ObjectRequest defines parameters for ingesting an object store entry.
type ObjectRequest struct {
	Key        string
	ArchiveKey string
	SourceURI  string
	Override   *SourceRef
	Notifier   port.ProgressNotifier
}

// RunStats tracks batch ingestion metrics matching the legacy reporting shape.
type RunStats struct {
	TotalFiles     int               `json:"totalFiles"`
	ProcessedFiles int               `json:"processedFiles"`
	FailedFiles    int               `json:"failedFiles"`
	Failures       map[string]string `json:"failures"`
}

// Ingestor coordinates object download, source resolution, fingerprinting, pipeline execution, and archival.
type Ingestor struct {
	runner   runner
	store    port.ObjectStore
	now      func() time.Time
	maxBytes int64
}

// IngestorOption configures optional behavior on Ingestor.
type IngestorOption func(*Ingestor)

// WithClock configures a custom timestamp provider.
func WithClock(clock func() time.Time) IngestorOption {
	return func(i *Ingestor) {
		if clock != nil {
			i.now = clock
		}
	}
}

// WithMaxInputBytes configures the maximum stream size accepted during fingerprinting.
func WithMaxInputBytes(maxBytes int64) IngestorOption {
	return func(i *Ingestor) {
		if maxBytes > 0 {
			i.maxBytes = maxBytes
		}
	}
}

// NewIngestor creates a new object ingestion use case coordinator.
func NewIngestor(r runner, store port.ObjectStore, opts ...IngestorOption) *Ingestor {
	i := &Ingestor{runner: r, store: store, now: time.Now, maxBytes: DefaultMaxInputBytes}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// IngestObject ingests a single object from the object store and archives it if requested.
func (i *Ingestor) IngestObject(ctx context.Context, req ObjectRequest) (*model.SyncLog, error) {
	if !strings.EqualFold(path.Ext(req.Key), ".csv") {
		return nil, ErrNotCSV
	}

	ref, err := i.resolveRequestSource(req)
	if err != nil {
		return nil, err
	}

	stream, dlErr := i.store.DownloadStream(ctx, req.Key)
	if dlErr != nil {
		return nil, dlErr
	}
	defer stream.Close()

	body, hash, fpErr := Fingerprint(stream, i.maxBytes)
	if fpErr != nil {
		return i.handleFingerprintError(ctx, req, ref, fpErr)
	}

	meta := NewFileMetadata(req.Key, ref, hash, req.SourceURI, i.now())
	return i.runner.Run(ctx, pipeline.Request{
		Reader:     body,
		Meta:       meta,
		SourceType: ref.SourceType,
		Finalize:   i.makeFinalize(req),
		Notifier:   req.Notifier,
	})
}

func (i *Ingestor) resolveRequestSource(req ObjectRequest) (SourceRef, error) {
	if req.Override != nil {
		return *req.Override, nil
	}
	return ResolveSource(req.Key)
}

func (i *Ingestor) handleFingerprintError(
	ctx context.Context,
	req ObjectRequest,
	ref SourceRef,
	fpErr error,
) (*model.SyncLog, error) {
	if !errors.Is(fpErr, ErrInputTooLarge) {
		return nil, fpErr
	}
	meta := NewFileMetadata(req.Key, ref, "", req.SourceURI, i.now())
	failedLog, auditErr := i.runner.RecordFailure(ctx, ref.SourceType, meta, fpErr)
	if auditErr != nil {
		return failedLog, errors.Join(fpErr, auditErr)
	}
	return failedLog, fpErr
}

func (i *Ingestor) makeFinalize(req ObjectRequest) func(context.Context) error {
	if req.ArchiveKey == "" {
		return nil
	}
	return func(fCtx context.Context) error {
		return i.store.ArchiveFile(fCtx, req.Key, req.ArchiveKey)
	}
}

// IngestAll sequentially ingests a slice of object requests, continuing on individual failures.
func (i *Ingestor) IngestAll(ctx context.Context, reqs []ObjectRequest) RunStats {
	stats := RunStats{
		TotalFiles: len(reqs),
		Failures:   make(map[string]string),
	}
	for _, req := range reqs {
		if _, err := i.IngestObject(ctx, req); err == nil || errors.Is(err, pipeline.ErrAuditWrite) {
			stats.ProcessedFiles++
		} else {
			stats.FailedFiles++
			stats.Failures[path.Base(req.Key)] = err.Error()
		}
	}
	return stats
}

// ArchiveKeyFor derives an archive key by replacing fromPrefix with toPrefix.
// Returns ("", false) if key does not match fromPrefix.
func ArchiveKeyFor(key, fromPrefix, toPrefix string) (string, bool) {
	from := normalizePrefix(fromPrefix)
	if !strings.HasPrefix(key, from) {
		return "", false
	}
	rel := strings.TrimPrefix(key, from)
	return normalizePrefix(toPrefix) + rel, true
}

func normalizePrefix(p string) string {
	if p == "" || strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// RegisterDefaults registers standard extractors and destinations for Chase, Fidelity, and Synthetic sources.
func RegisterDefaults(p *pipeline.Pipeline, chase, fidelity, syntheticCSV port.Extractor) error {
	if p == nil || chase == nil || fidelity == nil || syntheticCSV == nil {
		return pipeline.ErrInvalidRegistration
	}
	for _, r := range []struct {
		sourceType string
		extractor  port.Extractor
		fallback   model.Destination
	}{
		{SourceChase, chase, model.DestChaseChecking},
		{SourceFidelity, fidelity, model.DestFidelityPosition},
		{SourceSynthetic, syntheticCSV, model.DestSynthetic},
	} {
		if err := p.Register(r.sourceType, r.extractor, r.fallback); err != nil {
			return err
		}
	}
	return nil
}
