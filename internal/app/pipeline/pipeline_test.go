package pipeline_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

type fakeRepo struct {
	mu           sync.Mutex
	saveBatchFn  func(ctx context.Context, batch *model.IngestionBatch) (*model.SyncLog, error)
	logSyncFn    func(ctx context.Context, log *model.SyncLog) error
	savedBatches []*model.IngestionBatch
	syncedLogs   []*model.SyncLog
}

func (f *fakeRepo) SaveBatch(ctx context.Context, b *model.IngestionBatch) (*model.SyncLog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.savedBatches = append(f.savedBatches, b)
	if f.saveBatchFn != nil {
		return f.saveBatchFn(ctx, b)
	}
	n := int64(b.RecordCount)
	return model.NewSuccessSyncLog(b.SourceFile, b.FileHash, string(b.Destination), n, n, 0, time.Now(), 10), nil
}

func (f *fakeRepo) LogSync(ctx context.Context, log *model.SyncLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncedLogs = append(f.syncedLogs, log)
	if f.logSyncFn != nil {
		return f.logSyncFn(ctx, log)
	}
	return nil
}

type fakeExtractor struct {
	extractFn func(ctx context.Context, r io.Reader, meta model.FileMetadata) (*model.IngestionBatch, error)
}

func (f *fakeExtractor) SourceType() string { return "chase" }
func (f *fakeExtractor) Extract(ctx context.Context, r io.Reader, meta model.FileMetadata) (*model.IngestionBatch, error) {
	if f.extractFn != nil {
		return f.extractFn(ctx, r, meta)
	}
	return &model.IngestionBatch{
		Destination: model.DestChaseChecking, AccountID: meta.AccountID,
		SourceFile: meta.FileName, FileHash: meta.FileHash,
		IngestedAt: meta.IngestedAt, RecordCount: 2, Payload: []model.ChaseTransaction{},
	}, nil
}

type fakeNotifier struct {
	mu     sync.Mutex
	stages []port.ProgressStage
}

func (f *fakeNotifier) NotifyProgress(_ context.Context, stage port.ProgressStage, _, _ int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stages = append(f.stages, stage)
}

func newValidMeta() model.FileMetadata {
	return model.FileMetadata{FileName: "chase.csv", FileHash: "hash123", AccountID: "1234", IngestedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func newValidTestRequest() pipeline.Request {
	return pipeline.Request{Reader: strings.NewReader("col\nval"), Meta: newValidMeta(), SourceType: "chase"}
}

func newTestHarness(opts ...pipeline.Option) (*pipeline.Pipeline, *fakeRepo, *fakeExtractor) {
	repo, ext := &fakeRepo{}, &fakeExtractor{}
	p := pipeline.New(repo, nil, opts...)
	_ = p.Register("chase", ext, model.DestChaseChecking)
	return p, repo, ext
}

func assertAudited(t *testing.T, repo *fakeRepo, wantStatus model.SyncStatus, wantColl string) {
	t.Helper()
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.syncedLogs) == 0 {
		t.Fatalf("expected audit log, got none")
	}
	last := repo.syncedLogs[len(repo.syncedLogs)-1]
	if last.Status != wantStatus || (wantColl != "" && last.CollectionName != wantColl) || last.Validate() != nil {
		t.Fatalf("unexpected or invalid audited log: %+v", last)
	}
}

func TestRegister_RejectsInvalid(t *testing.T) {
	_, _, ext := newTestHarness()
	p := pipeline.New(&fakeRepo{}, nil)
	for _, tc := range []struct {
		source string
		ext    port.Extractor
		dest   model.Destination
	}{
		{"", ext, model.DestChaseChecking},
		{"chase", nil, model.DestChaseChecking},
		{"chase", ext, "invalid"},
	} {
		if !errors.Is(p.Register(tc.source, tc.ext, tc.dest), pipeline.ErrInvalidRegistration) {
			t.Fatalf("Register(%q) expected ErrInvalidRegistration", tc.source)
		}
	}
	_ = p.Register("chase", ext, model.DestChaseChecking)
	if !errors.Is(p.Register("chase", ext, model.DestChaseChecking), pipeline.ErrDuplicateSourceType) {
		t.Fatalf("expected ErrDuplicateSourceType")
	}
}

func TestRun_UnknownSourceTypeNotAudited(t *testing.T) {
	p, repo, _ := newTestHarness()
	req := newValidTestRequest()
	req.SourceType = "unknown"
	if _, err := p.Run(context.Background(), req); !errors.Is(err, pipeline.ErrUnknownSourceType) {
		t.Fatalf("expected ErrUnknownSourceType, got %v", err)
	}
	if len(repo.syncedLogs) != 0 {
		t.Fatalf("expected no audits, got %d", len(repo.syncedLogs))
	}
}

func TestRun_InvalidRequest(t *testing.T) {
	p, repo, _ := newTestHarness()
	for _, tc := range []struct {
		modify  func(*pipeline.Request)
		wantErr error
	}{
		{func(r *pipeline.Request) { r.Reader = nil }, pipeline.ErrNilReader},
		{func(r *pipeline.Request) { r.Meta.FileName = "" }, model.ErrMissingFileName},
		{func(r *pipeline.Request) { r.Meta.AccountID = "" }, model.ErrMissingAccountID},
		{func(r *pipeline.Request) { r.Meta.IngestedAt = time.Time{} }, model.ErrMissingIngestedAt},
	} {
		req := newValidTestRequest()
		tc.modify(&req)
		if _, err := p.Run(context.Background(), req); !errors.Is(err, tc.wantErr) {
			t.Fatalf("expected %v, got %v", tc.wantErr, err)
		}
	}
	if len(repo.syncedLogs) != 0 {
		t.Fatalf("expected no audits, got %d", len(repo.syncedLogs))
	}
}

func TestRun_SuccessPersistsAndAudits(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	p, repo, _ := newTestHarness(pipeline.WithClock(func() time.Time { return fixed }))
	log, err := p.Run(context.Background(), newValidTestRequest())
	if err != nil || log.RecordCount != 2 || log.SyncedAt != fixed {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	assertAudited(t, repo, model.SyncStatusSuccess, "transactions_chase")
}

func TestRun_CopiesSourceURIToBatch(t *testing.T) {
	p, repo, _ := newTestHarness()
	req := newValidTestRequest()
	req.Meta.SourceURI = "s3://bucket/data.csv"
	if _, err := p.Run(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(repo.savedBatches) != 1 || repo.savedBatches[0].SourceURI != "s3://bucket/data.csv" {
		t.Fatalf("expected provenance copy, got %+v", repo.savedBatches)
	}
}

func TestRun_ExtractFailureAuditsWithFallbackCollection(t *testing.T) {
	p, repo, ext := newTestHarness()
	extErr := errors.New("parse error")
	ext.extractFn = func(_ context.Context, _ io.Reader, _ model.FileMetadata) (*model.IngestionBatch, error) {
		return nil, extErr
	}
	log, err := p.Run(context.Background(), newValidTestRequest())
	if !errors.Is(err, extErr) || log == nil || log.RecordCount != 0 {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	assertAudited(t, repo, model.SyncStatusFailed, "transactions_chase")
}

func TestRun_SaveBatchFailedLogIsAudited(t *testing.T) {
	p, repo, _ := newTestHarness()
	saveErr := errors.New("save failed")
	retLog := model.NewFailedSyncLog("chase.csv", "hash123", "transactions_chase", saveErr.Error(), 2, time.Time{}, 0)
	repo.saveBatchFn = func(_ context.Context, _ *model.IngestionBatch) (*model.SyncLog, error) { return retLog, saveErr }
	log, err := p.Run(context.Background(), newValidTestRequest())
	if !errors.Is(err, saveErr) || log.SyncedAt.IsZero() {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	assertAudited(t, repo, model.SyncStatusFailed, "transactions_chase")
}

func TestRun_SaveBatchNilLogBuildsFailedLog(t *testing.T) {
	p, repo, _ := newTestHarness()
	saveErr := errors.New("nil log save error")
	repo.saveBatchFn = func(_ context.Context, _ *model.IngestionBatch) (*model.SyncLog, error) { return nil, saveErr }
	log, err := p.Run(context.Background(), newValidTestRequest())
	if !errors.Is(err, saveErr) || log == nil || log.RecordCount != 2 {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	assertAudited(t, repo, model.SyncStatusFailed, "transactions_chase")
}

func TestRun_FailureAuditUsesDetachedContext(t *testing.T) {
	p, repo, ext := newTestHarness(pipeline.WithAuditTimeout(2 * time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	ext.extractFn = func(_ context.Context, _ io.Reader, _ model.FileMetadata) (*model.IngestionBatch, error) {
		cancel()
		return nil, errors.New("extract failed")
	}
	var auditCtxErr error
	var hasDeadline bool
	repo.logSyncFn = func(ctx context.Context, _ *model.SyncLog) error {
		auditCtxErr = ctx.Err()
		_, hasDeadline = ctx.Deadline()
		return nil
	}
	if _, err := p.Run(ctx, newValidTestRequest()); err == nil {
		t.Fatalf("expected extract error")
	}
	if auditCtxErr != nil || !hasDeadline {
		t.Fatalf("expected detached context, got err=%v, deadline=%v", auditCtxErr, hasDeadline)
	}
}

func TestRun_FinalizeRunsBeforeSuccessAudit(t *testing.T) {
	p, repo, _ := newTestHarness()
	var steps []string
	repo.saveBatchFn = func(_ context.Context, b *model.IngestionBatch) (*model.SyncLog, error) {
		steps = append(steps, "save")
		n := int64(b.RecordCount)
		return model.NewSuccessSyncLog(b.SourceFile, b.FileHash, string(b.Destination), n, n, 0, time.Now(), 1), nil
	}
	repo.logSyncFn = func(_ context.Context, _ *model.SyncLog) error { steps = append(steps, "audit"); return nil }
	req := newValidTestRequest()
	req.Finalize = func(_ context.Context) error { steps = append(steps, "finalize"); return nil }
	if _, err := p.Run(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Join(steps, "->") != "save->finalize->audit" {
		t.Fatalf("expected save->finalize->audit, got %v", steps)
	}
}

func TestRun_FinalizeFailureWritesSingleFailedLog(t *testing.T) {
	p, repo, _ := newTestHarness()
	finErr := errors.New("s3 archive timeout")
	req := newValidTestRequest()
	req.Finalize = func(_ context.Context) error { return finErr }
	log, err := p.Run(context.Background(), req)
	if !errors.Is(err, finErr) || log == nil || !strings.Contains(log.ErrorMessage, "file archival failed") {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	if len(repo.syncedLogs) != 1 {
		t.Fatalf("expected 1 audit log, got %d", len(repo.syncedLogs))
	}
	assertAudited(t, repo, model.SyncStatusFailed, "transactions_chase")
}

func TestRun_SuccessAuditFailureIsNonFatal(t *testing.T) {
	p, repo, _ := newTestHarness()
	auditErr := errors.New("mongo network error")
	repo.logSyncFn = func(_ context.Context, _ *model.SyncLog) error { return auditErr }
	log, err := p.Run(context.Background(), newValidTestRequest())
	if log == nil || log.Status != model.SyncStatusSuccess {
		t.Fatalf("expected success log, got %+v", log)
	}
	if !errors.Is(err, pipeline.ErrAuditWrite) || !errors.Is(err, auditErr) {
		t.Fatalf("expected wrapped ErrAuditWrite, got %v", err)
	}
}

func TestRun_FailureAuditFailureIsJoined(t *testing.T) {
	p, repo, ext := newTestHarness()
	extErr, auditErr := errors.New("parse error"), errors.New("audit disk full")
	ext.extractFn = func(_ context.Context, _ io.Reader, _ model.FileMetadata) (*model.IngestionBatch, error) {
		return nil, extErr
	}
	repo.logSyncFn = func(_ context.Context, _ *model.SyncLog) error { return auditErr }
	_, err := p.Run(context.Background(), newValidTestRequest())
	if !errors.Is(err, extErr) || !errors.Is(err, pipeline.ErrAuditWrite) || !errors.Is(err, auditErr) {
		t.Fatalf("expected joined errors, got %v", err)
	}
}

func TestRun_NotifierStages(t *testing.T) {
	defNotifier, reqNotifier := &fakeNotifier{}, &fakeNotifier{}
	p := pipeline.New(&fakeRepo{}, defNotifier)
	_ = p.Register("chase", &fakeExtractor{}, model.DestChaseChecking)
	req := newValidTestRequest()
	req.Notifier = reqNotifier
	if _, err := p.Run(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []port.ProgressStage{port.StageParsing, port.StageSaving, port.StageComplete}
	if len(defNotifier.stages) != 0 || len(reqNotifier.stages) != 3 || !slices.Equal(reqNotifier.stages, want) {
		t.Fatalf("unexpected stages: default=%v, req=%v", defNotifier.stages, reqNotifier.stages)
	}
}

func TestRun_CancelledContextUnwrapped(t *testing.T) {
	p, repo, _ := newTestHarness()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log, err := p.Run(ctx, newValidTestRequest())
	if log != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got log=%v, err=%v", log, err)
	}
	if len(repo.syncedLogs) != 0 {
		t.Fatalf("expected no audits, got %d", len(repo.syncedLogs))
	}
}

func TestRun_ConcurrentRunsRaceFree(t *testing.T) {
	p, _, _ := newTestHarness()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Run(context.Background(), newValidTestRequest())
		}()
	}
	wg.Wait()
}

func TestRecordFailure_AuditsWithFallbackAndEmptyHash(t *testing.T) {
	p, repo, _ := newTestHarness()
	_ = p.Register("fidelity", &fakeExtractor{}, model.DestFidelityPosition)
	cause := errors.New("file too large")
	meta := newValidMeta()
	meta.FileHash = "ignored-hash"
	log, err := p.RecordFailure(context.Background(), "fidelity", meta, cause)
	if err != nil || log == nil || log.FileHash != "" || log.ErrorMessage != cause.Error() {
		t.Fatalf("unexpected result: err=%v, log=%+v", err, log)
	}
	assertAudited(t, repo, model.SyncStatusFailed, "positions_fidelity")
}

func TestRecordFailure_UnknownSourceType(t *testing.T) {
	p, repo, _ := newTestHarness()
	_, err := p.RecordFailure(context.Background(), "unknown", newValidMeta(), errors.New("err"))
	if !errors.Is(err, pipeline.ErrUnknownSourceType) {
		t.Fatalf("expected ErrUnknownSourceType, got %v", err)
	}
	if len(repo.syncedLogs) != 0 {
		t.Fatalf("expected no audits, got %d", len(repo.syncedLogs))
	}
}
