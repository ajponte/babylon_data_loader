package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/app/pipeline"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

type trackingReadCloser struct {
	io.Reader

	closed bool
}

func (t *trackingReadCloser) Close() error {
	t.closed = true
	return nil
}

type fakeRunner struct {
	runFn                  func(context.Context, pipeline.Request) (*model.SyncLog, error)
	recordFailureFn        func(context.Context, string, model.FileMetadata, error) (*model.SyncLog, error)
	runCalls, failureCalls int
	lastRequest            pipeline.Request
	lastSourceType         string
	lastMeta               model.FileMetadata
}

func (f *fakeRunner) Run(ctx context.Context, req pipeline.Request) (*model.SyncLog, error) {
	f.runCalls++
	f.lastRequest = req
	if f.runFn != nil {
		return f.runFn(ctx, req)
	}
	return model.NewSuccessSyncLog(req.Meta.FileName, req.Meta.FileHash, "dest", 1, 1, 0, time.Now(), 1), nil
}

func (f *fakeRunner) RecordFailure(ctx context.Context, sourceType string, meta model.FileMetadata, cause error) (*model.SyncLog, error) {
	f.failureCalls++
	f.lastSourceType, f.lastMeta = sourceType, meta
	if f.recordFailureFn != nil {
		return f.recordFailureFn(ctx, sourceType, meta, cause)
	}
	return model.NewFailedSyncLog(meta.FileName, meta.FileHash, "dest", cause.Error(), 0, time.Now(), 1), nil
}

type fakeStore struct {
	downloadFn                func(context.Context, string) (io.ReadCloser, error)
	archiveFn                 func(context.Context, string, string) error
	downloadKeys              []string
	archivedSrc, archivedDest string
}

func (f *fakeStore) DownloadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	f.downloadKeys = append(f.downloadKeys, key)
	if f.downloadFn != nil {
		return f.downloadFn(ctx, key)
	}
	return &trackingReadCloser{Reader: strings.NewReader("date,amount\n2026-01-01,10.0\n")}, nil
}

func (f *fakeStore) ArchiveFile(ctx context.Context, srcKey, destKey string) error {
	f.archivedSrc, f.archivedDest = srcKey, destKey
	if f.archiveFn != nil {
		return f.archiveFn(ctx, srcKey, destKey)
	}
	return nil
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

func newTestIngestor(opts ...IngestorOption) (*fakeRunner, *fakeStore, *Ingestor) {
	r, s := &fakeRunner{}, &fakeStore{}
	return r, s, NewIngestor(r, s, opts...)
}

func TestIngestObject_ResolvesAndRuns(t *testing.T) {
	fixedTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	runner, _, ingestor := newTestIngestor(WithClock(func() time.Time { return fixedTime }))

	req := ObjectRequest{Key: "unprocessed/chase1234.csv", SourceURI: "s3://bucket/key"}
	syncLog, err := ingestor.IngestObject(context.Background(), req)
	if err != nil || syncLog == nil {
		t.Fatalf("unexpected failure: log=%v, err=%v", syncLog, err)
	}
	if runner.runCalls != 1 || runner.lastRequest.SourceType != SourceChase {
		t.Fatalf("run calls=%d, source=%s", runner.runCalls, runner.lastRequest.SourceType)
	}
	meta := runner.lastRequest.Meta
	if meta.FileName != "chase1234.csv" || meta.AccountID != "1234" || meta.SourceURI != req.SourceURI || !meta.IngestedAt.Equal(fixedTime) || meta.FileHash == "" {
		t.Errorf("metadata mismatch: %+v", meta)
	}
}

func TestIngestObject_OverrideSkipsResolution(t *testing.T) {
	runner, _, ingestor := newTestIngestor()
	req := ObjectRequest{Key: "unprocessed/custom_export.csv", Override: &SourceRef{SourceType: SourceChase, AccountID: "8888"}}
	if _, err := ingestor.IngestObject(context.Background(), req); err != nil {
		t.Fatalf("unexpected error with override: %v", err)
	}
	if runner.lastRequest.SourceType != SourceChase || runner.lastRequest.Meta.AccountID != "8888" {
		t.Errorf("override not applied: src=%s, acc=%s", runner.lastRequest.SourceType, runner.lastRequest.Meta.AccountID)
	}
}

func TestIngestObject_CancelledContext(t *testing.T) {
	runner, store, ingestor := newTestIngestor()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ingestor.IngestObject(ctx, ObjectRequest{Key: "unprocessed/chase1234.csv"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if len(store.downloadKeys) != 0 || runner.runCalls != 0 {
		t.Error("cancelled context triggered download/run")
	}
}

func TestIngestObject_RejectsNonCSV(t *testing.T) {
	runner, store, ingestor := newTestIngestor()
	for _, key := range []string{"unprocessed/chase1234.json", "unprocessed/chase1234", "chase1234.pdf"} {
		if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: key}); !errors.Is(err, ErrNotCSV) {
			t.Errorf("key %q: got error %v, want ErrNotCSV", key, err)
		}
	}
	if len(store.downloadKeys) != 0 || runner.runCalls != 0 {
		t.Errorf("rejected files triggered download/run: dl=%d, run=%d", len(store.downloadKeys), runner.runCalls)
	}
}

func TestIngestObject_Unresolvable(t *testing.T) {
	runner, store, ingestor := newTestIngestor()
	if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/unknown_statement.csv"}); !errors.Is(err, ErrUnresolvableSource) {
		t.Errorf("got %v, want ErrUnresolvableSource", err)
	}
	if len(store.downloadKeys) != 0 || runner.runCalls != 0 {
		t.Error("unresolvable source triggered store download")
	}
}

func TestIngestObject_DownloadNotFound(t *testing.T) {
	runner, store, ingestor := newTestIngestor()
	store.downloadFn = func(context.Context, string) (io.ReadCloser, error) { return nil, port.ErrObjectNotFound }
	if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/chase1234.csv"}); !errors.Is(err, port.ErrObjectNotFound) {
		t.Errorf("got %v, want port.ErrObjectNotFound", err)
	}
	if runner.runCalls != 0 || runner.failureCalls != 0 {
		t.Error("download failure invoked runner")
	}
}

func TestIngestObject_TooLargeIsAudited(t *testing.T) {
	tracker := &trackingReadCloser{Reader: strings.NewReader("12345678901234567890")}
	runner, store, ingestor := newTestIngestor(WithMaxInputBytes(10))
	store.downloadFn = func(context.Context, string) (io.ReadCloser, error) { return tracker, nil }
	req := ObjectRequest{Key: "unprocessed/chase1234.csv", SourceURI: "s3://bucket/key"}
	failedLog, err := ingestor.IngestObject(context.Background(), req)
	if !errors.Is(err, ErrInputTooLarge) || failedLog == nil || !tracker.closed {
		t.Fatalf("expected ErrInputTooLarge and closed tracker: err=%v, closed=%v", err, tracker.closed)
	}
	if runner.failureCalls != 1 || runner.runCalls != 0 || runner.lastSourceType != SourceChase || runner.lastMeta.FileHash != "" || runner.lastMeta.SourceURI != req.SourceURI {
		t.Errorf("failureCalls=%d, runCalls=%d, src=%s, hash=%q, uri=%q", runner.failureCalls, runner.runCalls, runner.lastSourceType, runner.lastMeta.FileHash, runner.lastMeta.SourceURI)
	}
}

func TestIngestObject_TooLargeAuditFailureJoined(t *testing.T) {
	runner, store, ingestor := newTestIngestor(WithMaxInputBytes(10))
	runner.recordFailureFn = func(context.Context, string, model.FileMetadata, error) (*model.SyncLog, error) {
		return nil, pipeline.ErrAuditWrite
	}
	store.downloadFn = func(context.Context, string) (io.ReadCloser, error) {
		return &trackingReadCloser{Reader: strings.NewReader("12345678901234567890")}, nil
	}
	if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/chase1234.csv"}); !errors.Is(err, ErrInputTooLarge) || !errors.Is(err, pipeline.ErrAuditWrite) {
		t.Fatalf("expected joined ErrInputTooLarge and ErrAuditWrite, got: %v", err)
	}
}

func TestIngestObject_FingerprintReadError(t *testing.T) {
	ioErr := errors.New("underlying network failure")
	runner, store, ingestor := newTestIngestor()
	store.downloadFn = func(context.Context, string) (io.ReadCloser, error) {
		return &trackingReadCloser{Reader: &errReader{err: ioErr}}, nil
	}
	if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/chase1234.csv"}); !errors.Is(err, ioErr) {
		t.Fatalf("got %v, want %v", err, ioErr)
	}
	if runner.failureCalls != 0 || runner.runCalls != 0 {
		t.Error("stream read failure should not be audited or run")
	}
}

func TestIngestObject_FinalizeArchives(t *testing.T) {
	runner, store, ingestor := newTestIngestor()
	req := ObjectRequest{Key: "unprocessed/chase1234.csv", ArchiveKey: "processed/chase1234.csv"}
	if _, err := ingestor.IngestObject(context.Background(), req); err != nil || runner.lastRequest.Finalize == nil {
		t.Fatalf("unexpected error or nil Finalize: %v", err)
	}
	if err := runner.lastRequest.Finalize(context.Background()); err != nil || store.archivedSrc != req.Key || store.archivedDest != req.ArchiveKey {
		t.Fatalf("Finalize mismatch: err=%v, src=%s, dest=%s", err, store.archivedSrc, store.archivedDest)
	}
}

func TestIngestObject_NoArchiveKeyNoFinalize(t *testing.T) {
	runner, _, ingestor := newTestIngestor()
	if _, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/chase1234.csv"}); err != nil || runner.lastRequest.Finalize != nil {
		t.Fatalf("err=%v, finalize is non-nil", err)
	}
}

func TestIngestObject_ClosesStream(t *testing.T) {
	errBoom := errors.New("boom")
	for _, tc := range []struct {
		name    string
		stream  io.Reader
		max     int64
		runErr  error
		wantErr error
	}{
		{"success", strings.NewReader("ok"), 0, nil, nil},
		{"empty stream", strings.NewReader(""), 0, nil, nil},
		{"run error", strings.NewReader("ok"), 0, errBoom, errBoom},
		{"read error", &errReader{err: io.ErrUnexpectedEOF}, 0, nil, io.ErrUnexpectedEOF},
		{"too large", strings.NewReader("too long content"), 5, nil, ErrInputTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &trackingReadCloser{Reader: tc.stream}
			store := &fakeStore{downloadFn: func(context.Context, string) (io.ReadCloser, error) { return tracker, nil }}
			runner := &fakeRunner{runFn: func(context.Context, pipeline.Request) (*model.SyncLog, error) { return nil, tc.runErr }}
			ingestor := NewIngestor(runner, store, WithMaxInputBytes(tc.max))
			_, err := ingestor.IngestObject(context.Background(), ObjectRequest{Key: "unprocessed/chase1234.csv"})
			if !tracker.closed {
				t.Errorf("%s: stream was not closed", tc.name)
			}
			if tc.wantErr == nil && err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			} else if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: error = %v, want %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

func TestIngestAll_ContinuesAndCounts(t *testing.T) {
	runner := &fakeRunner{runFn: func(_ context.Context, req pipeline.Request) (*model.SyncLog, error) {
		if req.Meta.FileName == "audit_fail.csv" {
			return nil, pipeline.ErrAuditWrite
		}
		if req.Meta.FileName == "fatal.csv" {
			return nil, errors.New("fatal run failure")
		}
		return model.NewSuccessSyncLog(req.Meta.FileName, req.Meta.FileHash, "dest", 1, 1, 0, time.Now(), 1), nil
	}}
	ingestor := NewIngestor(runner, &fakeStore{})
	reqs := []ObjectRequest{
		{Key: "unprocessed/chase1234.csv"},
		{Key: "unprocessed/fatal.csv", Override: &SourceRef{SourceType: SourceChase, AccountID: "2222"}},
		{Key: "unprocessed/audit_fail.csv", Override: &SourceRef{SourceType: SourceChase, AccountID: "1111"}},
		{Key: "unprocessed/invalid.txt"},
		{Key: "unprocessed/unknown.csv"},
	}
	stats := ingestor.IngestAll(context.Background(), reqs)
	if stats.TotalFiles != 5 || stats.ProcessedFiles != 2 || stats.FailedFiles != 3 || len(stats.Failures) != 3 {
		t.Fatalf("stats mismatch: %+v", stats)
	}
	for _, expectedKey := range []string{"invalid.txt", "unknown.csv", "fatal.csv"} {
		if _, ok := stats.Failures[expectedKey]; !ok {
			t.Errorf("missing failure entry for %s", expectedKey)
		}
	}
}

func TestRunStats_JSONMatchesLegacy(t *testing.T) {
	stats := RunStats{TotalFiles: 2, ProcessedFiles: 1, FailedFiles: 1, Failures: map[string]string{"err.csv": "parse error"}}
	data, err := json.Marshal(stats)
	wantJSON := `{"totalFiles":2,"processedFiles":1,"failedFiles":1,"failures":{"err.csv":"parse error"}}`
	if err != nil || string(data) != wantJSON {
		t.Errorf("json mismatch: got %s (err=%v), want %s", string(data), err, wantJSON)
	}
}

func TestArchiveKeyFor(t *testing.T) {
	for _, tc := range []struct {
		key, from, to, wantKey string
		wantOK                 bool
	}{
		{"unprocessed/chase1234.csv", "unprocessed", "processed", "processed/chase1234.csv", true},
		{"unprocessed/chase1234.csv", "unprocessed", "", "chase1234.csv", true},
		{"unprocessed/2026/01/report.csv", "unprocessed/", "processed", "processed/2026/01/report.csv", true},
		{"unprocessed/nested/dir/f.csv", "unprocessed", "archive/", "archive/nested/dir/f.csv", true},
		{"other/file.csv", "unprocessed", "processed", "", false},
		{"unprocessed_file.csv", "unprocessed", "processed", "", false},
		{"", "unprocessed", "processed", "", false},
		{"unprocessed/f.csv", "", "processed", "", false},
	} {
		gotKey, gotOK := ArchiveKeyFor(tc.key, tc.from, tc.to)
		if gotKey != tc.wantKey || gotOK != tc.wantOK {
			t.Errorf("ArchiveKeyFor(%q, %q, %q) = (%q, %v), want (%q, %v)", tc.key, tc.from, tc.to, gotKey, gotOK, tc.wantKey, tc.wantOK)
		}
	}
}

type fakeExtractor struct {
	called     bool
	sourceType string
	dest       model.Destination
}

func (f *fakeExtractor) SourceType() string { return f.sourceType }

func (f *fakeExtractor) Extract(context.Context, io.Reader, model.FileMetadata) (*model.IngestionBatch, error) {
	f.called = true
	return &model.IngestionBatch{
		Destination: f.dest, AccountID: "0000", SourceFile: "f.csv", IngestedAt: time.Now(), RecordCount: 1, Payload: []any{1},
	}, nil
}

type dummyRepo struct{}

func (d *dummyRepo) SaveBatch(context.Context, *model.IngestionBatch) (*model.SyncLog, error) {
	return model.NewSuccessSyncLog("f", "h", "c", 1, 1, 0, time.Now(), 1), nil
}

func (d *dummyRepo) LogSync(context.Context, *model.SyncLog) error { return nil }

func TestRegisterDefaults_Routing(t *testing.T) {
	chaseExt := &fakeExtractor{sourceType: SourceChase, dest: model.DestChaseChecking}
	fidelityExt := &fakeExtractor{sourceType: SourceFidelity, dest: model.DestFidelityPosition}
	syntheticExt := &fakeExtractor{sourceType: SourceSynthetic, dest: model.DestSynthetic}
	p := pipeline.New(&dummyRepo{}, nil)

	if err := RegisterDefaults(p, chaseExt, fidelityExt, syntheticExt); err != nil {
		t.Fatalf("RegisterDefaults failed: %v", err)
	}

	for _, tc := range []struct {
		src, acc, file string
		ext            *fakeExtractor
	}{
		{SourceChase, "1234", "chase1234.csv", chaseExt},
		{SourceFidelity, "portfolio", "fidelity.csv", fidelityExt},
		{SourceSynthetic, "0000", "synthetic.csv", syntheticExt},
	} {
		meta := NewFileMetadata(tc.file, SourceRef{SourceType: tc.src, AccountID: tc.acc}, "hash", "uri", time.Now())
		if _, err := p.Run(context.Background(), pipeline.Request{
			Reader:     strings.NewReader("col\nval\n"),
			Meta:       meta,
			SourceType: tc.src,
		}); err != nil || !tc.ext.called {
			t.Fatalf("%s run failed (called=%v): %v", tc.src, tc.ext.called, err)
		}
	}

	meta := NewFileMetadata("fallback.csv", SourceRef{SourceType: SourceChase, AccountID: "0000"}, "", "uri", time.Now())
	for src, wantDest := range map[string]model.Destination{
		SourceChase:     model.DestChaseChecking,
		SourceFidelity:  model.DestFidelityPosition,
		SourceSynthetic: model.DestSynthetic,
	} {
		failedLog, recErr := p.RecordFailure(context.Background(), src, meta, errors.New("err"))
		if recErr != nil || failedLog.CollectionName != string(wantDest) {
			t.Errorf("%s collection = %q, want %q", src, failedLog.CollectionName, wantDest)
		}
	}

	for _, tc := range []struct {
		name        string
		pipe        *pipeline.Pipeline
		ch, fid, sy port.Extractor
	}{
		{"nil pipeline", nil, chaseExt, fidelityExt, syntheticExt},
		{"nil chase", p, nil, fidelityExt, syntheticExt},
		{"nil fidelity", p, chaseExt, nil, syntheticExt},
		{"nil synthetic", p, chaseExt, fidelityExt, nil},
	} {
		if err := RegisterDefaults(tc.pipe, tc.ch, tc.fid, tc.sy); !errors.Is(err, pipeline.ErrInvalidRegistration) {
			t.Errorf("%s: got %v, want ErrInvalidRegistration", tc.name, err)
		}
	}
}
