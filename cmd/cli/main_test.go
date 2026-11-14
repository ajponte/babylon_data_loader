package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/sources/synthetic"
)

const inlineLegacySyntheticCSV = `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,10/31/2026,Synthetic grocery,Shopping,-45.50,DEBIT,3500.00,
CREDIT,10/30/2026,Synthetic payroll,Income,1200.00,CREDIT,4700.00,
`

const inlineFidelityPositionsCSV = `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,SPAXX**,HELD IN MONEY MARKET,,,,$3250.00,,,,,25.00%,,,Cash,
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash,
SYNTH-ROTH-101,ROTH IRA,FBGRX,FIDELITY BLUE CHIP GROWTH,10.000,$296.25,+$2.10,$2962.50,+$21.00,+0.71%,+$462.50,+18.50%,22.79%,$2500.00,$250.00,Cash,
SYNTH-401K-202,TRADITIONAL 401K,FXAIX,FIDELITY 500 INDEX,25.500,$180.20,-$0.80,$4595.10,-$20.40,-0.44%,+$595.10,+14.88%,100.00%,$4000.00,$156.86,Cash,

"Date downloaded Sep-25-2026 9:50 p.m ET"
`

type fakeRepo struct {
	mu           sync.Mutex
	batches      []*model.IngestionBatch
	syncLogs     []*model.SyncLog
	saveBatchErr error
	logSyncErr   error
	closeErr     error
	closed       bool
}

func (f *fakeRepo) SaveBatch(_ context.Context, batch *model.IngestionBatch) (*model.SyncLog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveBatchErr != nil {
		return nil, f.saveBatchErr
	}
	f.batches = append(f.batches, batch)
	return model.NewSuccessSyncLog(
		batch.SourceFile,
		batch.FileHash,
		string(batch.Destination),
		int64(batch.RecordCount),
		int64(batch.RecordCount),
		0,
		time.Now(),
		10,
	), nil
}

func (f *fakeRepo) LogSync(_ context.Context, log *model.SyncLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncLogs = append(f.syncLogs, log)
	return f.logSyncErr
}

func (f *fakeRepo) Close(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

type stubStore struct{}

func (s *stubStore) DownloadStream(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *stubStore) ArchiveFile(_ context.Context, _, _ string) error {
	return nil
}

func (s *stubStore) List(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func fixedClock() time.Time {
	return time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
}

func copyFixture(t *testing.T, src, dest string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		data = []byte(inlineFidelityPositionsCSV)
	}
	if writeErr := os.WriteFile(dest, data, 0o600); writeErr != nil {
		t.Fatalf("failed to write test file %s: %v", dest, writeErr)
	}
}

func setupIngestDirs(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	unprocessed := filepath.Join(root, "unprocessed")
	processed := filepath.Join(root, "processed")

	if err := os.MkdirAll(unprocessed, 0o750); err != nil {
		t.Fatalf("failed to create unprocessed dir: %v", err)
	}
	if err := os.MkdirAll(processed, 0o750); err != nil {
		t.Fatalf("failed to create processed dir: %v", err)
	}

	t.Setenv("CSV_DIR", root)
	t.Setenv("UNPROCESSED_DIR", "unprocessed")
	t.Setenv("PROCESSED_DIR", "processed")
	t.Setenv("MOVE_PROCESSED_FILES", "false")
	return unprocessed, processed
}

func TestRun_UsageAndUnknownCommand(t *testing.T) {
	t.Parallel()

	connectCalled := false
	storeCalled := false
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			connectCalled = true
			return &fakeRepo{}, nil
		},
		newStore: func(_ string) (objectStore, error) {
			storeCalled = true
			return &stubStore{}, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	tests := []struct {
		name      string
		args      []string
		targetErr error
		wantErr   string
	}{
		{
			name:      "empty args returns usage",
			args:      []string{},
			targetErr: ErrUsage,
			wantErr:   "usage: data-loader <command> [options]",
		},
		{
			name:    "unknown command returns error",
			args:    []string{"unknown-cmd"},
			wantErr: "unknown command: unknown-cmd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := run(context.Background(), tt.args, d)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
			if tt.targetErr != nil && !errors.Is(err, tt.targetErr) {
				t.Fatalf("expected errors.Is(%v), got %v", tt.targetErr, err)
			}
			if connectCalled || storeCalled {
				t.Errorf("factory called unexpectedly (connect=%v, store=%v)", connectCalled, storeCalled)
			}
		})
	}
}

func TestRun_IngestWiresPipeline(t *testing.T) {
	unprocessed, _ := setupIngestDirs(t)

	fidelityFixture := filepath.Join("..", "..", "testdata", "fidelity", "Portfolio_Positions_Synthetic.csv")
	copyFixture(t, fidelityFixture, filepath.Join(unprocessed, "Portfolio_Positions_Synthetic.csv"))

	synthFile := filepath.Join(unprocessed, "test_synthetic.csv")
	if err := os.WriteFile(synthFile, []byte(inlineLegacySyntheticCSV), 0o600); err != nil {
		t.Fatalf("failed to write synthetic csv: %v", err)
	}

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	if err := run(context.Background(), []string{"ingest"}, d); err != nil {
		t.Fatalf("run ingest failed unexpectedly: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if !repo.closed {
		t.Errorf("expected repository to be closed")
	}
	if len(repo.batches) != 2 {
		t.Fatalf("expected 2 batches saved, got %d", len(repo.batches))
	}

	var fidelityBatch, synthBatch *model.IngestionBatch
	for _, b := range repo.batches {
		//nolint:exhaustive // DestChaseCreditCard and DestChaseChecking share the same value.
		switch b.Destination {
		case model.DestFidelityPosition:
			fidelityBatch = b
		case model.DestSynthetic:
			synthBatch = b
		default:
		}
	}

	assertFidelityBatch(t, fidelityBatch)
	assertSyntheticBatch(t, synthBatch)
}

func assertFidelityBatch(t *testing.T, fidelityBatch *model.IngestionBatch) {
	t.Helper()
	if fidelityBatch == nil {
		t.Fatalf("expected batch for destination %s", model.DestFidelityPosition)
	}
	if fidelityBatch.RecordCount == 0 {
		t.Errorf("expected fidelity batch to have records, got 0")
	}

	fidelityPositions, err := fidelityBatch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to deserialize fidelity positions: %v", err)
	}
	if len(fidelityPositions) == 0 {
		t.Fatalf("expected non-empty fidelity positions list")
	}
	for _, pos := range fidelityPositions {
		if pos.Symbol == "SPAXX**" {
			if pos.CurrentValue != 3250.00 {
				t.Errorf("expected SPAXX** CurrentValue 3250.00, got %v", pos.CurrentValue)
			}
			if pos.AsOfDate.IsZero() {
				t.Errorf("expected non-zero AsOfDate for SPAXX** position")
			}
			return
		}
	}
	t.Errorf("expected to find SPAXX** position in deserialized fidelity batch")
}

func assertSyntheticBatch(t *testing.T, synthBatch *model.IngestionBatch) {
	t.Helper()
	if synthBatch == nil {
		t.Fatalf("expected batch for destination %s", model.DestSynthetic)
	}
	if synthBatch.RecordCount != 2 {
		t.Fatalf("expected synthetic batch count 2, got %d", synthBatch.RecordCount)
	}

	txs, err := synthBatch.AsChaseTransactions()
	if err != nil {
		t.Fatalf("failed to deserialize synthetic transactions: %v", err)
	}
	if len(txs) != 2 {
		t.Fatalf("expected 2 synthetic transactions, got %d", len(txs))
	}

	tx0 := txs[0]
	if tx0.Details != "DEBIT" {
		t.Errorf("expected details DEBIT, got %q", tx0.Details)
	}
	if tx0.Balance == nil || *tx0.Balance != 3500.00 {
		t.Errorf("expected balance 3500.00, got %v", tx0.Balance)
	}
	if tx0.Description != "Synthetic grocery" {
		t.Errorf("expected description 'Synthetic grocery', got %q", tx0.Description)
	}
	if tx0.Amount != -45.50 {
		t.Errorf("expected amount -45.50, got %v", tx0.Amount)
	}
}

func TestRun_IngestEmptyDirectory(t *testing.T) {
	setupIngestDirs(t)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: logger,
		now:    fixedClock,
	}

	if err := run(context.Background(), []string{"ingest"}, d); err != nil {
		t.Fatalf("expected run ingest on empty dir to succeed, got: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if !repo.closed {
		t.Errorf("expected repository to be closed")
	}
	if len(repo.batches) != 0 {
		t.Errorf("expected 0 batches saved, got %d", len(repo.batches))
	}

	output := buf.String()
	if !strings.Contains(output, `"totalFiles": 0`) && !strings.Contains(output, `\"totalFiles\": 0`) {
		t.Errorf("expected log output to contain '\"totalFiles\": 0', got:\n%s", output)
	}
}

func TestRun_IngestContextCancelled(t *testing.T) {
	setupIngestDirs(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(ctx context.Context, _ string) (repository, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(ctx, []string{"ingest"}, d)
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is context.Canceled, got: %v", err)
	}
}

func TestRun_IngestArchivesWhenConfigured(t *testing.T) {
	unprocessed, processed := setupIngestDirs(t)
	t.Setenv("MOVE_PROCESSED_FILES", "true")

	synthFile := filepath.Join(unprocessed, "test_synthetic.csv")
	if err := os.WriteFile(synthFile, []byte(inlineLegacySyntheticCSV), 0o600); err != nil {
		t.Fatalf("failed to write synthetic csv: %v", err)
	}

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	if err := run(context.Background(), []string{"ingest"}, d); err != nil {
		t.Fatalf("run ingest failed: %v", err)
	}

	if _, err := os.Stat(synthFile); !os.IsNotExist(err) {
		t.Errorf("expected source file to be removed after archival")
	}
	archived := filepath.Join(processed, "test_synthetic.csv")
	if _, err := os.Stat(archived); err != nil {
		t.Errorf("expected archived file at %s, got error: %v", archived, err)
	}
}

func TestRun_IngestNoArchiveWhenDisabled(t *testing.T) {
	unprocessed, processed := setupIngestDirs(t)
	t.Setenv("MOVE_PROCESSED_FILES", "false")

	synthFile := filepath.Join(unprocessed, "test_synthetic.csv")
	if err := os.WriteFile(synthFile, []byte(inlineLegacySyntheticCSV), 0o600); err != nil {
		t.Fatalf("failed to write synthetic csv: %v", err)
	}

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	if err := run(context.Background(), []string{"ingest"}, d); err != nil {
		t.Fatalf("run ingest failed: %v", err)
	}

	if _, err := os.Stat(synthFile); err != nil {
		t.Errorf("expected source file to remain in unprocessed dir: %v", err)
	}
	archived := filepath.Join(processed, "test_synthetic.csv")
	if _, err := os.Stat(archived); !os.IsNotExist(err) {
		t.Errorf("expected processed file to NOT exist")
	}
}

func TestRun_IngestRespectsDirEnvOverrides(t *testing.T) {
	root := t.TempDir()
	inDir := filepath.Join(root, "custom_in")
	outDir := filepath.Join(root, "custom_out")
	if err := os.MkdirAll(inDir, 0o750); err != nil {
		t.Fatalf("failed to create custom_in: %v", err)
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		t.Fatalf("failed to create custom_out: %v", err)
	}

	t.Setenv("CSV_DIR", root)
	t.Setenv("UNPROCESSED_DIR", "custom_in")
	t.Setenv("PROCESSED_DIR", "custom_out")
	t.Setenv("MOVE_PROCESSED_FILES", "true")

	file := filepath.Join(inDir, "custom_synthetic.csv")
	if err := os.WriteFile(file, []byte(inlineLegacySyntheticCSV), 0o600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	if err := run(context.Background(), []string{"ingest"}, d); err != nil {
		t.Fatalf("run ingest failed: %v", err)
	}

	archived := filepath.Join(outDir, "custom_synthetic.csv")
	if _, err := os.Stat(archived); err != nil {
		t.Errorf("expected archived file at %s: %v", archived, err)
	}
}

func TestRun_IngestMissingDirFails(t *testing.T) {
	t.Setenv("CSV_DIR", filepath.Join(t.TempDir(), "nonexistent"))
	t.Setenv("UNPROCESSED_DIR", "unprocessed")

	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return &fakeRepo{}, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(context.Background(), []string{"ingest"}, d)
	if err == nil {
		t.Fatalf("expected error for missing unprocessed dir, got nil")
	}
}

func TestRun_ConnectFailureDoesNotLeakURI(t *testing.T) {
	sensitiveURI := "mongodb://dbuser:supersecretpass@atlas-cluster.example.com:27017/datalake"
	t.Setenv("MONGO_URI", sensitiveURI)

	d := deps{
		connectRepo: func(_ context.Context, uri string) (repository, error) {
			return nil, fmt.Errorf("dial tcp: failed to connect to %s", uri)
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(context.Background(), []string{"ingest"}, d)
	if err == nil {
		t.Fatalf("expected connect error, got nil")
	}

	errStr := err.Error()
	if strings.Contains(errStr, "supersecretpass") {
		t.Errorf("error leaked sensitive password: %s", errStr)
	}
	if strings.Contains(errStr, sensitiveURI) {
		t.Errorf("error leaked full raw URI: %s", errStr)
	}
}

func TestRun_PersistSyntheticRoutesToSyntheticCollection(t *testing.T) {
	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	args := []string{"generate-synthetic-data", "--rows", "5", "--persist-to-mongo"}
	if err := run(context.Background(), args, d); err != nil {
		t.Fatalf("run generate-synthetic-data failed: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.batches) != 1 {
		t.Fatalf("expected 1 batch persisted, got %d", len(repo.batches))
	}
	batch := repo.batches[0]
	if batch.Destination != model.DestSynthetic {
		t.Errorf("expected destination %s, got %s", model.DestSynthetic, batch.Destination)
	}
	if batch.RecordCount != 5 {
		t.Errorf("expected 5 records in batch, got %d", batch.RecordCount)
	}
}

func TestRun_GenerateSyntheticDataToDir(t *testing.T) {
	outDir := t.TempDir()
	d := deps{
		logger: testLogger(),
		now:    fixedClock,
	}

	args := []string{"generate-synthetic-data", "--rows", "3", "--dir", outDir}
	if err := run(context.Background(), args, d); err != nil {
		t.Fatalf("run generate-synthetic-data to dir failed: %v", err)
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("failed to read outDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected generated synthetic file in %s", outDir)
	}
}

func TestRun_GenerateSyntheticDataFlagError(t *testing.T) {
	d := deps{
		logger: testLogger(),
		now:    fixedClock,
	}

	args := []string{"generate-synthetic-data", "--invalid-nonexistent-flag"}
	err := run(context.Background(), args, d)
	if err == nil {
		t.Fatalf("expected flag parsing error, got nil")
	}
}

func TestRun_GenerateSyntheticData_InvalidRows(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "should-not-exist")
	d := deps{
		logger: testLogger(),
		now:    fixedClock,
	}

	tests := []struct {
		name    string
		rows    string
		wantErr string
	}{
		{
			name:    "negative rows rejected",
			rows:    "-1",
			wantErr: "invalid rows -1: must be between 0 and",
		},
		{
			name:    "excessive rows rejected",
			rows:    strconv.Itoa(synthetic.MaxSyntheticRowCount + 1),
			wantErr: "must be between 0 and",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"generate-synthetic-data", "--rows", tc.rows, "--dir", outDir}
			err := run(context.Background(), args, d)
			if err == nil {
				t.Fatalf("expected error for rows %s, got nil", tc.rows)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
			if _, statErr := os.Stat(outDir); !os.IsNotExist(statErr) {
				t.Errorf("expected dir %s not to be created, statErr=%v", outDir, statErr)
			}
		})
	}
}

func TestRun_PartialFailureExitZero(t *testing.T) {
	unprocessed, _ := setupIngestDirs(t)

	validFile := filepath.Join(unprocessed, "valid_synthetic.csv")
	if err := os.WriteFile(validFile, []byte(inlineLegacySyntheticCSV), 0o600); err != nil {
		t.Fatalf("failed to write valid csv: %v", err)
	}

	invalidFile := filepath.Join(unprocessed, "broken_file.csv")
	if err := os.WriteFile(invalidFile, []byte("Unrecognized header,column2\nsome,val\n"), 0o600); err != nil {
		t.Fatalf("failed to write invalid csv: %v", err)
	}

	repo := &fakeRepo{}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(context.Background(), []string{"ingest"}, d)
	if err != nil {
		t.Fatalf("expected run to exit 0 (nil error) on partial failure, got: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.batches) != 1 {
		t.Fatalf("expected 1 valid batch persisted despite broken file, got %d", len(repo.batches))
	}
}

func TestSlogNotifier_LogsStages(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	notifier := &slogNotifier{logger: logger}
	ctx := context.Background()

	stages := []struct {
		stage   port.ProgressStage
		current int
		total   int
	}{
		{port.StageParsing, 1, 10},
		{port.StageSaving, 10, 10},
		{port.StageComplete, 10, 10},
	}

	for _, s := range stages {
		notifier.NotifyProgress(ctx, s.stage, s.current, s.total)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(stages) {
		t.Fatalf("expected %d log lines, got %d", len(stages), len(lines))
	}

	piiKeys := []string{"account_id", "balance", "details", "description", "ssn", "amount"}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to parse log line as json: %v", err)
		}

		if record["stage"] != string(stages[i].stage) {
			t.Errorf("line %d: expected stage %q, got %q", i, stages[i].stage, record["stage"])
		}
		if int(record["current"].(float64)) != stages[i].current {
			t.Errorf("line %d: expected current %d, got %v", i, stages[i].current, record["current"])
		}
		if int(record["total"].(float64)) != stages[i].total {
			t.Errorf("line %d: expected total %d, got %v", i, stages[i].total, record["total"])
		}

		for _, pii := range piiKeys {
			if _, exists := record[pii]; exists {
				t.Errorf("line %d: contains prohibited PII key %q", i, pii)
			}
		}
	}

	// Test nil receiver and nil logger safety.
	var nilNotifier *slogNotifier
	nilNotifier.NotifyProgress(ctx, port.StageParsing, 0, 0)

	emptyNotifier := &slogNotifier{logger: nil}
	emptyNotifier.NotifyProgress(ctx, port.StageParsing, 0, 0)
}

func TestDeps_WithDefaults(t *testing.T) {
	t.Parallel()
	d := deps{}.withDefaults()
	if d.connectRepo == nil {
		t.Errorf("expected default connectRepo to be non-nil")
	}
	if d.newStore == nil {
		t.Errorf("expected default newStore to be non-nil")
	}
	if d.now == nil {
		t.Errorf("expected default now to be non-nil")
	}
	if d.logger == nil {
		t.Errorf("expected default logger to be non-nil")
	}
}

func TestSanitizeError(t *testing.T) {
	t.Parallel()

	if sanitizeError(nil, "some-uri") != nil {
		t.Errorf("expected nil for nil error")
	}

	baseErr := errors.New("underlying sentinel error")

	tests := []struct {
		name        string
		inputErr    error
		rawURI      string
		leakCheck   string
		expectMatch string
		wantIsBase  bool
	}{
		{
			name:        "standard uri credentials",
			inputErr:    errors.New("failed with mongodb://user:pass123@localhost:27017/db"),
			rawURI:      "",
			leakCheck:   "pass123",
			expectMatch: "mongodb://[REDACTED]:[REDACTED]@localhost:27017/db",
		},
		{
			name:        "percent encoded special characters",
			inputErr:    errors.New("connection err: mongodb://app%40domain.com:p%25ss%21w0rd@db.internal:27017/prod"),
			rawURI:      "",
			leakCheck:   "p%25ss%21w0rd",
			expectMatch: "mongodb://[REDACTED]:[REDACTED]@db.internal:27017/prod",
		},
		{
			name:        "exact rawURI replacement even with odd chars",
			inputErr:    fmt.Errorf("dial error on mongodb://svc:unmasked_secret#123@10.0.0.1:27017: %w", baseErr),
			rawURI:      "mongodb://svc:unmasked_secret#123@10.0.0.1:27017",
			leakCheck:   "unmasked_secret#123",
			expectMatch: "[REDACTED_URI]",
			wantIsBase:  true,
		},
		{
			name:        "context.Canceled unwrapping preserved",
			inputErr:    context.Canceled,
			rawURI:      "mongodb://admin:secret@localhost:27017",
			leakCheck:   "secret",
			expectMatch: "context canceled",
			wantIsBase:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sanitized := sanitizeError(tt.inputErr, tt.rawURI)
			if sanitized == nil {
				t.Fatalf("expected non-nil error")
			}
			if strings.Contains(sanitized.Error(), tt.leakCheck) {
				t.Errorf("sanitized error leaked secret %q: %s", tt.leakCheck, sanitized.Error())
			}
			if !strings.Contains(sanitized.Error(), tt.expectMatch) {
				t.Errorf("expected sanitized error to contain %q, got: %s", tt.expectMatch, sanitized.Error())
			}
			if tt.wantIsBase && !errors.Is(sanitized, baseErr) {
				t.Errorf("expected errors.Is(sanitized, baseErr) to be true")
			}
			if errors.Is(tt.inputErr, context.Canceled) && !errors.Is(sanitized, context.Canceled) {
				t.Errorf("expected errors.Is(sanitized, context.Canceled) to be true")
			}
		})
	}
}

func TestRun_IngestStoreInitError(t *testing.T) {
	t.Parallel()
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return &fakeRepo{}, nil
		},
		newStore: func(_ string) (objectStore, error) {
			return nil, errors.New("store creation failed")
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(context.Background(), []string{"ingest"}, d)
	if err == nil || !strings.Contains(err.Error(), "failed to initialize object store") {
		t.Fatalf("expected store init error, got: %v", err)
	}
}

func TestRun_IngestRepoCloseError(t *testing.T) {
	setupIngestDirs(t)
	repo := &fakeRepo{closeErr: errors.New("close network drop")}
	d := deps{
		connectRepo: func(_ context.Context, _ string) (repository, error) {
			return repo, nil
		},
		logger: testLogger(),
		now:    fixedClock,
	}

	err := run(context.Background(), []string{"ingest"}, d)
	if err != nil {
		t.Fatalf("expected nil error despite close error, got: %v", err)
	}
}

func TestRun_PersistSyntheticErrors(t *testing.T) {
	t.Parallel()

	t.Run("connect error", func(t *testing.T) {
		t.Parallel()
		d := deps{
			connectRepo: func(_ context.Context, _ string) (repository, error) {
				return nil, errors.New("dial failed")
			},
			logger: testLogger(),
			now:    fixedClock,
		}

		err := run(context.Background(), []string{"generate-synthetic-data", "--persist-to-mongo"}, d)
		if err == nil || !strings.Contains(err.Error(), "connection to repository failed") {
			t.Fatalf("expected connect error, got: %v", err)
		}
	})

	t.Run("pipeline save error", func(t *testing.T) {
		t.Parallel()
		repo := &fakeRepo{saveBatchErr: errors.New("disk full")}
		d := deps{
			connectRepo: func(_ context.Context, _ string) (repository, error) {
				return repo, nil
			},
			logger: testLogger(),
			now:    fixedClock,
		}

		err := run(context.Background(), []string{"generate-synthetic-data", "--persist-to-mongo"}, d)
		if err == nil || !strings.Contains(err.Error(), "failed to generate and persist synthetic data") {
			t.Fatalf("expected persist error, got: %v", err)
		}
	})

	t.Run("close error is logged and does not fail successful persist", func(t *testing.T) {
		t.Parallel()
		repo := &fakeRepo{closeErr: errors.New("close drop")}
		d := deps{
			connectRepo: func(_ context.Context, _ string) (repository, error) {
				return repo, nil
			},
			logger: testLogger(),
			now:    fixedClock,
		}

		err := run(context.Background(), []string{"generate-synthetic-data", "--rows", "2", "--persist-to-mongo"}, d)
		if err != nil {
			t.Fatalf("expected successful persist despite close error, got: %v", err)
		}
	})
}
