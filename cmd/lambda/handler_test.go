package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/storage/mongo"
	s3storage "babylon/dataloader/internal/infra/storage/s3"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

const (
	testBucket    = "test-bucket"
	testSecretID  = "test-secret-id"
	testSecretURI = "mongodb://user:pass@localhost:27017/datalake"

	chaseCSVFixture = "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
		"DEBIT,01/01/2026,CHASE COFFEE SHOP,-4.50,DEBIT,1200.00,\n"

	fidelityCSVFixture = "Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type\n" +
		"SYNTH-ROTH-101,ROTH IRA,SPAXX**,HELD IN MONEY MARKET,,,,$3250.00,,,,,25.00%,,,Cash,\n\n" +
		"\"Date downloaded Sep-25-2026 9:50 p.m ET\"\n"

	syntheticCSVFixture = "Posting Date,Description,Amount\n01/01/2026,Test Synthetic,-25.00\n"
)

type mockS3API struct {
	mu             sync.Mutex
	objects        map[string][]byte
	getObjectFn    func(ctx context.Context, in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error)
	copyObjectFn   func(ctx context.Context, in *awss3.CopyObjectInput) (*awss3.CopyObjectOutput, error)
	deleteObjectFn func(ctx context.Context, in *awss3.DeleteObjectInput) (*awss3.DeleteObjectOutput, error)
}

func newMockS3API(initial map[string][]byte) *mockS3API {
	m := &mockS3API{objects: make(map[string][]byte)}
	for k, v := range initial {
		m.objects[k] = v
	}
	return m
}

func (m *mockS3API) GetObject(
	ctx context.Context,
	in *awss3.GetObjectInput,
	_ ...func(*awss3.Options),
) (*awss3.GetObjectOutput, error) {
	if m.getObjectFn != nil {
		return m.getObjectFn(ctx, in)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	data, ok := m.objects[*in.Key]
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("key not found")}
	}
	return &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3API) CopyObject(
	ctx context.Context,
	in *awss3.CopyObjectInput,
	_ ...func(*awss3.Options),
) (*awss3.CopyObjectOutput, error) {
	if m.copyObjectFn != nil {
		return m.copyObjectFn(ctx, in)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	copySource := *in.CopySource
	parts := strings.SplitN(copySource, "/", 2)
	srcKey := parts[1]
	unescaped, _ := url.PathUnescape(srcKey)

	data, ok := m.objects[unescaped]
	if !ok {
		data, ok = m.objects[srcKey]
	}
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("source not found")}
	}
	m.objects[*in.Key] = data
	return &awss3.CopyObjectOutput{}, nil
}

func (m *mockS3API) DeleteObject(
	ctx context.Context,
	in *awss3.DeleteObjectInput,
	_ ...func(*awss3.Options),
) (*awss3.DeleteObjectOutput, error) {
	if m.deleteObjectFn != nil {
		return m.deleteObjectFn(ctx, in)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, *in.Key)
	return &awss3.DeleteObjectOutput{}, nil
}

type handlerMockSecretsClient struct {
	secretValue string
	err         error
}

func (m *handlerMockSecretsClient) GetSecretValue(
	_ context.Context,
	_ *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &secretsmanager.GetSecretValueOutput{
		SecretString: aws.String(m.secretValue),
	}, nil
}

type fakeRepository struct {
	mu           sync.Mutex
	savedBatches []*model.IngestionBatch
	syncLogs     []*model.SyncLog
	saveErr      error
	syncLogErr   error
	closeCalls   int
}

func (f *fakeRepository) SaveBatch(_ context.Context, b *model.IngestionBatch) (*model.SyncLog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return nil, f.saveErr
	}
	f.savedBatches = append(f.savedBatches, b)
	log := &model.SyncLog{
		FileName:       b.SourceFile,
		FileHash:       b.FileHash,
		CollectionName: string(b.Destination),
		RecordCount:    int64(b.RecordCount),
		UpsertedCount:  int64(b.RecordCount),
		Status:         model.SyncStatusSuccess,
		SyncedAt:       time.Now(),
	}
	return log, nil
}

func (f *fakeRepository) LogSync(_ context.Context, log *model.SyncLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncLogErr != nil {
		return f.syncLogErr
	}
	f.syncLogs = append(f.syncLogs, log)
	return nil
}

func (f *fakeRepository) Close(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}

func newS3Record(key, urlDecodedKey string) events.S3EventRecord {
	return events.S3EventRecord{
		S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: testBucket},
			Object: events.S3Object{
				Key:           key,
				URLDecodedKey: urlDecodedKey,
			},
		},
	}
}

func newTestHandler(
	api s3storage.ObjectAPI,
	repo repository,
	opts ...HandlerOption,
) *S3EventHandler {
	secrets := &handlerMockSecretsClient{secretValue: testSecretURI}
	connect := func(_ context.Context, _ string) (repository, error) {
		return repo, nil
	}
	logger := slog.New(slog.DiscardHandler)
	return NewS3EventHandler(api, secrets, testSecretID, connect, logger, opts...)
}

func TestS3Handler_SkipsNonEligibleKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "not unprocessed prefix", key: "processed/chase9876_statement.csv"},
		{name: "not csv file", key: "unprocessed/chase9876_statement.txt"},
		{name: "unprocessed prefix only with no filename", key: "unprocessed/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := newMockS3API(map[string][]byte{tc.key: []byte("content")})
			repo := &fakeRepository{}
			h := newTestHandler(api, repo)

			event := events.S3Event{
				Records: []events.S3EventRecord{newS3Record(tc.key, "")},
			}
			if err := h.HandleS3Event(context.Background(), event); err != nil {
				t.Fatalf("expected nil error on skipped key, got %v", err)
			}
			if len(repo.savedBatches) != 0 {
				t.Fatalf("expected 0 saved batches, got %d", len(repo.savedBatches))
			}
		})
	}
}

func TestS3Handler_URLDecodingFallbacks(t *testing.T) {
	tests := []struct {
		name          string
		rawKey        string
		urlDecodedKey string
		storedKey     string
	}{
		{
			name:          "prefers URLDecodedKey when present",
			rawKey:        "unprocessed/test+space/chase9876_statement.csv",
			urlDecodedKey: "unprocessed/test space/chase9876_statement.csv",
			storedKey:     "unprocessed/test space/chase9876_statement.csv",
		},
		{
			name:          "decodes query escaped key when URLDecodedKey empty",
			rawKey:        "unprocessed/test+space/chase9876_statement.csv",
			urlDecodedKey: "",
			storedKey:     "unprocessed/test space/chase9876_statement.csv",
		},
		{
			name:          "falls back to raw key on invalid url escaping",
			rawKey:        "unprocessed/invalid%ZZ/chase9876_statement.csv",
			urlDecodedKey: "",
			storedKey:     "unprocessed/invalid%ZZ/chase9876_statement.csv",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := newMockS3API(map[string][]byte{tc.storedKey: []byte(chaseCSVFixture)})
			repo := &fakeRepository{}
			h := newTestHandler(api, repo)

			event := events.S3Event{
				Records: []events.S3EventRecord{newS3Record(tc.rawKey, tc.urlDecodedKey)},
			}
			if err := h.HandleS3Event(context.Background(), event); err != nil {
				t.Fatalf("expected successful ingestion, got %v", err)
			}
			if len(repo.savedBatches) != 1 {
				t.Fatalf("expected 1 saved batch, got %d", len(repo.savedBatches))
			}
		})
	}
}

func TestS3Handler_SuccessArchivesAndSetsSourceURI(t *testing.T) {
	const key = "unprocessed/chase9876_statement.csv"
	const expectedArchiveKey = "processed/chase9876_statement.csv"
	expectedSourceURI := fmt.Sprintf("s3://%s/%s", testBucket, key)

	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
	repo := &fakeRepository{}
	fixedTime := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	h := newTestHandler(api, repo, WithClock(func() time.Time { return fixedTime }))

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.savedBatches) != 1 {
		t.Fatalf("expected 1 saved batch, got %d", len(repo.savedBatches))
	}
	batch := repo.savedBatches[0]
	if batch.SourceURI != expectedSourceURI {
		t.Errorf("expected SourceURI %q, got %q", expectedSourceURI, batch.SourceURI)
	}
	if batch.RecordCount != 1 {
		t.Errorf("expected 1 record in batch, got %d", batch.RecordCount)
	}

	// Verify original deleted and archive copy exists.
	if _, exists := api.objects[key]; exists {
		t.Errorf("expected original object %q to be deleted", key)
	}
	if _, exists := api.objects[expectedArchiveKey]; !exists {
		t.Errorf("expected archived object %q to exist", expectedArchiveKey)
	}
}

func TestS3Handler_PreservesNestedKey(t *testing.T) {
	const key = "unprocessed/chase/2026/01/chase9876_statement.csv"
	const expectedArchive = "processed/chase/2026/01/chase9876_statement.csv"

	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, exists := api.objects[key]; exists {
		t.Errorf("expected original nested object %q to be deleted", key)
	}
	if _, exists := api.objects[expectedArchive]; !exists {
		t.Errorf("expected archived nested object %q to exist", expectedArchive)
	}
}

func TestS3Handler_TransientFailureRetries(t *testing.T) {
	t.Run("mongo transient error returns error", func(t *testing.T) {
		const key = "unprocessed/chase9876_statement.csv"
		api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
		repo := &fakeRepository{saveErr: mongo.ErrTransient}
		h := newTestHandler(api, repo)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record(key, "")},
		}
		err := h.HandleS3Event(context.Background(), event)
		if err == nil || !errors.Is(err, mongo.ErrTransient) {
			t.Fatalf("expected mongo.ErrTransient, got %v", err)
		}
		// Object must remain in unprocessed.
		if _, exists := api.objects[key]; !exists {
			t.Errorf("expected object %q to remain in unprocessed/", key)
		}
	})

	t.Run("s3 download object not found returns error", func(t *testing.T) {
		const key = "unprocessed/chase9876_statement.csv"
		api := newMockS3API(nil) // empty store
		repo := &fakeRepository{}
		h := newTestHandler(api, repo)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record(key, "")},
		}
		err := h.HandleS3Event(context.Background(), event)
		if err == nil || !errors.Is(err, port.ErrObjectNotFound) {
			t.Fatalf("expected port.ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("s3 network failure returns error", func(t *testing.T) {
		const key = "unprocessed/chase9876_statement.csv"
		api := newMockS3API(nil)
		networkErr := errors.New("s3 connection reset by peer")
		api.getObjectFn = func(_ context.Context, _ *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
			return nil, networkErr
		}
		repo := &fakeRepository{}
		h := newTestHandler(api, repo)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record(key, "")},
		}
		err := h.HandleS3Event(context.Background(), event)
		if err == nil || !errors.Is(err, networkErr) {
			t.Fatalf("expected networkErr, got %v", err)
		}
	})
}

func TestS3Handler_DeterministicFailureLeavesObject(t *testing.T) {
	t.Run("unresolvable source logs and returns nil", func(t *testing.T) {
		const key = "unprocessed/unknown_dialect_1234.csv"
		api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
		repo := &fakeRepository{}
		h := newTestHandler(api, repo)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record(key, "")},
		}
		if err := h.HandleS3Event(context.Background(), event); err != nil {
			t.Fatalf("expected nil for deterministic unresolvable source, got %v", err)
		}
		if _, exists := api.objects[key]; !exists {
			t.Errorf("object %q should remain in unprocessed/", key)
		}
	})

	t.Run("malformed csv returns nil and leaves object", func(t *testing.T) {
		const key = "unprocessed/chase9876_statement.csv"
		const malformedCSV = "Details,Posting Date,Description,Amount,Type\nINVALID,NOT_A_DATE,Store,ABC,DEBIT\n"
		api := newMockS3API(map[string][]byte{key: []byte(malformedCSV)})
		repo := &fakeRepository{}
		h := newTestHandler(api, repo)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record(key, "")},
		}
		if err := h.HandleS3Event(context.Background(), event); err != nil {
			t.Fatalf("expected nil for deterministic malformed csv, got %v", err)
		}
		if _, exists := api.objects[key]; !exists {
			t.Errorf("object %q should remain in unprocessed/", key)
		}
	})
}

func TestS3Handler_ArchiveFailureReturnsError(t *testing.T) {
	const key = "unprocessed/chase9876_statement.csv"
	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
	copyErr := errors.New("s3 copy permission denied")
	api.copyObjectFn = func(_ context.Context, _ *awss3.CopyObjectInput) (*awss3.CopyObjectOutput, error) {
		return nil, copyErr
	}
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	err := h.HandleS3Event(context.Background(), event)
	if err == nil || !strings.Contains(err.Error(), "file archival failed") {
		t.Fatalf("expected archival failure error, got %v", err)
	}
	// Source object must not have been deleted.
	if _, exists := api.objects[key]; !exists {
		t.Errorf("source object %q must not be deleted on archive failure", key)
	}
}

func TestS3Handler_LazyInitOnceAndRetryAfterFailure(t *testing.T) {
	const key = "unprocessed/chase9876_statement.csv"
	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})

	connectCalls := 0
	connectFail := true
	repo := &fakeRepository{}

	connect := func(_ context.Context, _ string) (repository, error) {
		connectCalls++
		if connectFail {
			return nil, errors.New("connection timed out")
		}
		return repo, nil
	}

	secrets := &handlerMockSecretsClient{secretValue: testSecretURI}
	logger := slog.New(slog.DiscardHandler)
	h := NewS3EventHandler(api, secrets, testSecretID, connect, logger)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}

	// First attempt: connect fails
	if err := h.HandleS3Event(context.Background(), event); err == nil {
		t.Fatalf("expected connect failure error on first attempt")
	}
	if connectCalls != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls)
	}

	// Second attempt: connect succeeds and caches pipeline
	connectFail = false
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("expected success on second attempt, got %v", err)
	}
	if connectCalls != 2 {
		t.Errorf("expected 2 connect calls, got %d", connectCalls)
	}

	// Third attempt: should reuse cached pipeline, connect not called again
	api.objects[key] = []byte(chaseCSVFixture)
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("expected success on third attempt, got %v", err)
	}
	if connectCalls != 2 {
		t.Errorf("expected connectCalls to stay 2 on warm reuse, got %d", connectCalls)
	}

	// Close resets cached pipeline
	if err := h.Close(context.Background()); err != nil {
		t.Fatalf("expected clean close, got %v", err)
	}
	if repo.closeCalls != 1 {
		t.Errorf("expected 1 close call, got %d", repo.closeCalls)
	}
}

func TestS3Handler_SecretErrorDoesNotLeakURI(t *testing.T) {
	const rawURI = "mongodb://admin:super_secret_password@cluster.mongodb.net/datalake"
	const key = "unprocessed/chase9876_statement.csv"
	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})

	connect := func(_ context.Context, uri string) (repository, error) {
		return nil, fmt.Errorf("auth error for %s: connection rejected", uri)
	}

	secrets := &handlerMockSecretsClient{secretValue: rawURI}
	logger := slog.New(slog.DiscardHandler)
	h := NewS3EventHandler(api, secrets, testSecretID, connect, logger)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	err := h.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected connect error")
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "super_secret_password") {
		t.Fatalf("plaintext credential leaked in error message: %s", errMsg)
	}
	if !strings.Contains(errMsg, "[REDACTED]") && !strings.Contains(errMsg, "[REDACTED_URI]") {
		t.Fatalf("expected redacted marker in error message, got: %s", errMsg)
	}
}

func TestS3Handler_StopsAtFirstError(t *testing.T) {
	const key1 = "unprocessed/chase9876_statement.csv"
	const key2 = "unprocessed/transient_failure/chase9876_statement.csv"
	const key3 = "unprocessed/third_file/chase9876_statement.csv"

	api := newMockS3API(map[string][]byte{
		key1: []byte(chaseCSVFixture),
		key2: []byte(chaseCSVFixture),
		key3: []byte(chaseCSVFixture),
	})

	repo := &fakeRepository{}
	callCount := 0
	repoSave := func(_ context.Context, b *model.IngestionBatch) (*model.SyncLog, error) {
		callCount++
		if callCount == 2 {
			return nil, mongo.ErrTransient
		}
		repo.savedBatches = append(repo.savedBatches, b)
		return &model.SyncLog{
			FileName:       b.SourceFile,
			FileHash:       b.FileHash,
			CollectionName: string(b.Destination),
			RecordCount:    int64(b.RecordCount),
			UpsertedCount:  int64(b.RecordCount),
			Status:         model.SyncStatusSuccess,
			SyncedAt:       time.Now(),
		}, nil
	}

	mockRepo := &customRepo{
		fakeRepository: repo,
		saveFn:         repoSave,
	}

	h := newTestHandler(api, mockRepo)

	event := events.S3Event{
		Records: []events.S3EventRecord{
			newS3Record(key1, ""),
			newS3Record(key2, ""),
			newS3Record(key3, ""),
		},
	}

	err := h.HandleS3Event(context.Background(), event)
	if err == nil || !errors.Is(err, mongo.ErrTransient) {
		t.Fatalf("expected mongo.ErrTransient, got %v", err)
	}

	if callCount != 2 {
		t.Errorf("expected execution to stop at record 2, total calls: %d", callCount)
	}
	if _, exists := api.objects[key3]; !exists {
		t.Errorf("record 3 must not have been touched/deleted: %s", key3)
	}
}

type customRepo struct {
	*fakeRepository

	saveFn func(ctx context.Context, b *model.IngestionBatch) (*model.SyncLog, error)
}

func (c *customRepo) SaveBatch(ctx context.Context, b *model.IngestionBatch) (*model.SyncLog, error) {
	if c.saveFn != nil {
		return c.saveFn(ctx, b)
	}
	return c.fakeRepository.SaveBatch(ctx, b)
}

func TestS3Handler_AuditWriteErrorIsSuccess(t *testing.T) {
	const key = "unprocessed/chase9876_statement.csv"
	api := newMockS3API(map[string][]byte{key: []byte(chaseCSVFixture)})
	repo := &fakeRepository{syncLogErr: errors.New("audit log write failure")}
	h := newTestHandler(api, repo)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	// Per Decision P5, audit log write failure is non-fatal: returns nil, batch persisted, archived.
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("expected nil error for audit log failure, got %v", err)
	}
	if len(repo.savedBatches) != 1 {
		t.Fatalf("expected batch to be persisted despite audit error, got %d", len(repo.savedBatches))
	}
	if _, exists := api.objects[key]; exists {
		t.Errorf("expected file %q to be archived on successful batch persistence", key)
	}
}

func TestS3Handler_DefaultDependencies(t *testing.T) {
	api := newMockS3API(nil)
	secrets := &handlerMockSecretsClient{secretValue: testSecretURI}
	h := NewS3EventHandler(api, secrets, testSecretID, nil, nil)
	if h.logger == nil {
		t.Errorf("expected default logger")
	}
	if h.connect == nil {
		t.Errorf("expected default connect")
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatalf("expected nil close error, got %v", err)
	}
}

func TestS3Handler_CustomS3StoreFactory(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		api := newMockS3API(map[string][]byte{"unprocessed/chase9876_statement.csv": []byte(chaseCSVFixture)})
		repo := &fakeRepository{}
		factoryCalled := false
		customFactory := func(a s3storage.ObjectAPI, bucket string) (port.ObjectStore, error) {
			factoryCalled = true
			return s3storage.NewClient(a, bucket)
		}
		h := newTestHandler(api, repo, WithS3StoreFactory(customFactory))
		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record("unprocessed/chase9876_statement.csv", "")},
		}
		if err := h.HandleS3Event(context.Background(), event); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !factoryCalled {
			t.Errorf("expected custom s3 factory to be called")
		}
	})

	t.Run("factory error propagates", func(t *testing.T) {
		api := newMockS3API(map[string][]byte{"unprocessed/chase9876_statement.csv": []byte(chaseCSVFixture)})
		repo := &fakeRepository{}
		initErr := errors.New("s3 store init failed")
		customFactory := func(_ s3storage.ObjectAPI, _ string) (port.ObjectStore, error) {
			return nil, initErr
		}
		h := newTestHandler(api, repo, WithS3StoreFactory(customFactory))
		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record("unprocessed/chase9876_statement.csv", "")},
		}
		err := h.HandleS3Event(context.Background(), event)
		if err == nil || !errors.Is(err, initErr) {
			t.Fatalf("expected initErr, got %v", err)
		}
	})
}

func TestS3Handler_SanitizedErrorAndKeyValidation(t *testing.T) {
	t.Run("secret resolution error fails pipeline initialization", func(t *testing.T) {
		api := newMockS3API(map[string][]byte{"unprocessed/chase9876_statement.csv": []byte(chaseCSVFixture)})
		secretErr := errors.New("access denied to secret")
		secrets := &handlerMockSecretsClient{err: secretErr}
		logger := slog.New(slog.DiscardHandler)
		h := NewS3EventHandler(api, secrets, testSecretID, nil, logger)

		event := events.S3Event{
			Records: []events.S3EventRecord{newS3Record("unprocessed/chase9876_statement.csv", "")},
		}
		err := h.HandleS3Event(context.Background(), event)
		if err == nil {
			t.Fatalf("expected error from secret resolution")
		}
	})

	t.Run("sanitizedError unwraps sentinel errors", func(t *testing.T) {
		orig := context.Canceled
		sanitized := sanitizeError(orig, "mongodb://admin:pass@host/db")
		if !errors.Is(sanitized, orig) {
			t.Errorf("expected sanitized error to unwrap context.Canceled")
		}
		if sanitizeError(nil, "") != nil {
			t.Errorf("expected nil for nil input error")
		}
	})

	t.Run("validateS3Key rejects invalid filename", func(t *testing.T) {
		api := newMockS3API(nil)
		repo := &fakeRepository{}
		h := newTestHandler(api, repo)
		_, valid := h.validateS3Key(context.Background(), testBucket, "unprocessed/.")
		if valid {
			t.Errorf("expected invalid filename for unprocessed/.")
		}
	})

	t.Run("sanitizes complex punctuation credentials", func(t *testing.T) {
		const rawURI = "mongodb://app%40user:p%40%24%24w0rd%21@localhost:27017/datalake"
		sanitized := sanitizeError(fmt.Errorf("connection to %s failed", rawURI), rawURI)
		if strings.Contains(sanitized.Error(), "w0rd") {
			t.Fatalf("credential leaked in sanitized error: %s", sanitized.Error())
		}
	})
}

func TestS3Handler_PreCanceledContext(t *testing.T) {
	api := newMockS3API(map[string][]byte{"unprocessed/chase9876_statement.csv": []byte(chaseCSVFixture)})
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record("unprocessed/chase9876_statement.csv", "")},
	}
	err := h.HandleS3Event(ctx, event)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(repo.savedBatches) != 0 {
		t.Errorf("expected zero saved batches on canceled context, got %d", len(repo.savedBatches))
	}
}

func TestS3Handler_EmptyRecordsEvent(t *testing.T) {
	api := newMockS3API(nil)
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	if err := h.HandleS3Event(context.Background(), events.S3Event{Records: nil}); err != nil {
		t.Fatalf("expected nil error on empty records event, got %v", err)
	}
	if len(repo.savedBatches) != 0 {
		t.Errorf("expected zero saved batches on empty records event")
	}
}

func TestS3Handler_FidelitySuccess(t *testing.T) {
	const key = "unprocessed/Portfolio_Positions_Sep-25-2026.csv"
	expectedSourceURI := fmt.Sprintf("s3://%s/%s", testBucket, key)

	api := newMockS3API(map[string][]byte{key: []byte(fidelityCSVFixture)})
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("unexpected error on fidelity ingestion: %v", err)
	}

	if len(repo.savedBatches) != 1 {
		t.Fatalf("expected 1 saved batch, got %d", len(repo.savedBatches))
	}
	batch := repo.savedBatches[0]
	if batch.Destination != model.DestFidelityPosition {
		t.Errorf("expected destination %q, got %q", model.DestFidelityPosition, batch.Destination)
	}
	if batch.SourceURI != expectedSourceURI {
		t.Errorf("expected SourceURI %q, got %q", expectedSourceURI, batch.SourceURI)
	}
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		t.Fatalf("failed to cast batch to fidelity positions: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
	if positions[0].Symbol != "SPAXX**" || positions[0].CurrentValue != 3250.00 {
		t.Errorf("unexpected position data: %+v", positions[0])
	}
}

func TestS3Handler_SyntheticSuccess(t *testing.T) {
	const key = "unprocessed/synthetic_file.csv"
	expectedSourceURI := fmt.Sprintf("s3://%s/%s", testBucket, key)

	api := newMockS3API(map[string][]byte{key: []byte(syntheticCSVFixture)})
	repo := &fakeRepository{}
	h := newTestHandler(api, repo)

	event := events.S3Event{
		Records: []events.S3EventRecord{newS3Record(key, "")},
	}
	if err := h.HandleS3Event(context.Background(), event); err != nil {
		t.Fatalf("unexpected error on synthetic ingestion: %v", err)
	}

	if len(repo.savedBatches) != 1 {
		t.Fatalf("expected 1 saved batch, got %d", len(repo.savedBatches))
	}
	batch := repo.savedBatches[0]
	if batch.Destination != model.DestSynthetic {
		t.Errorf("expected destination %q, got %q", model.DestSynthetic, batch.Destination)
	}
	if batch.SourceURI != expectedSourceURI {
		t.Errorf("expected SourceURI %q, got %q", expectedSourceURI, batch.SourceURI)
	}
}
