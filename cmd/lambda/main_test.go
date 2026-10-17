package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"babylon/dataloader/config"
	"babylon/dataloader/storage"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mockStorageService struct {
	downloadFunc func(ctx context.Context, bucket, key, localPath string) error
	copyFunc     func(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error
	deleteFunc   func(ctx context.Context, bucket, key string) error

	downloadCalled bool
	copyCalled     bool
	deleteCalled   bool

	downloadLocalPath string
	copyDestKey       string
}

func (m *mockStorageService) Download(ctx context.Context, bucket, key, localPath string) error {
	m.downloadCalled = true
	m.downloadLocalPath = localPath
	if m.downloadFunc != nil {
		return m.downloadFunc(ctx, bucket, key, localPath)
	}
	_ = os.MkdirAll(filepath.Dir(localPath), 0o755)
	_ = os.WriteFile(localPath, []byte("Date,Description,Amount\n2026-01-01,Test,-10.00\n"), 0o644)
	return nil
}

func (m *mockStorageService) Copy(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error {
	m.copyCalled = true
	m.copyDestKey = destKey
	if m.copyFunc != nil {
		return m.copyFunc(ctx, srcBucket, srcKey, destBucket, destKey)
	}
	return nil
}

func (m *mockStorageService) Delete(ctx context.Context, bucket, key string) error {
	m.deleteCalled = true
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, bucket, key)
	}
	return nil
}

type mockSecretsClient struct {
	output *secretsmanager.GetSecretValueOutput
	err    error
}

func (m *mockSecretsClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return m.output, m.err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func makeS3Event(bucket, key string) events.S3Event {
	return events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: bucket},
					Object: events.S3Object{Key: key},
				},
			},
		},
	}
}

func TestHandleS3Event_FilterNonCSV(t *testing.T) {
	mockStore := &mockStorageService{}
	handler := NewHandler(mockStore, &mockSecretsClient{}, "secret", t.TempDir(), nil, discardLogger())

	event := makeS3Event("my-bucket", "unprocessed/image.png")
	err := handler.HandleS3Event(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockStore.downloadCalled {
		t.Error("expected download not to be called for non-csv file")
	}
}

func TestHandleS3Event_FilterNonUnprocessedPrefix(t *testing.T) {
	mockStore := &mockStorageService{}
	handler := NewHandler(mockStore, &mockSecretsClient{}, "secret", t.TempDir(), nil, discardLogger())

	event := makeS3Event("my-bucket", "processed/transactions.csv")
	err := handler.HandleS3Event(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockStore.downloadCalled {
		t.Error("expected download not to be called for file outside unprocessed/ prefix")
	}
}

func TestHandleS3Event_URLDecodedKeyHandling(t *testing.T) {
	config.ResetMongoURICache()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	mockStore := &mockStorageService{}
	ingestCalled := false
	runner := func(ctx context.Context, cfg *config.Config) error {
		ingestCalled = true
		return nil
	}

	handler := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), runner, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/synthetic%2Bdata.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockStore.downloadCalled {
		t.Error("expected download to be called")
	}
	if !ingestCalled {
		t.Error("expected ingest runner to be called")
	}
	if mockStore.copyDestKey != "processed/synthetic+data.csv" {
		t.Errorf("got copy destination %s, want processed/synthetic+data.csv", mockStore.copyDestKey)
	}
}

func TestHandleS3Event_DownloadError(t *testing.T) {
	config.ResetMongoURICache()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	mockStore := &mockStorageService{
		downloadFunc: func(ctx context.Context, bucket, key, localPath string) error {
			return errors.New("s3 download failed")
		},
	}

	handler := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/test.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected error on download failure, got nil")
	}
	if mockStore.copyCalled || mockStore.deleteCalled {
		t.Error("copy or delete should not be called when download fails")
	}
}

func TestHandleS3Event_SecretResolutionError(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")
	_ = os.Unsetenv("MONGO_SECRET_ID")

	mockStore := &mockStorageService{}
	mockSM := &mockSecretsClient{
		err: errors.New("secrets manager permission denied"),
	}

	handler := NewHandler(mockStore, mockSM, "my-secret", t.TempDir(), nil, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/test.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when secret resolution fails, got nil")
	}
	if mockStore.copyCalled || mockStore.deleteCalled {
		t.Error("copy/delete should not be called when secret resolution fails")
	}
}

func TestHandleS3Event_IngestError(t *testing.T) {
	config.ResetMongoURICache()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	mockStore := &mockStorageService{}
	runner := func(ctx context.Context, cfg *config.Config) error {
		return errors.New("ingestion parse error")
	}

	handler := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), runner, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/test.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when ingestion fails, got nil")
	}
	if mockStore.copyCalled || mockStore.deleteCalled {
		t.Error("copy or delete should not be called when ingestion fails")
	}
}

func TestHandleS3Event_CopyError(t *testing.T) {
	config.ResetMongoURICache()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	mockStore := &mockStorageService{
		copyFunc: func(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error {
			return errors.New("s3 copy error")
		},
	}
	runner := func(ctx context.Context, cfg *config.Config) error {
		return nil
	}

	handler := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), runner, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/test.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when s3 copy fails, got nil")
	}
	if !mockStore.copyCalled {
		t.Error("expected copy to be attempted")
	}
	if mockStore.deleteCalled {
		t.Error("delete should not be called if copy failed")
	}
}

func TestHandleS3Event_DeleteError(t *testing.T) {
	config.ResetMongoURICache()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	mockStore := &mockStorageService{
		deleteFunc: func(ctx context.Context, bucket, key string) error {
			return errors.New("s3 delete error")
		},
	}
	runner := func(ctx context.Context, cfg *config.Config) error {
		return nil
	}

	handler := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), runner, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/test.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when s3 delete fails, got nil")
	}
	if !mockStore.copyCalled {
		t.Error("expected copy to succeed before delete")
	}
	if !mockStore.deleteCalled {
		t.Error("expected delete to be attempted")
	}
}

func TestHandleS3Event_Success(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	mockStore := &mockStorageService{}
	mockSM := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(`{"mongo_uri": "mongodb://atlas.remote:27017/datalake"}`),
		},
	}

	var capturedCfg *config.Config
	runner := func(ctx context.Context, cfg *config.Config) error {
		capturedCfg = cfg
		return nil
	}

	tmpDir := t.TempDir()
	handler := NewHandler(mockStore, mockSM, "babylon/staging/secret", tmpDir, runner, discardLogger())
	event := makeS3Event("landing-bucket", "unprocessed/synthetic_data.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !mockStore.downloadCalled {
		t.Error("download should have been called")
	}
	if capturedCfg == nil {
		t.Fatal("expected config to be passed to runner")
	}
	if capturedCfg.MongoURI != "mongodb://atlas.remote:27017/datalake" {
		t.Errorf("got MongoURI %s, want mongodb://atlas.remote:27017/datalake", capturedCfg.MongoURI)
	}
	if !capturedCfg.MoveProcessedFiles {
		t.Error("expected MoveProcessedFiles to be true")
	}
	if !mockStore.copyCalled {
		t.Error("expected copy to be called")
	}
	if mockStore.copyDestKey != "processed/synthetic_data.csv" {
		t.Errorf("got copy destination %s, want processed/synthetic_data.csv", mockStore.copyDestKey)
	}
	if !mockStore.deleteCalled {
		t.Error("expected delete to be called")
	}

	// Verify ephemeral directory was cleaned up
	invDir := filepath.Dir(capturedCfg.UnprocessedDir)
	if _, statErr := os.Stat(invDir); !os.IsNotExist(statErr) {
		t.Errorf("expected ephemeral directory %s to be cleaned up, but it still exists", invDir)
	}
}

func TestHandleS3Event_PreservesNestedDirectoryKey(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	mockStore := &mockStorageService{}
	mockSM := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(`{"mongo_uri": "mongodb://localhost:27017"}`),
		},
	}

	var capturedCfg *config.Config
	runner := func(ctx context.Context, cfg *config.Config) error {
		capturedCfg = cfg
		return nil
	}

	tmpDir := t.TempDir()
	handler := NewHandler(mockStore, mockSM, "secret", tmpDir, runner, discardLogger())
	event := makeS3Event("my-bucket", "unprocessed/2026/01/monthly_report.csv")

	err := handler.HandleS3Event(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedDestKey := "processed/2026/01/monthly_report.csv"
	if mockStore.copyDestKey != expectedDestKey {
		t.Errorf("got copy destination %q, want %q", mockStore.copyDestKey, expectedDestKey)
	}

	invDir := filepath.Dir(capturedCfg.UnprocessedDir)
	if _, statErr := os.Stat(invDir); !os.IsNotExist(statErr) {
		t.Errorf("expected ephemeral directory %s to be cleaned up, but it still exists", invDir)
	}
}

func TestHandleS3Event_WarmInvocationIsolation(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	mockStore := &mockStorageService{}
	mockSM := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(`{"mongo_uri": "mongodb://localhost:27017"}`),
		},
	}

	var observedDirs []string
	runner := func(ctx context.Context, cfg *config.Config) error {
		observedDirs = append(observedDirs, cfg.UnprocessedDir)
		return nil
	}

	tmpDir := t.TempDir()
	handler := NewHandler(mockStore, mockSM, "secret", tmpDir, runner, discardLogger())

	// Invocation 1
	event1 := makeS3Event("my-bucket", "unprocessed/first.csv")
	if err := handler.HandleS3Event(context.Background(), event1); err != nil {
		t.Fatalf("first invocation failed: %v", err)
	}

	// Invocation 2
	event2 := makeS3Event("my-bucket", "unprocessed/second.csv")
	if err := handler.HandleS3Event(context.Background(), event2); err != nil {
		t.Fatalf("second invocation failed: %v", err)
	}

	if len(observedDirs) != 2 {
		t.Fatalf("expected 2 observed directories, got %d", len(observedDirs))
	}
	if observedDirs[0] == observedDirs[1] {
		t.Errorf("expected distinct ephemeral directories across warm invocations, both were %s", observedDirs[0])
	}
	for _, dir := range observedDirs {
		parent := filepath.Dir(dir)
		if _, statErr := os.Stat(parent); !os.IsNotExist(statErr) {
			t.Errorf("expected invocation dir %s to be removed, but it exists", parent)
		}
	}
}

type mockLambdaMongoClient struct{}

func (m *mockLambdaMongoClient) Disconnect(ctx context.Context) error {
	return nil
}

func (m *mockLambdaMongoClient) Database(name string, opts ...*options.DatabaseOptions) *mongo.Database {
	return nil
}

func TestDefaultIngestRunner_MongoConnectError(t *testing.T) {
	origConnect := storage.ConnectToMongoDBFunc
	//nolint:reassign // Test stubbing for MongoDB connection
	storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
		return nil, errors.New("cannot reach mongo")
	}
	defer func() {
		//nolint:reassign // Test stubbing for MongoDB connection
		storage.ConnectToMongoDBFunc = origConnect
	}()

	cfg := &config.Config{
		MongoURI:       "mongodb://bad-host:27017",
		UnprocessedDir: t.TempDir(),
		ProcessedDir:   t.TempDir(),
	}

	err := DefaultIngestRunner(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error on mongo connect failure, got nil")
	}
}

func TestDefaultIngestRunner_NonExistentDir(t *testing.T) {
	origConnect := storage.ConnectToMongoDBFunc
	//nolint:reassign // Test stubbing for MongoDB connection
	storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
		return &mockLambdaMongoClient{}, nil
	}
	defer func() {
		//nolint:reassign // Test stubbing for MongoDB connection
		storage.ConnectToMongoDBFunc = origConnect
	}()

	cfg := &config.Config{
		MongoURI:       "mongodb://localhost:27017",
		UnprocessedDir: "/nonexistent/path/here",
		ProcessedDir:   t.TempDir(),
	}

	err := DefaultIngestRunner(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error on non-existent directory, got nil")
	}
}

func TestHandler_CreateStagingDirs_Success(t *testing.T) {
	h := NewHandler(&mockStorageService{}, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
	staging, cleanup, err := h.createStagingDirs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()

	if staging == nil {
		t.Fatal("expected non-nil stagingDirs")
	}
	if _, statErr := os.Stat(staging.rootDir); statErr != nil {
		t.Errorf("expected rootDir %s to exist: %v", staging.rootDir, statErr)
	}
	if _, statErr := os.Stat(staging.unprocessedDir); statErr != nil {
		t.Errorf("expected unprocessedDir %s to exist: %v", staging.unprocessedDir, statErr)
	}
	if _, statErr := os.Stat(staging.processedDir); statErr != nil {
		t.Errorf("expected processedDir %s to exist: %v", staging.processedDir, statErr)
	}

	// Invoke cleanup and verify removal
	cleanup()
	if _, statErr := os.Stat(staging.rootDir); !os.IsNotExist(statErr) {
		t.Errorf("expected rootDir %s to be removed after cleanup", staging.rootDir)
	}
}

func TestHandler_ValidateS3Key(t *testing.T) {
	ctx := context.Background()
	h := NewHandler(&mockStorageService{}, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())

	tests := []struct {
		name         string
		bucket       string
		key          string
		wantFilename string
		wantValid    bool
	}{
		{
			name:         "valid key",
			bucket:       "b",
			key:          "unprocessed/chase.csv",
			wantFilename: "chase.csv",
			wantValid:    true,
		},
		{
			name:         "valid uppercase extension",
			bucket:       "b",
			key:          "unprocessed/fidelity.CSV",
			wantFilename: "fidelity.CSV",
			wantValid:    true,
		},
		{
			name:         "nested valid key",
			bucket:       "b",
			key:          "unprocessed/2026/01/report.csv",
			wantFilename: "report.csv",
			wantValid:    true,
		},
		{
			name:         "missing unprocessed prefix",
			bucket:       "b",
			key:          "processed/chase.csv",
			wantFilename: "",
			wantValid:    false,
		},
		{
			name:         "non csv extension",
			bucket:       "b",
			key:          "unprocessed/data.json",
			wantFilename: "",
			wantValid:    false,
		},
		{
			name:         "unprocessed prefix only with no filename",
			bucket:       "b",
			key:          "unprocessed/",
			wantFilename: "",
			wantValid:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filename, valid := h.validateS3Key(ctx, tc.bucket, tc.key)
			if valid != tc.wantValid {
				t.Errorf("validateS3Key(%q) valid = %v, want %v", tc.key, valid, tc.wantValid)
			}
			if filename != tc.wantFilename {
				t.Errorf("validateS3Key(%q) filename = %q, want %q", tc.key, filename, tc.wantFilename)
			}
		})
	}
}

func TestHandler_DownloadFile(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockStore := &mockStorageService{}
		h := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
		dest := filepath.Join(t.TempDir(), "test.csv")
		err := h.downloadFile(ctx, "my-bucket", "unprocessed/test.csv", dest)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !mockStore.downloadCalled {
			t.Error("expected download to be called")
		}
	})

	t.Run("failure wraps s3 URI", func(t *testing.T) {
		mockStore := &mockStorageService{
			downloadFunc: func(ctx context.Context, bucket, key, localPath string) error {
				return errors.New("access denied")
			},
		}
		h := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
		err := h.downloadFile(ctx, "my-bucket", "unprocessed/test.csv", "/tmp/nonexistent")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		expectedSub := "s3://my-bucket/unprocessed/test.csv"
		if !strings.Contains(err.Error(), expectedSub) {
			t.Errorf("expected error %q to contain %q", err.Error(), expectedSub)
		}
	})
}

func TestHandler_ArchiveS3File(t *testing.T) {
	ctx := context.Background()

	t.Run("success with nested directory key", func(t *testing.T) {
		mockStore := &mockStorageService{}
		h := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
		err := h.archiveS3File(ctx, "my-bucket", "unprocessed/2026/01/data.csv")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !mockStore.copyCalled {
			t.Error("expected copy to be called")
		}
		if mockStore.copyDestKey != "processed/2026/01/data.csv" {
			t.Errorf("expected dest key 'processed/2026/01/data.csv', got %q", mockStore.copyDestKey)
		}
		if !mockStore.deleteCalled {
			t.Error("expected delete to be called")
		}
	})

	t.Run("copy failure prevents delete", func(t *testing.T) {
		mockStore := &mockStorageService{
			copyFunc: func(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error {
				return errors.New("copy failed")
			},
		}
		h := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
		err := h.archiveS3File(ctx, "my-bucket", "unprocessed/test.csv")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if mockStore.deleteCalled {
			t.Error("delete should not be called when copy fails")
		}
	})

	t.Run("delete failure returns error", func(t *testing.T) {
		mockStore := &mockStorageService{
			deleteFunc: func(ctx context.Context, bucket, key string) error {
				return errors.New("delete failed")
			},
		}
		h := NewHandler(mockStore, &mockSecretsClient{}, "", t.TempDir(), nil, discardLogger())
		err := h.archiveS3File(ctx, "my-bucket", "unprocessed/test.csv")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !mockStore.copyCalled {
			t.Error("copy should be called before delete")
		}
	})
}
