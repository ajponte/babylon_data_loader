package main

import (
	"babylon/dataloader/config"
	"babylon/dataloader/datalake"
	"babylon/dataloader/datalake/datasource"
	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"
	"babylon/dataloader/storage"
	"context"
	"errors"
	"testing"

	csvparser "babylon/dataloader/csv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mockMongoClient wrapping a client that won't panic on Database/Collection calls
type mockMongoClient struct {
	client *mongo.Client
}

func newMockMongoClient() *mockMongoClient {
	// Construct a client without connecting it
	cli, _ := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	return &mockMongoClient{client: cli}
}

func (m *mockMongoClient) Disconnect(ctx context.Context) error {
	return nil
}

func (m *mockMongoClient) Database(name string, opts ...*options.DatabaseOptions) *mongo.Database {
	return m.client.Database(name, opts...)
}

// mockRepo implements repository.Repository
type mockRepo struct{}

func (m *mockRepo) BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) (repository.UpsertStats, error) {
	return repository.UpsertStats{}, nil
}

// mockDatalakeClient implements datalake.Client
type mockDatalakeClient struct {
	ingestCalled bool
	retStats     *datalake.Stats
	retErr       error
}

func (m *mockDatalakeClient) IngestCSVFiles(
	ctx context.Context,
	repo repository.Repository,
	extractor datasource.InfoExtractor,
	parser csvparser.Parser,
	unprocessedDir string,
	processedDir string,
	moveProcessedFiles bool,
) (*datalake.Stats, error) {
	return nil, nil
}

func (m *mockDatalakeClient) IngestCSVFile(
	ctx context.Context,
	repo repository.Repository,
	parser csvparser.Parser,
	filePath string,
	dataSource string,
	accountID string,
	opts datalake.IngestFileOptions,
) (*datalake.Stats, error) {
	m.ingestCalled = true
	return m.retStats, m.retErr
}

func TestApp_OnStartup_Success(t *testing.T) {
	origConnect := storage.ConnectToMongoDBFunc
	storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
		return newMockMongoClient(), nil
	}
	defer func() {
		storage.ConnectToMongoDBFunc = origConnect
	}()

	app := NewApp()
	ctx := context.Background()
	app.OnStartup(ctx)

	if !app.dbConn {
		t.Error("Expected dbConn to be true on successful connection")
	}
	if app.mongo == nil {
		t.Error("Expected mongo client to be initialized")
	}
	if app.repo == nil {
		t.Error("Expected repo to be initialized")
	}
}

func TestApp_OnStartup_Failure(t *testing.T) {
	origConnect := storage.ConnectToMongoDBFunc
	storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
		return nil, errors.New("connection timeout")
	}
	defer func() {
		storage.ConnectToMongoDBFunc = origConnect
	}()

	app := NewApp()
	ctx := context.Background()
	app.OnStartup(ctx)

	if app.dbConn {
		t.Error("Expected dbConn to be false on connection failure")
	}
	if app.mongo != nil {
		t.Error("Expected mongo client to be nil on failure")
	}
	if app.repo != nil {
		t.Error("Expected repo to be nil on failure")
	}
}

func TestApp_RetryConnectDB(t *testing.T) {
	origConnect := storage.ConnectToMongoDBFunc
	defer func() {
		storage.ConnectToMongoDBFunc = origConnect
	}()

	t.Run("success after failure", func(t *testing.T) {
		storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
			return nil, errors.New("error")
		}
		app := NewApp()
		ctx := context.Background()
		app.OnStartup(ctx)

		if app.dbConn {
			t.Fatal("expected connection failed")
		}

		storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
			return newMockMongoClient(), nil
		}

		success := app.RetryConnectDB()
		if !success {
			t.Error("expected RetryConnectDB to return true")
		}
		if !app.dbConn {
			t.Error("expected dbConn to be true after retry success")
		}
	})

	t.Run("re-failure", func(t *testing.T) {
		storage.ConnectToMongoDBFunc = func(ctx context.Context, uri string) (storage.MongoClient, error) {
			return nil, errors.New("error")
		}
		app := NewApp()
		ctx := context.Background()
		app.OnStartup(ctx)

		success := app.RetryConnectDB()
		if success {
			t.Error("expected RetryConnectDB to return false on connection failure")
		}
		if app.dbConn {
			t.Error("expected dbConn to remain false")
		}
	})
}

func TestApp_IngestFile_NotConnected(t *testing.T) {
	app := NewApp()
	app.ctx = context.Background()
	app.cfg = &config.Config{}
	app.dbConn = false // degraded state

	_, err := app.IngestFile("file.csv", "chase", "1234")
	if err == nil {
		t.Error("Expected IngestFile to return error when not connected to database")
	}
}

func TestApp_IngestFile_Success(t *testing.T) {
	app := NewApp()
	app.ctx = context.Background()
	app.cfg = &config.Config{}
	app.dbConn = true
	app.repo = &mockRepo{}

	mockStats := datalake.NewStats()
	mockStats.TotalFiles = 1
	mockStats.IncrementProcessed()

	dlMock := &mockDatalakeClient{
		retStats: mockStats,
		retErr:   nil,
	}
	app.datalake = dlMock

	stats, err := app.IngestFile("path/to/file.csv", "synthetic", "9999")
	if err != nil {
		t.Fatalf("IngestFile returned unexpected error: %v", err)
	}

	if !dlMock.ingestCalled {
		t.Error("Expected IngestCSVFile to be called on datalake client")
	}

	if stats.ProcessedFiles != 1 {
		t.Errorf("Expected 1 processed file, got %d", stats.ProcessedFiles)
	}
}
