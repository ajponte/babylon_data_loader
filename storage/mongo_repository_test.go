package storage_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"babylon/dataloader/datalake/model"
	"babylon/dataloader/storage"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Mock for DataStore interface.
type mockDataStore struct {
	bulkWriteFunc func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	insertOneFunc func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
}

func (m *mockDataStore) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	if m.bulkWriteFunc != nil {
		return m.bulkWriteFunc(ctx, models, opts...)
	}
	return &mongo.BulkWriteResult{}, nil
}

func (m *mockDataStore) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	if m.insertOneFunc != nil {
		return m.insertOneFunc(ctx, document, opts...)
	}
	return &mongo.InsertOneResult{}, nil
}

// Mock for CollectionProvider interface.
type mockCollectionProvider struct {
	collectionFunc func(name string) storage.DataStore
}

func (m *mockCollectionProvider) Collection(name string) storage.DataStore {
	if m.collectionFunc != nil {
		return m.collectionFunc(name)
	}
	return &mockDataStore{}
}

func TestNewMongoRepository(t *testing.T) {
	provider := &mockCollectionProvider{}
	repo := storage.NewMongoRepository(provider)
	if repo == nil {
		t.Error("NewMongoRepository returned nil")
	}
}

func TestBulkUpsertTransactions_Success(t *testing.T) {
	ctx := context.Background()
	transactions := []model.Transaction{
		{Details: "Test1", PostingDate: "2024-01-01", DataSource: "synthetic", AccountID: "123"},
		{Details: "Test2", PostingDate: "2024-01-02", DataSource: "synthetic", AccountID: "123"},
	}

	mockDS := &mockDataStore{
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			if len(models) != 2 {
				t.Errorf("Expected 2 write models, got %d", len(models))
			}
			return &mongo.BulkWriteResult{UpsertedCount: 2}, nil
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			syncLog, ok := document.(model.SyncLog)
			if !ok {
				t.Errorf("Expected SyncLog document, got %T", document)
			}
			if syncLog.CollectionName != "transactions_synthetic" {
				t.Errorf("Expected CollectionName %s, got %s", "transactions_synthetic", syncLog.CollectionName)
			}
			if syncLog.RecordsUploaded != int64(len(transactions)) {
				t.Errorf("Expected RecordsUploaded %d, got %d", len(transactions), syncLog.RecordsUploaded)
			}
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name != "transactions_synthetic" && name != "dataSync" {
				t.Errorf("Expected collection name %s or dataSync, got %s", "transactions_synthetic", name)
			}
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	err := repo.BulkUpsertTransactions(ctx, transactions)
	if err != nil {
		t.Errorf("BulkUpsertTransactions failed: %v", err)
	}
}

func TestBulkUpsertTransactions_EmptyTransactions(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMongoRepository(&mockCollectionProvider{})
	err := repo.BulkUpsertTransactions(ctx, []model.Transaction{})
	if err != nil {
		t.Errorf("BulkUpsertTransactions failed for empty transactions: %v", err)
	}
}

func TestBulkUpsertTransactions_BulkWriteError(t *testing.T) {
	ctx := context.Background()
	transactions := []model.Transaction{
		{Details: "Test1", PostingDate: "2024-01-01", DataSource: "synthetic", AccountID: "123"},
	}
	expectedErr := errors.New("bulk write error")

	mockDS := &mockDataStore{
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return nil, expectedErr
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	err := repo.BulkUpsertTransactions(ctx, transactions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected bulk write error, got: %v", err)
	}
}

func TestBulkUpsertTransactions_SyncLogError(t *testing.T) {
	ctx := context.Background()
	transactions := []model.Transaction{
		{Details: "Test1", PostingDate: "2024-01-01", DataSource: "synthetic", AccountID: "123"},
	}
	expectedErr := errors.New("sync log error")

	mockDS := &mockDataStore{
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return &mongo.BulkWriteResult{UpsertedCount: 1}, nil
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			return nil, expectedErr
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	err := repo.BulkUpsertTransactions(ctx, transactions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected sync log error, got: %v", err)
	}
}
