package storage_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/datalake/model"
	"babylon/dataloader/storage"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Mock for IndexView interface.
type mockIndexView struct {
	createOneFunc func(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error)
}

func (m *mockIndexView) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	if m.createOneFunc != nil {
		return m.createOneFunc(ctx, model, opts...)
	}
	return "idx_account_symbol_asof", nil
}

// Mock for DataStore interface.
type mockDataStore struct {
	indexesFunc   func() storage.IndexView
	bulkWriteFunc func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	insertOneFunc func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	findFunc      func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error)
}

func (m *mockDataStore) Indexes() storage.IndexView {
	if m.indexesFunc != nil {
		return m.indexesFunc()
	}
	return &mockIndexView{}
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

func (m *mockDataStore) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	if m.findFunc != nil {
		return m.findFunc(ctx, filter, opts...)
	}
	return &mongo.Cursor{}, nil
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
	stats, err := repo.BulkUpsertTransactions(ctx, transactions)
	if err != nil {
		t.Errorf("BulkUpsertTransactions failed: %v", err)
	}
	if stats.UpsertedCount != 2 {
		t.Errorf("Expected UpsertedCount 2, got %d", stats.UpsertedCount)
	}
}

func TestBulkUpsertTransactions_EmptyTransactions(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMongoRepository(&mockCollectionProvider{})
	stats, err := repo.BulkUpsertTransactions(ctx, []model.Transaction{})
	if err != nil {
		t.Errorf("BulkUpsertTransactions failed for empty transactions: %v", err)
	}
	if stats.UpsertedCount != 0 || stats.MatchedCount != 0 {
		t.Errorf("Expected empty stats, got %+v", stats)
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
	_, err := repo.BulkUpsertTransactions(ctx, transactions)
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
	_, err := repo.BulkUpsertTransactions(ctx, transactions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected sync log error, got: %v", err)
	}
}

func TestGetSyncLogs_Success(t *testing.T) {
	ctx := context.Background()
	expectedLogs := []interface{}{
		model.SyncLog{
			CollectionName:  "transactions_synthetic",
			SyncTimestamp:   time.Now(),
			RecordsUploaded: 10,
		},
	}

	cursor, err := mongo.NewCursorFromDocuments(expectedLogs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mockDS := &mockDataStore{
		findFunc: func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
			return cursor, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name != "dataSync" {
				t.Errorf("Expected collection name dataSync, got %s", name)
			}
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	logs, err := repo.GetSyncLogs(ctx)
	if err != nil {
		t.Fatalf("GetSyncLogs failed: %v", err)
	}

	if len(logs) != 1 {
		t.Errorf("Expected 1 log, got %d", len(logs))
	}
	if logs[0].CollectionName != "transactions_synthetic" {
		t.Errorf("Expected CollectionName transactions_synthetic, got %s", logs[0].CollectionName)
	}
}

func TestGetSyncLogs_FindError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("find error")

	mockDS := &mockDataStore{
		findFunc: func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
			return nil, expectedErr
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	_, err := repo.GetSyncLogs(ctx)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected find error, got: %v", err)
	}
}

func TestGetSyncLogs_DecodeError(t *testing.T) {
	ctx := context.Background()
	corruptedDocs := []interface{}{
		bson.D{
			{Key: "collection_name", Value: "transactions_synthetic"},
			{Key: "sync_timestamp", Value: "invalid-timestamp-type"},
			{Key: "records_uploaded", Value: 10},
		},
	}

	cursor, err := mongo.NewCursorFromDocuments(corruptedDocs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mockDS := &mockDataStore{
		findFunc: func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
			return cursor, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	_, err = repo.GetSyncLogs(ctx)
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode sync logs") {
		t.Errorf("expected error to contain 'failed to decode sync logs', got: %v", err)
	}
}

type mockFidelityStore struct {
	documents map[string]model.FidelityPosition
	syncLogs  []model.SyncLog
}

func applyFidelityWriteModel(store *mockFidelityStore, m mongo.WriteModel) (bool, bool) {
	upd, ok := m.(*mongo.UpdateOneModel)
	if !ok {
		return false, false
	}
	filter, ok := upd.Filter.(bson.M)
	if !ok {
		return false, false
	}
	acct, _ := filter["accountNumber"].(string)
	sym, _ := filter["symbol"].(string)
	asOf, _ := filter["asOfDate"].(time.Time)
	key := fmt.Sprintf("%s|%s|%s", acct, sym, asOf.UTC().Format(time.RFC3339Nano))

	updateBson, ok := upd.Update.(bson.M)
	if !ok {
		return false, false
	}
	doc, ok := updateBson["$set"].(model.FidelityPosition)
	if !ok {
		return false, false
	}

	_, exists := store.documents[key]
	store.documents[key] = doc
	if exists {
		return false, true
	}
	return true, false
}

func newFidelityMockProvider() (*mockCollectionProvider, *mockFidelityStore) {
	store := &mockFidelityStore{
		documents: make(map[string]model.FidelityPosition),
	}

	fidelityDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			var upserted, matched int64
			for _, m := range models {
				isUpserted, isMatched := applyFidelityWriteModel(store, m)
				if isUpserted {
					upserted++
				} else if isMatched {
					matched++
				}
			}
			return &mongo.BulkWriteResult{UpsertedCount: upserted, MatchedCount: matched}, nil
		},
	}

	syncDS := &mockDataStore{
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			if syncLog, ok := document.(model.SyncLog); ok {
				store.syncLogs = append(store.syncLogs, syncLog)
			}
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name == storage.FidelityPositionsCollection {
				return fidelityDS
			}
			if name == "dataSync" {
				return syncDS
			}
			return &mockDataStore{}
		},
	}

	return provider, store
}

func sampleFidelityPositions(asOfDate time.Time) []model.FidelityPosition {
	return []model.FidelityPosition{
		{
			AccountNumber: "SYNTH-ROTH-101",
			AccountName:   "ROTH IRA",
			Symbol:        "SPAXX**",
			Description:   "HELD IN MONEY MARKET",
			CurrentValue:  3250.00,
			AsOfDate:      asOfDate,
			DataSource:    "fidelity",
		},
		{
			AccountNumber: "SYNTH-ROTH-101",
			AccountName:   "ROTH IRA",
			Symbol:        "FSKAX",
			Description:   "FIDELITY TOTAL MARKET INDEX",
			Quantity:      45.250,
			LastPrice:     150.00,
			CurrentValue:  6787.50,
			AsOfDate:      asOfDate,
			DataSource:    "fidelity",
		},
		{
			AccountNumber: "SYNTH-ROTH-101",
			AccountName:   "ROTH IRA",
			Symbol:        "FBGRX",
			Description:   "FIDELITY BLUE CHIP GROWTH",
			Quantity:      10.000,
			LastPrice:     296.25,
			CurrentValue:  2962.50,
			AsOfDate:      asOfDate,
			DataSource:    "fidelity",
		},
		{
			AccountNumber: "SYNTH-401K-202",
			AccountName:   "TRADITIONAL 401K",
			Symbol:        "FXAIX",
			Description:   "FIDELITY 500 INDEX",
			Quantity:      25.500,
			LastPrice:     180.20,
			CurrentValue:  4595.10,
			AsOfDate:      asOfDate,
			DataSource:    "fidelity",
		},
		{
			AccountNumber: "SYNTH-TAX-303",
			AccountName:   "INDIVIDUAL TOD",
			Symbol:        "**",
			CurrentValue:  0.00,
			AsOfDate:      asOfDate,
			DataSource:    "fidelity",
		},
	}
}

// TC-11: Ingests 5 synthetic positions; asserts UpsertedCount == 5, MatchedCount == 0, and database collection contains all 5 records.
func TestBulkUpsertFidelity_Success(t *testing.T) {
	ctx := context.Background()
	provider, store := newFidelityMockProvider()
	repo := storage.NewMongoRepository(provider)

	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	positions := sampleFidelityPositions(asOfDate)

	stats, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err != nil {
		t.Fatalf("BulkUpsertFidelityPositions failed: %v", err)
	}

	if stats.UpsertedCount != 5 {
		t.Errorf("expected UpsertedCount == 5, got %d", stats.UpsertedCount)
	}
	if stats.MatchedCount != 0 {
		t.Errorf("expected MatchedCount == 0, got %d", stats.MatchedCount)
	}
	if len(store.documents) != 5 {
		t.Errorf("expected store to contain 5 documents, got %d", len(store.documents))
	}
}

// TC-12: Re-executes bulk write with identical positions; asserts UpsertedCount == 0, MatchedCount == 5, and total document count remains 5.
func TestBulkUpsertFidelity_Idempotency(t *testing.T) {
	ctx := context.Background()
	provider, store := newFidelityMockProvider()
	repo := storage.NewMongoRepository(provider)

	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	positions := sampleFidelityPositions(asOfDate)

	firstStats, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err != nil {
		t.Fatalf("first BulkUpsertFidelityPositions failed: %v", err)
	}
	if firstStats.UpsertedCount != 5 || firstStats.MatchedCount != 0 {
		t.Fatalf("expected initial upsert 5/0, got %d/%d", firstStats.UpsertedCount, firstStats.MatchedCount)
	}

	secondStats, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err != nil {
		t.Fatalf("second BulkUpsertFidelityPositions failed: %v", err)
	}
	if secondStats.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount == 0 on second run, got %d", secondStats.UpsertedCount)
	}
	if secondStats.MatchedCount != 5 {
		t.Errorf("expected MatchedCount == 5 on second run, got %d", secondStats.MatchedCount)
	}
	if len(store.documents) != 5 {
		t.Errorf("expected total document count to remain 5, got %d", len(store.documents))
	}
}

// TC-13: Inserts same account and symbol across two different asOfDate timestamps; asserts 2 separate historical snapshot documents exist.
func TestBulkUpsertFidelity_HistoricalSnapshots(t *testing.T) {
	ctx := context.Background()
	provider, store := newFidelityMockProvider()
	repo := storage.NewMongoRepository(provider)

	asOfDate1 := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	asOfDate2 := time.Date(2026, time.October, 16, 21, 50, 0, 0, time.UTC)

	pos1 := model.FidelityPosition{
		AccountNumber: "SYNTH-ROTH-101",
		AccountName:   "ROTH IRA",
		Symbol:        "FSKAX",
		Quantity:      45.250,
		CurrentValue:  6787.50,
		AsOfDate:      asOfDate1,
		DataSource:    "fidelity",
	}

	pos2 := model.FidelityPosition{
		AccountNumber: "SYNTH-ROTH-101",
		AccountName:   "ROTH IRA",
		Symbol:        "FSKAX",
		Quantity:      45.250,
		CurrentValue:  6950.00,
		AsOfDate:      asOfDate2,
		DataSource:    "fidelity",
	}

	stats1, err := repo.BulkUpsertFidelityPositions(ctx, []model.FidelityPosition{pos1})
	if err != nil {
		t.Fatalf("first snapshot upsert failed: %v", err)
	}
	if stats1.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount == 1 for first snapshot, got %d", stats1.UpsertedCount)
	}

	stats2, err := repo.BulkUpsertFidelityPositions(ctx, []model.FidelityPosition{pos2})
	if err != nil {
		t.Fatalf("second snapshot upsert failed: %v", err)
	}
	if stats2.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount == 1 for second snapshot, got %d", stats2.UpsertedCount)
	}

	if len(store.documents) != 2 {
		t.Errorf("expected 2 distinct historical snapshot documents, got %d", len(store.documents))
	}
}

// TC-14: Asserts a record is inserted into datalake.dataSync with collectionName == "positions_fidelity" and recordsUploaded == 5.
func TestBulkUpsertFidelity_DataSyncLog(t *testing.T) {
	ctx := context.Background()
	provider, store := newFidelityMockProvider()
	repo := storage.NewMongoRepository(provider)

	asOfDate := time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC)
	positions := sampleFidelityPositions(asOfDate)

	_, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err != nil {
		t.Fatalf("BulkUpsertFidelityPositions failed: %v", err)
	}

	if len(store.syncLogs) != 1 {
		t.Fatalf("expected 1 sync log, got %d", len(store.syncLogs))
	}

	log := store.syncLogs[0]
	if log.CollectionName != storage.FidelityPositionsCollection {
		t.Errorf("expected CollectionName %s, got %s", storage.FidelityPositionsCollection, log.CollectionName)
	}
	if log.RecordsUploaded != 5 {
		t.Errorf("expected RecordsUploaded == 5, got %d", log.RecordsUploaded)
	}
	if log.SyncTimestamp.IsZero() {
		t.Errorf("expected non-zero SyncTimestamp")
	}
}

func TestBulkUpsertFidelity_EmptyPositions(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMongoRepository(&mockCollectionProvider{})
	stats, err := repo.BulkUpsertFidelityPositions(ctx, []model.FidelityPosition{})
	if err != nil {
		t.Fatalf("BulkUpsertFidelityPositions failed on empty slice: %v", err)
	}
	if stats.UpsertedCount != 0 || stats.MatchedCount != 0 {
		t.Errorf("expected zero stats, got %+v", stats)
	}
}

func TestBulkUpsertFidelity_BulkWriteError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated bulk write failure")

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
	positions := []model.FidelityPosition{{AccountNumber: "TEST", Symbol: "SYM"}}
	_, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("expected bulk write error containing %q, got: %v", expectedErr.Error(), err)
	}
}

func TestBulkUpsertFidelity_SyncLogError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated sync log insert failure")

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
	positions := []model.FidelityPosition{{AccountNumber: "TEST", Symbol: "SYM"}}
	_, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("expected sync log error containing %q, got: %v", expectedErr.Error(), err)
	}
}
