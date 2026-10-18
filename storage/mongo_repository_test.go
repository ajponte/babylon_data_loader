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
	createOneFunc  func(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error)
	createManyFunc func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error)
}

func (m *mockIndexView) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	if m.createOneFunc != nil {
		return m.createOneFunc(ctx, model, opts...)
	}
	return "idx_account_symbol_asof", nil
}

func (m *mockIndexView) CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	if m.createManyFunc != nil {
		return m.createManyFunc(ctx, models, opts...)
	}
	return []string{"idx_chase_account_posting_date", "idx_chase_account_post_desc_amount_row"}, nil
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
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name != "transactions_synthetic" {
				t.Errorf("Expected collection name %s, got %s", "transactions_synthetic", name)
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

func TestGetSyncLogs_Success(t *testing.T) {
	ctx := context.Background()
	expectedLogs := []interface{}{
		model.SyncLog{
			CollectionName:        "transactions_synthetic",
			LegacySyncTimestamp:   time.Now(),
			LegacyRecordsUploaded: 10,
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

func TestGetSyncLogsWithLimit(t *testing.T) {
	ctx := context.Background()

	var capturedOpts []*options.FindOptions
	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					return []string{"idx1", "idx2"}, nil
				},
			}
		},
		findFunc: func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
			capturedOpts = opts
			return mongo.NewCursorFromDocuments([]interface{}{}, nil, nil)
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)

	// Test default GetSyncLogs applies limit 100
	_, err := repo.GetSyncLogs(ctx)
	if err != nil {
		t.Fatalf("GetSyncLogs failed: %v", err)
	}
	if len(capturedOpts) == 0 || capturedOpts[0].Limit == nil || *capturedOpts[0].Limit != storage.DefaultSyncLogLimit {
		t.Errorf("expected default limit %d, got %v", storage.DefaultSyncLogLimit, capturedOpts[0].Limit)
	}

	// Test custom limit
	_, err = repo.GetSyncLogsWithLimit(ctx, 42)
	if err != nil {
		t.Fatalf("GetSyncLogsWithLimit failed: %v", err)
	}
	if len(capturedOpts) == 0 || capturedOpts[0].Limit == nil || *capturedOpts[0].Limit != 42 {
		t.Errorf("expected custom limit 42, got %v", capturedOpts[0].Limit)
	}

	// Test non-positive limit (unbounded)
	_, err = repo.GetSyncLogsWithLimit(ctx, 0)
	if err != nil {
		t.Fatalf("GetSyncLogsWithLimit(0) failed: %v", err)
	}
	if len(capturedOpts) > 0 && capturedOpts[0].Limit != nil {
		t.Errorf("expected no limit for limit <= 0, got %v", *capturedOpts[0].Limit)
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

func TestBulkUpsertFidelity_IndexError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated index creation failure")
	var bulkWriteCalled bool

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createOneFunc: func(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
					return "", expectedErr
				},
			}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			bulkWriteCalled = true
			return &mongo.BulkWriteResult{}, nil
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
		t.Fatalf("expected error containing %q, got: %v", expectedErr.Error(), err)
	}
	if bulkWriteCalled {
		t.Errorf("expected BulkWrite NOT to be called when index creation fails")
	}
}

func TestEnsureFidelityIndexes_RetryAfterFailure(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("transient fidelity index creation error")
	var callCount int

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createOneFunc: func(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
					callCount++
					if callCount == 1 {
						return "", expectedErr
					}
					return "idx_account_symbol_asof", nil
				},
			}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return &mongo.BulkWriteResult{UpsertedCount: 1}, nil
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	positions := []model.FidelityPosition{{AccountNumber: "TEST", Symbol: "SYM"}}

	// First attempt fails transiently
	_, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected transient index error on first call, got: %v", err)
	}

	// Second attempt retries index creation and succeeds
	stats, err := repo.BulkUpsertFidelityPositions(ctx, positions)
	if err != nil {
		t.Fatalf("expected second call to succeed on retry, got error: %v", err)
	}
	if stats.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount == 1, got %d", stats.UpsertedCount)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls to createOneFunc, got %d", callCount)
	}
}

// TC-ST-01: Verifies bulk write models, filter keys, collection name (transactions_chase), and sync log entry.
func TestBulkUpsertChaseTransactions_Success(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	postingDate := time.Date(2026, time.January, 15, 0, 0, 0, 0, time.UTC)

	transactions := []model.ChaseTransaction{
		{
			AccountID:       "CHASE-1234",
			PostingDate:     postingDate,
			Description:     "COFFEE SHOP",
			Amount:          -4.50,
			TransactionType: "DEBIT",
			Source: model.SourceLineage{
				FileName:   "Chase1234_Activity.CSV",
				RowNumber:  2,
				IngestedAt: now,
			},
		},
		{
			AccountID:       "CHASE-1234",
			PostingDate:     postingDate,
			Description:     "GROCERY STORE",
			Amount:          -52.10,
			TransactionType: "DEBIT",
			Source: model.SourceLineage{
				FileName:   "Chase1234_Activity.CSV",
				RowNumber:  3,
				IngestedAt: now,
			},
		},
	}

	var capturedModels []mongo.WriteModel
	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			capturedModels = models
			return &mongo.BulkWriteResult{UpsertedCount: 2, MatchedCount: 0}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name != storage.ChaseTransactionsCollection {
				t.Errorf("Unexpected collection name %s", name)
			}
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	stats, err := repo.BulkUpsertChaseTransactions(ctx, transactions)
	if err != nil {
		t.Fatalf("BulkUpsertChaseTransactions failed: %v", err)
	}
	if stats.UpsertedCount != 2 {
		t.Errorf("Expected UpsertedCount 2, got %d", stats.UpsertedCount)
	}

	// Verify write models and compound filter
	if len(capturedModels) != 2 {
		t.Fatalf("Expected 2 write models, got %d", len(capturedModels))
	}
	upd0, ok := capturedModels[0].(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("Expected *mongo.UpdateOneModel, got %T", capturedModels[0])
	}
	filter0, ok := upd0.Filter.(bson.M)
	if !ok {
		t.Fatalf("Expected bson.M filter, got %T", upd0.Filter)
	}
	if filter0["accountId"] != "CHASE-1234" {
		t.Errorf("Expected accountId CHASE-1234, got %v", filter0["accountId"])
	}
	if filter0["postingDate"] != postingDate {
		t.Errorf("Expected postingDate %v, got %v", postingDate, filter0["postingDate"])
	}
	if filter0["description"] != "COFFEE SHOP" {
		t.Errorf("Expected description 'COFFEE SHOP', got %v", filter0["description"])
	}
	if filter0["amount"] != -4.50 {
		t.Errorf("Expected amount -4.50, got %v", filter0["amount"])
	}
	if filter0["source.rowNumber"] != int64(2) {
		t.Errorf("Expected source.rowNumber 2, got %v", filter0["source.rowNumber"])
	}
}

// TC-ST-02: Verifies compound unique index creation with accountId, postingDate, description, amount, source.rowNumber.
func TestEnsureChaseIndexes_ModelDefinitions(t *testing.T) {
	ctx := context.Background()
	var createdIndexes []mongo.IndexModel

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					createdIndexes = models
					return []string{"idx_chase_account_posting_date", "idx_chase_account_post_desc_amount_row"}, nil
				},
			}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return &mongo.BulkWriteResult{UpsertedCount: 1}, nil
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	tx := model.ChaseTransaction{
		AccountID:   "CHASE-1234",
		PostingDate: time.Now(),
		Description: "Test",
		Amount:      10.0,
		Source:      model.SourceLineage{RowNumber: 1},
	}
	_, err := repo.BulkUpsertChaseTransactions(ctx, []model.ChaseTransaction{tx})
	if err != nil {
		t.Fatalf("BulkUpsertChaseTransactions failed: %v", err)
	}

	if len(createdIndexes) != 2 {
		t.Fatalf("Expected 2 index models created, got %d", len(createdIndexes))
	}

	// Verify query index
	qIdx := createdIndexes[0]
	if *qIdx.Options.Name != "idx_chase_account_posting_date" {
		t.Errorf("Expected index name idx_chase_account_posting_date, got %s", *qIdx.Options.Name)
	}

	// Verify compound unique index
	uIdx := createdIndexes[1]
	if *uIdx.Options.Name != "idx_chase_account_post_desc_amount_row" {
		t.Errorf("Expected index name idx_chase_account_post_desc_amount_row, got %s", *uIdx.Options.Name)
	}
	if uIdx.Options.Unique == nil || !*uIdx.Options.Unique {
		t.Errorf("Expected unique=true for idx_chase_account_post_desc_amount_row")
	}
	if uIdx.Options.Sparse == nil || !*uIdx.Options.Sparse {
		t.Errorf("Expected sparse=true for idx_chase_account_post_desc_amount_row")
	}
}

// TC-ST-03: Verifies zero-length transaction slice returns immediately with zero count.
func TestBulkUpsertChaseTransactions_Empty(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMongoRepository(&mockCollectionProvider{})
	stats, err := repo.BulkUpsertChaseTransactions(ctx, []model.ChaseTransaction{})
	if err != nil {
		t.Fatalf("BulkUpsertChaseTransactions failed for empty transactions: %v", err)
	}
	if stats.UpsertedCount != 0 || stats.MatchedCount != 0 || stats.ModifiedCount != 0 {
		t.Errorf("Expected empty stats, got %+v", stats)
	}
}

func TestBulkUpsertChaseTransactions_IndexError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated index creation failure")
	var bulkWriteCalled bool

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					return nil, expectedErr
				},
			}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			bulkWriteCalled = true
			return &mongo.BulkWriteResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	transactions := []model.ChaseTransaction{
		{AccountID: "CHASE-123", Description: "Test"},
	}
	_, err := repo.BulkUpsertChaseTransactions(ctx, transactions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected error containing %q, got: %v", expectedErr.Error(), err)
	}
	if bulkWriteCalled {
		t.Errorf("expected BulkWrite NOT to be called when index creation fails")
	}
}

func TestEnsureChaseIndexes_RetryAfterFailure(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("transient index creation error")
	var callCount int

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					callCount++
					if callCount == 1 {
						return nil, expectedErr
					}
					return []string{"idx1", "idx2"}, nil
				},
			}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return &mongo.BulkWriteResult{UpsertedCount: 1}, nil
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	txs := []model.ChaseTransaction{{AccountID: "TEST", Description: "DESC"}}

	// First attempt fails transiently
	_, err := repo.BulkUpsertChaseTransactions(ctx, txs)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected transient index error on first call, got: %v", err)
	}

	// Second attempt retries index creation and succeeds
	stats, err := repo.BulkUpsertChaseTransactions(ctx, txs)
	if err != nil {
		t.Fatalf("expected second call to succeed on retry, got error: %v", err)
	}
	if stats.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount == 1, got %d", stats.UpsertedCount)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls to createManyFunc, got %d", callCount)
	}
}

func TestBulkUpsertChaseTransactions_BulkWriteError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated bulk write failure")

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{}
		},
		bulkWriteFunc: func(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			return &mongo.BulkWriteResult{UpsertedCount: 2, MatchedCount: 1}, expectedErr
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	transactions := []model.ChaseTransaction{{AccountID: "TEST", Description: "DESC"}}
	stats, err := repo.BulkUpsertChaseTransactions(ctx, transactions)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("expected bulk write error containing %q, got: %v", expectedErr.Error(), err)
	}
	if stats.UpsertedCount != 2 || stats.MatchedCount != 1 {
		t.Errorf("expected partial stats (upserted: 2, matched: 1), got %+v", stats)
	}
}

// TC-SR-01: RecordSyncLog (Success) verifies index creation and document insertion into dataSync.
func TestRecordSyncLog_Success(t *testing.T) {
	ctx := context.Background()
	var createdIndexes []mongo.IndexModel
	var insertedDoc model.SyncLog

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					createdIndexes = models
					return []string{"idx_datasync_synced_at", "idx_datasync_filename_synced_at"}, nil
				},
			}
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			if log, ok := document.(model.SyncLog); ok {
				insertedDoc = log
			}
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			if name != "dataSync" {
				t.Errorf("expected collection name 'dataSync', got %q", name)
			}
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	now := time.Now().UTC()
	log := model.NewSuccessSyncLog("chase.csv", "hash123", "transactions_chase", 10, 8, 2, now, 150)

	err := repo.RecordSyncLog(ctx, log)
	if err != nil {
		t.Fatalf("RecordSyncLog failed: %v", err)
	}

	// Verify automated index creation
	if len(createdIndexes) != 2 {
		t.Fatalf("expected 2 indexes created, got %d", len(createdIndexes))
	}
	if *createdIndexes[0].Options.Name != "idx_datasync_synced_at" {
		t.Errorf("expected index name 'idx_datasync_synced_at', got %q", *createdIndexes[0].Options.Name)
	}
	if *createdIndexes[1].Options.Name != "idx_datasync_filename_synced_at" {
		t.Errorf("expected index name 'idx_datasync_filename_synced_at', got %q", *createdIndexes[1].Options.Name)
	}

	// Verify inserted document
	if insertedDoc.FileName != "chase.csv" {
		t.Errorf("expected FileName 'chase.csv', got %q", insertedDoc.FileName)
	}
	if insertedDoc.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status 'SUCCESS', got %q", insertedDoc.Status)
	}
	if insertedDoc.RecordCount != 10 || insertedDoc.UpsertedCount != 8 || insertedDoc.MatchedCount != 2 {
		t.Errorf("unexpected counts in insertedDoc: %+v", insertedDoc)
	}
}

// TC-SR-02: RecordSyncLog (Failure Entry) verifies insertion of failed sync log with error message.
func TestRecordSyncLog_FailedEntry(t *testing.T) {
	ctx := context.Background()
	var insertedDoc model.SyncLog

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					return []string{"idx1", "idx2"}, nil
				},
			}
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			if log, ok := document.(model.SyncLog); ok {
				insertedDoc = log
			}
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	now := time.Now().UTC()
	failedLog := model.NewFailedSyncLog("bad.csv", "hash456", "transactions_chase", "corrupted CSV header", 0, now, 25)

	err := repo.RecordSyncLog(ctx, failedLog)
	if err != nil {
		t.Fatalf("RecordSyncLog failed: %v", err)
	}

	if insertedDoc.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status 'FAILED', got %q", insertedDoc.Status)
	}
	if insertedDoc.ErrorMessage != "corrupted CSV header" {
		t.Errorf("expected ErrorMessage 'corrupted CSV header', got %q", insertedDoc.ErrorMessage)
	}
	if insertedDoc.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", insertedDoc.UpsertedCount)
	}
}

// TC-SR-03: RecordSyncLog (Index Error) verifies proper error propagation if index creation fails.
func TestRecordSyncLog_IndexError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("index creation failed")
	var insertCalled bool

	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					return nil, expectedErr
				},
			}
		},
		insertOneFunc: func(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
			insertCalled = true
			return &mongo.InsertOneResult{}, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	log := model.NewSuccessSyncLog("file.csv", "hash", "transactions", 5, 5, 0, time.Now(), 50)

	err := repo.RecordSyncLog(ctx, log)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected index error %v, got: %v", expectedErr, err)
	}
	if insertCalled {
		t.Errorf("expected InsertOne NOT to be called when index creation fails")
	}
}

// TC-SR-04: GetSyncLogs (Canonical & Legacy) tests querying mixed legacy and canonical sync logs, verifying descending order and normalization.
func TestGetSyncLogs_MixedCanonicalAndLegacy(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	earlier := now.Add(-2 * time.Hour)

	mixedDocs := []interface{}{
		// Canonical doc (newer)
		model.NewSuccessSyncLog("chase.csv", "hash1", "transactions_chase", 20, 18, 2, now, 100),
		// Legacy BSON-like document (older)
		bson.D{
			{Key: "collection_name", Value: "transactions_synthetic"},
			{Key: "sync_timestamp", Value: earlier},
			{Key: "records_uploaded", Value: int64(15)},
		},
	}

	cursor, err := mongo.NewCursorFromDocuments(mixedDocs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	var capturedSort bson.D
	mockDS := &mockDataStore{
		indexesFunc: func() storage.IndexView {
			return &mockIndexView{
				createManyFunc: func(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
					return []string{"idx1", "idx2"}, nil
				},
			}
		},
		findFunc: func(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
			if len(opts) > 0 && opts[0].Sort != nil {
				if s, ok := opts[0].Sort.(bson.D); ok {
					capturedSort = s
				}
			}
			return cursor, nil
		},
	}

	provider := &mockCollectionProvider{
		collectionFunc: func(name string) storage.DataStore {
			return mockDS
		},
	}

	repo := storage.NewMongoRepository(provider)
	logs, err := repo.GetSyncLogs(ctx)
	if err != nil {
		t.Fatalf("GetSyncLogs failed: %v", err)
	}

	if len(logs) != 2 {
		t.Fatalf("expected 2 logs returned, got %d", len(logs))
	}

	assertMixedSortOptions(t, capturedSort)
	assertCanonicalLog(t, logs[0])
	assertLegacyLog(t, logs[1], earlier)
}

func assertMixedSortOptions(t *testing.T, sort bson.D) {
	t.Helper()
	if len(sort) != 1 {
		t.Fatalf("expected 1 sort key, got %d", len(sort))
	}
	if sort[0].Key != "syncedAt" || sort[0].Value != -1 {
		t.Errorf("expected sort syncedAt: -1, got %+v", sort[0])
	}
}

func assertCanonicalLog(t *testing.T, log model.SyncLog) {
	t.Helper()
	if log.FileName != "chase.csv" || log.CollectionName != "transactions_chase" {
		t.Errorf("unexpected canonical log: %+v", log)
	}
	if log.RecordCount != 20 || log.UpsertedCount != 18 || log.MatchedCount != 2 {
		t.Errorf("unexpected canonical counts: %+v", log)
	}
}

func assertLegacyLog(t *testing.T, log model.SyncLog, earlier time.Time) {
	t.Helper()
	if log.CollectionName != "transactions_synthetic" {
		t.Errorf("expected CollectionName 'transactions_synthetic', got %q", log.CollectionName)
	}
	if !log.SyncedAt.Equal(earlier) {
		t.Errorf("expected SyncedAt normalized to %v, got %v", earlier, log.SyncedAt)
	}
	if log.RecordCount != 15 || log.UpsertedCount != 15 {
		t.Errorf("expected RecordCount/UpsertedCount normalized to 15, got %d / %d", log.RecordCount, log.UpsertedCount)
	}
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status normalized to 'SUCCESS', got %q", log.Status)
	}
}
