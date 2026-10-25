package mongo

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"babylon/dataloader/internal/domain/model"
)

type fakeCollection struct {
	name            string
	db              *fakeDatabase
	mu              sync.Mutex
	insertedDocs    []any
	createdIndex    [][]mongodrv.IndexModel
	insertOneErr    error
	createIdxErr    error
	failIdxCount    int
	idxCalls        int32
	bulkWriteModels []mongodrv.WriteModel
	bulkWriteOpts   []*options.BulkWriteOptions
	bulkWriteResult *mongodrv.BulkWriteResult
	bulkWriteErr    error
	bulkCalls       int32
}

func (f *fakeCollection) InsertOne(_ context.Context, doc any, _ ...*options.InsertOneOptions) (*mongodrv.InsertOneResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertOneErr != nil {
		return nil, f.insertOneErr
	}
	f.insertedDocs = append(f.insertedDocs, doc)
	return &mongodrv.InsertOneResult{}, nil
}

func (f *fakeCollection) BulkWrite(
	_ context.Context,
	models []mongodrv.WriteModel,
	opts ...*options.BulkWriteOptions,
) (*mongodrv.BulkWriteResult, error) {
	atomic.AddInt32(&f.bulkCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bulkWriteModels = append(f.bulkWriteModels, models...)
	f.bulkWriteOpts = append(f.bulkWriteOpts, opts...)
	if f.bulkWriteErr != nil {
		return f.bulkWriteResult, f.bulkWriteErr
	}
	if f.bulkWriteResult != nil {
		return f.bulkWriteResult, nil
	}
	return &mongodrv.BulkWriteResult{
		UpsertedCount: int64(len(models)),
	}, nil
}

func (f *fakeCollection) CreateIndexes(_ context.Context, models []mongodrv.IndexModel) error {
	atomic.AddInt32(&f.idxCalls, 1)
	if f.db != nil {
		f.db.mu.Lock()
		f.db.order = append(f.db.order, f.name)
		f.db.mu.Unlock()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failIdxCount > 0 {
		f.failIdxCount--
		return f.createIdxErr
	}
	f.createdIndex = append(f.createdIndex, models)
	return nil
}

type fakeDatabase struct {
	mu          sync.Mutex
	collections map[string]*fakeCollection
	order       []string
}

func newFakeDatabase() *fakeDatabase {
	return &fakeDatabase{
		collections: make(map[string]*fakeCollection),
	}
}

func (f *fakeDatabase) Collection(name string) collection {
	f.mu.Lock()
	defer f.mu.Unlock()
	coll, ok := f.collections[name]
	if !ok {
		coll = &fakeCollection{name: name, db: f}
		f.collections[name] = coll
	}
	return coll
}

func (f *fakeDatabase) dataSyncColl() *fakeCollection {
	return f.Collection(dataSyncCollection).(*fakeCollection)
}

func (f *fakeDatabase) collectionColl(name string) *fakeCollection {
	return f.Collection(name).(*fakeCollection)
}

func goldenSyncLogSuccessFields() []bsonFieldExpectation {
	return []bsonFieldExpectation{
		{key: "fileName", typ: bson.TypeString},
		{key: "fileHash", typ: bson.TypeString},
		{key: "collectionName", typ: bson.TypeString},
		{key: "recordCount", typ: bson.TypeInt64},
		{key: "upsertedCount", typ: bson.TypeInt64},
		{key: "matchedCount", typ: bson.TypeInt64},
		{key: "status", typ: bson.TypeString},
		{key: "syncedAt", typ: bson.TypeDateTime},
		{key: "durationMs", typ: bson.TypeInt64},
		{key: "collection_name", typ: bson.TypeString},
		{key: "sync_timestamp", typ: bson.TypeDateTime},
		{key: "records_uploaded", typ: bson.TypeInt64},
	}
}

func goldenSyncLogFailureFields() []bsonFieldExpectation {
	return []bsonFieldExpectation{
		{key: "fileName", typ: bson.TypeString},
		{key: "fileHash", typ: bson.TypeString},
		{key: "collectionName", typ: bson.TypeString},
		{key: "recordCount", typ: bson.TypeInt64},
		{key: "upsertedCount", typ: bson.TypeInt64},
		{key: "matchedCount", typ: bson.TypeInt64},
		{key: "status", typ: bson.TypeString},
		{key: "errorMessage", typ: bson.TypeString},
		{key: "syncedAt", typ: bson.TypeDateTime},
		{key: "durationMs", typ: bson.TypeInt64},
		{key: "collection_name", typ: bson.TypeString},
		{key: "sync_timestamp", typ: bson.TypeDateTime},
	}
}

func newTestSyncLog(status model.SyncStatus) *model.SyncLog {
	ts := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	if status == model.SyncStatusSuccess {
		return model.NewSuccessSyncLog("chase.csv", "sha256hash", "chase", 10, 8, 2, ts, 150)
	}
	return model.NewFailedSyncLog("chase.csv", "sha256hash", "chase", "syntax error", 10, ts, 50)
}

func TestSyncLogDoc_LegacyBSONParity(t *testing.T) {
	successDoc := toSyncLogDoc(newTestSyncLog(model.SyncStatusSuccess))
	assertDocFields(t, mustMarshal(t, successDoc), goldenSyncLogSuccessFields())

	failureDoc := toSyncLogDoc(newTestSyncLog(model.SyncStatusFailed))
	assertDocFields(t, mustMarshal(t, failureDoc), goldenSyncLogFailureFields())
}

func TestLogSync_WritesDualFields(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	log := newTestSyncLog(model.SyncStatusSuccess)
	if err := repo.LogSync(context.Background(), log); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	coll := fakeDB.dataSyncColl()
	if len(coll.insertedDocs) != 1 {
		t.Fatalf("expected 1 inserted doc, got %d", len(coll.insertedDocs))
	}
	doc := coll.insertedDocs[0].(syncLogDoc)
	if doc.DualCollectionName != doc.CollectionName || !doc.DualSyncTimestamp.Equal(doc.SyncedAt) || doc.DualRecordsUploaded != doc.UpsertedCount {
		t.Errorf("dual fields mismatch: got %+v", doc)
	}
}

func TestLogSync_RejectsZeroSyncedAt(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	log := &model.SyncLog{FileName: "chase.csv", CollectionName: "chase", RecordCount: 5, Status: model.SyncStatusSuccess}
	if err := repo.LogSync(context.Background(), log); !errors.Is(err, model.ErrMissingSyncTime) {
		t.Fatalf("expected ErrMissingSyncTime, got %v", err)
	}
	if len(fakeDB.dataSyncColl().insertedDocs) != 0 {
		t.Fatalf("expected 0 inserted docs")
	}
}

func TestLogSync_RejectsNilAndInvalid(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	if err := repo.LogSync(context.Background(), nil); !errors.Is(err, model.ErrNilSyncLog) {
		t.Fatalf("expected ErrNilSyncLog, got %v", err)
	}
	invalid := &model.SyncLog{FileName: "chase.csv", CollectionName: "chase", Status: model.SyncStatusFailed, SyncedAt: time.Now()}
	if err := repo.LogSync(context.Background(), invalid); !errors.Is(err, model.ErrMissingErrorMessage) {
		t.Fatalf("expected ErrMissingErrorMessage, got %v", err)
	}
}

func TestLogSync_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	if err := repo.LogSync(ctx, newTestSyncLog(model.SyncStatusSuccess)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(fakeDB.dataSyncColl().insertedDocs) != 0 {
		t.Fatalf("expected 0 inserted docs")
	}
}

func TestLogSync_InsertErrorWrapped(t *testing.T) {
	fakeDB := newFakeDatabase()
	driverErr := errors.New("underlying driver error")
	fakeDB.dataSyncColl().insertOneErr = driverErr
	repo := newRepository(fakeDB)

	err := repo.LogSync(context.Background(), newTestSyncLog(model.SyncStatusSuccess))
	if !errors.Is(err, driverErr) {
		t.Fatalf("expected driver error wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), dataSyncCollection) {
		t.Fatalf("expected error to contain collection %q, got %q", dataSyncCollection, err.Error())
	}
}

func TestLogSync_DoesNotMutateInput(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	log := newTestSyncLog(model.SyncStatusSuccess)
	original := *log
	if err := repo.LogSync(context.Background(), log); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *log != original {
		t.Fatalf("input struct was mutated: before=%+v, after=%+v", original, *log)
	}
}

func TestEnsureIndexes_DataSyncCatalogParity(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)
	if err := repo.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	coll := fakeDB.dataSyncColl()
	if len(coll.createdIndex) != 1 || len(coll.createdIndex[0]) != 2 {
		t.Fatalf("expected 1 call with 2 models, got %d calls", len(coll.createdIndex))
	}
	m0, m1 := coll.createdIndex[0][0], coll.createdIndex[0][1]
	if m0.Options == nil || *m0.Options.Name != "idx_datasync_synced_at" || !reflect.DeepEqual(m0.Keys, bson.D{{Key: "syncedAt", Value: -1}}) {
		t.Errorf("model 0 mismatch: %+v", m0)
	}
	wantKeys1 := bson.D{{Key: "fileName", Value: 1}, {Key: "syncedAt", Value: -1}}
	if m1.Options == nil || *m1.Options.Name != "idx_datasync_filename_synced_at" || !reflect.DeepEqual(m1.Keys, wantKeys1) {
		t.Errorf("model 1 mismatch: %+v", m1)
	}
}

func TestEnsureIndexes_RetriesAfterFailure(t *testing.T) {
	fakeDB := newFakeDatabase()
	coll := fakeDB.dataSyncColl()
	coll.createIdxErr = errors.New("transient network failure")
	coll.failIdxCount = 1
	repo := newRepository(fakeDB)

	if err := repo.EnsureIndexes(context.Background()); err == nil {
		t.Fatalf("expected error on first call, got nil")
	}
	if err := repo.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("expected success on second call, got %v", err)
	}
	if err := repo.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("expected success on third call, got %v", err)
	}
	const expectedDriverCalls = 2
	if atomic.LoadInt32(&coll.idxCalls) != expectedDriverCalls {
		t.Fatalf("expected %d driver calls, got %d", expectedDriverCalls, atomic.LoadInt32(&coll.idxCalls))
	}
}

func TestEnsureIndexes_ConcurrentCallsCreateOnce(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			if err := repo.EnsureIndexes(context.Background()); err != nil {
				t.Errorf("unexpected concurrent error: %v", err)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&fakeDB.dataSyncColl().idxCalls) != 1 {
		t.Fatalf("expected 1 CreateIndexes call, got %d", atomic.LoadInt32(&fakeDB.dataSyncColl().idxCalls))
	}
}

func TestNewRepository_RequiresURI(t *testing.T) {
	repo, err := NewRepository(context.Background(), Config{URI: ""})
	if repo != nil || !errors.Is(err, ErrMissingURI) {
		t.Fatalf("expected nil repo and ErrMissingURI, got repo=%v, err=%v", repo, err)
	}
}

func TestNewRepository_ErrorDoesNotLeakCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	cfg := Config{
		URI:     "mongodb://user:s3cr3t@127.0.0.1:1/?serverSelectionTimeoutMS=100",
		Timeout: 200 * time.Millisecond,
	}
	repo, err := NewRepository(ctx, cfg)
	if repo != nil || err == nil {
		t.Fatalf("expected error and nil repo, got repo=%v, err=%v", repo, err)
	}
	errStr := err.Error()
	if strings.Contains(errStr, "s3cr3t") || strings.Contains(errStr, cfg.URI) {
		t.Errorf("error leaked credentials or URI: %q", errStr)
	}
}

func TestNewRepository_AppliesDefaults(t *testing.T) {
	cfg := Config{URI: "mongodb://localhost:27017"}.withDefaults()
	if cfg.Database != DefaultDatabase || cfg.Timeout != DefaultTimeout {
		t.Errorf("expected defaults (%s, %v), got (%s, %v)", DefaultDatabase, DefaultTimeout, cfg.Database, cfg.Timeout)
	}
}

func TestClose_NilSafe(t *testing.T) {
	var nilRepo *Repository
	if err := nilRepo.Close(context.Background()); err != nil {
		t.Errorf("expected nil error on nil repo Close, got %v", err)
	}
	emptyRepo := &Repository{}
	if err := emptyRepo.Close(context.Background()); err != nil {
		t.Errorf("expected nil error on empty repo Close, got %v", err)
	}
}
