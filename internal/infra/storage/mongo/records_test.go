package mongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
)

func setupTestRepo() (*Repository, *fakeDatabase) {
	db := newFakeDatabase()
	return newRepository(db), db
}

func newValidTestBatch(dest model.Destination, count int) *model.IngestionBatch {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	lineage := model.SourceLineage{FileName: "f.csv", FileHash: "h1", RowNumber: 1, IngestedAt: now}

	var payload any
	switch dest {
	case model.DestChaseChecking, model.DestSynthetic:
		txs := make([]model.ChaseTransaction, count)
		for i := range count {
			txs[i] = model.ChaseTransaction{
				AccountID: "a1", PostingDate: now, Description: "T", Amount: 10.5, Type: "DEBIT", Lineage: lineage,
			}
		}
		payload = txs
	case model.DestFidelityPosition:
		positions := make([]model.FidelityPosition, count)
		for i := range count {
			positions[i] = model.FidelityPosition{
				AccountID: "fid1", Symbol: "AAPL", Quantity: 10, AsOfDate: now, Lineage: lineage,
			}
		}
		payload = positions
	}

	return &model.IngestionBatch{
		Destination: dest, AccountID: "a1", SourceFile: "f.csv", FileHash: "h1",
		IngestedAt: now, RecordCount: count, Payload: payload,
	}
}

func assertSyncLog(t *testing.T, log *model.SyncLog, wantStatus model.SyncStatus, wantColl string, n, up, match int64) {
	t.Helper()
	if log == nil {
		t.Fatal("expected non-nil SyncLog")
	}
	if err := log.Validate(); err != nil {
		t.Fatalf("expected valid SyncLog, got error: %v", err)
	}
	if log.Status != wantStatus || log.CollectionName != wantColl {
		t.Errorf("status/collection got (%s, %s), want (%s, %s)", log.Status, log.CollectionName, wantStatus, wantColl)
	}
	if log.RecordCount != n || log.UpsertedCount != up || log.MatchedCount != match {
		t.Errorf("counts got (%d, %d, %d), want (%d, %d, %d)", log.RecordCount, log.UpsertedCount, log.MatchedCount, n, up, match)
	}
	if log.DurationMs < 0 {
		t.Errorf("expected non-negative duration, got %d", log.DurationMs)
	}
}

func TestSaveBatch_NilAndInvalidBatch(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	validNow := time.Now()

	tests := []struct {
		name        string
		batch       *model.IngestionBatch
		expectedErr error
	}{
		{name: "nil batch", batch: nil, expectedErr: model.ErrNilBatch},
		{
			name: "invalid dest",
			batch: &model.IngestionBatch{
				Destination: "bad", AccountID: "a", SourceFile: "f.csv", IngestedAt: validNow,
			},
			expectedErr: model.ErrInvalidDestination,
		},
		{
			name: "missing account ID",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking, SourceFile: "f.csv", IngestedAt: validNow,
			},
			expectedErr: model.ErrMissingAccountID,
		},
		{
			name: "missing file name",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking, AccountID: "a", IngestedAt: validNow,
			},
			expectedErr: model.ErrMissingFileName,
		},
		{
			name: "missing ingested at",
			batch: &model.IngestionBatch{
				Destination: model.DestChaseChecking, AccountID: "a", SourceFile: "f.csv",
			},
			expectedErr: model.ErrMissingIngestedAt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			log, err := repo.SaveBatch(context.Background(), tc.batch)
			if log != nil || !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected (nil, %v), got (%v, %v)", tc.expectedErr, log, err)
			}
			coll := fakeDB.collectionColl(chaseCollection)
			if coll.bulkCalls != 0 || coll.idxCalls != 0 {
				t.Fatalf("expected zero calls, got bulk=%d idx=%d", coll.bulkCalls, coll.idxCalls)
			}
		})
	}
}

func TestSaveBatch_EmptyBatchSkipsWrite(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	batch := newValidTestBatch(model.DestChaseChecking, 0)
	batch.Payload = nil

	log, err := repo.SaveBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSyncLog(t, log, model.SyncStatusSuccess, chaseCollection, 0, 0, 0)

	coll := fakeDB.collectionColl(chaseCollection)
	if coll.bulkCalls != 0 || coll.idxCalls != 0 {
		t.Fatalf("expected no driver calls on empty batch, got bulk=%d idx=%d", coll.bulkCalls, coll.idxCalls)
	}
}

func TestSaveBatch_ChaseSuccessBuildsSyncLog(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	fixedTime := time.Date(2026, 10, 25, 15, 30, 0, 0, time.UTC)
	repo.now = func() time.Time { return fixedTime }

	coll := fakeDB.collectionColl(chaseCollection)
	coll.bulkWriteResult = &mongodrv.BulkWriteResult{UpsertedCount: 2, MatchedCount: 1}

	log, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 3))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSyncLog(t, log, model.SyncStatusSuccess, chaseCollection, 3, 2, 1)
	if !log.SyncedAt.Equal(fixedTime) {
		t.Errorf("expected SyncedAt %v, got %v", fixedTime, log.SyncedAt)
	}
}

func TestSaveBatch_UnorderedUpsertModels(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	_, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	coll := fakeDB.collectionColl(chaseCollection)
	if len(coll.bulkWriteOpts) == 0 || coll.bulkWriteOpts[0].Ordered == nil || *coll.bulkWriteOpts[0].Ordered != false {
		t.Fatal("expected BulkWrite Ordered option false")
	}
	if len(coll.bulkWriteModels) != 2 {
		t.Fatalf("expected 2 write models, got %d", len(coll.bulkWriteModels))
	}
	for i, m := range coll.bulkWriteModels {
		up, ok := m.(*mongodrv.UpdateOneModel)
		if !ok || up.Upsert == nil || !*up.Upsert {
			t.Fatalf("model %d expected *UpdateOneModel with Upsert=true, got %v", i, m)
		}
	}
}

func TestSaveBatch_RoutesAllDestinations(t *testing.T) {
	tests := []struct {
		name         string
		dest         model.Destination
		expectedColl string
	}{
		{name: "Chase Checking", dest: model.DestChaseChecking, expectedColl: chaseCollection},
		{name: "Chase Credit Card", dest: model.DestChaseCreditCard, expectedColl: chaseCollection},
		{name: "Synthetic", dest: model.DestSynthetic, expectedColl: syntheticCollection},
		{name: "Fidelity Position", dest: model.DestFidelityPosition, expectedColl: fidelityCollection},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, fakeDB := setupTestRepo()
			log, err := repo.SaveBatch(context.Background(), newValidTestBatch(tc.dest, 1))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertSyncLog(t, log, model.SyncStatusSuccess, tc.expectedColl, 1, 1, 0)
			if coll := fakeDB.collectionColl(tc.expectedColl); coll.bulkCalls != 1 {
				t.Errorf("expected 1 bulk call on %s, got %d", tc.expectedColl, coll.bulkCalls)
			}
		})
	}
}

func TestSaveBatch_PayloadMismatchReturnsFailedLog(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	batch := newValidTestBatch(model.DestChaseChecking, 1)
	batch.Payload = "not-a-slice"

	log, err := repo.SaveBatch(context.Background(), batch)
	if !errors.Is(err, model.ErrPayloadTypeMismatch) {
		t.Fatalf("expected ErrPayloadTypeMismatch, got %v", err)
	}
	assertSyncLog(t, log, model.SyncStatusFailed, chaseCollection, 1, 0, 0)
	if fakeDB.collectionColl(chaseCollection).bulkCalls != 0 {
		t.Error("expected no bulk calls")
	}
}

func TestSaveBatch_FidelityZeroAsOfDate(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	batch := newValidTestBatch(model.DestFidelityPosition, 1)
	batch.Payload = []model.FidelityPosition{{AccountID: "fid1", Symbol: "AAPL"}}

	log, err := repo.SaveBatch(context.Background(), batch)
	if !errors.Is(err, model.ErrMissingAsOfDate) {
		t.Fatalf("expected ErrMissingAsOfDate, got %v", err)
	}
	assertSyncLog(t, log, model.SyncStatusFailed, fidelityCollection, 1, 0, 0)
	if fakeDB.collectionColl(fidelityCollection).bulkCalls != 0 {
		t.Error("expected no bulk calls")
	}
}

func TestSaveBatch_BulkWriteFailureReturnsFailedLog(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	coll := fakeDB.collectionColl(chaseCollection)
	coll.bulkWriteErr = errors.New("simulated failure")

	log, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 2))
	if err == nil || !strings.Contains(err.Error(), "mongo: save batch to transactions_chase:") {
		t.Fatalf("expected wrapped error with collection name, got %v", err)
	}
	assertSyncLog(t, log, model.SyncStatusFailed, chaseCollection, 2, 0, 0)
	if log.ErrorMessage != "simulated failure" {
		t.Errorf("expected error message to match, got %q", log.ErrorMessage)
	}
}

func TestSaveBatch_PartialResultDiscarded(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	coll := fakeDB.collectionColl(chaseCollection)
	coll.bulkWriteResult = &mongodrv.BulkWriteResult{UpsertedCount: 5, MatchedCount: 2}
	coll.bulkWriteErr = errors.New("partial bulk write failure")

	log, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 7))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertSyncLog(t, log, model.SyncStatusFailed, chaseCollection, 7, 0, 0)
}

func TestSaveBatch_IndexEnsureFailureThenRetry(t *testing.T) {
	repo, fakeDB := setupTestRepo()
	coll := fakeDB.collectionColl(chaseCollection)
	coll.failIdxCount = 1
	coll.createIdxErr = errors.New("index creation failed")

	batch := newValidTestBatch(model.DestChaseChecking, 1)

	// First attempt fails during index ensure.
	log1, err1 := repo.SaveBatch(context.Background(), batch)
	if err1 == nil {
		t.Fatal("expected error on first attempt, got nil")
	}
	assertSyncLog(t, log1, model.SyncStatusFailed, chaseCollection, 1, 0, 0)
	if coll.bulkCalls != 0 {
		t.Fatalf("expected no bulk write on index failure, got %d", coll.bulkCalls)
	}

	// Second attempt succeeds and retries index creation.
	log2, err2 := repo.SaveBatch(context.Background(), batch)
	if err2 != nil {
		t.Fatalf("expected second attempt success, got %v", err2)
	}
	assertSyncLog(t, log2, model.SyncStatusSuccess, chaseCollection, 1, 1, 0)
	if coll.idxCalls != 2 || coll.bulkCalls != 1 {
		t.Fatalf("expected 2 index attempts and 1 bulk write, got idx=%d bulk=%d", coll.idxCalls, coll.bulkCalls)
	}
}

func TestSaveBatch_ClassifiesErrors(t *testing.T) {
	tests := []struct {
		name        string
		inputErr    error
		expectTrans bool
		expectDup   bool
	}{
		{name: "nil error", inputErr: nil},
		{name: "deadline exceeded", inputErr: context.DeadlineExceeded, expectTrans: true},
		{name: "network labeled", inputErr: mongodrv.CommandError{Labels: []string{"NetworkError"}}, expectTrans: true},
		{name: "duplicate key 11000", inputErr: mongodrv.CommandError{Code: 11000}, expectDup: true},
		{name: "generic", inputErr: errors.New("generic")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			classified := classify(tc.inputErr)
			if tc.inputErr == nil {
				if classified != nil {
					t.Fatalf("expected nil classified error, got %v", classified)
				}
				return
			}
			if errors.Is(classified, ErrTransient) != tc.expectTrans {
				t.Errorf("expected ErrTransient=%v, got %v", tc.expectTrans, errors.Is(classified, ErrTransient))
			}
			if errors.Is(classified, ErrDuplicateKey) != tc.expectDup {
				t.Errorf("expected ErrDuplicateKey=%v, got %v", tc.expectDup, errors.Is(classified, ErrDuplicateKey))
			}
		})
	}

	t.Run("SaveBatch wraps BulkWrite transient error", func(t *testing.T) {
		repo, fakeDB := setupTestRepo()
		fakeDB.collectionColl(chaseCollection).bulkWriteErr = mongodrv.CommandError{Labels: []string{"NetworkError"}}
		if _, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 1)); !errors.Is(err, ErrTransient) {
			t.Fatalf("expected ErrTransient, got %v", err)
		}
	})

	t.Run("SaveBatch wraps BulkWrite duplicate key error", func(t *testing.T) {
		repo, fakeDB := setupTestRepo()
		fakeDB.collectionColl(chaseCollection).bulkWriteErr = mongodrv.CommandError{Code: 11000}
		if _, err := repo.SaveBatch(context.Background(), newValidTestBatch(model.DestChaseChecking, 1)); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("expected ErrDuplicateKey, got %v", err)
		}
	})
}

func TestSaveBatch_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repo, fakeDB := setupTestRepo()
	log, err := repo.SaveBatch(ctx, newValidTestBatch(model.DestChaseChecking, 1))
	if log != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected (nil, context.Canceled), got (%v, %v)", log, err)
	}
	if coll := fakeDB.collectionColl(chaseCollection); coll.bulkCalls != 0 || coll.idxCalls != 0 {
		t.Fatalf("expected zero driver calls, got bulk=%d idx=%d", coll.bulkCalls, coll.idxCalls)
	}
}

func TestRepository_ImplementsPort(t *testing.T) {
	var repo any = (*Repository)(nil)
	if _, ok := repo.(port.Repository); !ok {
		t.Fatal("expected *Repository to implement port.Repository")
	}
}

func TestRepository_TimeNow(t *testing.T) {
	var nilRepo *Repository
	if nilRepo.timeNow().IsZero() {
		t.Error("expected non-zero time for nil repo timeNow")
	}
	emptyRepo := &Repository{}
	if emptyRepo.timeNow().IsZero() {
		t.Error("expected non-zero time for empty repo timeNow")
	}
}

func TestRepository_Elapsed_NegativeClampedToZero(t *testing.T) {
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := &Repository{now: func() time.Time { return past }}
	if _, dur := repo.elapsed(past.Add(1 * time.Hour)); dur != 0 {
		t.Errorf("expected 0 duration when elapsed is negative, got %d", dur)
	}
}
