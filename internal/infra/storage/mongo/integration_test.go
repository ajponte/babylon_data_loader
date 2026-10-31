//go:build integration

package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// validateIntegrationTarget enforces strict isolation guards for integration testing (AC-3H-3, RF-01).
func validateIntegrationTarget(rawURI, dbName string) error {
	cleanedURI := strings.ToLower(strings.TrimSpace(rawURI))
	if cleanedURI == "" || strings.HasPrefix(cleanedURI, "mongodb+srv://") {
		return errors.New("invalid or forbidden mongo URI")
	}
	cleanedDB := strings.ToLower(strings.TrimSpace(dbName))
	if cleanedDB == "" || cleanedDB == "datalake" {
		return errors.New("invalid or forbidden test database name")
	}
	return nil
}

func setupIntegration(t *testing.T) (*Repository, *mongodrv.Database) {
	t.Helper()
	uri := os.Getenv("MONGO_IT_URI")
	if uri == "" {
		t.Skip("skipping integration test: MONGO_IT_URI is unset")
	}
	dbName := fmt.Sprintf("babylon_it_%d", time.Now().UnixNano())
	if err := validateIntegrationTarget(uri, dbName); err != nil {
		t.Fatalf("integration guard violation: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repo, err := NewRepository(ctx, Config{URI: uri, Database: dbName, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("failed to connect to mongo: %v", err)
	}

	rawDB := repo.client.Database(dbName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = rawDB.Drop(cleanupCtx)
		_ = repo.Close(cleanupCtx)
	})
	return repo, rawDB
}

type indexRecord struct {
	Name   string `bson:"name"`
	Key    bson.D `bson:"key"`
	Unique bool   `bson:"unique"`
	Sparse bool   `bson:"sparse"`
}

func fetchIndexes(t *testing.T, coll *mongodrv.Collection) map[string]indexRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes failed: %v", err)
	}
	defer cursor.Close(ctx)

	var list []indexRecord
	if decodeErr := cursor.All(ctx, &list); decodeErr != nil {
		t.Fatalf("decode indexes failed: %v", decodeErr)
	}
	res := make(map[string]indexRecord, len(list))
	for _, idx := range list {
		res[idx.Name] = idx
	}
	return res
}

func assertIndex(t *testing.T, indexes map[string]indexRecord, name string, wantKeys bson.D, wantUnique, wantSparse bool) {
	t.Helper()
	idx, ok := indexes[name]
	if !ok {
		t.Fatalf("index %q not found in collection", name)
	}
	if idx.Unique != wantUnique || idx.Sparse != wantSparse {
		t.Fatalf("index %q options mismatch: unique=%v sparse=%v; want %v %v",
			name, idx.Unique, idx.Sparse, wantUnique, wantSparse)
	}
	assertIndexKeyMatch(t, name, idx.Key, wantKeys)
}

func assertIndexKeyMatch(t *testing.T, name string, actual, expected bson.D) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("index %q key length mismatch: got %d, want %d", name, len(actual), len(expected))
	}
	for i, elem := range expected {
		if actual[i].Key != elem.Key || toInt64(actual[i].Value) != toInt64(elem.Value) {
			t.Errorf("index %q key[%d] mismatch: got %v=%v, want %v=%v", name, i, actual[i].Key, actual[i].Value, elem.Key, elem.Value)
		}
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}

func newIntegrationChaseTx(acct string, row int, isCreditCard bool, t time.Time) model.ChaseTransaction {
	var txDate *time.Time
	if isCreditCard {
		cardDate := t.Add(-24 * time.Hour)
		txDate = &cardDate
	}
	bal := 100.50
	return model.ChaseTransaction{
		AccountID: acct, PostingDate: t, TransactionDate: txDate, Description: "GROCERY PURCHASE",
		Amount: -50.25, Type: "DEBIT", Balance: &bal,
		Lineage: model.SourceLineage{FileName: "chase.csv", FileHash: "hash123", RowNumber: row, IngestedAt: t},
	}
}

func newIntegrationFidelityPos(acct, sym string, asOf time.Time, row int) model.FidelityPosition {
	return model.FidelityPosition{
		AccountID: acct, AccountName: "Brokerage", Symbol: sym, Description: "Total Stock Index",
		Quantity: 15.0, LastPrice: 200.0, LastPriceChange: 1.5, CurrentValue: 3000.0,
		TodaysGainLossDollar: 22.5, TodaysGainLossPct: 0.75, TotalGainLossDollar: 450.0, TotalGainLossPct: 17.65,
		PercentOfAccount: 35.0, CostBasis: 2550.0, AverageCostBasis: 170.0, Type: "Cash", AsOfDate: asOf,
		Lineage: model.SourceLineage{FileName: "fid.csv", FileHash: "fidhash123", RowNumber: row, IngestedAt: asOf},
	}
}

func newIntegrationBatch(dest model.Destination, acct, file string, payload any, count int, t time.Time) *model.IngestionBatch {
	return &model.IngestionBatch{
		Destination: dest, AccountID: acct, SourceFile: file,
		FileHash: "batchhash123", IngestedAt: t, RecordCount: count, Payload: payload,
	}
}

func assertBSONDate(t *testing.T, val any, field string) {
	t.Helper()
	switch val.(type) {
	case primitive.DateTime, time.Time:
	default:
		t.Errorf("field %s expected date type, got %T (%v)", field, val, val)
	}
}

func assertBSONInt64(t *testing.T, val any, field string) {
	t.Helper()
	if _, ok := val.(int64); !ok {
		t.Errorf("field %s expected int64, got %T (%v)", field, val, val)
	}
}

func assertLineageBSON(t *testing.T, sourceVal any) {
	t.Helper()
	src, ok := sourceVal.(bson.M)
	if !ok {
		t.Fatalf("expected source subdocument, got %T", sourceVal)
	}
	assertBSONInt64(t, src["rowNumber"], "source.rowNumber")
	if _, hasS3 := src["s3Uri"]; hasS3 {
		t.Errorf("source.s3Uri should be absent on fresh insert, got %v", src["s3Uri"])
	}
}

func TestIntegration_HarnessGuards(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		uri     string
		db      string
		wantErr bool
	}{
		{"refuses empty uri", "", "babylon_it_1", true},
		{"refuses whitespace uri", "   ", "babylon_it_1", true},
		{"refuses mongodb+srv", "mongodb+srv://atlas.mongodb.net", "babylon_it_1", true},
		{"refuses datalake db", "mongodb://localhost:27017", "datalake", true},
		{"refuses datalake case-insensitive", "mongodb://localhost:27017", "DataLake", true},
		{"refuses empty db", "mongodb://localhost:27017", "", true},
		{"accepts isolated it db", "mongodb://localhost:27017", "babylon_it_12345", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateIntegrationTarget(tc.uri, tc.db)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateIntegrationTarget(%q, %q) error = %v, wantErr %v", tc.uri, tc.db, err, tc.wantErr)
			}
		})
	}
}

func TestIntegration_EnsureIndexes_CreatesLegacyCatalog(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = rawDB.CreateCollection(ctx, syntheticCollection)
	if err := repo.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes failed: %v", err)
	}

	dataSyncIndexes := fetchIndexes(t, rawDB.Collection(dataSyncCollection))
	assertIndex(t, dataSyncIndexes, "idx_datasync_synced_at", bson.D{{Key: fieldSyncedAt, Value: -1}}, false, false)
	assertIndex(t, dataSyncIndexes, "idx_datasync_filename_synced_at", bson.D{{Key: fieldFileName, Value: 1}, {Key: fieldSyncedAt, Value: -1}}, false, false)

	chaseIndexes := fetchIndexes(t, rawDB.Collection(chaseCollection))
	assertIndex(t, chaseIndexes, "idx_chase_account_posting_date", bson.D{{Key: fieldAccountID, Value: 1}, {Key: fieldPostingDate, Value: -1}}, false, false)
	assertIndex(t, chaseIndexes, "idx_chase_account_post_desc_amount_row", bson.D{
		{Key: fieldAccountID, Value: 1}, {Key: fieldPostingDate, Value: 1}, {Key: fieldDescription, Value: 1}, {Key: fieldAmount, Value: 1}, {Key: fieldSourceRowNumber, Value: 1},
	}, true, true)

	fidelityIndexes := fetchIndexes(t, rawDB.Collection(fidelityCollection))
	assertIndex(t, fidelityIndexes, "idx_account_symbol_asof", bson.D{
		{Key: fieldAccountNumber, Value: 1}, {Key: fieldSymbol, Value: 1}, {Key: fieldAsOfDate, Value: 1},
	}, true, false)

	synthIndexes := fetchIndexes(t, rawDB.Collection(syntheticCollection))
	if len(synthIndexes) != 1 || synthIndexes["_id_"].Name == "" {
		t.Fatalf("expected exactly 1 index (_id_) on synthetic collection, got %+v", synthIndexes)
	}
}

func TestIntegration_EnsureIndexes_NoOpOnPreexistingLegacyIndexes(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, ci := range indexCatalog() {
		models := toIndexModels(ci.indexes)
		if _, err := rawDB.Collection(ci.collection).Indexes().CreateMany(ctx, models); err != nil {
			t.Fatalf("failed pre-creating indexes on %s: %v", ci.collection, err)
		}
	}

	before := fetchIndexes(t, rawDB.Collection(chaseCollection))
	if err := repo.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes failed on pre-existing indexes: %v", err)
	}
	after := fetchIndexes(t, rawDB.Collection(chaseCollection))

	if len(before) != len(after) {
		t.Fatalf("index count changed: before %d, after %d", len(before), len(after))
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			t.Errorf("index %q missing after EnsureIndexes", k)
		}
	}
}

func TestIntegration_EnsureIndexes_NameConflictFailsLoudly(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conflictModel := mongodrv.IndexModel{
		Keys: bson.D{
			{Key: fieldAccountID, Value: 1}, {Key: fieldPostingDate, Value: 1}, {Key: fieldDescription, Value: 1}, {Key: fieldAmount, Value: 1}, {Key: fieldSourceRowNumber, Value: 1},
		},
		Options: options.Index().SetName("idx_chase_dedup").SetUnique(true).SetSparse(true),
	}
	if _, err := rawDB.Collection(chaseCollection).Indexes().CreateOne(ctx, conflictModel); err != nil {
		t.Fatalf("failed pre-creating conflicting index: %v", err)
	}

	err := repo.EnsureIndexes(ctx)
	if err == nil {
		t.Fatal("expected EnsureIndexes to fail loudly on name conflict, got nil")
	}
	if !strings.Contains(err.Error(), chaseCollection) {
		t.Fatalf("expected error to name %s, got %v", chaseCollection, err)
	}
}

func TestIntegration_SaveBatch_RerunIsIdempotent(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	txs := []model.ChaseTransaction{
		newIntegrationChaseTx("acct-1", 1, false, now),
		newIntegrationChaseTx("acct-1", 2, false, now),
		newIntegrationChaseTx("acct-1", 3, false, now),
	}
	batch := newIntegrationBatch(model.DestChaseChecking, "acct-1", "chase.csv", txs, len(txs), now)

	log1, err := repo.SaveBatch(ctx, batch)
	if err != nil || log1.UpsertedCount != 3 || log1.MatchedCount != 0 {
		t.Fatalf("first SaveBatch failed: err %v, upserted %d, matched %d", err, log1.UpsertedCount, log1.MatchedCount)
	}
	count, err := rawDB.Collection(chaseCollection).CountDocuments(ctx, bson.D{})
	if err != nil || count != 3 {
		t.Fatalf("expected count 3, got count %d, err %v", count, err)
	}

	log2, err := repo.SaveBatch(ctx, batch)
	if err != nil || log2.UpsertedCount != 0 || log2.MatchedCount != 3 {
		t.Fatalf("second SaveBatch failed: err %v, upserted %d, matched %d", err, log2.UpsertedCount, log2.MatchedCount)
	}
	countAfter, err := rawDB.Collection(chaseCollection).CountDocuments(ctx, bson.D{})
	if err != nil || countAfter != 3 {
		t.Fatalf("expected count 3 after rerun, got count %d, err %v", countAfter, err)
	}
}

func TestIntegration_SaveBatch_IdenticalSameDayChargesKeptByRowNumber(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	txs := []model.ChaseTransaction{
		newIntegrationChaseTx("acct-dup", 1, false, now),
		newIntegrationChaseTx("acct-dup", 2, false, now),
	}
	batch := newIntegrationBatch(model.DestChaseChecking, "acct-dup", "chase_dup.csv", txs, 2, now)
	log, err := repo.SaveBatch(ctx, batch)
	if err != nil || log.UpsertedCount != 2 {
		t.Fatalf("SaveBatch failed: err %v, upserted %d", err, log.UpsertedCount)
	}
	count, err := rawDB.Collection(chaseCollection).CountDocuments(ctx, bson.D{})
	if err != nil || count != 2 {
		t.Fatalf("expected count 2, got %d, err %v", count, err)
	}

	cursor, err := rawDB.Collection(chaseCollection).Find(ctx, bson.D{{Key: fieldAccountID, Value: "acct-dup"}})
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	if decodeErr := cursor.All(ctx, &docs); decodeErr != nil || len(docs) != 2 {
		t.Fatalf("expected 2 decoded docs, got %d, err %v", len(docs), decodeErr)
	}
	rows := make(map[int64]bool)
	for _, d := range docs {
		rows[d["source"].(bson.M)["rowNumber"].(int64)] = true
	}
	if !rows[1] || !rows[2] {
		t.Errorf("expected distinct row numbers 1 and 2, got %v", rows)
	}
}

func TestIntegration_SaveBatch_FidelitySnapshotHistoryPreserved(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	date1 := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)

	pos1 := newIntegrationFidelityPos("fid-hist", "AAPL", date1, 1)
	pos2 := newIntegrationFidelityPos("fid-hist", "AAPL", date2, 2)

	batch := newIntegrationBatch(model.DestFidelityPosition, "fid-hist", "fid.csv", []model.FidelityPosition{pos1, pos2}, 2, date2)
	log1, err := repo.SaveBatch(ctx, batch)
	if err != nil || log1.UpsertedCount != 2 {
		t.Fatalf("first SaveBatch failed: err %v, upserted %d", err, log1.UpsertedCount)
	}
	count, err := rawDB.Collection(fidelityCollection).CountDocuments(ctx, bson.D{})
	if err != nil || count != 2 {
		t.Fatalf("expected 2 positions, got %d, err %v", count, err)
	}

	rerunBatch := newIntegrationBatch(model.DestFidelityPosition, "fid-hist", "fid.csv", []model.FidelityPosition{pos2}, 1, date2)
	log2, err := repo.SaveBatch(ctx, rerunBatch)
	if err != nil || log2.UpsertedCount != 0 || log2.MatchedCount != 1 {
		t.Fatalf("rerun SaveBatch failed: err %v, upserted %d, matched %d", err, log2.UpsertedCount, log2.MatchedCount)
	}
	countAfter, err := rawDB.Collection(fidelityCollection).CountDocuments(ctx, bson.D{})
	if err != nil || countAfter != 2 {
		t.Fatalf("expected 2 positions after rerun, got %d, err %v", countAfter, err)
	}
}

func TestIntegration_SaveBatch_PreservesLegacyOnlyFields(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	asOf := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)

	seededFidelity := bson.M{
		"accountNumber": "fid-pres", "symbol": "VTI", "todaysGainLossDollar": 10.0, "asOfDate": asOf, "dataSource": dataSourceFidelity,
		"source": bson.M{"fileName": "old_f.csv", "rowNumber": int64(1), "ingestedAt": asOf, "s3Uri": "s3://legacy-datalake/fidelity/old_f.csv"},
	}
	if _, err := rawDB.Collection(fidelityCollection).InsertOne(ctx, seededFidelity); err != nil {
		t.Fatalf("failed seeding legacy fidelity doc: %v", err)
	}

	pos := newIntegrationFidelityPos("fid-pres", "VTI", asOf, 1)
	pos.TodaysGainLossDollar = 99.5
	pos.Lineage.FileName = "new_f.csv"

	batch := newIntegrationBatch(model.DestFidelityPosition, "fid-pres", "new_f.csv", []model.FidelityPosition{pos}, 1, asOf)
	if _, err := repo.SaveBatch(ctx, batch); err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}

	var doc bson.M
	filter := bson.D{{Key: fieldAccountNumber, Value: "fid-pres"}, {Key: fieldSymbol, Value: "VTI"}, {Key: fieldAsOfDate, Value: asOf}}
	if err := rawDB.Collection(fidelityCollection).FindOne(ctx, filter).Decode(&doc); err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}

	src := doc["source"].(bson.M)
	if src["s3Uri"] != "s3://legacy-datalake/fidelity/old_f.csv" {
		t.Errorf("expected source.s3Uri preserved, got %v", src["s3Uri"])
	}
	if src["fileName"] != "new_f.csv" || doc["todaysGainLossDollar"] != 99.5 {
		t.Errorf("expected fields refreshed: fileName=%v todaysGainLossDollar=%v", src["fileName"], doc["todaysGainLossDollar"])
	}
}

func TestIntegration_LegacyBSONParity_StoredDocuments(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	cardTx := newIntegrationChaseTx("acct-card", 1, true, now)
	checkTx := newIntegrationChaseTx("acct-check", 2, false, now)
	synthTx := newIntegrationChaseTx("acct-synth", 1, false, now)
	fidPos := newIntegrationFidelityPos("acct-fid", "MSFT", now, 1)

	if _, err := repo.SaveBatch(ctx, newIntegrationBatch(model.DestChaseChecking, "acct-chase", "c.csv", []model.ChaseTransaction{cardTx, checkTx}, 2, now)); err != nil {
		t.Fatalf("save chase batch failed: %v", err)
	}
	if _, err := repo.SaveBatch(ctx, newIntegrationBatch(model.DestSynthetic, "acct-synth", "s.csv", []model.ChaseTransaction{synthTx}, 1, now)); err != nil {
		t.Fatalf("save synth batch failed: %v", err)
	}
	if _, err := repo.SaveBatch(ctx, newIntegrationBatch(model.DestFidelityPosition, "acct-fid", "f.csv", []model.FidelityPosition{fidPos}, 1, now)); err != nil {
		t.Fatalf("save fidelity batch failed: %v", err)
	}
	if err := repo.LogSync(ctx, model.NewSuccessSyncLog("f.csv", "hash", chaseCollection, 1, 1, 0, now, 50)); err != nil {
		t.Fatalf("log sync failed: %v", err)
	}

	for _, tc := range []struct {
		coll   string
		filter bson.D
		check  func(bson.M)
	}{
		{chaseCollection, bson.D{{Key: fieldAccountID, Value: "acct-card"}}, func(d bson.M) {
			assertBSONDate(t, d[fieldPostingDate], fieldPostingDate)
			assertBSONDate(t, d["transactionDate"], "transactionDate")
			assertLineageBSON(t, d["source"])
		}},
		{chaseCollection, bson.D{{Key: fieldAccountID, Value: "acct-check"}}, func(d bson.M) {
			if val, ok := d["transactionDate"]; ok && val != nil {
				t.Errorf("transactionDate should be absent on checking insert, got %v", val)
			}
			if bal, ok := d["balance"].(float64); !ok || bal != 100.50 {
				t.Errorf("balance expected float64 100.50, got %v (%T)", d["balance"], d["balance"])
			}
			assertLineageBSON(t, d["source"])
		}},
		{fidelityCollection, bson.D{{Key: fieldAccountNumber, Value: "acct-fid"}}, func(d bson.M) {
			assertBSONDate(t, d[fieldAsOfDate], fieldAsOfDate)
			assertLineageBSON(t, d["source"])
			for _, f := range []string{"lastPriceChange", "todaysGainLossDollar", "todaysGainLossPct", "totalGainLossDollar", "totalGainLossPct", "percentOfAccount", "averageCostBasis", "type"} {
				if _, ok := d[f]; !ok {
					t.Errorf("fidelity 2E field %q missing from stored document", f)
				}
			}
		}},
		{dataSyncCollection, bson.D{{Key: fieldFileName, Value: "f.csv"}}, func(d bson.M) {
			assertBSONDate(t, d[fieldSyncedAt], fieldSyncedAt)
			assertBSONDate(t, d["sync_timestamp"], "sync_timestamp")
			assertBSONInt64(t, d["recordCount"], "recordCount")
			assertBSONInt64(t, d["records_uploaded"], "records_uploaded")
			if d["collection_name"] != d["collectionName"] {
				t.Errorf("collection_name (%v) != collectionName (%v)", d["collection_name"], d["collectionName"])
			}
		}},
	} {
		var d bson.M
		if err := rawDB.Collection(tc.coll).FindOne(ctx, tc.filter).Decode(&d); err != nil {
			t.Fatalf("find doc in %s failed: %v", tc.coll, err)
		}
		tc.check(d)
	}
}

func TestIntegration_LogSync_ListSyncLogs_RoundTrip(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(-2 * time.Minute)
	t2 := base.Add(-1 * time.Minute)
	t3 := base

	successLog := model.NewSuccessSyncLog("success.csv", "hash1", chaseCollection, 10, 10, 0, t1, 100)
	if err := repo.LogSync(ctx, successLog); err != nil {
		t.Fatalf("log success failed: %v", err)
	}

	legacyDoc := bson.M{
		"fileName": "legacy.csv", "collection_name": chaseCollection, "sync_timestamp": t2, "records_uploaded": int64(25),
	}
	if _, err := rawDB.Collection(dataSyncCollection).InsertOne(ctx, legacyDoc); err != nil {
		t.Fatalf("seed legacy sync doc failed: %v", err)
	}

	failedLog := model.NewFailedSyncLog("failed.csv", "hash2", chaseCollection, "io error", 5, t3, 150)
	if err := repo.LogSync(ctx, failedLog); err != nil {
		t.Fatalf("log failed failed: %v", err)
	}

	logs, err := repo.ListSyncLogs(ctx, 10)
	if err != nil || len(logs) != 3 {
		t.Fatalf("ListSyncLogs failed: err %v, count %d", err, len(logs))
	}
	// Invariant (Phase 3G §5 table): Documents are sorted in MongoDB by {syncedAt: -1}.
	// Legacy-only documents lacking syncedAt evaluate as null/missing in BSON sort and sort last.
	if !logs[0].SyncedAt.Equal(t3) || logs[0].Status != model.SyncStatusFailed || logs[0].ErrorMessage != "io error" {
		t.Errorf("logs[0] mismatch: %+v", logs[0])
	}
	if !logs[1].SyncedAt.Equal(t1) || logs[1].Status != model.SyncStatusSuccess || logs[1].RecordCount != 10 {
		t.Errorf("logs[1] mismatch: %+v", logs[1])
	}
	if !logs[2].SyncedAt.Equal(t2) || logs[2].Status != model.SyncStatusSuccess || logs[2].RecordCount != 25 || logs[2].UpsertedCount != 25 {
		t.Errorf("logs[2] legacy normalization mismatch: %+v", logs[2])
	}

	subLogs, err := repo.ListSyncLogs(ctx, 2)
	if err != nil || len(subLogs) != 2 {
		t.Fatalf("sub-limit ListSyncLogs failed: err %v, count %d", err, len(subLogs))
	}
	if !subLogs[0].SyncedAt.Equal(t3) || !subLogs[1].SyncedAt.Equal(t1) {
		t.Errorf("subLogs ordering mismatch: %+v", subLogs)
	}
}

func TestIntegration_SaveBatch_SyntheticUsesChaseShape(t *testing.T) {
	repo, rawDB := setupIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	synthTx := newIntegrationChaseTx("acct-synth-shape", 1, false, now)
	batch := newIntegrationBatch(model.DestSynthetic, "acct-synth-shape", "synth.csv", []model.ChaseTransaction{synthTx}, 1, now)

	log, err := repo.SaveBatch(ctx, batch)
	if err != nil || log.CollectionName != syntheticCollection || log.UpsertedCount != 1 {
		t.Fatalf("unexpected synthetic save result: err %v, log %+v", err, log)
	}

	var doc bson.M
	if findErr := rawDB.Collection(syntheticCollection).FindOne(ctx, bson.D{{Key: fieldAccountID, Value: "acct-synth-shape"}}).Decode(&doc); findErr != nil {
		t.Fatalf("FindOne synthetic failed: %v", findErr)
	}
	if doc["dataSource"] != dataSourceSynthetic || doc[fieldDescription] != "GROCERY PURCHASE" {
		t.Errorf("mismatch in synthetic stored doc: dataSource=%v, desc=%v", doc["dataSource"], doc[fieldDescription])
	}

	synthIndexes := fetchIndexes(t, rawDB.Collection(syntheticCollection))
	if len(synthIndexes) != 1 || synthIndexes["_id_"].Name == "" {
		t.Fatalf("expected exactly 1 index (_id_) on synthetic collection, got %+v", synthIndexes)
	}
}
