package mongo

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

func newTestChaseTx(acct string, row int, isCreditCard bool) model.ChaseTransaction {
	now := time.Date(2026, 10, 20, 14, 30, 0, 0, time.UTC)
	var txDate *time.Time
	if isCreditCard {
		t := now.Add(-24 * time.Hour)
		txDate = &t
	}
	bal := 1250.75
	return model.ChaseTransaction{
		AccountID:       acct,
		PostingDate:     now,
		TransactionDate: txDate,
		Description:     "TEST TRANSACTION",
		Amount:          -42.50,
		Type:            "Sale",
		Balance:         &bal,
		Lineage: model.SourceLineage{
			FileName:   "test_statement.csv",
			FileHash:   "abc123hash",
			RowNumber:  row,
			IngestedAt: now,
		},
	}
}

func newTestFidelityPos(acct, sym string, asOf time.Time) model.FidelityPosition {
	now := time.Date(2026, 10, 20, 14, 30, 0, 0, time.UTC)
	return model.FidelityPosition{
		AccountID:            acct,
		AccountName:          "Individual",
		Symbol:               sym,
		Description:          "Total Market ETF",
		Quantity:             10.5,
		LastPrice:            220.0,
		LastPriceChange:      1.25,
		CurrentValue:         2310.0,
		TodaysGainLossDollar: 13.12,
		TodaysGainLossPct:    0.57,
		TotalGainLossDollar:  310.0,
		TotalGainLossPct:     15.5,
		PercentOfAccount:     25.0,
		CostBasis:            2000.0,
		AverageCostBasis:     190.47,
		Type:                 "Cash",
		AsOfDate:             asOf,
		Lineage: model.SourceLineage{
			FileName:   "positions.csv",
			FileHash:   "def456hash",
			RowNumber:  1,
			IngestedAt: now,
		},
	}
}

func extractDocKeys(d bson.D) []string {
	keys := make([]string, len(d))
	for i, elem := range d {
		keys[i] = elem.Key
	}
	return keys
}

func findDocValue(d bson.D, key string) (any, bool) {
	for _, elem := range d {
		if elem.Key == key {
			return elem.Value, true
		}
	}
	return nil, false
}

// 1. TestIndexCatalog_LegacyParity: Verify chaseIndexes() and fidelityIndexes() match §5 byte-for-byte;
// verify no synthetic entry.
func TestIndexCatalog_LegacyParity(t *testing.T) {
	chase := chaseIndexes()
	if len(chase) != 2 {
		t.Fatalf("expected 2 chase indexes, got %d", len(chase))
	}

	// Query index
	qIdx := chase[0]
	if qIdx.name != "idx_chase_account_posting_date" || qIdx.unique || qIdx.sparse {
		t.Errorf("chase index 0 flags mismatch: %+v", qIdx)
	}
	wantQKeys := bson.D{{Key: "accountId", Value: 1}, {Key: "postingDate", Value: -1}}
	if !reflect.DeepEqual(qIdx.keys, wantQKeys) {
		t.Errorf("chase index 0 keys mismatch: got %+v, want %+v", qIdx.keys, wantQKeys)
	}

	// Unique compound index
	uIdx := chase[1]
	if uIdx.name != "idx_chase_account_post_desc_amount_row" || !uIdx.unique || !uIdx.sparse {
		t.Errorf("chase index 1 flags mismatch: %+v", uIdx)
	}
	wantUKeys := bson.D{
		{Key: "accountId", Value: 1},
		{Key: "postingDate", Value: 1},
		{Key: "description", Value: 1},
		{Key: "amount", Value: 1},
		{Key: "source.rowNumber", Value: 1},
	}
	if !reflect.DeepEqual(uIdx.keys, wantUKeys) {
		t.Errorf("chase index 1 keys mismatch: got %+v, want %+v", uIdx.keys, wantUKeys)
	}

	// Fidelity unique compound index
	fid := fidelityIndexes()
	if len(fid) != 1 {
		t.Fatalf("expected 1 fidelity index, got %d", len(fid))
	}
	fIdx := fid[0]
	if fIdx.name != "idx_account_symbol_asof" || !fIdx.unique || fIdx.sparse {
		t.Errorf("fidelity index flags mismatch: %+v", fIdx)
	}
	wantFidKeys := bson.D{
		{Key: "accountNumber", Value: 1},
		{Key: "symbol", Value: 1},
		{Key: "asOfDate", Value: 1},
	}
	if !reflect.DeepEqual(fIdx.keys, wantFidKeys) {
		t.Errorf("fidelity index keys mismatch: got %+v, want %+v", fIdx.keys, wantFidKeys)
	}

	// Verify synthetic collection has NO index registered in catalog
	catalog := indexCatalog()
	for _, ci := range catalog {
		if ci.collection == syntheticCollection || ci.collection == "transactions_synthetic" {
			t.Errorf("synthetic collection must have no index specs in catalog (D1)")
		}
	}

	// Verify toIndexModel produces exact IndexModel matching spec
	fModel := toIndexModel(fIdx)
	if *fModel.Options.Name != "idx_account_symbol_asof" || *fModel.Options.Unique != true || fModel.Options.Sparse != nil {
		t.Errorf("toIndexModel produced unexpected options: %+v", fModel.Options)
	}
}

// 2. TestIndexCatalog_RegisteredInRepository: Verify EnsureIndexes on mock DB creates specs
// for dataSync, transactions_chase, and positions_fidelity in deterministic order.
func TestIndexCatalog_RegisteredInRepository(t *testing.T) {
	fakeDB := newFakeDatabase()
	repo := newRepository(fakeDB)

	if err := repo.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes failed: %v", err)
	}

	wantOrder := []string{dataSyncCollection, chaseCollection, fidelityCollection}
	if !reflect.DeepEqual(fakeDB.order, wantOrder) {
		t.Fatalf("expected index creation order %v, got %v", wantOrder, fakeDB.order)
	}

	dataSyncModels := fakeDB.collectionColl(dataSyncCollection).createdIndex
	if len(dataSyncModels) != 1 || len(dataSyncModels[0]) != 2 {
		t.Errorf("expected 1 call with 2 models for dataSync, got %+v", dataSyncModels)
	}

	chaseModels := fakeDB.collectionColl(chaseCollection).createdIndex
	if len(chaseModels) != 1 || len(chaseModels[0]) != 2 {
		t.Errorf("expected 1 call with 2 models for chase, got %+v", chaseModels)
	}

	fidModels := fakeDB.collectionColl(fidelityCollection).createdIndex
	if len(fidModels) != 1 || len(fidModels[0]) != 1 {
		t.Errorf("expected 1 call with 1 model for fidelity, got %+v", fidModels)
	}

	if fakeDB.collections[syntheticCollection] != nil {
		t.Errorf("transactions_synthetic collection must never have indexes created")
	}
}

func assertChaseFilterValues(t *testing.T, filter bson.D, now time.Time, wantAmount float64) {
	t.Helper()
	wantKeys := []string{"accountId", "postingDate", "description", "amount", "source.rowNumber"}
	if gotKeys := extractDocKeys(filter); !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("filter keys mismatch: got %v, want %v", gotKeys, wantKeys)
	}

	if val, ok := findDocValue(filter, "accountId"); !ok || val != "act-1234" {
		t.Errorf("accountId mismatch: %v", val)
	}
	if val, ok := findDocValue(filter, "postingDate"); !ok || !val.(time.Time).Equal(now) {
		t.Errorf("postingDate mismatch: %v", val)
	}
	if val, ok := findDocValue(filter, "description"); !ok || val != "CHASE GROCERY" {
		t.Errorf("description mismatch: %v", val)
	}
	if val, ok := findDocValue(filter, "amount"); !ok || val != wantAmount {
		t.Errorf("amount mismatch: got %v, want %v", val, wantAmount)
	}
	if val, ok := findDocValue(filter, "source.rowNumber"); !ok || val != int64(7) {
		t.Errorf("source.rowNumber mismatch (must be int64): %v (type %T)", val, val)
	}
}

// 3. TestUpsertFilter_ChaseLegacyParity: Golden check against legacy Chase upsert filter with table-driven amounts.
func TestUpsertFilter_ChaseLegacyParity(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		amount     float64
		wantAmount float64
	}{
		{name: "negative debit amount", amount: -42.50, wantAmount: -42.50},
		{name: "zero fee amount", amount: 0.0, wantAmount: 0.0},
		{name: "positive credit amount", amount: 1250.75, wantAmount: 1250.75},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := newTestChaseTx("act-1234", 7, false)
			tx.PostingDate = now
			tx.Description = "CHASE GROCERY"
			tx.Amount = tt.amount

			d := toChaseDoc(tx, dataSourceChase)
			assertChaseFilterValues(t, chaseFilter(d), now, tt.wantAmount)
		})
	}
}

// 4. TestUpsertFilter_FidelityLegacyParity: Golden check against legacy Fidelity upsert filter.
func TestUpsertFilter_FidelityLegacyParity(t *testing.T) {
	asOf := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	pos := newTestFidelityPos("fid-9876", "AAPL", asOf)

	d := toFidelityDoc(pos)
	filter := fidelityFilter(d)

	wantKeys := []string{"accountNumber", "symbol", "asOfDate"}
	if gotKeys := extractDocKeys(filter); !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("filter keys mismatch: got %v, want %v", gotKeys, wantKeys)
	}

	if val, ok := findDocValue(filter, "accountNumber"); !ok || val != "fid-9876" {
		t.Errorf("accountNumber mismatch: %v", val)
	}
	if val, ok := findDocValue(filter, "symbol"); !ok || val != "AAPL" {
		t.Errorf("symbol mismatch: %v", val)
	}
	if val, ok := findDocValue(filter, "asOfDate"); !ok || !val.(time.Time).Equal(asOf) {
		t.Errorf("asOfDate mismatch: %v", val)
	}
}

// 5. TestUpsertFilter_EqualsUniqueIndexKeys: Invariant test verifying filter key sets equal unique index key sets.
func TestUpsertFilter_EqualsUniqueIndexKeys(t *testing.T) {
	// Chase: verify filter keys equal unique compound index keys in order
	var chaseUniqueKeys []string
	for _, spec := range chaseIndexes() {
		if spec.unique {
			chaseUniqueKeys = extractDocKeys(spec.keys)
			break
		}
	}
	chaseFiltKeys := extractDocKeys(chaseFilter(toChaseDoc(newTestChaseTx("a", 1, false), dataSourceChase)))
	if !reflect.DeepEqual(chaseFiltKeys, chaseUniqueKeys) {
		t.Errorf("chase filter keys %v do not equal unique index keys %v", chaseFiltKeys, chaseUniqueKeys)
	}

	// Fidelity: verify filter keys equal unique compound index keys in order
	var fidUniqueKeys []string
	for _, spec := range fidelityIndexes() {
		if spec.unique {
			fidUniqueKeys = extractDocKeys(spec.keys)
			break
		}
	}
	asOf := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	fidFiltKeys := extractDocKeys(fidelityFilter(toFidelityDoc(newTestFidelityPos("a", "s", asOf))))
	if !reflect.DeepEqual(fidFiltKeys, fidUniqueKeys) {
		t.Errorf("fidelity filter keys %v do not equal unique index keys %v", fidFiltKeys, fidUniqueKeys)
	}
}

// 6. TestSetDocument_FlattensSourceLineage: Verify $set contains dotted paths and no top-level source subdocument.
func TestSetDocument_FlattensSourceLineage(t *testing.T) {
	tx := newTestChaseTx("act-1", 42, false)
	doc := toChaseDoc(tx, dataSourceChase)

	u, err := setDocument(doc)
	if err != nil {
		t.Fatalf("setDocument failed: %v", err)
	}

	if len(u) != 1 || u[0].Key != "$set" {
		t.Fatalf("expected root $set wrapper, got %+v", u)
	}

	setDoc, ok := u[0].Value.(bson.D)
	if !ok {
		t.Fatalf("expected $set value to be bson.D, got %T", u[0].Value)
	}

	setKeys := extractDocKeys(setDoc)
	for _, k := range setKeys {
		if k == "source" {
			t.Errorf("found forbidden top-level 'source' key in $set; lineage must be flattened")
		}
	}

	wantDotted := []string{"source.fileName", "source.rowNumber", "source.ingestedAt", "source.sha256"}
	for _, want := range wantDotted {
		if _, found := findDocValue(setDoc, want); !found {
			t.Errorf("missing expected dotted lineage key %q in $set", want)
		}
	}
}

// TestFlattenSourceLineage_EdgeCases verifies flattenSourceLineage and appendDottedLineage behavior
// across document boundaries, empty subdocuments, and malformed/non-bson.D values.
func TestFlattenSourceLineage_EdgeCases(t *testing.T) {
	t.Run("DocumentWithoutSource", func(t *testing.T) {
		raw := bson.D{{Key: "accountId", Value: "act-1"}, {Key: "amount", Value: 100.5}}
		got := flattenSourceLineage(raw)
		if !reflect.DeepEqual(got, raw) {
			t.Errorf("expected unchanged document, got %+v", got)
		}
	})

	t.Run("EmptySourceSubdocument", func(t *testing.T) {
		raw := bson.D{
			{Key: "accountId", Value: "act-1"},
			{Key: "source", Value: bson.D{}},
		}
		got := flattenSourceLineage(raw)
		want := bson.D{{Key: "accountId", Value: "act-1"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected empty source to be omitted, got %+v", got)
		}
	})

	t.Run("MalformedSourceNonBsonD", func(t *testing.T) {
		raw := bson.D{
			{Key: "accountId", Value: "act-1"},
			{Key: "source", Value: "not-a-subdoc"},
		}
		got := flattenSourceLineage(raw)
		want := bson.D{{Key: "accountId", Value: "act-1"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected non-bson.D source to be safely ignored, got %+v", got)
		}
	})

	t.Run("NilSourceValue", func(t *testing.T) {
		raw := bson.D{
			{Key: "accountId", Value: "act-1"},
			{Key: "source", Value: nil},
		}
		got := flattenSourceLineage(raw)
		want := bson.D{{Key: "accountId", Value: "act-1"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected nil source to be safely ignored, got %+v", got)
		}
	})
}

// 7. TestSetDocument_OmitsS3URIPlaceholder: Verify $set excludes source.s3Uri when empty (E6).
func TestSetDocument_OmitsS3URIPlaceholder(t *testing.T) {
	tx := newTestChaseTx("act-1", 1, false)
	doc := toChaseDoc(tx, dataSourceChase)

	u, err := setDocument(doc)
	if err != nil {
		t.Fatalf("setDocument failed: %v", err)
	}

	setDoc := u[0].Value.(bson.D)
	for _, elem := range setDoc {
		if elem.Key == "source.s3Uri" || elem.Key == "s3Uri" {
			t.Errorf("forbidden s3Uri key %q found in $set (must be omitted when empty)", elem.Key)
		}
	}
}

// 8. TestSetDocument_IncludesRestoredA1Fields: Verify 2E fields (even zero values) and 2F transactionDate appear in $set.
func TestSetDocument_IncludesRestoredA1Fields(t *testing.T) {
	// Fidelity: verify all 8 2E fields appear even when zero/empty
	asOf := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	pos := model.FidelityPosition{
		AccountID: "fid-zero",
		Symbol:    "ZERO",
		AsOfDate:  asOf,
		Lineage: model.SourceLineage{
			FileName:  "pos.csv",
			RowNumber: 1,
		},
	}
	fDoc := toFidelityDoc(pos)
	uFid, err := setDocument(fDoc)
	if err != nil {
		t.Fatalf("setDocument for fidelity failed: %v", err)
	}

	fidSetDoc := uFid[0].Value.(bson.D)
	required2EFields := []string{
		"lastPriceChange",
		"todaysGainLossDollar",
		"todaysGainLossPct",
		"totalGainLossDollar",
		"totalGainLossPct",
		"percentOfAccount",
		"averageCostBasis",
		"type",
	}
	for _, field := range required2EFields {
		if _, ok := findDocValue(fidSetDoc, field); !ok {
			t.Errorf("missing restored 2E field %q in Fidelity $set", field)
		}
	}

	// Chase credit card: transactionDate must appear
	ccTx := newTestChaseTx("cc-1", 1, true)
	ccDoc := toChaseDoc(ccTx, dataSourceChase)
	uCC, err := setDocument(ccDoc)
	if err != nil {
		t.Fatalf("setDocument for credit card failed: %v", err)
	}
	ccSetDoc := uCC[0].Value.(bson.D)
	if _, ok := findDocValue(ccSetDoc, "transactionDate"); !ok {
		t.Errorf("expected transactionDate in credit card $set")
	}

	// Chase checking: transactionDate must NOT appear (omitted when nil)
	chkTx := newTestChaseTx("chk-1", 1, false)
	chkDoc := toChaseDoc(chkTx, dataSourceChase)
	uChk, err := setDocument(chkDoc)
	if err != nil {
		t.Fatalf("setDocument for checking failed: %v", err)
	}
	chkSetDoc := uChk[0].Value.(bson.D)
	if _, ok := findDocValue(chkSetDoc, "transactionDate"); ok {
		t.Errorf("transactionDate must be omitted in checking $set when nil")
	}
}

// 9. TestCollectionFor_AllDestinations: Table-driven test testing all 4 destinations and invalid destination handling.
func TestCollectionFor_AllDestinations(t *testing.T) {
	tests := []struct {
		name       string
		dest       model.Destination
		wantColl   string
		wantErr    error
		isErrCheck bool
	}{
		{
			name:     "Chase Checking",
			dest:     model.DestChaseChecking,
			wantColl: chaseCollection,
			wantErr:  nil,
		},
		{
			name:     "Chase Credit Card",
			dest:     model.DestChaseCreditCard,
			wantColl: chaseCollection,
			wantErr:  nil,
		},
		{
			name:     "Fidelity Position",
			dest:     model.DestFidelityPosition,
			wantColl: fidelityCollection,
			wantErr:  nil,
		},
		{
			name:     "Synthetic",
			dest:     model.DestSynthetic,
			wantColl: syntheticCollection,
			wantErr:  nil,
		},
		{
			name:       "Unknown destination",
			dest:       model.Destination("unknown_dest"),
			wantColl:   "",
			wantErr:    model.ErrInvalidDestination,
			isErrCheck: true,
		},
		{
			name:       "Empty destination",
			dest:       model.Destination(""),
			wantColl:   "",
			wantErr:    model.ErrInvalidDestination,
			isErrCheck: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coll, err := collectionFor(tt.dest)
			if tt.isErrCheck {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("collectionFor(%q) error = %v; want %v", tt.dest, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.dest, err)
			}
			if coll != tt.wantColl {
				t.Errorf("collectionFor(%q) = %q; want %q", tt.dest, coll, tt.wantColl)
			}
		})
	}
}

// 10. TestBuildWritePlan_PayloadMismatch: Verify Fidelity destination with Chase payload yields model.ErrPayloadTypeMismatch.
func TestBuildWritePlan_PayloadMismatch(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

	// Fidelity destination with Chase payload
	batchFid := &model.IngestionBatch{
		Destination: model.DestFidelityPosition,
		AccountID:   "fid-1",
		SourceFile:  "fidelity.csv",
		IngestedAt:  now,
		RecordCount: 1,
		Payload:     []model.ChaseTransaction{newTestChaseTx("chk-1", 1, false)},
	}
	_, err := buildWritePlan(batchFid)
	if !errors.Is(err, model.ErrPayloadTypeMismatch) {
		t.Errorf("expected ErrPayloadTypeMismatch, got %v", err)
	}

	// Chase destination with Fidelity payload
	batchChase := &model.IngestionBatch{
		Destination: model.DestChaseChecking,
		AccountID:   "chk-1",
		SourceFile:  "chase.csv",
		IngestedAt:  now,
		RecordCount: 1,
		Payload:     []model.FidelityPosition{newTestFidelityPos("fid-1", "VTI", now)},
	}
	_, err = buildWritePlan(batchChase)
	if !errors.Is(err, model.ErrPayloadTypeMismatch) {
		t.Errorf("expected ErrPayloadTypeMismatch, got %v", err)
	}
}

// 11. TestBuildWritePlan_SyntheticUsesChaseShape: Verify synthetic write plan routes to transactions_synthetic
// with Chase filter and dataSource: "synthetic".
func TestBuildWritePlan_SyntheticUsesChaseShape(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	tx := newTestChaseTx("syn-1", 3, false)
	batch := &model.IngestionBatch{
		Destination: model.DestSynthetic,
		AccountID:   "syn-1",
		SourceFile:  "synthetic.csv",
		IngestedAt:  now,
		RecordCount: 1,
		Payload:     []model.ChaseTransaction{tx},
	}

	plan, err := buildWritePlan(batch)
	if err != nil {
		t.Fatalf("buildWritePlan failed: %v", err)
	}

	if plan.collection != syntheticCollection {
		t.Errorf("expected collection %q, got %q", syntheticCollection, plan.collection)
	}
	if len(plan.models) != 1 {
		t.Fatalf("expected 1 write model, got %d", len(plan.models))
	}

	updateModel, ok := plan.models[0].(*mongodrv.UpdateOneModel)
	if !ok {
		t.Fatalf("expected *UpdateOneModel, got %T", plan.models[0])
	}

	// Filter matches Chase 5 fields
	filt, ok := updateModel.Filter.(bson.D)
	if !ok {
		t.Fatalf("expected Filter to be bson.D, got %T", updateModel.Filter)
	}
	wantKeys := []string{"accountId", "postingDate", "description", "amount", "source.rowNumber"}
	if gotKeys := extractDocKeys(filt); !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Errorf("synthetic filter keys mismatch: got %v, want %v", gotKeys, wantKeys)
	}

	// Update contains dataSource: "synthetic"
	upd, ok := updateModel.Update.(bson.D)
	if !ok || len(upd) != 1 || upd[0].Key != "$set" {
		t.Fatalf("invalid update shape: %+v", updateModel.Update)
	}
	setDoc := upd[0].Value.(bson.D)
	val, ok := findDocValue(setDoc, "dataSource")
	if !ok || val != dataSourceSynthetic {
		t.Errorf("expected dataSource to be %q, got %v", dataSourceSynthetic, val)
	}
}

func assertValidUpdateModels(t *testing.T, models []mongodrv.WriteModel, wantCount int) {
	t.Helper()
	if len(models) != wantCount {
		t.Fatalf("expected %d models, got %d", wantCount, len(models))
	}
	for i, m := range models {
		um, ok := m.(*mongodrv.UpdateOneModel)
		if !ok {
			t.Fatalf("model %d is not *UpdateOneModel: %T", i, m)
		}
		if um.Upsert == nil || !*um.Upsert {
			t.Errorf("model %d Upsert must be true", i)
		}
		if um.Filter == nil {
			t.Errorf("model %d Filter must not be nil", i)
		}
		if um.Update == nil {
			t.Errorf("model %d Update must not be nil", i)
		}
	}
}

func assertChaseCheckingUpdateModel(t *testing.T, m *mongodrv.UpdateOneModel, wantUTC time.Time) {
	t.Helper()
	filt := m.Filter.(bson.D)
	if val, ok := findDocValue(filt, "accountId"); !ok || val != "chk-1" {
		t.Errorf("chk filter accountId mismatch: %v", val)
	}
	if val, ok := findDocValue(filt, "postingDate"); !ok || !val.(time.Time).Equal(wantUTC) || val.(time.Time).Location() != time.UTC {
		t.Errorf("chk filter postingDate mismatch or non-UTC: %v", val)
	}
	if val, ok := findDocValue(filt, "source.rowNumber"); !ok || val != int64(1) {
		t.Errorf("chk filter rowNumber mismatch: %v", val)
	}

	upd := m.Update.(bson.D)[0].Value.(bson.D)
	if val, ok := findDocValue(upd, "amount"); !ok || val != -42.50 {
		t.Errorf("chk update amount mismatch: %v", val)
	}
	if val, ok := findDocValue(upd, "dataSource"); !ok || val != "chase" {
		t.Errorf("chk update dataSource mismatch: %v", val)
	}
	if _, ok := findDocValue(upd, "transactionDate"); ok {
		t.Errorf("chk update transactionDate must be omitted for checking")
	}
}

func assertChaseCreditCardUpdateModel(t *testing.T, m *mongodrv.UpdateOneModel, txDateEST time.Time) {
	t.Helper()
	filt := m.Filter.(bson.D)
	if val, ok := findDocValue(filt, "accountId"); !ok || val != "cc-1" {
		t.Errorf("cc filter accountId mismatch: %v", val)
	}
	if val, ok := findDocValue(filt, "source.rowNumber"); !ok || val != int64(10) {
		t.Errorf("cc filter rowNumber mismatch: %v", val)
	}

	upd := m.Update.(bson.D)[0].Value.(bson.D)
	val, ok := findDocValue(upd, "transactionDate")
	if !ok {
		t.Fatalf("cc update missing transactionDate")
	}
	pdt, isDT := val.(primitive.DateTime)
	if !isDT {
		t.Fatalf("expected transactionDate to be primitive.DateTime, got %T", val)
	}
	if !pdt.Time().UTC().Equal(txDateEST.UTC()) {
		t.Errorf("cc update transactionDate mismatch: %v (want %v)", pdt.Time().UTC(), txDateEST.UTC())
	}
}

func assertFidelityUpdateModel(t *testing.T, m *mongodrv.UpdateOneModel, wantUTC time.Time) {
	t.Helper()
	filt := m.Filter.(bson.D)
	if val, ok := findDocValue(filt, "accountNumber"); !ok || val != "fid-1" {
		t.Errorf("fid filter accountNumber mismatch: %v", val)
	}
	if val, ok := findDocValue(filt, "symbol"); !ok || val != "AAPL" {
		t.Errorf("fid filter symbol mismatch: %v", val)
	}
	if val, ok := findDocValue(filt, "asOfDate"); !ok || !val.(time.Time).Equal(wantUTC) || val.(time.Time).Location() != time.UTC {
		t.Errorf("fid filter asOfDate mismatch or non-UTC: %v", val)
	}

	upd := m.Update.(bson.D)[0].Value.(bson.D)
	if val, ok := findDocValue(upd, "quantity"); !ok || val != 10.5 {
		t.Errorf("fid update quantity mismatch: %v", val)
	}
	if val, ok := findDocValue(upd, "dataSource"); !ok || val != "fidelity" {
		t.Errorf("fid update dataSource mismatch: %v", val)
	}
}

// 12. TestBuildWritePlan_UpsertModels: Verify UpdateOneModel properties (upsert=true, filter, $set)
// with value-level assertions and timezone normalization across Chase checking, credit card, and Fidelity.
func TestBuildWritePlan_UpsertModels(t *testing.T) {
	estZone := time.FixedZone("EST", -5*3600)
	nowEST := time.Date(2026, 10, 20, 7, 0, 0, 0, estZone)
	wantUTC := nowEST.UTC()

	t.Run("ChaseChecking", func(t *testing.T) {
		txChk1 := newTestChaseTx("chk-1", 1, false)
		txChk1.PostingDate = nowEST
		txChk2 := newTestChaseTx("chk-1", 2, false)
		txChk2.PostingDate = nowEST

		batchChase := &model.IngestionBatch{
			Destination: model.DestChaseChecking,
			AccountID:   "chk-1",
			SourceFile:  "chase.csv",
			IngestedAt:  nowEST,
			RecordCount: 2,
			Payload:     []model.ChaseTransaction{txChk1, txChk2},
		}

		planChase, err := buildWritePlan(batchChase)
		if err != nil {
			t.Fatalf("buildWritePlan for chase checking failed: %v", err)
		}
		assertValidUpdateModels(t, planChase.models, 2)
		assertChaseCheckingUpdateModel(t, planChase.models[0].(*mongodrv.UpdateOneModel), wantUTC)
	})

	t.Run("ChaseCreditCard", func(t *testing.T) {
		txCC := newTestChaseTx("cc-1", 10, true)
		txCC.PostingDate = nowEST
		txDateEST := nowEST.Add(-48 * time.Hour)
		txCC.TransactionDate = &txDateEST

		batchCC := &model.IngestionBatch{
			Destination: model.DestChaseCreditCard,
			AccountID:   "cc-1",
			SourceFile:  "credit.csv",
			IngestedAt:  nowEST,
			RecordCount: 1,
			Payload:     []model.ChaseTransaction{txCC},
		}

		planCC, err := buildWritePlan(batchCC)
		if err != nil {
			t.Fatalf("buildWritePlan for chase credit card failed: %v", err)
		}
		assertValidUpdateModels(t, planCC.models, 1)
		assertChaseCreditCardUpdateModel(t, planCC.models[0].(*mongodrv.UpdateOneModel), txDateEST)
	})

	t.Run("FidelityPosition", func(t *testing.T) {
		pos1 := newTestFidelityPos("fid-1", "AAPL", nowEST)
		pos2 := newTestFidelityPos("fid-1", "MSFT", nowEST)
		batchFid := &model.IngestionBatch{
			Destination: model.DestFidelityPosition,
			AccountID:   "fid-1",
			SourceFile:  "fidelity.csv",
			IngestedAt:  nowEST,
			RecordCount: 2,
			Payload:     []model.FidelityPosition{pos1, pos2},
		}

		planFid, err := buildWritePlan(batchFid)
		if err != nil {
			t.Fatalf("buildWritePlan for fidelity failed: %v", err)
		}
		assertValidUpdateModels(t, planFid.models, 2)
		assertFidelityUpdateModel(t, planFid.models[0].(*mongodrv.UpdateOneModel), wantUTC)
	})
}

// 13. TestBuildWritePlan_FidelityMissingAsOfDate: Verify zero AsOfDate yields model.ErrMissingAsOfDate.
func TestBuildWritePlan_FidelityMissingAsOfDate(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	posZero := newTestFidelityPos("fid-1", "AAPL", time.Time{}) // zero AsOfDate
	batch := &model.IngestionBatch{
		Destination: model.DestFidelityPosition,
		AccountID:   "fid-1",
		SourceFile:  "fidelity.csv",
		IngestedAt:  now,
		RecordCount: 1,
		Payload:     []model.FidelityPosition{posZero},
	}

	_, err := buildWritePlan(batch)
	if !errors.Is(err, model.ErrMissingAsOfDate) {
		t.Fatalf("expected ErrMissingAsOfDate, got %v", err)
	}
}

// 14. TestBuildWritePlan_EmptyPayload: Verify empty batch yields 0 models, nil batch yields ErrNilBatch,
// negative count yields ErrNegativeRecordCount, and invalid destination yields ErrInvalidDestination.
func TestBuildWritePlan_EmptyPayload(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	destinations := []struct {
		dest     model.Destination
		wantColl string
	}{
		{model.DestChaseChecking, chaseCollection},
		{model.DestChaseCreditCard, chaseCollection},
		{model.DestFidelityPosition, fidelityCollection},
		{model.DestSynthetic, syntheticCollection},
	}

	for _, d := range destinations {
		t.Run(string(d.dest), func(t *testing.T) {
			batch := &model.IngestionBatch{
				Destination: d.dest,
				AccountID:   "act-empty",
				SourceFile:  "empty.csv",
				IngestedAt:  now,
				RecordCount: 0,
				Payload:     nil,
			}

			plan, err := buildWritePlan(batch)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.collection != d.wantColl {
				t.Errorf("expected collection %q, got %q", d.wantColl, plan.collection)
			}
			if len(plan.models) != 0 {
				t.Errorf("expected 0 models, got %d", len(plan.models))
			}
		})
	}

	// Verify nil batch returns ErrNilBatch
	_, err := buildWritePlan(nil)
	if !errors.Is(err, model.ErrNilBatch) {
		t.Errorf("expected ErrNilBatch for nil batch, got %v", err)
	}

	// Verify negative RecordCount returns ErrNegativeRecordCount
	negBatch := &model.IngestionBatch{
		Destination: model.DestChaseChecking,
		AccountID:   "act-neg",
		SourceFile:  "neg.csv",
		IngestedAt:  now,
		RecordCount: -1,
	}
	_, err = buildWritePlan(negBatch)
	if !errors.Is(err, model.ErrNegativeRecordCount) {
		t.Errorf("expected ErrNegativeRecordCount for negative RecordCount, got %v", err)
	}

	// Verify invalid destination returns ErrInvalidDestination
	badBatch := &model.IngestionBatch{Destination: "bad_destination"}
	_, err = buildWritePlan(badBatch)
	if !errors.Is(err, model.ErrInvalidDestination) {
		t.Errorf("expected ErrInvalidDestination, got %v", err)
	}
}

// 15. TestBuildWritePlan_DuplicateKeysPreservedInBatch: Verifies that batches containing duplicate
// unique keys emit both write models without in-memory deduplication, leaving upsert semantics to MongoDB.
func TestBuildWritePlan_DuplicateKeysPreservedInBatch(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	tx1 := newTestChaseTx("chk-dup", 1, false)
	tx2 := newTestChaseTx("chk-dup", 1, false) // identical key fields

	batch := &model.IngestionBatch{
		Destination: model.DestChaseChecking,
		AccountID:   "chk-dup",
		SourceFile:  "dup.csv",
		IngestedAt:  now,
		RecordCount: 2,
		Payload:     []model.ChaseTransaction{tx1, tx2},
	}

	plan, err := buildWritePlan(batch)
	if err != nil {
		t.Fatalf("buildWritePlan failed: %v", err)
	}
	assertValidUpdateModels(t, plan.models, 2)

	m1 := plan.models[0].(*mongodrv.UpdateOneModel)
	m2 := plan.models[1].(*mongodrv.UpdateOneModel)

	filt1 := m1.Filter.(bson.D)
	filt2 := m2.Filter.(bson.D)

	if !reflect.DeepEqual(filt1, filt2) {
		t.Errorf("expected identical filters for duplicate input records, got %+v and %+v", filt1, filt2)
	}
}
