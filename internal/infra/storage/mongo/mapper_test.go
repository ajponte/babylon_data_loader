package mongo

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type bsonFieldExpectation struct {
	key string
	typ bsontype.Type
}

// goldenChaseCCFields transcribed from legacy datalake/model/chase_transaction.go.
func goldenChaseCCFields() []bsonFieldExpectation {
	return []bsonFieldExpectation{
		{"accountId", bson.TypeString},
		{"postingDate", bson.TypeDateTime},
		{"transactionDate", bson.TypeDateTime},
		{"description", bson.TypeString},
		{"amount", bson.TypeDouble},
		{"transactionType", bson.TypeString},
		{"balance", bson.TypeDouble},
		{"category", bson.TypeString},
		{"details", bson.TypeString},
		{"checkOrSlipNum", bson.TypeString},
		{"memo", bson.TypeString},
		{"dataSource", bson.TypeString},
		{"source", bson.TypeEmbeddedDocument},
	}
}

// goldenChaseCheckingFields transcribed from legacy datalake/model/chase_transaction.go.
// It matches Chase CC fields with transactionDate omitted.
func goldenChaseCheckingFields() []bsonFieldExpectation {
	cc := goldenChaseCCFields()
	return append(cc[:2:2], cc[3:]...)
}

// goldenSourceFields transcribed from legacy datalake/model/fidelity_position.go and datalake/lineage.go.
func goldenSourceFields() []bsonFieldExpectation {
	return []bsonFieldExpectation{
		{"fileName", bson.TypeString},
		{"rowNumber", bson.TypeInt64},
		{"ingestedAt", bson.TypeDateTime},
		{"sha256", bson.TypeString},
	}
}

// goldenFidelityFields transcribed from legacy datalake/model/fidelity_position.go.
func goldenFidelityFields() []bsonFieldExpectation {
	return []bsonFieldExpectation{
		{"accountNumber", bson.TypeString},
		{"accountName", bson.TypeString},
		{"symbol", bson.TypeString},
		{"description", bson.TypeString},
		{"quantity", bson.TypeDouble},
		{"lastPrice", bson.TypeDouble},
		{"lastPriceChange", bson.TypeDouble},
		{"currentValue", bson.TypeDouble},
		{"todaysGainLossDollar", bson.TypeDouble},
		{"todaysGainLossPct", bson.TypeDouble},
		{"totalGainLossDollar", bson.TypeDouble},
		{"totalGainLossPct", bson.TypeDouble},
		{"percentOfAccount", bson.TypeDouble},
		{"costBasisTotal", bson.TypeDouble},
		{"averageCostBasis", bson.TypeDouble},
		{"type", bson.TypeString},
		{"asOfDate", bson.TypeDateTime},
		{"dataSource", bson.TypeString},
		{"source", bson.TypeEmbeddedDocument},
	}
}

func newValidSourceLineage() model.SourceLineage {
	return model.SourceLineage{
		FileName: "Chase1234_Activity_20261015.CSV", FileHash: "a1b2c3d4e5f67890",
		RowNumber: 42, IngestedAt: time.Date(2026, 10, 15, 12, 30, 0, 0, time.UTC),
	}
}

func newValidChaseCC() model.ChaseTransaction {
	txDate := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	bal := 1234.56
	return model.ChaseTransaction{
		Lineage: newValidSourceLineage(), AccountID: "chase-cc-1234",
		PostingDate: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC),
		Description: "GROCERY STORE", Details: "DEBIT", Amount: -85.25, Type: "Sale",
		Balance: &bal, CheckOrSlipNum: "CHK-99", Category: "Groceries", Memo: "Weekly groceries",
		TransactionDate: &txDate,
	}
}

func newValidChaseChecking() model.ChaseTransaction {
	bal := 5000.00
	return model.ChaseTransaction{
		Lineage: newValidSourceLineage(), AccountID: "chase-chk-5678",
		PostingDate: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC),
		Description: "PAYROLL DIRECT DEPOSIT", Details: "CREDIT", Amount: 3000.00,
		Type: "Direct Deposit", Balance: &bal, CheckOrSlipNum: "DIR-DEP",
		Category: "Income", Memo: "Bi-weekly paycheck",
	}
}

func newValidFidelityPosition() model.FidelityPosition {
	return model.FidelityPosition{
		Lineage: newValidSourceLineage(), AccountID: "fidelity-acc-9999", AccountName: "Individual Brokerage",
		Symbol: "AAPL", Description: "APPLE INC COM", Quantity: 100.0, LastPrice: 180.50, LastPriceChange: 2.50,
		CurrentValue: 18050.0, TodaysGainLossDollar: 250.0, TodaysGainLossPct: 1.40, TotalGainLossDollar: 3050.0,
		TotalGainLossPct: 20.33, PercentOfAccount: 15.25, CostBasis: 15000.0, AverageCostBasis: 150.0, Type: "Cash",
		AsOfDate: time.Date(2026, 10, 15, 20, 0, 0, 0, time.UTC),
	}
}

func mustMarshal(t *testing.T, doc any) []byte {
	t.Helper()
	b, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal failed: %v", err)
	}
	return b
}

func assertElements(t *testing.T, elems []bson.RawElement, expected []bsonFieldExpectation) {
	t.Helper()
	if len(elems) != len(expected) {
		t.Fatalf("element count mismatch: got %d, want %d", len(elems), len(expected))
	}
	for i, exp := range expected {
		if got := elems[i].Key(); got != exp.key {
			t.Errorf("field [%d] key: got %q, want %q", i, got, exp.key)
		}
		if got := elems[i].Value().Type; got != exp.typ {
			t.Errorf("field [%d] (%s) type: got %s, want %s", i, exp.key, got, exp.typ)
		}
	}
}

func assertDocFields(t *testing.T, raw []byte, expected []bsonFieldExpectation) {
	t.Helper()
	elems, err := bson.Raw(raw).Elements()
	if err != nil {
		t.Fatalf("failed to parse raw elements: %v", err)
	}
	assertElements(t, elems, expected)
}

func assertSourceFields(t *testing.T, raw []byte, expected []bsonFieldExpectation) {
	t.Helper()
	val, err := bson.Raw(raw).LookupErr("source")
	if err != nil {
		t.Fatalf("failed to lookup 'source': %v", err)
	}
	if val.Type != bson.TypeEmbeddedDocument {
		t.Fatalf("expected 'source' to be EmbeddedDocument, got %s", val.Type)
	}
	elems, err := val.Document().Elements()
	if err != nil {
		t.Fatalf("failed to parse source elements: %v", err)
	}
	assertElements(t, elems, expected)
}

func collectKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	elems, err := bson.Raw(raw).Elements()
	if err != nil {
		t.Fatalf("failed to parse raw elements: %v", err)
	}
	keys := make([]string, len(elems))
	for i, elem := range elems {
		keys[i] = elem.Key()
	}
	return keys
}

// 1. TestChaseDoc_LegacyBSONParity asserts full chaseDoc parity against golden legacy literals.
func TestChaseDoc_LegacyBSONParity(t *testing.T) {
	tests := []struct {
		name      string
		tx        model.ChaseTransaction
		wantDoc   []bsonFieldExpectation
		hasTxDate bool
	}{
		{"CreditCard", newValidChaseCC(), goldenChaseCCFields(), true},
		{"Checking", newValidChaseChecking(), goldenChaseCheckingFields(), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustMarshal(t, toChaseDoc(tc.tx, dataSourceChase))
			assertDocFields(t, raw, tc.wantDoc)
			assertSourceFields(t, raw, goldenSourceFields())

			sourceDoc := bson.Raw(raw).Lookup("source").Document()
			rowNum := sourceDoc.Lookup("rowNumber")
			if rowNum.Type != bson.TypeInt64 || rowNum.Int64() != 42 {
				t.Errorf("expected source.rowNumber Int64(42), got %s(%v)", rowNum.Type, rowNum)
			}

			_, lookupErr := bson.Raw(raw).LookupErr("transactionDate")
			if tc.hasTxDate && lookupErr != nil {
				t.Errorf("expected transactionDate to be present")
			} else if !tc.hasTxDate && lookupErr == nil {
				t.Errorf("expected transactionDate to be omitted")
			}
		})
	}
}

// 2. TestFidelityDoc_LegacyBSONParity verifies exact field order, names, and types against legacy table.
func TestFidelityDoc_LegacyBSONParity(t *testing.T) {
	raw := mustMarshal(t, toFidelityDoc(newValidFidelityPosition()))
	assertDocFields(t, raw, goldenFidelityFields())
	assertSourceFields(t, raw, goldenSourceFields())

	rawDoc := bson.Raw(raw)
	if asOf := rawDoc.Lookup("asOfDate"); asOf.Type != bson.TypeDateTime {
		t.Errorf("expected asOfDate DateTime, got %s", asOf.Type)
	}
	if cost := rawDoc.Lookup("costBasisTotal"); cost.Type != bson.TypeDouble {
		t.Errorf("expected costBasisTotal Double, got %s", cost.Type)
	}
}

// 3. TestFidelityDoc_ZeroLegacyFieldsEmitted asserts zero 2E fields (0.0 doubles and "" string) are emitted.
func TestFidelityDoc_ZeroLegacyFieldsEmitted(t *testing.T) {
	pos := newValidFidelityPosition()
	pos.LastPriceChange, pos.TodaysGainLossDollar = 0.0, 0.0
	pos.TodaysGainLossPct, pos.TotalGainLossDollar = 0.0, 0.0
	pos.TotalGainLossPct, pos.PercentOfAccount = 0.0, 0.0
	pos.AverageCostBasis, pos.Type = 0.0, ""

	rawDoc := bson.Raw(mustMarshal(t, toFidelityDoc(pos)))
	zeroDoubleFields := []string{
		"lastPriceChange", "todaysGainLossDollar", "todaysGainLossPct",
		"totalGainLossDollar", "totalGainLossPct", "percentOfAccount", "averageCostBasis",
	}

	for _, field := range zeroDoubleFields {
		val, lookupErr := rawDoc.LookupErr(field)
		if lookupErr != nil {
			t.Errorf("expected field %q present when zero", field)
			continue
		}
		if val.Type != bson.TypeDouble || val.Double() != 0.0 {
			t.Errorf("expected %q Double(0.0), got %s(%f)", field, val.Type, val.Double())
		}
	}

	typeVal, err := rawDoc.LookupErr("type")
	if err != nil || typeVal.Type != bson.TypeString || typeVal.StringValue() != "" {
		t.Errorf("expected type String(\"\"), got %s(%q), err: %v", typeVal.Type, typeVal.StringValue(), err)
	}
}

// 4. TestChaseDoc_TransactionDateUTCAndNotAliased asserts UTC normalization and pointer independence.
func TestChaseDoc_TransactionDateUTCAndNotAliased(t *testing.T) {
	estZone := time.FixedZone("EST", -5*3600)
	txDate := time.Date(2026, 10, 15, 14, 30, 0, 0, estZone)
	originalTxDate := txDate

	tx := newValidChaseCC()
	tx.TransactionDate = &txDate

	doc := toChaseDoc(tx, dataSourceChase)
	if doc.TransactionDate == nil || doc.TransactionDate.Location() != time.UTC || !doc.TransactionDate.Equal(originalTxDate) {
		t.Fatalf("expected UTC instant %v, got %v", originalTxDate, doc.TransactionDate)
	}

	// Mutate input pointer and ensure DTO remains unaffected.
	*tx.TransactionDate = txDate.Add(48 * time.Hour)
	if doc.TransactionDate.Equal(*tx.TransactionDate) || !doc.TransactionDate.Equal(originalTxDate) {
		t.Errorf("DTO TransactionDate was mutated via aliased input pointer")
	}
}

// 5. TestLineageDoc_S3URIPlaceholderOmitted verifies mapped docs omit s3Uri and placeholder tag locks Phase 4 order.
func TestLineageDoc_S3URIPlaceholderOmitted(t *testing.T) {
	t.Run("MappedDocOmitsS3URI", func(t *testing.T) {
		raw := mustMarshal(t, toChaseDoc(newValidChaseCC(), dataSourceChase))
		sourceDoc := bson.Raw(raw).Lookup("source").Document()
		if _, lookupErr := sourceDoc.LookupErr("s3Uri"); lookupErr == nil {
			t.Errorf("expected mapped source.s3Uri to be omitted, but was present")
		}
	})

	t.Run("HandBuiltLocksFirstPosition", func(t *testing.T) {
		placeholder := lineageDoc{
			S3URI:      "s3://example-bucket/chase/activity.csv",
			FileName:   "activity.csv",
			RowNumber:  10,
			IngestedAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC),
			SHA256:     "samplehash",
		}
		elems, err := bson.Raw(mustMarshal(t, placeholder)).Elements()
		if err != nil || len(elems) == 0 {
			t.Fatalf("failed parsing elements, len=%d, err=%v", len(elems), err)
		}
		if elems[0].Key() != "s3Uri" || elems[0].Value().Type != bson.TypeString || elems[0].Value().StringValue() != placeholder.S3URI {
			t.Errorf("expected first element s3Uri string %q, got %q", placeholder.S3URI, elems[0].Key())
		}
	})
}

// 6. TestSyntheticDoc_UsesChaseShape asserts synthetic mapping adheres to Chase shape and dataSource is synthetic.
func TestSyntheticDoc_UsesChaseShape(t *testing.T) {
	raw := mustMarshal(t, toChaseDoc(newValidChaseChecking(), dataSourceSynthetic))
	rawDoc := bson.Raw(raw)

	dsVal, err := rawDoc.LookupErr("dataSource")
	if err != nil || dsVal.StringValue() != "synthetic" {
		t.Fatalf("expected dataSource 'synthetic', got %q, err: %v", dsVal.StringValue(), err)
	}

	legacyPascalCaseKeys := []string{"Details", "PostingDate", "Description", "Amount", "AccountID", "TransactionType"}
	for _, key := range legacyPascalCaseKeys {
		if _, lookupErr := rawDoc.LookupErr(key); lookupErr == nil {
			t.Errorf("legacy PascalCase key %q should not be present in synthetic doc", key)
		}
	}

	assertDocFields(t, raw, goldenChaseCheckingFields())
}

// 7. TestChaseDoc_OmitEmptyParity asserts nil and empty optional fields are omitted from marshaled BSON.
func TestChaseDoc_OmitEmptyParity(t *testing.T) {
	minimalTx := model.ChaseTransaction{
		Lineage: model.SourceLineage{
			FileName:   "minimal.csv",
			RowNumber:  1,
			IngestedAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC),
		},
		AccountID:   "min-acc",
		PostingDate: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC),
		Description: "Minimal Tx",
		Amount:      10.50,
		Type:        "DEBIT",
	}

	raw := mustMarshal(t, toChaseDoc(minimalTx, dataSourceChase))
	rawDoc := bson.Raw(raw)

	omittedKeys := []string{"balance", "transactionDate", "category", "details", "checkOrSlipNum", "memo"}
	for _, key := range omittedKeys {
		if _, lookupErr := rawDoc.LookupErr(key); lookupErr == nil {
			t.Errorf("expected optional key %q omitted when empty/nil", key)
		}
	}

	sourceDoc := rawDoc.Lookup("source").Document()
	if _, lookupErr := sourceDoc.LookupErr("sha256"); lookupErr == nil {
		t.Errorf("expected source.sha256 omitted when empty")
	}
	if _, lookupErr := sourceDoc.LookupErr("s3Uri"); lookupErr == nil {
		t.Errorf("expected source.s3Uri omitted")
	}

	requiredKeys := []string{"accountId", "postingDate", "description", "amount", "transactionType", "dataSource", "source"}
	for _, key := range requiredKeys {
		if _, lookupErr := rawDoc.LookupErr(key); lookupErr != nil {
			t.Errorf("expected required key %q present: %v", key, lookupErr)
		}
	}
}

// 8. TestDocs_KeySetEqualsLegacyStruct checks mapped key sets equal legacy struct tags.
func TestDocs_KeySetEqualsLegacyStruct(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want []bsonFieldExpectation
	}{
		{"ChaseCreditCard", mustMarshal(t, toChaseDoc(newValidChaseCC(), dataSourceChase)), goldenChaseCCFields()},
		{"ChaseChecking", mustMarshal(t, toChaseDoc(newValidChaseChecking(), dataSourceChase)), goldenChaseCheckingFields()},
		{"Fidelity", mustMarshal(t, toFidelityDoc(newValidFidelityPosition())), goldenFidelityFields()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectKeys(t, tc.raw)
			want := make([]string, len(tc.want))
			for i, exp := range tc.want {
				want[i] = exp.key
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("keys mismatch:\ngot:  %v\nwant: %v", got, want)
			}
		})
	}
}

// 9. TestDocs_TimesNormalizedToUTC verifies non-UTC timestamps are converted to UTC instants.
func TestDocs_TimesNormalizedToUTC(t *testing.T) {
	jst := time.FixedZone("JST", 9*3600)
	cst := time.FixedZone("CST", -6*3600)
	postingTime := time.Date(2026, 10, 15, 9, 0, 0, 0, jst)
	txTime := time.Date(2026, 10, 14, 18, 0, 0, 0, cst)
	ingestedTime := time.Date(2026, 10, 15, 12, 0, 0, 0, jst)
	asOfTime := time.Date(2026, 10, 15, 23, 0, 0, 0, cst)

	tx := newValidChaseCC()
	tx.PostingDate, tx.TransactionDate, tx.Lineage.IngestedAt = postingTime, &txTime, ingestedTime
	chaseResult := toChaseDoc(tx, dataSourceChase)

	assertUTC := func(name string, got time.Time, want time.Time) {
		t.Helper()
		if got.Location() != time.UTC || !got.Equal(want) {
			t.Errorf("%s: got %v (loc %v), want %v", name, got, got.Location(), want)
		}
	}
	assertUTC("PostingDate", chaseResult.PostingDate, postingTime)
	assertUTC("TransactionDate", *chaseResult.TransactionDate, txTime)
	assertUTC("Source.IngestedAt", chaseResult.Source.IngestedAt, ingestedTime)

	pos := newValidFidelityPosition()
	pos.AsOfDate, pos.Lineage.IngestedAt = asOfTime, ingestedTime
	fidResult := toFidelityDoc(pos)
	assertUTC("AsOfDate", fidResult.AsOfDate, asOfTime)
	assertUTC("Fidelity Source.IngestedAt", fidResult.Source.IngestedAt, ingestedTime)
}

// 10. TestMapper_PureNoMutation asserts inputs are never mutated and mappers are deterministic.
func TestMapper_PureNoMutation(t *testing.T) {
	t.Run("ChaseTransaction", func(t *testing.T) {
		txDate := time.Date(2026, 10, 14, 10, 0, 0, 0, time.FixedZone("EST", -5*3600))
		bal := 2500.50
		tx := newValidChaseCC()
		tx.Balance = &bal
		tx.TransactionDate = &txDate

		origBal := bal
		origTxDate := txDate
		orig := tx
		orig.Balance = &origBal
		orig.TransactionDate = &origTxDate

		raw1 := mustMarshal(t, toChaseDoc(tx, dataSourceChase))
		raw2 := mustMarshal(t, toChaseDoc(tx, dataSourceChase))

		if !bytes.Equal(raw1, raw2) {
			t.Errorf("mapper is non-deterministic")
		}
		if !reflect.DeepEqual(tx, orig) {
			t.Errorf("input mutated: got %+v, want %+v", tx, orig)
		}
	})

	t.Run("FidelityPosition", func(t *testing.T) {
		pos := newValidFidelityPosition()
		orig := pos

		raw1 := mustMarshal(t, toFidelityDoc(pos))
		raw2 := mustMarshal(t, toFidelityDoc(pos))

		if !bytes.Equal(raw1, raw2) {
			t.Errorf("mapper is non-deterministic")
		}
		if pos != orig {
			t.Errorf("input mutated: got %+v, want %+v", pos, orig)
		}
	})
}
