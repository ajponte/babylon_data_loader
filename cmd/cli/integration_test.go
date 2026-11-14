//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/infra/storage/mongo"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	collTransactionsChase     = "transactions_chase"
	collPositionsFidelity     = "positions_fidelity"
	collTransactionsSynthetic = "transactions_synthetic"
	collDataSync              = "dataSync"
)

const itChaseCheckingCSV = `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,10/15/2026,"GROCERY STORE",-85.50,DEBIT,2414.50,
CREDIT,10/16/2026,"PAYROLL DIRECT DEP",1500.00,ACH,3914.50,
`

const itFidelityPositionsCSV = `Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,Today's gain/loss dollar,Today's gain/loss percent,Total gain/loss dollar,Total gain/loss percent,Percent of account,Cost basis total,Average cost basis,Type
SYNTH-ROTH-101,ROTH IRA,SPAXX**,HELD IN MONEY MARKET,,,,$3250.00,,,,,25.00%,,,Cash,
SYNTH-ROTH-101,ROTH IRA,FSKAX,FIDELITY TOTAL MARKET INDEX,45.250,$150.00,+$1.25,$6787.50,+$56.56,+0.84%,+$787.50,+13.13%,52.21%,$6000.00,$132.59,Cash,

"Date downloaded Sep-25-2026 9:50 p.m ET"
`

const itSyntheticCSV = `Details,Posting Date,Description,Category,Amount,Type,Balance,Check or Slip #
DEBIT,10/31/2026,Synthetic grocery,Shopping,-45.50,DEBIT,3500.00,
CREDIT,10/30/2026,Synthetic payroll,Income,1200.00,CREDIT,4700.00,
`

// validateIntegrationTarget enforces strict isolation guards for integration testing.
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

type cliTestEnv struct {
	unprocessed string
	processed   string
	dbName      string
	deps        deps
	rawDB       *mongodrv.Database
}

func setupCLIIntegration(t *testing.T) cliTestEnv {
	t.Helper()

	uri := os.Getenv("MONGO_IT_URI")
	if uri == "" {
		t.Skip("skipping integration test: MONGO_IT_URI is unset")
	}

	dbName := fmt.Sprintf("babylon_it_cli_%d", time.Now().UnixNano())
	if err := validateIntegrationTarget(uri, dbName); err != nil {
		t.Fatalf("integration guard violation: %v", err)
	}

	clientCtx, clientCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer clientCancel()

	client, err := mongodrv.Connect(clientCtx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("failed to connect to mongo: %v", err)
	}
	if pingErr := client.Ping(clientCtx, nil); pingErr != nil {
		_ = client.Disconnect(clientCtx)
		t.Fatalf("failed to ping mongo: %v", pingErr)
	}

	rawDB := client.Database(dbName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = rawDB.Drop(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})

	root := t.TempDir()
	unprocessed := filepath.Join(root, "unprocessed")
	processed := filepath.Join(root, "processed")

	if mkErr := os.MkdirAll(unprocessed, 0o750); mkErr != nil {
		t.Fatalf("failed to create unprocessed dir: %v", mkErr)
	}
	if mkErr := os.MkdirAll(processed, 0o750); mkErr != nil {
		t.Fatalf("failed to create processed dir: %v", mkErr)
	}

	t.Setenv("CSV_DIR", root)
	t.Setenv("UNPROCESSED_DIR", "unprocessed")
	t.Setenv("PROCESSED_DIR", "processed")
	t.Setenv("MOVE_PROCESSED_FILES", "true")
	t.Setenv("MONGO_URI", uri)

	d := deps{
		connectRepo: func(ctx context.Context, u string) (repository, error) {
			return mongo.NewRepository(ctx, mongo.Config{URI: u, Database: dbName, Timeout: 10 * time.Second})
		},
		logger: slog.New(slog.DiscardHandler),
		now:    time.Now,
	}

	return cliTestEnv{
		unprocessed: unprocessed,
		processed:   processed,
		dbName:      dbName,
		deps:        d,
		rawDB:       rawDB,
	}
}

func writeFixture(t *testing.T, dir, filename, content string) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed writing fixture %s: %v", filename, err)
	}
	return path
}

func assertCollectionCount(t *testing.T, ctx context.Context, coll *mongodrv.Collection, want int64, label string) {
	t.Helper()
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("failed counting documents in %s: %v", label, err)
	}
	if count != want {
		t.Fatalf("collection %s document count mismatch: got %d, want %d", label, count, want)
	}
}

// TestE2E_CLIIngest_RealMongo verifies end-to-end multi-statement ingestion via CLI run()
// writing to live MongoDB collections (Chase, Fidelity, Synthetic) and audit logs.
func TestE2E_CLIIngest_RealMongo(t *testing.T) {
	env := setupCLIIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	writeFixture(t, env.unprocessed, "chase1234_checking.csv", itChaseCheckingCSV)
	writeFixture(t, env.unprocessed, "Portfolio_Positions_Nov2026.csv", itFidelityPositionsCSV)
	writeFixture(t, env.unprocessed, "test_synthetic_records.csv", itSyntheticCSV)

	if err := run(ctx, []string{"ingest"}, env.deps); err != nil {
		t.Fatalf("run ingest failed: %v", err)
	}

	assertCollectionCount(t, ctx, env.rawDB.Collection(collTransactionsChase), 2, collTransactionsChase)
	assertCollectionCount(t, ctx, env.rawDB.Collection(collPositionsFidelity), 2, collPositionsFidelity)
	assertCollectionCount(t, ctx, env.rawDB.Collection(collTransactionsSynthetic), 2, collTransactionsSynthetic)
	assertCollectionCount(t, ctx, env.rawDB.Collection(collDataSync), 3, collDataSync)

	// Verify all dataSync audit logs are recorded with SUCCESS status
	syncColl := env.rawDB.Collection(collDataSync)
	successCount, err := syncColl.CountDocuments(ctx, bson.M{"status": "SUCCESS"})
	if err != nil {
		t.Fatalf("failed counting success sync logs: %v", err)
	}
	if successCount != 3 {
		t.Errorf("expected 3 SUCCESS sync logs, got %d", successCount)
	}

	// Verify archival: unprocessed directory should have 0 CSV files; processed should have 3
	unprocEntries, err := filepath.Glob(filepath.Join(env.unprocessed, "*.csv"))
	if err != nil {
		t.Fatalf("glob unprocessed failed: %v", err)
	}
	if len(unprocEntries) != 0 {
		t.Errorf("expected 0 files remaining in unprocessed, got %d", len(unprocEntries))
	}

	procEntries, err := filepath.Glob(filepath.Join(env.processed, "*.csv"))
	if err != nil {
		t.Fatalf("glob processed failed: %v", err)
	}
	if len(procEntries) != 3 {
		t.Errorf("expected 3 archived files in processed, got %d", len(procEntries))
	}
}

// TestE2E_CLIIngest_RerunIsIdempotent verifies repeated ingestion of identical statements
// preserves existing record counts, updates lineage safely, and preserves historical source.s3Uri.
func TestE2E_CLIIngest_RerunIsIdempotent(t *testing.T) {
	env := setupCLIIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const legacyS3URI = "s3://legacy-datalake/chase1234/legacy_chase.csv"
	postingDate := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

	// Pre-seed an existing document containing a legacy s3Uri to verify dotted lineage update preservation (E6)
	seededDoc := bson.M{
		"accountId":       "1234",
		"postingDate":     postingDate,
		"description":     "GROCERY STORE",
		"amount":          -85.50,
		"transactionType": "DEBIT",
		"dataSource":      "chase",
		"source": bson.M{
			"fileName":   "legacy_chase.csv",
			"rowNumber":  int64(2),
			"ingestedAt": postingDate,
			"s3Uri":      legacyS3URI,
		},
	}
	chaseColl := env.rawDB.Collection(collTransactionsChase)
	if _, err := chaseColl.InsertOne(ctx, seededDoc); err != nil {
		t.Fatalf("failed pre-seeding chase document: %v", err)
	}

	// Ingest fixture (1st run)
	writeFixture(t, env.unprocessed, "chase1234_checking.csv", itChaseCheckingCSV)
	if err := run(ctx, []string{"ingest"}, env.deps); err != nil {
		t.Fatalf("run ingest 1st attempt failed: %v", err)
	}

	assertCollectionCount(t, ctx, chaseColl, 2, collTransactionsChase)

	// Verify legacy s3Uri was preserved while fileName was refreshed
	var matchedDoc bson.M
	filter := bson.D{
		{Key: "accountId", Value: "1234"},
		{Key: "description", Value: "GROCERY STORE"},
	}
	if err := chaseColl.FindOne(ctx, filter).Decode(&matchedDoc); err != nil {
		t.Fatalf("failed finding pre-seeded document: %v", err)
	}

	sourceSub, ok := matchedDoc["source"].(bson.M)
	if !ok {
		t.Fatalf("expected source subdocument, got: %v", matchedDoc["source"])
	}
	if sourceSub["s3Uri"] != legacyS3URI {
		t.Errorf("expected source.s3Uri preserved as %q, got %v", legacyS3URI, sourceSub["s3Uri"])
	}
	if sourceSub["fileName"] != "chase1234_checking.csv" {
		t.Errorf("expected source.fileName updated to chase1234_checking.csv, got %v", sourceSub["fileName"])
	}

	// Re-run with the exact same file (2nd run)
	writeFixture(t, env.unprocessed, "chase1234_checking.csv", itChaseCheckingCSV)
	if err := run(ctx, []string{"ingest"}, env.deps); err != nil {
		t.Fatalf("run ingest 2nd attempt failed: %v", err)
	}

	// Verify record count is strictly unchanged (idempotency)
	assertCollectionCount(t, ctx, chaseColl, 2, collTransactionsChase)

	// Verify dataSync records 2 sync logs, with the 2nd run recording 0 new upserts and 2 matched
	syncColl := env.rawDB.Collection(collDataSync)
	assertCollectionCount(t, ctx, syncColl, 2, collDataSync)

	cursor, err := syncColl.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "syncedAt", Value: -1}}).SetLimit(1))
	if err != nil {
		t.Fatalf("failed querying latest sync log: %v", err)
	}
	defer cursor.Close(ctx)

	var latestLogs []bson.M
	if err := cursor.All(ctx, &latestLogs); err != nil || len(latestLogs) != 1 {
		t.Fatalf("expected 1 latest sync log, got %d, err: %v", len(latestLogs), err)
	}

	latestLog := latestLogs[0]
	if latestLog["status"] != "SUCCESS" {
		t.Errorf("expected latest sync log status SUCCESS, got %v", latestLog["status"])
	}
	if upserted, ok := latestLog["upsertedCount"].(int64); ok && upserted != 0 {
		t.Errorf("expected 0 upserted records on idempotent re-run, got %d", upserted)
	}
	if matched, ok := latestLog["matchedCount"].(int64); ok && matched != 2 {
		t.Errorf("expected 2 matched records on idempotent re-run, got %d", matched)
	}
}

// TestSyntheticCSV_RoutesToSyntheticDestination verifies synthetic statements route exclusively
// to transactions_synthetic while preserving schema fields (category, details, balance).
func TestSyntheticCSV_RoutesToSyntheticDestination(t *testing.T) {
	env := setupCLIIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	writeFixture(t, env.unprocessed, "test_synthetic_records.csv", itSyntheticCSV)

	if err := run(ctx, []string{"ingest"}, env.deps); err != nil {
		t.Fatalf("run ingest failed: %v", err)
	}

	synthColl := env.rawDB.Collection(collTransactionsSynthetic)
	chaseColl := env.rawDB.Collection(collTransactionsChase)
	fidelityColl := env.rawDB.Collection(collPositionsFidelity)

	assertCollectionCount(t, ctx, synthColl, 2, collTransactionsSynthetic)
	assertCollectionCount(t, ctx, chaseColl, 0, collTransactionsChase)
	assertCollectionCount(t, ctx, fidelityColl, 0, collPositionsFidelity)

	// Verify schema field preservation on an ingested synthetic document
	var doc bson.M
	filter := bson.D{{Key: "description", Value: "Synthetic grocery"}}
	if err := synthColl.FindOne(ctx, filter).Decode(&doc); err != nil {
		t.Fatalf("failed querying synthetic document: %v", err)
	}

	if doc["details"] != "DEBIT" {
		t.Errorf("expected details 'DEBIT', got %v", doc["details"])
	}
	if doc["category"] != "Shopping" {
		t.Errorf("expected category 'Shopping', got %v", doc["category"])
	}
	if balance, ok := doc["balance"].(float64); !ok || balance != 3500.00 {
		t.Errorf("expected balance 3500.00, got %v", doc["balance"])
	}
	if doc["dataSource"] != "synthetic" {
		t.Errorf("expected dataSource 'synthetic', got %v", doc["dataSource"])
	}
	if doc["accountId"] != "0000" {
		t.Errorf("expected accountId '0000', got %v", doc["accountId"])
	}
}
