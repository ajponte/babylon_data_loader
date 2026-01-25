package datalake

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ---- Mocks ----

// mockCollection implements dataStore for testing.
type mockCollection struct {
	bulkWriteCalled bool
	insertOneCalled bool
}

func (m *mockCollection) BulkWrite(
	ctx context.Context,
	models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	m.bulkWriteCalled = true

	return &mongo.BulkWriteResult{UpsertedCount: int64(len(models))}, nil
}

func (m *mockCollection) InsertOne(
	ctx context.Context,
	document interface{},
	opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	m.insertOneCalled = true

	return &mongo.InsertOneResult{}, nil
}

// mockProvider always returns the same mockCollection.
type mockProvider struct {
	col *mockCollection
}

func (p *mockProvider) Collection(name string) dataStore {
	return p.col
}

// ---- Tests ----

func TestProcessCSV_BulkWriteAndInsertOneCalled(t *testing.T) {
	ctx := context.Background()

	// Create a temporary CSV file with one valid record
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Chase_test.csv")

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/31/2023,"WHOLEFDS HAR 102 230 B OAKLAND CA    211023  01/31",-75.77,DEBIT_CARD,11190.76,`
	if err := os.WriteFile(filePath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}

	mockCol := &mockCollection{}
	provider := &mockProvider{col: mockCol}

	// Call ProcessCSV with mock provider
	if err := ProcessCSV(ctx, provider, filePath, "chase", ""); err != nil {
		t.Fatalf("ProcessCSV failed: %v", err)
	}

	// Assertions
	if !mockCol.bulkWriteCalled {
		t.Errorf("expected BulkWrite to be called at least once, but it wasn't")
	}

	if !mockCol.insertOneCalled {
		t.Errorf("expected InsertOne to be called at least once, but it wasn't")
	}
}

func TestProcessCSV_NoValidRecords(t *testing.T) {
	ctx := context.Background()

	// Create a temporary CSV file with only a header (no valid rows)
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "Chase_empty.csv")

	csvContent := `Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #`
	if err := os.WriteFile(filePath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV file: %v", err)
	}

	mockCol := &mockCollection{}
	provider := &mockProvider{col: mockCol}

	// Call ProcessCSV with mock provider
	err := ProcessCSV(ctx, provider, filePath, "chase", "")
	if err == nil {
		t.Fatalf("expected ProcessCSV to fail with no valid documents, but got nil error")
	}

	// Assertions
	if mockCol.bulkWriteCalled {
		t.Errorf("expected BulkWrite not to be called, but it was")
	}

	if mockCol.insertOneCalled {
		t.Errorf("expected InsertOne not to be called, but it was")
	}
}
