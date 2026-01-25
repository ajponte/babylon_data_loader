package datalake

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"babylon/dataloader/storage"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ---- Mocks ----

// mockCollection implements storage.DataStore for testing.
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

func (p *mockProvider) Collection(name string) storage.DataStore {
	return p.col
}

// ---- Tests ----

func TestProcessFile(t *testing.T) {
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

	// create a dummy os.DirEntry
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to get file info: %v", err)
	}

	dirEntry := newMockDirEntry(fileInfo)

	// Call processFile with mock provider
	if err := processFile(ctx, provider, dirEntry, tmpDir, "", false); err != nil {
		t.Fatalf("processFile failed: %v", err)
	}

	// Assertions
	if !mockCol.bulkWriteCalled {
		t.Errorf("expected BulkWrite to be called at least once, but it wasn't")
	}

	if !mockCol.insertOneCalled {
		t.Errorf("expected InsertOne to be called at least once, but it wasn't")
	}
}

// mockDirEntry implements fs.DirEntry for testing.
type mockDirEntry struct {
	os.FileInfo
}

func newMockDirEntry(info os.FileInfo) mockDirEntry {
	return mockDirEntry{info}
}

func (m mockDirEntry) Type() os.FileMode {
	return m.Mode().Type()
}

func (m mockDirEntry) Info() (os.FileInfo, error) {
	return m, nil
}