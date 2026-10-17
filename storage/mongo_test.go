package storage_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	bcontext "babylon/dataloader/appcontext"
	"babylon/dataloader/storage"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestWithLogger verifies that WithLogger injects the logger into the context.
func TestWithLogger(t *testing.T) {
	logger := slog.Default()
	ctx := storage.WithLogger(context.Background(), logger)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	ctxLogger := bcontext.LoggerFromContext(ctx)
	if ctxLogger != logger {
		t.Errorf("expected context logger %v, got %v", logger, ctxLogger)
	}
}

// TestNewMongoClient verifies that NewMongoClient instantiates a MongoClient wrapper.
func TestNewMongoClient(t *testing.T) {
	client := storage.NewMongoClient(nil)
	if client == nil {
		t.Fatal("expected non-nil MongoClient")
	}
}

// TestConnectToMongoDB_InvalidURI verifies error handling on malformed MongoDB URIs.
func TestConnectToMongoDB_InvalidURI(t *testing.T) {
	ctx := context.Background()
	client, err := storage.ConnectToMongoDB(ctx, "://malformed-uri")
	if err == nil {
		t.Fatal("expected error for invalid URI, got nil")
	}
	if !strings.Contains(err.Error(), "failed to connect to MongoDB") {
		t.Errorf("expected error message to contain 'failed to connect to MongoDB', got: %v", err)
	}
	if client != nil {
		t.Errorf("expected nil client on error, got: %v", client)
	}
}

// TestConnectToMongoDB_PingFailure verifies error handling when MongoDB is unreachable.
func TestConnectToMongoDB_PingFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Connecting to an unreachable port with short timeout triggers client.Ping failure.
	client, err := storage.ConnectToMongoDB(
		ctx,
		"mongodb://127.0.0.1:27018/?connectTimeoutMS=50&serverSelectionTimeoutMS=50",
	)
	if err == nil {
		t.Fatal("expected ping error for unreachable MongoDB, got nil")
	}
	if !strings.Contains(err.Error(), "failed to ping MongoDB") {
		t.Errorf("expected error message to contain 'failed to ping MongoDB', got: %v", err)
	}
	if client != nil {
		t.Errorf("expected nil client on ping failure, got: %v", client)
	}
}

func setupTestMongoCollection(t *testing.T) (*storage.MongoCollection, func()) {
	t.Helper()

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	cleanup := func() {
		_ = client.Disconnect(context.Background())
	}

	wrappedClient := storage.NewMongoClient(client)
	provider := storage.NewMongoProvider(wrappedClient)
	if provider == nil {
		cleanup()
		t.Fatal("expected non-nil MongoProvider")
	}

	ds := provider.Collection("test_collection")
	if ds == nil {
		cleanup()
		t.Fatal("expected non-nil DataStore from Collection")
	}

	mongoColl, ok := ds.(*storage.MongoCollection)
	if !ok || mongoColl == nil {
		cleanup()
		t.Fatalf("expected *storage.MongoCollection, got: %T", ds)
	}

	return mongoColl, cleanup
}

// TestMongoCollection_InsertOne_Error verifies InsertOne error wrapping.
func TestMongoCollection_InsertOne_Error(t *testing.T) {
	mongoColl, cleanup := setupTestMongoCollection(t)
	defer cleanup()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := mongoColl.InsertOne(canceledCtx, bson.M{"key": "value"})
	if err == nil {
		t.Fatal("expected error on canceled context, got nil")
	}
	if !strings.Contains(err.Error(), "failed to perform InsertOne") {
		t.Errorf("unexpected error string: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil result on error, got: %v", res)
	}
}

// TestMongoCollection_BulkWrite_Error verifies BulkWrite error wrapping.
func TestMongoCollection_BulkWrite_Error(t *testing.T) {
	mongoColl, cleanup := setupTestMongoCollection(t)
	defer cleanup()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	models := []mongo.WriteModel{
		mongo.NewInsertOneModel().SetDocument(bson.M{"key": "value"}),
	}
	res, err := mongoColl.BulkWrite(canceledCtx, models)
	if err == nil {
		t.Fatal("expected error on canceled context, got nil")
	}
	if !strings.Contains(err.Error(), "failed to perform BulkWrite") {
		t.Errorf("unexpected error string: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil result on error, got: %v", res)
	}
}

// TestMongoCollection_Find_Error verifies Find error wrapping.
func TestMongoCollection_Find_Error(t *testing.T) {
	mongoColl, cleanup := setupTestMongoCollection(t)
	defer cleanup()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cursor, err := mongoColl.Find(canceledCtx, bson.M{})
	if err == nil {
		t.Fatal("expected error on canceled context, got nil")
	}
	if !strings.Contains(err.Error(), "failed to perform Find") {
		t.Errorf("unexpected error string: %v", err)
	}
	if cursor != nil {
		t.Errorf("expected nil cursor on error, got: %v", cursor)
	}
}
