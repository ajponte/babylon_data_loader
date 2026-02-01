package storage

import (
	"context"
	"fmt"
	"log/slog"

	bcontext "babylon/dataloader/appcontext" // Added this import

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dbName = "datalake"
)

// ---- Abstractions for Testability ----

// DataStore defines the interface for database operations.
type DataStore interface {
	BulkWrite(
		ctx context.Context,
		models []mongo.WriteModel,
		opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	InsertOne(
		ctx context.Context,
		document interface{},
		opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
}

// CollectionProvider defines the interface for obtaining a collection.
type CollectionProvider interface {
	Collection(name string) DataStore
}

// MongoCollection adapts *mongo.Collection to DataStore.
type MongoCollection struct {
	*mongo.Collection
}

// BulkWrite performs a bulk write operation.
func (c *MongoCollection) BulkWrite(
	ctx context.Context,
	models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	result, err := c.Collection.BulkWrite(ctx, models, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to perform BulkWrite: %w", err)
	}

	return result, nil
}

// InsertOne inserts a single document.
func (c *MongoCollection) InsertOne(
	ctx context.Context,
	document interface{},
	opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	result, err := c.Collection.InsertOne(ctx, document, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to perform InsertOne: %w", err)
	}

	return result, nil
}

// MongoProvider adapts *mongo.Client to CollectionProvider.
type MongoProvider struct {
	client *mongo.Client
}

// NewMongoProvider creates a new MongoProvider.
func NewMongoProvider(client *mongo.Client) *MongoProvider {
	return &MongoProvider{client: client}
}

// Collection returns a DataStore for the given collection name.
func (p *MongoProvider) Collection(name string) DataStore {
	return &MongoCollection{p.client.Database(dbName).Collection(name)}
}

// ConnectToMongoDB establishes a connection to MongoDB.
func ConnectToMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	logger := bcontext.LoggerFromContext(ctx) // Changed to use bcontext
	logger.DebugContext(ctx, "Attempting to connect to MongoDB", "uri", uri)

	clientOptions := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	logger.InfoContext(ctx, "Successfully established connection to MongoDB")
	return client, nil
}

// WithLogger returns a new context with the provided logger.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return bcontext.WithLogger(ctx, logger) // Changed to use bcontext
}
