package storage

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoClient defines the interface for a MongoDB client.
type MongoClient interface {
	Disconnect(ctx context.Context) error
	Database(name string, opts ...*options.DatabaseOptions) *mongo.Database
}

// mongoClientWrapper is a wrapper for the mongo.Client that implements the MongoClient interface.
type mongoClientWrapper struct {
	*mongo.Client
}

// NewMongoClient creates a new MongoClient.
func NewMongoClient(client *mongo.Client) MongoClient {
	return &mongoClientWrapper{client}
}
