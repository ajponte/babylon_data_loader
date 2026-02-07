package storage

import (
	"context"
	"fmt"
	"time"

	"babylon/dataloader/datalake/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	transactionsCollection = "transactions" // Hardcoded collection name for transactions
	syncTableName          = "dataSync"
)

// MongoRepository implements the datalake.Repository interface for MongoDB.
type MongoRepository struct {
	provider CollectionProvider
}

// NewMongoRepository creates a new MongoRepository.
func NewMongoRepository(provider CollectionProvider) *MongoRepository {
	return &MongoRepository{
		provider: provider,
	}
}

// BulkUpsertTransactions bulk upserts transactions into the MongoDB "transactions" collection.
func (r *MongoRepository) BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) error {
	if len(transactions) == 0 {
		return nil // Nothing to upsert
	}

	var models []mongo.WriteModel
	for _, doc := range transactions {
		filter := bson.M{
			"Details":     doc.Details,
			"PostingDate": doc.PostingDate,
			"Description": doc.Description,
			"dataSource":  doc.DataSource,
			"accountID":   doc.AccountID,
		}
		update := bson.M{"$set": doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	collection := r.provider.Collection(transactionsCollection)
	_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("failed to perform bulk write for collection %s: %w", transactionsCollection, err)
	}

	// Update sync log
	syncCollection := r.provider.Collection(syncTableName)
	syncLog := model.SyncLog{
		CollectionName:  transactionsCollection,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: int64(len(transactions)), // Using len(transactions) for recordsUploaded
	}
	_, err = syncCollection.InsertOne(ctx, syncLog)
	if err != nil {
		return fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	return nil
}
