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
	TransactionsCollection = "transactions"
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

	// Assume all transactions in a batch belong to the same data source
	// This assumption holds based on the current processing flow (one file = one data source)
	dataSource := transactions[0].DataSource

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

	collectionName := fmt.Sprintf("%s_%s", TransactionsCollection, dataSource)
	collection := r.provider.Collection(collectionName)
	_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("failed to perform bulk write for collection %s: %w", collectionName, err)
	}

	// Update sync log
	syncCollection := r.provider.Collection(syncTableName)
	syncLog := model.SyncLog{
		CollectionName:  collectionName,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: int64(len(transactions)), // Using len(transactions) for recordsUploaded
	}
	_, err = syncCollection.InsertOne(ctx, syncLog)
	if err != nil {
		return fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	return nil
}
