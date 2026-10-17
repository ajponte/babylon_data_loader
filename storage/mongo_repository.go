package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"babylon/dataloader/datalake/model"
	"babylon/dataloader/datalake/repository"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	TransactionsCollection      = "transactions"
	ChaseTransactionsCollection = "transactions_chase"
	FidelityPositionsCollection = "positions_fidelity"
	syncTableName               = "dataSync"

	mongoSetOp       = "$set"
	fieldAccountID   = "accountId"
	fieldPostingDate = "postingDate"
)

var _ repository.Repository = (*MongoRepository)(nil)

// MongoRepository implements the datalake.Repository interface for MongoDB.
type MongoRepository struct {
	provider            CollectionProvider
	indexOnce           sync.Once
	indexErr            error
	chaseIndexMu        sync.RWMutex
	chaseIndexesCreated bool
}

// NewMongoRepository creates a new MongoRepository.
func NewMongoRepository(provider CollectionProvider) *MongoRepository {
	return &MongoRepository{
		provider: provider,
	}
}

// BulkUpsertTransactions bulk upserts transactions into the MongoDB "transactions" collection.
func (r *MongoRepository) BulkUpsertTransactions(
	ctx context.Context,
	transactions []model.Transaction,
) (repository.UpsertStats, error) {
	if len(transactions) == 0 {
		return repository.UpsertStats{}, nil // Nothing to upsert
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
		update := bson.M{mongoSetOp: doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	collectionName := fmt.Sprintf("%s_%s", TransactionsCollection, dataSource)
	collection := r.provider.Collection(collectionName)
	res, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return repository.UpsertStats{}, fmt.Errorf(
			"failed to perform bulk write for collection %s: %w",
			collectionName,
			err,
		)
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
		return repository.UpsertStats{}, fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	return toUpsertStats(res), nil
}

// ensureFidelityIndexes ensures the compound unique index exists for Fidelity positions.
func (r *MongoRepository) ensureFidelityIndexes(ctx context.Context) error {
	r.indexOnce.Do(func() {
		collection := r.provider.Collection(FidelityPositionsCollection)
		indexModel := mongo.IndexModel{
			Keys: bson.D{
				{Key: "accountNumber", Value: 1},
				{Key: "symbol", Value: 1},
				{Key: "asOfDate", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("idx_account_symbol_asof"),
		}
		_, err := collection.Indexes().CreateOne(ctx, indexModel)
		if err != nil {
			r.indexErr = fmt.Errorf("failed to create index on %s: %w", FidelityPositionsCollection, err)
		}
	})
	return r.indexErr
}

// BulkUpsertFidelityPositions bulk upserts portfolio positions into the "positions_fidelity" collection.
func (r *MongoRepository) BulkUpsertFidelityPositions(
	ctx context.Context,
	positions []model.FidelityPosition,
) (repository.UpsertStats, error) {
	if len(positions) == 0 {
		return repository.UpsertStats{}, nil
	}

	if err := r.ensureFidelityIndexes(ctx); err != nil {
		return repository.UpsertStats{}, err
	}

	collection := r.provider.Collection(FidelityPositionsCollection)

	var models []mongo.WriteModel
	for _, doc := range positions {
		filter := bson.M{
			"accountNumber": doc.AccountNumber,
			"symbol":        doc.Symbol,
			"asOfDate":      doc.AsOfDate,
		}
		update := bson.M{mongoSetOp: doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	res, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return repository.UpsertStats{}, fmt.Errorf(
			"failed to perform bulk write for collection %s: %w",
			FidelityPositionsCollection,
			err,
		)
	}

	// Update sync log audit trail
	syncCollection := r.provider.Collection(syncTableName)
	syncLog := model.SyncLog{
		CollectionName:  FidelityPositionsCollection,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: int64(len(positions)),
	}
	_, err = syncCollection.InsertOne(ctx, syncLog)
	if err != nil {
		return repository.UpsertStats{}, fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}

	return toUpsertStats(res), nil
}

// ensureChaseIndexes ensures the compound indexes exist for Chase transactions.
func (r *MongoRepository) ensureChaseIndexes(ctx context.Context) error {
	r.chaseIndexMu.RLock()
	if r.chaseIndexesCreated {
		r.chaseIndexMu.RUnlock()
		return nil
	}
	r.chaseIndexMu.RUnlock()

	r.chaseIndexMu.Lock()
	defer r.chaseIndexMu.Unlock()

	if r.chaseIndexesCreated {
		return nil
	}

	collection := r.provider.Collection(ChaseTransactionsCollection)

	queryIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: fieldAccountID, Value: 1},
			{Key: fieldPostingDate, Value: -1},
		},
		Options: options.Index().SetName("idx_chase_account_posting_date"),
	}

	uniqueIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: fieldAccountID, Value: 1},
			{Key: fieldPostingDate, Value: 1},
			{Key: "description", Value: 1},
			{Key: "amount", Value: 1},
			{Key: "source.rowNumber", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetSparse(true).SetName("idx_chase_account_post_desc_amount_row"),
	}

	_, err := collection.Indexes().CreateMany(ctx, []mongo.IndexModel{queryIndex, uniqueIndex})
	if err != nil {
		return fmt.Errorf("failed to create indexes on %s: %w", ChaseTransactionsCollection, err)
	}

	r.chaseIndexesCreated = true
	return nil
}

// BulkUpsertChaseTransactions bulk upserts Chase transactions into the "transactions_chase" collection.
func (r *MongoRepository) BulkUpsertChaseTransactions(
	ctx context.Context,
	transactions []model.ChaseTransaction,
) (repository.UpsertStats, error) {
	if len(transactions) == 0 {
		return repository.UpsertStats{}, nil
	}

	if err := r.ensureChaseIndexes(ctx); err != nil {
		return repository.UpsertStats{}, err
	}

	models := buildChaseWriteModels(transactions)
	res, err := r.executeBulkWrite(ctx, ChaseTransactionsCollection, models)
	stats := toUpsertStats(res)
	if err != nil {
		return stats, err
	}

	if auditErr := r.recordSyncAuditLog(ctx, ChaseTransactionsCollection, int64(len(transactions))); auditErr != nil {
		return stats, auditErr
	}

	return stats, nil
}

func buildChaseWriteModels(transactions []model.ChaseTransaction) []mongo.WriteModel {
	models := make([]mongo.WriteModel, 0, len(transactions))
	for _, doc := range transactions {
		filter := buildChaseFilter(doc)
		update := bson.M{mongoSetOp: doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}
	return models
}

func buildChaseFilter(doc model.ChaseTransaction) bson.M {
	return bson.M{
		fieldAccountID:     doc.AccountID,
		fieldPostingDate:   doc.PostingDate,
		"description":      doc.Description,
		"amount":           doc.Amount,
		"source.rowNumber": doc.Source.RowNumber,
	}
}

func toUpsertStats(res *mongo.BulkWriteResult) repository.UpsertStats {
	if res == nil {
		return repository.UpsertStats{}
	}
	return repository.UpsertStats{
		UpsertedCount: res.UpsertedCount,
		MatchedCount:  res.MatchedCount,
		ModifiedCount: res.ModifiedCount,
	}
}

func (r *MongoRepository) executeBulkWrite(
	ctx context.Context,
	collectionName string,
	models []mongo.WriteModel,
) (*mongo.BulkWriteResult, error) {
	collection := r.provider.Collection(collectionName)
	res, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return res, fmt.Errorf("failed to perform bulk write for collection %s: %w", collectionName, err)
	}
	return res, nil
}

func (r *MongoRepository) recordSyncAuditLog(
	ctx context.Context,
	collectionName string,
	recordCount int64,
) error {
	syncCollection := r.provider.Collection(syncTableName)
	syncLog := model.SyncLog{
		CollectionName:  collectionName,
		SyncTimestamp:   time.Now(),
		RecordsUploaded: recordCount,
	}
	if _, err := syncCollection.InsertOne(ctx, syncLog); err != nil {
		return fmt.Errorf("failed to insert into dataSync collection: %w", err)
	}
	return nil
}

// GetSyncLogs retrieves all sync logs from the dataSync collection, sorted by sync_timestamp descending.
func (r *MongoRepository) GetSyncLogs(ctx context.Context) ([]model.SyncLog, error) {
	syncCollection := r.provider.Collection(syncTableName)

	opts := options.Find().SetSort(bson.M{"sync_timestamp": -1})
	cursor, err := syncCollection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query sync logs: %w", err)
	}
	defer cursor.Close(ctx)

	var syncLogs []model.SyncLog
	err = cursor.All(ctx, &syncLogs)
	if err != nil {
		return nil, fmt.Errorf("failed to decode sync logs: %w", err)
	}

	return syncLogs, nil
}
