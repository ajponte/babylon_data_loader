package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"babylon/dataloader/internal/domain/model"
)

var (
	// ErrTransient indicates a transient MongoDB failure (timeout or network) where retry is safe.
	ErrTransient = errors.New("mongo: transient failure (timeout or network); retry is safe")
	// ErrDuplicateKey indicates a MongoDB duplicate key constraint violation.
	ErrDuplicateKey = errors.New("mongo: duplicate key")
)

func classify(err error) error {
	if err == nil {
		return nil
	}
	if mongodrv.IsDuplicateKeyError(err) {
		return fmt.Errorf("%w: %w", ErrDuplicateKey, err)
	}
	if mongodrv.IsTimeout(err) || mongodrv.IsNetworkError(err) {
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}
	return err
}

// SaveBatch persists an IngestionBatch to the routed MongoDB collection using an unordered bulk upsert.
func (r *Repository) SaveBatch(ctx context.Context, batch *model.IngestionBatch) (*model.SyncLog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := batch.Validate(); err != nil {
		return nil, err
	}

	coll, err := collectionFor(batch.Destination)
	if err != nil {
		return nil, err
	}

	start := r.timeNow()

	if batch.RecordCount == 0 {
		now, dur := r.elapsed(start)
		return model.NewSuccessSyncLog(batch.SourceFile, batch.FileHash, coll, 0, 0, 0, now, dur), nil
	}

	plan, planErr := buildWritePlan(batch)
	if planErr != nil {
		return r.failedLog(batch, coll, planErr, start), fmt.Errorf("mongo: save batch to %s: %w", coll, planErr)
	}

	if idxErr := r.ensureIndexes(ctx, coll); idxErr != nil {
		return r.failedLog(
				batch,
				coll,
				idxErr,
				start,
			), fmt.Errorf(
				"mongo: save batch to %s: %w",
				coll,
				classify(idxErr),
			)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, bulkErr := r.db.Collection(coll).BulkWrite(ctx, plan.models, opts)
	if bulkErr != nil {
		return r.failedLog(
				batch,
				coll,
				bulkErr,
				start,
			), fmt.Errorf(
				"mongo: save batch to %s: %w",
				coll,
				classify(bulkErr),
			)
	}

	now, dur := r.elapsed(start)
	return model.NewSuccessSyncLog(
		batch.SourceFile, batch.FileHash, coll,
		int64(batch.RecordCount), res.UpsertedCount, res.MatchedCount,
		now, dur,
	), nil
}

func (r *Repository) failedLog(b *model.IngestionBatch, coll string, err error, start time.Time) *model.SyncLog {
	now, dur := r.elapsed(start)
	return model.NewFailedSyncLog(b.SourceFile, b.FileHash, coll, err.Error(), int64(b.RecordCount), now, dur)
}

func (r *Repository) elapsed(start time.Time) (time.Time, int64) {
	now := r.timeNow()
	dur := now.Sub(start).Milliseconds()
	if dur < 0 {
		dur = 0
	}
	return now, dur
}
