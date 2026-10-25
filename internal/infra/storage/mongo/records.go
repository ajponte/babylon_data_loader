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
	// RF-06: Idempotency guard against double-wrapping already classified errors.
	if errors.Is(err, ErrDuplicateKey) || errors.Is(err, ErrTransient) {
		return err
	}
	if mongodrv.IsDuplicateKeyError(err) {
		return fmt.Errorf("%w: %w", ErrDuplicateKey, err)
	}
	if mongodrv.IsTimeout(err) || mongodrv.IsNetworkError(err) {
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}

	// RF-01: Driver BulkWriteException inspection for transient write concern failures.
	var bwe mongodrv.BulkWriteException
	var bwePtr *mongodrv.BulkWriteException
	var wcErr *mongodrv.WriteConcernError
	if errors.As(err, &bwe) {
		wcErr = bwe.WriteConcernError
	} else if errors.As(err, &bwePtr) && bwePtr != nil {
		wcErr = bwePtr.WriteConcernError
	}
	if wcErr != nil {
		if wcErr.IsMaxTimeMSExpiredError() || mongodrv.IsTimeout(wcErr) || mongodrv.IsNetworkError(wcErr) {
			return fmt.Errorf("%w: %w", ErrTransient, err)
		}
	}

	return err
}

// SaveBatch persists an IngestionBatch to the routed MongoDB collection using an unordered bulk upsert.
// Invariant (Decision D5): Nil or structurally invalid batches return (nil, err) because a valid
// SyncLog requires batch metadata. Valid batches that fail write planning, index ensuring, or bulk execution
// return (failedSyncLog, wrappedErr) with the routed collection name and 0 upserted/matched counts.
// Invariant (Decision E7): If BulkWrite returns an error, any partial result counts from the driver
// are discarded. The failed SyncLog records exactly 0 upserted and 0 matched records.
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
		return r.failBatch(batch, coll, planErr, start)
	}

	if idxErr := r.ensureIndexes(ctx, coll); idxErr != nil {
		return r.failBatch(batch, coll, idxErr, start)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, bulkErr := r.db.Collection(coll).BulkWrite(ctx, plan.models, opts)
	if bulkErr != nil {
		return r.failBatch(batch, coll, bulkErr, start)
	}

	// RF-03: Defensive guard against nil *mongodrv.BulkWriteResult.
	var upserted, matched int64
	if res != nil {
		upserted = res.UpsertedCount
		matched = res.MatchedCount
	}

	now, dur := r.elapsed(start)
	return model.NewSuccessSyncLog(
		batch.SourceFile, batch.FileHash, coll,
		int64(batch.RecordCount), upserted, matched,
		now, dur,
	), nil
}

// failBatch constructs a failed audit SyncLog (Decision D5/E7) and returns a wrapped, classified error.
func (r *Repository) failBatch(
	b *model.IngestionBatch,
	coll string,
	err error,
	start time.Time,
) (*model.SyncLog, error) {
	log := r.failedLog(b, coll, err, start)
	return log, fmt.Errorf("mongo: save batch to %s: %w", coll, classify(err))
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
