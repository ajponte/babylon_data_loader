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
	if mongodrv.IsTimeout(err) || mongodrv.IsNetworkError(err) || isWriteConcernTransient(err) {
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}

	return err
}

func isWriteConcernTransient(err error) bool {
	var bwe mongodrv.BulkWriteException
	var bwePtr *mongodrv.BulkWriteException
	var wcErr *mongodrv.WriteConcernError
	if errors.As(err, &bwe) {
		wcErr = bwe.WriteConcernError
	} else if errors.As(err, &bwePtr) && bwePtr != nil {
		wcErr = bwePtr.WriteConcernError
	}
	if wcErr == nil {
		return false
	}
	return wcErr.IsMaxTimeMSExpiredError() || mongodrv.IsTimeout(wcErr) || mongodrv.IsNetworkError(wcErr)
}

// SaveBatch persists an IngestionBatch to the routed MongoDB collection using an unordered bulk upsert.
// Invariant (Decision D5): Nil or structurally invalid batches return (nil, err) because a valid
// SyncLog requires batch metadata. Valid batches that fail write planning, index ensuring, or bulk execution
// return (failedSyncLog, wrappedErr) with the routed collection name and 0 upserted/matched counts.
// Invariant (Decision E7): If BulkWrite returns an error, any partial result counts from the driver
// are discarded. The failed SyncLog records exactly 0 upserted and 0 matched records.
func (r *Repository) SaveBatch(ctx context.Context, batch *model.IngestionBatch) (*model.SyncLog, error) {
	coll, err := validateBatch(ctx, batch)
	if err != nil {
		return nil, err
	}

	start := r.timeNow()
	if batch.RecordCount == 0 {
		return r.emptySuccessLog(batch, coll, start), nil
	}

	plan, planErr := buildWritePlan(batch)
	if planErr != nil {
		return r.failBatch(batch, coll, planErr, start)
	}

	res, writeErr := r.executeBulkWrite(ctx, coll, plan.models)
	if writeErr != nil {
		return r.failBatch(batch, coll, writeErr, start)
	}

	return r.successLog(batch, coll, res, start), nil
}

func validateBatch(ctx context.Context, batch *model.IngestionBatch) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := batch.Validate(); err != nil {
		return "", err
	}
	return collectionFor(batch.Destination)
}

func (r *Repository) executeBulkWrite(
	ctx context.Context,
	coll string,
	models []mongodrv.WriteModel,
) (*mongodrv.BulkWriteResult, error) {
	if err := r.ensureIndexes(ctx, coll); err != nil {
		return nil, err
	}
	opts := options.BulkWrite().SetOrdered(false)
	return r.db.Collection(coll).BulkWrite(ctx, models, opts)
}

func (r *Repository) successLog(
	b *model.IngestionBatch,
	coll string,
	res *mongodrv.BulkWriteResult,
	start time.Time,
) *model.SyncLog {
	var upserted, matched int64
	if res != nil {
		upserted = res.UpsertedCount
		matched = res.MatchedCount
	}
	now, dur := r.elapsed(start)
	return model.NewSuccessSyncLog(
		b.SourceFile, b.FileHash, coll,
		int64(b.RecordCount), upserted, matched,
		now, dur,
	)
}

func (r *Repository) emptySuccessLog(b *model.IngestionBatch, coll string, start time.Time) *model.SyncLog {
	now, dur := r.elapsed(start)
	return model.NewSuccessSyncLog(b.SourceFile, b.FileHash, coll, 0, 0, 0, now, dur)
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
