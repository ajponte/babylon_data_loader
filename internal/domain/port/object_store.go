package port

import (
	"context"
	"errors"
	"io"
)

// ErrObjectNotFound is returned when an object does not exist in the object store.
var ErrObjectNotFound = errors.New("object not found")

// ObjectStore is the driven port for reading and archiving raw source objects.
type ObjectStore interface {
	// DownloadStream opens the object for streaming. The caller must Close the reader.
	DownloadStream(ctx context.Context, key string) (io.ReadCloser, error)
	// ArchiveFile copies srcKey to destKey, then deletes srcKey. Not atomic.
	ArchiveFile(ctx context.Context, srcKey, destKey string) error
}
