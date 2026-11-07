package localfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"babylon/dataloader/internal/domain/port"
)

var (
	// ErrMissingRoot indicates that the root directory path is empty.
	ErrMissingRoot = errors.New("localfs: root directory is required")
	// ErrInvalidKey indicates that a key resolves outside the root or is not relative.
	ErrInvalidKey = errors.New("localfs: key must be a relative path inside the root")
	// ErrSameSourceAndDestination indicates that the source and destination keys are identical.
	ErrSameSourceAndDestination = errors.New("localfs: source and destination keys must differ")
)

var _ port.ObjectStore = (*Store)(nil)

// Store implements port.ObjectStore over a local filesystem directory.
type Store struct {
	root   string
	rename func(oldpath, newpath string) error
}

// New constructs a new local filesystem object store rooted at root.
// root must be a non-empty path to an existing directory.
func New(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, ErrMissingRoot
	}

	cleanRoot := filepath.Clean(root)
	fi, err := os.Stat(cleanRoot)
	if err != nil {
		return nil, fmt.Errorf("localfs: root directory: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("localfs: root must be a directory: %w", fs.ErrInvalid)
	}

	return &Store{
		root:   cleanRoot,
		rename: os.Rename,
	}, nil
}

// DownloadStream opens the object identified by key for reading.
// The caller must Close the returned ReadCloser.
func (s *Store) DownloadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fullPath, resolveErr := s.resolvePath(key)
	if resolveErr != nil {
		return nil, resolveErr
	}

	f, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("localfs: download %q: %w", key, port.ErrObjectNotFound)
		}
		return nil, fmt.Errorf("localfs: download %q: %w", key, sanitizeError(err))
	}

	return f, nil
}

// ArchiveFile copies srcKey to destKey, then deletes srcKey.
// Destination directories are created with 0o750 permissions if they do not exist.
func (s *Store) ArchiveFile(ctx context.Context, srcKey, destKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if srcKey == destKey {
		return ErrSameSourceAndDestination
	}

	srcPath, resolveErr := s.resolvePath(srcKey)
	if resolveErr != nil {
		return resolveErr
	}
	destPath, resolveErr := s.resolvePath(destKey)
	if resolveErr != nil {
		return resolveErr
	}

	destDir := filepath.Dir(destPath)
	const dirPerm = 0o750
	if mkdirErr := os.MkdirAll(destDir, dirPerm); mkdirErr != nil {
		return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, sanitizeError(mkdirErr))
	}

	if renameErr := s.rename(srcPath, destPath); renameErr == nil {
		return nil
	}

	return s.copyAndRemove(srcPath, destPath, srcKey, destKey)
}

// List returns the sorted keys of all regular files located under prefix.
// Subdirectories are excluded from the returned keys.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var prefixPath string
	var cleanPrefix string
	if prefix == "" || prefix == "." {
		prefixPath = s.root
		cleanPrefix = ""
	} else {
		resolved, resolveErr := s.resolvePath(prefix)
		if resolveErr != nil {
			return nil, resolveErr
		}
		prefixPath = resolved
		cleanPrefix = path.Clean(prefix)
	}

	entries, err := os.ReadDir(prefixPath)
	if err != nil {
		return nil, fmt.Errorf("localfs: list %q: %w", prefix, sanitizeError(err))
	}

	var keys []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if cleanPrefix == "" {
			keys = append(keys, entry.Name())
		} else {
			keys = append(keys, path.Join(cleanPrefix, entry.Name()))
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func (s *Store) copyAndRemove(srcPath, destPath, srcKey, destKey string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, port.ErrObjectNotFound)
		}
		return fmt.Errorf("localfs: archive %q -> %q: open source: %w", srcKey, destKey, sanitizeError(err))
	}
	defer srcFile.Close()

	const filePerm = 0o600
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("localfs: archive %q -> %q: create destination: %w", srcKey, destKey, sanitizeError(err))
	}

	_, copyErr := io.Copy(destFile, srcFile)
	closeErr := destFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(destPath)
		cause := copyErr
		if cause == nil {
			cause = closeErr
		}
		return fmt.Errorf("localfs: archive %q -> %q: copy: %w", srcKey, destKey, sanitizeError(cause))
	}

	_ = srcFile.Close()

	if removeErr := os.Remove(srcPath); removeErr != nil {
		return fmt.Errorf("localfs: archive %q -> %q: remove source: %w", srcKey, destKey, sanitizeError(removeErr))
	}

	return nil
}

func (s *Store) resolvePath(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") || filepath.IsAbs(key) {
		return "", ErrInvalidKey
	}

	for _, seg := range strings.FieldsFunc(key, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == "." || seg == ".." {
			return "", ErrInvalidKey
		}
	}

	fullPath := filepath.Join(s.root, filepath.Clean(filepath.FromSlash(key)))
	relPath, err := filepath.Rel(s.root, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return "", ErrInvalidKey
	}

	return fullPath, nil
}

func sanitizeError(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}
