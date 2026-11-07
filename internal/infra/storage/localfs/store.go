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
	// ErrSameSourceAndDestination indicates that source and destination resolve to the same file.
	ErrSameSourceAndDestination = errors.New("localfs: source and destination keys must differ")
)

var _ port.ObjectStore = (*Store)(nil)

// Store implements port.ObjectStore over a local filesystem directory.
type Store struct {
	root   string
	rename func(oldpath, newpath string) error
}

// New constructs a new local filesystem object store rooted at root.
func New(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, ErrMissingRoot
	}
	cleanRoot := filepath.Clean(root)
	fi, err := os.Stat(cleanRoot)
	if err != nil {
		return nil, fmt.Errorf("localfs: root directory: %w", sanitizeError(err))
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("localfs: root must be a directory: %w", fs.ErrInvalid)
	}
	return &Store{root: cleanRoot, rename: os.Rename}, nil
}

// DownloadStream opens the object identified by key for reading.
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
	if valErr := validateRegularFile(f); valErr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("localfs: download %q: %w", key, valErr)
	}
	return f, nil
}

// validateRegularFile asserts that f points to a regular file, returning fs.ErrInvalid if not.
func validateRegularFile(f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return sanitizeError(err)
	}
	if !fi.Mode().IsRegular() {
		return fs.ErrInvalid
	}
	return nil
}

// ArchiveFile copies srcKey to destKey, then deletes srcKey.
func (s *Store) ArchiveFile(ctx context.Context, srcKey, destKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if srcKey == destKey {
		return ErrSameSourceAndDestination
	}
	srcPath, srcErr := s.resolvePath(srcKey)
	if srcErr != nil {
		return srcErr
	}
	destPath, destErr := s.resolvePath(destKey)
	if destErr != nil {
		return destErr
	}
	if verifyErr := s.verifyArchiveSource(srcPath, destPath, srcKey, destKey); verifyErr != nil {
		return verifyErr
	}
	if dirErr := ensureParentDir(destPath, srcKey, destKey); dirErr != nil {
		return dirErr
	}
	if renameErr := s.rename(srcPath, destPath); renameErr == nil {
		return nil
	}
	return s.copyAndRemove(ctx, srcPath, destPath, srcKey, destKey)
}

// verifyArchiveSource validates that source exists, is a regular file, and does not collide with destPath.
func (s *Store) verifyArchiveSource(srcPath, destPath, srcKey, destKey string) error {
	if srcPath == destPath {
		return ErrSameSourceAndDestination
	}
	srcFi, err := os.Stat(srcPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, port.ErrObjectNotFound)
		}
		return fmt.Errorf("localfs: archive %q -> %q: stat source: %w", srcKey, destKey, sanitizeError(err))
	}
	if !srcFi.Mode().IsRegular() {
		return fmt.Errorf("localfs: archive %q -> %q: source is not a regular file: %w", srcKey, destKey, fs.ErrInvalid)
	}
	if destFi, destStatErr := os.Stat(destPath); destStatErr == nil && os.SameFile(srcFi, destFi) {
		return ErrSameSourceAndDestination
	}
	return nil
}

// ensureParentDir ensures the parent directory for targetPath exists with 0o750 permissions.
func ensureParentDir(targetPath, srcKey, destKey string) error {
	const dirPerm = 0o750
	if err := os.MkdirAll(filepath.Dir(targetPath), dirPerm); err != nil {
		return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, sanitizeError(err))
	}
	return nil
}

// List returns the sorted keys of all regular files located under prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefixPath, cleanPrefix := s.root, ""
	if prefix != "" && prefix != "." {
		resolved, resolveErr := s.resolvePath(prefix)
		if resolveErr != nil {
			return nil, resolveErr
		}
		prefixPath, cleanPrefix = resolved, path.Clean(prefix)
	}
	entries, err := os.ReadDir(prefixPath)
	if err != nil {
		return nil, fmt.Errorf("localfs: list %q: %w", prefix, sanitizeError(err))
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			keys = append(keys, formatListKey(cleanPrefix, entry.Name()))
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// formatListKey joins the normalized prefix and file name.
func formatListKey(cleanPrefix, entryName string) string {
	if cleanPrefix == "" {
		return entryName
	}
	return path.Join(cleanPrefix, entryName)
}

func (s *Store) copyAndRemove(ctx context.Context, srcPath, destPath, srcKey, destKey string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, port.ErrObjectNotFound)
		}
		return fmt.Errorf("localfs: archive %q -> %q: open source: %w", srcKey, destKey, sanitizeError(err))
	}
	defer srcFile.Close()

	if copyErr := copyToFile(ctx, srcFile, destPath); copyErr != nil {
		return fmt.Errorf("localfs: archive %q -> %q: copy: %w", srcKey, destKey, sanitizeError(copyErr))
	}
	_ = srcFile.Close()

	if removeErr := os.Remove(srcPath); removeErr != nil {
		return fmt.Errorf("localfs: archive %q -> %q: remove source: %w", srcKey, destKey, sanitizeError(removeErr))
	}
	return nil
}

// copyToFile creates destPath with 0o600 permissions and copies src to it, cleaning up on error.
func copyToFile(ctx context.Context, src io.Reader, destPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	const filePerm = 0o600
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(destFile, src)
	closeErr := destFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(destPath)
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = os.Remove(destPath)
		return ctxErr
	}
	return nil
}

func (s *Store) resolvePath(key string) (string, error) {
	if !isRelativeKey(key) {
		return "", ErrInvalidKey
	}
	fullPath := filepath.Join(s.root, filepath.Clean(filepath.FromSlash(key)))
	if !isWithinRoot(s.root, fullPath) {
		return "", ErrInvalidKey
	}
	return fullPath, nil
}

// isRelativeKey verifies that key is a well-formed relative path without escapes or null bytes.
func isRelativeKey(key string) bool {
	if key == "" || strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") || filepath.IsAbs(key) ||
		strings.Contains(key, "\x00") || filepath.VolumeName(filepath.FromSlash(key)) != "" {
		return false
	}
	for _, seg := range strings.FieldsFunc(key, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// isWithinRoot verifies that fullPath is strictly contained within root.
func isWithinRoot(root, fullPath string) bool {
	relPath, err := filepath.Rel(root, fullPath)
	if err != nil {
		return false
	}
	return relPath != "." && relPath != ".." &&
		!strings.HasPrefix(relPath, ".."+string(filepath.Separator)) && !filepath.IsAbs(relPath)
}

func sanitizeError(err error) error {
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return linkErr.Err
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}
