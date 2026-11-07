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
	if err := validateRootDirectory(cleanRoot); err != nil {
		return nil, err
	}
	return &Store{root: cleanRoot, rename: os.Rename}, nil
}

// validateRootDirectory verifies that root exists and is a directory.
func validateRootDirectory(root string) error {
	fi, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("localfs: root directory: %w", sanitizeError(err))
	}
	if !fi.IsDir() {
		return fmt.Errorf("localfs: root must be a directory: %w", fs.ErrInvalid)
	}
	return nil
}

// DownloadStream opens the object identified by key for reading.
func (s *Store) DownloadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullPath, err := s.resolvePath(key)
	if err != nil {
		return nil, err
	}
	return openRegularFile(fullPath, key)
}

// openRegularFile opens fullPath and validates that it is a regular file.
func openRegularFile(fullPath, key string) (io.ReadCloser, error) {
	f, openErr := os.Open(fullPath)
	if openErr != nil {
		return nil, fmt.Errorf("localfs: download %q: %w", key, mapNotFound(openErr))
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
	srcPath, destPath, resolveErr := s.resolveArchiveKeys(srcKey, destKey)
	if resolveErr != nil {
		return resolveErr
	}
	if verifyErr := verifyArchiveSource(srcPath, destPath, srcKey, destKey); verifyErr != nil {
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

func (s *Store) resolveArchiveKeys(srcKey, destKey string) (string, string, error) {
	if srcKey == destKey {
		return "", "", ErrSameSourceAndDestination
	}
	srcPath, srcErr := s.resolvePath(srcKey)
	if srcErr != nil {
		return "", "", srcErr
	}
	destPath, destErr := s.resolvePath(destKey)
	if destErr != nil {
		return "", "", destErr
	}
	return srcPath, destPath, nil
}

// verifyArchiveSource validates that source exists, is a regular file, and does not collide with destPath.
func verifyArchiveSource(srcPath, destPath, srcKey, destKey string) error {
	srcFi, err := statRegularFile(srcPath)
	if err != nil {
		return archiveError(srcKey, destKey, err)
	}
	if isSameFile(srcFi, srcPath, destPath) {
		return ErrSameSourceAndDestination
	}
	return nil
}

// statRegularFile stats path and ensures it is a regular file.
func statRegularFile(path string) (os.FileInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, mapNotFound(err)
	}
	if !fi.Mode().IsRegular() {
		return nil, fs.ErrInvalid
	}
	return fi, nil
}

func archiveError(srcKey, destKey string, err error) error {
	return fmt.Errorf("localfs: archive %q -> %q: %w", srcKey, destKey, err)
}

// isSameFile reports whether srcPath and destPath point to the same file.
func isSameFile(srcFi os.FileInfo, srcPath, destPath string) bool {
	if srcPath == destPath {
		return true
	}
	destFi, err := os.Stat(destPath)
	return err == nil && os.SameFile(srcFi, destFi)
}

// ensureParentDir ensures the parent directory for targetPath exists with 0o750 permissions.
func ensureParentDir(targetPath, srcKey, destKey string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
		return archiveError(srcKey, destKey, sanitizeError(err))
	}
	return nil
}

// List returns the sorted keys of all regular files located under prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dirPath, cleanPrefix, err := s.resolvePrefixPath(prefix)
	if err != nil {
		return nil, err
	}
	entries, readErr := os.ReadDir(dirPath)
	if readErr != nil {
		return nil, fmt.Errorf("localfs: list %q: %w", prefix, sanitizeError(readErr))
	}
	return collectFileKeys(entries, cleanPrefix), nil
}

// collectFileKeys filters out directories, formats key paths, and returns sorted keys.
func collectFileKeys(entries []os.DirEntry, cleanPrefix string) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			keys = append(keys, formatListKey(cleanPrefix, entry.Name()))
		}
	}
	sort.Strings(keys)
	return keys
}

// resolvePrefixPath resolves the target directory path and normalized prefix for List.
func (s *Store) resolvePrefixPath(prefix string) (string, string, error) {
	if prefix == "" || prefix == "." {
		return s.root, "", nil
	}
	resolved, err := s.resolvePath(prefix)
	if err != nil {
		return "", "", err
	}
	return resolved, path.Clean(prefix), nil
}

// formatListKey joins the normalized prefix and file name.
func formatListKey(cleanPrefix, entryName string) string {
	if cleanPrefix == "" {
		return entryName
	}
	return path.Join(cleanPrefix, entryName)
}

func (s *Store) copyAndRemove(ctx context.Context, srcPath, destPath, srcKey, destKey string) error {
	if err := transferFile(ctx, srcPath, destPath); err != nil {
		return archiveError(srcKey, destKey, err)
	}
	return nil
}

// transferFile copies srcPath to destPath and deletes srcPath upon success.
func transferFile(ctx context.Context, srcPath, destPath string) error {
	srcFile, openErr := os.Open(srcPath)
	if openErr != nil {
		return mapNotFound(openErr)
	}
	defer srcFile.Close()

	if copyErr := copyToFile(ctx, srcFile, destPath); copyErr != nil {
		return sanitizeError(copyErr)
	}
	_ = srcFile.Close()

	if remErr := os.Remove(srcPath); remErr != nil {
		return sanitizeError(remErr)
	}
	return nil
}

// copyToFile creates destPath with 0o600 permissions and copies src to it, cleaning up on error.
func copyToFile(ctx context.Context, src io.Reader, destPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(destFile, src)
	closeErr := destFile.Close()
	if copyErr != nil || closeErr != nil || ctx.Err() != nil {
		_ = os.Remove(destPath)
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return ctx.Err()
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
	return err == nil && relPath != "." && relPath != ".." &&
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

// mapNotFound maps fs.ErrNotExist to port.ErrObjectNotFound and sanitizes other errors.
func mapNotFound(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return port.ErrObjectNotFound
	}
	return sanitizeError(err)
}
