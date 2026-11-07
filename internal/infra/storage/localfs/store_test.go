package localfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"babylon/dataloader/internal/domain/port"
)

func newTestStore(t *testing.T) (string, *Store) {
	t.Helper()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	return root, store
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("failed to write file %s: %v", path, err)
	}
}

func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o750); err != nil {
		t.Fatalf("failed to create dir %s: %v", path, err)
	}
}

func TestNew_RequiresExistingDirectory(t *testing.T) {
	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "file.txt")
	writeFile(t, filePath, []byte("data"))

	tests := []struct {
		name        string
		root        string
		expectedErr error
	}{
		{name: "empty root", root: "", expectedErr: ErrMissingRoot},
		{name: "whitespace root", root: "   ", expectedErr: ErrMissingRoot},
		{name: "missing directory", root: filepath.Join(tmp, "missing"), expectedErr: fs.ErrNotExist},
		{name: "file as root", root: filePath, expectedErr: fs.ErrInvalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := New(tc.root)
			if err == nil || !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
			if store != nil {
				t.Errorf("expected nil store on error, got %v", store)
			}
		})
	}

	t.Run("valid directory", func(t *testing.T) {
		store, err := New(tmp)
		if err != nil || store == nil {
			t.Fatalf("expected valid store, got %v (err: %v)", store, err)
		}
	})
}

func TestDownloadStream_ReadsFile(t *testing.T) {
	root, store := newTestStore(t)
	content := []byte("header1,header2\nval1,val2\n")
	writeFile(t, filepath.Join(root, "data.csv"), content)

	rc, dlErr := store.DownloadStream(context.Background(), "data.csv")
	if dlErr != nil {
		t.Fatalf("unexpected error: %v", dlErr)
	}
	defer rc.Close()

	readBytes, readErr := io.ReadAll(rc)
	if readErr != nil {
		t.Fatalf("failed to read stream: %v", readErr)
	}
	if string(readBytes) != string(content) {
		t.Errorf("read content = %q, want %q", string(readBytes), string(content))
	}
}

func TestDownloadStream_NotFound(t *testing.T) {
	_, store := newTestStore(t)
	_, err := store.DownloadStream(context.Background(), "missing.csv")
	if !errors.Is(err, port.ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got: %v", err)
	}
}

func TestKeys_RejectTraversal(t *testing.T) {
	_, store := newTestStore(t)
	invalidKeys := []string{
		"../secret.txt", "/etc/passwd", "a/../../x", "..", "", ".", "\\", "/", "folder/..", "folder/.",
	}

	ctx := context.Background()
	for _, key := range invalidKeys {
		if _, dlErr := store.DownloadStream(ctx, key); !errors.Is(dlErr, ErrInvalidKey) {
			t.Errorf("DownloadStream(%q) expected ErrInvalidKey, got: %v", key, dlErr)
		}
		if archErr := store.ArchiveFile(ctx, key, "dest.csv"); !errors.Is(archErr, ErrInvalidKey) {
			t.Errorf("ArchiveFile(src=%q) expected ErrInvalidKey, got: %v", key, archErr)
		}
		if archErr := store.ArchiveFile(ctx, "src.csv", key); !errors.Is(archErr, ErrInvalidKey) {
			t.Errorf("ArchiveFile(dest=%q) expected ErrInvalidKey, got: %v", key, archErr)
		}
		if key != "" && key != "." {
			if _, listErr := store.List(ctx, key); !errors.Is(listErr, ErrInvalidKey) {
				t.Errorf("List(%q) expected ErrInvalidKey, got: %v", key, listErr)
			}
		}
	}
}

func TestArchiveFile_RenamesAndCreatesDir(t *testing.T) {
	root, store := newTestStore(t)
	unprocessedDir := filepath.Join(root, "unprocessed")
	mkdir(t, unprocessedDir)

	srcFile := filepath.Join(unprocessedDir, "statement.csv")
	content := []byte("date,amount\n2026-01-01,10.00\n")
	writeFile(t, srcFile, content)

	if archErr := store.ArchiveFile(context.Background(), "unprocessed/statement.csv", "processed/sub/statement.csv"); archErr != nil {
		t.Fatalf("ArchiveFile failed: %v", archErr)
	}

	destPath := filepath.Join(root, "processed", "sub", "statement.csv")
	destContent, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("failed to read dest file: %v", readErr)
	}
	if string(destContent) != string(content) {
		t.Errorf("dest content = %q, want %q", string(destContent), string(content))
	}
	if _, statErr := os.Stat(srcFile); !os.IsNotExist(statErr) {
		t.Errorf("expected src file to be removed, stat err: %v", statErr)
	}

	dirInfo, statDirErr := os.Stat(filepath.Dir(destPath))
	if statDirErr != nil || dirInfo.Mode().Perm() != 0o750 {
		t.Errorf("dest dir perm = %o, want 0750 (stat err: %v)", dirInfo.Mode().Perm(), statDirErr)
	}
}

func TestArchiveFile_CopyFallback(t *testing.T) {
	root, store := newTestStore(t)
	srcPath := filepath.Join(root, "src.csv")
	content := []byte("record1,record2\n")
	writeFile(t, srcPath, content)

	store.rename = func(_, _ string) error {
		return errors.New("simulated cross-device link failure")
	}

	if archErr := store.ArchiveFile(context.Background(), "src.csv", "processed/dest.csv"); archErr != nil {
		t.Fatalf("ArchiveFile fallback failed: %v", archErr)
	}

	destPath := filepath.Join(root, "processed", "dest.csv")
	destBytes, readErr := os.ReadFile(destPath)
	if readErr != nil || string(destBytes) != string(content) {
		t.Fatalf("dest read error or content mismatch: %v", readErr)
	}

	destInfo, statErr := os.Stat(destPath)
	if statErr != nil || destInfo.Mode().Perm() != 0o600 {
		t.Errorf("dest file perm = %o, want 0600 (stat err: %v)", destInfo.Mode().Perm(), statErr)
	}
	if _, statSrcErr := os.Stat(srcPath); !os.IsNotExist(statSrcErr) {
		t.Errorf("expected source to be removed, got err: %v", statSrcErr)
	}
}

func TestArchiveFile_CopyFailureCleansDest(t *testing.T) {
	root, store := newTestStore(t)
	srcDir := filepath.Join(root, "source_dir")
	mkdir(t, srcDir)

	store.rename = func(_, _ string) error {
		return errors.New("simulated rename failure")
	}

	if archErr := store.ArchiveFile(context.Background(), "source_dir", "dest.csv"); archErr == nil {
		t.Fatal("expected ArchiveFile to fail when copying a directory, got nil")
	}

	destPath := filepath.Join(root, "dest.csv")
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Errorf("expected dest file to be cleaned up upon failure, stat: %v", statErr)
	}
	if fi, statDirErr := os.Stat(srcDir); statDirErr != nil || !fi.IsDir() {
		t.Errorf("expected source_dir to remain intact, stat: %v", statDirErr)
	}
}

func TestArchiveFile_SameKey(t *testing.T) {
	_, store := newTestStore(t)
	err := store.ArchiveFile(context.Background(), "file.csv", "file.csv")
	if !errors.Is(err, ErrSameSourceAndDestination) {
		t.Errorf("expected ErrSameSourceAndDestination, got: %v", err)
	}
}

func TestArchiveFile_MkdirAllError(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "conflict"), []byte("blocker"))
	writeFile(t, filepath.Join(root, "src.csv"), []byte("data"))

	if err := store.ArchiveFile(context.Background(), "src.csv", "conflict/dest.csv"); err == nil {
		t.Fatal("expected ArchiveFile to fail when destination dir conflicts with file")
	}
}

func TestArchiveFile_CopyFallbackErrors(t *testing.T) {
	root, store := newTestStore(t)
	store.rename = func(_, _ string) error {
		return errors.New("simulated rename failure")
	}

	if err := store.ArchiveFile(context.Background(), "missing_src.csv", "dest.csv"); !errors.Is(err, port.ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound for missing source, got: %v", err)
	}

	writeFile(t, filepath.Join(root, "src2.csv"), []byte("data"))
	destDir := filepath.Join(root, "dest_dir")
	mkdir(t, destDir)

	if err := store.ArchiveFile(context.Background(), "src2.csv", "dest_dir"); err == nil {
		t.Fatal("expected ArchiveFile to fail when destination is an existing directory")
	}
}

func TestList_FilesOnlySorted(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "root.csv"), []byte("root"))

	unprocessedDir := filepath.Join(root, "unprocessed")
	mkdir(t, unprocessedDir)
	writeFile(t, filepath.Join(unprocessedDir, "b.csv"), []byte("b"))
	writeFile(t, filepath.Join(unprocessedDir, "a.csv"), []byte("a"))

	nestedDir := filepath.Join(unprocessedDir, "nested")
	mkdir(t, nestedDir)
	writeFile(t, filepath.Join(nestedDir, "c.csv"), []byte("c"))

	keys, listErr := store.List(context.Background(), "unprocessed")
	if listErr != nil {
		t.Fatalf("List failed: %v", listErr)
	}
	expected := []string{"unprocessed/a.csv", "unprocessed/b.csv"}
	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("List returned %v, want %v", keys, expected)
	}

	rootKeys, listRootErr := store.List(context.Background(), "")
	if listRootErr != nil {
		t.Fatalf("List with empty prefix failed: %v", listRootErr)
	}
	expectedRoot := []string{"root.csv"}
	if !reflect.DeepEqual(rootKeys, expectedRoot) {
		t.Errorf("List(\"\") returned %v, want %v", rootKeys, expectedRoot)
	}
}

func TestList_MissingPrefix(t *testing.T) {
	_, store := newTestStore(t)
	_, err := store.List(context.Background(), "missing_prefix")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got: %v", err)
	}
}

func TestMethods_CancelledContext(t *testing.T) {
	root, store := newTestStore(t)
	testFile := filepath.Join(root, "file.csv")
	writeFile(t, testFile, []byte("content"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, dlErr := store.DownloadStream(ctx, "file.csv"); !errors.Is(dlErr, context.Canceled) {
		t.Errorf("DownloadStream expected context.Canceled, got: %v", dlErr)
	}
	if archErr := store.ArchiveFile(ctx, "file.csv", "dest.csv"); !errors.Is(archErr, context.Canceled) {
		t.Errorf("ArchiveFile expected context.Canceled, got: %v", archErr)
	}
	if _, listErr := store.List(ctx, ""); !errors.Is(listErr, context.Canceled) {
		t.Errorf("List expected context.Canceled, got: %v", listErr)
	}
	if _, statErr := os.Stat(testFile); statErr != nil {
		t.Errorf("file should not be modified, stat err: %v", statErr)
	}
}

func TestErrors_DoNotLeakRoot(t *testing.T) {
	root, store := newTestStore(t)
	ctx := context.Background()

	errorChecks := []struct {
		name string
		op   func() error
		key  string
	}{
		{
			name: "download not found",
			op: func() error {
				_, dlErr := store.DownloadStream(ctx, "not_found.csv")
				return dlErr
			},
			key: "not_found.csv",
		},
		{
			name: "archive source not found",
			op: func() error {
				store.rename = func(_, _ string) error { return errors.New("force fallback") }
				return store.ArchiveFile(ctx, "missing_source.csv", "dest.csv")
			},
			key: "missing_source.csv",
		},
		{
			name: "list missing prefix",
			op: func() error {
				_, listErr := store.List(ctx, "missing_folder")
				return listErr
			},
			key: "missing_folder",
		},
	}

	for _, tc := range errorChecks {
		t.Run(tc.name, func(t *testing.T) {
			opErr := tc.op()
			if opErr == nil {
				t.Fatalf("expected error, got nil")
			}
			errStr := opErr.Error()
			if strings.Contains(errStr, root) {
				t.Errorf("error leaked root directory %q: %s", root, errStr)
			}
			if !strings.Contains(errStr, tc.key) {
				t.Errorf("error should contain key %q: %s", tc.key, errStr)
			}
		})
	}
}
