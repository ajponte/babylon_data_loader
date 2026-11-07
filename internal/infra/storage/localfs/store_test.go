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
	missingDir := filepath.Join(tmp, "missing_subdir")

	tests := []struct {
		name        string
		root        string
		expectedErr error
	}{
		{name: "empty root", root: "", expectedErr: ErrMissingRoot},
		{name: "whitespace root", root: "   ", expectedErr: ErrMissingRoot},
		{name: "missing directory", root: missingDir, expectedErr: fs.ErrNotExist},
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
			if tc.root == missingDir && strings.Contains(err.Error(), tmp) {
				t.Errorf("New leaked root path: %s", err.Error())
			}
		})
	}

	if store, err := New(tmp); err != nil || store == nil {
		t.Fatalf("expected valid store, got %v (err: %v)", store, err)
	}
}

func TestDownloadStream_Scenarios(t *testing.T) {
	root, store := newTestStore(t)
	content := []byte("header1,header2\nval1,val2\n")
	writeFile(t, filepath.Join(root, "data.csv"), content)
	mkdir(t, filepath.Join(root, "subdir"))
	ctx := context.Background()

	rc, dlErr := store.DownloadStream(ctx, "data.csv")
	if dlErr != nil {
		t.Fatalf("unexpected error: %v", dlErr)
	}
	defer rc.Close()
	readBytes, readErr := io.ReadAll(rc)
	if readErr != nil || string(readBytes) != string(content) {
		t.Fatalf("read content mismatch or error: %v", readErr)
	}

	if _, missingErr := store.DownloadStream(ctx, "missing.csv"); !errors.Is(missingErr, port.ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got: %v", missingErr)
	}

	if rcDir, dirErr := store.DownloadStream(ctx, "subdir"); rcDir != nil || !errors.Is(dirErr, fs.ErrInvalid) {
		if rcDir != nil {
			_ = rcDir.Close()
		}
		t.Errorf("expected fs.ErrInvalid for directory stream, got rc=%v, err=%v", rcDir, dirErr)
	}
}

func TestKeys_RejectTraversalAndInvalid(t *testing.T) {
	_, store := newTestStore(t)
	invalidKeys := []string{
		"../secret.txt", "/etc/passwd", "a/../../x", "..", "", ".", "\\", "/", "folder/..", "folder/.", "file\x00name.csv",
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
	srcFile := filepath.Join(root, "unprocessed", "statement.csv")
	mkdir(t, filepath.Dir(srcFile))
	content := []byte("date,amount\n2026-01-01,10.00\n")
	writeFile(t, srcFile, content)

	if err := store.ArchiveFile(context.Background(), "unprocessed/statement.csv", "processed/sub/statement.csv"); err != nil {
		t.Fatalf("ArchiveFile failed: %v", err)
	}

	destPath := filepath.Join(root, "processed", "sub", "statement.csv")
	destContent, readErr := os.ReadFile(destPath)
	if readErr != nil || string(destContent) != string(content) {
		t.Fatalf("dest read error or content mismatch: %v", readErr)
	}
	if _, statErr := os.Stat(srcFile); !os.IsNotExist(statErr) {
		t.Errorf("expected src file removed, stat err: %v", statErr)
	}
	if fi, statDirErr := os.Stat(filepath.Dir(destPath)); statDirErr != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("dest dir perm = %o, want 0750 (err: %v)", fi.Mode().Perm(), statDirErr)
	}
}

func TestArchiveFile_SameResolvedPath(t *testing.T) {
	root, store := newTestStore(t)
	content := []byte("untruncated record")
	srcPath := filepath.Join(root, "file.csv")
	writeFile(t, srcPath, content)
	linkPath := filepath.Join(root, "linked.csv")
	_ = os.Link(srcPath, linkPath)

	mkdir(t, filepath.Join(root, "sub"))
	writeFile(t, filepath.Join(root, "sub", "file.csv"), content)

	ctx := context.Background()
	for _, tc := range []struct{ src, dest string }{
		{"file.csv", "file.csv"},
		{"sub/file.csv", "sub//file.csv"},
		{"file.csv", "linked.csv"},
	} {
		if err := store.ArchiveFile(ctx, tc.src, tc.dest); !errors.Is(err, ErrSameSourceAndDestination) {
			t.Errorf("ArchiveFile(%q, %q) got %v, want ErrSameSourceAndDestination", tc.src, tc.dest, err)
		}
	}

	srcBytes, readErr := os.ReadFile(srcPath)
	if readErr != nil || string(srcBytes) != string(content) {
		t.Fatalf("source file was truncated or corrupted: %v", readErr)
	}
}

func TestArchiveFile_CopyFallback(t *testing.T) {
	root, store := newTestStore(t)
	srcPath := filepath.Join(root, "src.csv")
	content := []byte("record1,record2\n")
	writeFile(t, srcPath, content)
	store.rename = func(_, _ string) error { return errors.New("simulated cross-device link failure") }

	if err := store.ArchiveFile(context.Background(), "src.csv", "processed/dest.csv"); err != nil {
		t.Fatalf("ArchiveFile fallback failed: %v", err)
	}

	destPath := filepath.Join(root, "processed", "dest.csv")
	destBytes, readErr := os.ReadFile(destPath)
	if readErr != nil || string(destBytes) != string(content) {
		t.Fatalf("dest read error or content mismatch: %v", readErr)
	}
	if fi, statErr := os.Stat(destPath); statErr != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("dest file perm = %o, want 0600 (err: %v)", fi.Mode().Perm(), statErr)
	}
	if _, statSrcErr := os.Stat(srcPath); !os.IsNotExist(statSrcErr) {
		t.Errorf("expected source removed, got err: %v", statSrcErr)
	}
}

func TestArchiveFile_ErrorScenarios(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "src.csv"), []byte("data"))
	writeFile(t, filepath.Join(root, "conflict"), []byte("blocker"))
	mkdir(t, filepath.Join(root, "dest_dir"))
	mkdir(t, filepath.Join(root, "src_dir"))
	store.rename = func(_, _ string) error { return errors.New("simulated rename failure") }

	ctx := context.Background()
	// Missing source does not create dest dir
	if err := store.ArchiveFile(ctx, "missing.csv", "orphaned/dest.csv"); !errors.Is(err, port.ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "orphaned")); !os.IsNotExist(statErr) {
		t.Error("orphaned directory should not have been created")
	}

	// Conflict with existing file as dir, existing dir as dest, and dir as source
	if err := store.ArchiveFile(ctx, "src.csv", "conflict/dest.csv"); err == nil {
		t.Error("expected error on dest dir conflict")
	}
	if err := store.ArchiveFile(ctx, "src.csv", "dest_dir"); err == nil {
		t.Error("expected error when dest is dir")
	}
	if err := store.ArchiveFile(ctx, "src_dir", "dest.csv"); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("expected fs.ErrInvalid for dir src, got: %v", err)
	}
}

func TestArchiveFile_ContextCanceledFallback(t *testing.T) {
	root, store := newTestStore(t)
	srcFile := filepath.Join(root, "source.csv")
	content := []byte("valuable records")
	writeFile(t, srcFile, content)

	ctx, cancel := context.WithCancel(context.Background())
	store.rename = func(_, _ string) error {
		cancel()
		return errors.New("simulated rename failure")
	}

	if err := store.ArchiveFile(ctx, "source.csv", "dest_folder/dest.csv"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if srcBytes, err := os.ReadFile(srcFile); err != nil || string(srcBytes) != string(content) {
		t.Errorf("source file was corrupted or deleted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "dest_folder", "dest.csv")); !os.IsNotExist(err) {
		t.Errorf("destination file should be removed on cancellation")
	}
}

func TestList_Scenarios(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "root.csv"), []byte("root"))
	mkdir(t, filepath.Join(root, "unprocessed", "nested"))
	writeFile(t, filepath.Join(root, "unprocessed", "b.csv"), []byte("b"))
	writeFile(t, filepath.Join(root, "unprocessed", "a.csv"), []byte("a"))
	writeFile(t, filepath.Join(root, "unprocessed", "nested", "c.csv"), []byte("c"))
	mkdir(t, filepath.Join(root, "empty_dir"))
	ctx := context.Background()

	keys, listErr := store.List(ctx, "unprocessed")
	if listErr != nil || !reflect.DeepEqual(keys, []string{"unprocessed/a.csv", "unprocessed/b.csv"}) {
		t.Errorf("List(unprocessed) = %v, err: %v", keys, listErr)
	}

	emptyKeys, err1 := store.List(ctx, "")
	dotKeys, err2 := store.List(ctx, ".")
	if err1 != nil || err2 != nil || !reflect.DeepEqual(emptyKeys, dotKeys) {
		t.Errorf("List dot parity mismatch: empty=%v, dot=%v", emptyKeys, dotKeys)
	}

	if emptyDirKeys, emptyErr := store.List(ctx, "empty_dir"); emptyErr != nil || emptyDirKeys == nil || len(emptyDirKeys) != 0 {
		t.Errorf("List(empty_dir) = %#v, err: %v", emptyDirKeys, emptyErr)
	}
	if _, fileErr := store.List(ctx, "root.csv"); fileErr == nil {
		t.Error("expected error when prefix is file")
	}
	if _, missErr := store.List(ctx, "missing_prefix"); !errors.Is(missErr, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got: %v", missErr)
	}
}

func TestMethods_CancelledContext(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "file.csv"), []byte("content"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.DownloadStream(ctx, "file.csv"); !errors.Is(err, context.Canceled) {
		t.Errorf("DownloadStream got %v, want context.Canceled", err)
	}
	if err := store.ArchiveFile(ctx, "file.csv", "dest.csv"); !errors.Is(err, context.Canceled) {
		t.Errorf("ArchiveFile got %v, want context.Canceled", err)
	}
	if _, err := store.List(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("List got %v, want context.Canceled", err)
	}
}

func TestErrors_DoNotLeakRoot(t *testing.T) {
	root, store := newTestStore(t)
	ctx := context.Background()

	cases := []struct {
		name string
		op   func() error
		key  string
	}{
		{"download not found", func() error { _, err := store.DownloadStream(ctx, "not_found.csv"); return err }, "not_found.csv"},
		{"archive source not found", func() error { return store.ArchiveFile(ctx, "missing.csv", "dest.csv") }, "missing.csv"},
		{"list missing prefix", func() error { _, err := store.List(ctx, "missing_folder"); return err }, "missing_folder"},
	}

	for _, tc := range cases {
		err := tc.op()
		if err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
		if strings.Contains(err.Error(), root) {
			t.Errorf("%s: leaked root: %s", tc.name, err.Error())
		}
		if !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s: should contain key %q: %s", tc.name, tc.key, err.Error())
		}
	}

	linkErr := &os.LinkError{Op: "rename", Old: "/confidential/old", New: "/confidential/new", Err: fs.ErrPermission}
	if sanitized := sanitizeError(linkErr); !errors.Is(sanitized, fs.ErrPermission) || strings.Contains(sanitized.Error(), "confidential") {
		t.Errorf("sanitizeError failed: %v", sanitized)
	}
}
