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

func assertFileContent(t *testing.T, path string, want []byte, perm os.FileMode) {
	t.Helper()
	if fi, err := os.Stat(path); err != nil || (perm != 0 && fi.Mode().Perm() != perm) {
		t.Errorf("%s stat/perm error: %v", path, err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != string(want) {
		t.Fatalf("content mismatch for %s: %v", path, err)
	}
}

func TestNew_RequiresExistingDirectory(t *testing.T) {
	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "file.txt")
	writeFile(t, filePath, []byte("data"))
	missingDir := filepath.Join(tmp, "missing_subdir")

	for _, tc := range []struct {
		name, root string
		want       error
	}{
		{"empty root", "", ErrMissingRoot},
		{"whitespace root", "   ", ErrMissingRoot},
		{"missing directory", missingDir, fs.ErrNotExist},
		{"file as root", filePath, fs.ErrInvalid},
	} {
		store, err := New(tc.root)
		if err == nil || !errors.Is(err, tc.want) || store != nil {
			t.Errorf("New(%q) got store=%v, err=%v, want %v", tc.root, store, err, tc.want)
		}
		if tc.root == missingDir && strings.Contains(err.Error(), tmp) {
			t.Errorf("New leaked root path: %s", err.Error())
		}
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
	if b, readErr := io.ReadAll(rc); readErr != nil || string(b) != string(content) {
		t.Fatalf("read mismatch: %v, got %q", readErr, string(b))
	}
	if _, missErr := store.DownloadStream(ctx, "missing.csv"); !errors.Is(missErr, port.ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got: %v", missErr)
	}
	if rcDir, dirErr := store.DownloadStream(ctx, "subdir"); rcDir != nil || !errors.Is(dirErr, fs.ErrInvalid) {
		if rcDir != nil {
			_ = rcDir.Close()
		}
		t.Errorf("expected fs.ErrInvalid for dir stream, got rc=%v, err=%v", rcDir, dirErr)
	}
}

func TestKeys_RejectTraversalAndInvalid(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	for _, key := range []string{"../s", "/etc/p", "a/../../x", "..", "", ".", "\\", "/", "f/..", "f/.", "f\x00n"} {
		if _, err := store.DownloadStream(ctx, key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("DownloadStream(%q) got %v, want ErrInvalidKey", key, err)
		}
		if err := store.ArchiveFile(ctx, key, "dest.csv"); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ArchiveFile(src=%q) got %v, want ErrInvalidKey", key, err)
		}
		if err := store.ArchiveFile(ctx, "src.csv", key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ArchiveFile(dest=%q) got %v, want ErrInvalidKey", key, err)
		}
		if key != "" && key != "." {
			if _, err := store.List(ctx, key); !errors.Is(err, ErrInvalidKey) {
				t.Errorf("List(%q) got %v, want ErrInvalidKey", key, err)
			}
		}
	}
}

func TestArchiveFile_Archiving(t *testing.T) {
	root, store := newTestStore(t)
	content := []byte("date,amount\n2026-01-01,10.00\n")
	ctx := context.Background()

	// 1. Atomic rename creates dir with 0750
	srcFile := filepath.Join(root, "unprocessed", "statement.csv")
	mkdir(t, filepath.Dir(srcFile))
	writeFile(t, srcFile, content)
	if err := store.ArchiveFile(ctx, "unprocessed/statement.csv", "processed/sub/statement.csv"); err != nil {
		t.Fatalf("ArchiveFile failed: %v", err)
	}
	destPath := filepath.Join(root, "processed", "sub", "statement.csv")
	assertFileContent(t, destPath, content, 0)
	if fi, err := os.Stat(filepath.Dir(destPath)); err != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("dest dir perm = %o, want 0750", fi.Mode().Perm())
	}

	// 2. Cross-device fallback copies with 0600
	fallbackSrc := filepath.Join(root, "src.csv")
	writeFile(t, fallbackSrc, content)
	store.rename = func(_, _ string) error { return errors.New("simulated rename failure") }
	if err := store.ArchiveFile(ctx, "src.csv", "processed/dest.csv"); err != nil {
		t.Fatalf("ArchiveFile fallback failed: %v", err)
	}
	assertFileContent(t, filepath.Join(root, "processed", "dest.csv"), content, 0o600)
	for _, p := range []string{srcFile, fallbackSrc} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s removed: %v", p, err)
		}
	}
}

func TestArchiveFile_SameResolvedPath(t *testing.T) {
	root, store := newTestStore(t)
	content := []byte("untruncated record")
	mkdir(t, filepath.Join(root, "sub"))
	srcPath := filepath.Join(root, "sub", "file.csv")
	writeFile(t, srcPath, content)
	_ = os.Link(srcPath, filepath.Join(root, "sub", "linked.csv"))

	ctx := context.Background()
	for _, tc := range []struct{ src, dest string }{
		{"sub/file.csv", "sub/file.csv"},
		{"sub/file.csv", "sub//file.csv"},
		{"sub/file.csv", "sub/linked.csv"},
	} {
		if err := store.ArchiveFile(ctx, tc.src, tc.dest); !errors.Is(err, ErrSameSourceAndDestination) {
			t.Errorf("ArchiveFile(%q, %q) got %v, want ErrSameSourceAndDestination", tc.src, tc.dest, err)
		}
	}
	assertFileContent(t, srcPath, content, 0)
}

func TestArchiveFile_ErrorScenarios(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "src.csv"), []byte("data"))
	writeFile(t, filepath.Join(root, "conflict"), []byte("blocker"))
	mkdir(t, filepath.Join(root, "dest_dir"))
	mkdir(t, filepath.Join(root, "src_dir"))
	store.rename = func(_, _ string) error { return errors.New("simulated rename failure") }
	ctx := context.Background()

	for _, tc := range []struct {
		src, dest string
		wantErr   error
	}{
		{"missing.csv", "orphaned/dest.csv", port.ErrObjectNotFound},
		{"src.csv", "conflict/dest.csv", nil},
		{"src.csv", "dest_dir", nil},
		{"src_dir", "dest.csv", fs.ErrInvalid},
	} {
		if err := store.ArchiveFile(ctx, tc.src, tc.dest); err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
			t.Errorf("ArchiveFile(%q, %q) got %v, want wrap: %v", tc.src, tc.dest, err, tc.wantErr)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "orphaned")); !os.IsNotExist(err) {
		t.Error("orphaned directory should not have been created")
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
	assertFileContent(t, srcFile, content, 0)
	if _, err := os.Stat(filepath.Join(root, "dest_folder", "dest.csv")); !os.IsNotExist(err) {
		t.Errorf("destination file should be removed on cancellation")
	}
}

func TestList_Scenarios(t *testing.T) {
	root, store := newTestStore(t)
	mkdir(t, filepath.Join(root, "unprocessed", "nested"))
	mkdir(t, filepath.Join(root, "empty_dir"))
	for _, p := range []string{"root.csv", "unprocessed/b.csv", "unprocessed/a.csv", "unprocessed/nested/c.csv"} {
		writeFile(t, filepath.Join(root, filepath.FromSlash(p)), []byte("x"))
	}
	ctx := context.Background()

	if keys, err := store.List(ctx, "unprocessed"); err != nil || !reflect.DeepEqual(keys, []string{"unprocessed/a.csv", "unprocessed/b.csv"}) {
		t.Errorf("List(unprocessed) = %v, err: %v", keys, err)
	}
	emptyKeys, err1 := store.List(ctx, "")
	dotKeys, err2 := store.List(ctx, ".")
	if err1 != nil || err2 != nil || !reflect.DeepEqual(emptyKeys, dotKeys) {
		t.Errorf("List dot parity mismatch: empty=%v, dot=%v", emptyKeys, dotKeys)
	}
	if keys, err := store.List(ctx, "empty_dir"); err != nil || keys == nil || len(keys) != 0 {
		t.Errorf("List(empty_dir) = %#v, err: %v", keys, err)
	}
	if _, err := store.List(ctx, "root.csv"); err == nil {
		t.Error("expected error when prefix is file")
	}
	if _, err := store.List(ctx, "missing_prefix"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got: %v", err)
	}
}

func TestMethods_CancelledContext(t *testing.T) {
	root, store := newTestStore(t)
	writeFile(t, filepath.Join(root, "file.csv"), []byte("content"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, dlErr := store.DownloadStream(ctx, "file.csv")
	_, listErr := store.List(ctx, "")
	for op, err := range map[string]error{"Download": dlErr, "Archive": store.ArchiveFile(ctx, "file.csv", "dest.csv"), "List": listErr} {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s got %v, want context.Canceled", op, err)
		}
	}
}

func TestErrors_DoNotLeakRoot(t *testing.T) {
	root, store := newTestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		op  func() error
		key string
	}{
		{func() error { _, err := store.DownloadStream(ctx, "not_found.csv"); return err }, "not_found.csv"},
		{func() error { return store.ArchiveFile(ctx, "missing.csv", "dest.csv") }, "missing.csv"},
		{func() error { _, err := store.List(ctx, "missing_folder"); return err }, "missing_folder"},
	} {
		if err := tc.op(); err == nil || strings.Contains(err.Error(), root) || !strings.Contains(err.Error(), tc.key) {
			t.Fatalf("failed validation: err=%v, root=%s, key=%s", err, root, tc.key)
		}
	}

	linkErr := &os.LinkError{Op: "rename", Old: "/confidential/old", New: "/confidential/new", Err: fs.ErrPermission}
	if sanitized := sanitizeError(linkErr); !errors.Is(sanitized, fs.ErrPermission) || strings.Contains(sanitized.Error(), "confidential") {
		t.Errorf("sanitizeError failed: %v", sanitized)
	}
}
