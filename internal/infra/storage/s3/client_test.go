package s3_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/storage/s3"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type optFn = func(*awss3.Options)

type fakeObjectAPI struct {
	mu sync.Mutex

	getObjectFn    func(ctx context.Context, in *awss3.GetObjectInput, opts ...optFn) (*awss3.GetObjectOutput, error)
	copyObjectFn   func(ctx context.Context, in *awss3.CopyObjectInput, opts ...optFn) (*awss3.CopyObjectOutput, error)
	deleteObjectFn func(ctx context.Context, in *awss3.DeleteObjectInput, opts ...optFn) (*awss3.DeleteObjectOutput, error)

	calls           []string
	lastGetCtx      context.Context
	lastGetInput    *awss3.GetObjectInput
	lastCopyInput   *awss3.CopyObjectInput
	lastDeleteInput *awss3.DeleteObjectInput
}

func (f *fakeObjectAPI) GetObject(ctx context.Context, in *awss3.GetObjectInput, opts ...optFn) (*awss3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "GetObject")
	f.lastGetCtx = ctx
	f.lastGetInput = in
	if f.getObjectFn != nil {
		return f.getObjectFn(ctx, in, opts...)
	}
	return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(""))}, nil
}

func (f *fakeObjectAPI) CopyObject(ctx context.Context, in *awss3.CopyObjectInput, opts ...optFn) (*awss3.CopyObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CopyObject")
	f.lastCopyInput = in
	if f.copyObjectFn != nil {
		return f.copyObjectFn(ctx, in, opts...)
	}
	return &awss3.CopyObjectOutput{}, nil
}

func (f *fakeObjectAPI) DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, opts ...optFn) (*awss3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "DeleteObject")
	f.lastDeleteInput = in
	if f.deleteObjectFn != nil {
		return f.deleteObjectFn(ctx, in, opts...)
	}
	return &awss3.DeleteObjectOutput{}, nil
}

func (f *fakeObjectAPI) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type codedError struct {
	code string
	msg  string
}

func (e *codedError) Error() string     { return e.msg }
func (e *codedError) ErrorCode() string { return e.code }

type ctxKey string

func newTestClient(t *testing.T, api s3.ObjectAPI, bucket string) *s3.Client {
	t.Helper()
	c, err := s3.NewClient(api, bucket)
	if err != nil {
		t.Fatalf("unexpected NewClient error: %v", err)
	}
	return c
}

func assertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("expected error %v, got %v", want, got)
	}
}

func assertStringEqual(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("expected string %q, got %q", want, got)
	}
}

func TestNewClient_Validation(t *testing.T) {
	fake := &fakeObjectAPI{}
	tests := []struct {
		name      string
		api       s3.ObjectAPI
		bucket    string
		expectErr error
	}{
		{name: "nil API client", api: nil, bucket: "my-bucket", expectErr: s3.ErrNilAPI},
		{name: "empty bucket", api: fake, bucket: "", expectErr: s3.ErrMissingBucket},
		{name: "whitespace-only bucket", api: fake, bucket: "   \t\n  ", expectErr: s3.ErrMissingBucket},
		{name: "valid api and bucket", api: fake, bucket: "valid-bucket", expectErr: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, err := s3.NewClient(tc.api, tc.bucket)
			if tc.expectErr != nil {
				assertErrorIs(t, err, tc.expectErr)
				return
			}
			if err != nil || client == nil {
				t.Fatalf("expected valid client, got err: %v, client: %v", err, client)
			}
		})
	}
}

func TestDownloadStream_ReturnsBodyUnbuffered(t *testing.T) {
	ctx := context.Background()
	const payload = "statement-date,amount,memo\n2026-01-15,123.45,groceries"

	fake := &fakeObjectAPI{
		getObjectFn: func(_ context.Context, _ *awss3.GetObjectInput, _ ...optFn) (*awss3.GetObjectOutput, error) {
			return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(payload))}, nil
		},
	}
	client := newTestClient(t, fake, "test-bucket")

	rc, err := client.DownloadStream(ctx, "unprocessed/chase.csv")
	if err != nil {
		t.Fatalf("unexpected DownloadStream error: %v", err)
	}
	defer rc.Close()

	body, readErr := io.ReadAll(rc)
	if readErr != nil {
		t.Fatalf("unexpected ReadAll error: %v", readErr)
	}
	assertStringEqual(t, string(body), payload)

	if fake.lastGetInput == nil {
		t.Fatal("expected GetObjectInput to be captured")
	}
	assertStringEqual(t, *fake.lastGetInput.Bucket, "test-bucket")
	assertStringEqual(t, *fake.lastGetInput.Key, "unprocessed/chase.csv")
}

func TestDownloadStream_EmptyKey(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{"", "    ", "\t\n "} {
		fake := &fakeObjectAPI{}
		client := newTestClient(t, fake, "test-bucket")

		rc, err := client.DownloadStream(ctx, key)
		if rc != nil {
			_ = rc.Close()
		}
		assertErrorIs(t, err, s3.ErrMissingKey)
		if len(fake.Calls()) != 0 {
			t.Fatalf("expected 0 calls on empty key, got %d", len(fake.Calls()))
		}
	}
}

func TestDownloadStream_NoSuchKey(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		apiErr   error
		expectIs error
	}{
		{name: "types.NoSuchKey pointer", apiErr: &types.NoSuchKey{}, expectIs: port.ErrObjectNotFound},
		{name: "types.NotFound pointer", apiErr: &types.NotFound{}, expectIs: port.ErrObjectNotFound},
		{name: "coded error NoSuchKey", apiErr: &codedError{code: "NoSuchKey", msg: "missing"}, expectIs: port.ErrObjectNotFound},
		{name: "coded error NotFound", apiErr: &codedError{code: "NotFound", msg: "not found"}, expectIs: port.ErrObjectNotFound},
		{name: "arbitrary api error", apiErr: errors.New("i/o timeout"), expectIs: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeObjectAPI{
				getObjectFn: func(_ context.Context, _ *awss3.GetObjectInput, _ ...optFn) (*awss3.GetObjectOutput, error) {
					return nil, tc.apiErr
				},
			}
			client := newTestClient(t, fake, "test-bucket")

			rc, err := client.DownloadStream(ctx, "missing.csv")
			if rc != nil {
				_ = rc.Close()
			}
			if tc.expectIs != nil {
				assertErrorIs(t, err, tc.expectIs)
				return
			}
			if errors.Is(err, port.ErrObjectNotFound) || !strings.Contains(err.Error(), tc.apiErr.Error()) {
				t.Fatalf("unexpected error for %v: %v", tc.apiErr, err)
			}
		})
	}
}

func TestDownloadStream_ContextPropagated(t *testing.T) {
	key := ctxKey("request-id")
	ctx := context.WithValue(context.Background(), key, "trace-12345")

	fake := &fakeObjectAPI{}
	client := newTestClient(t, fake, "test-bucket")

	rc, err := client.DownloadStream(ctx, "file.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = rc.Close()

	if fake.lastGetCtx == nil {
		t.Fatal("expected lastGetCtx to be captured")
	}
	val, ok := fake.lastGetCtx.Value(key).(string)
	if !ok || val != "trace-12345" {
		t.Fatalf("expected propagated context value %q, got %q", "trace-12345", val)
	}
}

func TestArchiveFile_CopyThenDelete(t *testing.T) {
	ctx := context.Background()
	fake := &fakeObjectAPI{}
	client := newTestClient(t, fake, "archive-bucket")

	err := client.ArchiveFile(ctx, "unprocessed/report.csv", "processed/report.csv")
	if err != nil {
		t.Fatalf("unexpected ArchiveFile error: %v", err)
	}

	calls := fake.Calls()
	if len(calls) != 2 || calls[0] != "CopyObject" || calls[1] != "DeleteObject" {
		t.Fatalf("expected calls [CopyObject, DeleteObject], got %v", calls)
	}

	assertStringEqual(t, *fake.lastCopyInput.Bucket, "archive-bucket")
	assertStringEqual(t, *fake.lastCopyInput.Key, "processed/report.csv")
	assertStringEqual(t, *fake.lastCopyInput.CopySource, "archive-bucket/unprocessed/report.csv")

	assertStringEqual(t, *fake.lastDeleteInput.Bucket, "archive-bucket")
	assertStringEqual(t, *fake.lastDeleteInput.Key, "unprocessed/report.csv")
}

func TestArchiveFile_NoDeleteWhenCopyFails(t *testing.T) {
	ctx := context.Background()
	copyErr := errors.New("s3 copy internal failure")
	fake := &fakeObjectAPI{
		copyObjectFn: func(_ context.Context, _ *awss3.CopyObjectInput, _ ...optFn) (*awss3.CopyObjectOutput, error) {
			return nil, copyErr
		},
	}
	client := newTestClient(t, fake, "test-bucket")

	err := client.ArchiveFile(ctx, "src.csv", "dest.csv")
	if err == nil || !strings.Contains(err.Error(), copyErr.Error()) {
		t.Fatalf("expected error containing %q, got %v", copyErr.Error(), err)
	}

	calls := fake.Calls()
	if len(calls) != 1 || calls[0] != "CopyObject" {
		t.Fatalf("expected only CopyObject call and no DeleteObject, got %v", calls)
	}
}

func TestArchiveFile_DeleteFailureSurfaced(t *testing.T) {
	ctx := context.Background()
	deleteErr := errors.New("delete access denied")
	fake := &fakeObjectAPI{
		deleteObjectFn: func(_ context.Context, _ *awss3.DeleteObjectInput, _ ...optFn) (*awss3.DeleteObjectOutput, error) {
			return nil, deleteErr
		},
	}
	client := newTestClient(t, fake, "test-bucket")

	err := client.ArchiveFile(ctx, "src.csv", "dest.csv")
	if err == nil || !strings.Contains(err.Error(), deleteErr.Error()) {
		t.Fatalf("expected error containing %q, got %v", deleteErr.Error(), err)
	}

	calls := fake.Calls()
	if len(calls) != 2 || calls[0] != "CopyObject" || calls[1] != "DeleteObject" {
		t.Fatalf("expected both CopyObject and DeleteObject to be called, got %v", calls)
	}
}

func TestArchiveFile_CopySourceEscaping_LegacyParity(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name               string
		srcKey             string
		destKey            string
		expectedCopySource string
	}{
		{name: "standard key", srcKey: "unprocessed/test.csv", destKey: "processed/test.csv", expectedCopySource: "my-bucket/unprocessed/test.csv"},
		{name: "key with spaces", srcKey: "unprocessed/bank report 2026.csv", destKey: "processed/bank report 2026.csv", expectedCopySource: "my-bucket/unprocessed/bank%20report%202026.csv"},
		{name: "key with plus signs", srcKey: "unprocessed/data+file.csv", destKey: "processed/data+file.csv", expectedCopySource: "my-bucket/unprocessed/data+file.csv"},
		{name: "key with percent signs", srcKey: "unprocessed/100%real.csv", destKey: "processed/100%real.csv", expectedCopySource: "my-bucket/unprocessed/100%25real.csv"},
		{name: "key with unicode characters", srcKey: "unprocessed/résumé.csv", destKey: "processed/résumé.csv", expectedCopySource: "my-bucket/unprocessed/r%C3%A9sum%C3%A9.csv"},
		{name: "nested slashes", srcKey: "a/b/c/file.csv", destKey: "processed/file.csv", expectedCopySource: "my-bucket/a/b/c/file.csv"},
		{name: "leading slash trimmed", srcKey: "/unprocessed/test.csv", destKey: "processed/test.csv", expectedCopySource: "my-bucket/unprocessed/test.csv"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeObjectAPI{}
			client := newTestClient(t, fake, "my-bucket")

			if err := client.ArchiveFile(ctx, tc.srcKey, tc.destKey); err != nil {
				t.Fatalf("unexpected ArchiveFile error: %v", err)
			}
			if fake.lastCopyInput == nil {
				t.Fatal("expected CopyObjectInput to be captured")
			}
			assertStringEqual(t, *fake.lastCopyInput.CopySource, tc.expectedCopySource)
		})
	}
}

func TestArchiveFile_SourceMissing(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		apiErr error
	}{
		{name: "types.NoSuchKey", apiErr: &types.NoSuchKey{}},
		{name: "types.NotFound", apiErr: &types.NotFound{}},
		{name: "coded error NoSuchKey", apiErr: &codedError{code: "NoSuchKey", msg: "missing"}},
		{name: "coded error NotFound", apiErr: &codedError{code: "NotFound", msg: "not found"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeObjectAPI{
				copyObjectFn: func(_ context.Context, _ *awss3.CopyObjectInput, _ ...optFn) (*awss3.CopyObjectOutput, error) {
					return nil, tc.apiErr
				},
			}
			client := newTestClient(t, fake, "test-bucket")

			assertErrorIs(t, client.ArchiveFile(ctx, "missing-src.csv", "dest.csv"), port.ErrObjectNotFound)
			calls := fake.Calls()
			if len(calls) != 1 || calls[0] != "CopyObject" {
				t.Fatalf("expected only CopyObject and never DeleteObject, got %v", calls)
			}
		})
	}
}

func TestArchiveFile_EmptyKeys(t *testing.T) {
	ctx := context.Background()
	cases := [][2]string{{"", "dest.csv"}, {"   ", "dest.csv"}, {"src.csv", ""}, {"src.csv", "   "}, {"", ""}}
	for _, c := range cases {
		fake := &fakeObjectAPI{}
		client := newTestClient(t, fake, "test-bucket")

		assertErrorIs(t, client.ArchiveFile(ctx, c[0], c[1]), s3.ErrMissingKey)
		if len(fake.Calls()) != 0 {
			t.Fatalf("expected 0 calls on invalid keys, got %d", len(fake.Calls()))
		}
	}
}

func TestNewClientFromConfig_Validation(t *testing.T) {
	ctx := context.Background()
	for _, b := range []string{"", "   \t\n"} {
		client, err := s3.NewClientFromConfig(ctx, s3.Config{Bucket: b})
		if client != nil {
			t.Fatalf("expected nil client on error, got %v", client)
		}
		assertErrorIs(t, err, s3.ErrMissingBucket)
	}
}

func TestNewClientFromConfig_Success(t *testing.T) {
	ctx := context.Background()
	cfg := s3.Config{
		Bucket: "valid-bucket",
		Region: "us-east-1",
	}

	client, err := s3.NewClientFromConfig(ctx, cfg)
	if err != nil || client == nil {
		t.Fatalf("expected valid client, got err: %v, client: %v", err, client)
	}
}
