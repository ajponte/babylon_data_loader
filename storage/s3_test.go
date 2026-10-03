package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"babylon/dataloader/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3Client struct {
	getObjectFunc    func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	copyObjectFunc   func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	deleteObjectFunc func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)

	capturedGetInput    *s3.GetObjectInput
	capturedCopyInput   *s3.CopyObjectInput
	capturedDeleteInput *s3.DeleteObjectInput
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.capturedGetInput = params
	if m.getObjectFunc != nil {
		return m.getObjectFunc(ctx, params, optFns...)
	}
	return nil, errors.New("unimplemented")
}

func (m *mockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.capturedCopyInput = params
	if m.copyObjectFunc != nil {
		return m.copyObjectFunc(ctx, params, optFns...)
	}
	return &s3.CopyObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.capturedDeleteInput = params
	if m.deleteObjectFunc != nil {
		return m.deleteObjectFunc(ctx, params, optFns...)
	}
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3Storage_Download(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expectedContent := "test,data,content\n1,2,3"
		mockClient := &mockS3Client{
			getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(bytes.NewBufferString(expectedContent)),
				}, nil
			},
		}

		s3Storage := storage.NewS3Storage(mockClient)
		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "sub", "test.csv")

		err := s3Storage.Download(context.Background(), "my-bucket", "unprocessed/test.csv", localFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if mockClient.capturedGetInput == nil || *mockClient.capturedGetInput.Bucket != "my-bucket" || *mockClient.capturedGetInput.Key != "unprocessed/test.csv" {
			t.Errorf("unexpected GetObjectInput: %v", mockClient.capturedGetInput)
		}

		data, err := os.ReadFile(localFile)
		if err != nil {
			t.Fatalf("failed to read downloaded file: %v", err)
		}
		if string(data) != expectedContent {
			t.Errorf("content got %q, want %q", string(data), expectedContent)
		}
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := &mockS3Client{
			getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				return nil, errors.New("s3 NoSuchKey")
			},
		}

		s3Storage := storage.NewS3Storage(mockClient)
		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "test.csv")

		err := s3Storage.Download(context.Background(), "my-bucket", "nonexistent.csv", localFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("validation and nil client", func(t *testing.T) {
		s3Storage := storage.NewS3Storage(nil)
		if err := s3Storage.Download(context.Background(), "b", "k", "p"); err == nil {
			t.Error("expected error for nil client")
		}

		mockClient := &mockS3Client{}
		s3Storage = storage.NewS3Storage(mockClient)
		if err := s3Storage.Download(context.Background(), "", "k", "p"); err == nil {
			t.Error("expected error for empty bucket")
		}
		if err := s3Storage.Download(context.Background(), "b", "", "p"); err == nil {
			t.Error("expected error for empty key")
		}
		if err := s3Storage.Download(context.Background(), "b", "k", ""); err == nil {
			t.Error("expected error for empty localPath")
		}
	})
}

func TestS3Storage_Copy_Success(t *testing.T) {
	mockClient := &mockS3Client{
		copyObjectFunc: func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return &s3.CopyObjectOutput{}, nil
		},
	}

	s3Storage := storage.NewS3Storage(mockClient)
	err := s3Storage.Copy(context.Background(), "src-bucket", "unprocessed/data.csv", "dest-bucket", "processed/data.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockClient.capturedCopyInput == nil {
		t.Fatal("expected CopyObjectInput to be captured")
	}
	if aws.ToString(mockClient.capturedCopyInput.Bucket) != "dest-bucket" {
		t.Errorf("destination bucket got %s, want dest-bucket", aws.ToString(mockClient.capturedCopyInput.Bucket))
	}
	if aws.ToString(mockClient.capturedCopyInput.Key) != "processed/data.csv" {
		t.Errorf("destination key got %s, want processed/data.csv", aws.ToString(mockClient.capturedCopyInput.Key))
	}
	if aws.ToString(mockClient.capturedCopyInput.CopySource) != "src-bucket/unprocessed/data.csv" {
		t.Errorf("copy source got %s, want src-bucket/unprocessed/data.csv", aws.ToString(mockClient.capturedCopyInput.CopySource))
	}
}

func TestS3Storage_Copy_UrlEncodedSourceKey(t *testing.T) {
	mockClient := &mockS3Client{
		copyObjectFunc: func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return &s3.CopyObjectOutput{}, nil
		},
	}

	s3Storage := storage.NewS3Storage(mockClient)
	err := s3Storage.Copy(context.Background(), "src-bucket", "unprocessed/bank report 2026.csv", "dest-bucket", "processed/bank report 2026.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockClient.capturedCopyInput == nil {
		t.Fatal("expected CopyObjectInput to be captured")
	}
	expectedSource := "src-bucket/unprocessed/bank%20report%202026.csv"
	if aws.ToString(mockClient.capturedCopyInput.CopySource) != expectedSource {
		t.Errorf("copy source got %s, want %s", aws.ToString(mockClient.capturedCopyInput.CopySource), expectedSource)
	}
}

func TestS3Storage_Copy_ClientError(t *testing.T) {
	mockClient := &mockS3Client{
		copyObjectFunc: func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			return nil, errors.New("copy failed")
		},
	}

	s3Storage := storage.NewS3Storage(mockClient)
	err := s3Storage.Copy(context.Background(), "b", "k1", "b", "k2")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestS3Storage_Copy_Validation(t *testing.T) {
	s3Storage := storage.NewS3Storage(nil)
	if err := s3Storage.Copy(context.Background(), "b1", "k1", "b2", "k2"); err == nil {
		t.Error("expected error for nil client")
	}

	mockClient := &mockS3Client{}
	s3Storage = storage.NewS3Storage(mockClient)
	if err := s3Storage.Copy(context.Background(), "", "k1", "b2", "k2"); err == nil {
		t.Error("expected error for empty srcBucket")
	}
	if err := s3Storage.Copy(context.Background(), "b1", "", "b2", "k2"); err == nil {
		t.Error("expected error for empty srcKey")
	}
	if err := s3Storage.Copy(context.Background(), "b1", "k1", "", "k2"); err == nil {
		t.Error("expected error for empty destBucket")
	}
	if err := s3Storage.Copy(context.Background(), "b1", "k1", "b2", ""); err == nil {
		t.Error("expected error for empty destKey")
	}
}

func TestS3Storage_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockClient := &mockS3Client{
			deleteObjectFunc: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
				return &s3.DeleteObjectOutput{}, nil
			},
		}

		s3Storage := storage.NewS3Storage(mockClient)
		err := s3Storage.Delete(context.Background(), "my-bucket", "unprocessed/data.csv")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if mockClient.capturedDeleteInput == nil {
			t.Fatal("expected DeleteObjectInput to be captured")
		}
		if aws.ToString(mockClient.capturedDeleteInput.Bucket) != "my-bucket" {
			t.Errorf("bucket got %s, want my-bucket", aws.ToString(mockClient.capturedDeleteInput.Bucket))
		}
		if aws.ToString(mockClient.capturedDeleteInput.Key) != "unprocessed/data.csv" {
			t.Errorf("key got %s, want unprocessed/data.csv", aws.ToString(mockClient.capturedDeleteInput.Key))
		}
	})

	t.Run("client error", func(t *testing.T) {
		mockClient := &mockS3Client{
			deleteObjectFunc: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
				return nil, errors.New("delete failed")
			},
		}

		s3Storage := storage.NewS3Storage(mockClient)
		err := s3Storage.Delete(context.Background(), "my-bucket", "unprocessed/data.csv")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("validation and nil client", func(t *testing.T) {
		s3Storage := storage.NewS3Storage(nil)
		if err := s3Storage.Delete(context.Background(), "b", "k"); err == nil {
			t.Error("expected error for nil client")
		}

		mockClient := &mockS3Client{}
		s3Storage = storage.NewS3Storage(mockClient)
		if err := s3Storage.Delete(context.Background(), "", "k"); err == nil {
			t.Error("expected error for empty bucket")
		}
		if err := s3Storage.Delete(context.Background(), "b", ""); err == nil {
			t.Error("expected error for empty key")
		}
	})
}
