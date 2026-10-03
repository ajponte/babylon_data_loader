package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client defines the interface for S3 operations required by S3Storage.
type S3Client interface {
	GetObject(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	CopyObject(
		ctx context.Context,
		params *s3.CopyObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.CopyObjectOutput, error)
	DeleteObject(
		ctx context.Context,
		params *s3.DeleteObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.DeleteObjectOutput, error)
}

// S3Storage provides helper methods for interacting with Amazon S3.
type S3Storage struct {
	client S3Client
}

// NewS3Storage creates a new S3Storage helper instance.
func NewS3Storage(client S3Client) *S3Storage {
	return &S3Storage{client: client}
}

// Download fetches an object from S3 and streams it to localPath on disk.
func (s *S3Storage) Download(ctx context.Context, bucket, key, localPath string) error {
	if s.client == nil {
		return errors.New("s3 client is nil")
	}
	if bucket == "" || key == "" || localPath == "" {
		return errors.New("bucket, key, and localPath must not be empty")
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get S3 object from %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	dir := filepath.Dir(localPath)
	if mkdirErr := os.MkdirAll(dir, 0o750); mkdirErr != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, mkdirErr)
	}

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %w", localPath, err)
	}
	defer f.Close()

	if _, copyErr := io.Copy(f, out.Body); copyErr != nil {
		return fmt.Errorf("failed to write S3 object data to %s: %w", localPath, copyErr)
	}

	return nil
}

// Copy duplicates an S3 object within or across buckets.
func (s *S3Storage) Copy(ctx context.Context, srcBucket, srcKey, destBucket, destKey string) error {
	if s.client == nil {
		return errors.New("s3 client is nil")
	}
	if srcBucket == "" || srcKey == "" || destBucket == "" || destKey == "" {
		return errors.New("source and destination bucket/key must not be empty")
	}

	copySource := srcBucket + "/" + strings.TrimPrefix(srcKey, "/")
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to copy S3 object from %s/%s to %s/%s: %w",
			srcBucket, srcKey, destBucket, destKey, err,
		)
	}

	return nil
}

// Delete removes an object from S3.
func (s *S3Storage) Delete(ctx context.Context, bucket, key string) error {
	if s.client == nil {
		return errors.New("s3 client is nil")
	}
	if bucket == "" || key == "" {
		return errors.New("bucket and key must not be empty")
	}

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete S3 object %s/%s: %w", bucket, key, err)
	}

	return nil
}
