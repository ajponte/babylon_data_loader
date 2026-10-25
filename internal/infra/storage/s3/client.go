package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"babylon/dataloader/internal/domain/port"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	// ErrMissingBucket indicates that a required bucket name was empty.
	ErrMissingBucket = errors.New("s3: bucket is required")
	// ErrMissingKey indicates that a required object key was empty.
	ErrMissingKey = errors.New("s3: object key is required")
	// ErrNilAPI indicates that the provided ObjectAPI client is nil.
	ErrNilAPI = errors.New("s3: api client is nil")
)

// ObjectAPI defines the subset of S3 SDK operations required by Client.
type ObjectAPI interface {
	GetObject(
		ctx context.Context,
		in *awss3.GetObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.GetObjectOutput, error)
	CopyObject(
		ctx context.Context,
		in *awss3.CopyObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.CopyObjectOutput, error)
	DeleteObject(
		ctx context.Context,
		in *awss3.DeleteObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.DeleteObjectOutput, error)
}

// Config holds S3 configuration parameters.
type Config struct {
	Bucket string
	Region string // optional; empty uses the SDK default chain
}

// Client implements port.ObjectStore for AWS S3.
type Client struct {
	api    ObjectAPI
	bucket string
}

var _ port.ObjectStore = (*Client)(nil)

// NewClient constructs an S3 Client bound to the specified bucket using the provided ObjectAPI.
func NewClient(api ObjectAPI, bucket string) (*Client, error) {
	if api == nil {
		return nil, ErrNilAPI
	}
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return nil, ErrMissingBucket
	}
	return &Client{
		api:    api,
		bucket: trimmedBucket,
	}, nil
}

// NewClientFromConfig constructs an S3 Client using the AWS default configuration chain.
func NewClientFromConfig(ctx context.Context, cfg Config) (*Client, error) {
	trimmedBucket := strings.TrimSpace(cfg.Bucket)
	if trimmedBucket == "" {
		return nil, ErrMissingBucket
	}

	var optFns []func(*awsconfig.LoadOptions) error
	if region := strings.TrimSpace(cfg.Region); region != "" {
		optFns = append(optFns, awsconfig.WithRegion(region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	return NewClient(awss3.NewFromConfig(awsCfg), trimmedBucket)
}

// DownloadStream opens an object for streaming from S3. The caller is responsible for closing the returned ReadCloser.
func (c *Client) DownloadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if strings.TrimSpace(key) == "" {
		return nil, ErrMissingKey
	}

	out, err := c.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("s3: get %s: %w", key, port.ErrObjectNotFound)
		}
		return nil, fmt.Errorf("s3: get %s: %w", key, err)
	}

	return out.Body, nil
}

// ArchiveFile copies srcKey to destKey within the configured bucket and then deletes srcKey.
// It is non-atomic. If copying fails, the source file is never deleted.
func (c *Client) ArchiveFile(ctx context.Context, srcKey, destKey string) error {
	if strings.TrimSpace(srcKey) == "" || strings.TrimSpace(destKey) == "" {
		return ErrMissingKey
	}

	copySource := buildCopySource(c.bucket, srcKey)
	_, err := c.api.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("s3: archive copy %s: %w", srcKey, port.ErrObjectNotFound)
		}
		return fmt.Errorf("s3: archive copy %s to %s: %w", srcKey, destKey, err)
	}

	_, err = c.api.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		return fmt.Errorf("s3: archive delete %s: %w", srcKey, err)
	}

	return nil
}

func buildCopySource(bucket, key string) string {
	cleanKey := strings.TrimPrefix(key, "/")
	parts := strings.Split(cleanKey, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return bucket + "/" + strings.Join(parts, "/")
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var noSuchKey *types.NoSuchKey
	var notFound *types.NotFound
	if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
		return true
	}
	type coder interface{ ErrorCode() string }
	var c coder
	if errors.As(err, &c) {
		code := c.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound"
	}
	return false
}
