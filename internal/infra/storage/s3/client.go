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
	// ErrSameSourceAndDestination indicates that source and destination keys are identical.
	ErrSameSourceAndDestination = errors.New("s3: source and destination keys must differ")
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

	optFns := buildAWSConfigOptions(cfg.Region)
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	return NewClient(awss3.NewFromConfig(awsCfg), trimmedBucket)
}

// buildAWSConfigOptions constructs optional configuration loaders for the AWS SDK.
func buildAWSConfigOptions(region string) []func(*awsconfig.LoadOptions) error {
	trimmed := strings.TrimSpace(region)
	if trimmed == "" {
		return nil
	}
	return []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(trimmed),
	}
}

// DownloadStream opens an object for streaming from S3.
// Invariant: The returned io.ReadCloser is an unbuffered stream body; the caller owns closing it.
// Note on key handling: The raw key string is preserved as-is without trimming leading slashes,
// ensuring S3 prefix-exactness matching the caller's request, unlike ArchiveFile which normalizes keys.
func (c *Client) DownloadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(key) == "" {
		return nil, ErrMissingKey
	}

	out, err := c.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, c.wrapGetError(key, err)
	}

	return out.Body, nil
}

// wrapGetError maps GetObject errors to domain port errors or contextual wrapped errors.
func (c *Client) wrapGetError(key string, err error) error {
	if isNotFound(err) {
		return fmt.Errorf("s3: get %s/%s: %w", c.bucket, key, port.ErrObjectNotFound)
	}
	return fmt.Errorf("s3: get %s/%s: %w", c.bucket, key, err)
}

// ArchiveFile copies srcKey to destKey within the configured bucket and then deletes srcKey.
// Invariant: Durability is preserved because this is a non-atomic two-phase operation (copy-then-delete);
// if the copy step fails for any reason, the source object is never deleted.
func (c *Client) ArchiveFile(ctx context.Context, srcKey, destKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	src, dest, err := validateArchiveKeys(srcKey, destKey)
	if err != nil {
		return err
	}
	if err = c.copyObject(ctx, src, dest); err != nil {
		return err
	}
	return c.deleteObject(ctx, src)
}

// validateArchiveKeys cleans and validates the source and destination keys.
// Invariant: Prevents destructive self-overwrite and data loss by rejecting identical normalized keys.
// Leading slashes are trimmed because S3 CopySource requires a canonical bucket/key format.
func validateArchiveKeys(srcKey, destKey string) (string, string, error) {
	src := cleanKey(srcKey)
	dest := cleanKey(destKey)
	if src == "" || dest == "" {
		return "", "", ErrMissingKey
	}
	if src == dest {
		return "", "", ErrSameSourceAndDestination
	}
	return src, dest, nil
}

// copyObject executes the S3 CopyObject API call and maps errors to domain errors.
func (c *Client) copyObject(ctx context.Context, src, dest string) error {
	copySource := buildCopySource(c.bucket, src)
	_, err := c.api.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		Key:        aws.String(dest),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("s3: archive copy %s: %w", src, port.ErrObjectNotFound)
		}
		return fmt.Errorf("s3: archive copy %s to %s: %w", src, dest, err)
	}
	return nil
}

// deleteObject executes the S3 DeleteObject API call and wraps delete errors.
func (c *Client) deleteObject(ctx context.Context, src string) error {
	_, err := c.api.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(src),
	})
	if err != nil {
		return fmt.Errorf("s3: archive delete %s: %w", src, err)
	}
	return nil
}

func cleanKey(key string) string {
	return strings.TrimPrefix(strings.TrimSpace(key), "/")
}

func buildCopySource(bucket, key string) string {
	parts := strings.Split(cleanKey(key), "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return bucket + "/" + strings.Join(parts, "/")
}

// isNotFound identifies missing-object S3 errors.
// Invariant: Uses duck-typing (ErrorCode interface) alongside types.NoSuchKey and types.NotFound
// to inspect errors without requiring a direct dependency on internal smithy-go error types.
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
