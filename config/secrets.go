package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

//nolint:gochecknoglobals // Required for thread-safe in-memory caching of Mongo URI across Lambda invocations
var (
	cacheMu        sync.RWMutex
	cachedMongoURI string
)

// MongoCredentials models the structured JSON stored in AWS Secrets Manager.
type MongoCredentials struct {
	Engine     string `json:"engine"`
	Host       string `json:"host"`
	Port       string `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	Database   string `json:"database"`
	AuthSource string `json:"auth_source"`
	MongoURI   string `json:"mongo_uri"`
}

// SecretsClient defines the interface for fetching secret strings (allows easy mocking).
type SecretsClient interface {
	GetSecretValue(
		ctx context.Context,
		params *secretsmanager.GetSecretValueInput,
		optFns ...func(*secretsmanager.Options),
	) (*secretsmanager.GetSecretValueOutput, error)
}

// GetMongoURI retrieves the MongoDB connection URI.
// Fallback Priority:
// 1. If MONGO_URI is set in the environment, it is returned immediately.
// 2. Thread-safe in-memory cache is checked.
// 3. Fetches from AWS Secrets Manager using the provided secretID or MONGO_SECRET_ID env var.
func GetMongoURI(ctx context.Context, client SecretsClient, secretID string) (string, error) {
	if envURI := os.Getenv("MONGO_URI"); envURI != "" {
		return envURI, nil
	}

	cacheMu.RLock()
	if cachedMongoURI != "" {
		uri := cachedMongoURI
		cacheMu.RUnlock()
		return uri, nil
	}
	cacheMu.RUnlock()

	cacheMu.Lock()
	defer cacheMu.Unlock()

	// Double-check after acquiring write lock
	if cachedMongoURI != "" {
		return cachedMongoURI, nil
	}

	if secretID == "" {
		secretID = os.Getenv("MONGO_SECRET_ID")
	}
	if secretID == "" {
		return "", errors.New("mongo secret ID is not configured and MONGO_URI env var is not set")
	}
	if client == nil {
		return "", errors.New("secrets manager client is nil")
	}

	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get secret value for %q: %w", secretID, err)
	}

	var secretStr string
	switch {
	case out.SecretString != nil:
		secretStr = *out.SecretString
	case len(out.SecretBinary) > 0:
		secretStr = string(out.SecretBinary)
	default:
		return "", errors.New("secret value contains neither string nor binary data")
	}

	resolvedURI, err := parseSecretToURI(secretStr)
	if err != nil {
		return "", err
	}

	cachedMongoURI = resolvedURI
	return resolvedURI, nil
}

func parseSecretToURI(secretStr string) (string, error) {
	var creds MongoCredentials
	if err := json.Unmarshal([]byte(secretStr), &creds); err == nil {
		return buildURIFromCredentials(&creds)
	}

	trimmed := strings.TrimSpace(secretStr)
	if strings.HasPrefix(trimmed, "mongodb://") || strings.HasPrefix(trimmed, "mongodb+srv://") {
		return trimmed, nil
	}

	return "", fmt.Errorf("failed to parse secret as JSON or direct MongoDB URI: %s", secretStr)
}

func buildURIFromCredentials(creds *MongoCredentials) (string, error) {
	if creds.MongoURI != "" {
		return creds.MongoURI, nil
	}
	if creds.Host == "" {
		return "", errors.New("invalid secret payload: missing mongo_uri and host")
	}

	port := creds.Port
	if port == "" {
		port = defaultMongoPort
	}
	hostPort := net.JoinHostPort(creds.Host, port)

	database := creds.Database
	if database == "" {
		database = "datalake"
	}
	authSource := creds.AuthSource
	if authSource == "" {
		authSource = "admin"
	}

	if creds.Username != "" && creds.Password != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s/%s?authSource=%s",
			creds.Username, creds.Password, hostPort, database, authSource), nil
	}

	return fmt.Sprintf("mongodb://%s/%s", hostPort, database), nil
}

// ResetMongoURICache clears the cached MongoDB URI (primarily for tests).
func ResetMongoURICache() {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	cachedMongoURI = ""
}

// SetCachedMongoURI manually sets the cached MongoDB URI (primarily for tests).
func SetCachedMongoURI(uri string) {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	cachedMongoURI = uri
}
