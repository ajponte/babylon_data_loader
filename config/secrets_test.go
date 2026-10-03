package config_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"babylon/dataloader/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type mockSecretsClient struct {
	mu         sync.Mutex
	callCount  int32
	output     *secretsmanager.GetSecretValueOutput
	err        error
	capturedID *string
}

func (m *mockSecretsClient) GetSecretValue(
	ctx context.Context,
	params *secretsmanager.GetSecretValueInput,
	optFns ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	atomic.AddInt32(&m.callCount, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if params != nil {
		m.capturedID = params.SecretId
	}
	return m.output, m.err
}

func (m *mockSecretsClient) getCallCount() int32 {
	return atomic.LoadInt32(&m.callCount)
}

func TestGetMongoURI_EnvFallback(t *testing.T) {
	config.ResetMongoURICache()
	expectedURI := "mongodb://env-user:env-pass@localhost:27017/datalake"
	t.Setenv("MONGO_URI", expectedURI)

	mockClient := &mockSecretsClient{}
	uri, err := config.GetMongoURI(context.Background(), mockClient, "some-secret-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uri != expectedURI {
		t.Errorf("got %q, want %q", uri, expectedURI)
	}
	if mockClient.getCallCount() != 0 {
		t.Errorf("expected client not to be called, got call count %d", mockClient.getCallCount())
	}
}

func TestGetMongoURI_JSONWithMongoURI(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	expectedURI := "mongodb+srv://admin:secret@cluster0.abcde.mongodb.net/datalake?retryWrites=true&w=majority"
	jsonPayload := `{
		"engine": "mongodb",
		"host": "cluster0.abcde.mongodb.net",
		"port": "27017",
		"username": "admin",
		"password": "secret",
		"database": "datalake",
		"auth_source": "admin",
		"mongo_uri": "` + expectedURI + `"
	}`

	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(jsonPayload),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "babylon/staging/datalake/credentials")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uri != expectedURI {
		t.Errorf("got %q, want %q", uri, expectedURI)
	}
	if mockClient.getCallCount() != 1 {
		t.Errorf("expected call count 1, got %d", mockClient.getCallCount())
	}
	if mockClient.capturedID == nil || *mockClient.capturedID != "babylon/staging/datalake/credentials" {
		t.Errorf("unexpected secret ID: %v", mockClient.capturedID)
	}
}

func TestGetMongoURI_JSONStructuredComponents(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	jsonPayload := `{
		"engine": "mongodb",
		"host": "mongo.local",
		"port": "27017",
		"username": "myuser",
		"password": "mypassword",
		"database": "datalake",
		"auth_source": "admin"
	}`

	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(jsonPayload),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "babylon/dev/credentials")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "mongodb://myuser:mypassword@mongo.local:27017/datalake?authSource=admin"
	if uri != expected {
		t.Errorf("got %q, want %q", uri, expected)
	}
}

func TestGetMongoURI_DirectURIString(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	rawURI := "mongodb+srv://admin:pass@atlas.example.com/datalake"
	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(rawURI),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uri != rawURI {
		t.Errorf("got %q, want %q", uri, rawURI)
	}
}

func TestGetMongoURI_BinarySecret(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	expectedURI := "mongodb://user:pass@remote:27017/datalake"
	jsonPayload := `{"mongo_uri": "` + expectedURI + `"}`
	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretBinary: []byte(jsonPayload),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uri != expectedURI {
		t.Errorf("got %q, want %q", uri, expectedURI)
	}
}

func TestGetMongoURI_CachingHit(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	jsonPayload := `{"mongo_uri": "mongodb://cached-host:27017/datalake"}`
	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(jsonPayload),
		},
	}

	// First call - cache miss
	uri1, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("call 1 failed: %v", err)
	}

	// Second call - cache hit
	uri2, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("call 2 failed: %v", err)
	}

	// Third call - cache hit
	uri3, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("call 3 failed: %v", err)
	}

	if uri1 != uri2 || uri2 != uri3 {
		t.Errorf("inconsistent URIs: %s, %s, %s", uri1, uri2, uri3)
	}
	if mockClient.getCallCount() != 1 {
		t.Errorf("expected client to be called exactly once due to caching, got %d", mockClient.getCallCount())
	}
}

func TestGetMongoURI_ConcurrentAccess(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	expectedURI := "mongodb://concurrent:27017/datalake"
	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(`{"mongo_uri": "` + expectedURI + `"}`),
		},
	}

	var wg sync.WaitGroup
	const goroutines = 20
	errChan := make(chan error, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uri, err := config.GetMongoURI(context.Background(), mockClient, "concurrent-secret")
			if err != nil {
				errChan <- err
				return
			}
			if uri != expectedURI {
				errChan <- errors.New("URI mismatch in goroutine")
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent error: %v", err)
	}
	if mockClient.getCallCount() != 1 {
		t.Errorf("expected exactly 1 call under concurrency, got %d", mockClient.getCallCount())
	}
}

func TestGetMongoURI_SecretIDFromEnv(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")
	t.Setenv("MONGO_SECRET_ID", "env-secret-id")

	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(`{"mongo_uri": "mongodb://from-env-secret:27017"}`),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uri != "mongodb://from-env-secret:27017" {
		t.Errorf("unexpected uri: %s", uri)
	}
	if mockClient.capturedID == nil || *mockClient.capturedID != "env-secret-id" {
		t.Errorf("expected secret ID 'env-secret-id', got %v", mockClient.capturedID)
	}
}

func TestGetMongoURI_Errors(t *testing.T) {
	t.Run("missing secret id and env var", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")
		_ = os.Unsetenv("MONGO_SECRET_ID")

		mockClient := &mockSecretsClient{}
		_, err := config.GetMongoURI(context.Background(), mockClient, "")
		if err == nil {
			t.Fatal("expected error for empty secret ID, got nil")
		}
	})

	t.Run("nil client", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")

		_, err := config.GetMongoURI(context.Background(), nil, "some-secret")
		if err == nil {
			t.Fatal("expected error for nil client, got nil")
		}
	})

	t.Run("client returns error", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")

		mockClient := &mockSecretsClient{
			err: errors.New("access denied"),
		}
		_, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
		if err == nil {
			t.Fatal("expected error from client, got nil")
		}
	})

	t.Run("empty secret value", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")

		mockClient := &mockSecretsClient{
			output: &secretsmanager.GetSecretValueOutput{},
		}
		_, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
		if err == nil {
			t.Fatal("expected error for empty secret output, got nil")
		}
	})

	t.Run("invalid json and invalid uri does not leak secret in error", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")

		sensitiveSecret := "sensitive-mongodb-credential-123456"
		mockClient := &mockSecretsClient{
			output: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(sensitiveSecret),
			},
		}
		_, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
		if err == nil {
			t.Fatal("expected error for invalid secret string, got nil")
		}
		if strings.Contains(err.Error(), sensitiveSecret) {
			t.Errorf("error message leaks plaintext secret payload: %v", err)
		}
	})

	t.Run("json missing both mongo_uri and host", func(t *testing.T) {
		config.ResetMongoURICache()
		_ = os.Unsetenv("MONGO_URI")

		mockClient := &mockSecretsClient{
			output: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(`{"database": "datalake"}`),
			},
		}
		_, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
		if err == nil {
			t.Fatal("expected error for incomplete JSON, got nil")
		}
	})
}

func TestGetMongoURI_SpecialCharactersEscaped(t *testing.T) {
	config.ResetMongoURICache()
	_ = os.Unsetenv("MONGO_URI")

	rawUser := "user@babylon:finance"
	rawPass := "p@ss:w0rd#123!/?&="
	jsonPayload := `{
		"engine": "mongodb",
		"host": "cluster0.abcde.mongodb.net",
		"port": "27017",
		"username": "` + rawUser + `",
		"password": "` + rawPass + `",
		"database": "datalake",
		"auth_source": "admin"
	}`

	mockClient := &mockSecretsClient{
		output: &secretsmanager.GetSecretValueOutput{
			SecretString: aws.String(jsonPayload),
		},
	}

	uri, err := config.GetMongoURI(context.Background(), mockClient, "secret-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedUser := url.QueryEscape(rawUser)
	expectedPass := url.QueryEscape(rawPass)
	expectedURI := "mongodb://" + expectedUser + ":" + expectedPass + "@cluster0.abcde.mongodb.net:27017/datalake?authSource=admin"
	if uri != expectedURI {
		t.Errorf("got %q, want %q", uri, expectedURI)
	}

	// Verify standard URL parsing round-trip
	parsed, parseErr := url.Parse(uri)
	if parseErr != nil {
		t.Fatalf("failed to parse generated URI: %v", parseErr)
	}
	if parsed.User == nil {
		t.Fatal("expected UserInfo in parsed URI")
	}
	if parsed.User.Username() != rawUser {
		t.Errorf("got parsed username %q, want %q", parsed.User.Username(), rawUser)
	}
	pass, set := parsed.User.Password()
	if !set || pass != rawPass {
		t.Errorf("got parsed password %q, want %q", pass, rawPass)
	}
}
