package chase

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// DialectType represents the specific statement dialect within Chase exports.
type DialectType string

const (
	// DialectChecking represents a Chase checking or deposit account statement.
	DialectChecking DialectType = "CHASE_CHECKING"
	// DialectCreditCard represents a Chase credit card statement.
	DialectCreditCard DialectType = "CHASE_CREDIT_CARD"
	// DialectUnknown indicates that the statement header does not match a known Chase dialect.
	DialectUnknown DialectType = "UNKNOWN"
)

const peekBufferSize = 4096

// Classifier inspects an unconsumed reader stream to determine the Chase statement dialect.
type Classifier interface {
	// Classify inspects the stream header and returns the detected dialect alongside an unconsumed reader.
	Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error)
}

// DefaultClassifier implements Classifier via non-destructive header peeking.
type DefaultClassifier struct{}

// NewDefaultClassifier creates a new DefaultClassifier.
func NewDefaultClassifier() Classifier {
	return &DefaultClassifier{}
}

// Classify inspects the stream header and returns the detected dialect alongside an unconsumed reader.
func (c *DefaultClassifier) Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return DialectUnknown, nil, ErrNilReader
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return DialectUnknown, nil, ctxErr
	}

	br := bufio.NewReaderSize(r, peekBufferSize)
	peekBytes, err := br.Peek(peekBufferSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return DialectUnknown, br, fmt.Errorf("failed to peek stream header: %w", err)
	}

	if len(peekBytes) == 0 {
		return DialectUnknown, br, nil
	}

	header := findFirstNonEmptyLine(peekBytes)
	if header == "" {
		return DialectUnknown, br, nil
	}

	lower := strings.ToLower(header)

	// Credit card markers take precedence over checking indicators
	if strings.Contains(lower, "category") ||
		strings.Contains(lower, "memo") ||
		strings.Contains(lower, "transaction date") ||
		strings.Contains(lower, "trans date") {
		return DialectCreditCard, br, nil
	}

	// Checking markers require explicit Chase deposit statement indicators
	if strings.Contains(lower, "check or slip") ||
		(strings.Contains(lower, "details") && (strings.Contains(lower, "posting date") || strings.Contains(lower, "post date"))) ||
		(strings.Contains(lower, "details") && strings.Contains(lower, "description") && strings.Contains(lower, "amount")) ||
		(strings.Contains(lower, "posting date") && strings.Contains(lower, "balance")) ||
		(strings.Contains(lower, "details") && strings.Contains(lower, "balance")) {
		return DialectChecking, br, nil
	}

	return DialectUnknown, br, nil
}

func findFirstNonEmptyLine(b []byte) string {
	clean := bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
	scanner := bufio.NewScanner(bytes.NewReader(clean))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			return line
		}
	}
	return ""
}
