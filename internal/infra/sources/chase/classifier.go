package chase

import (
	"bufio"
	"bytes"
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

// Classifier inspects an unconsumed reader stream to determine the Chase statement dialect.
type Classifier interface {
	// Classify inspects the stream header and returns the detected dialect alongside a reconstructed stream.
	Classify(r io.Reader) (DialectType, io.Reader, error)
}

// DefaultClassifier implements Classifier via non-destructive header peeking.
type DefaultClassifier struct{}

// NewDefaultClassifier creates a new DefaultClassifier.
func NewDefaultClassifier() Classifier {
	return &DefaultClassifier{}
}

// Classify inspects the stream header and returns the detected dialect alongside a reconstructed stream.
func (c *DefaultClassifier) Classify(r io.Reader) (DialectType, io.Reader, error) {
	if r == nil {
		return DialectUnknown, nil, errors.New("reader cannot be nil")
	}

	var buf bytes.Buffer
	br := bufio.NewReader(r)

	var firstNonEmpty string
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			buf.Write(line)
			trimmed := strings.TrimSpace(string(line))
			if trimmed != "" && firstNonEmpty == "" {
				firstNonEmpty = trimmed
				break
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return DialectUnknown, io.MultiReader(&buf, br), fmt.Errorf("failed to read stream header: %w", err)
		}
	}

	fullReader := io.MultiReader(&buf, br)
	if firstNonEmpty == "" {
		return DialectUnknown, fullReader, nil
	}

	cleanHeader := strings.TrimPrefix(firstNonEmpty, "\xef\xbb\xbf")
	lower := strings.ToLower(cleanHeader)

	if strings.Contains(lower, "check or slip") || strings.Contains(lower, "balance") {
		return DialectChecking, fullReader, nil
	}
	if strings.Contains(lower, "category") ||
		strings.Contains(lower, "memo") ||
		strings.Contains(lower, "transaction date") {
		return DialectCreditCard, fullReader, nil
	}

	return DialectUnknown, fullReader, nil
}
