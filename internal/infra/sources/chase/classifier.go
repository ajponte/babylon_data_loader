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

const (
	// initialPeekSize is the stream prefix buffered on the fast path, enough for any Chase header.
	initialPeekSize = 4096
	// maxPeekSize bounds how far the classifier reads past blank preamble rows to find the header.
	maxPeekSize = 64 * 1024
)

var _ Classifier = (*DefaultClassifier)(nil)

// Classifier inspects an unconsumed reader stream to determine the Chase statement dialect.
type Classifier interface {
	// Classify inspects the stream header and returns the detected dialect alongside a reader
	// that replays every byte of r, including any bytes consumed during inspection.
	//
	// ctx must be non-nil. Extractor normalizes a nil context before calling Classify, so
	// implementations never receive nil from it.
	Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error)
}

// DefaultClassifier implements Classifier via non-destructive header peeking.
type DefaultClassifier struct{}

// NewDefaultClassifier creates a new DefaultClassifier.
func NewDefaultClassifier() *DefaultClassifier {
	return &DefaultClassifier{}
}

// Classify inspects the stream header and returns the detected dialect alongside an unconsumed reader.
//
// The header is the first non-empty CSV record, located with the same reader settings, BOM
// handling and emptiness rule as the parser, and matched by exact normalized column name. The
// buffered prefix starts at 4 KiB and doubles up to 64 KiB while no complete header has been
// found, checking ctx before the first read and after each read. ctx must be non-nil.
func (c *DefaultClassifier) Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
	if r == nil {
		return DialectUnknown, nil, ErrNilReader
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return DialectUnknown, nil, ctxErr
	}

	br := bufio.NewReaderSize(r, initialPeekSize)
	for size := initialPeekSize; ; size *= 2 {
		if br.Size() < size {
			// Slow path for long blank preambles: rebuffer without losing the bytes already peeked.
			br = bufio.NewReaderSize(br, maxPeekSize)
		}

		peekBytes, err := br.Peek(size)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return DialectUnknown, nil, ctxErr
		}
		atEOF := errors.Is(err, io.EOF)
		if err != nil && !atEOF {
			return DialectUnknown, br, fmt.Errorf("failed to peek stream header: %w", err)
		}

		header, complete := peekHeaderRecord(peekBytes)
		if complete || atEOF || size >= maxPeekSize {
			return classifyHeader(header), br, nil
		}
	}
}

// peekHeaderRecord locates the header record within the buffered prefix. complete reports
// whether the record ended inside the prefix, so a header cut off at the peek boundary is not
// mistaken for the whole header. The prefix is in memory and bounded by maxPeekSize, so it is
// parsed without the caller's context; Classify checks ctx after every read instead.
func peekHeaderRecord(peekBytes []byte) ([]string, bool) {
	data := bytes.TrimPrefix(peekBytes, []byte(utf8BOM))
	if len(data) == 0 {
		return nil, false
	}
	reader := newChaseCSVReader(bytes.NewReader(data))
	header, err := readHeaderRecord(context.Background(), reader)
	if err != nil {
		// No non-empty record yet, or a syntax error. The caller reads more or, at EOF or the
		// peek limit, reports DialectUnknown.
		return nil, false
	}
	end := reader.InputOffset()
	return header, end < int64(len(data)) || bytes.HasSuffix(data, []byte("\n"))
}

// classifyHeader matches the header column names against the dialect signatures.
func classifyHeader(header []string) DialectType {
	has := func(names ...string) bool {
		return hasAnyColumn(header, names...)
	}

	switch {
	// Credit card markers take precedence over checking indicators.
	case has("category", "memo", "transaction date", "trans date"):
		return DialectCreditCard
	// Checking markers require explicit Chase deposit statement indicators.
	case has("check or slip #", "check or slip num"),
		has("details") && has("posting date", "post date", "balance"),
		has("details") && has("description") && has("amount"),
		has("posting date") && has("balance"):
		return DialectChecking
	// A minimal credit card export carries only the credit card parser's required columns.
	case has("post date") && has("description") && has("amount"):
		return DialectCreditCard
	default:
		return DialectUnknown
	}
}

// hasAnyColumn reports whether any column in cols matches one of the lower-case names. For the
// ASCII signature names, EqualFold on the trimmed column is equivalent to comparing
// normalizeColumn(col), as the parser does, without allocating a lower-cased copy. A linear scan
// is used because Chase headers have fewer than ten columns.
func hasAnyColumn(cols []string, names ...string) bool {
	for _, col := range cols {
		trimmed := strings.TrimSpace(col)
		for _, name := range names {
			if strings.EqualFold(trimmed, name) {
				return true
			}
		}
	}
	return false
}
