package synthetic

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"time"
)

// ErrNilWriter indicates that a nil io.Writer was supplied to WriteCSV.
var ErrNilWriter = errors.New("synthetic csv: writer cannot be nil")

// ErrExcessiveRowCount indicates that requested row count exceeds MaxSyntheticRowCount.
var ErrExcessiveRowCount = errors.New("synthetic csv: row count exceeds maximum allowable limit")

const csvColumnCount = 8

func legacyCSVHeader() []string {
	return []string{
		"Details",
		"Posting Date",
		"Description",
		"Category",
		"Amount",
		"Type",
		"Balance",
		"Check or Slip #",
	}
}

// formatSyntheticRecord formats a single synthetic transaction row into the provided reusable record buffer,
// returning the updated running balance.
func formatSyntheticRecord(
	rng *rand.Rand,
	baseDate time.Time,
	index int,
	balance float64,
	record []string,
) float64 {
	amt := math.Round((rng.Float64()*maxAmount-maxAmount/2.0)*centsFactor) / centsFactor
	// Zero amount violates domain invariant model.ErrInvalidAmount; default to $10.00 debit.
	if amt == 0 {
		amt = 10.00
	}
	balance += amt
	balanceCopy := math.Round(balance*centsFactor) / centsFactor

	txType := typeDebit
	details := typeDebit
	checkOrSlipNum := ""
	if amt > 0 {
		txType = typeCredit
		details = typeCredit
	} else if index%checkCadence == 0 {
		details = typeCheck
		checkOrSlipNum = strconv.Itoa(baseCheckNum + index)
	}

	record[0] = details
	record[1] = baseDate.AddDate(0, 0, -index).Format("01/02/2006")
	record[2] = fmt.Sprintf("Synthetic transaction %d", index)
	record[3] = sourceSynthetic
	record[4] = fmt.Sprintf("%.2f", amt)
	record[5] = txType
	record[6] = fmt.Sprintf("%.2f", balanceCopy)
	record[7] = checkOrSlipNum

	return balance
}

// WriteCSV generates deterministic synthetic transaction rows and streams them as CSV.
func WriteCSV(w io.Writer, opts GeneratorOptions) error {
	if w == nil {
		return ErrNilWriter
	}
	if opts.RowCount < 0 {
		opts.RowCount = 0
	}
	if opts.RowCount > MaxSyntheticRowCount {
		return fmt.Errorf(
			"%w: %d exceeds maximum allowable limit of %d",
			ErrExcessiveRowCount,
			opts.RowCount,
			MaxSyntheticRowCount,
		)
	}

	seed := opts.Seed
	if seed == 0 {
		seed = defaultSeed
	}

	//nolint:gosec // G404: Use of math/rand/v2 is acceptable for non-cryptographic synthetic test data.
	rng := rand.New(rand.NewPCG(uint64(seed), 1))

	csvWriter := csv.NewWriter(w)

	// Note: csv.Writer internally buffers writes using a default 4KB bufio.Writer.
	// Writing the header writes to this buffer and does not flush to the underlying io.Writer
	// unless the buffer fills or Flush() is called.
	if err := csvWriter.Write(legacyCSVHeader()); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Fixed anchor date guarantees byte-for-byte deterministic posting dates across test runs.
	baseDate := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	currentBalance := baseBalance
	record := make([]string, csvColumnCount)

	for i := range opts.RowCount {
		currentBalance = formatSyntheticRecord(rng, baseDate, i, currentBalance, record)
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record %d: %w", i+1, err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return nil
}
