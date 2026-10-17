package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Embed timezone database to support time.LoadLocation across all environments.
)

const (
	edtOffsetSeconds = -4 * 3600 // Eastern Daylight Time UTC offset in seconds
)

// ParseFidelityCurrency converts "$3250.00", "+$35.26", "-$12.40", "$0.00", "--", or "" to float64.
func ParseFidelityCurrency(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") {
		return 0.0, nil
	}
	clean := strings.ReplaceAll(trimmed, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.ReplaceAll(clean, " ", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid currency value '%s': %w", val, err)
	}
	return f, nil
}

// ParseFidelityPercentage converts "+0.60%", "29.13%", "-3.50%", "--", or "" to float64.
func ParseFidelityPercentage(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") {
		return 0.0, nil
	}
	clean := strings.ReplaceAll(trimmed, "%", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.ReplaceAll(clean, " ", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid percentage value '%s': %w", val, err)
	}
	return f, nil
}

// ParseFidelityQuantity converts "167.946", "4968.08", "--", or "" to float64.
func ParseFidelityQuantity(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" || strings.EqualFold(trimmed, "n/a") {
		return 0.0, nil
	}
	clean := strings.ReplaceAll(trimmed, ",", "")
	clean = strings.ReplaceAll(clean, " ", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid quantity value '%s': %w", val, err)
	}
	return f, nil
}

// ParseFidelityDateDownloaded extracts timestamp from "Date downloaded Sep-25-2026 9:50 p.m ET".
func ParseFidelityDateDownloaded(line string) (time.Time, error) {
	clean := strings.Trim(line, `" `)
	prefix := "Date downloaded "
	if !strings.HasPrefix(clean, prefix) {
		return time.Time{}, fmt.Errorf("line does not contain Date downloaded prefix: %s", line)
	}
	dateStr := strings.TrimPrefix(clean, prefix)
	// Normalize variations of a.m./p.m. to standard AM/PM for Go time parsing
	dateStr = strings.ReplaceAll(dateStr, "p.m.", "PM")
	dateStr = strings.ReplaceAll(dateStr, "a.m.", "AM")
	dateStr = strings.ReplaceAll(dateStr, "p.m", "PM")
	dateStr = strings.ReplaceAll(dateStr, "a.m", "AM")

	isET := strings.HasSuffix(strings.TrimSpace(dateStr), "ET")
	dateStr = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(dateStr), "ET"))

	loc := time.UTC
	if isET {
		var locErr error
		loc, locErr = time.LoadLocation("America/New_York")
		if locErr != nil {
			loc = time.FixedZone("EDT", edtOffsetSeconds)
		}
	}

	parsed, err := time.ParseInLocation("Jan-02-2006 3:04 PM", dateStr, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date '%s': %w", dateStr, err)
	}
	return parsed, nil
}

// fidelityRecordParser encapsulates a CSV record map and accumulates the first parsing error encountered.
type fidelityRecordParser struct {
	record map[string]string
	err    error
}

func (p *fidelityRecordParser) parse(key string, fn func(string) (float64, error)) float64 {
	if p.err != nil {
		return 0.0
	}
	val, err := fn(p.record[key])
	if err != nil {
		p.err = err
		return 0.0
	}
	return val
}

func (p *fidelityRecordParser) currency(key string) float64 {
	return p.parse(key, ParseFidelityCurrency)
}

func (p *fidelityRecordParser) percentage(key string) float64 {
	return p.parse(key, ParseFidelityPercentage)
}

func (p *fidelityRecordParser) quantity(key string) float64 {
	return p.parse(key, ParseFidelityQuantity)
}

// MapRawRecordToFidelityPosition converts a raw record map into a typed FidelityPosition.
func MapRawRecordToFidelityPosition(
	record map[string]string,
	asOfDate time.Time,
	source SourceLineage,
) (FidelityPosition, error) {
	p := fidelityRecordParser{record: record}

	pos := FidelityPosition{
		AccountNumber:        record["account number"],
		AccountName:          record["account name"],
		Symbol:               record["symbol"],
		Description:          record["description"],
		Quantity:             p.quantity("quantity"),
		LastPrice:            p.currency("last price"),
		LastPriceChange:      p.currency("last price change"),
		CurrentValue:         p.currency("current value"),
		TodaysGainLossDollar: p.currency("today's gain/loss dollar"),
		TodaysGainLossPct:    p.percentage("today's gain/loss percent"),
		TotalGainLossDollar:  p.currency("total gain/loss dollar"),
		TotalGainLossPct:     p.percentage("total gain/loss percent"),
		PercentOfAccount:     p.percentage("percent of account"),
		CostBasisTotal:       p.currency("cost basis total"),
		AverageCostBasis:     p.currency("average cost basis"),
		Type:                 record["type"],
		AsOfDate:             asOfDate,
		DataSource:           "fidelity",
		Source:               source,
	}

	if p.err != nil {
		return FidelityPosition{}, p.err
	}

	return pos, nil
}
