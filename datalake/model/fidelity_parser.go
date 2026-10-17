package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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
	clean = strings.ReplaceAll(clean, "+", "")
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
	dateStr = strings.TrimSpace(strings.TrimSuffix(dateStr, "ET"))

	parsed, err := time.Parse("Jan-02-2006 3:04 PM", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date '%s': %w", dateStr, err)
	}
	return parsed, nil
}

// MapRawRecordToFidelityPosition converts a raw record map into a typed FidelityPosition.
func MapRawRecordToFidelityPosition(
	record map[string]string,
	asOfDate time.Time,
	source SourceLineage,
) (FidelityPosition, error) {
	qty, err := ParseFidelityQuantity(record["quantity"])
	if err != nil {
		return FidelityPosition{}, err
	}

	lastPrice, err := ParseFidelityCurrency(record["last price"])
	if err != nil {
		return FidelityPosition{}, err
	}

	lastPriceChange, err := ParseFidelityCurrency(record["last price change"])
	if err != nil {
		return FidelityPosition{}, err
	}

	currentVal, err := ParseFidelityCurrency(record["current value"])
	if err != nil {
		return FidelityPosition{}, err
	}

	todaysGainLossDollar, err := ParseFidelityCurrency(record["today's gain/loss dollar"])
	if err != nil {
		return FidelityPosition{}, err
	}

	todaysGainLossPct, err := ParseFidelityPercentage(record["today's gain/loss percent"])
	if err != nil {
		return FidelityPosition{}, err
	}

	totalGainLossDollar, err := ParseFidelityCurrency(record["total gain/loss dollar"])
	if err != nil {
		return FidelityPosition{}, err
	}

	totalGainLossPct, err := ParseFidelityPercentage(record["total gain/loss percent"])
	if err != nil {
		return FidelityPosition{}, err
	}

	percentOfAccount, err := ParseFidelityPercentage(record["percent of account"])
	if err != nil {
		return FidelityPosition{}, err
	}

	costBasisTotal, err := ParseFidelityCurrency(record["cost basis total"])
	if err != nil {
		return FidelityPosition{}, err
	}

	avgCostBasis, err := ParseFidelityCurrency(record["average cost basis"])
	if err != nil {
		return FidelityPosition{}, err
	}

	return FidelityPosition{
		AccountNumber:        record["account number"],
		AccountName:          record["account name"],
		Symbol:               record["symbol"],
		Description:          record["description"],
		Quantity:             qty,
		LastPrice:            lastPrice,
		LastPriceChange:      lastPriceChange,
		CurrentValue:         currentVal,
		TodaysGainLossDollar: todaysGainLossDollar,
		TodaysGainLossPct:    todaysGainLossPct,
		TotalGainLossDollar:  totalGainLossDollar,
		TotalGainLossPct:     totalGainLossPct,
		PercentOfAccount:     percentOfAccount,
		CostBasisTotal:       costBasisTotal,
		AverageCostBasis:     avgCostBasis,
		Type:                 record["type"],
		AsOfDate:             asOfDate,
		DataSource:           "fidelity",
		Source:               source,
	}, nil
}
