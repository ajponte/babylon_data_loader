package fidelity

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"babylon/dataloader/internal/domain/model"
)

type positionMetrics struct {
	quantity             float64
	lastPrice            float64
	currentValue         float64
	costBasis            float64
	lastPriceChange      float64
	todaysGainLossDollar float64
	todaysGainLossPct    float64
	totalGainLossDollar  float64
	totalGainLossPct     float64
	percentOfAccount     float64
	averageCostBasis     float64
}

type rowParser struct {
	colIndex map[string]int
	meta     model.FileMetadata
}

func newRowParser(colIndex map[string]int, meta model.FileMetadata) *rowParser {
	return &rowParser{
		colIndex: colIndex,
		meta:     meta,
	}
}

func (p *rowParser) parse(row []string, lineNum int) (model.FidelityPosition, bool, error) {
	if isFidelityDisclaimerOrEmpty(row) {
		return model.FidelityPosition{}, false, nil
	}

	symbol, description, ok := p.resolveHolding(row)
	if !ok {
		return model.FidelityPosition{}, false, nil
	}

	metrics, err := p.parseMetrics(row, lineNum)
	if err != nil {
		return model.FidelityPosition{}, false, err
	}

	accountID := p.resolveAccountID(row)
	accountName := p.getString(row, "account name", "account nickname")
	posType := p.getString(row, "type")

	pos, err := p.buildPosition(lineNum, accountID, accountName, symbol, description, posType, metrics)
	if err != nil {
		return model.FidelityPosition{}, false, err
	}

	return pos, true, nil
}

func (p *rowParser) resolveAccountID(row []string) string {
	accountID := p.getString(row, "account number", "account id", "account #", "account")
	if isPlaceholderAccountID(accountID) {
		return p.meta.AccountID
	}
	return accountID
}

func (p *rowParser) resolveHolding(row []string) (string, string, bool) {
	symbol := p.getString(row, "symbol", "ticker", "symbol/cusip")
	description := p.getString(row, "description", "security description", "investment name")

	if isSummaryRow(symbol, description) {
		return "", "", false
	}

	normalizedSymbol, ok := normalizeHoldingSymbol(symbol, description)
	if !ok {
		return "", "", false
	}

	return normalizedSymbol, description, true
}

type metricRowReader struct {
	parser  *rowParser
	row     []string
	lineNum int
	err     error
}

func (p *rowParser) newMetricRowReader(row []string, lineNum int) *metricRowReader {
	return &metricRowReader{
		parser:  p,
		row:     row,
		lineNum: lineNum,
	}
}

func (r *metricRowReader) read(
	fieldLabel string,
	parseFn func(string) (float64, error),
	aliases ...string,
) float64 {
	if r.err != nil {
		return 0.0
	}
	val, err := parseFn(r.parser.getString(r.row, aliases...))
	if err != nil {
		r.err = fmt.Errorf("row %d %s: %w", r.lineNum, fieldLabel, err)
		return 0.0
	}
	return val
}

func (p *rowParser) parseMetrics(row []string, lineNum int) (positionMetrics, error) {
	r := p.newMetricRowReader(row, lineNum)
	m := positionMetrics{
		quantity:     r.read("quantity", parseQuantity, "quantity", "shares", "qty"),
		lastPrice:    r.read("last price", parseCurrency, "last price", "price", "last"),
		currentValue: r.read("current value", parseCurrency, "current value", "market value", "current value ($)"),
		costBasis:    r.read("cost basis", parseCurrency, "cost basis total", "cost basis", "total cost basis"),
		lastPriceChange: r.read(
			"last price change",
			parseCurrency,
			"last price change",
		),
		todaysGainLossDollar: r.read(
			"todays gain/loss dollar",
			parseCurrency,
			"today's gain/loss dollar",
			"todays gain/loss dollar",
		),
		todaysGainLossPct: r.read(
			"todays gain/loss percent",
			parsePercent,
			"today's gain/loss percent",
			"todays gain/loss percent",
		),
		totalGainLossDollar: r.read(
			"total gain/loss dollar",
			parseCurrency,
			"total gain/loss dollar",
		),
		totalGainLossPct: r.read(
			"total gain/loss percent",
			parsePercent,
			"total gain/loss percent",
		),
		percentOfAccount: r.read(
			"percent of account",
			parsePercent,
			"percent of account",
		),
		averageCostBasis: r.read(
			"average cost basis",
			parseCurrency,
			"average cost basis",
		),
	}
	if r.err != nil {
		return positionMetrics{}, r.err
	}
	return m, nil
}

func (p *rowParser) buildPosition(
	lineNum int,
	accountID string,
	accountName string,
	symbol string,
	description string,
	posType string,
	m positionMetrics,
) (model.FidelityPosition, error) {
	pos := model.FidelityPosition{
		Lineage: model.SourceLineage{
			FileName:   p.meta.FileName,
			FileHash:   p.meta.FileHash,
			RowNumber:  lineNum,
			IngestedAt: p.meta.IngestedAt,
		},
		AccountID:            accountID,
		AccountName:          accountName,
		Symbol:               symbol,
		Description:          description,
		Quantity:             m.quantity,
		LastPrice:            m.lastPrice,
		CurrentValue:         m.currentValue,
		CostBasis:            m.costBasis,
		LastPriceChange:      m.lastPriceChange,
		TodaysGainLossDollar: m.todaysGainLossDollar,
		TodaysGainLossPct:    m.todaysGainLossPct,
		TotalGainLossDollar:  m.totalGainLossDollar,
		TotalGainLossPct:     m.totalGainLossPct,
		PercentOfAccount:     m.percentOfAccount,
		AverageCostBasis:     m.averageCostBasis,
		Type:                 posType,
	}

	if valErr := pos.Validate(); valErr != nil {
		return model.FidelityPosition{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return pos, nil
}

func (p *rowParser) getString(row []string, aliases ...string) string {
	idx, ok := findColumn(p.colIndex, aliases...)
	if !ok || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

func isSummaryRow(symbol, description string) bool {
	symLower := strings.ToLower(symbol)
	descLower := strings.ToLower(description)

	if isSummaryKeyword(symLower) {
		return true
	}
	return (symbol == "" || symbol == "**") && isSummaryKeyword(descLower)
}

func normalizeHoldingSymbol(symbol, description string) (string, bool) {
	if symbol != "" && symbol != "**" && !strings.EqualFold(symbol, "fcash") && !strings.EqualFold(symbol, "core") {
		return symbol, true
	}

	descUpper := strings.ToUpper(description)
	if isCashHolding(symbol, descUpper) {
		return "CASH", true
	}

	return "", false
}

func isPlaceholderAccountID(val string) bool {
	return val == "" ||
		val == "-" ||
		val == "--" ||
		strings.EqualFold(val, "n/a") ||
		strings.EqualFold(val, "none")
}

func isSummaryKeyword(val string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(val))
	return trimmed == "total" ||
		trimmed == "account total" ||
		trimmed == "portfolio total" ||
		trimmed == "total market value" ||
		trimmed == "ending value" ||
		strings.HasPrefix(trimmed, "total ") ||
		strings.HasPrefix(trimmed, "account total ") ||
		strings.HasPrefix(trimmed, "portfolio total ")
}

func isCashHolding(symbol, descUpper string) bool {
	if symbol == "**" || strings.EqualFold(symbol, "fcash") || strings.EqualFold(symbol, "core") {
		return true
	}
	return isCashDescription(descUpper)
}

func isCashDescription(descUpper string) bool {
	return strings.Contains(descUpper, "HELD IN") ||
		strings.Contains(descUpper, "CASH") ||
		strings.Contains(descUpper, "FCASH") ||
		strings.Contains(descUpper, "CORE") ||
		strings.Contains(descUpper, "DEPOSIT SWEEP") ||
		strings.Contains(descUpper, "SWEEP") ||
		strings.Contains(descUpper, "MONEY MARKET")
}

func isFidelityDisclaimerOrEmpty(record []string) bool {
	if len(record) == 0 {
		return true
	}
	first := firstNonEmptyField(record)
	if first == "" {
		return true
	}
	return isDisclaimerText(first)
}

func firstNonEmptyField(record []string) string {
	for _, f := range record {
		trimmed := strings.ToLower(strings.TrimSpace(f))
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func isDisclaimerText(firstLower string) bool {
	return strings.HasPrefix(firstLower, "the data and information") ||
		strings.HasPrefix(firstLower, "brokerage services") ||
		firstLower == "total" ||
		strings.HasPrefix(firstLower, "total market") ||
		strings.HasPrefix(firstLower, "account total") ||
		strings.HasPrefix(firstLower, "portfolio total") ||
		strings.HasPrefix(firstLower, "ending value") ||
		strings.HasPrefix(firstLower, "disclaimer") ||
		strings.HasPrefix(firstLower, "please note") ||
		strings.HasPrefix(firstLower, "important") ||
		(strings.HasPrefix(firstLower, "*") && firstLower != "**") ||
		strings.HasPrefix(firstLower, "1.") ||
		strings.HasPrefix(firstLower, "quotes are") ||
		strings.HasPrefix(firstLower, "security prices")
}

func parseCurrency(val string) (float64, error) {
	return parseFinancialNumber(val, true, "currency")
}

func parseQuantity(val string) (float64, error) {
	return parseFinancialNumber(val, false, "quantity")
}

func parsePercent(val string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if isNumericDefaultZero(trimmed) {
		return 0.0, nil
	}

	withoutPct, ok := stripPercentSymbol(trimmed)
	if !ok || isNumericDefaultZero(withoutPct) {
		return 0.0, fmt.Errorf("invalid percent value %q: empty or malformed percentage", val)
	}

	clean := sanitizeFinancialToken(withoutPct, false)
	return parseSanitizedFloat(clean, val, "percent")
}

func stripPercentSymbol(trimmed string) (string, bool) {
	candidate := trimmed
	switch {
	case strings.HasSuffix(candidate, "%"):
		candidate = strings.TrimSpace(strings.TrimSuffix(candidate, "%"))
	case strings.HasPrefix(candidate, "(") && strings.HasSuffix(candidate, ")"):
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(candidate, "("), ")"))
		if strings.HasSuffix(inner, "%") {
			inner = strings.TrimSpace(strings.TrimSuffix(inner, "%"))
			if isNumericDefaultZero(inner) {
				return "", false
			}
			candidate = "(" + inner + ")"
		}
	}
	if strings.Contains(candidate, "%") {
		return "", false
	}
	return candidate, true
}

func parseFinancialNumber(val string, stripDollar bool, fieldName string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if isNumericDefaultZero(trimmed) {
		return 0.0, nil
	}

	clean := sanitizeFinancialToken(trimmed, stripDollar)
	return parseSanitizedFloat(clean, val, fieldName)
}

func parseSanitizedFloat(clean, rawVal, fieldName string) (float64, error) {
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid %s value %q: %w", fieldName, rawVal, err)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0.0, fmt.Errorf("invalid %s value %q: non-finite float value", fieldName, rawVal)
	}
	return f, nil
}

func sanitizeFinancialToken(trimmed string, stripDollar bool) string {
	clean := trimmed
	if stripDollar {
		clean = strings.ReplaceAll(clean, "$", "")
	}
	clean = strings.ReplaceAll(clean, "\u00a0", "")
	clean = strings.ReplaceAll(clean, "\u202f", "")
	clean = strings.ReplaceAll(clean, "+", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.TrimSpace(clean)

	if strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")") {
		clean = "-" + clean[1:len(clean)-1]
	}
	return strings.ReplaceAll(clean, " ", "")
}

func isNumericDefaultZero(val string) bool {
	return val == "" ||
		val == "-" ||
		val == "--" ||
		strings.EqualFold(val, "n/a") ||
		strings.EqualFold(val, "unknown") ||
		strings.EqualFold(val, "pending")
}

func findColumn(colIndex map[string]int, aliases ...string) (int, bool) {
	for _, alias := range aliases {
		if idx, ok := colIndex[strings.ToLower(alias)]; ok {
			return idx, true
		}
	}
	return -1, false
}
