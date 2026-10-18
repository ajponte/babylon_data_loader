package fidelity

import (
	"fmt"
	"strconv"
	"strings"

	"babylon/dataloader/internal/domain/model"
)

type positionMetrics struct {
	quantity     float64
	lastPrice    float64
	currentValue float64
	costBasis    float64
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

	pos, err := p.buildPosition(lineNum, accountID, accountName, symbol, description, metrics)
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

func (p *rowParser) parseMetrics(row []string, lineNum int) (positionMetrics, error) {
	qty, err := parseQuantity(p.getString(row, "quantity", "shares", "qty"))
	if err != nil {
		return positionMetrics{}, fmt.Errorf("row %d quantity: %w", lineNum, err)
	}

	lastPrice, err := parseCurrency(p.getString(row, "last price", "price", "last"))
	if err != nil {
		return positionMetrics{}, fmt.Errorf("row %d last price: %w", lineNum, err)
	}

	currentVal, err := parseCurrency(p.getString(row, "current value", "market value", "current value ($)"))
	if err != nil {
		return positionMetrics{}, fmt.Errorf("row %d current value: %w", lineNum, err)
	}

	costBasis, err := parseCurrency(p.getString(row, "cost basis total", "cost basis", "total cost basis"))
	if err != nil {
		return positionMetrics{}, fmt.Errorf("row %d cost basis: %w", lineNum, err)
	}

	return positionMetrics{
		quantity:     qty,
		lastPrice:    lastPrice,
		currentValue: currentVal,
		costBasis:    costBasis,
	}, nil
}

func (p *rowParser) buildPosition(
	lineNum int,
	accountID string,
	accountName string,
	symbol string,
	description string,
	m positionMetrics,
) (model.FidelityPosition, error) {
	pos := model.FidelityPosition{
		Lineage: model.SourceLineage{
			FileName:   p.meta.FileName,
			FileHash:   p.meta.FileHash,
			RowNumber:  lineNum,
			IngestedAt: p.meta.IngestedAt,
		},
		AccountID:    accountID,
		AccountName:  accountName,
		Symbol:       symbol,
		Description:  description,
		Quantity:     m.quantity,
		LastPrice:    m.lastPrice,
		CurrentValue: m.currentValue,
		CostBasis:    m.costBasis,
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
		strings.HasPrefix(firstLower, "date downloaded") ||
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

func parseFinancialNumber(val string, stripDollar bool, fieldName string) (float64, error) {
	trimmed := strings.TrimSpace(val)
	if isNumericDefaultZero(trimmed) {
		return 0.0, nil
	}

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
	clean = strings.ReplaceAll(clean, " ", "")

	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0.0, fmt.Errorf("invalid %s value %q: %w", fieldName, val, err)
	}
	return f, nil
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
