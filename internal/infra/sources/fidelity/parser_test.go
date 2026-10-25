package fidelity

import (
	"math"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func sampleTestMeta() model.FileMetadata {
	return model.FileMetadata{
		FileName:   "test_portfolio.csv",
		FileHash:   "hash_test_123",
		AccountID:  "DEFAULT-ACCT-999",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
}

func sampleTestColIndex() map[string]int {
	return map[string]int{
		"account number": 0,
		"account name":   1,
		"symbol":         2,
		"description":    3,
		"quantity":       4,
		"last price":     5,
		"current value":  6,
		"cost basis":     7,
	}
}

type expectedPositionMetrics struct {
	accountID    string
	symbol       string
	description  string
	quantity     float64
	lastPrice    float64
	currentValue float64
	costBasis    float64
	rowNumber    int
	fileName     string
}

func assertExpectedPosition(t *testing.T, pos model.FidelityPosition, exp expectedPositionMetrics) {
	t.Helper()
	if pos.AccountID != exp.accountID {
		t.Errorf("AccountID: expected %q, got %q", exp.accountID, pos.AccountID)
	}
	if pos.Symbol != exp.symbol {
		t.Errorf("Symbol: expected %q, got %q", exp.symbol, pos.Symbol)
	}
	if pos.Description != exp.description {
		t.Errorf("Description: expected %q, got %q", exp.description, pos.Description)
	}
	if math.Abs(pos.Quantity-exp.quantity) > 1e-6 {
		t.Errorf("Quantity: expected %f, got %f", exp.quantity, pos.Quantity)
	}
	if math.Abs(pos.LastPrice-exp.lastPrice) > 1e-6 {
		t.Errorf("LastPrice: expected %f, got %f", exp.lastPrice, pos.LastPrice)
	}
	if math.Abs(pos.CurrentValue-exp.currentValue) > 1e-6 {
		t.Errorf("CurrentValue: expected %f, got %f", exp.currentValue, pos.CurrentValue)
	}
	if math.Abs(pos.CostBasis-exp.costBasis) > 1e-6 {
		t.Errorf("CostBasis: expected %f, got %f", exp.costBasis, pos.CostBasis)
	}
	if pos.Lineage.RowNumber != exp.rowNumber {
		t.Errorf("Lineage.RowNumber: expected %d, got %d", exp.rowNumber, pos.Lineage.RowNumber)
	}
	if pos.Lineage.FileName != exp.fileName {
		t.Errorf("Lineage.FileName: expected %q, got %q", exp.fileName, pos.Lineage.FileName)
	}
}

func TestRowParser_Parse_Success(t *testing.T) {
	meta := sampleTestMeta()
	colIndex := sampleTestColIndex()
	parser := newRowParser(colIndex, meta)

	tests := []struct {
		name        string
		row         []string
		wantAccount string
		wantSymbol  string
		wantDesc    string
		wantQty     float64
		wantPrice   float64
		wantValue   float64
		wantCost    float64
	}{
		{
			name:        "standard equity position with row account id",
			row:         []string{"ACCT-12345", "Individual", "AAPL", "Apple Inc.", "10.5", "$150.25", "$1,577.63", "$1,400.00"},
			wantAccount: "ACCT-12345",
			wantSymbol:  "AAPL",
			wantDesc:    "Apple Inc.",
			wantQty:     10.5,
			wantPrice:   150.25,
			wantValue:   1577.63,
			wantCost:    1400.00,
		},
		{
			name:        "placeholder account id falls back to metadata account id",
			row:         []string{"-", "Rollover IRA", "MSFT", "Microsoft Corp.", "5", "$300.00", "$1,500.00", "$1,200.00"},
			wantAccount: meta.AccountID,
			wantSymbol:  "MSFT",
			wantDesc:    "Microsoft Corp.",
			wantQty:     5.0,
			wantPrice:   300.00,
			wantValue:   1500.00,
			wantCost:    1200.00,
		},
		{
			name:        "cash holding with double asterisk symbol normalizes to CASH",
			row:         []string{"ACCT-CASH-1", "Individual", "**", "HELD IN MONEY MARKET", "250.00", "$1.00", "$250.00", "$250.00"},
			wantAccount: "ACCT-CASH-1",
			wantSymbol:  "CASH",
			wantDesc:    "HELD IN MONEY MARKET",
			wantQty:     250.00,
			wantPrice:   1.00,
			wantValue:   250.00,
			wantCost:    250.00,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, ok, err := parser.parse(tc.row, 10)
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if !ok {
				t.Fatalf("expected parse to return ok=true, got ok=false")
			}
			assertExpectedPosition(t, pos, expectedPositionMetrics{
				accountID:    tc.wantAccount,
				symbol:       tc.wantSymbol,
				description:  tc.wantDesc,
				quantity:     tc.wantQty,
				lastPrice:    tc.wantPrice,
				currentValue: tc.wantValue,
				costBasis:    tc.wantCost,
				rowNumber:    10,
				fileName:     meta.FileName,
			})
		})
	}
}

func TestRowParser_Parse_IgnoredRows(t *testing.T) {
	meta := sampleTestMeta()
	colIndex := sampleTestColIndex()
	parser := newRowParser(colIndex, meta)

	ignoredRows := []struct {
		name string
		row  []string
	}{
		{name: "empty slice", row: []string{}},
		{name: "all blank fields", row: []string{"", "  ", "\t"}},
		{name: "disclaimer row", row: []string{"Please note that returns are not guaranteed", "", "", "", "", "", "", ""}},
		{name: "summary keyword in symbol", row: []string{"ACCT-1", "Individual", "Total", "Total Holdings", "100", "$10.00", "$1,000.00", "$900.00"}},
		{name: "summary keyword in description with blank symbol", row: []string{"ACCT-1", "Individual", "", "Total Market Value", "0", "$0.00", "$50,000.00", "$40,000.00"}},
		{name: "blank symbol with non-cash description", row: []string{"ACCT-1", "Individual", "", "Acme Corp Common Stock", "10", "$5.00", "$50.00", "$50.00"}},
	}

	for _, tc := range ignoredRows {
		t.Run(tc.name, func(t *testing.T) {
			pos, ok, err := parser.parse(tc.row, 5)
			if err != nil {
				t.Fatalf("unexpected error parsing ignored row: %v", err)
			}
			if ok {
				t.Errorf("expected ok=false for ignored row, got ok=true (pos: %+v)", pos)
			}
		})
	}
}

func TestRowParser_Parse_Errors(t *testing.T) {
	meta := sampleTestMeta()
	colIndex := sampleTestColIndex()
	parser := newRowParser(colIndex, meta)

	errorRows := []struct {
		name      string
		row       []string
		errSubstr string
	}{
		{
			name:      "invalid quantity",
			row:       []string{"ACCT-1", "Indiv", "AAPL", "Apple Inc.", "not_a_number", "$150.00", "$1,500.00", "$1,200.00"},
			errSubstr: "row 7 quantity",
		},
		{
			name:      "invalid last price",
			row:       []string{"ACCT-1", "Indiv", "AAPL", "Apple Inc.", "10", "bad_price", "$1,500.00", "$1,200.00"},
			errSubstr: "row 7 last price",
		},
		{
			name:      "invalid current value",
			row:       []string{"ACCT-1", "Indiv", "AAPL", "Apple Inc.", "10", "$150.00", "bad_value", "$1,200.00"},
			errSubstr: "row 7 current value",
		},
		{
			name:      "invalid cost basis",
			row:       []string{"ACCT-1", "Indiv", "AAPL", "Apple Inc.", "10", "$150.00", "$1,500.00", "bad_cost"},
			errSubstr: "row 7 cost basis",
		},
	}

	for _, tc := range errorRows {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, err := parser.parse(tc.row, 7)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
			}
			if ok {
				t.Errorf("expected ok=false on error, got ok=true")
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("expected error containing %q, got %q", tc.errSubstr, err.Error())
			}
		})
	}

	t.Run("validation error on missing account id in row and meta", func(t *testing.T) {
		emptyMeta := model.FileMetadata{
			FileName: "empty.csv",
			FileHash: "h1",
		}
		emptyParser := newRowParser(colIndex, emptyMeta)
		row := []string{
			"", "Indiv", "AAPL", "Apple Inc.",
			"10", "$150.00", "$1,500.00", "$1,200.00",
		}
		_, ok, err := emptyParser.parse(row, 12)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}
		if ok {
			t.Errorf("expected ok=false on validation failure, got ok=true")
		}
		if !strings.Contains(err.Error(), "row 12 validation error") {
			t.Errorf("expected error containing 'row 12 validation error', got %q", err.Error())
		}
	})
}

func TestRowParser_GetString(t *testing.T) {
	colIndex := map[string]int{
		"primary":   0,
		"secondary": 1,
		"overflow":  5,
	}
	parser := newRowParser(colIndex, sampleTestMeta())

	row := []string{"  first_value  ", "second_value"}

	t.Run("finds primary alias and trims whitespace", func(t *testing.T) {
		val := parser.getString(row, "primary")
		if val != "first_value" {
			t.Errorf("expected 'first_value', got %q", val)
		}
	})

	t.Run("finds secondary alias when first not found", func(t *testing.T) {
		val := parser.getString(row, "missing_alias", "secondary")
		if val != "second_value" {
			t.Errorf("expected 'second_value', got %q", val)
		}
	})

	t.Run("returns empty string when column index exceeds row length", func(t *testing.T) {
		val := parser.getString(row, "overflow")
		if val != "" {
			t.Errorf("expected empty string for overflow index, got %q", val)
		}
	})

	t.Run("returns empty string when no aliases match", func(t *testing.T) {
		val := parser.getString(row, "unknown_col_1", "unknown_col_2")
		if val != "" {
			t.Errorf("expected empty string for missing aliases, got %q", val)
		}
	})
}

func TestRowParser_ResolveAccountID(t *testing.T) {
	meta := sampleTestMeta()

	tests := []struct {
		name        string
		colIndex    map[string]int
		row         []string
		wantAccount string
	}{
		{
			name:        "explicit account number present",
			colIndex:    map[string]int{"account number": 0},
			row:         []string{"ACCT-DIRECT-77"},
			wantAccount: "ACCT-DIRECT-77",
		},
		{
			name:        "hyphen placeholder falls back to metadata account",
			colIndex:    map[string]int{"account id": 0},
			row:         []string{"-"},
			wantAccount: meta.AccountID,
		},
		{
			name:        "double hyphen placeholder falls back to metadata account",
			colIndex:    map[string]int{"account #": 0},
			row:         []string{"--"},
			wantAccount: meta.AccountID,
		},
		{
			name:        "n/a placeholder falls back to metadata account",
			colIndex:    map[string]int{"account": 0},
			row:         []string{"n/a"},
			wantAccount: meta.AccountID,
		},
		{
			name:        "none placeholder falls back to metadata account",
			colIndex:    map[string]int{"account number": 0},
			row:         []string{"none"},
			wantAccount: meta.AccountID,
		},
		{
			name:        "missing account column in colIndex falls back to metadata",
			colIndex:    map[string]int{"symbol": 0},
			row:         []string{"AAPL"},
			wantAccount: meta.AccountID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := newRowParser(tc.colIndex, meta)
			got := parser.resolveAccountID(tc.row)
			if got != tc.wantAccount {
				t.Errorf("expected account ID %q, got %q", tc.wantAccount, got)
			}
		})
	}
}

func TestIsSummaryRow(t *testing.T) {
	tests := []struct {
		name        string
		symbol      string
		description string
		want        bool
	}{
		{name: "exact total in symbol", symbol: "total", description: "", want: true},
		{name: "exact account total in symbol", symbol: "account total", description: "", want: true},
		{name: "exact portfolio total in symbol", symbol: "portfolio total", description: "", want: true},
		{name: "exact total market value in symbol", symbol: "total market value", description: "", want: true},
		{name: "exact ending value in symbol", symbol: "ending value", description: "", want: true},
		{name: "prefixed total in symbol", symbol: "Total Holdings", description: "", want: true},
		{name: "prefixed account total in symbol", symbol: "Account Total Value", description: "", want: true},
		{name: "prefixed portfolio total in symbol", symbol: "Portfolio Total Value", description: "", want: true},
		{name: "legitimate fund holding with total in description VTI", symbol: "VTI", description: "Vanguard Total Stock Market Index Fund", want: false},
		{name: "legitimate fund holding with total in description TTE", symbol: "TTE", description: "TotalEnergies SE", want: false},
		{name: "empty symbol with total in description", symbol: "", description: "Total Market Value", want: true},
		{name: "asterisks symbol with total in description", symbol: "**", description: "Portfolio Total Value", want: true},
		{name: "regular equity holding", symbol: "AAPL", description: "Apple Inc.", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isSummaryRow(tc.symbol, tc.description)
			if got != tc.want {
				t.Errorf("isSummaryRow(%q, %q): expected %v, got %v", tc.symbol, tc.description, tc.want, got)
			}
		})
	}
}

func TestNormalizeHoldingSymbol(t *testing.T) {
	tests := []struct {
		name        string
		symbol      string
		description string
		wantSymbol  string
		wantOK      bool
	}{
		{name: "regular stock AAPL", symbol: "AAPL", description: "Apple Inc.", wantSymbol: "AAPL", wantOK: true},
		{name: "regular ETF VTI", symbol: "VTI", description: "Vanguard Total Stock Market", wantSymbol: "VTI", wantOK: true},
		{name: "asterisks with money market desc", symbol: "**", description: "HELD IN MONEY MARKET", wantSymbol: "CASH", wantOK: true},
		{name: "asterisks with blank desc", symbol: "**", description: "", wantSymbol: "CASH", wantOK: true},
		{name: "symbol FCASH upper", symbol: "FCASH", description: "Fidelity Cash", wantSymbol: "CASH", wantOK: true},
		{name: "symbol fcash lower", symbol: "fcash", description: "fidelity cash", wantSymbol: "CASH", wantOK: true},
		{name: "symbol CORE upper", symbol: "CORE", description: "Core Sweep", wantSymbol: "CASH", wantOK: true},
		{name: "symbol core lower", symbol: "core", description: "core account", wantSymbol: "CASH", wantOK: true},
		{name: "blank symbol with deposit sweep", symbol: "", description: "DEPOSIT SWEEP PROGRAM", wantSymbol: "CASH", wantOK: true},
		{name: "blank symbol with non-cash asset", symbol: "", description: "Some Common Stock", wantSymbol: "", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sym, ok := normalizeHoldingSymbol(tc.symbol, tc.description)
			if sym != tc.wantSymbol || ok != tc.wantOK {
				t.Errorf("normalizeHoldingSymbol(%q, %q): expected (%q, %v), got (%q, %v)",
					tc.symbol, tc.description, tc.wantSymbol, tc.wantOK, sym, ok)
			}
		})
	}
}

func TestIsPlaceholderAccountID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{input: "", want: true},
		{input: "-", want: true},
		{input: "--", want: true},
		{input: "n/a", want: true},
		{input: "N/A", want: true},
		{input: "none", want: true},
		{input: "NONE", want: true},
		{input: "123456789", want: false},
		{input: "Z12345678", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := isPlaceholderAccountID(tc.input)
			if got != tc.want {
				t.Errorf("isPlaceholderAccountID(%q): expected %v, got %v", tc.input, tc.want, got)
			}
		})
	}
}

func TestIsCashDescription(t *testing.T) {
	tests := []struct {
		desc string
		want bool
	}{
		{desc: "HELD IN MONEY MARKET", want: true},
		{desc: "CASH BALANCE", want: true},
		{desc: "FCASH SWEEP", want: true},
		{desc: "CORE CASH", want: true},
		{desc: "DEPOSIT SWEEP PROGRAM", want: true},
		{desc: "SWEEP VEHICLE", want: true},
		{desc: "MONEY MARKET FUND", want: true},
		{desc: "COMMON STOCK", want: false},
		{desc: "INDEX MUTUAL FUND", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := isCashDescription(tc.desc)
			if got != tc.want {
				t.Errorf("isCashDescription(%q): expected %v, got %v", tc.desc, tc.want, got)
			}
		})
	}
}

func TestIsFidelityDisclaimerOrEmpty(t *testing.T) {
	tests := []struct {
		name   string
		record []string
		want   bool
	}{
		{name: "empty slice", record: []string{}, want: true},
		{name: "all blank fields", record: []string{"", "   ", "\t"}, want: true},
		{name: "disclaimer data and info", record: []string{"", "The data and information provided are..."}, want: true},
		{name: "disclaimer brokerage services", record: []string{"Brokerage services are provided by..."}, want: true},
		{name: "date downloaded footer handled by extractor not disclaimer filter", record: []string{"Date downloaded 10/18/2026"}, want: false},
		{name: "disclaimer total keyword", record: []string{"total"}, want: true},
		{name: "disclaimer total market", record: []string{"Total Market Value"}, want: true},
		{name: "disclaimer account total", record: []string{"Account Total:"}, want: true},
		{name: "disclaimer portfolio total", record: []string{"Portfolio Total:"}, want: true},
		{name: "disclaimer ending value", record: []string{"Ending Value"}, want: true},
		{name: "disclaimer generic prefix", record: []string{"Disclaimer: not investment advice"}, want: true},
		{name: "disclaimer please note", record: []string{"Please note quotes are 15 mins delayed"}, want: true},
		{name: "disclaimer important", record: []string{"Important disclosures below"}, want: true},
		{name: "disclaimer asterisk footnote", record: []string{"*Unrealized gain/loss is an estimate"}, want: true},
		{name: "asterisk symbol ** is NOT disclaimer", record: []string{"**", "MONEY MARKET"}, want: false},
		{name: "disclaimer numbered footnote", record: []string{"1. Historical pricing provided by third parties"}, want: true},
		{name: "disclaimer quotes are", record: []string{"Quotes are delayed unless real-time selected"}, want: true},
		{name: "disclaimer security prices", record: []string{"Security prices are obtained from..."}, want: true},
		{name: "valid equity data row", record: []string{"ACCT-1", "Individual", "AAPL", "Apple Inc."}, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isFidelityDisclaimerOrEmpty(tc.record)
			if got != tc.want {
				t.Errorf("isFidelityDisclaimerOrEmpty(%v): expected %v, got %v", tc.record, tc.want, got)
			}
		})
	}
}

func TestParseFinancialNumber(t *testing.T) {
	tests := []struct {
		name        string
		val         string
		stripDollar bool
		fieldName   string
		want        float64
		wantErr     bool
		errSubstr   string
	}{
		{name: "standard currency with dollar", val: "$1,234.56", stripDollar: true, fieldName: "currency", want: 1234.56},
		{name: "standard quantity without dollar", val: "1,234.56", stripDollar: false, fieldName: "quantity", want: 1234.56},
		{name: "quantity rejects dollar sign when stripDollar is false", val: "$100.00", stripDollar: false, fieldName: "quantity", wantErr: true, errSubstr: "invalid quantity value"},
		{name: "plus sign currency", val: "+$500.00", stripDollar: true, fieldName: "currency", want: 500.00},
		{name: "accounting negative parentheses with dollar", val: "($50.00)", stripDollar: true, fieldName: "currency", want: -50.00},
		{name: "accounting negative parentheses outer dollar", val: "$(50.00)", stripDollar: true, fieldName: "currency", want: -50.00},
		{name: "unicode non-breaking space", val: "$1\u00a0000.50", stripDollar: true, fieldName: "currency", want: 1000.50},
		{name: "unicode narrow non-breaking space", val: "$1\u202f000.50", stripDollar: true, fieldName: "currency", want: 1000.50},
		{name: "empty string defaults to zero", val: "", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "single dash defaults to zero", val: "-", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "double dash defaults to zero", val: "--", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "n/a defaults to zero", val: "N/A", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "unknown defaults to zero", val: "unknown", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "pending defaults to zero", val: "Pending", stripDollar: true, fieldName: "currency", want: 0.0},
		{name: "non-finite NaN rejection", val: "NaN", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "non-finite float value"},
		{name: "non-finite lowercase nan rejection", val: "nan", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "non-finite float value"},
		{name: "non-finite positive Inf rejection", val: "+Inf", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "non-finite float value"},
		{name: "non-finite negative Inf rejection", val: "-Inf", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "non-finite float value"},
		{name: "non-finite Infinity rejection", val: "Infinity", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "non-finite float value"},
		{name: "malformed text string", val: "not_a_number", stripDollar: true, fieldName: "currency", wantErr: true, errSubstr: "invalid currency value"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFinancialNumber(tc.val, tc.stripDollar, tc.fieldName)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("expected error containing %q, got %q", tc.errSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if math.Abs(got-tc.want) > 1e-6 {
				t.Errorf("expected %f, got %f", tc.want, got)
			}
		})
	}
}

func TestFindColumn(t *testing.T) {
	colIndex := map[string]int{
		"account number": 0,
		"symbol":         1,
		"last price":     2,
	}

	t.Run("found by primary alias exact case", func(t *testing.T) {
		idx, ok := findColumn(colIndex, "account number")
		if !ok || idx != 0 {
			t.Errorf("expected (0, true), got (%d, %v)", idx, ok)
		}
	})

	t.Run("found by case-insensitive alias", func(t *testing.T) {
		idx, ok := findColumn(colIndex, "SYMBOL")
		if !ok || idx != 1 {
			t.Errorf("expected (1, true), got (%d, %v)", idx, ok)
		}
	})

	t.Run("found by secondary alias", func(t *testing.T) {
		idx, ok := findColumn(colIndex, "ticker", "symbol")
		if !ok || idx != 1 {
			t.Errorf("expected (1, true), got (%d, %v)", idx, ok)
		}
	})

	t.Run("not found returns -1 and false", func(t *testing.T) {
		idx, ok := findColumn(colIndex, "nonexistent", "unknown")
		if ok || idx != -1 {
			t.Errorf("expected (-1, false), got (%d, %v)", idx, ok)
		}
	})
}

func sampleLegacy16ColIndex() map[string]int {
	return map[string]int{
		"account number":            0,
		"account name":              1,
		"symbol":                    2,
		"description":               3,
		"quantity":                  4,
		"last price":                5,
		"last price change":         6,
		"current value":             7,
		"today's gain/loss dollar":  8,
		"today's gain/loss percent": 9,
		"total gain/loss dollar":    10,
		"total gain/loss percent":   11,
		"percent of account":        12,
		"cost basis total":          13,
		"average cost basis":        14,
		"type":                      15,
	}
}

func TestParsePercent(t *testing.T) {
	tests := []struct {
		name      string
		val       string
		want      float64
		wantErr   bool
		errSubstr string
	}{
		{name: "positive percent with plus sign", val: "+0.84%", want: 0.84},
		{name: "negative percent with minus sign", val: "-0.44%", want: -0.44},
		{name: "standard unsigned percent", val: "52.21%", want: 52.21},
		{name: "thousands comma and plus sign", val: "+1,250.00%", want: 1250.00},
		{name: "surrounding and inner whitespace before percent sign", val: " 25.00 % ", want: 25.00},
		{name: "empty string defaults to zero", val: "", want: 0.0},
		{name: "single dash defaults to zero", val: "-", want: 0.0},
		{name: "double dash defaults to zero", val: "--", want: 0.0},
		{name: "lowercase n/a defaults to zero", val: "n/a", want: 0.0},
		{name: "uppercase N/A defaults to zero", val: "N/A", want: 0.0},
		{name: "bare percent sign rejected", val: "%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "single dash with percent sign rejected", val: "-%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "double dash with percent sign rejected", val: "--%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "plus sign with percent sign rejected", val: "+%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "N/A with percent sign rejected", val: "N/A%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "alphabetic string with percent sign rejected", val: "abc%", wantErr: true, errSubstr: "invalid percent value"},
		{name: "NaN with percent sign rejected", val: "NaN%", wantErr: true, errSubstr: "non-finite float value"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runParsePercentCase(t, tc.val, tc.want, tc.wantErr, tc.errSubstr)
		})
	}
}

func runParsePercentCase(t *testing.T, val string, want float64, wantErr bool, errSubstr string) {
	t.Helper()
	got, err := parsePercent(val)
	if wantErr {
		if err == nil {
			t.Fatalf("expected error for %q, got nil", val)
		}
		if errSubstr != "" && !strings.Contains(err.Error(), errSubstr) {
			t.Errorf("expected error containing %q, got %q", errSubstr, err.Error())
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error for %q: %v", val, err)
	}
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("parsePercent(%q): expected %f, got %f", val, want, got)
	}
}

type legacyFieldExpectation struct {
	lastPriceChange      float64
	todaysGainLossDollar float64
	todaysGainLossPct    float64
	totalGainLossDollar  float64
	totalGainLossPct     float64
	percentOfAccount     float64
	averageCostBasis     float64
	posType              string
}

func assertLegacyParityFields(t *testing.T, got model.FidelityPosition, want legacyFieldExpectation) {
	t.Helper()
	if math.Abs(got.LastPriceChange-want.lastPriceChange) > 1e-6 {
		t.Errorf("LastPriceChange: expected %f, got %f", want.lastPriceChange, got.LastPriceChange)
	}
	if math.Abs(got.TodaysGainLossDollar-want.todaysGainLossDollar) > 1e-6 {
		t.Errorf("TodaysGainLossDollar: expected %f, got %f", want.todaysGainLossDollar, got.TodaysGainLossDollar)
	}
	if math.Abs(got.TodaysGainLossPct-want.todaysGainLossPct) > 1e-6 {
		t.Errorf("TodaysGainLossPct: expected %f, got %f", want.todaysGainLossPct, got.TodaysGainLossPct)
	}
	if math.Abs(got.TotalGainLossDollar-want.totalGainLossDollar) > 1e-6 {
		t.Errorf("TotalGainLossDollar: expected %f, got %f", want.totalGainLossDollar, got.TotalGainLossDollar)
	}
	if math.Abs(got.TotalGainLossPct-want.totalGainLossPct) > 1e-6 {
		t.Errorf("TotalGainLossPct: expected %f, got %f", want.totalGainLossPct, got.TotalGainLossPct)
	}
	if math.Abs(got.PercentOfAccount-want.percentOfAccount) > 1e-6 {
		t.Errorf("PercentOfAccount: expected %f, got %f", want.percentOfAccount, got.PercentOfAccount)
	}
	if math.Abs(got.AverageCostBasis-want.averageCostBasis) > 1e-6 {
		t.Errorf("AverageCostBasis: expected %f, got %f", want.averageCostBasis, got.AverageCostBasis)
	}
	if got.Type != want.posType {
		t.Errorf("Type: expected %q, got %q", want.posType, got.Type)
	}
}

func TestRowParser_LegacyFieldParity_Fixture(t *testing.T) {
	parser := newRowParser(sampleLegacy16ColIndex(), sampleTestMeta())

	// Golden values verified against legacy datalake/model/fidelity_parser.go:133-167 (MapRawRecordToFidelityPosition).
	tests := []struct {
		name string
		row  []string
		want legacyFieldExpectation
	}{
		{
			name: "FSKAX positive daily and total gain/loss metrics (datalake/model/fidelity_parser.go:140-160)",
			row: []string{
				"SYNTH-ROTH-101", "ROTH IRA", "FSKAX", "FIDELITY TOTAL MARKET INDEX",
				"45.250", "$150.00", "+$1.25", "$6787.50",
				"+$56.56", "+0.84%", "+$787.50", "+13.13%",
				"52.21%", "$6000.00", "$132.59", "Cash",
			},
			want: legacyFieldExpectation{
				lastPriceChange:      1.25,
				todaysGainLossDollar: 56.56,
				todaysGainLossPct:    0.84,
				totalGainLossDollar:  787.50,
				totalGainLossPct:     13.13,
				percentOfAccount:     52.21,
				averageCostBasis:     132.59,
				posType:              "Cash",
			},
		},
		{
			name: "FXAIX negative daily gain/loss and positive total gain/loss (datalake/model/fidelity_parser.go:140-160)",
			row: []string{
				"SYNTH-401K-202", "TRADITIONAL 401K", "FXAIX", "FIDELITY 500 INDEX",
				"25.500", "$180.20", "-$0.80", "$4595.10",
				"-$20.40", "-0.44%", "+$595.10", "+14.88%",
				"100.00%", "$4000.00", "$156.86", "Cash",
			},
			want: legacyFieldExpectation{
				lastPriceChange:      -0.80,
				todaysGainLossDollar: -20.40,
				todaysGainLossPct:    -0.44,
				totalGainLossDollar:  595.10,
				totalGainLossPct:     14.88,
				percentOfAccount:     100.00,
				averageCostBasis:     156.86,
				posType:              "Cash",
			},
		},
		{
			name: "SPAXX** money market row with blank performance columns (datalake/model/fidelity_parser.go:140-160)",
			row: []string{
				"SYNTH-ROTH-101", "ROTH IRA", "SPAXX**", "HELD IN MONEY MARKET",
				"", "", "", "$3250.00",
				"", "", "", "",
				"25.00%", "", "", "Cash",
			},
			want: legacyFieldExpectation{
				lastPriceChange:      0.0,
				todaysGainLossDollar: 0.0,
				todaysGainLossPct:    0.0,
				totalGainLossDollar:  0.0,
				totalGainLossPct:     0.0,
				percentOfAccount:     25.00,
				averageCostBasis:     0.0,
				posType:              "Cash",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, ok, err := parser.parse(tc.row, 2)
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if !ok {
				t.Fatalf("expected ok=true, got false")
			}
			assertLegacyParityFields(t, pos, tc.want)
		})
	}
}

func TestRowParser_LegacyFields_PlaceholdersAreZero(t *testing.T) {
	parser := newRowParser(sampleLegacy16ColIndex(), sampleTestMeta())

	// SYNTH-TAX-303 ** row with "--" placeholders across performance and cost columns.
	row := []string{
		"SYNTH-TAX-303", "INDIVIDUAL TOD", "**", "",
		"--", "--", "--", "$0.00",
		"--", "", "--", "--",
		"--", "--", "--", "",
	}

	pos, ok, err := parser.parse(row, 6)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true for SYNTH-TAX-303 cash placeholder row, got false")
	}
	if pos.Symbol != "CASH" {
		t.Errorf("expected normalized symbol 'CASH', got %q", pos.Symbol)
	}
	assertLegacyParityFields(t, pos, legacyFieldExpectation{})
}

func TestRowParser_LegacyFields_AbsentColumnsAreZero(t *testing.T) {
	// 8-column header lacking all 8 legacy performance/allocation/type columns.
	parser := newRowParser(sampleTestColIndex(), sampleTestMeta())
	row := []string{"ACCT-1", "Individual", "AAPL", "Apple Inc.", "10", "$150.00", "$1,500.00", "$1,200.00"}

	pos, ok, err := parser.parse(row, 3)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	assertLegacyParityFields(t, pos, legacyFieldExpectation{})
}

func TestRowParser_LegacyFields_InvalidValueFailsRow(t *testing.T) {
	parser := newRowParser(sampleLegacy16ColIndex(), sampleTestMeta())

	validBaseRow := func() []string {
		return []string{
			"SYNTH-ROTH-101", "ROTH IRA", "FSKAX", "FIDELITY TOTAL MARKET INDEX",
			"45.250", "$150.00", "+$1.25", "$6787.50",
			"+$56.56", "+0.84%", "+$787.50", "+13.13%",
			"52.21%", "$6000.00", "$132.59", "Cash",
		}
	}

	tests := []struct {
		name      string
		colIdx    int
		badVal    string
		errSubstr string
	}{
		{name: "invalid last price change", colIdx: 6, badVal: "bad_chg", errSubstr: "row 9 last price change"},
		{name: "invalid todays gain/loss dollar", colIdx: 8, badVal: "bad_td_dlr", errSubstr: "row 9 todays gain/loss dollar"},
		{name: "invalid todays gain/loss percent", colIdx: 9, badVal: "bad_td_pct%", errSubstr: "row 9 todays gain/loss percent"},
		{name: "invalid total gain/loss dollar", colIdx: 10, badVal: "bad_tot_dlr", errSubstr: "row 9 total gain/loss dollar"},
		{name: "invalid total gain/loss percent", colIdx: 11, badVal: "--%", errSubstr: "row 9 total gain/loss percent"},
		{name: "invalid percent of account", colIdx: 12, badVal: "%", errSubstr: "row 9 percent of account"},
		{name: "invalid average cost basis", colIdx: 14, badVal: "bad_avg_cost", errSubstr: "row 9 average cost basis"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := validBaseRow()
			row[tc.colIdx] = tc.badVal

			_, ok, err := parser.parse(row, 9)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
			}
			if ok {
				t.Errorf("expected ok=false on error, got true")
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("expected error containing %q, got %q", tc.errSubstr, err.Error())
			}
		})
	}
}

func TestRowParser_LegacyFields_TypeTrimmedVerbatim(t *testing.T) {
	parser := newRowParser(sampleLegacy16ColIndex(), sampleTestMeta())
	row := []string{
		"SYNTH-ROTH-101", "ROTH IRA", "FSKAX", "FIDELITY TOTAL MARKET INDEX",
		"45.250", "$150.00", "+$1.25", "$6787.50",
		"+$56.56", "+0.84%", "+$787.50", "+13.13%",
		"52.21%", "$6000.00", "$132.59", "  Margin  ",
	}

	pos, ok, err := parser.parse(row, 4)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if pos.Type != "Margin" {
		t.Errorf("expected Type 'Margin' (trimmed verbatim without case folding), got %q", pos.Type)
	}
}
