package chase

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestCleanNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    string
		wantNeg bool
		wantOK  bool
	}{
		{name: "standard currency", input: "$1,234.56", want: "1234.56", wantOK: true},
		{name: "number with comma", input: "1,234.56", want: "1234.56", wantOK: true},
		{name: "millions grouping", input: "1,234,567", want: "1234567", wantOK: true},
		{name: "plus sign currency", input: "+$500.00", want: "500.00", wantOK: true},
		{name: "accounting parentheses", input: "($50.00)", want: "50.00", wantNeg: true, wantOK: true},
		{name: "outer dollar accounting parentheses", input: "$(45.67)", want: "45.67", wantNeg: true, wantOK: true},
		{name: "unicode non-breaking space", input: "$\u00a01,000.50", want: "1000.50", wantOK: true},
		{name: "narrow non-breaking space", input: "$\u202f1,000.50", want: "1000.50", wantOK: true},
		{name: "spaces and tabs", input: "$ 1 000 . 50 \t", want: "1000.50", wantOK: true},
		{name: "unicode minus sign", input: "\u22124.50", want: "4.50", wantNeg: true, wantOK: true},
		{name: "en dash with dollar and grouping", input: "\u2013$1,234.00", want: "1234.00", wantNeg: true, wantOK: true},
		{name: "em dash", input: "\u20144.50", want: "4.50", wantNeg: true, wantOK: true},
		{name: "dollar before unicode minus", input: "$\u22124.50", want: "4.50", wantNeg: true, wantOK: true},
		{name: "fullwidth hyphen-minus", input: "\uff0d4.50", want: "4.50", wantNeg: true, wantOK: true},
		{name: "single decimal place", input: "4.5", want: "4.5", wantOK: true},
		{name: "empty string rejected", input: "", wantOK: false},
		{name: "single dash rejected without zero coercion", input: "-", wantOK: false},
		{name: "double dash rejected without zero coercion", input: "--", wantOK: false},
		{name: "currency with single dash rejected", input: "$ -", wantOK: false},
		{name: "n/a rejected", input: "n/a", wantOK: false},
		{name: "comma decimal separator rejected", input: "12,34", wantOK: false},
		{name: "misplaced grouping rejected", input: "1,2,3", wantOK: false},
		{name: "oversized leading group rejected", input: "1234,567", wantOK: false},
		{name: "trailing comma rejected", input: "1,234,", wantOK: false},
		{name: "interior plus rejected", input: "1+2", wantOK: false},
		{name: "hex float rejected", input: "0x1p4", wantOK: false},
		{name: "exponent rejected", input: "1e3", wantOK: false},
		{name: "underscore separator rejected", input: "1_000", wantOK: false},
		{name: "three decimal places rejected", input: "4.505", wantOK: false},
		{name: "trailing dot rejected", input: "4.", wantOK: false},
		{name: "leading dot rejected", input: ".50", wantOK: false},
		{name: "trailing minus rejected", input: "4.50-", wantOK: false},
		{name: "double sign rejected", input: "--4.50", wantOK: false},
		{name: "sign inside parentheses rejected", input: "(-4.50)", wantOK: false},
		{name: "embedded dollar rejected", input: "4$.50", wantOK: false},
		{name: "NaN rejected", input: "NaN", wantOK: false},
		{name: "Inf rejected", input: "+Inf", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, neg, ok := cleanNumeric(tc.input)
			if ok != tc.wantOK || got != tc.want || neg != tc.wantNeg {
				t.Errorf("cleanNumeric(%q): expected (%q, %v, %v), got (%q, %v, %v)",
					tc.input, tc.want, tc.wantNeg, tc.wantOK, got, neg, ok)
			}
		})
	}
}

func assertAmountResult(t *testing.T, got float64, err error, want float64, wantErr bool, errSubstr string) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", errSubstr)
		}
		if !strings.Contains(err.Error(), errSubstr) {
			t.Errorf("expected error containing %q, got %q", errSubstr, err.Error())
		}
		if !errors.Is(err, model.ErrInvalidAmount) {
			t.Errorf("expected errors.Is(err, model.ErrInvalidAmount), got %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("expected %f, got %f", want, got)
	}
}

func TestParseAmount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		want      float64
		wantErr   bool
		errSubstr string
	}{
		{name: "standard positive amount", raw: "150.25", want: 150.25},
		{name: "negative accounting parentheses", raw: "($75.00)", want: -75.00},
		{name: "outer dollar negative", raw: "$(120.50)", want: -120.50},
		{name: "empty amount returns error", raw: "", wantErr: true, errSubstr: "transaction amount is required"},
		{name: "single dash fails with invalid amount", raw: "-", wantErr: true, errSubstr: "invalid amount"},
		{name: "double dash fails with invalid amount", raw: "--", wantErr: true, errSubstr: "invalid amount"},
		{name: "currency dash fails with invalid amount", raw: "$ -", wantErr: true, errSubstr: "invalid amount"},
		{name: "currency multi space dash fails with invalid amount", raw: "$   -", wantErr: true, errSubstr: "invalid amount"},
		{name: "N/A uppercase fails with invalid amount", raw: "N/A", wantErr: true, errSubstr: "invalid amount"},
		{name: "n/a lowercase fails with invalid amount", raw: "n/a", wantErr: true, errSubstr: "invalid amount"},
		{name: "alphabetic string error", raw: "not_a_number", wantErr: true, errSubstr: "invalid amount"},
		{name: "non-finite NaN", raw: "NaN", wantErr: true, errSubstr: "invalid amount"},
		{name: "non-finite lowercase nan", raw: "nan", wantErr: true, errSubstr: "invalid amount"},
		{name: "non-finite positive Inf", raw: "+Inf", wantErr: true, errSubstr: "invalid amount"},
		{name: "non-finite negative Inf", raw: "-Inf", wantErr: true, errSubstr: "invalid amount"},
		{name: "non-finite Infinity", raw: "Infinity", wantErr: true, errSubstr: "invalid amount"},
		{name: "out of range magnitude", raw: "1" + strings.Repeat("0", 400), wantErr: true, errSubstr: "invalid amount"},
		{name: "comma decimal separator rejected", raw: "12,34", wantErr: true, errSubstr: `row 5: invalid amount "12,34"`},
		{name: "misplaced grouping rejected", raw: "1,2,3", wantErr: true, errSubstr: "invalid amount"},
		{name: "interior plus rejected", raw: "1+2", wantErr: true, errSubstr: `invalid amount "1+2"`},
		{name: "hex float rejected", raw: "0x1p4", wantErr: true, errSubstr: `invalid amount "0x1p4"`},
		{name: "exponent rejected", raw: "1e3", wantErr: true, errSubstr: "invalid amount"},
		{name: "underscore separator rejected", raw: "1_000", wantErr: true, errSubstr: "invalid amount"},
		{name: "trailing minus rejected", raw: "4.50-", wantErr: true, errSubstr: `row 5: invalid amount "4.50-"`},
		{name: "unicode minus sign", raw: "\u22124.50", want: -4.50},
		{name: "en dash with dollar and grouping", raw: "\u2013$1,234.00", want: -1234.00},
		{name: "dollar before unicode minus", raw: "$\u22124.50", want: -4.50},
		{name: "grouped thousands", raw: "$1,234,567.89", want: 1234567.89},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseAmount(tc.raw, 5)
			assertAmountResult(t, got, err, tc.want, tc.wantErr, tc.errSubstr)
		})
	}
}

func TestParseDescription(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		want      string
		wantErr   bool
		errSubstr string
	}{
		{name: "standard description", raw: "Starbucks Coffee", want: "Starbucks Coffee"},
		{name: "trimmed description", raw: "   Amazon.com   ", want: "Amazon.com"},
		{name: "empty description", raw: "", wantErr: true, errSubstr: "row 10: description is required"},
		{name: "all whitespace description", raw: "   \t   ", wantErr: true, errSubstr: "row 10: description is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseDescription(tc.raw, 10)
			assertDescriptionResult(t, got, err, tc.want, tc.wantErr, tc.errSubstr)
		})
	}
}

func assertDescriptionResult(t *testing.T, got string, err error, want string, wantErr bool, errSubstr string) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", errSubstr)
		}
		if !strings.Contains(err.Error(), errSubstr) {
			t.Errorf("expected error containing %q, got %q", errSubstr, err.Error())
		}
		if !errors.Is(err, model.ErrMissingDescription) {
			t.Errorf("expected errors.Is(err, model.ErrMissingDescription), got %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func assertBalanceResult(t *testing.T, bal *float64, err error, wantVal float64, wantNil, wantErr bool, errSubstr string) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", errSubstr)
		}
		if !strings.Contains(err.Error(), errSubstr) {
			t.Errorf("expected error containing %q, got %q", errSubstr, err.Error())
		}
		if !errors.Is(err, model.ErrInvalidBalance) {
			t.Errorf("expected errors.Is(err, model.ErrInvalidBalance), got %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wantNil {
		if bal != nil {
			t.Errorf("expected nil balance, got %v", *bal)
		}
		return
	}
	if bal == nil {
		t.Fatalf("expected non-nil balance, got nil")
	}
	if math.Abs(*bal-wantVal) > 1e-6 {
		t.Errorf("expected %f, got %f", wantVal, *bal)
	}
}

func TestParseBalance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		wantVal   float64
		wantNil   bool
		wantErr   bool
		errSubstr string
	}{
		{name: "valid positive balance", raw: "$1,250.75", wantVal: 1250.75},
		{name: "valid negative balance", raw: "($20.00)", wantVal: -20.00},
		{name: "empty string is nil", raw: "", wantNil: true},
		{name: "whitespace only string is nil", raw: "   ", wantNil: true},
		{name: "single dash is nil", raw: "-", wantNil: true},
		{name: "single dash with whitespace is nil", raw: " - ", wantNil: true},
		{name: "double dash is nil", raw: "--", wantNil: true},
		{name: "double dash with whitespace is nil", raw: " -- ", wantNil: true},
		{name: "currency dash is nil", raw: "$ -", wantNil: true},
		{name: "currency multiple space dash is nil", raw: "$   -", wantNil: true},
		{name: "n/a lowercase is nil", raw: "n/a", wantNil: true},
		{name: "N/A uppercase is nil", raw: "N/A", wantNil: true},
		{name: "N/A with whitespace is nil", raw: " N/A ", wantNil: true},
		{name: "syntax error balance", raw: "corrupted_val", wantErr: true, errSubstr: "invalid balance"},
		{name: "non-finite NaN", raw: "NaN", wantErr: true, errSubstr: "invalid balance"},
		{name: "non-finite Inf", raw: "+Inf", wantErr: true, errSubstr: "invalid balance"},
		{name: "underscore separator rejected", raw: "1_000", wantErr: true, errSubstr: `row 12: invalid balance "1_000"`},
		{name: "comma decimal separator rejected", raw: "12,34", wantErr: true, errSubstr: "invalid balance"},
		{name: "unicode minus sign", raw: "\u2212200.00", wantVal: -200.00},
		{name: "unicode minus placeholder is nil", raw: "\u2212", wantNil: true},
		{name: "currency unicode dash placeholder is nil", raw: "$ \u2014", wantNil: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bal, err := parseBalance(tc.raw, 12)
			assertBalanceResult(t, bal, err, tc.wantVal, tc.wantNil, tc.wantErr, tc.errSubstr)
		})
	}
}

func assertDateResult(t *testing.T, got time.Time, err error, wantYear int, wantMonth time.Month, wantDay int, wantErr bool, errSubstr string) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", errSubstr)
		}
		if !strings.Contains(err.Error(), errSubstr) {
			t.Errorf("expected error containing %q, got %q", errSubstr, err.Error())
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Year() != wantYear || got.Month() != wantMonth || got.Day() != wantDay {
		t.Errorf("expected %04d-%02d-%02d, got %04d-%02d-%02d",
			wantYear, wantMonth, wantDay, got.Year(), got.Month(), got.Day())
	}
	if got.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", got.Location())
	}
}

func TestParseDate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		wantYear  int
		wantMonth time.Month
		wantDay   int
		wantErr   bool
		errSubstr string
	}{
		{name: "MM/DD/YYYY format", raw: "01/15/2026", wantYear: 2026, wantMonth: time.January, wantDay: 15},
		{name: "M/D/YYYY format", raw: "1/5/2026", wantYear: 2026, wantMonth: time.January, wantDay: 5},
		{name: "YYYY-MM-DD format", raw: "2026-01-15", wantYear: 2026, wantMonth: time.January, wantDay: 15},
		{name: "YYYY/01/02 format", raw: "2026/01/15", wantYear: 2026, wantMonth: time.January, wantDay: 15},
		{name: "empty date", raw: "", wantErr: true, errSubstr: "date value is empty"},
		{name: "invalid layout", raw: "2026.01.15", wantErr: true, errSubstr: "invalid date format"},
		{name: "invalid calendar date", raw: "02/31/2026", wantErr: true, errSubstr: "invalid date format"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseDate(tc.raw)
			assertDateResult(t, got, err, tc.wantYear, tc.wantMonth, tc.wantDay, tc.wantErr, tc.errSubstr)
		})
	}
}

func TestFindColumnAndGetFieldValue(t *testing.T) {
	t.Parallel()
	colIndex := map[string]int{
		"primary":   0,
		"secondary": 1,
		"overflow":  5,
	}

	row := []string{"  val_one  ", "val_two"}

	t.Run("matches primary alias", func(t *testing.T) {
		t.Parallel()
		idx, ok := findColumn(colIndex, "primary")
		if !ok || idx != 0 {
			t.Errorf("expected (0, true), got (%d, %v)", idx, ok)
		}
		val := getFieldValue(row, colIndex, "primary")
		if val != "val_one" {
			t.Errorf("expected 'val_one', got %q", val)
		}
	})

	t.Run("matches secondary alias", func(t *testing.T) {
		t.Parallel()
		idx, ok := findColumn(colIndex, "missing", "secondary")
		if !ok || idx != 1 {
			t.Errorf("expected (1, true), got (%d, %v)", idx, ok)
		}
		val := getFieldValue(row, colIndex, "missing", "secondary")
		if val != "val_two" {
			t.Errorf("expected 'val_two', got %q", val)
		}
	})

	t.Run("bounds overflow returns empty", func(t *testing.T) {
		t.Parallel()
		val := getFieldValue(row, colIndex, "overflow")
		if val != "" {
			t.Errorf("expected empty string for overflow column, got %q", val)
		}
	})

	t.Run("column not found returns empty", func(t *testing.T) {
		t.Parallel()
		val := getFieldValue(row, colIndex, "nonexistent")
		if val != "" {
			t.Errorf("expected empty string for nonexistent column, got %q", val)
		}
	})
}

func TestIsRowEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		row  []string
		want bool
	}{
		{name: "nil row", row: nil, want: true},
		{name: "empty row", row: []string{}, want: true},
		{name: "all blank fields", row: []string{"", "   ", "\t"}, want: true},
		{name: "single non-empty field", row: []string{"", "data", ""}, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isRowEmpty(tc.row)
			if got != tc.want {
				t.Errorf("isRowEmpty(%v): expected %v, got %v", tc.row, tc.want, got)
			}
		})
	}
}

func TestBuildLineage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	meta := model.FileMetadata{
		FileName:   "chase_checking.csv",
		FileHash:   "hash_abc_123",
		AccountID:  "chk-999",
		IngestedAt: now,
	}

	lineage := buildLineage(meta, 42)
	if lineage.FileName != meta.FileName {
		t.Errorf("FileName: expected %q, got %q", meta.FileName, lineage.FileName)
	}
	if lineage.FileHash != meta.FileHash {
		t.Errorf("FileHash: expected %q, got %q", meta.FileHash, lineage.FileHash)
	}
	if lineage.RowNumber != 42 {
		t.Errorf("RowNumber: expected 42, got %d", lineage.RowNumber)
	}
	if !lineage.IngestedAt.Equal(now) {
		t.Errorf("IngestedAt: expected %v, got %v", now, lineage.IngestedAt)
	}
}

func TestIsPlaceholderNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty field", input: "", want: true},
		{name: "whitespace-only field", input: "   ", want: true},
		{name: "single placeholder dash", input: "-", want: true},
		{name: "double placeholder dash with surrounding spaces", input: " -- ", want: true},
		{name: "triple dash", input: "---", want: true},
		{name: "lowercase n/a", input: "n/a", want: true},
		{name: "uppercase N/A", input: "N/A", want: true},
		{name: "lowercase na", input: "na", want: true},
		{name: "uppercase NA", input: "NA", want: true},
		{name: "hash N/A uppercase", input: "#N/A", want: true},
		{name: "hash n/a lowercase", input: "#n/a", want: true},
		{name: "none token", input: "none", want: true},
		{name: "null token", input: "null", want: true},
		{name: "nil token", input: "nil", want: true},
		{name: "negative dollar placeholder", input: "-$", want: true},
		{name: "unicode en-dash", input: "\u2013", want: true},
		{name: "unicode em-dash", input: "\u2014", want: true},
		{name: "unicode minus sign", input: "\u2212", want: true},
		{name: "unicode horizontal bar", input: "\u2015", want: true},
		{name: "zero string", input: "0", want: false},
		{name: "decimal zero", input: "0.00", want: false},
		{name: "currency zero", input: "$0.00", want: false},
		{name: "negative zero", input: "-0.00", want: false},
		{name: "positive float", input: "123.45", want: false},
		{name: "negative float", input: "-50.00", want: false},
		{name: "pending text", input: "pending", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isPlaceholderNumeric(tc.input)
			if got != tc.want {
				t.Errorf("isPlaceholderNumeric(%q): expected %v, got %v", tc.input, tc.want, got)
			}
		})
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		inputs []string
		want   string
	}{
		{name: "zero arguments", inputs: nil, want: ""},
		{name: "single non-empty argument", inputs: []string{"test"}, want: "test"},
		{name: "multiple empty strings", inputs: []string{"", "   ", "\t"}, want: ""},
		{name: "fallback chain", inputs: []string{"", "   ", "Second", "Third"}, want: "Second"},
		{name: "trimming behavior", inputs: []string{"  Valid  "}, want: "Valid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := firstNonEmpty(tc.inputs...)
			if got != tc.want {
				t.Errorf("firstNonEmpty(%v): expected %q, got %q", tc.inputs, tc.want, got)
			}
		})
	}
}

func testRowReaderGet(t *testing.T, rr rowReader) {
	t.Helper()
	if got := rr.get("description"); got != "GROCERY STORE" {
		t.Errorf("expected 'GROCERY STORE', got %q", got)
	}
	if got := rr.get("post date", "posting date"); got != "10/18/2026" {
		t.Errorf("expected '10/18/2026', got %q", got)
	}
	if got := rr.get("nonexistent"); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
	if got := rr.get("overflow"); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func testRowReaderParseDescription(t *testing.T, rr rowReader, lineNum int) {
	t.Helper()
	desc, err := rr.parseDescription("description")
	if err != nil || desc != "GROCERY STORE" {
		t.Errorf("expected 'GROCERY STORE', got %q, err: %v", desc, err)
	}

	emptyRR := newRowReader([]string{""}, map[string]int{"description": 0}, lineNum)
	_, err = emptyRR.parseDescription("description")
	if !errors.Is(err, model.ErrMissingDescription) || !strings.Contains(err.Error(), "row 12:") {
		t.Errorf("expected ErrMissingDescription for row 12, got: %v", err)
	}
}

func testRowReaderParseAmount(t *testing.T, rr rowReader, lineNum int) {
	t.Helper()
	amt, err := rr.parseAmount("amount")
	if err != nil || amt != -50.00 {
		t.Errorf("expected -50.00, got %f, err: %v", amt, err)
	}

	nanRR := newRowReader([]string{"NaN"}, map[string]int{"amount": 0}, lineNum)
	_, err = nanRR.parseAmount("amount")
	if !errors.Is(err, model.ErrInvalidAmount) || !strings.Contains(err.Error(), "row 12:") {
		t.Errorf("expected ErrInvalidAmount for row 12, got: %v", err)
	}
}

func testRowReaderParseBalance(t *testing.T, rr rowReader, lineNum int) {
	t.Helper()
	bal, err := rr.parseBalance("balance")
	if err != nil || bal == nil || *bal != 1234.56 {
		t.Errorf("expected 1234.56, got %v, err: %v", bal, err)
	}

	placeholderRR := newRowReader([]string{"-"}, map[string]int{"balance": 0}, lineNum)
	bal, err = placeholderRR.parseBalance("balance")
	if err != nil || bal != nil {
		t.Errorf("expected nil balance without error, got %v, err: %v", bal, err)
	}

	emptyBalRR := newRowReader([]string{""}, map[string]int{"balance": 0}, lineNum)
	bal, err = emptyBalRR.parseBalance("balance")
	if err != nil || bal != nil {
		t.Errorf("expected nil balance without error, got %v, err: %v", bal, err)
	}

	invalidRR := newRowReader([]string{"corrupt"}, map[string]int{"balance": 0}, lineNum)
	_, err = invalidRR.parseBalance("balance")
	if !errors.Is(err, model.ErrInvalidBalance) || !strings.Contains(err.Error(), "invalid balance") {
		t.Errorf("expected ErrInvalidBalance, got: %v", err)
	}
}

func testRowReaderParseDate(t *testing.T, rr rowReader, lineNum int) {
	t.Helper()
	parsed, err := rr.parseDate("posting date", "post date")
	if err != nil {
		t.Fatalf("unexpected parseDate error: %v", err)
	}
	if parsed.Year() != 2026 || parsed.Month() != time.October || parsed.Day() != 18 {
		t.Errorf("expected 2026-10-18, got %v", parsed)
	}

	emptyRR := newRowReader([]string{""}, map[string]int{"posting date": 0}, lineNum)
	_, err = emptyRR.parseDate("posting date")
	if !errors.Is(err, model.ErrMissingPostingDate) || !strings.Contains(err.Error(), "row 12:") {
		t.Errorf("expected ErrMissingPostingDate for row 12, got: %v", err)
	}

	invalidRR := newRowReader([]string{"invalid-date"}, map[string]int{"posting date": 0}, lineNum)
	_, err = invalidRR.parseDate("posting date")
	if err == nil || !strings.Contains(err.Error(), "row 12: invalid date format") {
		t.Errorf("expected invalid date format error for row 12, got: %v", err)
	}
}

func TestRowReader(t *testing.T) {
	t.Parallel()

	colIndex := map[string]int{
		"posting date": 0,
		"description":  1,
		"amount":       2,
		"balance":      3,
		"overflow":     10,
	}
	row := []string{
		"10/18/2026",
		"GROCERY STORE",
		"($50.00)",
		"1,234.56",
	}
	const lineNum = 12
	rr := newRowReader(row, colIndex, lineNum)

	t.Run("get", func(t *testing.T) {
		t.Parallel()
		testRowReaderGet(t, rr)
	})

	t.Run("parseDate", func(t *testing.T) {
		t.Parallel()
		testRowReaderParseDate(t, rr, lineNum)
	})

	t.Run("parseDescription", func(t *testing.T) {
		t.Parallel()
		testRowReaderParseDescription(t, rr, lineNum)
	})

	t.Run("parseAmount", func(t *testing.T) {
		t.Parallel()
		testRowReaderParseAmount(t, rr, lineNum)
	})

	t.Run("parseBalance", func(t *testing.T) {
		t.Parallel()
		testRowReaderParseBalance(t, rr, lineNum)
	})
}

func TestRowReader_ZeroValueAndNegativeBounds(t *testing.T) {
	t.Parallel()

	t.Run("zero-value struct safety", func(t *testing.T) {
		t.Parallel()
		var rr rowReader
		if val := rr.get("any"); val != "" {
			t.Errorf("expected empty string from zero-value rowReader, got %q", val)
		}
		if _, err := rr.parseDescription("any"); err == nil {
			t.Errorf("expected error from parseDescription on zero-value rowReader")
		}
		if _, err := rr.parseAmount("any"); err == nil {
			t.Errorf("expected error from parseAmount on zero-value rowReader")
		}
		bal, balErr := rr.parseBalance("any")
		if balErr != nil || bal != nil {
			t.Errorf("expected (nil, nil) from parseBalance on zero-value rowReader, got (%v, %v)", bal, balErr)
		}
		if _, dateErr := rr.parseDate("any"); dateErr == nil {
			t.Errorf("expected error from parseDate on zero-value rowReader")
		}
	})

	t.Run("negative index bounds check", func(t *testing.T) {
		t.Parallel()
		row := []string{"value1", "value2"}
		colIndex := map[string]int{
			"neg":  -1,
			"bad":  -99,
			"good": 0,
		}
		rr := newRowReader(row, colIndex, 1)
		if val := rr.get("neg"); val != "" {
			t.Errorf("expected empty string for negative index, got %q", val)
		}
		if val := rr.get("bad"); val != "" {
			t.Errorf("expected empty string for negative index, got %q", val)
		}
		if val := rr.get("good"); val != "value1" {
			t.Errorf("expected 'value1', got %q", val)
		}
	})
}

func TestCleanNumeric_CRLFAndFigureSpace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    string
		wantNeg bool
	}{
		{name: "CRLF carriage returns", input: "\r-100.50\r", want: "100.50", wantNeg: true},
		{name: "figure space tabular alignment", input: "1\u2007234.56", want: "1234.56"},
		{name: "mixed tabs, spaces, and CR", input: " \t\r 45.20 \r ", want: "45.20"},
		{name: "accounting negative with CRLF", input: " ($50.00) \r", want: "50.00", wantNeg: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, neg, ok := cleanNumeric(tc.input)
			if !ok || got != tc.want || neg != tc.wantNeg {
				t.Errorf("cleanNumeric(%q): expected (%q, %v, true), got (%q, %v, %v)", tc.input, tc.want, tc.wantNeg, got, neg, ok)
			}
		})
	}
}

func TestFindColumn_TrimmingAndEmptyAliases(t *testing.T) {
	t.Parallel()
	colIndex := map[string]int{
		"posting date": 0,
		"amount":       1,
	}

	t.Run("untrimmed alias matches", func(t *testing.T) {
		t.Parallel()
		idx, ok := findColumn(colIndex, "  posting date  ")
		if !ok || idx != 0 {
			t.Errorf("expected (0, true), got (%d, %v)", idx, ok)
		}
	})

	t.Run("empty and blank candidate skipped", func(t *testing.T) {
		t.Parallel()
		idx, ok := findColumn(colIndex, "", "   ", "amount")
		if !ok || idx != 1 {
			t.Errorf("expected (1, true), got (%d, %v)", idx, ok)
		}
	})
}

func BenchmarkDirect_ParseRow(b *testing.B) {
	colIndex := map[string]int{
		"details":         0,
		"posting date":    1,
		"description":     2,
		"amount":          3,
		"type":            4,
		"balance":         5,
		"check or slip #": 6,
	}
	row := []string{
		"DEBIT",
		"10/18/2026",
		"WHOLE FOODS MARKET #1024",
		"-42.75",
		"POS",
		"1,582.40",
		"",
	}
	const lineNum = 42

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		dateStr := getFieldValue(row, colIndex, "posting date", "post date")
		if dateStr == "" {
			b.Fatal("unexpected empty date")
		}
		postingDate, err := parseDate(dateStr)
		if err != nil || postingDate.IsZero() {
			b.Fatalf("unexpected date error: %v", err)
		}

		desc, err := parseDescription(getFieldValue(row, colIndex, "description"), lineNum)
		if err != nil || desc == "" {
			b.Fatalf("unexpected desc error: %v", err)
		}

		amt, err := parseAmount(getFieldValue(row, colIndex, "amount"), lineNum)
		if err != nil || amt != -42.75 {
			b.Fatalf("unexpected amount error: %v", err)
		}

		bal, err := parseBalance(getFieldValue(row, colIndex, "balance"), lineNum)
		if err != nil || bal == nil || *bal != 1582.40 {
			b.Fatalf("unexpected balance error: %v", err)
		}

		txType := firstNonEmpty(
			getFieldValue(row, colIndex, "type"),
			getFieldValue(row, colIndex, "details"),
			"TRANSACTION",
		)
		if txType != "POS" {
			b.Fatalf("unexpected txType: %s", txType)
		}
	}
}

func BenchmarkRowReader_ParseRow(b *testing.B) {
	colIndex := map[string]int{
		"details":         0,
		"posting date":    1,
		"description":     2,
		"amount":          3,
		"type":            4,
		"balance":         5,
		"check or slip #": 6,
	}
	row := []string{
		"DEBIT",
		"10/18/2026",
		"WHOLE FOODS MARKET #1024",
		"-42.75",
		"POS",
		"1,582.40",
		"",
	}
	const lineNum = 42

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		rr := newRowReader(row, colIndex, lineNum)

		postingDate, err := rr.parseDate("posting date", "post date")
		if err != nil || postingDate.IsZero() {
			b.Fatalf("unexpected date error: %v", err)
		}

		desc, err := rr.parseDescription("description")
		if err != nil || desc == "" {
			b.Fatalf("unexpected desc error: %v", err)
		}

		amt, err := rr.parseAmount("amount")
		if err != nil || amt != -42.75 {
			b.Fatalf("unexpected amount error: %v", err)
		}

		bal, err := rr.parseBalance("balance")
		if err != nil || bal == nil || *bal != 1582.40 {
			b.Fatalf("unexpected balance error: %v", err)
		}

		txType := firstNonEmpty(rr.get("type"), rr.get("details"), "TRANSACTION")
		if txType != "POS" {
			b.Fatalf("unexpected txType: %s", txType)
		}
	}
}

func verifyDirectParseIteration(row []string, colIndex map[string]int, lineNum int) bool {
	dateStr := getFieldValue(row, colIndex, "posting date", "post date")
	if dateStr == "" {
		return false
	}
	postingDate, err := parseDate(dateStr)
	if err != nil || postingDate.IsZero() {
		return false
	}

	desc, err := parseDescription(getFieldValue(row, colIndex, "description"), lineNum)
	if err != nil || desc == "" {
		return false
	}

	amt, err := parseAmount(getFieldValue(row, colIndex, "amount"), lineNum)
	if err != nil || amt != -42.75 {
		return false
	}

	bal, err := parseBalance(getFieldValue(row, colIndex, "balance"), lineNum)
	if err != nil || bal == nil || *bal != 1582.40 {
		return false
	}

	txType := firstNonEmpty(
		getFieldValue(row, colIndex, "type"),
		getFieldValue(row, colIndex, "details"),
		"TRANSACTION",
	)
	return txType == "POS"
}

func BenchmarkDirect_ParseRow_Parallel(b *testing.B) {
	colIndex := map[string]int{
		"details":         0,
		"posting date":    1,
		"description":     2,
		"amount":          3,
		"type":            4,
		"balance":         5,
		"check or slip #": 6,
	}
	row := []string{
		"DEBIT",
		"10/18/2026",
		"WHOLE FOODS MARKET #1024",
		"-42.75",
		"POS",
		"1,582.40",
		"",
	}
	const lineNum = 42

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !verifyDirectParseIteration(row, colIndex, lineNum) {
				b.Errorf("direct parse row verification failed")
				return
			}
		}
	})
}

func verifyRowReaderParseIteration(row []string, colIndex map[string]int, lineNum int) bool {
	rr := newRowReader(row, colIndex, lineNum)

	postingDate, err := rr.parseDate("posting date", "post date")
	if err != nil || postingDate.IsZero() {
		return false
	}

	desc, err := rr.parseDescription("description")
	if err != nil || desc == "" {
		return false
	}

	amt, err := rr.parseAmount("amount")
	if err != nil || amt != -42.75 {
		return false
	}

	bal, err := rr.parseBalance("balance")
	if err != nil || bal == nil || *bal != 1582.40 {
		return false
	}

	txType := firstNonEmpty(rr.get("type"), rr.get("details"), "TRANSACTION")
	return txType == "POS"
}

func BenchmarkRowReader_ParseRow_Parallel(b *testing.B) {
	colIndex := map[string]int{
		"details":         0,
		"posting date":    1,
		"description":     2,
		"amount":          3,
		"type":            4,
		"balance":         5,
		"check or slip #": 6,
	}
	row := []string{
		"DEBIT",
		"10/18/2026",
		"WHOLE FOODS MARKET #1024",
		"-42.75",
		"POS",
		"1,582.40",
		"",
	}
	const lineNum = 42

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !verifyRowReaderParseIteration(row, colIndex, lineNum) {
				b.Errorf("row reader parse verification failed")
				return
			}
		}
	})
}

func TestIsPlaceholderDate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty string", input: "", want: true},
		{name: "whitespace only", input: "   ", want: true},
		{name: "single dash", input: "-", want: true},
		{name: "double dash", input: "--", want: true},
		{name: "lowercase n/a", input: "n/a", want: true},
		{name: "uppercase N/A", input: "N/A", want: true},
		{name: "lowercase pending", input: "pending", want: true},
		{name: "uppercase PENDING", input: "PENDING", want: true},
		{name: "titlecase Pending", input: "Pending", want: true},
		{name: "pending with whitespace", input: "  pending  ", want: true},
		{name: "valid slash date", input: "10/18/2026", want: false},
		{name: "valid iso date", input: "2026-10-18", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isPlaceholderDate(tc.input)
			if got != tc.want {
				t.Errorf("isPlaceholderDate(%q): expected %v, got %v", tc.input, tc.want, got)
			}
		})
	}
}

func TestResolveCreditCardDate(t *testing.T) {
	t.Parallel()
	const lineNum = 7

	tests := []struct {
		name      string
		row       []string
		colIndex  map[string]int
		wantDate  string
		wantErr   bool
		errSubstr string
		wantIs    error
	}{
		{
			name:     "post date present and valid",
			row:      []string{"10/18/2026", "10/17/2026"},
			colIndex: map[string]int{"post date": 0, "transaction date": 1},
			wantDate: "2026-10-18",
		},
		{
			name:     "post date pending falls back to transaction date",
			row:      []string{"pending", "10/17/2026"},
			colIndex: map[string]int{"post date": 0, "transaction date": 1},
			wantDate: "2026-10-17",
		},
		{
			name:     "post date placeholder dash falls back to transaction date",
			row:      []string{"-", "10/17/2026"},
			colIndex: map[string]int{"post date": 0, "transaction date": 1},
			wantDate: "2026-10-17",
		},
		{
			name:      "missing both post date and transaction date returns error",
			row:       []string{"pending", "-"},
			colIndex:  map[string]int{"post date": 0, "transaction date": 1},
			wantErr:   true,
			errSubstr: "row 7: missing both post date and transaction date",
			wantIs:    model.ErrMissingPostingDate,
		},
		{
			name:      "corrupted date returns parse error with line number",
			row:       []string{"invalid-date"},
			colIndex:  map[string]int{"post date": 0},
			wantErr:   true,
			errSubstr: "invalid date format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rr := newRowReader(tc.row, tc.colIndex, lineNum)
			got, err := resolveCreditCardDate(rr)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("expected error containing %q, got: %v", tc.errSubstr, err)
				}
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Errorf("expected errors.Is(err, %v), got: %v", tc.wantIs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Format("2006-01-02") != tc.wantDate {
				t.Errorf("expected %s, got %v", tc.wantDate, got)
			}
		})
	}
}

func testParseCheckingRowStandard(t *testing.T, meta model.FileMetadata, colIndex map[string]int) {
	t.Helper()
	row := []string{"10/18/2026", "CHECK PAYMENT", "-150.00", "CHECK", "2,450.00", "1001", "DEBIT"}
	tx, err := parseCheckingRow(row, colIndex, 10, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.PostingDate.Format("2006-01-02") != "2026-10-18" {
		t.Errorf("expected 2026-10-18, got %v", tx.PostingDate)
	}
	if tx.Description != "CHECK PAYMENT" {
		t.Errorf("expected 'CHECK PAYMENT', got %q", tx.Description)
	}
	if tx.Amount != -150.00 {
		t.Errorf("expected -150.00, got %f", tx.Amount)
	}
	if tx.Type != "CHECK" {
		t.Errorf("expected 'CHECK', got %q", tx.Type)
	}
	if tx.Balance == nil || *tx.Balance != 2450.00 {
		t.Errorf("expected 2450.00, got %v", tx.Balance)
	}
	if tx.CheckOrSlipNum != "1001" {
		t.Errorf("expected '1001', got %q", tx.CheckOrSlipNum)
	}
	if tx.Details != "DEBIT" {
		t.Errorf("expected 'DEBIT', got %q", tx.Details)
	}
	if tx.Lineage.RowNumber != 10 {
		t.Errorf("expected row number 10, got %d", tx.Lineage.RowNumber)
	}
}

func testParseCheckingRowTypeFallbacks(t *testing.T, meta model.FileMetadata, colIndex map[string]int) {
	t.Helper()
	rowATM := []string{"10/18/2026", "ATM WITHDRAWAL", "-100.00", "", "2,350.00", "", "ATM"}
	txATM, err := parseCheckingRow(rowATM, colIndex, 11, meta)
	if err != nil || txATM.Type != "ATM" {
		t.Errorf("expected 'ATM', got tx.Type=%q, err=%v", txATM.Type, err)
	}

	rowDefault := []string{"10/18/2026", "MISC TRANSACTION", "-50.00", "", "2,300.00", "", ""}
	txDef, err := parseCheckingRow(rowDefault, colIndex, 12, meta)
	if err != nil || txDef.Type != "TRANSACTION" {
		t.Errorf("expected 'TRANSACTION', got tx.Type=%q, err=%v", txDef.Type, err)
	}
}

func testParseCheckingRowBalanceAndAliases(t *testing.T, meta model.FileMetadata) {
	t.Helper()
	colIndex := map[string]int{
		"posting date": 0, "description": 1, "amount": 2, "type": 3, "balance": 4,
	}
	rowNoBal := []string{"10/18/2026", "MISC", "-50.00", "DEBIT", ""}
	txNoBal, err := parseCheckingRow(rowNoBal, colIndex, 13, meta)
	if err != nil || txNoBal.Balance != nil {
		t.Errorf("expected nil balance, got %v, err=%v", txNoBal.Balance, err)
	}

	aliasColIndex := map[string]int{
		"posting date": 0, "description": 1, "amount": 2, "check or slip num": 3,
	}
	rowAlias := []string{"10/18/2026", "CHECK", "-25.00", "2002"}
	txAlias, err := parseCheckingRow(rowAlias, aliasColIndex, 14, meta)
	if err != nil || txAlias.CheckOrSlipNum != "2002" {
		t.Errorf("expected '2002', got %q, err=%v", txAlias.CheckOrSlipNum, err)
	}
}

func testParseCheckingRowValidationError(t *testing.T, colIndex map[string]int) {
	t.Helper()
	invalidMeta := model.FileMetadata{AccountID: ""}
	row := []string{"10/18/2026", "VALID DESC", "-50.00", "DEBIT", "", "", ""}
	_, err := parseCheckingRow(row, colIndex, 15, invalidMeta)
	if err == nil || !strings.Contains(err.Error(), "row 15 validation error:") {
		t.Errorf("expected 'row 15 validation error:', got: %v", err)
	}
}

func TestParseCheckingRow(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "test_checking.csv",
		FileHash:   "hash123",
		AccountID:  "acc-chk-001",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"posting date":    0,
		"description":     1,
		"amount":          2,
		"type":            3,
		"balance":         4,
		"check or slip #": 5,
		"details":         6,
	}

	t.Run("standard", func(t *testing.T) {
		t.Parallel()
		testParseCheckingRowStandard(t, meta, colIndex)
	})
	t.Run("type fallbacks", func(t *testing.T) {
		t.Parallel()
		testParseCheckingRowTypeFallbacks(t, meta, colIndex)
	})
	t.Run("balance and aliases", func(t *testing.T) {
		t.Parallel()
		testParseCheckingRowBalanceAndAliases(t, meta)
	})
	t.Run("validation error", func(t *testing.T) {
		t.Parallel()
		testParseCheckingRowValidationError(t, colIndex)
	})
}

func testParseCreditCardRowStandard(t *testing.T, meta model.FileMetadata, colIndex map[string]int) {
	t.Helper()
	row := []string{"10/18/2026", "AIRLINE TICKET", "-350.00", "Travel", "1,200.00", "Travel", "Flight to SFO", "10/17/2026"}
	tx, err := parseCreditCardRow(row, colIndex, 20, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.PostingDate.Format("2006-01-02") != "2026-10-18" {
		t.Errorf("expected 2026-10-18, got %v", tx.PostingDate)
	}
	if tx.Description != "AIRLINE TICKET" {
		t.Errorf("expected 'AIRLINE TICKET', got %q", tx.Description)
	}
	if tx.Amount != -350.00 {
		t.Errorf("expected -350.00, got %f", tx.Amount)
	}
	if tx.Type != "Travel" {
		t.Errorf("expected 'Travel', got %q", tx.Type)
	}
	if tx.Category != "Travel" {
		t.Errorf("expected 'Travel', got %q", tx.Category)
	}
	if tx.Memo != "Flight to SFO" {
		t.Errorf("expected 'Flight to SFO', got %q", tx.Memo)
	}
	if tx.Balance == nil || *tx.Balance != 1200.00 {
		t.Errorf("expected 1200.00, got %v", tx.Balance)
	}
	if tx.TransactionDate == nil || tx.TransactionDate.Format("2006-01-02") != "2026-10-17" {
		t.Errorf("expected TransactionDate 2026-10-17, got %v", tx.TransactionDate)
	}
}

func testParseCreditCardRowFallbacks(t *testing.T, meta model.FileMetadata, colIndex map[string]int) {
	t.Helper()
	rowDefaultType := []string{"10/18/2026", "GROCERY", "-50.00", "", "", "", "", "10/17/2026"}
	txDef, err := parseCreditCardRow(rowDefaultType, colIndex, 21, meta)
	if err != nil || txDef.Type != "Sale" {
		t.Errorf("expected 'Sale', got %q, err=%v", txDef.Type, err)
	}
	if txDef.TransactionDate == nil || txDef.TransactionDate.Format("2006-01-02") != "2026-10-17" {
		t.Errorf("expected TransactionDate 2026-10-17, got %v", txDef.TransactionDate)
	}

	rowPending := []string{"pending", "COFFEE SHOP", "-4.50", "Sale", "", "", "", "10/17/2026"}
	txPending, err := parseCreditCardRow(rowPending, colIndex, 22, meta)
	if err != nil || txPending.PostingDate.Format("2006-01-02") != "2026-10-17" {
		t.Errorf("expected 2026-10-17, got %v, err=%v", txPending.PostingDate, err)
	}
	if txPending.TransactionDate == nil || !txPending.TransactionDate.Equal(txPending.PostingDate) {
		t.Errorf("expected TransactionDate equal to PostingDate 2026-10-17, got %v", txPending.TransactionDate)
	}
}

func testParseCreditCardRowValidationError(t *testing.T, colIndex map[string]int) {
	t.Helper()
	invalidMeta := model.FileMetadata{AccountID: ""}
	row := []string{"10/18/2026", "COFFEE", "-4.50", "Sale", "", "", "", "10/17/2026"}
	_, err := parseCreditCardRow(row, colIndex, 23, invalidMeta)
	if err == nil || !strings.Contains(err.Error(), "row 23 validation error:") {
		t.Errorf("expected 'row 23 validation error:', got: %v", err)
	}
}

func TestParseCreditCardRow(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "test_credit_card.csv",
		FileHash:   "hash456",
		AccountID:  "acc-cc-001",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"post date":        0,
		"description":      1,
		"amount":           2,
		"type":             3,
		"balance":          4,
		"category":         5,
		"memo":             6,
		"transaction date": 7,
	}

	t.Run("standard", func(t *testing.T) {
		t.Parallel()
		testParseCreditCardRowStandard(t, meta, colIndex)
	})
	t.Run("defaults and fallbacks", func(t *testing.T) {
		t.Parallel()
		testParseCreditCardRowFallbacks(t, meta, colIndex)
	})
	t.Run("validation error", func(t *testing.T) {
		t.Parallel()
		testParseCreditCardRowValidationError(t, colIndex)
	})
}

func TestParseChaseRecords_CancellationBetweenRows(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	csvData := "Details,Posting Date,Description,Amount\n" +
		"DEBIT,10/15/2026,ROW 1,-10.00\n" +
		"DEBIT,10/16/2026,ROW 2,-20.00\n" +
		"DEBIT,10/17/2026,ROW 3,-30.00\n"
	meta := model.FileMetadata{
		FileName:   "cancel.csv",
		AccountID:  "acc-cancel",
		IngestedAt: time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
	}
	required := [][]string{{"posting date"}, {"description"}, {"amount"}}

	calls := 0
	cancelAfterFirstRow := func(
		row []string,
		colIndex map[string]int,
		lineNum int,
		fm model.FileMetadata,
	) (model.ChaseTransaction, error) {
		calls++
		if calls == 1 {
			cancel()
		}
		return parseCheckingRow(row, colIndex, lineNum, fm)
	}

	_, err := parseChaseRecords(ctx, strings.NewReader(csvData), meta, "checking", required, cancelAfterFirstRow)
	//nolint:errorlint // intentional direct equality check to ensure error is strictly unwrapped root sentinel
	if err != context.Canceled {
		t.Fatalf("expected strictly unwrapped context.Canceled, got: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected the row loop to stop after 1 row, parsed %d", calls)
	}
}

func assertResolvedTransactionDate(
	t *testing.T,
	got *time.Time,
	err error,
	wantDate string,
	wantNil bool,
	wantErr bool,
	errSubstr string,
) {
	t.Helper()
	if wantErr {
		if err == nil || !strings.Contains(err.Error(), errSubstr) {
			t.Fatalf("expected error containing %q, got: %v", errSubstr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wantNil {
		if got != nil {
			t.Errorf("expected nil TransactionDate, got %v", *got)
		}
		return
	}
	if got == nil {
		t.Fatalf("expected non-nil TransactionDate %q, got nil", wantDate)
	}
	if got.Format("2006-01-02") != wantDate {
		t.Errorf("expected %s, got %s", wantDate, got.Format("2006-01-02"))
	}
	if got.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", got.Location())
	}
}

func TestResolveTransactionDate(t *testing.T) {
	t.Parallel()
	const lineNum = 7
	stdIndex := map[string]int{"transaction date": 0}

	tests := []struct {
		name      string
		row       []string
		colIndex  map[string]int
		wantDate  string
		wantNil   bool
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "MM/DD/YYYY 10/14/2026 resolves to UTC midnight",
			row:      []string{"10/14/2026"},
			colIndex: stdIndex,
			wantDate: "2026-10-14",
		},
		{
			name:     "M/D/YYYY 1/2/2026 resolves to UTC midnight",
			row:      []string{"1/2/2026"},
			colIndex: stdIndex,
			wantDate: "2026-01-02",
		},
		{
			name:     "ISO 2026-10-14 resolves to UTC midnight",
			row:      []string{"2026-10-14"},
			colIndex: stdIndex,
			wantDate: "2026-10-14",
		},
		{
			name:     "trans date alias resolves to UTC midnight",
			row:      []string{"10/14/2026"},
			colIndex: map[string]int{"trans date": 0},
			wantDate: "2026-10-14",
		},
		{name: "empty string returns nil", row: []string{""}, colIndex: stdIndex, wantNil: true},
		{name: "double dash -- returns nil", row: []string{"--"}, colIndex: stdIndex, wantNil: true},
		{name: "unicode em dash — returns nil", row: []string{"\u2014"}, colIndex: stdIndex, wantNil: true},
		{name: "pending placeholder returns nil", row: []string{"pending"}, colIndex: stdIndex, wantNil: true},
		{name: "N/A placeholder returns nil", row: []string{"N/A"}, colIndex: stdIndex, wantNil: true},
		{
			name:     "column absent returns nil",
			row:      []string{"10/18/2026"},
			colIndex: map[string]int{"post date": 0},
			wantNil:  true,
		},
		{
			name:      "invalid date 13/45/2026 returns row error",
			row:       []string{"13/45/2026"},
			colIndex:  stdIndex,
			wantErr:   true,
			errSubstr: "row 7: transaction date",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rr := newRowReader(tc.row, tc.colIndex, lineNum)
			got, err := resolveTransactionDate(rr)
			assertResolvedTransactionDate(t, got, err, tc.wantDate, tc.wantNil, tc.wantErr, tc.errSubstr)
		})
	}
}

func TestParseCreditCardRow_TransactionDatePopulated(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "cc_settled.csv",
		AccountID:  "acc-cc-settled",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"transaction date": 0,
		"post date":        1,
		"description":      2,
		"amount":           3,
	}
	row := []string{"10/14/2026", "10/16/2026", "COFFEE ROASTERS", "-6.75"}

	tx, err := parseCreditCardRow(row, colIndex, 2, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.TransactionDate == nil {
		t.Fatal("expected non-nil TransactionDate")
	}
	wantTxDate := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	wantPostDate := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	if !tx.TransactionDate.Equal(wantTxDate) {
		t.Errorf("expected TransactionDate %v, got %v", wantTxDate, *tx.TransactionDate)
	}
	if !tx.PostingDate.Equal(wantPostDate) {
		t.Errorf("expected PostingDate %v, got %v", wantPostDate, tx.PostingDate)
	}
	if tx.TransactionDate.Equal(tx.PostingDate) {
		t.Errorf("expected purchase TransactionDate (%v) to differ from PostingDate (%v)", *tx.TransactionDate, tx.PostingDate)
	}
}

func TestParseCreditCardRow_PendingUsesTransactionDateForBoth(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "cc_pending.csv",
		AccountID:  "acc-cc-pending",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"transaction date": 0,
		"post date":        1,
		"description":      2,
		"amount":           3,
	}
	row := []string{"10/14/2026", "", "PENDING MERCHANT", "-24.00"}

	tx, err := parseCreditCardRow(row, colIndex, 3, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.TransactionDate == nil {
		t.Fatal("expected non-nil TransactionDate for pending credit card row")
	}
	wantDay := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	if !tx.PostingDate.Equal(wantDay) || !tx.TransactionDate.Equal(wantDay) {
		t.Errorf("expected both PostingDate and TransactionDate to equal %v, got post=%v tx=%v",
			wantDay, tx.PostingDate, *tx.TransactionDate)
	}
}

func TestParseCreditCardRow_InvalidTransactionDateFailsRow(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "cc_invalid_txdate.csv",
		AccountID:  "acc-cc-err",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"transaction date": 0,
		"post date":        1,
		"description":      2,
		"amount":           3,
	}
	row := []string{"13/45/2026", "10/16/2026", "BAD TX DATE", "-15.00"}

	_, rowErr := parseCreditCardRow(row, colIndex, 4, meta)
	if rowErr == nil || !strings.Contains(rowErr.Error(), "row 4: transaction date") {
		t.Fatalf("expected row 4 transaction date error, got: %v", rowErr)
	}

	csvData := "Transaction Date,Post Date,Description,Amount\n13/45/2026,10/16/2026,BAD TX DATE,-15.00\n"
	_, streamErr := parseCreditCard(context.Background(), strings.NewReader(csvData), meta)
	if !errors.Is(streamErr, ErrInvalidRow) {
		t.Fatalf("expected errors.Is(streamErr, ErrInvalidRow), got: %v", streamErr)
	}
}

func TestParseCheckingRow_TransactionDateNil(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "checking_single.csv",
		AccountID:  "acc-chk-single",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	colIndex := map[string]int{
		"posting date": 0,
		"description":  1,
		"amount":       2,
	}
	row := []string{"10/18/2026", "DIRECT DEPOSIT", "2500.00"}

	tx, err := parseCheckingRow(row, colIndex, 2, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.TransactionDate != nil {
		t.Errorf("expected nil TransactionDate for checking row, got %v", *tx.TransactionDate)
	}
}

func TestParseCreditCard_StreamTransactionDateParity(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "cc_stream_parity.csv",
		AccountID:  "acc-cc-parity",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name         string
		csvData      string
		wantPostDate string
		wantTxDate   string
		wantNilTx    bool
	}{
		{
			name: "settled charge with distinct Transaction Date and Post Date",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"10/14/2026,10/16/2026,BOOKSTORE,Shopping,Sale,-28.50,\n",
			wantPostDate: "2026-10-16",
			wantTxDate:   "2026-10-14",
		},
		{
			name: "pending row with blank Post Date uses Transaction Date for both",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"10/15/2026,,DINER,Food & Drink,Sale,-18.00,\n",
			wantPostDate: "2026-10-15",
			wantTxDate:   "2026-10-15",
		},
		{
			name: "placeholder Transaction Date -- leaves TransactionDate nil",
			csvData: "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n" +
				"--,10/16/2026,PAYMENT THANK YOU,,Payment,250.00,\n",
			wantPostDate: "2026-10-16",
			wantNilTx:    true,
		},
		{
			name: "Trans Date header alias populates TransactionDate",
			csvData: "Trans Date,Post Date,Description,Amount\n" +
				"10/12/2026,10/14/2026,HARDWARE STORE,-42.10\n",
			wantPostDate: "2026-10-14",
			wantTxDate:   "2026-10-12",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txs, err := parseCreditCard(context.Background(), strings.NewReader(tc.csvData), meta)
			if err != nil || len(txs) != 1 {
				t.Fatalf("expected 1 transaction without error, got len=%d err=%v", len(txs), err)
			}
			tx := txs[0]
			if tx.PostingDate.Format("2006-01-02") != tc.wantPostDate {
				t.Errorf("expected PostingDate %s, got %s", tc.wantPostDate, tx.PostingDate.Format("2006-01-02"))
			}
			assertResolvedTransactionDate(t, tx.TransactionDate, nil, tc.wantTxDate, tc.wantNilTx, false, "")
		})
	}
}

func TestParseChecking_StreamTransactionDateAlwaysNil(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "chk_stream.csv",
		AccountID:  "acc-chk-stream",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	csvData := "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n" +
		"DEBIT,10/15/2026,GROCERY,-54.20,POS,1200.00,\n" +
		"CREDIT,10/16/2026,PAYROLL,2000.00,ACH,3200.00,\n" +
		"CHECK,10/17/2026,CHECK 101,-100.00,CHECK,3100.00,101\n"

	txs, err := parseChecking(context.Background(), strings.NewReader(csvData), meta)
	if err != nil || len(txs) != 3 {
		t.Fatalf("expected 3 checking transactions without error, got len=%d err=%v", len(txs), err)
	}
	for i, tx := range txs {
		if tx.TransactionDate != nil {
			t.Errorf("row %d: expected nil TransactionDate for checking transaction, got %v", i, *tx.TransactionDate)
		}
	}
}

func TestChaseTransactionDate_PointerIsolationAndUTC(t *testing.T) {
	t.Parallel()
	meta := model.FileMetadata{
		FileName:   "cc_isolation.csv",
		AccountID:  "acc-cc-iso",
		IngestedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	csvData := "Transaction Date,Post Date,Description,Amount\n" +
		"10/14/2026,10/16/2026,FIRST CHARGE,-10.00\n" +
		"10/14/2026,10/16/2026,SECOND CHARGE,-20.00\n"

	txs, err := parseCreditCard(context.Background(), strings.NewReader(csvData), meta)
	if err != nil || len(txs) != 2 {
		t.Fatalf("expected 2 transactions without error, got len=%d err=%v", len(txs), err)
	}
	if txs[0].TransactionDate == nil || txs[1].TransactionDate == nil {
		t.Fatal("expected non-nil TransactionDate on both rows")
	}
	if txs[0].TransactionDate == txs[1].TransactionDate {
		t.Error("expected distinct *time.Time pointer instances across rows")
	}
	if txs[0].TransactionDate.Location() != time.UTC || txs[1].TransactionDate.Location() != time.UTC {
		t.Errorf("expected time.UTC locations, got %v and %v",
			txs[0].TransactionDate.Location(), txs[1].TransactionDate.Location())
	}

	mutated := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	*txs[0].TransactionDate = mutated
	if txs[1].TransactionDate.Equal(mutated) {
		t.Error("mutating *txs[0].TransactionDate should not affect *txs[1].TransactionDate")
	}
}
