package chase

import (
	"math"
	"strings"
	"testing"
	"time"

	"babylon/dataloader/internal/domain/model"
)

func TestCleanNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "standard currency", input: "$1,234.56", want: "1234.56"},
		{name: "number with comma", input: "1,234.56", want: "1234.56"},
		{name: "plus sign currency", input: "+$500.00", want: "500.00"},
		{name: "accounting parentheses", input: "($50.00)", want: "-50.00"},
		{name: "outer dollar accounting parentheses", input: "$(45.67)", want: "-45.67"},
		{name: "unicode non-breaking space", input: "$\u00a01,000.50", want: "1000.50"},
		{name: "narrow non-breaking space", input: "$\u202f1,000.50", want: "1000.50"},
		{name: "spaces and tabs", input: "$ 1 000 . 50 \t", want: "1000.50"},
		{name: "empty string returns empty", input: "", want: ""},
		{name: "single dash preserved without zero coercion", input: "-", want: "-"},
		{name: "double dash preserved without zero coercion", input: "--", want: "--"},
		{name: "currency with single dash", input: "$ -", want: "-"},
		{name: "currency with multiple spaces and dash", input: "$   -", want: "-"},
		{name: "n/a lowercase preserved", input: "n/a", want: "n/a"},
		{name: "N/A uppercase preserved", input: "N/A", want: "N/A"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cleanNumeric(tc.input)
			if got != tc.want {
				t.Errorf("cleanNumeric(%q): expected %q, got %q", tc.input, tc.want, got)
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
		{name: "non-finite NaN", raw: "NaN", wantErr: true, errSubstr: "non-finite number"},
		{name: "non-finite lowercase nan", raw: "nan", wantErr: true, errSubstr: "non-finite number"},
		{name: "non-finite positive Inf", raw: "+Inf", wantErr: true, errSubstr: "non-finite number"},
		{name: "non-finite negative Inf", raw: "-Inf", wantErr: true, errSubstr: "non-finite number"},
		{name: "non-finite Infinity", raw: "Infinity", wantErr: true, errSubstr: "non-finite number"},
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
		{name: "empty description", raw: "", wantErr: true, errSubstr: "transaction description is required"},
		{name: "all whitespace description", raw: "   \t   ", wantErr: true, errSubstr: "transaction description is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseDescription(tc.raw, 10)
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
			if got != tc.want {
				t.Errorf("expected %q, got %q", tc.want, got)
			}
		})
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
		{name: "non-finite NaN", raw: "NaN", wantErr: true, errSubstr: "non-finite number"},
		{name: "non-finite Inf", raw: "+Inf", wantErr: true, errSubstr: "non-finite number"},
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
