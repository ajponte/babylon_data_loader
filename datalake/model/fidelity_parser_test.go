package model_test

import (
	"testing"
	"time"

	"babylon/dataloader/datalake/model"
)

func TestParseFidelityCurrency(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
		wantErr  bool
	}{
		{name: "standard currency", input: "$3250.00", expected: 3250.00},
		{name: "positive currency", input: "+$35.26", expected: 35.26},
		{name: "negative currency", input: "-$12.40", expected: -12.40},
		{name: "zero currency", input: "$0.00", expected: 0.0},
		{name: "with thousands comma", input: "$1,234,567.89", expected: 1234567.89},
		{name: "placeholder dash", input: "--", expected: 0.0},
		{name: "empty string", input: "", expected: 0.0},
		{name: "not applicable", input: "N/A", expected: 0.0},
		{name: "invalid string", input: "invalid", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := model.ParseFidelityCurrency(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("expected %f, got %f for input %q", tc.expected, got, tc.input)
			}
		})
	}
}

func TestParseFidelityPercentage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
		wantErr  bool
	}{
		{name: "standard percentage", input: "29.13%", expected: 29.13},
		{name: "positive percentage", input: "+0.60%", expected: 0.60},
		{name: "negative percentage", input: "-3.50%", expected: -3.50},
		{name: "zero percentage", input: "0.00%", expected: 0.0},
		{name: "placeholder dash", input: "--", expected: 0.0},
		{name: "empty string", input: "", expected: 0.0},
		{name: "invalid string", input: "invalid%", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := model.ParseFidelityPercentage(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("expected %f, got %f for input %q", tc.expected, got, tc.input)
			}
		})
	}
}

func TestParseFidelityQuantity(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
		wantErr  bool
	}{
		{name: "fractional shares", input: "167.946", expected: 167.946},
		{name: "thousands quantity", input: "4,968.08", expected: 4968.08},
		{name: "whole number", input: "10", expected: 10.0},
		{name: "placeholder dash", input: "--", expected: 0.0},
		{name: "empty string", input: "", expected: 0.0},
		{name: "invalid string", input: "bad", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := model.ParseFidelityQuantity(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("expected %f, got %f for input %q", tc.expected, got, tc.input)
			}
		})
	}
}

func TestParseFidelityDateDownloaded(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "valid pm format",
			input:    `"Date downloaded Sep-25-2026 9:50 p.m ET"`,
			expected: time.Date(2026, time.September, 25, 21, 50, 0, 0, time.UTC),
		},
		{
			name:     "valid am format",
			input:    `Date downloaded Jan-15-2026 10:15 a.m ET`,
			expected: time.Date(2026, time.January, 15, 10, 15, 0, 0, time.UTC),
		},
		{
			name:    "missing prefix",
			input:   `"Downloaded on Sep-25-2026"`,
			wantErr: true,
		},
		{
			name:    "invalid date structure",
			input:   `Date downloaded Invalid-Date 9:50 p.m ET`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := model.ParseFidelityDateDownloaded(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if !got.Equal(tc.expected) {
				t.Errorf("expected %v, got %v for input %q", tc.expected, got, tc.input)
			}
		})
	}
}
