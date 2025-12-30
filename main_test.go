package main

import (
	"testing"
	"time"
)

// TestSlugify verifies the slugify function behavior.
func TestSlugify(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Hello World", "hello-world"},
		{"Install pg_stat_statements", "install-pg-stat-statements"},
		{"  Leading and Trailing  ", "leading-and-trailing"},
		{"Multiple---Hyphens", "multiple-hyphens"},
		{"CamelCase", "camelcase"},
		{"with_underscores", "with-underscores"},
		{"MixedCase123Numbers", "mixedcase123numbers"},
		{"", ""},
		{"---", ""},
		{"Single", "single"},
		{"a", "a"},
		{"A-B-C", "a-b-c"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := slugify(tt.input)
			if result != tt.expected {
				t.Errorf("slugify(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParseSuppressedSet verifies suppression list parsing.
func TestParseSuppressedSet(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]struct{}
	}{
		{
			"install-pgss,cache-overall",
			map[string]struct{}{"install-pgss": {}, "cache-overall": {}},
		},
		{
			"  install-pgss , cache-overall  ",
			map[string]struct{}{"install-pgss": {}, "cache-overall": {}},
		},
		{
			"Install PGSS",
			map[string]struct{}{"install-pgss": {}},
		},
		{
			"",
			map[string]struct{}{},
		},
		{
			"single",
			map[string]struct{}{"single": {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSuppressedSet(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseSuppressedSet(%q) returned %d items, expected %d",
					tt.input, len(result), len(tt.expected))
			}
			for k := range tt.expected {
				if _, ok := result[k]; !ok {
					t.Errorf("parseSuppressedSet(%q) missing key %q", tt.input, k)
				}
			}
		})
	}
}

// TestSplitCSV verifies CSV splitting behavior.
func TestSplitCSV(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"a,b,c", []string{"a", "b", "c"}},
		{"  a , b , c  ", []string{"a", "b", "c"}},
		{"single", []string{"single"}},
		{"", nil},
		{"a,,b", []string{"a", "b"}},
		{"  ,  ,  ", nil},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := splitCSV(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("splitCSV(%q) = %v, expected %v", tt.input, result, tt.expected)
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("splitCSV(%q)[%d] = %q, expected %q",
						tt.input, i, v, tt.expected[i])
				}
			}
		})
	}
}

// TestExpandOutPlaceholders verifies timestamp placeholder expansion.
func TestExpandOutPlaceholders(t *testing.T) {
	testTime := time.Date(2024, 8, 30, 14, 25, 0, 0, time.UTC)

	tests := []struct {
		input    string
		expected string
	}{
		{"report_{ts}.html", "report_2024-08-30_1425.html"},
		{"{ts}_report.html", "2024-08-30_1425_report.html"},
		{"report.html", "report.html"},
		{"{ts}/{ts}.html", "2024-08-30_1425/2024-08-30_1425.html"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := expandOutPlaceholders(tt.input, testTime)
			if result != tt.expected {
				t.Errorf("expandOutPlaceholders(%q) = %q, expected %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

// TestExpandOutPlaceholdersZeroTime verifies behavior with zero time.
func TestExpandOutPlaceholdersZeroTime(t *testing.T) {
	result := expandOutPlaceholders("report_{ts}.html", time.Time{})
	// Should use current time, so just verify the placeholder is replaced
	if result == "report_{ts}.html" {
		t.Error("expected {ts} placeholder to be replaced for zero time")
	}
}

// TestFirstNonEmpty verifies the first non-empty string selection.
func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		input    []string
		expected string
	}{
		{[]string{"a", "b", "c"}, "a"},
		{[]string{"", "b", "c"}, "b"},
		{[]string{"", "", "c"}, "c"},
		{[]string{"", "", ""}, ""},
		{[]string{}, ""},
		{[]string{"only"}, "only"},
	}

	for _, tt := range tests {
		result := firstNonEmpty(tt.input...)
		if result != tt.expected {
			t.Errorf("firstNonEmpty(%v) = %q, expected %q",
				tt.input, result, tt.expected)
		}
	}
}

// TestFlagsValidate verifies configuration validation.
func TestFlagsValidate(t *testing.T) {
	tests := []struct {
		name      string
		flags     Flags
		expectErr bool
	}{
		{
			name: "valid configuration",
			flags: Flags{
				URL:     "postgres://localhost/test",
				Timeout: 30 * time.Second,
			},
			expectErr: false,
		},
		{
			name: "missing URL",
			flags: Flags{
				URL:     "",
				Timeout: 30 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "zero timeout",
			flags: Flags{
				URL:     "postgres://localhost/test",
				Timeout: 0,
			},
			expectErr: true,
		},
		{
			name: "negative timeout",
			flags: Flags{
				URL:     "postgres://localhost/test",
				Timeout: -1 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "excessive timeout",
			flags: Flags{
				URL:     "postgres://localhost/test",
				Timeout: 15 * time.Minute,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.flags.Validate()
			if (err != nil) != tt.expectErr {
				t.Errorf("Validate() error = %v, expectErr = %v", err, tt.expectErr)
			}
		})
	}
}

// TestResolveOutputPath verifies output path resolution.
func TestResolveOutputPath(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		input    string
		expected string
	}{
		{"", defaultOutputFile},
		{"-", defaultOutputFile},
		{"custom.html", "custom.html"},
		{"report_{ts}.html", "report_2024-01-15_1030.html"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := resolveOutputPath(tt.input, testTime)
			if result != tt.expected {
				t.Errorf("resolveOutputPath(%q) = %q, expected %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

// BenchmarkSlugify benchmarks the slugify function.
func BenchmarkSlugify(b *testing.B) {
	input := "Install pg_stat_statements Extension for Better Performance"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slugify(input)
	}
}

// BenchmarkParseSuppressedSet benchmarks suppression list parsing.
func BenchmarkParseSuppressedSet(b *testing.B) {
	input := "install-pgss,cache-overall,unused-indexes,long-running,table-bloat"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseSuppressedSet(input)
	}
}
