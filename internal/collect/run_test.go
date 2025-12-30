package collect

import (
	"testing"
	"time"
)

// TestConfigValidate verifies configuration validation.
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		expectErr bool
	}{
		{
			name: "valid configuration",
			config: Config{
				URL:     "postgres://localhost/test",
				Timeout: 30 * time.Second,
			},
			expectErr: false,
		},
		{
			name: "missing URL",
			config: Config{
				URL:     "",
				Timeout: 30 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "timeout too short",
			config: Config{
				URL:     "postgres://localhost/test",
				Timeout: 1 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "timeout too long",
			config: Config{
				URL:     "postgres://localhost/test",
				Timeout: 15 * time.Minute,
			},
			expectErr: true,
		},
		{
			name: "minimum valid timeout",
			config: Config{
				URL:     "postgres://localhost/test",
				Timeout: MinTimeout,
			},
			expectErr: false,
		},
		{
			name: "maximum valid timeout",
			config: Config{
				URL:     "postgres://localhost/test",
				Timeout: MaxTimeout,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.expectErr {
				t.Errorf("Validate() error = %v, expectErr = %v", err, tt.expectErr)
			}
		})
	}
}

// TestSwapDBInURL verifies database URL manipulation.
func TestSwapDBInURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		db       string
		expected string
	}{
		{
			name:     "simple URL",
			url:      "postgres://localhost/olddb",
			db:       "newdb",
			expected: "postgres://localhost/newdb",
		},
		{
			name:     "URL with params",
			url:      "postgres://localhost/olddb?sslmode=require",
			db:       "newdb",
			expected: "postgres://localhost/newdb?sslmode=require",
		},
		{
			name:     "URL with credentials",
			url:      "postgres://user:pass@localhost:5432/olddb",
			db:       "newdb",
			expected: "postgres://user:pass@localhost:5432/newdb",
		},
		{
			name:     "URL without path",
			url:      "postgres://localhost",
			db:       "newdb",
			expected: "postgres://localhost/newdb",
		},
		{
			name:     "invalid URL format",
			url:      "not-a-valid-url",
			db:       "newdb",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := swapDBInURL(tt.url, tt.db)
			if result != tt.expected {
				t.Errorf("swapDBInURL(%q, %q) = %q, expected %q",
					tt.url, tt.db, result, tt.expected)
			}
		})
	}
}

// TestQuoteIdent verifies identifier quoting.
func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{`with"quote`, `"with""quote"`},
		{"", `""`},
		{"CamelCase", `"CamelCase"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteIdent(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdent(%q) = %q, expected %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

// TestQualifiedPSS verifies pg_stat_statements table qualification.
func TestQualifiedPSS(t *testing.T) {
	tests := []struct {
		schema   string
		expected string
	}{
		{"", "pg_stat_statements"},
		{"public", `"public".pg_stat_statements`},
		{"my_schema", `"my_schema".pg_stat_statements`},
	}

	for _, tt := range tests {
		t.Run(tt.schema, func(t *testing.T) {
			result := qualifiedPSS(tt.schema)
			if result != tt.expected {
				t.Errorf("qualifiedPSS(%q) = %q, expected %q",
					tt.schema, result, tt.expected)
			}
		})
	}
}

// TestConstants verifies that constants have sensible values.
func TestConstants(t *testing.T) {
	if MinTimeout <= 0 {
		t.Error("MinTimeout should be positive")
	}

	if MaxTimeout <= MinTimeout {
		t.Error("MaxTimeout should be greater than MinTimeout")
	}

	if DefaultTimeout < MinTimeout || DefaultTimeout > MaxTimeout {
		t.Error("DefaultTimeout should be between MinTimeout and MaxTimeout")
	}

	if unusedIndexMinSize <= 0 {
		t.Error("unusedIndexMinSize should be positive")
	}

	if planPerListCap <= 0 {
		t.Error("planPerListCap should be positive")
	}
}

// TestResultInitialization verifies Result struct can be used with zero values.
func TestResultInitialization(t *testing.T) {
	var res Result

	// Verify slices are nil by default (Go behavior)
	if res.Tables != nil {
		t.Error("Tables should be nil by default")
	}

	// Verify zero values work correctly
	if res.CacheHitCurrent != 0 {
		t.Error("CacheHitCurrent should be zero by default")
	}

	if res.TotalConnections != 0 {
		t.Error("TotalConnections should be zero by default")
	}
}

// BenchmarkQuoteIdent benchmarks identifier quoting.
func BenchmarkQuoteIdent(b *testing.B) {
	input := "my_schema_name"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		quoteIdent(input)
	}
}

// BenchmarkSwapDBInURL benchmarks URL database swapping.
func BenchmarkSwapDBInURL(b *testing.B) {
	url := "postgres://user:password@localhost:5432/olddb?sslmode=require"
	db := "newdb"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swapDBInURL(url, db)
	}
}
