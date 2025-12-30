// Package collect provides PostgreSQL metrics collection functionality.
//
// This package handles connecting to PostgreSQL databases and collecting
// various performance metrics including:
//   - Connection information and settings
//   - Table and index statistics
//   - Query performance data from pg_stat_statements
//   - Cache hit ratios and memory usage
//   - Blocking queries and long-running transactions
//   - Replication status and WAL statistics
package collect

import (
	"errors"
	"time"
)

// Default configuration values.
const (
	// DefaultTimeout is the default timeout for database operations.
	DefaultTimeout = 30 * time.Second

	// MinTimeout is the minimum allowed timeout.
	MinTimeout = 5 * time.Second

	// MaxTimeout is the maximum allowed timeout.
	MaxTimeout = 10 * time.Minute
)

// Config holds the configuration for the metrics collector.
type Config struct {
	// URL is the PostgreSQL connection string.
	// Format: postgres://user:pass@host:5432/database?sslmode=require
	URL string `json:"url" yaml:"url"`

	// Timeout is the maximum duration for the entire collection process.
	Timeout time.Duration `json:"timeout" yaml:"timeout"`

	// StatsSince filters pg_stat_statements data to only include stats
	// newer than this duration (e.g., "24h", "7d").
	StatsSince string `json:"stats_since" yaml:"stats_since"`

	// DBs is a list of additional database names to collect metrics from.
	// The collector will connect to each database to gather database-specific stats.
	DBs []string `json:"dbs" yaml:"dbs"`
}

// Validate checks that the configuration is valid.
func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("database URL is required")
	}

	if c.Timeout < MinTimeout {
		return errors.New("timeout must be at least 5 seconds")
	}

	if c.Timeout > MaxTimeout {
		return errors.New("timeout exceeds maximum of 10 minutes")
	}

	return nil
}

// Meta contains metadata about the collection run.
type Meta struct {
	// StartedAt is when the collection started.
	StartedAt time.Time `json:"started_at"`

	// Duration is how long the collection took.
	Duration time.Duration `json:"duration"`

	// Version is the pghealth version that generated the report.
	Version string `json:"version"`
}
