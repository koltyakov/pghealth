// Package main provides the pghealth command-line tool for PostgreSQL health analysis.
//
// pghealth connects to a PostgreSQL database, collects performance metrics,
// analyzes them against best practices, and generates an HTML report with
// actionable recommendations.
//
// Usage:
//
//	pghealth -url postgres://user:pass@host:5432/db
//	pghealth -url postgres://host/db -out report.html -timeout 60s
//
// Environment variables:
//
//	PGURL or DATABASE_URL - Default PostgreSQL connection string
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/koltyakov/pghealth/internal/analyze"
	"github.com/koltyakov/pghealth/internal/collect"
	"github.com/koltyakov/pghealth/internal/report"
)

// version is the current application version, set at build time.
var version = "0.1.0"

// Configuration constants define default values and limits.
const (
	// defaultTimeout is the default timeout for database operations.
	defaultTimeout = 30 * time.Second

	// defaultOutputFile is the default output file name for the HTML report.
	defaultOutputFile = "report.html"

	// timestampPlaceholder is replaced with the report generation timestamp.
	timestampPlaceholder = "{ts}"

	// timestampFormat defines the format for timestamp placeholders.
	timestampFormat = "2006-01-02_1504"
)

// Exit codes for different error conditions.
const (
	exitSuccess      = 0
	exitUsageError   = 1
	exitCollectError = 2
	exitReportError  = 3
	exitOpenError    = 4
)

func main() {
	os.Exit(run())
}

// run executes the main application logic and returns an exit code.
// This separation allows for easier testing and cleaner error handling.
//
// WORKFLOW:
//  1. Parse and validate command-line flags
//  2. Create collector configuration with timeout context
//  3. Collect PostgreSQL metrics (may be partial)
//  4. Run analysis on collected metrics
//  5. Generate HTML report (and optional prompt sidecar)
//  6. Optionally open report in browser
//
// EXIT CODES:
//   - 0: Success
//   - 1: Configuration/usage error
//   - 2: Collection error (timeout, connection failure)
//   - 3: Report generation error
//   - 4: Report open error (currently unused - non-fatal)
func run() int {
	cfg, err := parseFlags()
	if err != nil {
		if errors.Is(err, errShowVersion) {
			fmt.Println(version)
			return exitSuccess
		}
		log.Printf("configuration error: %v", err)
		return exitUsageError
	}

	// Validate configuration before proceeding
	if err := cfg.Validate(); err != nil {
		log.Printf("invalid configuration: %v", err)
		return exitUsageError
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	start := time.Now()

	res, err := collect.Run(ctx, cfg.ToCollectorConfig())
	if err != nil {
		// Log as warning but continue - partial data may still be useful
		log.Printf("collection warning: %v", err)
	}

	// Check if context was cancelled during collection
	if ctx.Err() != nil {
		log.Printf("operation timed out after %v", cfg.Timeout)
		return exitCollectError
	}

	analysis := analyze.Run(res)

	// Filter recommendations if suppression list is provided
	if cfg.Suppress != "" {
		analysis = filterSuppressedRecommendations(analysis, cfg.Suppress)
	}

	outPath := resolveOutputPath(cfg.Output, start)

	meta := collect.Meta{
		StartedAt: start,
		Duration:  time.Since(start),
		Version:   version,
	}

	if err := report.WriteHTML(outPath, res, analysis, meta); err != nil {
		log.Printf("failed to write report: %v", err)
		return exitReportError
	}

	fmt.Printf("Report written to %s\n", outPath)

	if cfg.Prompt {
		if err := writePromptIfRequested(outPath, res, meta); err != nil {
			log.Printf("failed to write prompt: %v", err)
			// Continue execution - prompt is supplementary
		}
	}

	if cfg.Open && outPath != "-" {
		if err := openReport(outPath); err != nil {
			log.Printf("failed to open report: %v", err)
			// Non-fatal error - report was generated successfully
		}
	}

	return exitSuccess
}

// filterSuppressedRecommendations removes recommendations matching the suppression list.
func filterSuppressedRecommendations(analysis analyze.Analysis, suppressList string) analyze.Analysis {
	suppressed := parseSuppressedSet(suppressList)
	if len(suppressed) == 0 {
		return analysis
	}

	filtered := make([]analyze.Finding, 0, len(analysis.Recommendations))
	for _, rec := range analysis.Recommendations {
		code := rec.Code
		if code == "" {
			code = slugify(rec.Title)
		}
		if _, skip := suppressed[code]; !skip {
			filtered = append(filtered, rec)
		}
	}
	analysis.Recommendations = filtered
	return analysis
}

// resolveOutputPath determines the final output path, applying defaults and placeholders.
func resolveOutputPath(path string, timestamp time.Time) string {
	if path == "-" || path == "" {
		path = defaultOutputFile
	}
	return expandOutPlaceholders(path, timestamp)
}

// writePromptIfRequested writes the LLM prompt sidecar file if successfully generated.
func writePromptIfRequested(outPath string, res collect.Result, meta collect.Meta) error {
	promptPath, err := report.WritePrompt(outPath, res, meta)
	if err != nil {
		return fmt.Errorf("write prompt: %w", err)
	}
	if promptPath != "" {
		fmt.Printf("LLM prompt written to %s\n", promptPath)
	}
	return nil
}

// errShowVersion is returned when the -version flag is set.
var errShowVersion = errors.New("show version requested")

// Flags holds the command-line configuration options.
type Flags struct {
	URL      string        // PostgreSQL connection string
	Output   string        // Output file path for HTML report
	Timeout  time.Duration // Overall timeout for database operations
	Open     bool          // Whether to open the report after generation
	Suppress string        // Comma-separated recommendation codes to suppress
	DBs      string        // Comma-separated additional database names
	Prompt   bool          // Whether to generate LLM prompt sidecar
}

// Validate checks that the configuration is valid and returns an error if not.
func (f Flags) Validate() error {
	if f.URL == "" {
		return errors.New("database URL is required: use -url flag or set PGURL/DATABASE_URL environment variable")
	}

	if f.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	if f.Timeout > 10*time.Minute {
		return errors.New("timeout exceeds maximum allowed value of 10 minutes")
	}

	return nil
}

// ToCollectorConfig converts Flags to the collector configuration.
func (f Flags) ToCollectorConfig() collect.Config {
	return collect.Config{
		URL:     f.URL,
		Timeout: f.Timeout,
		DBs:     splitCSV(f.DBs),
	}
}

// parseFlags parses command-line flags and returns the configuration.
// Returns errShowVersion if the -version flag was specified.
func parseFlags() (Flags, error) {
	var f Flags
	defURL := firstNonEmpty(os.Getenv("PGURL"), os.Getenv("DATABASE_URL"))

	flag.StringVar(&f.URL, "url", defURL, "Postgres connection string (e.g., postgres://user:pass@host:5432/db?sslmode=require)")
	flag.StringVar(&f.Output, "out", defaultOutputFile, "Output HTML file path (supports {ts} -> 2006-01-02_1504)")
	flag.DurationVar(&f.Timeout, "timeout", defaultTimeout, "Overall timeout for database operations")
	flag.BoolVar(&f.Open, "open", true, "Open the report after generation")
	flag.StringVar(&f.DBs, "dbs", "", "Comma-separated database names to extend metrics from")
	flag.BoolVar(&f.Prompt, "prompt", false, "Generate an LLM prompt sidecar (.prompt.txt) next to the HTML report")
	flag.StringVar(&f.Suppress, "suppress", "", "Comma-separated recommendation codes to suppress")
	showVersion := flag.Bool("version", false, "Show version and exit")

	flag.Parse()

	// Check for version flag first
	if *showVersion {
		return Flags{}, errShowVersion
	}

	// Allow URL as positional argument for convenience
	if f.URL == "" && flag.NArg() >= 1 {
		f.URL = flag.Arg(0)
	}

	return f, nil
}

// firstNonEmpty returns the first non-empty string from the provided values.
// Returns empty string if all values are empty.
func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// openReport opens the generated report using the system's default browser.
// Returns an error if the open command fails to start.
func openReport(path string) error {
	if path == "" {
		return errors.New("empty path provided")
	}

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", path)
	case "windows":
		// Using rundll32 avoids issues with cmd quoting
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", path)
	default:
		// Assume Linux/Unix with xdg-open
		cmd = exec.Command("xdg-open", path)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start browser command: %w", err)
	}
	return nil
}

// slugify converts a string to a simple code: lowercase, non-alphanumerics to '-'.
func slugify(s string) string {
	// Fast path: if empty
	if s == "" {
		return s
	}
	// To lower and map runes
	b := make([]rune, 0, len(s))
	prevHyphen := false
	for _, r := range s {
		// normalize ASCII letters to lower
		if r >= 'A' && r <= 'Z' {
			r = r + ('a' - 'A')
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b = append(b, r)
			prevHyphen = false
			continue
		}
		// turn any other char into single hyphen (collapse repeats)
		if !prevHyphen {
			b = append(b, '-')
			prevHyphen = true
		}
	}
	// trim leading/trailing '-'
	// find start
	start := 0
	for start < len(b) && b[start] == '-' {
		start++
	}
	end := len(b)
	for end > start && b[end-1] == '-' {
		end--
	}
	return string(b[start:end])
}

func parseSuppressedSet(list string) map[string]struct{} {
	m := map[string]struct{}{}
	if list == "" {
		return m
	}
	parts := strings.Split(list, ",")
	for _, p := range parts {
		code := strings.TrimSpace(p)
		if code == "" {
			continue
		}
		// Normalize by slugifying as well to match title-derived slugs
		m[slugify(code)] = struct{}{}
	}
	return m
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// expandOutPlaceholders replaces placeholder tokens in the output path.
// Currently supported placeholders:
//   - {ts} -> timestamp in format 2006-01-02_1504 (e.g., 2024-08-30_0823)
//
// If the provided time is zero, the current time is used.
func expandOutPlaceholders(p string, t time.Time) string {
	if p == "" {
		return p
	}

	// Use current time if provided time is zero
	if t.IsZero() {
		t = time.Now()
	}

	return strings.ReplaceAll(p, timestampPlaceholder, t.Format(timestampFormat))
}
