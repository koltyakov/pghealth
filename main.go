package main

import (
	"context"
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

var version = "0.1.0"

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	start := time.Now()

	res, err := collect.Run(ctx, cfg.ToCollectorConfig())
	if err != nil {
		log.Printf("collection warning: %v", err)
	}

	analysis := analyze.Run(res)
	// Filter recommendations if requested
	if cfg.Suppress != "" {
		suppressed := parseSuppressedSet(cfg.Suppress)
		if len(suppressed) > 0 {
			filtered := analysis.Recommendations[:0]
			for _, rec := range analysis.Recommendations {
				code := rec.Code
				if code == "" {
					code = slugify(rec.Title)
				}
				if _, skip := suppressed[code]; skip {
					continue
				}
				filtered = append(filtered, rec)
			}
			analysis.Recommendations = filtered
		}
	}

	outPath := cfg.Output
	if outPath == "-" || outPath == "" {
		outPath = "report.html"
	}
	// Expand timestamp placeholders in output path
	outPath = expandOutPlaceholders(outPath, start)

	if err := report.WriteHTML(outPath, res, analysis, collect.Meta{StartedAt: start, Duration: time.Since(start), Version: version}); err != nil {
		log.Fatalf("failed to write report: %v", err)
	}

	fmt.Printf("Report written to %s\n", outPath)
	if cfg.Open && outPath != "-" {
		if err := openReport(outPath); err != nil {
			log.Printf("failed to open report: %v", err)
		}
	}
}

type Flags struct {
	URL      string
	Output   string
	Timeout  time.Duration
	Open     bool
	Stats    string
	Suppress string
}

func (f Flags) ToCollectorConfig() collect.Config {
	return collect.Config{
		URL:        f.URL,
		Timeout:    f.Timeout,
		StatsSince: f.Stats,
	}
}

func parseFlags() Flags {
	var f Flags
	defURL := firstNonEmpty(os.Getenv("PGURL"), os.Getenv("DATABASE_URL"))

	flag.StringVar(&f.URL, "url", defURL, "Postgres connection string (e.g., postgres://user:pass@host:5432/db?sslmode=require)")
	flag.StringVar(&f.Output, "out", "report.html", "Output HTML file path (supports {ts} -> 2006-01-02_1504)")
	flag.DurationVar(&f.Timeout, "timeout", 30*time.Second, "Overall timeout")
	flag.BoolVar(&f.Open, "open", true, "Open the report after generation")
	flag.StringVar(&f.Stats, "stats", "", "Collect pg_stat_statements data since this duration (e.g. '24h', '7d')")
	flag.StringVar(&f.Suppress, "suppress", "", "Comma-separated recommendation codes to suppress (e.g. install-pgss,large-unused-indexes; also accepts title slugs)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if f.URL == "" {
		// Optional positional arg as URL
		if flag.NArg() >= 1 {
			f.URL = flag.Arg(0)
		}
	}

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	return f
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func openReport(path string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", path).Start()
	case "windows":
		// Using rundll32 avoids issues with cmd quoting
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", path).Start()
	default:
		return exec.Command("xdg-open", path).Start()
	}
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

// expandOutPlaceholders replaces {ts} in the output path.
// {ts} -> 2006-01-02_1504 (e.g., 2024-08-30_0823)
func expandOutPlaceholders(p string, t time.Time) string {
	if p == "" {
		return p
	}
	// Ensure t is valid
	if t.IsZero() {
		t = time.Now()
	}
	// Only {ts}
	return strings.ReplaceAll(p, "{ts}", t.Format("2006-01-02_1504"))
}
