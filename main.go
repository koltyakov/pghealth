package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
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

	outPath := cfg.Output
	if outPath == "-" || outPath == "" {
		outPath = "report.html"
	}

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
	URL     string
	Output  string
	Timeout time.Duration
	Open    bool
	Stats   string
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
	flag.StringVar(&f.Output, "out", "report.html", "Output HTML file path")
	flag.DurationVar(&f.Timeout, "timeout", 30*time.Second, "Overall timeout")
	flag.BoolVar(&f.Open, "open", true, "Open the report after generation")
	flag.StringVar(&f.Stats, "stats", "", "Collect pg_stat_statements data since this duration (e.g. '24h', '7d')")
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
