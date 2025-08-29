package analyze

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/koltyakov/pghealth/internal/collect"
)

type Analysis struct {
	Recommendations []Finding
	Warnings        []Finding
	Infos           []Finding
}

type Finding struct {
	Title       string
	Severity    string // info, warn, rec
	Description string
	Action      string
}

func Run(res collect.Result) Analysis {
	a := Analysis{}

	// Privilege and extensions
	if !res.Extensions.PgStatStatements {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Install pg_stat_statements",
			Severity:    "rec",
			Description: "pg_stat_statements is not installed. Without it, detailed query performance analysis is limited.",
			Action:      "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; and set shared_preload_libraries='pg_stat_statements' then restart.",
		})
	}
	if !res.ConnInfo.IsSuperuser && !res.Roles.HasPgMonitor {
		a.Infos = append(a.Infos, Finding{
			Title:       "Limited privileges",
			Severity:    "info",
			Description: "Current role lacks superuser/pg_monitor; some stats may be unavailable.",
			Action:      "Ask a DBA/DevOps to grant membership in pg_monitor for richer visibility.",
		})
	}

	// Connections health
	totalActive := 0
	for _, s := range res.Activity {
		if s.State == "active" {
			totalActive += s.Count
		}
	}
	if res.ConnInfo.MaxConnections > 0 && totalActive > int(float64(res.ConnInfo.MaxConnections)*0.8) {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "High active connections",
			Severity:    "warn",
			Description: fmt.Sprintf("Active connections %d are above 80%% of max_connections (%d)", totalActive, res.ConnInfo.MaxConnections),
			Action:      "Consider using a connection pooler (e.g., pgbouncer) and review max_connections and work_mem settings.",
		})
	}

	// Settings quick checks
	setting := func(name string) (collect.Setting, bool) {
		for _, s := range res.Settings {
			if s.Name == name {
				return s, true
			}
		}
		return collect.Setting{}, false
	}
	if s, ok := setting("track_io_timing"); ok && (s.Val == "off" || s.Val == "0") {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Enable track_io_timing",
			Severity:    "rec",
			Description: "track_io_timing is off; enabling provides better latency insights.",
			Action:      "SET track_io_timing = on; then persist in postgresql.conf and reload.",
		})
	}
	if s, ok := setting("autovacuum"); ok && (s.Val == "off" || s.Val == "0") {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Autovacuum disabled",
			Severity:    "warn",
			Description: "Autovacuum appears disabled; this risks bloat and xid wraparound.",
			Action:      "Enable autovacuum and tune thresholds/freeze settings.",
		})
	}

	// wal_level best practice
	if s, ok := setting("wal_level"); ok && s.Val == "minimal" {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "wal_level is minimal",
			Severity:    "rec",
			Description: "wal_level=minimal disables replication and can hinder PITR; production systems typically use 'replica' or 'logical'.",
			Action:      "Set wal_level=replica (or logical if needed) and restart.",
		})
	}
	// checkpoint timeout sanity
	if s, ok := setting("checkpoint_timeout"); ok {
		if secs := asSeconds(s, true); secs > 0 && secs < 120 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "checkpoint_timeout is very low",
				Severity:    "rec",
				Description: fmt.Sprintf("checkpoint_timeout=%.0fs; frequent checkpoints may increase write amplification.", secs),
				Action:      "Consider 5-15 minutes depending on workload; tune with max_wal_size.",
			})
		}
	}
	// memory ratios
	sb, _ := asBytes(setting("shared_buffers"))
	ecs, _ := asBytes(setting("effective_cache_size"))
	if sb > 0 && ecs > 0 && ecs < 2*sb {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "effective_cache_size seems low vs shared_buffers",
			Severity:    "rec",
			Description: "effective_cache_size is typically 2-3x shared_buffers to reflect OS page cache.",
			Action:      "Increase effective_cache_size to approximate available OS cache.",
		})
	}
	wm, _ := asBytes(setting("work_mem"))
	if wm > 0 && res.ConnInfo.MaxConnections > 0 && ecs > 0 {
		totalPotential := wm * int64(res.ConnInfo.MaxConnections)
		if totalPotential > ecs*2 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "work_mem may be high",
				Severity:    "warn",
				Description: fmt.Sprintf("work_mem x max_connections could exceed memory (%.1f GB vs cache %.1f GB)", bytesToGB(totalPotential), bytesToGB(ecs)),
				Action:      "Lower work_mem or rely on memory context tuning; consider connection pooler to cap concurrency.",
			})
		}
	}

	// Table bloat heuristics
	type blo struct {
		schema, table string
		pct           float64
	}
	var bloats []blo
	for _, t := range res.Tables {
		if t.BloatPct > 20 && (t.NLiveTup+t.NDeadTup) > 10000 {
			bloats = append(bloats, blo{t.Schema, t.Name, t.BloatPct})
		}
	}
	sort.Slice(bloats, func(i, j int) bool { return bloats[i].pct > bloats[j].pct })
	if len(bloats) > 0 {
		top := bloats
		if len(top) > 10 {
			top = top[:10]
		}
		list := ""
		for i, b := range top {
			if i > 0 {
				list += ", "
			}
			list += fmt.Sprintf("%s.%s(%.0f%%)", b.schema, b.table, b.pct)
		}
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Potential table bloat",
			Severity:    "warn",
			Description: fmt.Sprintf("Tables with high dead tuple ratio: %s", list),
			Action:      "Investigate autovacuum, consider REINDEX/VACUUM (FULL) or pg_repack on large bloated relations.",
		})
	}

	// Unused indexes
	if len(res.IndexUnused) > 0 {
		names := ""
		max := 10
		for i, ix := range res.IndexUnused {
			if i >= max {
				break
			}
			if i > 0 {
				names += ", "
			}
			names += fmt.Sprintf("%s.%s", ix.Schema, ix.Name)
		}
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Unused large indexes",
			Severity:    "rec",
			Description: fmt.Sprintf("%d indexes show zero scans; first examples: %s", len(res.IndexUnused), names),
			Action:      "Validate with workload owners and drop truly unused indexes to reduce write/maintenance overhead.",
		})
	}

	// Missing index hints
	if len(res.MissingIndexes) > 0 {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Possible missing indexes",
			Severity:    "rec",
			Description: "Some tables show heavy sequential scans with low index usage.",
			Action:      "EXPLAIN problematic queries; create indexes on selective predicates/joins as appropriate.",
		})
	}

	// Statements if available
	if res.Statements.Available {
		if len(res.Statements.TopByTotalTime) > 0 {
			q := res.Statements.TopByTotalTime[0]
			a.Infos = append(a.Infos, Finding{
				Title:       "Top query by total time",
				Severity:    "info",
				Description: fmt.Sprintf("Calls: %.0f, TotalTime: %.2f ms", q.Calls, q.TotalTime),
				Action:      "Review execution plan and caching. Consider increasing work_mem for heavy sorts/aggregations.",
			})
		}
	} else {
		a.Infos = append(a.Infos, Finding{
			Title:       "Query-level analysis limited",
			Severity:    "info",
			Description: "pg_stat_statements not available; only coarse-grained insights reported.",
			Action:      "Install and configure pg_stat_statements for detailed top queries.",
		})
	}

	return a
}

func asBytes(s collect.Setting, ok bool) (int64, bool) {
	if !ok {
		return 0, false
	}
	return parseWithUnit(s.Val, s.Unit)
}
func asSeconds(s collect.Setting, ok bool) float64 {
	if !ok {
		return 0
	}
	val, _ := strconv.ParseFloat(s.Val, 64)
	switch s.Unit {
	case "ms":
		return val / 1000
	case "s", "":
		return val
	case "min":
		return val * 60
	case "h":
		return val * 3600
	default:
		return val
	}
}
func parseWithUnit(val string, unit string) (int64, bool) {
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false
	}
	switch unit {
	case "B", "":
		return n, true
	case "kB":
		return n * 1024, true
	case "8kB":
		return n * 8 * 1024, true
	case "MB":
		return n * 1024 * 1024, true
	case "GB":
		return n * 1024 * 1024 * 1024, true
	default:
		return n, true
	}
}
func bytesToGB(b int64) float64 { return float64(b) / (1024 * 1024 * 1024) }
