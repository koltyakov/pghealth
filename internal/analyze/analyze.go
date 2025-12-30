// Package analyze provides analysis and recommendations for PostgreSQL health metrics.
//
// This package takes collected metrics from the collect package and generates
// actionable findings including:
//   - Recommendations: Suggested improvements for performance and reliability
//   - Warnings: Issues that need attention but aren't critical
//   - Infos: Informational findings about the database state
package analyze

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/koltyakov/pghealth/internal/collect"
)

// Severity levels for findings.
const (
	SeverityInfo    = "info" // Informational finding
	SeverityWarning = "warn" // Warning - needs attention
	SeverityRec     = "rec"  // Recommendation for improvement
)

// Threshold constants for analysis heuristics.
// These values are based on PostgreSQL best practices and can be tuned.
const (
	// cacheHitThreshold is the minimum acceptable cache hit ratio percentage.
	cacheHitThreshold = 95.0

	// connectionUsageWarningPct triggers a warning when connection usage exceeds this.
	connectionUsageWarningPct = 80.0

	// longRunningQueryThreshold defines what constitutes a "long" query.
	longRunningQueryThreshold = 5 * time.Minute

	// tableBloatWarningPct triggers a warning when table bloat exceeds this.
	tableBloatWarningPct = 20.0

	// tableBloatSevereThreshold indicates severe bloat requiring VACUUM FULL.
	tableBloatSevereThreshold = 50.0

	// minRowsForBloatAnalysis is the minimum row count to consider for bloat analysis.
	minRowsForBloatAnalysis = 10000

	// unusedIndexSizeThreshold is the minimum size (bytes) for an unused index to be flagged.
	unusedIndexSizeThreshold = 100 * 1024 * 1024 // 100MB

	// maxIndexesPerTableWarning triggers a warning when a table has more indexes than this.
	maxIndexesPerTableWarning = 10

	// minRowsForIndexWarning is the minimum rows for a table without indexes to be flagged.
	minRowsForIndexWarning = 1000

	// highConnectionsThreshold triggers a recommendation when max_connections exceeds this.
	highConnectionsThreshold = 100

	// walHighWriteRateBytesPerSec is the WAL write rate (bytes/sec) that triggers a warning.
	walHighWriteRateBytesPerSec = 10 * 1024 * 1024 // 10MB/s

	// fpiHighRatio is the full-page image ratio that triggers a warning.
	fpiHighRatio = 0.5

	// fpiModerateRatio is the FPI ratio that triggers a recommendation.
	fpiModerateRatio = 0.2

	// xidWarningPct triggers a warning when XID age exceeds this percentage of max.
	xidWarningPct = 50.0

	// xidCriticalPct triggers a critical warning when XID age exceeds this.
	xidCriticalPct = 75.0

	// idleInTransactionMinutes is the minimum idle-in-transaction duration to flag.
	idleInTransactionMinutes = 5

	// staleStatsDays is the number of days without analyze to flag.
	staleStatsDays = 7

	// sequenceWarningPct triggers a warning when sequence usage exceeds this.
	sequenceWarningPct = 50.0

	// sequenceCriticalPct triggers a critical warning for sequence exhaustion risk.
	sequenceCriticalPct = 80.0

	// preparedXactAgeHours is the age in hours for a prepared transaction to be flagged.
	preparedXactAgeHours = 1
)

// Analysis contains categorized findings from the metrics analysis.
type Analysis struct {
	// Recommendations are suggested improvements that would benefit performance.
	Recommendations []Finding

	// Warnings are issues that need attention and may impact availability.
	Warnings []Finding

	// Infos are informational observations about the database state.
	Infos []Finding
}

// Finding represents a single analysis finding with its details.
type Finding struct {
	// Title is a short descriptive name for the finding.
	Title string

	// Severity indicates the importance level (info, warn, rec).
	Severity string

	// Code is a machine-readable identifier for suppression support.
	Code string

	// Description provides details about what was found.
	Description string

	// Action suggests what steps to take to address the finding.
	Action string
}

// Run analyzes the collected PostgreSQL metrics and returns categorized findings.
// The analysis covers connection health, cache efficiency, query performance,
// index usage, bloat detection, and configuration best practices.
//
// INVARIANTS:
//   - Input res should contain valid collected metrics (not necessarily complete)
//   - Output slices are never nil (always initialized)
//   - All findings have non-empty Title and Severity
func Run(res collect.Result) Analysis {
	a := Analysis{
		Recommendations: make([]Finding, 0, 16), // Pre-allocate for typical case
		Warnings:        make([]Finding, 0, 8),
		Infos:           make([]Finding, 0, 16),
	}

	// Uptime info
	if !res.ConnInfo.StartTime.IsZero() {
		up := time.Since(res.ConnInfo.StartTime)
		a.Infos = append(a.Infos, Finding{
			Title:       "Server uptime",
			Severity:    SeverityInfo,
			Description: fmt.Sprintf("%s (since %s)", humanizeDuration(up), formatLocalTime(res.ConnInfo.StartTime)),
			Action:      "",
		})
	}

	// Cache hit ratios
	if res.CacheHitCurrent > 0 {
		if res.CacheHitCurrent < cacheHitThreshold {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Low cache hit ratio (current DB)",
				Severity:    SeverityWarning,
				Description: fmt.Sprintf("Cache hit: %.1f%%", res.CacheHitCurrent),
				Action:      "Review working set size, shared_buffers, and query patterns; ensure sufficient memory and indexes.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Cache hit ratio (current)", Severity: SeverityInfo, Description: fmt.Sprintf("%.1f%%", res.CacheHitCurrent)})
		}
	}
	if res.CacheHitOverall > 0 {
		if res.CacheHitOverall < cacheHitThreshold {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Overall cache hit could improve",
				Severity:    SeverityRec,
				Code:        "cache-overall",
				Description: fmt.Sprintf("Cluster-wide cache hit: %.1f%%", res.CacheHitOverall),
				Action:      "Consider memory tuning and index coverage across busiest databases.",
			})
		}
	}

	// Connection usage
	if res.ConnInfo.MaxConnections > 0 && res.TotalConnections > 0 {
		pct := float64(res.TotalConnections) / float64(res.ConnInfo.MaxConnections) * 100
		if pct >= connectionUsageWarningPct {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "High connection usage",
				Severity:    SeverityWarning,
				Description: fmt.Sprintf("%d/%d (%.0f%%) connections in use", res.TotalConnections, res.ConnInfo.MaxConnections, pct),
				Action:      "Use a pooler (pgbouncer), limit app connection pools, and tune max_connections accordingly.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Connection usage", Severity: SeverityInfo, Description: fmt.Sprintf("%d/%d (%.0f%%)", res.TotalConnections, res.ConnInfo.MaxConnections, pct)})
		}
	}

	// Blocking and long running queries
	if len(res.Blocking) > 0 {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Blocking detected",
			Severity:    "warn",
			Description: fmt.Sprintf("%d blocked sessions", len(res.Blocking)),
			Action:      "Inspect lock tree, add indexes, shorten transactions, consider lock timeouts.",
		})
	}
	if len(res.LongRunning) > 0 {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Long-running queries",
			Severity:    "rec",
			Code:        "long-running",
			Description: fmt.Sprintf("%d active queries > 5m", len(res.LongRunning)),
			Action:      "EXPLAIN ANALYZE top offenders; optimize plans, add indexes, break large batches.",
		})
	}
	if len(res.AutoVacuum) > 0 {
		a.Infos = append(a.Infos, Finding{
			Title:       "Autovacuum activity",
			Severity:    "info",
			Description: fmt.Sprintf("%d vacuum workers in progress", len(res.AutoVacuum)),
			Action:      "Ensure autovacuum is not throttled for large tables; tune naptime, scale_factor, and cost limits if needed.",
		})
	}

	// Privilege and extensions
	if !res.Extensions.PgStatStatements {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Install pg_stat_statements",
			Severity:    "rec",
			Code:        "install-pgss",
			Description: "pg_stat_statements is not installed. Without it, detailed query performance analysis is limited.",
			Action:      "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; and set shared_preload_libraries='pg_stat_statements' then restart.",
		})
	}
	if !res.ConnInfo.IsSuperuser && !res.Roles.HasPgMonitor {
		a.Infos = append(a.Infos, Finding{
			Title:       "Limited privileges",
			Severity:    "info",
			Description: "Current role lacks superuser/pg_monitor; some stats may be unavailable.",
			Action:      "Ask an admin to grant membership in pg_monitor for richer visibility.",
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
			Code:        "enable-track-io",
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
			Code:        "wal-level-minimal",
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
				Code:        "checkpoint-timeout-low",
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
			Code:        "ecs-low-vs-sb",
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

	// Memory consumption insights
	if res.MemoryStats.SharedBuffersBytes > 0 {
		used := res.MemoryStats.BuffercacheUsedBytes
		total := res.MemoryStats.SharedBuffersBytes
		if used > 0 && total > 0 {
			pct := float64(used) / float64(total) * 100
			a.Infos = append(a.Infos, Finding{
				Title:       "Shared buffers utilization",
				Severity:    "info",
				Description: fmt.Sprintf("~%.0f%% of shared_buffers in use (%0.2f GB of %0.2f GB)", pct, bytesToGB(used), bytesToGB(total)),
				Action:      "If utilization is persistently low, consider right-sizing shared_buffers; if high with low hit ratio, consider more memory and indexing.",
			})
		}
	}
	if res.MemoryStats.TempBytesCurrentDB > 0 {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Temporary file churn",
			Severity:    "warn",
			Description: fmt.Sprintf("Current DB used %.2f GB in temp files across %d files (since stats reset)", bytesToGB(res.MemoryStats.TempBytesCurrentDB), res.MemoryStats.TempFilesCurrentDB),
			Action:      "Increase work_mem for large sorts/hashes, optimize queries to avoid spills, and consider temp_file_limit.",
		})
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
			Title:       "Potential table bloat (heuristic)",
			Severity:    "warn",
			Code:        "table-bloat-heuristic",
			Description: fmt.Sprintf("Tables with high dead tuple ratio: %s", list),
			Action:      "Rows highlighted in 'Tables with index counts' exceed ~20% bloat by dead tuple share. Short-term: run VACUUM; for severe cases (>50%) schedule VACUUM FULL or pg_repack during maintenance. Long-term: tune autovacuum thresholds (lower scale_factor for hot tables), consider lower fillfactor to improve HOT updates, and periodically REINDEX if indexes are bloated.",
		})
	}

	// Unused indexes (consolidated): combine candidates from idx_scan=0 and from index bloat stats with scans=0
	if len(res.IndexUnused) > 0 || len(res.IndexBloatStats) > 0 {
		type key struct{ db, schema, name string }
		combined := map[key]collect.IndexUnused{}
		for _, iu := range res.IndexUnused {
			db := strings.TrimSpace(iu.Database)
			if db == "" {
				db = strings.TrimSpace(res.ConnInfo.CurrentDB)
			}
			k := key{db, iu.Schema, iu.Name}
			if prev, ok := combined[k]; !ok || iu.SizeBytes > prev.SizeBytes {
				combined[k] = iu
			}
		}
		for _, ib := range res.IndexBloatStats {
			if ib.Scans == 0 {
				k := key{strings.TrimSpace(res.ConnInfo.CurrentDB), ib.Schema, ib.Name}
				if prev, ok := combined[k]; !ok || ib.WastedBytes > prev.SizeBytes {
					combined[k] = collect.IndexUnused{Database: res.ConnInfo.CurrentDB, Schema: ib.Schema, Table: ib.Table, Name: ib.Name, SizeBytes: ib.WastedBytes}
				}
			}
		}
		if len(combined) > 0 {
			// materialize for sampling and count large ones
			list := make([]collect.IndexUnused, 0, len(combined))
			for _, v := range combined {
				list = append(list, v)
			}
			sort.Slice(list, func(i, j int) bool { return list[i].SizeBytes > list[j].SizeBytes })
			names := ""
			max := 10
			for i, ix := range list {
				if i >= max {
					break
				}
				if i > 0 {
					names += ", "
				}
				names += fmt.Sprintf("%s.%s", ix.Schema, ix.Name)
			}
			large := 0
			for _, ix := range list {
				if ix.SizeBytes > 100*1024*1024 {
					large++
				}
			}
			desc := fmt.Sprintf("%d unused index candidates; examples: %s", len(list), names)
			if large > 0 {
				desc += fmt.Sprintf(" (%d >100MB)", large)
			}
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Unused indexes",
				Severity:    "rec",
				Code:        "unused-indexes",
				Description: desc,
				Action:      "Validate with workload owners and drop truly unused indexes to reduce write/maintenance overhead.",
			})
		}
	}

	// Missing index hints
	if len(res.MissingIndexes) > 0 {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Possible missing indexes",
			Severity:    "rec",
			Code:        "missing-indexes",
			Description: "Some tables show heavy sequential scans with low index usage.",
			Action:      "EXPLAIN problematic queries; create indexes on selective predicates/joins as appropriate.",
		})
	}

	// Statements / pg_stat_statements context
	if res.Statements.Available {
		if !res.Statements.StatsResetTime.IsZero() {
			statsAge := time.Since(res.Statements.StatsResetTime)
			a.Infos = append(a.Infos, Finding{
				Title:       "Query stats window",
				Severity:    "info",
				Description: fmt.Sprintf("pg_stat_statements data covers the last %s (since %s)", humanizeDuration(statsAge), formatLocalTime(res.Statements.StatsResetTime)),
				Action:      "Run `SELECT pg_stat_statements_reset()` to clear stats if needed.",
			})
		}

		if len(res.Statements.TopByTotalTime) > 0 {
			q := res.Statements.TopByTotalTime[0]
			desc := fmt.Sprintf("Calls: %s, Total: %s", formatThousands0(q.Calls), humanizeMs(q.TotalTime))
			if !res.Statements.StatsResetTime.IsZero() {
				statsAgeHours := time.Since(res.Statements.StatsResetTime).Hours()
				if statsAgeHours > 0 {
					callsPerHour := q.Calls / statsAgeHours
					desc += fmt.Sprintf(", Calls/hr: %.1f", callsPerHour)
				}
			}
			a.Infos = append(a.Infos, Finding{
				Title:       "Top query by total time",
				Severity:    "info",
				Description: desc,
				Action:      "Review execution plan and caching. Consider increasing work_mem for heavy sorts/aggregations.",
			})
		}

		// Derive optimization recommendations from collected EXPLAIN plan advice
		seqScanTables := map[string]struct{}{}
		canBeIndexedCount := 0
		canBeRefactoredCount := 0
		hasSort := false
		hasJoin := false
		for _, st := range res.Statements.TopByTotalTime {
			if st.Advice == nil {
				continue
			}
			if st.Advice.CanBeIndexed {
				canBeIndexedCount++
			}
			if st.Advice.CanBeRefactored {
				canBeRefactoredCount++
			}
			for _, h := range st.Advice.Highlights {
				uh := strings.ToUpper(h)
				if strings.HasPrefix(uh, "SEQ SCAN ON ") {
					// extract table name portion after prefix using original case
					name := h[len("Seq Scan on "):]
					name = strings.TrimSpace(name)
					if name != "" {
						seqScanTables[name] = struct{}{}
					}
				}
				if strings.Contains(uh, "SORT") {
					hasSort = true
				}
				if strings.Contains(uh, "JOIN") {
					hasJoin = true
				}
			}
		}
		if len(seqScanTables) > 0 {
			// build table list
			names := make([]string, 0, len(seqScanTables))
			for n := range seqScanTables {
				names = append(names, n)
			}
			sort.Strings(names)
			// cap the list for readability
			max := 8
			if len(names) > max {
				names = names[:max]
			}
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Slow queries use sequential scans",
				Severity:    "rec",
				Code:        "slow-seq-scans",
				Description: fmt.Sprintf("Sequential scans detected on: %s", strings.Join(names, ", ")),
				Action:      "Create or refine indexes on selective WHERE and JOIN columns; analyze tables; ensure statistics are up to date.",
			})
		}
		if canBeIndexedCount > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Index improvements possible for slow queries",
				Severity:    "rec",
				Code:        "slow-index-improve",
				Description: fmt.Sprintf("%d slow queries could be improved with new or better indexes.", canBeIndexedCount),
				Action:      "Run EXPLAIN on slow queries to identify missing indexes on columns used in WHERE clauses, JOINs, or ORDER BY.",
			})
		}
		if canBeRefactoredCount > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Query refactoring needed for slow queries",
				Severity:    "rec",
				Code:        "slow-refactor",
				Description: fmt.Sprintf("%d slow queries may need refactoring as indexes alone may not solve the performance issue.", canBeRefactoredCount),
				Action:      "Analyze the execution plan of slow queries to understand the cause. Consider rewriting the query, breaking it into smaller parts, or using different join strategies.",
			})
		}
		if hasSort {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Sorting in slow queries may lack index support",
				Severity:    "rec",
				Code:        "slow-sorts",
				Description: "Plans include Sort nodes for top slow queries.",
				Action:      "Add or adjust indexes matching ORDER BY leading columns to enable sorted index scans where appropriate.",
			})
		}
		if hasJoin {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Joins in slow queries may be missing indexes",
				Severity:    "rec",
				Code:        "slow-joins",
				Description: "Join operations detected; missing or suboptimal indexes can cause hash/merge joins to spill or nested loops to scan many rows.",
				Action:      "Ensure join key columns are indexed on both sides; consider composite indexes matching join + filter predicates.",
			})
		}
	} else {
		if res.Extensions.PgStatStatements {
			a.Infos = append(a.Infos, Finding{
				Title:       "pg_stat_statements installed",
				Severity:    "info",
				Description: "Extension is present but returned no rows for top queries (possibly recently reset or limited visibility).",
				Action:      "Run workload, ensure pg_stat_statements is preloaded and tracking settings are appropriate; verify role has access.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{
				Title:       "Query-level analysis limited",
				Severity:    "info",
				Description: "pg_stat_statements not available; only coarse-grained insights reported.",
				Action:      "Install and configure pg_stat_statements for detailed top queries.",
			})
		}
	}

	// Analyze tables with index counts
	if len(res.TablesWithIndexCount) > 0 {
		tablesWithoutIndexes := 0
		tablesWithManyIndexes := 0
		for _, t := range res.TablesWithIndexCount {
			if t.IndexCount == 0 && t.RowCount > 1000 {
				tablesWithoutIndexes++
			}
			if t.IndexCount > 10 {
				tablesWithManyIndexes++
			}
		}
		if tablesWithoutIndexes > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Tables without indexes",
				Severity:    "warn",
				Description: fmt.Sprintf("%d large tables have no indexes", tablesWithoutIndexes),
				Action:      "Review tables with >1000 rows and no indexes; consider adding primary keys and selective indexes.",
			})
		}
		if tablesWithManyIndexes > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Tables with many indexes",
				Severity:    "rec",
				Code:        "too-many-indexes",
				Description: fmt.Sprintf("%d tables have >10 indexes", tablesWithManyIndexes),
				Action:      "Review index usage; consider dropping unused indexes to reduce write overhead and storage.",
			})
		}
	}

	// Advanced bloat analysis
	if len(res.TableBloatStats) > 0 {
		severeBloat := 0
		totalWasted := int64(0)
		for _, b := range res.TableBloatStats {
			if b.EstimatedBloat > 50 {
				severeBloat++
			}
			totalWasted += b.WastedBytes
		}
		if severeBloat > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Severe table bloat detected",
				Severity:    "warn",
				Description: fmt.Sprintf("%d tables with >50%% bloat, wasting %.2f GB", severeBloat, bytesToGB(totalWasted)),
				Action:      "Run VACUUM FULL or use pg_repack on severely bloated tables; review autovacuum settings.",
			})
		}
	}

	// Index bloat analysis (keep other bloat insights here in future; large unused covered above)

	// Replication health
	if len(res.ReplicationStats) > 0 {
		lagIssues := 0
		for _, r := range res.ReplicationStats {
			if r.SyncState != "sync" && r.SyncState != "quorum" {
				lagIssues++
			}
		}
		if lagIssues > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Replication lag detected",
				Severity:    "warn",
				Description: fmt.Sprintf("%d replicas not in sync state", lagIssues),
				Action:      "Check network connectivity, replica performance, and wal_sender/wal_receiver processes.",
			})
		}
	} else if res.ConnInfo.IsSuperuser {
		a.Infos = append(a.Infos, Finding{
			Title:       "No replication configured",
			Severity:    "info",
			Description: "No replication slots or replicas detected",
			Action:      "Consider setting up streaming replication for high availability and read scaling.",
		})
	}

	// Checkpoint analysis
	if res.CheckpointStats.RequestedCheckpoints > 0 {
		reqRatio := float64(res.CheckpointStats.RequestedCheckpoints) /
			float64(res.CheckpointStats.RequestedCheckpoints+res.CheckpointStats.ScheduledCheckpoints) * 100
		if reqRatio > 10 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Frequent requested checkpoints",
				Severity:    "warn",
				Description: fmt.Sprintf("%.1f%% of checkpoints are requested (not scheduled)", reqRatio),
				Action:      "Increase max_wal_size and checkpoint_timeout; reduce checkpoint_completion_target if needed.",
			})
		}
	}

	// IO performance analysis
	if res.IOStats.HeapBlksRead+res.IOStats.HeapBlksHit > 0 {
		heapHitRatio := float64(res.IOStats.HeapBlksHit) /
			float64(res.IOStats.HeapBlksRead+res.IOStats.HeapBlksHit) * 100
		if heapHitRatio < 95 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Low heap cache hit ratio",
				Severity:    "warn",
				Description: fmt.Sprintf("Heap cache hit ratio: %.1f%%", heapHitRatio),
				Action:      "Increase shared_buffers; ensure working set fits in memory; check for memory pressure.",
			})
		}
	}

	// Wait events overview & heuristics (pg_monitor)
	if len(res.WaitEvents) > 0 {
		total := 0
		byType := map[string]int{}
		byEvent := map[string]int{}
		for _, w := range res.WaitEvents {
			total += w.Count
			t := strings.ToUpper(strings.TrimSpace(w.Type))
			e := strings.ToUpper(strings.TrimSpace(w.Event))
			byType[t] += w.Count
			byEvent[e] += w.Count
		}
		// Helper: top-N from a map
		type kv struct {
			k string
			v int
		}
		topN := func(m map[string]int, n int) []kv {
			arr := make([]kv, 0, len(m))
			for k, v := range m {
				arr = append(arr, kv{k, v})
			}
			sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
			if len(arr) > n {
				arr = arr[:n]
			}
			return arr
		}
		topsType := topN(byType, 3)
		topsEvent := topN(byEvent, 3)
		// Compose summary info
		if total > 0 {
			parts := []string{}
			for _, t := range topsType {
				pct := float64(t.v) / float64(total) * 100
				parts = append(parts, fmt.Sprintf("%s %.0f%%", titleCase(t.k), pct))
			}
			evs := []string{}
			for _, e := range topsEvent {
				evs = append(evs, titleCase(e.k))
			}
			a.Infos = append(a.Infos, Finding{
				Title:       "Top wait types",
				Severity:    "info",
				Description: fmt.Sprintf("%s; top events: %s", strings.Join(parts, ", "), strings.Join(evs, ", ")),
				Action:      "Use this to guide whether to focus on IO, locks, or application behavior.",
			})
		}
		// Targeted recommendations based on dominant waits
		get := func(key string) int { return byType[strings.ToUpper(key)] }
		dom := func(key string) bool { return total > 0 && float64(get(key))/float64(total) >= 0.6 }
		// IO waits
		if get("IO") > 0 {
			sev := "rec"
			if dom("IO") {
				sev = "warn"
			}
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "IO-related waits",
				Severity:    sev,
				Code:        "io-waits",
				Description: "pg_stat_activity shows IO waits (reads/writes/sync).",
				Action:      "Improve cache hit (shared_buffers, indexing), tune effective_io_concurrency and checkpoint settings, and consider faster storage.",
			})
		}
		// Lock and LWLock waits
		lw := get("LWLOCK")
		lk := get("LOCK")
		if lw+lk > 0 {
			sev := "rec"
			if dom("LOCK") || dom("LWLOCK") {
				sev = "warn"
			}
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Lock contention waits",
				Severity:    sev,
				Code:        "lock-waits",
				Description: "Waits due to locks/LWLocks detected; possible blockers or high contention.",
				Action:      "Identify blockers (Blocking section), shorten transactions, add indexes to reduce lock duration, and consider lock timeouts.",
			})
		}
		// BufferPin waits (often long-running transactions pin buffers)
		if get("BUFFERPIN") > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Buffer pin waits",
				Severity:    "rec",
				Code:        "bufferpin-waits",
				Description: "BufferPin waits suggest pinned buffers—often due to long-running queries/transactions.",
				Action:      "Avoid long transactions and idle-in-transaction sessions; commit sooner and set idle_in_transaction_session_timeout.",
			})
		}
		// Client waits: usually benign, but high proportions can indicate app idling
		if dom("CLIENT") {
			a.Infos = append(a.Infos, Finding{
				Title:       "Client-side waits dominate",
				Severity:    "info",
				Description: "Many sessions are waiting on client reads/writes (often benign).",
				Action:      "Validate application behavior and connection pooling settings.",
			})
		}
		// Activity/Extension/IPС/Timeout etc. could be surfaced later if needed
	}

	// WAL volume context & FPI ratio (pg_monitor)
	if res.WAL != nil && res.WAL.Bytes > 0 && !res.WAL.StatsReset.IsZero() {
		dur := time.Since(res.WAL.StatsReset)
		if dur > 0 {
			bytesPerSec := float64(res.WAL.Bytes) / dur.Seconds()
			// High sustained WAL write rate
			if bytesPerSec > 10*1024*1024 { // >10MB/s
				a.Warnings = append(a.Warnings, Finding{Title: "High WAL write rate", Severity: "warn", Code: "high-wal",
					Description: fmt.Sprintf("~%.1f MB/s since %s", bytesPerSec/(1024*1024), formatLocalTime(res.WAL.StatsReset)),
					Action:      "Tune checkpoint_timeout and max_wal_size; avoid unnecessary bulk updates and bloated indexes; ensure autovacuum keeps up.",
				})
			} else {
				a.Infos = append(a.Infos, Finding{Title: "WAL rate", Severity: "info",
					Description: fmt.Sprintf("~%.1f MB/s since %s", bytesPerSec/(1024*1024), formatLocalTime(res.WAL.StatsReset))})
			}
		}
		if res.WAL.Records > 0 {
			fpiRatio := float64(res.WAL.FullPage) / float64(res.WAL.Records)
			if fpiRatio > 0.5 {
				a.Warnings = append(a.Warnings, Finding{
					Title:       "Very high full-page image rate",
					Severity:    "warn",
					Code:        "wal-fpi-high",
					Description: fmt.Sprintf("FPI/records ratio ~%.0f%%", fpiRatio*100),
					Action:      "Likely frequent checkpoints or many first-touches of pages. Increase checkpoint_timeout/max_wal_size and avoid unnecessary table rewrites.",
				})
			} else if fpiRatio > 0.2 {
				a.Recommendations = append(a.Recommendations, Finding{
					Title:       "High full-page image rate",
					Severity:    "rec",
					Code:        "wal-fpi",
					Description: fmt.Sprintf("FPI/records ratio ~%.0f%%", fpiRatio*100),
					Action:      "Consider fewer checkpoints (tune checkpoint_timeout, max_wal_size) and reduce bulk page modifications where possible.",
				})
			}
		}
	}

	// Functions hotspot analysis (pg_monitor)
	if len(res.FunctionStats) > 0 {
		// Top function emphasis
		f := res.FunctionStats[0]
		avgSelf := 0.0
		if f.Calls > 0 {
			avgSelf = f.SelfTime / float64(f.Calls)
		}
		// Heuristics: flag high total and notable self-time per call
		if f.TotalTime > 15000 && f.Calls > 100 { // >15s total across >100 calls
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Hot function by total time",
				Severity:    "rec",
				Code:        "hot-function",
				Description: fmt.Sprintf("%s.%s — calls: %s, total: %.1f ms, self: %.1f ms (avg self %.2f ms)", f.Schema, f.Name, formatThousands0(float64(f.Calls)), f.TotalTime, f.SelfTime, avgSelf),
				Action:      "Profile function logic; reduce loops and per-row work; consider set-based SQL or indexing; enable track_functions='pl'/'all' if more granularity is needed.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Top function", Severity: "info",
				Description: fmt.Sprintf("%s.%s — total: %.1f ms, calls: %s", f.Schema, f.Name, f.TotalTime, formatThousands0(float64(f.Calls)))})
		}
		// Multiple heavy functions (avg self time threshold)
		heavy := 0
		for _, fn := range res.FunctionStats {
			if fn.Calls >= 100 && (fn.SelfTime/float64(fn.Calls)) > 5.0 { // >5ms self per call
				heavy++
			}
		}
		if heavy >= 3 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Several functions show high per-call CPU time",
				Severity:    "rec",
				Code:        "hot-functions-multi",
				Description: fmt.Sprintf("%d functions exceed ~5ms self time per call (>=100 calls)", heavy),
				Action:      "Look for row-by-row PL/pgSQL patterns; push work into SQL set operations; add indexes to speed lookups inside functions.",
			})
		}
	}

	// Progress views (pg_monitor): detect waits during index builds/analyze
	if len(res.ProgressCreateIndex) > 0 {
		waiting := 0
		for _, pr := range res.ProgressCreateIndex {
			if strings.Contains(strings.ToLower(pr.Phase), "wait") || (pr.LockersTotal > 0 && pr.LockersDone < pr.LockersTotal) {
				waiting++
			}
		}
		if waiting > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Index builds waiting for lockers",
				Severity:    "warn",
				Code:        "ci-wait-lockers",
				Description: fmt.Sprintf("%d CREATE INDEX operations are waiting on locks", waiting),
				Action:      "Prefer CREATE INDEX CONCURRENTLY for live systems; schedule builds off-peak; reduce long transactions holding locks.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Index builds in progress", Severity: "info",
				Description: fmt.Sprintf("%d CREATE INDEX operations running", len(res.ProgressCreateIndex)),
			})
		}
	}
	if len(res.ProgressAnalyze) > 0 {
		a.Infos = append(a.Infos, Finding{Title: "ANALYZE in progress", Severity: "info",
			Description: fmt.Sprintf("%d relations being analyzed", len(res.ProgressAnalyze)),
			Action:      "Allow ANALYZE to complete for up-to-date planner statistics.",
		})
	}

	// Lock contention analysis
	if len(res.LockStats) > 0 {
		totalWaiting := 0
		for _, l := range res.LockStats {
			if !l.Granted {
				totalWaiting += l.Count
			}
		}
		if totalWaiting > 10 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "High lock contention",
				Severity:    "warn",
				Description: fmt.Sprintf("%d locks are waiting to be granted", totalWaiting),
				Action:      "Review long-running transactions; consider shorter transaction durations and lock timeouts.",
			})
		}
	}

	// Temporary file analysis
	if len(res.TempFileStats) > 0 {
		totalTempBytes := int64(0)
		for _, t := range res.TempFileStats {
			totalTempBytes += t.Bytes
		}
		if totalTempBytes > 1024*1024*1024 { // >1GB
			a.Warnings = append(a.Warnings, Finding{
				Title:       "High temporary file usage",
				Severity:    "warn",
				Description: fmt.Sprintf("Sessions using %.2f GB in temporary files", bytesToGB(totalTempBytes)),
				Action:      "Increase work_mem; review queries with large sorts/hashes; consider temp_file_limit.",
			})
		}
	}

	// Extension analysis
	if len(res.ExtensionStats) > 0 {
		usefulExtensions := []string{"pg_stat_statements"}
		missing := []string{}
		for _, ext := range usefulExtensions {
			found := false
			for _, e := range res.ExtensionStats {
				if e.Name == ext {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, ext)
			}
		}
		if len(missing) > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Useful extensions not installed",
				Severity:    "rec",
				Code:        "missing-extensions",
				Description: fmt.Sprintf("Consider installing: %s", strings.Join(missing, ", ")),
				Action:      "CREATE EXTENSION IF NOT EXISTS extension_name; (requires superuser or appropriate privileges)",
			})
		}
	}

	// Memory configuration analysis
	if s, ok := setting("shared_buffers"); ok {
		if s.Val == "128MB" || s.Val == "16384" { // Default values
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "shared_buffers may be too low",
				Severity:    "rec",
				Code:        "shared-buffers-low",
				Description: "shared_buffers is at default value",
				Action:      "Set shared_buffers to 25-40% of available RAM for dedicated PostgreSQL servers.",
			})
		}
	}

	// work_mem guardrails already covered above; add low suggestion if very small
	if wmS, ok := setting("work_mem"); ok {
		if wm, _ := asBytes(wmS, true); wm > 0 && wm < 4*1024*1024 { // <4MB
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "work_mem may be too low",
				Severity:    "rec",
				Code:        "work-mem-low",
				Description: fmt.Sprintf("work_mem=%s can cause frequent temp spills for sorts/hashes", wmS.Val),
				Action:      "Consider 16-64MB depending on workload; prefer per-query SET work_mem for heavy reports.",
			})
		}
	}

	// max_wal_size sanity (too small)
	if s, ok := setting("max_wal_size"); ok {
		if mb, ok2 := asBytes(s, true); ok2 && mb > 0 && mb < 2*1024*1024*1024 { // <2GB
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "max_wal_size may be too low",
				Severity:    "rec",
				Code:        "max-wal-size-low",
				Description: "Small max_wal_size can cause frequent checkpoints and high FPI rate.",
				Action:      "Consider 4-16GB depending on write workload to reduce checkpoint frequency.",
			})
		}
	}

	// wal_buffers heuristic: very small explicit value (<8MB) may be suboptimal; 0 means auto
	if s, ok := setting("wal_buffers"); ok {
		if s.Val != "-1" && s.Val != "0" { // -1/0 = auto-tuned
			if b, ok2 := asBytes(s, true); ok2 && b > 0 && b < 8*1024*1024 {
				a.Recommendations = append(a.Recommendations, Finding{
					Title:       "wal_buffers is very low",
					Severity:    "rec",
					Code:        "wal-buffers-low",
					Description: fmt.Sprintf("wal_buffers=%s; small buffers can throttle WAL writes under bursty load", s.Val),
					Action:      "Either leave wal_buffers at default (auto) or set to at least 16MB for busy systems.",
				})
			}
		}
	}

	// max_parallel_workers heuristic
	if s, ok := setting("max_parallel_workers"); ok {
		val, _ := strconv.Atoi(s.Val)
		if val > 0 && val < 2 { // effectively disabled
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Parallel workers effectively disabled",
				Severity:    "rec",
				Code:        "parallel-workers-low",
				Description: fmt.Sprintf("max_parallel_workers=%d can limit parallel query speedups", val),
				Action:      "Set max_parallel_workers (and per-gather variants) to 4-8+ depending on CPU cores and workload.",
			})
		}
	}

	// WAL configuration analysis
	if s, ok := setting("wal_level"); ok && s.Val == "replica" {
		a.Infos = append(a.Infos, Finding{
			Title:       "WAL level supports replication",
			Severity:    "info",
			Description: "wal_level=replica enables streaming replication",
			Action:      "Consider 'logical' if you need logical replication for specific use cases.",
		})
	}

	// Connection pooling recommendation
	if res.ConnInfo.MaxConnections > 100 {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "High max_connections setting",
			Severity:    "rec",
			Description: fmt.Sprintf("max_connections=%d may be high", res.ConnInfo.MaxConnections),
			Action:      "Consider using a connection pooler (pgbouncer) and reducing max_connections to 50-100.",
			Code:        "high-max-connections",
		})
	}

	// Autovacuum configuration analysis
	if s, ok := setting("autovacuum_naptime"); ok {
		if secs := asSeconds(s, true); secs > 300 { // >5 minutes
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "autovacuum_naptime may be too high",
				Severity:    "rec",
				Description: fmt.Sprintf("autovacuum_naptime=%.0fs", secs),
				Action:      "Consider reducing to 20-60 seconds for more aggressive autovacuum scheduling.",
				Code:        "autovacuum-naptime-high",
			})
		}
	}

	// Maintenance work memory analysis
	if s, ok := setting("maintenance_work_mem"); ok {
		if val, _ := asBytes(s, true); val < 64*1024*1024 { // <64MB
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "maintenance_work_mem may be too low",
				Severity:    "rec",
				Description: "maintenance_work_mem is low for VACUUM/REINDEX operations",
				Action:      "Increase maintenance_work_mem to 256MB-1GB for better maintenance performance.",
				Code:        "maintenance-work-mem-low",
			})
		}
	}

	// Random page cost analysis
	if s, ok := setting("random_page_cost"); ok {
		if s.Val == "4" { // Default
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "random_page_cost at default",
				Severity:    "rec",
				Code:        "random-page-cost-default",
				Description: "random_page_cost=4.0 may not reflect modern storage",
				Action:      "For SSD storage, consider reducing to 1.1-2.0; for HDD, 4.0 is usually appropriate.",
			})
		}
	}

	// Work memory analysis
	if s, ok := setting("work_mem"); ok {
		if val, _ := asBytes(s, true); val > 50*1024*1024 { // >50MB
			a.Warnings = append(a.Warnings, Finding{
				Title:       "work_mem may be too high",
				Severity:    "warn",
				Description: fmt.Sprintf("work_mem=%s", s.Val),
				Action:      "High work_mem can cause memory pressure; consider per-query work_mem or lower global setting.",
			})
		}
	}

	// SSL configuration
	if res.ConnInfo.SSL == "off" || res.ConnInfo.SSL == "" {
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "SSL not enabled",
			Severity:    "rec",
			Code:        "ssl-off",
			Description: "SSL encryption is not enabled for connections",
			Action:      "Enable SSL for encrypted client connections; configure ssl=on and provide certificates.",
		})
	}

	// Statement timeout analysis
	if s, ok := setting("statement_timeout"); ok {
		if s.Val == "0" { // No timeout
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "No statement timeout configured",
				Severity:    "rec",
				Code:        "no-statement-timeout",
				Description: "statement_timeout is disabled",
				Action:      "Set statement_timeout to prevent runaway queries; consider 30s-5m depending on workload.",
			})
		}
	}

	// Idle transaction timeout
	if s, ok := setting("idle_in_transaction_session_timeout"); ok {
		if s.Val == "0" { // No timeout
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "No idle transaction timeout",
				Severity:    "rec",
				Code:        "no-idle-tx-timeout",
				Description: "idle_in_transaction_session_timeout is disabled",
				Action:      "Set idle_in_transaction_session_timeout to 10-60 minutes to prevent abandoned transactions.",
			})
		}
	}

	// ============================================================
	// Additional Health Checks Analysis
	// ============================================================

	// 1. XID Wraparound Risk Analysis - CRITICAL safety check
	if len(res.XIDAge) > 0 {
		criticalDBs := []string{}
		warningDBs := []string{}
		for _, x := range res.XIDAge {
			if x.PctToLimit >= xidCriticalPct {
				criticalDBs = append(criticalDBs, fmt.Sprintf("%s (%.1f%%)", x.Datname, x.PctToLimit))
			} else if x.PctToLimit >= xidWarningPct {
				warningDBs = append(warningDBs, fmt.Sprintf("%s (%.1f%%)", x.Datname, x.PctToLimit))
			}
		}
		if len(criticalDBs) > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "CRITICAL: XID wraparound imminent",
				Severity:    SeverityWarning,
				Code:        "xid-wraparound-critical",
				Description: fmt.Sprintf("Databases approaching XID wraparound: %s. PostgreSQL will SHUT DOWN to prevent data corruption if this reaches 100%%.", strings.Join(criticalDBs, ", ")),
				Action:      "IMMEDIATELY run VACUUM FREEZE on affected databases. Consider emergency maintenance window. Check for long-running transactions blocking vacuum.",
			})
		}
		if len(warningDBs) > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "XID age warning",
				Severity:    SeverityWarning,
				Code:        "xid-age-warning",
				Description: fmt.Sprintf("Databases with elevated XID age: %s", strings.Join(warningDBs, ", ")),
				Action:      "Schedule VACUUM FREEZE operations. Review autovacuum_freeze_max_age settings. Ensure autovacuum is not blocked.",
			})
		}
		// Info for healthy databases
		if len(criticalDBs) == 0 && len(warningDBs) == 0 && len(res.XIDAge) > 0 {
			oldest := res.XIDAge[0] // Already sorted by age DESC
			a.Infos = append(a.Infos, Finding{
				Title:       "XID age healthy",
				Severity:    SeverityInfo,
				Description: fmt.Sprintf("Oldest XID age: %s at %.1f%% of limit", oldest.Datname, oldest.PctToLimit),
			})
		}
	}

	// 2. Idle-in-Transaction Analysis
	if len(res.IdleInTransaction) > 0 {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Idle-in-transaction sessions detected",
			Severity:    SeverityWarning,
			Code:        "idle-in-transaction",
			Description: fmt.Sprintf("%d sessions have been idle-in-transaction for >5 minutes. These block vacuum, hold locks, and consume connection slots.", len(res.IdleInTransaction)),
			Action:      "Investigate application connection handling. Set idle_in_transaction_session_timeout. Consider terminating with pg_terminate_backend() if safe.",
		})
	}

	// 3. Stale Statistics Analysis
	if len(res.StaleStatsTables) > 0 {
		count := len(res.StaleStatsTables)
		tables := make([]string, 0, 5)
		for i, t := range res.StaleStatsTables {
			if i >= 5 {
				break
			}
			tables = append(tables, fmt.Sprintf("%s.%s", t.Schema, t.Table))
		}
		desc := fmt.Sprintf("%d tables have outdated statistics (not analyzed in %d+ days): %s", count, staleStatsDays, strings.Join(tables, ", "))
		if count > 5 {
			desc += fmt.Sprintf(" and %d more", count-5)
		}
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Stale table statistics",
			Severity:    SeverityRec,
			Code:        "stale-statistics",
			Description: desc,
			Action:      "Run ANALYZE on affected tables. Review autovacuum_analyze_threshold and autovacuum_analyze_scale_factor settings.",
		})
	}

	// 4. Duplicate Indexes Analysis
	if len(res.DuplicateIndexes) > 0 {
		totalWasted := int64(0)
		pairs := make([]string, 0, 5)
		for i, di := range res.DuplicateIndexes {
			// The smaller/less-used index is typically the one to drop
			wastedSize := di.Index1Size
			if di.Index2Size < di.Index1Size {
				wastedSize = di.Index2Size
			}
			totalWasted += wastedSize
			if i < 5 {
				pairs = append(pairs, fmt.Sprintf("%s.%s ↔ %s", di.Schema, di.Index1, di.Index2))
			}
		}
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Duplicate indexes detected",
			Severity:    SeverityRec,
			Code:        "duplicate-indexes",
			Description: fmt.Sprintf("%d index pairs have identical column definitions, wasting ~%.2f GB: %s", len(res.DuplicateIndexes), bytesToGB(totalWasted), strings.Join(pairs, "; ")),
			Action:      "Compare scan counts and drop the less-used duplicate. Verify no unique constraints depend on them first.",
		})
	}

	// 5. Invalid Indexes Analysis
	if len(res.InvalidIndexes) > 0 {
		names := make([]string, 0, len(res.InvalidIndexes))
		totalSize := int64(0)
		for _, ii := range res.InvalidIndexes {
			names = append(names, fmt.Sprintf("%s.%s (%s)", ii.Schema, ii.Name, ii.Reason))
			totalSize += ii.SizeBytes
		}
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Invalid indexes found",
			Severity:    SeverityWarning,
			Code:        "invalid-indexes",
			Description: fmt.Sprintf("%d invalid indexes wasting %.2f GB and not providing any benefit: %s", len(res.InvalidIndexes), bytesToGB(totalSize), strings.Join(names, ", ")),
			Action:      "Drop invalid indexes with DROP INDEX and recreate with CREATE INDEX CONCURRENTLY. Investigate why they failed (disk space, locks, errors).",
		})
	}

	// 6. Foreign Key Missing Indexes Analysis
	if len(res.FKMissingIndexes) > 0 {
		// Prioritize by table size (rows)
		count := len(res.FKMissingIndexes)
		fks := make([]string, 0, 5)
		for i, fk := range res.FKMissingIndexes {
			if i >= 5 {
				break
			}
			fks = append(fks, fmt.Sprintf("%s.%s(%s)", fk.Schema, fk.Table, fk.Columns))
		}
		desc := fmt.Sprintf("%d foreign keys lack supporting indexes, causing slow JOINs and cascading deletes: %s", count, strings.Join(fks, ", "))
		if count > 5 {
			desc += fmt.Sprintf(" and %d more", count-5)
		}
		a.Recommendations = append(a.Recommendations, Finding{
			Title:       "Foreign keys without indexes",
			Severity:    SeverityRec,
			Code:        "fk-missing-index",
			Description: desc,
			Action:      "Create indexes on FK columns. Example: CREATE INDEX CONCURRENTLY ON table(fk_column). Review 'FK Missing Indexes' table for suggested DDL.",
		})
	}

	// 7. Sequence Exhaustion Analysis
	if len(res.SequenceHealth) > 0 {
		criticalSeqs := []string{}
		warningSeqs := []string{}
		for _, sq := range res.SequenceHealth {
			if sq.PctUsed >= sequenceCriticalPct {
				criticalSeqs = append(criticalSeqs, fmt.Sprintf("%s.%s (%.1f%%)", sq.Schema, sq.Name, sq.PctUsed))
			} else if sq.PctUsed >= sequenceWarningPct {
				warningSeqs = append(warningSeqs, fmt.Sprintf("%s.%s (%.1f%%)", sq.Schema, sq.Name, sq.PctUsed))
			}
		}
		if len(criticalSeqs) > 0 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Sequences near exhaustion",
				Severity:    SeverityWarning,
				Code:        "sequence-exhaustion-critical",
				Description: fmt.Sprintf("Sequences >%d%% exhausted will cause INSERT failures: %s", int(sequenceCriticalPct), strings.Join(criticalSeqs, ", ")),
				Action:      "Alter sequences to use bigint (ALTER SEQUENCE ... AS bigint) or reset with appropriate min/max values. Plan migration before exhaustion.",
			})
		}
		if len(warningSeqs) > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Sequences approaching exhaustion",
				Severity:    SeverityRec,
				Code:        "sequence-exhaustion-warning",
				Description: fmt.Sprintf("Sequences >%d%% used: %s", int(sequenceWarningPct), strings.Join(warningSeqs, ", ")),
				Action:      "Monitor sequence usage. Plan to convert to bigint before reaching limit.",
			})
		}
	}

	// 8. Prepared Transactions (2PC) Analysis
	if len(res.PreparedXacts) > 0 {
		a.Warnings = append(a.Warnings, Finding{
			Title:       "Prepared transactions detected",
			Severity:    SeverityWarning,
			Code:        "prepared-transactions",
			Description: fmt.Sprintf("%d prepared (2PC) transactions found. These block vacuum, prevent XID advancement, and hold locks indefinitely until committed or rolled back.", len(res.PreparedXacts)),
			Action:      "Investigate orphaned transactions with pg_prepared_xacts. Commit with COMMIT PREPARED 'gid' or rollback with ROLLBACK PREPARED 'gid'. Consider disabling max_prepared_transactions if not using 2PC.",
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

// humanizeDuration renders a duration like "4d 1h 25m" or "1h 25m 42s"
func humanizeDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	// For very short durations, prefer milliseconds
	if d < time.Second {
		if d <= 0 {
			return "0ms"
		}
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return d.String()
	}
	total := int64(d.Seconds())
	days := total / 86400
	total %= 86400
	hours := total / 3600
	total %= 3600
	mins := total / 60
	secs := total % 60
	parts := make([]string, 0, 4)
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if mins > 0 {
		parts = append(parts, fmt.Sprintf("%dm", mins))
	}
	if secs > 0 {
		if len(parts) < 3 {
			parts = append(parts, fmt.Sprintf("%ds", secs))
		}
	}
	if len(parts) == 0 {
		return "0s"
	}
	return strings.Join(parts, " ")
}

func formatLocalTime(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.Local().Format("2006-01-02 15:04:05 MST")
}

func titleCase(s string) string {
	return cases.Title(language.English).String(strings.ToLower(s))
}

func formatThousands0(f float64) string {
	s := strconv.FormatFloat(f, 'f', 0, 64)
	// add thousands separators
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	n := len(s)
	if n <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	out := make([]byte, 0, n+n/3)
	cnt := 0
	for i := n - 1; i >= 0; i-- {
		out = append(out, s[i])
		cnt++
		if cnt%3 == 0 && i != 0 {
			out = append(out, ',')
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// humanizeMs converts milliseconds to a compact human duration string like "6h 27m" or "42s"
func humanizeMs(ms float64) string {
	if ms <= 0 {
		return "0s"
	}
	d := time.Duration(ms * float64(time.Millisecond))
	return humanizeDuration(d)
}
