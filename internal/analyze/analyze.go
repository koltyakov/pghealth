package analyze

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
	// Uptime info
	if !res.ConnInfo.StartTime.IsZero() {
		up := time.Since(res.ConnInfo.StartTime)
		a.Infos = append(a.Infos, Finding{
			Title:       "Server uptime",
			Severity:    "info",
			Description: fmt.Sprintf("%s (since %s)", up.Truncate(time.Second), res.ConnInfo.StartTime.Format(time.RFC3339)),
			Action:      "",
		})
	}

	// Cache hit ratios
	if res.CacheHitCurrent > 0 {
		if res.CacheHitCurrent < 95 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "Low cache hit ratio (current DB)",
				Severity:    "warn",
				Description: fmt.Sprintf("Cache hit: %.1f%%", res.CacheHitCurrent),
				Action:      "Review working set size, shared_buffers, and query patterns; ensure sufficient memory and indexes.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Cache hit ratio (current)", Severity: "info", Description: fmt.Sprintf("%.1f%%", res.CacheHitCurrent)})
		}
	}
	if res.CacheHitOverall > 0 {
		if res.CacheHitOverall < 95 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Overall cache hit could improve",
				Severity:    "rec",
				Description: fmt.Sprintf("Cluster-wide cache hit: %.1f%%", res.CacheHitOverall),
				Action:      "Consider memory tuning and index coverage across busiest databases.",
			})
		}
	}

	// Connection usage
	if res.ConnInfo.MaxConnections > 0 && res.TotalConnections > 0 {
		pct := float64(res.TotalConnections) / float64(res.ConnInfo.MaxConnections) * 100
		if pct >= 80 {
			a.Warnings = append(a.Warnings, Finding{
				Title:       "High connection usage",
				Severity:    "warn",
				Description: fmt.Sprintf("%d/%d (%.0f%%) connections in use", res.TotalConnections, res.ConnInfo.MaxConnections, pct),
				Action:      "Use a pooler (pgbouncer), limit app connection pools, and tune max_connections accordingly.",
			})
		} else {
			a.Infos = append(a.Infos, Finding{Title: "Connection usage", Severity: "info", Description: fmt.Sprintf("%d/%d (%.0f%%)", res.TotalConnections, res.ConnInfo.MaxConnections, pct)})
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

	// Statements / pg_stat_statements context
	if res.Statements.Available {
		if !res.Statements.StatsResetTime.IsZero() {
			statsAge := time.Since(res.Statements.StatsResetTime)
			a.Infos = append(a.Infos, Finding{
				Title:       "Query stats window",
				Severity:    "info",
				Description: fmt.Sprintf("pg_stat_statements data covers the last %s (since %s)", statsAge.Truncate(time.Second), res.Statements.StatsResetTime.Format(time.RFC3339)),
				Action:      "Run `SELECT pg_stat_statements_reset()` to clear stats if needed.",
			})
		}

		if len(res.Statements.TopByTotalTime) > 0 {
			q := res.Statements.TopByTotalTime[0]
			desc := fmt.Sprintf("Calls: %.0f, TotalTime: %.2f ms", q.Calls, q.TotalTime)
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
				Description: fmt.Sprintf("Sequential scans detected on: %s", strings.Join(names, ", ")),
				Action:      "Create or refine indexes on selective WHERE and JOIN columns; analyze tables; ensure statistics are up to date.",
			})
		}
		if canBeIndexedCount > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Index improvements possible for slow queries",
				Severity:    "rec",
				Description: fmt.Sprintf("%d slow queries could be improved with new or better indexes.", canBeIndexedCount),
				Action:      "Run EXPLAIN on slow queries to identify missing indexes on columns used in WHERE clauses, JOINs, or ORDER BY.",
			})
		}
		if canBeRefactoredCount > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Query refactoring needed for slow queries",
				Severity:    "rec",
				Description: fmt.Sprintf("%d slow queries may need refactoring as indexes alone may not solve the performance issue.", canBeRefactoredCount),
				Action:      "Analyze the execution plan of slow queries to understand the cause. Consider rewriting the query, breaking it into smaller parts, or using different join strategies.",
			})
		}
		if hasSort {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Sorting in slow queries may lack index support",
				Severity:    "rec",
				Description: "Plans include Sort nodes for top slow queries.",
				Action:      "Add or adjust indexes matching ORDER BY leading columns to enable sorted index scans where appropriate.",
			})
		}
		if hasJoin {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Joins in slow queries may be missing indexes",
				Severity:    "rec",
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

	// Index bloat analysis
	if len(res.IndexBloatStats) > 0 {
		largeUnusedIndexes := 0
		for _, ib := range res.IndexBloatStats {
			if ib.Scans == 0 && ib.WastedBytes > 100*1024*1024 { // >100MB
				largeUnusedIndexes++
			}
		}
		if largeUnusedIndexes > 0 {
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "Large unused indexes",
				Severity:    "rec",
				Description: fmt.Sprintf("%d indexes >100MB with zero scans", largeUnusedIndexes),
				Action:      "Consider dropping large unused indexes; monitor impact before removal.",
			})
		}
	}

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
				Description: "shared_buffers is at default value",
				Action:      "Set shared_buffers to 25-40% of available RAM for dedicated PostgreSQL servers.",
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
			Action:      "Consider using a connection pooler (pgbouncer) and reducing max_connections to 20-50.",
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
			})
		}
	}

	// Random page cost analysis
	if s, ok := setting("random_page_cost"); ok {
		if s.Val == "4" { // Default
			a.Recommendations = append(a.Recommendations, Finding{
				Title:       "random_page_cost at default",
				Severity:    "rec",
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
				Description: "idle_in_transaction_session_timeout is disabled",
				Action:      "Set idle_in_transaction_session_timeout to 10-60 minutes to prevent abandoned transactions.",
			})
		}
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
