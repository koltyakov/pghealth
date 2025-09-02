package report

import (
	_ "embed"
	"fmt"
	"html/template"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/koltyakov/pghealth/internal/analyze"
	"github.com/koltyakov/pghealth/internal/collect"
)

func WriteHTML(path string, res collect.Result, a analyze.Analysis, meta collect.Meta) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Sort numerical metrics descending so greater numbers show on top
	sort.Slice(res.DBs, func(i, j int) bool { return res.DBs[i].SizeBytes > res.DBs[j].SizeBytes })
	sort.Slice(res.Activity, func(i, j int) bool {
		if res.Activity[i].Count == res.Activity[j].Count {
			if res.Activity[i].Datname == res.Activity[j].Datname {
				return res.Activity[i].State < res.Activity[j].State
			}
			return res.Activity[i].Datname < res.Activity[j].Datname
		}
		return res.Activity[i].Count > res.Activity[j].Count
	})
	sort.Slice(res.IndexUnused, func(i, j int) bool { return res.IndexUnused[i].SizeBytes > res.IndexUnused[j].SizeBytes })
	sort.Slice(res.Indexes, func(i, j int) bool { return res.Indexes[i].SizeBytes > res.Indexes[j].SizeBytes })
	// Sort "Tables with index counts" by estimated bloat bytes (Size * Bloat%) desc, then by overall size desc
	sort.Slice(res.TablesWithIndexCount, func(i, j int) bool {
		a, b := res.TablesWithIndexCount[i], res.TablesWithIndexCount[j]
		wa := int64(math.Round(float64(a.SizeBytes) * a.BloatPct / 100.0))
		wb := int64(math.Round(float64(b.SizeBytes) * b.BloatPct / 100.0))
		if wa != wb {
			return wa > wb
		}
		if a.SizeBytes != b.SizeBytes {
			return a.SizeBytes > b.SizeBytes
		}
		if a.RowCount != b.RowCount {
			return a.RowCount > b.RowCount
		}
		if a.IndexCount != b.IndexCount {
			return a.IndexCount > b.IndexCount
		}
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		return a.Name < b.Name
	})
	// Prepare sorted copies for top tables by rows and by size
	tablesBySize := make([]collect.TableStat, len(res.Tables))
	copy(tablesBySize, res.Tables)
	sort.Slice(tablesBySize, func(i, j int) bool { return tablesBySize[i].SizeBytes > tablesBySize[j].SizeBytes })
	tablesByRows := make([]collect.TableStat, len(res.Tables))
	copy(tablesByRows, res.Tables)
	sort.Slice(tablesByRows, func(i, j int) bool { return tablesByRows[i].NLiveTup > tablesByRows[j].NLiveTup })

	// Aggregate estimated reclaimable space (via VACUUM) per database using table bloat heuristic
	reclaimByDB := map[string]int64{}
	reclaimTotal := int64(0)
	for _, t := range res.TablesWithIndexCount {
		db := strings.TrimSpace(t.Database)
		if db == "" {
			db = strings.TrimSpace(res.ConnInfo.CurrentDB)
		}
		est := int64(math.Round(float64(t.SizeBytes) * t.BloatPct / 100.0))
		if est > 0 {
			reclaimByDB[db] += est
			reclaimTotal += est
		}
	}
	// materialize and sort by bytes desc
	reclaimList := make([]struct {
		Database string
		Bytes    int64
	}, 0, len(reclaimByDB))
	for k, v := range reclaimByDB {
		reclaimList = append(reclaimList, struct {
			Database string
			Bytes    int64
		}{Database: k, Bytes: v})
	}
	sort.Slice(reclaimList, func(i, j int) bool { return reclaimList[i].Bytes > reclaimList[j].Bytes })

	// Precompute whether any client has a hostname to show
	showHostname := false
	for _, c := range res.ConnectionsByClient {
		if c.Hostname != "" {
			showHostname = true
			break
		}
	}

	// Build a combined set of unused indexes from both sources (candidates + bloat view), deduped
	combined := make(map[string]collect.IndexUnused)
	for _, iu := range res.IndexUnused {
		dbPart := strings.TrimSpace(iu.Database)
		if dbPart == "" {
			dbPart = strings.TrimSpace(res.ConnInfo.CurrentDB)
		}
		key := dbPart + "|" + iu.Schema + "." + iu.Name
		// keep the larger size if duplicate appears
		if prev, ok := combined[key]; !ok || iu.SizeBytes > prev.SizeBytes {
			combined[key] = iu
		}
	}
	for _, ib := range res.IndexBloatStats { // include zero-scan indexes seen as bloated
		if ib.Scans == 0 {
			db := strings.TrimSpace(res.ConnInfo.CurrentDB)
			key := db + "|" + ib.Schema + "." + ib.Name
			if prev, ok := combined[key]; !ok || ib.WastedBytes > prev.SizeBytes {
				combined[key] = collect.IndexUnused{Database: res.ConnInfo.CurrentDB, Schema: ib.Schema, Table: ib.Table, Name: ib.Name, SizeBytes: ib.WastedBytes}
			}
		}
	}
	// materialize and sort by size desc
	if len(combined) > 0 {
		merged := make([]collect.IndexUnused, 0, len(combined))
		for _, v := range combined {
			merged = append(merged, v)
		}
		sort.Slice(merged, func(i, j int) bool { return merged[i].SizeBytes > merged[j].SizeBytes })
		res.IndexUnused = merged
	}

	// Whether to show Database column in various sections
	showDBTablesByRows := false
	for _, t := range tablesByRows {
		if strings.TrimSpace(t.Database) != "" {
			showDBTablesByRows = true
			break
		}
	}
	showDBTablesBySize := false
	for _, t := range tablesBySize {
		if strings.TrimSpace(t.Database) != "" {
			showDBTablesBySize = true
			break
		}
	}
	showDBIndexUnused := false
	for _, iu := range res.IndexUnused {
		if strings.TrimSpace(iu.Database) != "" {
			showDBIndexUnused = true
			break
		}
	}
	showDBIndexUsageLow := false
	for _, iu := range res.IndexUsageLow {
		if strings.TrimSpace(iu.Database) != "" {
			showDBIndexUsageLow = true
			break
		}
	}
	showDBIndexCounts := false
	for _, ic := range res.TablesWithIndexCount {
		if strings.TrimSpace(ic.Database) != "" {
			showDBIndexCounts = true
			break
		}
	}

	// Top queries are not shown with DB scope

	// Filter connections activity to hide empty database/state entries and zero counts
	activity := make([]collect.Activity, 0, len(res.Activity))
	for _, it := range res.Activity {
		if it.Datname == "" {
			continue
		}
		if it.Count <= 0 {
			continue
		}
		activity = append(activity, it)
	}

	// Section summaries
	connSummary := func() string {
		if res.ConnInfo.MaxConnections > 0 {
			pct := float64(res.TotalConnections) / float64(res.ConnInfo.MaxConnections) * 100
			if pct >= 80 {
				return fmt.Sprintf("Attention: %s/%s (%.0f%%) connections in use. Consider a pooler and tuning max_connections.", addThousands(strconv.Itoa(res.TotalConnections)), addThousands(strconv.Itoa(res.ConnInfo.MaxConnections)), pct)
			}
			return fmt.Sprintf("Healthy: %s/%s (%.0f%%) connections in use.", addThousands(strconv.Itoa(res.TotalConnections)), addThousands(strconv.Itoa(res.ConnInfo.MaxConnections)), pct)
		}
		return ""
	}()
	dbsSummary := func() string {
		n := len(res.DBs)
		if n == 0 {
			return ""
		}
		top := res.DBs[0]
		return fmt.Sprintf("Databases: %d total. Largest: %s (%s).", n, top.Name, fmtBytesStr(top.SizeBytes))
	}()
	cacheHitsSummary := func() string {
		if len(res.CacheHits) == 0 {
			return ""
		}
		below := 0
		min := 101.0
		totalWith := 0
		for _, ch := range res.CacheHits {
			if ch.BlksHit+ch.BlksRead == 0 {
				continue
			}
			totalWith++
			if ch.Ratio < min {
				min = ch.Ratio
			}
			if ch.Ratio < 95.0 {
				below++
			}
		}
		if totalWith == 0 {
			return ""
		}
		if below == 0 {
			return fmt.Sprintf("Healthy: cache hit ratio looks good across databases (lowest %.2f%%).", min)
		}
		return fmt.Sprintf("Attention: %d database(s) below 95%% cache hit (lowest %.2f%%). Consider memory/indexing improvements.", below, min)
	}()
	indexUnusedSummary := func() string {
		total := len(res.IndexUnused)
		if total == 0 {
			return "Healthy: no unused indexes detected."
		}
		// count large ones (>100MB)
		large := 0
		for _, iu := range res.IndexUnused {
			if iu.SizeBytes > 100*1024*1024 {
				large++
			}
		}
		if large > 0 {
			return fmt.Sprintf("%d unused indexes (%d > 100MB). Validate with workload owners before dropping.", total, large)
		}
		if total == 1 {
			return "1 unused index detected; validate and consider dropping."
		}
		return fmt.Sprintf("%d unused indexes detected; validate with workload owners before dropping.", total)
	}()
	indexUsageSummary := func() string {
		if len(res.IndexUsageLow) == 0 {
			return ""
		}
		below50, below80 := 0, 0
		min := 100.0
		for _, iu := range res.IndexUsageLow {
			if iu.IndexUsagePct < min {
				min = iu.IndexUsagePct
			}
			if iu.IndexUsagePct < 50 {
				below50++
			}
			if iu.IndexUsagePct < 80 {
				below80++
			}
		}
		if below80 == 0 {
			return "Healthy: index usage looks healthy for sampled tables."
		}
		return fmt.Sprintf("Attention: %d table(s) with index usage < 80%% (min %.2f%%). Review predicates and add indexes.", below80, min)
	}()
	clientsSummary := func() string {
		if len(res.ConnectionsByClient) == 0 {
			return ""
		}
		top := res.ConnectionsByClient[0]
		who := top.Address
		if top.Hostname != "" {
			who = top.Hostname
		}
		suffix := "s"
		if top.Count == 1 {
			suffix = ""
		}
		return fmt.Sprintf("Top client: %s (%d connection%s).", who, top.Count, suffix)
	}()
	waitsSummary := func() string {
		if len(res.WaitEvents) == 0 {
			return ""
		}
		// Try to surface the analyzer's synthesized wait info and key actions concisely
		top := ""
		for _, f := range a.Infos {
			if f.Title == "Top wait types" {
				top = f.Description
				break
			}
		}
		// Short action hints for common wait categories
		ioHint := false
		lockHint := false
		pinHint := false
		for _, f := range a.Warnings {
			if f.Code == "high-wal" {
				continue
			}
			if f.Code == "io-waits" {
				ioHint = true
			}
			if f.Code == "lock-waits" {
				lockHint = true
			}
			if f.Code == "bufferpin-waits" {
				pinHint = true
			}
		}
		for _, f := range a.Recommendations {
			if f.Code == "io-waits" {
				ioHint = true
			}
			if f.Code == "lock-waits" {
				lockHint = true
			}
			if f.Code == "bufferpin-waits" {
				pinHint = true
			}
		}
		parts := []string{}
		if top != "" {
			parts = append(parts, top)
		}
		if ioHint {
			parts = append(parts, "IO waits: improve cache hit (shared_buffers, indexing), tune effective_io_concurrency and checkpoints, consider faster storage.")
		}
		if lockHint {
			parts = append(parts, "Lock waits: find blockers, shorten transactions, add indexes, and consider lock/statement timeouts.")
		}
		if pinHint {
			parts = append(parts, "BufferPin: avoid long idle-in-transaction; set idle_in_transaction_session_timeout.")
		}
		return strings.Join(parts, " ")
	}()
	blockingSummary := func() string {
		if len(res.Blocking) == 0 {
			return "Healthy: no blocking detected."
		}
		return fmt.Sprintf("Attention: %d blocking relationship(s); longest blocked for %s.", len(res.Blocking), res.Blocking[0].BlockedDuration)
	}()
	longRunningSummary := func() string {
		if len(res.LongRunning) == 0 {
			return "Healthy: no active queries > 5 minutes."
		}
		return fmt.Sprintf("Attention: %d long-running query(ies); longest %s.", len(res.LongRunning), res.LongRunning[0].Duration)
	}()
	autovacSummary := func() string {
		if len(res.AutoVacuum) == 0 {
			return "Healthy: no autovacuum workers active now."
		}
		return fmt.Sprintf("Autovacuum workers: %d active. Ensure cost settings aren’t throttling large tables.", len(res.AutoVacuum))
	}()

	// Brief explanation for Bloat in "Tables with index counts"
	bloatPctNote := "Bloat is estimated from dead tuple share: Bloat % ≈ n_dead_tup / (n_live_tup + n_dead_tup). 'Bloat (est.)' shows wasted bytes = table size × Bloat %. Rows over ~20% are highlighted. Use VACUUM to reclaim space; for severe bloat (>50%), consider VACUUM FULL or pg_repack and tune autovacuum (scale_factor, naptime, cost limits)."

	funcMap := template.FuncMap{
		"since":    func(t time.Time) string { return time.Since(t).String() },
		"add":      func(a, b int64) int64 { return a + b },
		"contains": func(s, sub string) bool { return strings.Contains(s, sub) },
		"fmtTime": func(t time.Time) string {
			if t.IsZero() {
				return "n/a"
			}
			return t.Local().Format("2006-01-02 15:04:05 MST")
		},
		"fmtDur": func(d time.Duration) string { return humanizeDuration(d) },
		// fmtMs converts milliseconds (float64) into a compact human duration.
		// For < 1000ms, render with two decimals (e.g., 12.34ms). For >= 1s, use humanized units.
		"fmtMs": func(ms float64) string {
			if ms <= 0 {
				return "0ms"
			}
			if ms < 1000 {
				return fmt.Sprintf("%.2fms", ms)
			}
			d := time.Duration(ms * float64(time.Millisecond))
			return humanizeDuration(d)
		},
		"fmtUptime": func(t time.Time) string {
			if t.IsZero() {
				return "n/a"
			}
			return humanizeDuration(time.Since(t))
		},
		"fmtBytes": func(b int64) string {
			units := []string{"B", "KB", "MB", "GB", "TB"}
			f := float64(b)
			i := 0
			for f >= 1024 && i < len(units)-1 {
				f /= 1024
				i++
			}
			return template.HTMLEscapeString((func() string { return fmtFloatPrecSep(f, 2) + " " + units[i] })())
		},
		"fmtInt": func(n int) string { return addThousands(strconv.FormatInt(int64(n), 10)) },
		"fmtI64": func(n int64) string { return addThousands(strconv.FormatInt(n, 10)) },
		"fmtF0":  func(f float64) string { return fmtFloatPrecSep(f, 0) },
		"fmtF1":  func(f float64) string { return fmtFloatPrecSep(f, 1) },
		// Map analyzer finding to a section anchor if available. If the section
		// isn’t rendered (no details), return empty so the card isn’t a link.
		"findingAnchor": func(code, title string) string {
			// Helpers for availability
			hasWaits := len(res.WaitEvents) > 0
			hasWal := res.WAL != nil
			hasTemp := len(res.TempFileStats) > 0
			hasExtList := len(res.ExtensionStats) > 0
			hasFuncs := len(res.FunctionStats) > 0
			hasCI := len(res.ProgressCreateIndex) > 0
			hasPSSLists := res.Extensions.PgStatStatements && res.Statements.SkippedReason == ""
			hasUnusedIdx := len(res.IndexUnused) > 0
			hasRepl := len(res.ReplicationStats) > 0

			switch code {
			case "io-waits", "lock-waits", "bufferpin-waits":
				if hasWaits {
					return "#hdr-waits"
				}
				return ""
			case "high-wal", "wal-fpi", "wal-fpi-high":
				if hasWal {
					return "#hdr-wal"
				}
				return ""
			case "unused-indexes":
				if hasUnusedIdx {
					return "#hdr-index-unused"
				}
				return ""
			case "too-many-indexes", "table-bloat-heuristic":
				return "#hdr-index-counts"
			case "missing-indexes":
				return "#hdr-index-usage-low"
			case "slow-index-improve", "slow-refactor", "slow-sorts", "slow-joins", "slow-seq-scans":
				if hasPSSLists {
					return "#hdr-queries-total-time"
				}
				return ""
			case "long-running":
				return "#hdr-long-running"
			case "ci-wait-lockers":
				if hasCI {
					return "#hdr-progress-ci"
				}
				return ""
			case "hot-function", "hot-functions-multi":
				if hasFuncs {
					return "#hdr-functions"
				}
				return ""
			case "install-pgss":
				return "#hdr-settings"
			case "missing-extensions":
				if hasExtList {
					return "#hdr-extensions"
				}
				return ""
			case "enable-track-io", "wal-level-minimal", "checkpoint-timeout-low", "ecs-low-vs-sb", "high-max-connections", "autovacuum-naptime-high", "maintenance-work-mem-low", "random-page-cost-default", "no-statement-timeout", "no-idle-tx-timeout", "ssl-off":
				return "#hdr-settings"
			case "cache-overall":
				return "#hdr-cache-hit"
			}
			// Fallback by keywords in title when code missing
			lt := strings.ToLower(title)
			switch {
			case strings.Contains(lt, "wait"):
				if hasWaits {
					return "#hdr-waits"
				}
				return ""
			case strings.Contains(lt, "block"):
				return "#hdr-blocking" // always present
			case strings.Contains(lt, "autovac"):
				return "#hdr-autovacuum" // always present
			case strings.Contains(lt, "replication"):
				if hasRepl {
					return "#hdr-replication"
				}
				return ""
			case strings.Contains(lt, "temp"):
				if hasTemp {
					return "#hdr-temp-files"
				}
				return ""
			case strings.Contains(lt, "cache hit"):
				return "#hdr-cache-hit" // always present
			}
			return ""
		},
		"fmtF2":        func(f float64) string { return fmtFloatPrecSep(f, 2) },
		"fmtThousands": func(n int64) string { return addThousands(strconv.FormatInt(n, 10)) },
		// bloatBytes estimates wasted bytes from size and percent
		"bloatBytes": func(size int64, pct float64) int64 {
			if size <= 0 || pct <= 0 {
				return 0
			}
			return int64(math.Round(float64(size) * pct / 100.0))
		},
	}

	// Parse embedded report template
	tmpl, err := template.New("report").Funcs(funcMap).Parse(reportHTML)
	if err != nil {
		return err
	}

	// Build attention lists for queries: high total time share and high invocations
	shorten := func(s string, n int) string {
		s = strings.TrimSpace(s)
		if len(s) <= n {
			return s
		}
		return s[:n] + "…"
	}
	median := func(vals []float64) float64 {
		if len(vals) == 0 {
			return 0
		}
		vv := make([]float64, len(vals))
		copy(vv, vals)
		sort.Slice(vv, func(i, j int) bool { return vv[i] < vv[j] })
		n := len(vv)
		if n%2 == 1 {
			return vv[n/2]
		}
		return (vv[n/2-1] + vv[n/2]) / 2.0
	}
	type attnItem struct {
		Query  string
		Suffix string
		Href   string
	}
	var attentionTotalTime []attnItem
	if len(res.Statements.TopByTotalTime) > 0 {
		var sumTT float64
		vals := make([]float64, 0, len(res.Statements.TopByTotalTime))
		for _, s := range res.Statements.TopByTotalTime {
			sumTT += s.TotalTime
			vals = append(vals, s.TotalTime)
		}
		med := median(vals)
		for i, s := range res.Statements.TopByTotalTime {
			if sumTT <= 0 {
				break
			}
			share := s.TotalTime / sumTT
			// Require at least 10% share to list, even if it's an outlier by median
			if share < 0.10 {
				continue
			}
			if share >= 0.20 || (med > 0 && s.TotalTime >= 1.8*med) {
				q := shorten(s.Query, 120)
				suf := fmt.Sprintf(" — %.0f%% of total time.", share*100)
				href := fmt.Sprintf("#query-pre-total-%d", i)
				attentionTotalTime = append(attentionTotalTime, attnItem{Query: q, Suffix: suf, Href: href})
				if len(attentionTotalTime) >= 5 {
					break
				}
			}
		}
	}
	var attentionCalls []attnItem
	if len(res.Statements.TopByCalls) > 0 {
		var sumCalls float64
		vals := make([]float64, 0, len(res.Statements.TopByCalls))
		for _, s := range res.Statements.TopByCalls {
			sumCalls += s.Calls
			vals = append(vals, s.Calls)
		}
		med := median(vals)
		for i, s := range res.Statements.TopByCalls {
			if sumCalls <= 0 {
				break
			}
			share := s.Calls / sumCalls
			// Require at least 10% share to list, even if it's an outlier by median
			if share < 0.10 {
				continue
			}
			if share >= 0.20 || (med > 0 && s.Calls >= 2.0*med) {
				// Prefer calls per hour if available
				callsStr := fmtFloatPrecSep(s.Calls, 0) + " calls"
				if s.CallsPerHour > 0 {
					callsStr = fmtFloatPrecSep(s.CallsPerHour, 1) + " calls/hr"
				}
				q := shorten(s.Query, 120)
				suf := fmt.Sprintf(" — %.0f%% of total invocations (%s).", share*100, callsStr)
				href := fmt.Sprintf("#query-pre-calls-%d", i)
				attentionCalls = append(attentionCalls, attnItem{Query: q, Suffix: suf, Href: href})
				if len(attentionCalls) >= 5 {
					break
				}
			}
		}
	}
	data := struct {
		Res                 collect.Result
		A                   analyze.Analysis
		Meta                collect.Meta
		ShowHostname        bool
		Activity            []collect.Activity
		TablesByRows        []collect.TableStat
		TablesBySize        []collect.TableStat
		ShowDBTablesByRows  bool
		ShowDBTablesBySize  bool
		ShowDBIndexUnused   bool
		ShowDBIndexUsageLow bool
		ShowDBIndexCounts   bool
		ReclaimByDB         []struct {
			Database string
			Bytes    int64
		}
		ReclaimTotal int64
		// summaries
		ConnSummary        string
		DBsSummary         string
		CacheHitsSummary   string
		IndexUnusedSummary string
		IndexUsageSummary  string
		ClientsSummary     string
		BlockingSummary    string
		LongRunningSummary string
		AutovacSummary     string
		WaitsSummary       string
		BloatPctNote       string
		// attention lists
		AttentionTotalTime []attnItem
		AttentionCalls     []attnItem
	}{Res: res, A: a, Meta: meta, ShowHostname: showHostname, Activity: activity, TablesByRows: tablesByRows, TablesBySize: tablesBySize,
		ShowDBTablesByRows: showDBTablesByRows, ShowDBTablesBySize: showDBTablesBySize, ShowDBIndexUnused: showDBIndexUnused, ShowDBIndexUsageLow: showDBIndexUsageLow, ShowDBIndexCounts: showDBIndexCounts,
		ReclaimByDB: reclaimList, ReclaimTotal: reclaimTotal,
		ConnSummary: connSummary, DBsSummary: dbsSummary, CacheHitsSummary: cacheHitsSummary, IndexUnusedSummary: indexUnusedSummary,
		IndexUsageSummary: indexUsageSummary, ClientsSummary: clientsSummary, BlockingSummary: blockingSummary, LongRunningSummary: longRunningSummary, AutovacSummary: autovacSummary, WaitsSummary: waitsSummary,
		BloatPctNote:       bloatPctNote,
		AttentionTotalTime: attentionTotalTime,
		AttentionCalls:     attentionCalls,
	}
	return tmpl.Execute(f, data)
}

// fmtFloat previously trimmed trailing zeros; replaced by fmtFloatPrecSep

// fmtFloatPrecSep formats a float with fixed precision and thousands separators in the integer part
func fmtFloatPrecSep(f float64, prec int) string {
	s := strconv.FormatFloat(f, 'f', prec, 64)
	// split integer and fractional parts
	dot := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			dot = i
			break
		}
	}
	if dot == -1 {
		return addThousands(s)
	}
	return addThousands(s[:dot]) + s[dot:]
}

// addThousands inserts commas as thousands separators into a numeric string (handles leading '-')
func addThousands(s string) string {
	if s == "" {
		return s
	}
	neg := false
	if s[0] == '-' {
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
	// build reversed with commas every 3 digits
	out := make([]byte, 0, n+n/3)
	cnt := 0
	for i := n - 1; i >= 0; i-- {
		out = append(out, s[i])
		cnt++
		if cnt%3 == 0 && i != 0 {
			out = append(out, ',')
		}
	}
	// reverse back
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

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
	// For >= 1s, round to seconds and build a compact representation
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
		// Add seconds if we have less than 3 parts (d, h, m)
		if len(parts) < 3 {
			parts = append(parts, fmt.Sprintf("%ds", secs))
		}
	}

	if len(parts) == 0 {
		return "0s"
	}

	return strings.Join(parts, " ")
}

// fmtBytesStr converts bytes into a human readable string with units (B, KB, MB, GB, TB)
func fmtBytesStr(b int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(b)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmtFloatPrecSep(f, 2) + " " + units[i]
}

//go:embed template.html
var reportHTML string
