package report

import (
	_ "embed"
	"fmt"
	"html/template"
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
	// Prepare sorted copies for top tables by rows and by size
	tablesBySize := make([]collect.TableStat, len(res.Tables))
	copy(tablesBySize, res.Tables)
	sort.Slice(tablesBySize, func(i, j int) bool { return tablesBySize[i].SizeBytes > tablesBySize[j].SizeBytes })
	tablesByRows := make([]collect.TableStat, len(res.Tables))
	copy(tablesByRows, res.Tables)
	sort.Slice(tablesByRows, func(i, j int) bool { return tablesByRows[i].NLiveTup > tablesByRows[j].NLiveTup })

	// Precompute whether any client has a hostname to show
	showHostname := false
	for _, c := range res.ConnectionsByClient {
		if c.Hostname != "" {
			showHostname = true
			break
		}
	}

	// Derive large unused indexes subset (>100MB) from both sources and dedupe
	largeUnusedMap := make(map[string]collect.IndexUnused)
	for _, iu := range res.IndexUnused {
		if iu.SizeBytes > 100*1024*1024 { // >100MB
			dbPart := strings.TrimSpace(iu.Database)
			key := dbPart + "|" + iu.Schema + "." + iu.Name
			largeUnusedMap[key] = iu
		}
	}
	// Also consider index bloat stats where scans=0 and size >100MB
	for _, ib := range res.IndexBloatStats {
		if ib.Scans == 0 && ib.WastedBytes > 100*1024*1024 {
			dbPart := strings.TrimSpace(res.ConnInfo.CurrentDB)
			key := dbPart + "|" + ib.Schema + "." + ib.Name
			if _, ok := largeUnusedMap[key]; !ok {
				largeUnusedMap[key] = collect.IndexUnused{Database: res.ConnInfo.CurrentDB, Schema: ib.Schema, Table: ib.Table, Name: ib.Name, SizeBytes: ib.WastedBytes}
			}
		}
	}
	largeUnused := make([]collect.IndexUnused, 0, len(largeUnusedMap))
	for _, v := range largeUnusedMap {
		largeUnused = append(largeUnused, v)
	}
	sort.Slice(largeUnused, func(i, j int) bool { return largeUnused[i].SizeBytes > largeUnused[j].SizeBytes })

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
	showDBLargeUnused := false
	for _, iu := range largeUnused {
		if strings.TrimSpace(iu.Database) != "" {
			showDBLargeUnused = true
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
		if total == 1 {
			return "1 unused index candidate detected; validate and consider dropping."
		}
		return fmt.Sprintf("%d unused index candidates detected; validate with workload owners before dropping.", total)
	}()
	largeUnusedSummary := func() string {
		large := len(largeUnused)
		if large == 0 {
			return ""
		}
		if large == 1 {
			return "1 large unused index detected; validate and consider dropping."
		}
		return fmt.Sprintf("%d large unused indexes detected; validate with workload owners before dropping.", large)
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
		"fmtInt":       func(n int) string { return addThousands(strconv.FormatInt(int64(n), 10)) },
		"fmtI64":       func(n int64) string { return addThousands(strconv.FormatInt(n, 10)) },
		"fmtF0":        func(f float64) string { return fmtFloatPrecSep(f, 0) },
		"fmtF1":        func(f float64) string { return fmtFloatPrecSep(f, 1) },
		"fmtF2":        func(f float64) string { return fmtFloatPrecSep(f, 2) },
		"fmtThousands": func(n int64) string { return addThousands(strconv.FormatInt(n, 10)) },
	}

	// Parse embedded report template
	tmpl, err := template.New("report").Funcs(funcMap).Parse(reportHTML)
	if err != nil {
		return err
	}
	data := struct {
		Res                 collect.Result
		A                   analyze.Analysis
		Meta                collect.Meta
		ShowHostname        bool
		Activity            []collect.Activity
		TablesByRows        []collect.TableStat
		TablesBySize        []collect.TableStat
		LargeUnused         []collect.IndexUnused
		ShowDBTablesByRows  bool
		ShowDBTablesBySize  bool
		ShowDBIndexUnused   bool
		ShowDBLargeUnused   bool
		ShowDBIndexUsageLow bool
		ShowDBIndexCounts   bool
		// summaries
		ConnSummary        string
		DBsSummary         string
		CacheHitsSummary   string
		IndexUnusedSummary string
		LargeUnusedSummary string
		IndexUsageSummary  string
		ClientsSummary     string
		BlockingSummary    string
		LongRunningSummary string
		AutovacSummary     string
	}{Res: res, A: a, Meta: meta, ShowHostname: showHostname, Activity: activity, TablesByRows: tablesByRows, TablesBySize: tablesBySize, LargeUnused: largeUnused,
		ShowDBTablesByRows: showDBTablesByRows, ShowDBTablesBySize: showDBTablesBySize, ShowDBIndexUnused: showDBIndexUnused, ShowDBLargeUnused: showDBLargeUnused, ShowDBIndexUsageLow: showDBIndexUsageLow, ShowDBIndexCounts: showDBIndexCounts,
		ConnSummary: connSummary, DBsSummary: dbsSummary, CacheHitsSummary: cacheHitsSummary, IndexUnusedSummary: indexUnusedSummary, LargeUnusedSummary: largeUnusedSummary,
		IndexUsageSummary: indexUsageSummary, ClientsSummary: clientsSummary, BlockingSummary: blockingSummary, LongRunningSummary: longRunningSummary, AutovacSummary: autovacSummary}
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
