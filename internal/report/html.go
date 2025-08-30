package report

import (
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
			return fmt.Sprintf("OK: %s/%s (%.0f%%) connections in use.", addThousands(strconv.Itoa(res.TotalConnections)), addThousands(strconv.Itoa(res.ConnInfo.MaxConnections)), pct)
		}
		return ""
	}()
	dbsSummary := func() string {
		n := len(res.DBs)
		if n == 0 {
			return ""
		}
		top := res.DBs[0]
		sizeMB := float64(top.SizeBytes) / 1024.0 / 1024.0
		return fmt.Sprintf("Databases: %d total. Largest: %s (%.2f MB).", n, top.Name, sizeMB)
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
			return fmt.Sprintf("OK: cache hit healthy across databases (lowest %.2f%%).", min)
		}
		return fmt.Sprintf("Attention: %d database(s) below 95%% cache hit (lowest %.2f%%). Consider memory/indexing improvements.", below, min)
	}()
	indexUnusedSummary := func() string {
		n := len(res.IndexUnused)
		if n == 0 {
			return "OK: no large unused indexes detected."
		}
		if n == 1 {
			return "1 unused index candidate detected; validate and consider dropping."
		}
		return fmt.Sprintf("%d unused index candidates detected; validate with workload owners before dropping.", n)
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
			return "OK: index usage looks healthy for sampled tables."
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
			return "OK: no blocking detected."
		}
		return fmt.Sprintf("Attention: %d blocking relationship(s); longest blocked for %s.", len(res.Blocking), res.Blocking[0].BlockedDuration)
	}()
	longRunningSummary := func() string {
		if len(res.LongRunning) == 0 {
			return "OK: no active queries > 5 minutes."
		}
		return fmt.Sprintf("Attention: %d long-running query(ies); longest %s.", len(res.LongRunning), res.LongRunning[0].Duration)
	}()
	autovacSummary := func() string {
		if len(res.AutoVacuum) == 0 {
			return "OK: no autovacuum workers active now."
		}
		return fmt.Sprintf("Autovacuum workers: %d active. Ensure cost settings aren’t throttling large tables.", len(res.AutoVacuum))
	}()

	tmpl := template.Must(template.New("report").Funcs(template.FuncMap{
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
		// fmtMs converts milliseconds (float64) into a compact human duration like "6h 27m" or "42s"
		"fmtMs": func(ms float64) string {
			if ms <= 0 {
				return "0s"
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
		"fmtMB": func(b int64) string {
			return fmtFloatPrecSep(float64(b)/1024.0/1024.0, 2)
		},
		"fmtInt": func(n int) string { return addThousands(strconv.FormatInt(int64(n), 10)) },
		"fmtI64": func(n int64) string { return addThousands(strconv.FormatInt(n, 10)) },
		"fmtF0":  func(f float64) string { return fmtFloatPrecSep(f, 0) },
		"fmtF2":  func(f float64) string { return fmtFloatPrecSep(f, 2) },
	}).Parse(htmlTemplate))
	data := struct {
		Res          collect.Result
		A            analyze.Analysis
		Meta         collect.Meta
		ShowHostname bool
		Activity     []collect.Activity
		TablesByRows []collect.TableStat
		TablesBySize []collect.TableStat
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
	}{Res: res, A: a, Meta: meta, ShowHostname: showHostname, Activity: activity, TablesByRows: tablesByRows, TablesBySize: tablesBySize,
		ConnSummary: connSummary, DBsSummary: dbsSummary, CacheHitsSummary: cacheHitsSummary, IndexUnusedSummary: indexUnusedSummary,
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

// humanizeDuration renders a duration like "2d 3h 4m" or "5m 12s" depending on magnitude
func humanizeDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	total := int64(d.Seconds())
	days := total / 86400
	total %= 86400
	hours := total / 3600
	total %= 3600
	mins := total / 60
	secs := total % 60
	parts := make([]string, 0, 3)
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if mins > 0 {
		parts = append(parts, fmt.Sprintf("%dm", mins))
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}
	return strings.Join(parts, " ")
}

const htmlTemplate = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>PostgreSQL Health Check Report</title>
  <style>
		body{font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin:24px; color:#111827;}
		header{margin-bottom:36px}
		h1{font-size:20px;margin:0 0 12px 0}
		header > div{margin-top:6px}
		h2{margin-top:24px;border-bottom:1px solid #e5e7eb;padding-bottom:4px}
		.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
		.card{border:1px solid #e5e7eb;padding:12px;background:#fff}
		/* Improve card readability */
		.card > strong{display:block;margin-bottom:8px}
		.card > div{margin:6px 0}
		.card small{display:block;margin-top:6px}
		.warn{border-left:4px solid #f59e0b}
		.rec{border-left:4px solid #10b981}
		.info{border-left:4px solid #3b82f6}
		.table-wrap{margin:8px 0; overflow:hidden}
		table{border-collapse:collapse;border-spacing:0;width:100%}
		th,td{border:1px solid #9ca3af;padding:10px 12px;text-align:left;vertical-align:top}
		thead th{background:#f3f4f6;font-weight:600;border-bottom:2px solid #9ca3af}
		tbody tr:nth-child(even){background:#fcfcfd}
		tbody tr:hover{background:#f8fafc}
		code{background:#f3f4f6;padding:2px 4px}
		.muted{color:#6b7280}
		small{font-size:12px;color:#4b5563}
		.table-wrap.collapsed tbody tr:nth-child(n+11){display:none}
		.table-tools{margin:12px 0 0;display:flex;justify-content:flex-end;padding:0}
		.hot{background:#fff7ed}
		.toggle-rows{background:#fff;border:1px solid #d1d5db;padding:6px 10px;cursor:pointer}
		.toggle-rows:hover{background:#f9fafb}
		pre{white-space:pre-wrap;max-height:8em;overflow:auto;margin:0;background:#f8fafc;border:1px solid #e5e7eb;padding:8px}
		pre.query.expanded{max-height:none}
		.plan-pre.expanded{max-height:none}
		.query-short{display:block;max-height:4em;overflow:hidden}
		.query-full{display:none}
		.show-full{background:#fff;border:1px solid #d1d5db;padding:2px 6px;margin-top:6px;cursor:pointer}
		.nowrap{white-space:nowrap}
		.badge-attn{display:inline-block;background:#fef3c7;color:#92400e;border:1px solid #fcd34d;padding:2px 6px;font-size:12px;border-radius:4px}
		.section-note{margin:8px 0 0;color:#4b5563}
		/* Plan advice styling */
		.plan-advice{margin-top:8px;padding:8px;border:1px solid #e5e7eb;background:#f9fafb}
		.plan-advice h4{margin:0 0 6px;font-size:14px}
		.plan-advice ul{margin:6px 0 8px 18px}
		.show-plan{background:#fff;border:1px solid #d1d5db;padding:2px 6px;margin-top:6px;cursor:pointer}
		.plan-pre{white-space:pre-wrap;max-height:12em;overflow:auto;margin:6px 0 0;background:#f8fafc;border:1px solid #e5e7eb;padding:8px}
  </style>
  </head>
<body>
  <header>
    <h1>PostgreSQL Health Check Report</h1>
  <div>{{if not (contains .Meta.Version "-dirty")}}Version: {{.Meta.Version}} &middot; {{end}}Started: {{fmtTime .Meta.StartedAt}} &middot; Duration: {{fmtDur .Meta.Duration}}</div>
    <div>Server: {{.Res.ConnInfo.Version}} &middot; DB: {{.Res.ConnInfo.CurrentDB}} &middot; User: {{.Res.ConnInfo.CurrentUser}} &middot; SSL: {{.Res.ConnInfo.SSL}}</div>
  </header>

  <section class="grid">
    {{range .A.Warnings}}
      <div class="card warn"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><small>{{.Action}}</small></div></div>
    {{end}}
    {{range .A.Recommendations}}
      <div class="card rec"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><small>{{.Action}}</small></div></div>
    {{end}}
    {{range .A.Infos}}
      <div class="card info"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><small>{{.Action}}</small></div></div>
    {{end}}
  </section>

  <h2>Connections</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Database</th><th>State</th><th>Count</th></tr></thead>
    <tbody>
    {{if .Activity}}
      {{range .Activity}}<tr><td>{{.Datname}}</td><td>{{.State}}</td><td>{{fmtInt .Count}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="3" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Activity) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{if .ConnSummary}}<p class="section-note">{{.ConnSummary}}</p>{{end}}

  <h2>Databases</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Name</th><th>Size, Mb</th><th>Tablespace</th><th>Connections</th></tr></thead>
    <tbody>
    {{if .Res.DBs}}
      {{range .Res.DBs}}<tr><td>{{.Name}}</td><td>{{fmtMB .SizeBytes}}</td><td>{{.Tablespaces}}</td><td>{{fmtInt .ConnCount}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.DBs) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{if .DBsSummary}}<p class="section-note">{{.DBsSummary}}</p>{{end}}

  <h2>Top tables by rows</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Schema</th><th>Table</th><th>Rows</th></tr></thead>
    <tbody>
    {{if .TablesByRows}}
      {{range $i, $t := .TablesByRows}}{{if lt $i 100}}<tr><td>{{$t.Schema}}</td><td>{{$t.Name}}</td><td>{{fmtI64 $t.NLiveTup}}</td></tr>{{end}}{{end}}
    {{else}}
      <tr><td colspan="3" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .TablesByRows) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{/* No explicit summary for this table to avoid noise */}}

  <h2>Top tables by size</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Schema</th><th>Table</th><th>Size, Mb</th></tr></thead>
    <tbody>
    {{if .TablesBySize}}
      {{range $i, $t := .TablesBySize}}{{if lt $i 100}}<tr><td>{{$t.Schema}}</td><td>{{$t.Name}}</td><td>{{fmtMB $t.SizeBytes}}</td></tr>{{end}}{{end}}
    {{else}}
      <tr><td colspan="3" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .TablesBySize) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{/* No explicit summary for this table to avoid noise */}}

  <h2>Settings (subset)</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Name</th><th>Value</th><th>Unit</th><th>Source</th></tr></thead>
    <tbody>
    {{if .Res.Settings}}
  {{range .Res.Settings}}<tr><td>{{.Name}}</td><td>{{.Val}}</td><td>{{.Unit}}</td><td>{{.Source}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.Settings) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>

  <h2>Indexes (unused candidates)</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Schema</th><th>Table</th><th>Index</th><th>Size, Mb</th></tr></thead>
    <tbody>
    {{if .Res.IndexUnused}}
      {{range .Res.IndexUnused}}<tr><td>{{.Schema}}</td><td>{{.Table}}</td><td>{{.Name}}</td><td>{{fmtMB .SizeBytes}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.IndexUnused) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  <p class="section-note">{{.IndexUnusedSummary}}</p>

  <h2>Tables with lowest index usage</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Schema</th><th>Table</th><th>Index usage (%)</th><th>Rows</th></tr></thead>
    <tbody>
    {{if .Res.IndexUsageLow}}
      {{range .Res.IndexUsageLow}}<tr><td>{{.Schema}}</td><td>{{.Table}}</td><td>{{fmtF2 .IndexUsagePct}}</td><td>{{fmtI64 .Rows}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.IndexUsageLow) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{if .IndexUsageSummary}}<p class="section-note">{{.IndexUsageSummary}}</p>{{end}}

  <h3>Cache hit ratio by database</h3>
  <p class="muted">Interpretation: closer to 100% is better. Values above ~99% are typical for OLTP workloads. Lower ratios indicate more disk reads; consider increasing shared_buffers, reviewing working set size, and improving indexing and query plans.</p>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Database</th><th>blks_hit</th><th>blks_read</th><th>Hit ratio (%)</th></tr></thead>
    <tbody>
    {{if .Res.CacheHits}}
      {{/* filter rows with zero total */}}
      {{- $rows := 0 -}}
      {{- range .Res.CacheHits -}}
        {{- $total := (printf "%d" (add .BlksHit .BlksRead)) -}}
      {{- end -}}
      {{range .Res.CacheHits}}{{if gt (add .BlksHit .BlksRead) 0}}<tr><td>{{.Datname}}</td><td>{{fmtI64 .BlksHit}}</td><td>{{fmtI64 .BlksRead}}</td><td>{{fmtF2 .Ratio}}</td></tr>{{end}}{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.CacheHits) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{if .CacheHitsSummary}}<p class="section-note">{{.CacheHitsSummary}}</p>{{end}}

  <h3>Connections by client</h3>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr>{{if .ShowHostname}}<th>Hostname</th>{{end}}<th>Address</th><th>User</th><th>Application</th><th>Connections</th></tr></thead>
    <tbody>
    {{if .Res.ConnectionsByClient}}
      {{range .Res.ConnectionsByClient}}<tr>{{if $.ShowHostname}}<td>{{.Hostname}}</td>{{end}}<td>{{.Address}}</td><td>{{.User}}</td><td>{{.Application}}</td><td>{{fmtInt .Count}}</td></tr>{{end}}
    {{else}}
      {{if .ShowHostname}}
        <tr><td colspan="5" class="muted">No data</td></tr>
      {{else}}
        <tr><td colspan="4" class="muted">No data</td></tr>
      {{end}}
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.ConnectionsByClient) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}
  </div>
  {{if .ClientsSummary}}<p class="section-note">{{.ClientsSummary}}</p>{{end}}

  {{if .Res.Extensions.PgStatStatements}}
  <h2>Top queries by total time</h2>
  <div class="table-wrap collapsed">
  <table>
  <thead><tr><th>Calls</th><th>Total time</th><th>Mean time (ms)</th><th>Attention</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByTotalTime}}
  	{{range $i, $q := .Res.Statements.TopByTotalTime}}
		<tr class="{{if lt $i 3}}hot{{end}}">
			<td class="nowrap">{{fmtF0 $q.Calls}}</td><td class="nowrap">{{fmtMs $q.TotalTime}}</td><td class="nowrap">{{fmtF2 $q.MeanTime}}</td><td>{{if $q.NeedsAttention}}<span class="badge-attn">Needs attention</span>{{else}}<span class="muted">-</span>{{end}}</td><td>
        <pre class="query"><span class="query-short">{{printf "%.200s" $q.Query}}{{if gt (len $q.Query) 200}}...{{end}}</span><span class="query-full">{{$q.Query}}</span></pre>
  			<button type="button" class="show-full">Show full</button>
        {{if $q.Advice}}
        <div class="plan-advice">
          {{if $q.Advice.Highlights}}
            <h4>Plan highlights</h4>
            <ul>
              {{range $q.Advice.Highlights}}<li>{{.}}</li>{{end}}
            </ul>
          {{end}}
          {{if $q.Advice.Suggestions}}
            <h4>Suggestions</h4>
            <ul>
              {{range $q.Advice.Suggestions}}<li>{{.}}</li>{{end}}
            </ul>
          {{end}}
          {{if $q.Advice.Plan}}
            <pre class="plan-pre" style="display:none">{{$q.Advice.Plan}}</pre>
            <button type="button" class="show-plan">Show plan</button>
          {{end}}
        </div>
        {{end}}
      </td>
		</tr>
		{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByTotalTime) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Top queries by calls</h2>
  <div class="table-wrap collapsed">
  <table>
  <thead><tr><th>Calls</th><th>Total time</th><th>Mean time (ms)</th><th>Attention</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByCalls}}
  	{{range $i, $q := .Res.Statements.TopByCalls}}
			<tr class="{{if lt $i 3}}hot{{end}}">
			<td class="nowrap">{{fmtF0 $q.Calls}}</td><td class="nowrap">{{fmtMs $q.TotalTime}}</td><td class="nowrap">{{fmtF2 $q.MeanTime}}</td><td>{{if $q.NeedsAttention}}<span class="badge-attn">Needs attention</span>{{else}}<span class="muted">-</span>{{end}}</td><td>
        <pre class="query"><span class="query-short">{{printf "%.200s" $q.Query}}{{if gt (len $q.Query) 200}}...{{end}}</span><span class="query-full">{{$q.Query}}</span></pre>
  			<button type="button" class="show-full">Show full</button>
      </td>
			</tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByCalls) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}</div>
  {{else}}
  <p>pg_stat_statements is not enabled in this database. Install and preload it for detailed query insights.</p>
  {{end}}

  <h2>Blocking queries</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>DB</th><th>Blocked PID</th><th>Blocked for</th><th>Blocking PID</th><th>Blocking for</th><th>Blocked query</th><th>Blocking query</th></tr></thead>
    <tbody>
    {{if .Res.Blocking}}
      {{range .Res.Blocking}}<tr><td>{{.Datname}}</td><td>{{.BlockedPID}}</td><td>{{.BlockedDuration}}</td><td>{{.BlockingPID}}</td><td>{{.BlockingDuration}}</td><td><pre>{{.BlockedQuery}}</pre></td><td><pre>{{.BlockingQuery}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="7" class="muted">No blocking detected</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Blocking) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}</div>
  <p class="section-note">{{.BlockingSummary}}</p>

  <h2>Long running queries (> 5m)</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>DB</th><th>PID</th><th>Duration</th><th>State</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.LongRunning}}
      {{range .Res.LongRunning}}<tr><td>{{.Datname}}</td><td>{{.PID}}</td><td>{{.Duration}}</td><td>{{.State}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="5" class="muted">No long running queries</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.LongRunning) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}</div>
  <p class="section-note">{{.LongRunningSummary}}</p>

  <h2>Autovacuum activities</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>DB</th><th>PID</th><th>Relation</th><th>Phase</th><th>Scanned</th><th>Total</th></tr></thead>
    <tbody>
    {{if .Res.AutoVacuum}}
      {{range .Res.AutoVacuum}}<tr><td>{{.Datname}}</td><td>{{.PID}}</td><td>{{.Relation}}</td><td>{{.Phase}}</td><td>{{fmtI64 .Scanned}}</td><td>{{fmtI64 .Total}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="6" class="muted">No autovacuum workers</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.AutoVacuum) 10}}<div class="table-tools"><button type="button" class="toggle-rows">Show all</button></div>{{end}}</div>
  <p class="section-note">{{.AutovacSummary}}</p>

  <footer style="margin-top:24px;color:#6b7280;display:flex;align-items:center;gap:8px">Report generated at {{fmtTime .Meta.StartedAt}} in {{fmtDur .Meta.Duration}}</footer>

  <script>
  (function(){
    // Initialize query states on page load
    document.addEventListener('DOMContentLoaded', function(){
      var fullEls = document.querySelectorAll('.query-full');
      for(var i = 0; i < fullEls.length; i++){
        fullEls[i].style.display = 'none';
      }
    });
    
    document.addEventListener('click', function(e){
      var btn;
      var el = (e.target && e.target.nodeType === 1) ? e.target : (e.target && e.target.parentElement);
      // Toggle rows (Show all / Show less)
      btn = el && el.closest && el.closest('.toggle-rows');
      if(btn){
    		e.preventDefault();
        var wrap = btn.closest('.table-wrap');
        if(!wrap) return;
        wrap.classList.toggle('collapsed');
        btn.textContent = wrap.classList.contains('collapsed') ? 'Show all' : 'Show less';
        return;
      }
      // Toggle query text (Show full / Show less)
      btn = el && el.closest && el.closest('.show-full');
      if (btn) {
    		e.preventDefault();
        var td = btn.closest('td');
        if(!td) return;
        var shortEl = td.querySelector('.query-short');
        var fullEl = td.querySelector('.query-full');
    		var pre = td.querySelector('pre.query');
        if(!shortEl || !fullEl) return;
        var expanded = fullEl.style.display === 'block';
        fullEl.style.display = expanded ? 'none' : 'block';
        shortEl.style.display = expanded ? 'block' : 'none';
				if(pre){ pre.classList.toggle('expanded', !expanded); }
				btn.textContent = expanded ? 'Show full' : 'Show less';
        return;
      }
      // Toggle plan visibility (Show plan / Hide plan)
      btn = el && el.closest && el.closest('.show-plan');
      if (btn) {
    		e.preventDefault();
        var card = btn.closest('.plan-advice');
        if(!card) return;
        var pre = card.querySelector('.plan-pre');
        if(!pre) return;
        var expanded = pre.style.display === 'block';
				pre.style.display = expanded ? 'none' : 'block';
				pre.classList.toggle('expanded', !expanded);
				btn.textContent = expanded ? 'Show plan' : 'Hide plan';
        return;
      }
    });
  })();
  </script>
</body>
</html>`
