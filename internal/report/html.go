package report

import (
	"html/template"
	"os"
	"sort"
	"strconv"
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

	// Precompute whether any client has a hostname to show
	showHostname := false
	for _, c := range res.ConnectionsByClient {
		if c.Hostname != "" {
			showHostname = true
			break
		}
	}

	tmpl := template.Must(template.New("report").Funcs(template.FuncMap{
		"since": func(t time.Time) string { return time.Since(t).String() },
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
	}{Res: res, A: a, Meta: meta, ShowHostname: showHostname}
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

const htmlTemplate = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>pghealth report</title>
  <style>
    body{font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin:24px; color:#111827;}
    header{margin-bottom:16px}
    h1{font-size:20px;margin:0}
    h2{margin-top:24px;border-bottom:1px solid #e5e7eb;padding-bottom:4px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
    .card{border:1px solid #e5e7eb;border-radius:8px;padding:12px;background:#fff;box-shadow:0 1px 1px rgba(0,0,0,.02)}
    .warn{border-left:4px solid #f59e0b}
    .rec{border-left:4px solid #10b981}
    .info{border-left:4px solid #3b82f6}
  table{border-collapse:separate;border-spacing:0;width:100%;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden}
  th,td{border:1px solid #e5e7eb;padding:10px 12px;text-align:left;vertical-align:top}
  thead th{background:#f9fafb;font-weight:600}
  tbody tr:nth-child(even){background:#fcfcfd}
  tbody tr:hover{background:#f8fafc}
    code{background:#f3f4f6;padding:2px 4px;border-radius:4px}
    .muted{color:#6b7280}
    .table-wrap{margin:8px 0}
    .table-wrap.collapsed tbody tr:nth-child(n+11){display:none}
    .table-tools{margin:6px 0 14px;display:flex;justify-content:flex-end}
    .toggle-rows{background:#fff;border:1px solid #d1d5db;border-radius:6px;padding:6px 10px;cursor:pointer}
    .toggle-rows:hover{background:#f9fafb}
  </style>
  </head>
<body>
  <header>
    <h1>pghealth report</h1>
    <div>Version: {{.Meta.Version}} &middot; Started: {{.Meta.StartedAt}} &middot; Duration: {{.Meta.Duration}}</div>
    <div>Server: {{.Res.ConnInfo.Version}} &middot; DB: {{.Res.ConnInfo.CurrentDB}} &middot; User: {{.Res.ConnInfo.CurrentUser}} &middot; SSL: {{.Res.ConnInfo.SSL}}</div>
  </header>

  <section class="grid">
    {{range .A.Warnings}}
      <div class="card warn"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><em>{{.Action}}</em></div></div>
    {{end}}
    {{range .A.Recommendations}}
      <div class="card rec"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><em>{{.Action}}</em></div></div>
    {{end}}
    {{range .A.Infos}}
      <div class="card info"><strong>{{.Title}}</strong><div>{{.Description}}</div><div><em>{{.Action}}</em></div></div>
    {{end}}
  </section>

  <h2>Connections</h2>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Database</th><th>State</th><th>Count</th></tr></thead>
    <tbody>
    {{if .Res.Activity}}
      {{range .Res.Activity}}<tr><td>{{.Datname}}</td><td>{{.State}}</td><td>{{fmtInt .Count}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="3" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.Activity) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

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
  {{if gt (len .Res.DBs) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

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
  {{if gt (len .Res.Settings) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
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
  {{if gt (len .Res.IndexUnused) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

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
  {{if gt (len .Res.IndexUsageLow) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

  <h2>Healthchecks</h2>
  <div class="grid">
    <div class="card info">
      <strong>Uptime</strong>
      <div>{{if .Res.ConnInfo.StartTime}}{{since .Res.ConnInfo.StartTime}} (since {{.Res.ConnInfo.StartTime}}){{else}}<span class="muted">n/a</span>{{end}}</div>
    </div>
    <div class="card info">
      <strong>Cache hit ratio</strong>
      <div>Current DB: {{if gt .Res.CacheHitCurrent 0.0}}{{fmtF2 .Res.CacheHitCurrent}}%{{else}}<span class="muted">n/a</span>{{end}}</div>
      <div>Overall: {{if gt .Res.CacheHitOverall 0.0}}{{fmtF2 .Res.CacheHitOverall}}%{{else}}<span class="muted">n/a</span>{{end}}</div>
    </div>
    <div class="card info">
      <strong>Connections</strong>
      <div>Used: {{fmtInt .Res.TotalConnections}} / {{fmtInt .Res.ConnInfo.MaxConnections}}</div>
    </div>
  </div>

  <h3>Cache hit ratio by database</h3>
  <div class="table-wrap collapsed">
  <table>
    <thead><tr><th>Database</th><th>blks_hit</th><th>blks_read</th><th>Hit ratio (%)</th></tr></thead>
    <tbody>
    {{if .Res.CacheHits}}
      {{range .Res.CacheHits}}<tr><td>{{.Datname}}</td><td>{{fmtI64 .BlksHit}}</td><td>{{fmtI64 .BlksRead}}</td><td>{{fmtF2 .Ratio}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>
  {{if gt (len .Res.CacheHits) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

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
  {{if gt (len .Res.ConnectionsByClient) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}
  </div>

  {{if .Res.Extensions.PgStatStatements}}
  <h2>Top queries by total time</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>Calls</th><th>Total time (ms)</th><th>Mean time (ms)</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByTotalTime}}
      {{range .Res.Statements.TopByTotalTime}}<tr><td>{{fmtF0 .Calls}}</td><td>{{fmtF2 .TotalTime}}</td><td>{{fmtF2 .MeanTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByTotalTime) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Top queries by CPU (approx)</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>Calls</th><th>CPU time (ms)</th><th>Total time (ms)</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByCPU}}
      {{range .Res.Statements.TopByCPU}}<tr><td>{{fmtF0 .Calls}}</td><td>{{fmtF2 .CPUTime}}</td><td>{{fmtF2 .TotalTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByCPU) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Top queries by IO time</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>Calls</th><th>IO time (ms)</th><th>Read (ms)</th><th>Write (ms)</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByIO}}
      {{range .Res.Statements.TopByIO}}<tr><td>{{fmtF0 .Calls}}</td><td>{{fmtF2 .IOTime}}</td><td>{{fmtF2 .BlkReadTime}}</td><td>{{fmtF2 .BlkWriteTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="5" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByIO) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Top queries by calls</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>Calls</th><th>Total time (ms)</th><th>Mean time (ms)</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.Statements.TopByCalls}}
      {{range .Res.Statements.TopByCalls}}<tr><td>{{fmtF0 .Calls}}</td><td>{{fmtF2 .TotalTime}}</td><td>{{fmtF2 .MeanTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" class="muted">No data</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Statements.TopByCalls) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>
  {{else}}
  <p>pg_stat_statements is not enabled in this database. Install and preload it for detailed query insights.</p>
  {{end}}

  <h2>Blocking queries</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>DB</th><th>Blocked PID</th><th>Blocked for</th><th>Blocking PID</th><th>Blocking for</th><th>Blocked query</th><th>Blocking query</th></tr></thead>
    <tbody>
    {{if .Res.Blocking}}
      {{range .Res.Blocking}}<tr><td>{{.Datname}}</td><td>{{.BlockedPID}}</td><td>{{.BlockedDuration}}</td><td>{{.BlockingPID}}</td><td>{{.BlockingDuration}}</td><td><pre>{{.BlockedQuery}}</pre></td><td><pre>{{.BlockingQuery}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="7" class="muted">No blocking detected</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.Blocking) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Long running queries (> 5m)</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>DB</th><th>PID</th><th>Duration</th><th>State</th><th>Query</th></tr></thead>
    <tbody>
    {{if .Res.LongRunning}}
      {{range .Res.LongRunning}}<tr><td>{{.Datname}}</td><td>{{.PID}}</td><td>{{.Duration}}</td><td>{{.State}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="5" class="muted">No long running queries</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.LongRunning) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <h2>Autovacuum activities</h2>
  <div class="table-wrap collapsed"><table>
    <thead><tr><th>DB</th><th>PID</th><th>Relation</th><th>Phase</th><th>Scanned</th><th>Total</th></tr></thead>
    <tbody>
    {{if .Res.AutoVacuum}}
      {{range .Res.AutoVacuum}}<tr><td>{{.Datname}}</td><td>{{.PID}}</td><td>{{.Relation}}</td><td>{{.Phase}}</td><td>{{fmtI64 .Scanned}}</td><td>{{fmtI64 .Total}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="6" class="muted">No autovacuum workers</td></tr>
    {{end}}
    </tbody>
  </table>{{if gt (len .Res.AutoVacuum) 10}}<div class="table-tools"><button class="toggle-rows">Show all</button></div>{{end}}</div>

  <footer style="margin-top:24px;color:#6b7280">Report generated at {{.Meta.StartedAt}} in {{.Meta.Duration}}</footer>

  <script>
  (function(){
    document.addEventListener('click', function(e){
      if(e.target && e.target.classList.contains('toggle-rows')){
        var wrap = e.target.closest('.table-wrap');
        if(!wrap) return;
        wrap.classList.toggle('collapsed');
        e.target.textContent = wrap.classList.contains('collapsed') ? 'Show all' : 'Show less';
      }
    });
  })();
  </script>
</body>
</html>`
