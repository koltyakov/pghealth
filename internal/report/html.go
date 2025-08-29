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
			return template.HTMLEscapeString((func() string { return fmtFloat(f) + " " + units[i] })())
		},
		"fmtMB": func(b int64) string {
			return fmtFloat(float64(b) / 1024.0 / 1024.0)
		},
	}).Parse(htmlTemplate))
	data := struct {
		Res  collect.Result
		A    analyze.Analysis
		Meta collect.Meta
	}{Res: res, A: a, Meta: meta}
	return tmpl.Execute(f, data)
}

func fmtFloat(f float64) string {
	// strip trailing zeros
	s := (func() string { return strconv.FormatFloat(f, 'f', 2, 64) })()
	for len(s) > 0 && s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '.' {
		s = s[:len(s)-1]
	}
	return s
}

const htmlTemplate = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>pghealth report</title>
  <style>
    body{font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin:24px;}
    header{margin-bottom:16px}
    h1{font-size:20px;margin:0}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
    .card{border:1px solid #ddd;border-radius:8px;padding:12px;background:#fff}
    .warn{border-left:4px solid #f59e0b}
    .rec{border-left:4px solid #10b981}
    .info{border-left:4px solid #3b82f6}
  table{border-collapse:collapse;width:100%;border:1px solid #ddd;border-radius:6px;overflow:hidden}
  th,td{border:1px solid #eee;padding:8px 10px;text-align:left;vertical-align:top}
  th{background:#fafafa}
    code{background:#f3f4f6;padding:2px 4px;border-radius:4px}
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
  <table>
    <tr><th>Database</th><th>State</th><th>Count</th></tr>
    {{if .Res.Activity}}
      {{range .Res.Activity}}<tr><td>{{.Datname}}</td><td>{{.State}}</td><td>{{.Count}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="3" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Databases</h2>
  <table>
    <tr><th>Name</th><th>Size, Mb</th><th>Tablespace</th><th>Connections</th></tr>
    {{if .Res.DBs}}
      {{range .Res.DBs}}<tr><td>{{.Name}}</td><td>{{fmtMB .SizeBytes}}</td><td>{{.Tablespaces}}</td><td>{{.ConnCount}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Settings (subset)</h2>
  <table>
    <tr><th>Name</th><th>Value</th><th>Unit</th><th>Source</th></tr>
    {{if .Res.Settings}}
      {{range .Res.Settings}}<tr><td><code>{{.Name}}</code></td><td>{{.Val}}</td><td>{{.Unit}}</td><td>{{.Source}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Indexes (unused candidates)</h2>
  <table>
    <tr><th>Schema</th><th>Table</th><th>Index</th><th>Size, Mb</th></tr>
    {{if .Res.IndexUnused}}
      {{range .Res.IndexUnused}}<tr><td>{{.Schema}}</td><td>{{.Table}}</td><td>{{.Name}}</td><td>{{fmtMB .SizeBytes}}</td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  {{if .Res.Statements.Available}}
  <h2>Top queries by total time</h2>
  <table>
    <tr><th>Calls</th><th>Total time (ms)</th><th>Mean time (ms)</th><th>Query</th></tr>
    {{if .Res.Statements.TopByTotalTime}}
      {{range .Res.Statements.TopByTotalTime}}<tr><td>{{printf "%.0f" .Calls}}</td><td>{{printf "%.2f" .TotalTime}}</td><td>{{printf "%.2f" .MeanTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Top queries by CPU (approx)</h2>
  <table>
    <tr><th>Calls</th><th>CPU time (ms)</th><th>Total time (ms)</th><th>Query</th></tr>
    {{if .Res.Statements.TopByCPU}}
      {{range .Res.Statements.TopByCPU}}<tr><td>{{printf "%.0f" .Calls}}</td><td>{{printf "%.2f" .CPUTime}}</td><td>{{printf "%.2f" .TotalTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Top queries by IO time</h2>
  <table>
    <tr><th>Calls</th><th>IO time (ms)</th><th>Read (ms)</th><th>Write (ms)</th><th>Query</th></tr>
    {{if .Res.Statements.TopByIO}}
      {{range .Res.Statements.TopByIO}}<tr><td>{{printf "%.0f" .Calls}}</td><td>{{printf "%.2f" .IOTime}}</td><td>{{printf "%.2f" .BlkReadTime}}</td><td>{{printf "%.2f" .BlkWriteTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="5" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>

  <h2>Top queries by calls</h2>
  <table>
    <tr><th>Calls</th><th>Total time (ms)</th><th>Mean time (ms)</th><th>Query</th></tr>
    {{if .Res.Statements.TopByCalls}}
      {{range .Res.Statements.TopByCalls}}<tr><td>{{printf "%.0f" .Calls}}</td><td>{{printf "%.2f" .TotalTime}}</td><td>{{printf "%.2f" .MeanTime}}</td><td><pre>{{.Query}}</pre></td></tr>{{end}}
    {{else}}
      <tr><td colspan="4" style="color:#6b7280">No data</td></tr>
    {{end}}
  </table>
  {{else}}
  <p>pg_stat_statements not available. Install it for detailed query insights.</p>
  {{end}}

  <footer style="margin-top:24px;color:#6b7280">Report generated at {{.Meta.StartedAt}} in {{.Meta.Duration}}</footer>
</body>
</html>`
