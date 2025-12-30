package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/koltyakov/pghealth/internal/collect"
)

// Prompt generation constants.
const (
	// maxQueryTextLen is the maximum length for query text in prompts.
	maxQueryTextLen = 8000

	// maxPlanLen is the maximum length for execution plan text in prompts.
	maxPlanLen = 20000

	// minTableRows is the minimum row count for a table to be included in prompts.
	minTableRows int64 = 100_000

	// promptFileSuffix is the file extension for prompt sidecar files.
	promptFileSuffix = ".prompt.txt"

	// promptFilePerms is the file permissions for prompt files.
	promptFilePerms = 0o644
)

// promptData is a minimal schema we export for LLM consumption.
type promptData struct {
	Queries       []promptQuery         `json:"queries"`
	DBs           []promptDB            `json:"db"`
	UnusedIndexes []collect.IndexUnused `json:"unused_indexes,omitempty"`
}

type promptQuery struct {
	Text      string  `json:"text"`
	TotalTime float64 `json:"total_time,omitempty"`
	Calls     float64 `json:"calls,omitempty"`
	MeanTime  float64 `json:"mean_time,omitempty"`
	Rows      float64 `json:"rows,omitempty"`
	Plan      string  `json:"plan,omitempty"`
}

type promptTable struct {
	Name      string   `json:"name"`
	SizeBytes int64    `json:"size_bytes"`
	BloatPct  float64  `json:"bloat_pct,omitempty"`
	RowCount  int64    `json:"n_live_tup,omitempty"`
	DeadRows  int64    `json:"n_dead_tup,omitempty"`
	Indexes   []string `json:"indexes,omitempty"`
}

type promptDB struct {
	Name    string         `json:"name"`
	Schemas []promptSchema `json:"schemas"`
}

type promptSchema struct {
	Name   string        `json:"name"`
	Tables []promptTable `json:"tables"`
}

// WritePrompt generates an LLM-friendly prompt file alongside the HTML report.
// The prompt contains structured JSON data about top queries, schema information,
// and unused indexes to facilitate automated performance analysis.
//
// Returns the path to the generated prompt file, or empty string if no prompt
// was generated (e.g., for stdout output).
func WritePrompt(htmlOutPath string, res collect.Result, meta collect.Meta) (string, error) {
	if htmlOutPath == "-" || strings.TrimSpace(htmlOutPath) == "" {
		return "", nil // nothing to do for stdout
	}

	base := strings.TrimSuffix(htmlOutPath, filepath.Ext(htmlOutPath))
	promptPath := base + promptFileSuffix

	// Build data payload
	pd := promptData{}

	// Queries: include those from TopByTotalTime and TopByCalls (deduped)
	// Truncate extremely long query texts and plans to keep the prompt manageable
	trimLong := func(s string, max int) string {
		s = strings.TrimSpace(s)
		if max > 0 && len(s) > max {
			return s[:max] + " … [truncated]"
		}
		return s
	}
	// Add a query to the payload
	addQuery := func(s collect.Statement) {
		pq := promptQuery{
			Text:      trimLong(s.Query, maxQueryTextLen),
			TotalTime: s.TotalTime,
			Calls:     s.Calls,
			MeanTime:  s.MeanTime,
			Rows:      s.Rows,
		}
		if s.Advice != nil {
			pq.Plan = trimLong(s.Advice.Plan, maxPlanLen)
		}
		pd.Queries = append(pd.Queries, pq)
	}
	// Build a unified, deduped list across top-by-time and top-by-calls
	type qwrap struct{ s collect.Statement }
	uniq := map[string]qwrap{}
	insertOrPromote := func(s collect.Statement) {
		qt := strings.TrimSpace(s.Query)
		if qt == "" {
			return
		}
		if existing, ok := uniq[qt]; ok {
			// prefer one with advice; otherwise higher total time, then higher calls
			if (s.Advice != nil && existing.s.Advice == nil) ||
				(existing.s.Advice != nil && s.Advice != nil && s.TotalTime > existing.s.TotalTime) ||
				(existing.s.Advice == nil && s.TotalTime > existing.s.TotalTime) ||
				(s.TotalTime == existing.s.TotalTime && s.Calls > existing.s.Calls) {
				uniq[qt] = qwrap{s: s}
			}
			return
		}
		uniq[qt] = qwrap{s: s}
	}
	for _, s := range res.Statements.TopByTotalTime {
		insertOrPromote(s)
	}
	for _, s := range res.Statements.TopByCalls {
		insertOrPromote(s)
	}
	// Convert to slice and sort: NeedsAttention first, then by TotalTime desc, then Calls desc
	list := make([]collect.Statement, 0, len(uniq))
	for _, w := range uniq {
		list = append(list, w.s)
	}
	// custom sort
	sort.SliceStable(list, func(i, j int) bool {
		ai, aj := list[i], list[j]
		if ai.NeedsAttention != aj.NeedsAttention {
			return ai.NeedsAttention && !aj.NeedsAttention
		}
		if ai.TotalTime != aj.TotalTime {
			return ai.TotalTime > aj.TotalTime
		}
		return ai.Calls > aj.Calls
	})
	// Add all (no artificial cap)
	for _, s := range list {
		addQuery(s)
	}

	// Build set of relevant tables from included queries' plans/highlights
	relevantTables := map[string]struct{}{}
	// heuristic regex focused on problematic nodes (Seq Scans and Parallel Seq Scans, Bitmap Heap Scans)
	reOn := regexp.MustCompile(`(?i)\b(?:Seq Scan|Bitmap Heap Scan|Parallel Seq Scan) on ([A-Za-z0-9_\.\"]+)`)
	for _, q := range pd.Queries {
		// From plan text
		for _, m := range reOn.FindAllStringSubmatch(q.Plan, -1) {
			if len(m) >= 2 {
				name := strings.Trim(m[1], "\"")
				if name != "" {
					relevantTables[strings.ToLower(name)] = struct{}{}
				}
			}
		}
	}

	// Build DB->Schema->Tables with indexes DDL
	// Include tables only if large-by-rows OR referenced in top query plans
	// map schema.table -> []DDL (deduped)
	idxDDL := map[string][]string{}
	seenDDL := map[string]struct{}{}
	for _, idx := range res.Indexes {
		key := strings.ToLower(idx.Schema + "." + idx.Table)
		ddl := strings.TrimSpace(idx.DDL)
		if ddl == "" {
			continue
		}
		k2 := key + "|" + ddl
		if _, ok := seenDDL[k2]; ok {
			continue
		}
		idxDDL[key] = append(idxDDL[key], ddl)
		seenDDL[k2] = struct{}{}
	}
	shouldIncludeTable := func(schema, table string, rowCount int64) bool {
		if rowCount >= minTableRows {
			return true
		}
		if _, ok := relevantTables[strings.ToLower(schema+"."+table)]; ok {
			return true
		}
		if _, ok := relevantTables[strings.ToLower(table)]; ok {
			return true
		}
		return false
	}
	byDB := map[string]map[string][]promptTable{} // db -> schema -> tables
	if len(res.TablesWithIndexCount) > 0 {
		for _, t := range res.TablesWithIndexCount {
			dbName := valueOr(res.ConnInfo.CurrentDB, t.Database)
			if byDB[dbName] == nil {
				byDB[dbName] = map[string][]promptTable{}
			}
			if shouldIncludeTable(t.Schema, t.Name, t.RowCount) {
				pt := promptTable{Name: t.Name, SizeBytes: t.SizeBytes, BloatPct: t.BloatPct, RowCount: t.RowCount, DeadRows: t.DeadRows}
				key := strings.ToLower(t.Schema + "." + t.Name)
				pt.Indexes = append(pt.Indexes, idxDDL[key]...)
				byDB[dbName][t.Schema] = append(byDB[dbName][t.Schema], pt)
			}
		}
	} else {
		for _, t := range res.Tables {
			dbName := valueOr(res.ConnInfo.CurrentDB, t.Database)
			if byDB[dbName] == nil {
				byDB[dbName] = map[string][]promptTable{}
			}
			if shouldIncludeTable(t.Schema, t.Name, t.NLiveTup) {
				pt := promptTable{Name: t.Name, SizeBytes: t.SizeBytes, BloatPct: t.BloatPct, RowCount: t.NLiveTup, DeadRows: t.NDeadTup}
				key := strings.ToLower(t.Schema + "." + t.Name)
				pt.Indexes = append(pt.Indexes, idxDDL[key]...)
				byDB[dbName][t.Schema] = append(byDB[dbName][t.Schema], pt)
			}
		}
	}
	// materialize hierarchy
	for dbName, schemas := range byDB {
		pdb := promptDB{Name: dbName}
		for schemaName, tables := range schemas {
			pdb.Schemas = append(pdb.Schemas, promptSchema{Name: schemaName, Tables: tables})
		}
		pd.DBs = append(pd.DBs, pdb)
	}

	// Unused indexes (already unified upstream)
	pd.UnusedIndexes = append(pd.UnusedIndexes, res.IndexUnused...)

	payload, err := json.MarshalIndent(pd, "", "  ")
	if err != nil {
		return "", err
	}

	// Compose final prompt with instructions and payload
	var b strings.Builder
	b.WriteString("PostgreSQL performance tuning assistant – environment-specific prompt\n\n")
	b.WriteString("Role\nYou are a senior PostgreSQL performance engineer. Using the provided inputs from a pghealth report, produce concrete, safe, and prioritized recommendations. Prefer specific DDL and query rewrites over general advice. Avoid duplicate/unnecessary indexes. Call out risks and validation steps.\n\n")
	b.WriteString("Output sections: Summary; Index proposals (prioritized with DDL); Unused/redundant indexes; Query improvements; Maintenance plan; Appendix (assumptions).\n\n")
	b.WriteString("Constraints: No more than 8 new indexes unless necessary. Never drop PK/UNIQUE/constraint-backed indexes. Provide validation via EXPLAIN ANALYZE, BUFFERS on staging.\n\n")
	b.WriteString("INPUT START\n")
	b.Write(payload)
	b.WriteString("\nINPUT END\n")

	if err := os.WriteFile(promptPath, []byte(b.String()), 0o644); err != nil {
		return "", fmt.Errorf("write prompt: %w", err)
	}
	return promptPath, nil
}

func valueOr(primary, alt string) string {
	alt = strings.TrimSpace(alt)
	if alt != "" {
		return alt
	}
	return strings.TrimSpace(primary)
}
