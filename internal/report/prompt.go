package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/koltyakov/pghealth/internal/collect"
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

// WritePrompt writes a sidecar .prompt.txt next to the HTML report with actual stats for LLM analysis.
func WritePrompt(htmlOutPath string, res collect.Result, meta collect.Meta) (string, error) {
	if htmlOutPath == "-" || strings.TrimSpace(htmlOutPath) == "" {
		return "", nil // nothing to do for stdout
	}
	base := strings.TrimSuffix(htmlOutPath, filepath.Ext(htmlOutPath))
	promptPath := base + ".prompt.txt"

	// Build data payload
	pd := promptData{}

	// Queries: include those with collected advice/plans from TopByTotalTime and TopByCalls
	// Truncate extremely long query texts and plans to keep the prompt manageable
	const maxQueryTextLen = 8000
	const maxPlanLen = 20000
	trimLong := func(s string, max int) string {
		s = strings.TrimSpace(s)
		if max > 0 && len(s) > max {
			return s[:max] + " … [truncated]"
		}
		return s
	}
	// Add a query to the payload if it isn't obviously already fast and unproblematic
	addQuery := func(s collect.Statement) {
		if s.Advice == nil && s.MeanTime > 0 && s.MeanTime < 2.0 {
			return // skip obviously fast queries to reduce prompt size
		}
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
	// Include top queries by total time and by calls; dedupe on exact text
	seenQ := make(map[string]struct{})
	queriesAdded := 0
	for _, s := range res.Statements.TopByTotalTime {
		qt := strings.TrimSpace(s.Query)
		if qt == "" {
			continue
		}
		if _, ok := seenQ[qt]; ok {
			continue
		}
		addQuery(s)
		if _, ok := seenQ[qt]; !ok {
			seenQ[qt] = struct{}{}
		}
		if len(pd.Queries) > queriesAdded {
			queriesAdded++
		}
		if queriesAdded >= 25 {
			break
		}
	}
	for _, s := range res.Statements.TopByCalls {
		qt := strings.TrimSpace(s.Query)
		if qt == "" {
			continue
		}
		if _, ok := seenQ[qt]; ok {
			continue
		}
		addQuery(s)
		if _, ok := seenQ[qt]; !ok {
			seenQ[qt] = struct{}{}
		}
		if len(pd.Queries) > queriesAdded {
			queriesAdded++
		}
		if queriesAdded >= 25 {
			break
		}
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

	// Build DB->Schema->Tables with indexes DDL (ignore small tables unless referenced)
	const minTableSizeForIndexes int64 = 128 * 1024 * 1024 // 128MB stricter threshold
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
	shouldIncludeIdx := func(schema, table string, size int64) bool {
		if size >= minTableSizeForIndexes {
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
			pt := promptTable{Name: t.Name, SizeBytes: t.SizeBytes, BloatPct: t.BloatPct, RowCount: t.RowCount, DeadRows: t.DeadRows}
			if shouldIncludeIdx(t.Schema, t.Name, t.SizeBytes) {
				key := strings.ToLower(t.Schema + "." + t.Name)
				pt.Indexes = append(pt.Indexes, idxDDL[key]...)
			}
			byDB[dbName][t.Schema] = append(byDB[dbName][t.Schema], pt)
		}
	} else {
		for _, t := range res.Tables {
			dbName := valueOr(res.ConnInfo.CurrentDB, t.Database)
			if byDB[dbName] == nil {
				byDB[dbName] = map[string][]promptTable{}
			}
			pt := promptTable{Name: t.Name, SizeBytes: t.SizeBytes, BloatPct: t.BloatPct, RowCount: t.NLiveTup, DeadRows: t.NDeadTup}
			if shouldIncludeIdx(t.Schema, t.Name, t.SizeBytes) {
				key := strings.ToLower(t.Schema + "." + t.Name)
				pt.Indexes = append(pt.Indexes, idxDDL[key]...)
			}
			byDB[dbName][t.Schema] = append(byDB[dbName][t.Schema], pt)
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
