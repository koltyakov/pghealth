package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/koltyakov/pghealth/internal/collect"
)

func TestWritePromptScopesIndexesByDatabase(t *testing.T) {
	dir := t.TempDir()
	htmlPath := filepath.Join(dir, "report.html")
	res := collect.Result{
		ConnInfo: collect.ConnInfo{CurrentDB: "db1"},
		TablesWithIndexCount: []collect.TableIndexCount{
			{Database: "db1", Schema: "public", Name: "users", RowCount: minTableRows},
			{Database: "db2", Schema: "public", Name: "users", RowCount: minTableRows},
		},
		Indexes: []collect.IndexStat{
			{Database: "db1", Schema: "public", Table: "users", DDL: "CREATE INDEX db1_users_idx ON public.users (email)"},
			{Database: "db2", Schema: "public", Table: "users", DDL: "CREATE INDEX db2_users_idx ON public.users (name)"},
		},
	}

	promptPath, err := WritePrompt(htmlPath, res, collect.Meta{})
	if err != nil {
		t.Fatalf("WritePrompt failed: %v", err)
	}
	info, err := os.Stat(promptPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("prompt permissions = %o, want 600", info.Mode().Perm())
	}

	pd := readPromptData(t, promptPath)
	if len(pd.DBs) != 2 {
		t.Fatalf("got %d databases, want 2", len(pd.DBs))
	}
	for _, db := range pd.DBs {
		indexes := db.Schemas[0].Tables[0].Indexes
		if len(indexes) != 1 || !strings.Contains(indexes[0], db.Name+"_users_idx") {
			t.Errorf("database %s indexes = %v", db.Name, indexes)
		}
	}
}

func TestWritePromptKeepsPlanWhenDeduplicating(t *testing.T) {
	dir := t.TempDir()
	htmlPath := filepath.Join(dir, "report.html")
	res := collect.Result{Statements: collect.Statements{
		TopByTotalTime: []collect.Statement{{Query: "SELECT 1", TotalTime: 10, Calls: 1, Advice: &collect.PlanAdvice{Plan: "Result"}}},
		TopByCalls:     []collect.Statement{{Query: "SELECT 1", TotalTime: 10, Calls: 100}},
	}}
	promptPath, err := WritePrompt(htmlPath, res, collect.Meta{})
	if err != nil {
		t.Fatalf("WritePrompt failed: %v", err)
	}
	pd := readPromptData(t, promptPath)
	if len(pd.Queries) != 1 || pd.Queries[0].Plan != "Result" {
		t.Fatalf("deduplicated queries = %#v", pd.Queries)
	}
}

func readPromptData(t *testing.T, path string) promptData {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	start := strings.Index(s, "INPUT START\n")
	end := strings.Index(s, "\nINPUT END")
	if start < 0 || end < 0 {
		t.Fatalf("prompt payload markers missing")
	}
	start += len("INPUT START\n")
	var pd promptData
	if err := json.Unmarshal([]byte(s[start:end]), &pd); err != nil {
		t.Fatalf("decode prompt data: %v", err)
	}
	return pd
}
