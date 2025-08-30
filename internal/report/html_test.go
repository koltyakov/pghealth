package report

import (
	"path/filepath"
	"testing"

	"github.com/koltyakov/pghealth/internal/analyze"
	"github.com/koltyakov/pghealth/internal/collect"
)

// TestTemplateExec ensures the embedded template parses and executes with empty data.
func TestTemplateExec(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "report.html")

	var res collect.Result
	var a analyze.Analysis
	var meta collect.Meta

	if err := WriteHTML(out, res, a, meta); err != nil {
		t.Fatalf("WriteHTML failed: %v", err)
	}
}
