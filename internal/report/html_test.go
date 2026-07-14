package report

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat report: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("report permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestWriteHTMLDoesNotReorderInput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "report.html")
	res := collect.Result{DBs: []collect.Database{{Name: "small", SizeBytes: 1}, {Name: "large", SizeBytes: 2}}}
	if err := WriteHTML(out, res, analyze.Analysis{}, collect.Meta{}); err != nil {
		t.Fatalf("WriteHTML failed: %v", err)
	}
	if res.DBs[0].Name != "small" {
		t.Fatalf("WriteHTML reordered caller data: %#v", res.DBs)
	}
}

func TestWriteHTMLShowsMonitoringPermissions(t *testing.T) {
	out := filepath.Join(t.TempDir(), "report.html")
	res := collect.Result{Permissions: []collect.PermissionCheck{{
		Name:      "pg_read_all_stats",
		Available: true,
		Granted:   false,
		Impact:    "Other sessions' SQL text and full statistics",
	}}}
	if err := WriteHTML(out, res, analyze.Analysis{}, collect.Meta{}); err != nil {
		t.Fatalf("WriteHTML failed: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	html := string(b)
	if !strings.Contains(html, "Monitoring permissions") || !strings.Contains(html, "pg_read_all_stats") {
		t.Fatal("monitoring permission coverage was not rendered")
	}
}

func TestAtomicWritePreservesExistingFileOnRenderError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.html")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("render failed")
	err := writeFileAtomic(path, 0o600, func(_ io.Writer) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("writeFileAtomic error = %v, want %v", err, wantErr)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "existing" {
		t.Fatalf("existing file changed to %q", got)
	}
}
