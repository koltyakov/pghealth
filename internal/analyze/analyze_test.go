package analyze

import (
	"testing"
	"time"

	"github.com/koltyakov/pghealth/internal/collect"
)

// TestRecommendationsWhenNoPSS verifies that pg_stat_statements installation
// is recommended when the extension is not present.
func TestRecommendationsWhenNoPSS(t *testing.T) {
	res := collect.Result{}
	a := Run(res)
	found := false
	for _, f := range a.Recommendations {
		if f.Title == "Install pg_stat_statements" {
			found = true
			if f.Code != "install-pgss" {
				t.Errorf("expected code 'install-pgss', got %q", f.Code)
			}
			if f.Severity != SeverityRec {
				t.Errorf("expected severity %q, got %q", SeverityRec, f.Severity)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected recommendation to install pg_stat_statements")
	}
}

// TestLowCacheHitWarning verifies that low cache hit ratio triggers a warning.
func TestLowCacheHitWarning(t *testing.T) {
	tests := []struct {
		name            string
		cacheHit        float64
		expectWarning   bool
		expectInfoCount int
	}{
		{"very low cache hit", 50.0, true, 0},
		{"borderline cache hit", 94.9, true, 0},
		{"acceptable cache hit", 95.0, false, 1},
		{"excellent cache hit", 99.5, false, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := collect.Result{
				CacheHitCurrent: tt.cacheHit,
				Extensions:      collect.Extensions{PgStatStatements: true}, // Skip PSS recommendation
			}
			a := Run(res)

			foundWarning := false
			for _, w := range a.Warnings {
				if w.Title == "Low cache hit ratio (current DB)" {
					foundWarning = true
					break
				}
			}

			if foundWarning != tt.expectWarning {
				t.Errorf("cache hit %.1f%%: expected warning=%v, got %v", tt.cacheHit, tt.expectWarning, foundWarning)
			}
		})
	}
}

// TestConnectionUsageWarning verifies connection usage warnings.
func TestConnectionUsageWarning(t *testing.T) {
	tests := []struct {
		name          string
		total         int
		max           int
		expectWarning bool
	}{
		{"low usage", 10, 100, false},
		{"moderate usage", 70, 100, false},
		{"high usage", 80, 100, true},
		{"critical usage", 95, 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := collect.Result{
				TotalConnections: tt.total,
				ConnInfo: collect.ConnInfo{
					MaxConnections: tt.max,
				},
				Extensions: collect.Extensions{PgStatStatements: true},
			}
			a := Run(res)

			foundWarning := false
			for _, w := range a.Warnings {
				if w.Title == "High connection usage" {
					foundWarning = true
					break
				}
			}

			if foundWarning != tt.expectWarning {
				t.Errorf("connections %d/%d: expected warning=%v, got %v",
					tt.total, tt.max, tt.expectWarning, foundWarning)
			}
		})
	}
}

// TestBlockingDetection verifies that blocking queries are detected.
func TestBlockingDetection(t *testing.T) {
	res := collect.Result{
		Blocking: []collect.Blocking{
			{BlockedPID: 1, BlockingPID: 2, BlockedQuery: "SELECT 1"},
		},
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	foundWarning := false
	for _, w := range a.Warnings {
		if w.Title == "Blocking detected" {
			foundWarning = true
			break
		}
	}

	if !foundWarning {
		t.Error("expected warning for blocking queries")
	}
}

// TestLongRunningQueries verifies long-running query detection.
func TestLongRunningQueries(t *testing.T) {
	res := collect.Result{
		LongRunning: []collect.LongQuery{
			{PID: 1, Duration: "10:00:00", Query: "SELECT * FROM large_table"},
		},
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	foundRec := false
	for _, r := range a.Recommendations {
		if r.Title == "Long-running queries" {
			foundRec = true
			if r.Code != "long-running" {
				t.Errorf("expected code 'long-running', got %q", r.Code)
			}
			break
		}
	}

	if !foundRec {
		t.Error("expected recommendation for long-running queries")
	}
}

// TestUptimeInfo verifies server uptime information is reported.
func TestUptimeInfo(t *testing.T) {
	startTime := time.Now().Add(-24 * time.Hour)
	res := collect.Result{
		ConnInfo: collect.ConnInfo{
			StartTime: startTime,
		},
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	foundInfo := false
	for _, i := range a.Infos {
		if i.Title == "Server uptime" {
			foundInfo = true
			if i.Severity != SeverityInfo {
				t.Errorf("expected severity %q, got %q", SeverityInfo, i.Severity)
			}
			break
		}
	}

	if !foundInfo {
		t.Error("expected info for server uptime")
	}
}

// TestTableBloatWarning verifies table bloat detection.
func TestTableBloatWarning(t *testing.T) {
	res := collect.Result{
		Tables: []collect.TableStat{
			{Schema: "public", Name: "users", BloatPct: 25.0, NLiveTup: 50000, NDeadTup: 15000},
			{Schema: "public", Name: "orders", BloatPct: 30.0, NLiveTup: 100000, NDeadTup: 40000},
		},
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	foundWarning := false
	for _, w := range a.Warnings {
		if w.Title == "Potential table bloat (heuristic)" {
			foundWarning = true
			break
		}
	}

	if !foundWarning {
		t.Error("expected warning for table bloat")
	}
}

// TestAnalysisInitialization verifies that Analysis slices are properly initialized.
func TestAnalysisInitialization(t *testing.T) {
	res := collect.Result{
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	if a.Recommendations == nil {
		t.Error("Recommendations slice should not be nil")
	}
	if a.Warnings == nil {
		t.Error("Warnings slice should not be nil")
	}
	if a.Infos == nil {
		t.Error("Infos slice should not be nil")
	}
}

// TestHighConnectionsRecommendation verifies high max_connections recommendation.
func TestHighConnectionsRecommendation(t *testing.T) {
	res := collect.Result{
		ConnInfo: collect.ConnInfo{
			MaxConnections: 200,
		},
		Extensions: collect.Extensions{PgStatStatements: true},
	}
	a := Run(res)

	foundRec := false
	for _, r := range a.Recommendations {
		if r.Code == "high-max-connections" {
			foundRec = true
			break
		}
	}

	if !foundRec {
		t.Error("expected recommendation for high max_connections")
	}
}

// BenchmarkRun benchmarks the analysis function with typical data.
func BenchmarkRun(b *testing.B) {
	res := collect.Result{
		CacheHitCurrent:  95.5,
		CacheHitOverall:  96.0,
		TotalConnections: 50,
		ConnInfo: collect.ConnInfo{
			MaxConnections: 100,
			StartTime:      time.Now().Add(-24 * time.Hour),
		},
		Tables: make([]collect.TableStat, 100),
		Extensions: collect.Extensions{
			PgStatStatements: true,
		},
	}

	// Initialize tables with some data
	for i := 0; i < 100; i++ {
		res.Tables[i] = collect.TableStat{
			Schema:   "public",
			Name:     "table_" + string(rune('a'+i%26)),
			NLiveTup: int64(i * 1000),
			NDeadTup: int64(i * 100),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Run(res)
	}
}
