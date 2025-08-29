package analyze

import (
	"testing"

	"github.com/koltyakov/pghealth/internal/collect"
)

func TestRecommendationsWhenNoPSS(t *testing.T) {
	res := collect.Result{}
	a := Run(res)
	found := false
	for _, f := range a.Recommendations {
		if f.Title == "Install pg_stat_statements" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected recommendation to install pg_stat_statements")
	}
}
