package collect

import "time"

type Config struct {
	URL        string
	Timeout    time.Duration
	StatsSince string   `json:"stats_since" yaml:"stats_since"`
	DBs        []string `json:"dbs" yaml:"dbs"`
	ExplainTop int      `json:"explain_top" yaml:"explain_top"`
}

type Meta struct {
	StartedAt time.Time
	Duration  time.Duration
	Version   string
}
