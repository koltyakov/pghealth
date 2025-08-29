package collect

import "time"

type Config struct {
	URL     string
	Timeout time.Duration
}

type Meta struct {
	StartedAt time.Time
	Duration  time.Duration
	Version   string
}
