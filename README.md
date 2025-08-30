# pghealth

Lightweight PostgreSQL health and AWR-like HTML report generator.

- Works without superuser; gracefully degrades with limited privileges.
- Uses pg_stat_statements when available; recommends installing when not.
- Provides best-practice checks, connection stats, table/index hygiene, and query insights.

## Usage

- Connection string only. Provide via:
  - `--url` flag, e.g. `--url postgres://user:pass@host:5432/db?sslmode=require`
  - or env: `PGURL` or `DATABASE_URL`
  - or as the first positional arg
- Other flags:
  - `--out` (default `report.html`)
  - `--timeout` (default `30s`)
  - `--stats` (e.g. `'24h'`, `'7d'`) to filter `pg_stat_statements` data since a duration
  - `--open` (default `true`) to open the report after generation

## Build

Go 1.21+ is required.

```sh
# Build the binary
go build

# Run with a connection string
./pghealth --url "postgres://user:pass@host/db"

# Run with a time window for query stats
./pghealth --url "postgres://user:pass@host/db" --stats 24h
```

## Notes

- Some checks require pg_monitor or superuser; the tool attempts queries opportunistically and continues when blocked.
- Missing-index and bloat heuristics are approximations; validate before acting.