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
  - `--out` (default `report.html`). Supports `{ts}` placeholder for a timestamp, e.g. `--out report-{ts}.html`.
  - `--timeout` (default `30s`).
  - `--stats` (e.g. `24h`, `7d`) to filter `pg_stat_statements` data since a duration.
  - `--open` (default `true`) to open the report after generation.
  - `--suppress` to hide specific recommendation codes (comma-separated), e.g. `--suppress missing-extensions,low-cache-hit`.
  - `--dbs` to include additional databases for tables/indexes metrics (comma-separated). Example: `--dbs db1,db2`.

Notes on multi-DB mode:
- When `--dbs` is provided, table and index sections aggregate and show a conditional "Database" column.
- "Top queries" (pg_stat_statements) remain scoped to the current database only.
- Installed extensions are listed per database when multiple DBs are collected.

## Build

Go 1.21+ is required.

```sh
# Build the binary
go build

# Run with a connection string
./pghealth --url "postgres://user:pass@host/db"

# Run with a time window for query stats
./pghealth --url "postgres://user:pass@host/db" --stats 24h

# Multi-DB metrics (tables/indexes across db1 and db2)
./pghealth --url "postgres://user:pass@host/defaultdb" --dbs db1,db2 --out report-{ts}.html

# Suppress specific recommendations by code
./pghealth --url "$PGURL" --suppress missing-extensions,low-index-usage
```

## Notes

- Some checks require pg_monitor or superuser; the tool attempts queries opportunistically and continues when blocked.
- Missing-index and bloat heuristics are approximations; validate before acting.

## Report UI tips

- Tables show first 10 rows by default; click "Show all" to expand/collapse.
- SQL in "Top queries" is truncated; use "Show full" to expand/collapse.
- When a plan is available, click "Show plan" to toggle it.
- Sizes are humanized (KB/MB/GB/TB); durations use compact units (ms/s/m/h/d).

If buttons don't respond when opening the report as a local file on macOS (Safari/Chrome), try:
- Opening the report in Chrome, or
- Serving the file over a simple local HTTP server.

Example local server:
```sh
python3 -m http.server 8000
# then open http://localhost:8000/report.html
```