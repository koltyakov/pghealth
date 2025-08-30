# pghealth

Lightweight PostgreSQL health and AWR-like HTML report generator.

- Works without superuser; gracefully degrades with limited privileges.
- Uses pg_stat_statements when available; recommends installing when not.
- Provides best-practice checks, connection stats, table/index hygiene, and query insights.

## Usage

- Connection string only. Provide via:
  - `--url` flag, e.g. `--url postgres://user:pass@host:5432/db?sslmode=require`
  - or env: `PGURL` or `DATABASE_URL`
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

## Installation (clone and build)

Go 1.21+ is required.

Clone the repo and build locally:

```sh
git clone https://github.com/koltyakov/pghealth.git
cd pghealth

# Option A: quick build
go build -o pghealth

# Option B: via Makefile (adds version from git)
make build

# Cross-compile binaries for common platforms
make build-all
```

Run the tool:

```sh
./bin/pghealth --url "postgres://user:pass@host:5432/db?sslmode=require" --out report-{ts}.html
# or when built without make:
./pghealth --url "$PGURL" --out report-{ts}.html
```

## Notes

- Some checks require pg_monitor or superuser; the tool attempts queries opportunistically and continues when blocked.
- Missing-index and bloat heuristics are approximations; validate before acting.
