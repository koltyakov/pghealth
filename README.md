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
  - `--out` (default pghealth_report.html)
  - `--timeout` (default 30s)

## Build

Go 1.21+

```sh
go build ./cmd/pghealth
./pghealth --url postgres://postgres:secret@localhost:5432/postgres?sslmode=prefer --out report.html
## or with env
export DATABASE_URL=postgres://postgres:secret@localhost:5432/postgres?sslmode=prefer
./pghealth --out report.html
```

## Notes

- Some checks require pg_monitor or superuser; the tool attempts queries opportunistically and continues when blocked.
- Missing-index and bloat heuristics are approximations; validate before acting.