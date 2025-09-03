# pghealth

Lightweight PostgreSQL health and AWR-like HTML report generator.

Highlights:

- One-shot HTML report with actionable insights; runs without superuser and degrades gracefully with limited privileges.
- Uses pg_stat_statements when available (and suggests installing when not) to surface top queries and outliers.
- Practical checks across connections, memory/cache, WAL, waits/locks, blocking, autovacuum, tables/indexes hygiene, functions, and replication.

What you'll see in the report:

- Overview cards: warnings, recommendations, and info. Cards link to section headers only when details exist.
- System & config:
  - Databases, Connections (+ by client), Settings (subset)
  - Memory and temporary files note; Cache hit ratio by database
  - WAL statistics (records, FPIs, bytes, reset time)
- Concurrency:
  - Wait events (top), Lock contention, Blocking queries, Long-running queries, Autovacuum activities
- Storage & indexing:
  - Top tables by rows/size
  - Tables with lowest index usage
  - Unused indexes
  - Tables dead rows bloat (est.), plus “Reclaimable space by DB (estimate)”
- Progress:
  - CREATE INDEX and ANALYZE progress (when available)
- Query performance (`pg_stat_statements`):
  - Top queries by total time and by calls with per-row details
  - Outlier summaries under each table: compact bullet lists that flag large shares (>=10%) and median outliers; only the query text is clickable and scrolls to the exact row
  - Query text is truncated by default with “Show full” toggle; optional plan advice with Highlights/Suggestions and a “Show plan” toggle
- Functions: Top functions by total time
- Replication status

Safety and behavior:

- No superuser required. The tool attempts optional queries and continues if blocked (pg_monitor helps but isn’t required).
- EXPLAIN plans are collected safely: SELECT/WITH only, no parameters, without ANALYZE, short timeouts.
- Navigation is resilient: links are shown only when the corresponding section is present; table toggles scroll to section headers for context.

Multi-DB mode:

- When `--dbs` is provided, table and index sections aggregate across those DBs and show a conditional "Database" column.
- “Top queries” (`pg_stat_statements`) remain scoped to the current database only.
- Installed extensions are listed per database when multiple DBs are collected.

## Usage

- Provide the connection string via:
  - `--url` flag, e.g. `--url postgres://user:pass@host:5432/db?sslmode=require`
  - or env: `PGURL` or `DATABASE_URL`
- Flags:
  - `--out` (default `report.html`). Supports `{ts}` placeholder for a timestamp, e.g. `--out report-{ts}.html`.
  - `--timeout` (default `30s`).
  - `--open` (default `true`) to open the report after generation.
  - `--suppress` to hide specific recommendation codes (comma-separated), e.g. `--suppress missing-extensions,cache-overall`.
  - `--dbs` to include additional databases for tables/indexes metrics (comma-separated). Example: `--dbs db1,db2`.
  - `--prompt` to generate an LLM-ready sidecar file (`.prompt.txt`) next to the HTML report.
  - Plans for top queries are collected automatically (safe: SELECT/WITH only). A soft per-list cap applies and clearly slow or very frequent queries are prioritized for planning.

## Installation (clone and build)

Requires Go 1.21+.

Clone and build:

```sh
git clone https://github.com/koltyakov/pghealth.git
cd pghealth

# Option A: via Makefile (writes bin/pghealth and stamps version)
make build

# Option B: quick build
go build -o pghealth

# Cross-compile binaries for common platforms into dist/
make build-all
```

Run the tool:

```sh
./bin/pghealth --url "postgres://user:pass@host:5432/db?sslmode=require" --out report-{ts}.html
# or when built without make:
./pghealth --url "$PGURL" --out report-{ts}.html
```

When `--prompt` is set, a sidecar prompt file for LLMs is written next to the HTML with the same name and the suffix `.prompt.txt` (e.g., `report-2025-08-30_1200.prompt.txt`). It contains environment-specific stats (top queries with any collected plans, tables, indexes, unused indexes, and settings) plus concise instructions for obtaining concrete recommendations.

## Notes

- Some checks require elevated privileges; missing data is handled gracefully and noted in the report.
- Heuristics (e.g., missing indexes, bloat estimates) are approximations—validate with owners before acting.
- Plans are sampled and displayed conservatively; large/slow or very frequent queries are emphasized.

## License

See `LICENSE` for details.
