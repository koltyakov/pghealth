package collect

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Result struct {
	ConnInfo       ConnInfo
	Extensions     Extensions
	Roles          Roles
	DBs            []Database
	Activity       []Activity
	Settings       []Setting
	Tables         []TableStat
	Indexes        []IndexStat
	IndexUnused    []IndexUnused
	MissingIndexes []MissingIndexHint
	Statements     Statements
	Errors         []string
	// Healthchecks
	CacheHitCurrent     float64
	CacheHitOverall     float64
	TotalConnections    int
	ConnectionsByClient []ClientConn
	Blocking            []Blocking
	LongRunning         []LongQuery
	AutoVacuum          []AutoVacuum
	CacheHits           []CacheHit
	IndexUsageLow       []IndexUsage
}

type ConnInfo struct {
	Version        string
	CurrentDB      string
	CurrentUser    string
	IsSuperuser    bool
	MaxConnections int
	SSL            string
	StartTime      time.Time
}

type Extensions struct {
	PgStatStatements bool
}

type Roles struct {
	HasPgMonitor bool
}

type Database struct {
	Name        string
	SizeBytes   int64
	Tablespaces string
	ConnCount   int
}

type Activity struct {
	Datname string
	State   string
	Count   int
}

type Setting struct {
	Name   string
	Val    string
	Unit   string
	Source string
}

type TableStat struct {
	Schema   string
	Name     string
	SeqScans int64
	IdxScans int64
	NLiveTup int64
	NDeadTup int64
	BloatPct float64 // heuristic
}

type IndexStat struct {
	Schema    string
	Table     string
	Name      string
	Scans     int64
	SizeBytes int64
}

type IndexUnused struct {
	Schema    string
	Table     string
	Name      string
	SizeBytes int64
}

type MissingIndexHint struct {
	Schema     string
	Table      string
	Columns    string
	EstBenefit string
}

type Statements struct {
	Available      bool
	TopByTotalTime []Statement
	TopByCPU       []Statement
	TopByCalls     []Statement
	TopByIO        []Statement
}

type Statement struct {
	Query        string
	Calls        float64
	TotalTime    float64
	MeanTime     float64
	Rows         float64
	BlkReadTime  float64
	BlkWriteTime float64
	CPUTime      float64 // approx: total - read - write
	IOTime       float64 // read + write
}

// Healthcheck types
type ClientConn struct {
	Hostname    string
	Address     string
	User        string
	Application string
	Count       int
}
type Blocking struct {
	Datname          string
	BlockedPID       int
	BlockingPID      int
	BlockedDuration  string
	BlockingDuration string
	BlockedQuery     string
	BlockingQuery    string
}
type LongQuery struct {
	Datname  string
	PID      int
	Duration string
	State    string
	Query    string
}
type AutoVacuum struct {
	Datname  string
	PID      int
	Relation string
	Phase    string
	Scanned  int64
	Total    int64
}

type CacheHit struct {
	Datname  string
	BlksHit  int64
	BlksRead int64
	Ratio    float64 // percent 0..100
}

type IndexUsage struct {
	Schema        string
	Table         string
	IndexUsagePct float64
	Rows          int64
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	var res Result

	conn, err := pgx.Connect(ctx, cfg.URL)
	if err != nil {
		return res, err
	}
	defer conn.Close(ctx)

	// basic info
	_ = queryRow(ctx, conn, `select version()`, &res.ConnInfo.Version)
	_ = queryRow(ctx, conn, `select current_database()`, &res.ConnInfo.CurrentDB)
	_ = queryRow(ctx, conn, `select current_user`, &res.ConnInfo.CurrentUser)
	_ = queryRow(ctx, conn, `select setting::int from pg_settings where name='max_connections'`, &res.ConnInfo.MaxConnections)
	_ = queryRow(ctx, conn, `show ssl`, &res.ConnInfo.SSL)
	_ = queryRow(ctx, conn, `select pg_postmaster_start_time()`, &res.ConnInfo.StartTime)

	// Is superuser
	_ = queryRow(ctx, conn, `select rolsuper from pg_roles where rolname = current_user`, &res.ConnInfo.IsSuperuser)

	// role membership (pg_monitor)
	var hasMonitor bool
	_ = queryRow(ctx, conn, `select exists(select 1 from pg_auth_members m join pg_roles r on r.oid=m.roleid where r.rolname='pg_monitor' and m.member=(select oid from pg_roles where rolname=current_user))`, &hasMonitor)
	res.Roles.HasPgMonitor = hasMonitor

	// extensions - robust detection
	res.Extensions.PgStatStatements = hasPgStatStatements(ctx, conn)

	// activity counts by state
	rows, err := conn.Query(ctx, `select datname, coalesce(state,'unknown') as state, count(*) from pg_stat_activity group by 1,2 order by 1,2`)
	if err == nil {
		for rows.Next() {
			var a Activity
			_ = rows.Scan(&a.Datname, &a.State, &a.Count)
			res.Activity = append(res.Activity, a)
		}
		rows.Close()
	}

	// databases size and connections
	rows, err = conn.Query(ctx, `select d.datname, pg_database_size(d.datname), coalesce(t.spcname,'pg_default'), coalesce(a.cnt,0)
        from pg_database d
        left join pg_tablespace t on t.oid = d.dattablespace
        left join (select datname, count(*) cnt from pg_stat_activity group by 1) a on a.datname = d.datname
        where not d.datistemplate
        order by pg_database_size(d.datname) desc`)
	if err == nil {
		for rows.Next() {
			var db Database
			_ = rows.Scan(&db.Name, &db.SizeBytes, &db.Tablespaces, &db.ConnCount)
			res.DBs = append(res.DBs, db)
		}
		rows.Close()
	}

	// settings of interest (subset)
	rows, err = conn.Query(ctx, `select name, setting, unit, source from pg_settings where name in (
        'shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_connections','wal_level','max_wal_size','checkpoint_timeout','random_page_cost','seq_page_cost','effective_io_concurrency','autovacuum','autovacuum_naptime','track_io_timing','track_functions') order by name`)
	if err == nil {
		for rows.Next() {
			var s Setting
			_ = rows.Scan(&s.Name, &s.Val, &s.Unit, &s.Source)
			res.Settings = append(res.Settings, s)
		}
		rows.Close()
	}

	// table stats (requires pg_stat_all_tables)
	rows, err = conn.Query(ctx, `select schemaname, relname, seq_scan, idx_scan, n_live_tup, n_dead_tup from pg_stat_all_tables`)
	if err == nil {
		for rows.Next() {
			var t TableStat
			_ = rows.Scan(&t.Schema, &t.Name, &t.SeqScans, &t.IdxScans, &t.NLiveTup, &t.NDeadTup)
			// rough bloat heuristic
			if t.NLiveTup > 0 {
				t.BloatPct = float64(t.NDeadTup) / float64(t.NLiveTup+t.NDeadTup) * 100
			}
			res.Tables = append(res.Tables, t)
		}
		rows.Close()
	}

	// index stats and size
	rows, err = conn.Query(ctx, `select s.schemaname, s.relname, s.indexrelname, s.idx_scan, pg_relation_size(format('%I.%I', s.schemaname, s.indexrelname))
        from pg_stat_all_indexes s`)
	if err == nil {
		for rows.Next() {
			var i IndexStat
			_ = rows.Scan(&i.Schema, &i.Table, &i.Name, &i.Scans, &i.SizeBytes)
			res.Indexes = append(res.Indexes, i)
		}
		rows.Close()
	}

	// unused indexes (idx_scan=0 and size > some threshold)
	for _, idx := range res.Indexes {
		if idx.Scans == 0 && idx.SizeBytes > 8*1024*1024 { // >8MB
			res.IndexUnused = append(res.IndexUnused, IndexUnused{Schema: idx.Schema, Table: idx.Table, Name: idx.Name, SizeBytes: idx.SizeBytes})
		}
	}

	// missing index hints (heuristic based on high seq_scan and low idx_scan)
	for _, t := range res.Tables {
		if t.SeqScans > 1000 && t.IdxScans < 100 { // simple heuristic
			res.MissingIndexes = append(res.MissingIndexes, MissingIndexHint{Schema: t.Schema, Table: t.Name, Columns: "(unknown)", EstBenefit: "High (heuristic)"})
		}
	}

	// pg_stat_statements if available
	if res.Extensions.PgStatStatements {
		// Top by total execution time
		if sts, ok := fetchPSS(ctx, conn, orderByTotal); ok {
			res.Statements.TopByTotalTime = sts
		}
		// Top by CPU time (approx = total - IO)
		if sts, ok := fetchPSS(ctx, conn, orderByCPUApprox); ok {
			res.Statements.TopByCPU = sts
		}
		// Top by IO time
		if sts, ok := fetchPSS(ctx, conn, orderByIO); ok {
			res.Statements.TopByIO = sts
		}
		// Top by calls
		if sts, ok := fetchPSS(ctx, conn, orderByCalls); ok {
			res.Statements.TopByCalls = sts
		}
		res.Statements.Available = len(res.Statements.TopByTotalTime) > 0 || len(res.Statements.TopByCalls) > 0
	}

	// Healthchecks collection
	// Overall connection count
	_ = queryRow(ctx, conn, `select count(*) from pg_stat_activity`, &res.TotalConnections)

	// Connections by client (hostname, address, user, application)
	if rows, err := conn.Query(ctx, `select coalesce(client_hostname,'') as client_hostname,
			coalesce(client_addr::text,'local') as client_addr,
			coalesce(usename,'') as usename,
			coalesce(application_name,'') as application_name,
			count(*) cnt
		from pg_stat_activity
		where usename is not null
		group by 1,2,3,4
		order by cnt desc`); err == nil {
		for rows.Next() {
			var c ClientConn
			_ = rows.Scan(&c.Hostname, &c.Address, &c.User, &c.Application, &c.Count)
			res.ConnectionsByClient = append(res.ConnectionsByClient, c)
		}
		rows.Close()
	}

	// Cache hit ratio (current DB and overall)
	{
		var hit, read int64
		if err := conn.QueryRow(ctx, `select coalesce(blks_hit,0), coalesce(blks_read,0) from pg_stat_database where datname=current_database()`).Scan(&hit, &read); err == nil {
			total := hit + read
			if total > 0 {
				res.CacheHitCurrent = float64(hit) / float64(total) * 100
			}
		}
		var hitSum, readSum int64
		if err := conn.QueryRow(ctx, `select coalesce(sum(blks_hit),0), coalesce(sum(blks_read),0) from pg_stat_database`).Scan(&hitSum, &readSum); err == nil {
			total := hitSum + readSum
			if total > 0 {
				res.CacheHitOverall = float64(hitSum) / float64(total) * 100
			}
		}
	}

	// Blocking queries
	if rows, err := conn.Query(ctx, `select a.datname, a.pid as blocked_pid, (now()-a.query_start)::text as blocked_for, a.query as blocked_query,
			b.pid as blocking_pid, (now()-b.query_start)::text as blocking_for, b.query as blocking_query
			from pg_stat_activity a
			join lateral unnest(pg_blocking_pids(a.pid)) as blocked_by(pid) on true
			join pg_stat_activity b on b.pid = blocked_by.pid
			order by (now()-a.query_start) desc limit 20`); err == nil {
		for rows.Next() {
			var bl Blocking
			_ = rows.Scan(&bl.Datname, &bl.BlockedPID, &bl.BlockedDuration, &bl.BlockedQuery, &bl.BlockingPID, &bl.BlockingDuration, &bl.BlockingQuery)
			res.Blocking = append(res.Blocking, bl)
		}
		rows.Close()
	}

	// Long running queries (> 5 minutes)
	if rows, err := conn.Query(ctx, `select datname, pid, (now()-query_start)::text as duration, state, query
			from pg_stat_activity where state='active' and now()-query_start > interval '5 minutes'
			order by (now()-query_start) desc limit 20`); err == nil {
		for rows.Next() {
			var lq LongQuery
			_ = rows.Scan(&lq.Datname, &lq.PID, &lq.Duration, &lq.State, &lq.Query)
			res.LongRunning = append(res.LongRunning, lq)
		}
		rows.Close()
	}

	// Autovacuum activities
	if rows, err := conn.Query(ctx, `select a.datname, p.pid, p.relid::regclass::text as relation, p.phase,
			p.heap_blks_scanned, p.heap_blks_total
			from pg_stat_progress_vacuum p
			join pg_stat_activity a on a.pid = p.pid
			order by a.datname, relation`); err == nil {
		for rows.Next() {
			var av AutoVacuum
			_ = rows.Scan(&av.Datname, &av.PID, &av.Relation, &av.Phase, &av.Scanned, &av.Total)
			res.AutoVacuum = append(res.AutoVacuum, av)
		}
		rows.Close()
	}

	// Cache hit ratio by database
	if rows, err := conn.Query(ctx, `select datname, blks_hit, blks_read,
			coalesce(round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 2), 0.0) as cache_hit_ratio
		from pg_stat_database
		order by cache_hit_ratio asc`); err == nil {
		for rows.Next() {
			var ch CacheHit
			_ = rows.Scan(&ch.Datname, &ch.BlksHit, &ch.BlksRead, &ch.Ratio)
			res.CacheHits = append(res.CacheHits, ch)
		}
		rows.Close()
	}

	// Lowest index usage tables (prefer user tables; fallback to all non-system)
	{
		q := `select schemaname, relname,
				coalesce(100.0 * idx_scan / nullif(seq_scan + idx_scan, 0), 0.0) as index_usage_pct,
				n_live_tup
			  from pg_stat_user_tables
			  where n_live_tup > 10000
			  order by index_usage_pct asc nulls last
			  limit 10`
		if rows, err := conn.Query(ctx, q); err == nil {
			for rows.Next() {
				var iu IndexUsage
				_ = rows.Scan(&iu.Schema, &iu.Table, &iu.IndexUsagePct, &iu.Rows)
				res.IndexUsageLow = append(res.IndexUsageLow, iu)
			}
			rows.Close()
		}
		if len(res.IndexUsageLow) == 0 {
			if rows, err := conn.Query(ctx, `select schemaname, relname,
					coalesce(100.0 * idx_scan / nullif(seq_scan + idx_scan, 0), 0.0) as index_usage_pct,
					n_live_tup
				  from pg_stat_all_tables
				  where schemaname not in ('pg_catalog','information_schema') and n_live_tup > 10000
				  order by index_usage_pct asc nulls last
				  limit 10`); err == nil {
				for rows.Next() {
					var iu IndexUsage
					_ = rows.Scan(&iu.Schema, &iu.Table, &iu.IndexUsagePct, &iu.Rows)
					res.IndexUsageLow = append(res.IndexUsageLow, iu)
				}
				rows.Close()
			}
		}
	}

	return res, nil
}

func hasPgStatStatements(ctx context.Context, conn *pgx.Conn) bool {
	// 1) check installed extension in current DB
	var hasExt bool
	_ = queryRow(ctx, conn, `select exists(select 1 from pg_extension where extname='pg_stat_statements')`, &hasExt)
	if hasExt {
		return true
	}
	// 2) check relation exists in any schema
	var hasRel bool
	_ = queryRow(ctx, conn, `select exists(
		select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
		where c.relname='pg_stat_statements'
	)`, &hasRel)
	if hasRel {
		return true
	}
	// 3) check function exists
	var hasProc bool
	_ = queryRow(ctx, conn, `select exists(
		select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace
		where p.proname in ('pg_stat_statements_reset','pg_stat_statements')
	)`, &hasProc)
	if hasProc {
		return true
	}
	// 4) last resort: probe a select
	if _, err := conn.Exec(ctx, `select 1 from pg_stat_statements limit 1`); err == nil {
		return true
	}
	return false
}

func queryRow[T any](ctx context.Context, conn *pgx.Conn, sql string, dst *T) error {
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	row := conn.QueryRow(ctx2, sql)
	return row.Scan(dst)
}

type pssOrder int

const (
	orderByTotal pssOrder = iota
	orderByCPUApprox
	orderByIO
	orderByCalls
)

// fetchPSS tries new (total_exec_time/mean_exec_time) first, then old (total_time/mean_time)
func fetchPSS(ctx context.Context, conn *pgx.Conn, ord pssOrder) ([]Statement, bool) {
	if sts, ok := fetchPSSVariant(ctx, conn, "total_exec_time", "mean_exec_time", ord); ok {
		return sts, true
	}
	if sts, ok := fetchPSSVariant(ctx, conn, "total_time", "mean_time", ord); ok {
		return sts, true
	}
	return nil, false
}

func fetchPSSVariant(ctx context.Context, conn *pgx.Conn, colTotal, colMean string, ord pssOrder) ([]Statement, bool) {
	orderExpr := ""
	switch ord {
	case orderByTotal:
		orderExpr = colTotal
	case orderByCPUApprox:
		orderExpr = fmt.Sprintf("(%s - blk_read_time - blk_write_time)", colTotal)
	case orderByIO:
		orderExpr = "(blk_read_time + blk_write_time)"
	case orderByCalls:
		orderExpr = "calls"
	}
	q := fmt.Sprintf(`select query, calls, %s as total_time, %s as mean_time, rows, blk_read_time, blk_write_time
		from pg_stat_statements order by %s desc nulls last limit 20`, colTotal, colMean, orderExpr)
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	var out []Statement
	for rows.Next() {
		var st Statement
		if err := rows.Scan(&st.Query, &st.Calls, &st.TotalTime, &st.MeanTime, &st.Rows, &st.BlkReadTime, &st.BlkWriteTime); err != nil {
			continue
		}
		st.IOTime = st.BlkReadTime + st.BlkWriteTime
		st.CPUTime = st.TotalTime - st.IOTime
		out = append(out, st)
	}
	return out, true
}
