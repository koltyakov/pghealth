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
}

type ConnInfo struct {
	Version        string
	CurrentDB      string
	CurrentUser    string
	IsSuperuser    bool
	MaxConnections int
	SSL            string
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

	// Is superuser
	_ = queryRow(ctx, conn, `select rolsuper from pg_roles where rolname = current_user`, &res.ConnInfo.IsSuperuser)

	// role membership (pg_monitor)
	var hasMonitor bool
	_ = queryRow(ctx, conn, `select exists(select 1 from pg_auth_members m join pg_roles r on r.oid=m.roleid where r.rolname='pg_monitor' and m.member=(select oid from pg_roles where rolname=current_user))`, &hasMonitor)
	res.Roles.HasPgMonitor = hasMonitor

	// extensions
	var hasPSS bool
	_ = queryRow(ctx, conn, `select exists(select 1 from pg_extension where extname='pg_stat_statements')`, &hasPSS)
	res.Extensions.PgStatStatements = hasPSS

	// activity counts by state
	rows, _ := conn.Query(ctx, `select datname, coalesce(state,'unknown') as state, count(*) from pg_stat_activity group by 1,2 order by 1,2`)
	for rows != nil && rows.Next() {
		var a Activity
		_ = rows.Scan(&a.Datname, &a.State, &a.Count)
		res.Activity = append(res.Activity, a)
	}
	if rows != nil {
		rows.Close()
	}

	// databases size and connections
	rows, _ = conn.Query(ctx, `select d.datname, pg_database_size(d.datname), coalesce(t.spcname,'pg_default'), coalesce(a.cnt,0)
        from pg_database d
        left join pg_tablespace t on t.oid = d.dattablespace
        left join (select datname, count(*) cnt from pg_stat_activity group by 1) a on a.datname = d.datname
        where not d.datistemplate
        order by pg_database_size(d.datname) desc`)
	for rows != nil && rows.Next() {
		var db Database
		_ = rows.Scan(&db.Name, &db.SizeBytes, &db.Tablespaces, &db.ConnCount)
		res.DBs = append(res.DBs, db)
	}
	if rows != nil {
		rows.Close()
	}

	// settings of interest (subset)
	rows, _ = conn.Query(ctx, `select name, setting, unit, source from pg_settings where name in (
        'shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_connections','wal_level','max_wal_size','checkpoint_timeout','random_page_cost','seq_page_cost','effective_io_concurrency','autovacuum','autovacuum_naptime','track_io_timing','track_functions') order by name`)
	for rows != nil && rows.Next() {
		var s Setting
		_ = rows.Scan(&s.Name, &s.Val, &s.Unit, &s.Source)
		res.Settings = append(res.Settings, s)
	}
	if rows != nil {
		rows.Close()
	}

	// table stats (requires pg_stat_all_tables)
	rows, _ = conn.Query(ctx, `select schemaname, relname, seq_scan, idx_scan, n_live_tup, n_dead_tup from pg_stat_all_tables`)
	for rows != nil && rows.Next() {
		var t TableStat
		_ = rows.Scan(&t.Schema, &t.Name, &t.SeqScans, &t.IdxScans, &t.NLiveTup, &t.NDeadTup)
		// rough bloat heuristic
		if t.NLiveTup > 0 {
			t.BloatPct = float64(t.NDeadTup) / float64(t.NLiveTup+t.NDeadTup) * 100
		}
		res.Tables = append(res.Tables, t)
	}
	if rows != nil {
		rows.Close()
	}

	// index stats and size
	rows, _ = conn.Query(ctx, `select s.schemaname, s.relname, s.indexrelname, s.idx_scan, pg_relation_size(format('%I.%I', s.schemaname, s.indexrelname))
        from pg_stat_all_indexes s`)
	for rows != nil && rows.Next() {
		var i IndexStat
		_ = rows.Scan(&i.Schema, &i.Table, &i.Name, &i.Scans, &i.SizeBytes)
		res.Indexes = append(res.Indexes, i)
	}
	if rows != nil {
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

	return res, nil
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
