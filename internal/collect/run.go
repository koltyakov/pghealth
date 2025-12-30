package collect

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Collection constants define thresholds and limits for data gathering.
const (
	// unusedIndexMinSize is the minimum size (bytes) for an index to be flagged as unused.
	unusedIndexMinSize = 8 * 1024 * 1024 // 8MB

	// seqScanThreshold is the minimum sequential scans for missing index heuristic.
	seqScanThreshold = 1000

	// idxScanThreshold is the maximum index scans for missing index heuristic.
	idxScanThreshold = 100

	// queryTimeoutShort is the timeout for simple queries.
	queryTimeoutShort = 5 * time.Second

	// queryTimeoutLong is the timeout for complex queries like EXPLAIN.
	queryTimeoutLong = 10 * time.Second

	// planPerListCap is the soft cap of planned queries per list.
	// Queries flagged as suspect can exceed this cap.
	planPerListCap = 10

	// maxResultRows limits results from unbounded queries.
	maxResultRows = 100

	// maxBlockingRows limits blocking query results.
	maxBlockingRows = 20

	// maxLongRunningRows limits long-running query results.
	maxLongRunningRows = 20
)

// Result contains all collected PostgreSQL metrics and statistics.
// Fields are populated based on available permissions and extensions.
type Result struct {
	// Connection and server information
	ConnInfo   ConnInfo   // Basic connection and server details
	Extensions Extensions // Installed PostgreSQL extensions
	Roles      Roles      // Role memberships for the connected user

	// Database-level metrics
	DBs      []Database // List of databases with sizes and connections
	Activity []Activity // Connection activity by database and state
	Settings []Setting  // PostgreSQL configuration settings

	// Table and index statistics
	Tables         []TableStat        // Table-level statistics
	Indexes        []IndexStat        // Index usage and size statistics
	IndexUnused    []IndexUnused      // Indexes with zero scans
	MissingIndexes []MissingIndexHint // Tables that may benefit from indexes

	// Query performance (requires pg_stat_statements)
	Statements Statements // Top queries by various metrics

	// Collection errors (non-fatal)
	Errors []string // Errors encountered during collection

	// Health check metrics
	CacheHitCurrent     float64      // Cache hit ratio for current database
	CacheHitOverall     float64      // Cluster-wide cache hit ratio
	TotalConnections    int          // Total active connections
	ConnectionsByClient []ClientConn // Connections grouped by client
	Blocking            []Blocking   // Currently blocked queries
	LongRunning         []LongQuery  // Queries running > 5 minutes
	AutoVacuum          []AutoVacuum // Active autovacuum workers

	// Detailed statistics
	CacheHits            []CacheHit        // Cache hit ratio per database
	IndexUsageLow        []IndexUsage      // Tables with low index usage
	TablesWithIndexCount []TableIndexCount // Tables with index counts
	TableBloatStats      []TableBloatStat  // Estimated table bloat
	IndexBloatStats      []IndexBloatStat  // Estimated index bloat
	ReplicationStats     []ReplicationStat // Streaming replication status
	CheckpointStats      CheckpointStats   // Checkpoint activity
	MemoryStats          MemoryStats       // Memory usage statistics
	IOStats              IOStats           // I/O statistics
	LockStats            []LockStat        // Lock contention statistics
	TempFileStats        []TempFileStat    // Temporary file usage
	ExtensionStats       []ExtensionStat   // Installed extensions details
	MemoryContexts       []MemoryContext   // Memory context information

	// Advanced metrics (may require pg_monitor role)
	WaitEvents          []WaitEventStat       // Wait event statistics
	FunctionStats       []FunctionStat        // User function statistics
	WAL                 *WALStat              // WAL statistics (PG13+)
	ProgressCreateIndex []ProgressCreateIndex // In-progress index builds
	ProgressAnalyze     []ProgressAnalyze     // In-progress ANALYZE operations

	// Additional health checks
	XIDAge            []DatabaseXIDAge    // Transaction ID age per database
	IdleInTransaction []IdleInTransaction // Long idle-in-transaction sessions
	StaleStatsTables  []StaleStatsTable   // Tables with outdated statistics
	DuplicateIndexes  []DuplicateIndex    // Indexes with identical definitions
	InvalidIndexes    []InvalidIndex      // Failed/invalid indexes
	FKMissingIndexes  []FKMissingIndex    // Foreign keys without supporting index
	SequenceHealth    []SequenceHealth    // Sequences approaching exhaustion
	PreparedXacts     []PreparedXact      // Orphaned prepared transactions
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
	PgStatStatements       bool
	PgStatStatementsSchema string
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
	Database  string
	Schema    string
	Name      string
	SeqScans  int64
	IdxScans  int64
	NLiveTup  int64
	NDeadTup  int64
	SizeBytes int64
	BloatPct  float64 // heuristic
}

type IndexStat struct {
	Database  string
	Schema    string
	Table     string
	Name      string
	Scans     int64
	SizeBytes int64
	DDL       string
}

type IndexUnused struct {
	Database  string
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
	TopByIOBlocks  []Statement
	StatsResetTime time.Time
	StatsDuration  time.Duration
	SkippedReason  string
}

type Statement struct {
	Query           string
	Calls           float64
	CallsPerHour    float64
	TotalTime       float64
	MeanTime        float64
	Rows            float64
	BlkReadTime     float64
	BlkWriteTime    float64
	CPUTime         float64 // approx: total - read - write
	IOTime          float64 // read + write
	SharedBlksRead  float64
	SharedBlksWrite float64
	LocalBlksRead   float64
	LocalBlksWrite  float64
	TempBlksRead    float64
	TempBlksWrite   float64
	Advice          *PlanAdvice
	NeedsAttention  bool
}

// PlanAdvice contains collected EXPLAIN plan text, highlights and human suggestions
type PlanAdvice struct {
	Plan            string
	Highlights      []string
	Suggestions     []string
	CanBeIndexed    bool
	CanBeRefactored bool
}

// Healthcheck types
type ClientConn struct {
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
	Database      string
	Schema        string
	Table         string
	IndexUsagePct float64
	Rows          int64
}

type TableIndexCount struct {
	Database   string
	Schema     string
	Name       string
	IndexCount int
	SizeBytes  int64
	RowCount   int64
	DeadRows   int64
	BloatPct   float64
}

type TableBloatStat struct {
	Schema         string
	Name           string
	EstimatedBloat float64 // percentage
	WastedBytes    int64
	LastVacuum     *time.Time
	LastAnalyze    *time.Time
}

type IndexBloatStat struct {
	Schema         string
	Table          string
	Name           string
	EstimatedBloat float64
	WastedBytes    int64
	Scans          int64
}

type ReplicationStat struct {
	Name         string
	State        string
	SyncState    string
	SyncPriority int
	ReplayLag    string
	WriteLag     string
	FlushLag     string
}

type CheckpointStats struct {
	RequestedCheckpoints int64
	ScheduledCheckpoints int64
	CheckpointWriteTime  time.Duration
	CheckpointSyncTime   time.Duration
	BuffersWritten       int64
	BuffersCheckpoint    int64
}

type MemoryStats struct {
	// Config and runtime metrics
	SharedBuffersUsed      int64 // buffers allocated since start (approx), from bgwriter
	SharedBuffersTotal     int64 // buffers total allocated (approx), from bgwriter
	SharedBuffersSetting   int64 // configured shared_buffers (number of buffers)
	SharedBuffersBytes     int64 // configured shared_buffers in bytes
	BlockSizeBytes         int64 // PostgreSQL block size in bytes
	BuffercacheAvailable   bool  // whether pg_buffercache is available
	BuffercacheUsedBuffers int64 // current number of buffers in use (from pg_buffercache)
	BuffercacheUsedBytes   int64 // BuffercacheUsedBuffers * BlockSizeBytes
	TempBytesCurrentDB     int64 // pg_stat_database.temp_bytes for current DB
	TempFilesCurrentDB     int64 // pg_stat_database.temp_files for current DB
	WorkMemUsed            int64 // placeholder (not available without sampling)
	MaintenanceWorkMem     int64 // placeholder (not available without sampling)
	TempBuffersUsed        int64 // placeholder
	LocalBuffersUsed       int64 // placeholder
}

type IOStats struct {
	HeapBlksRead  int64
	HeapBlksHit   int64
	IdxBlksRead   int64
	IdxBlksHit    int64
	ToastBlksRead int64
	ToastBlksHit  int64
	TidxBlksRead  int64
	TidxBlksHit   int64
	ReadTime      time.Duration
	WriteTime     time.Duration
}

type LockStat struct {
	LockType    string
	Mode        string
	Granted     bool
	Count       int
	WaitingPIDs []int
}

type TempFileStat struct {
	Datname string
	PID     int
	Files   int64
	Bytes   int64
}

type ExtensionStat struct {
	Database    string
	Name        string
	Version     string
	Description string
	Schema      string
}

// MemoryContext represents a row from pg_backend_memory_contexts for the current backend
type MemoryContext struct {
	Name       string
	Ident      string
	Parent     string
	Level      int
	TotalBytes int64
	FreeBytes  int64
	UsedBytes  int64
	NBlocks    int64
	FreeChunks int64
}

// WaitEventStat summarizes waits from pg_stat_activity
type WaitEventStat struct {
	Type  string
	Event string
	Count int
}

// FunctionStat from pg_stat_user_functions
type FunctionStat struct {
	Schema    string
	Name      string
	Calls     int64
	TotalTime float64
	SelfTime  float64
}

// WALStat from pg_stat_wal
type WALStat struct {
	Records    int64
	FullPage   int64
	Bytes      int64
	StatsReset time.Time
}

// ProgressCreateIndex from pg_stat_progress_create_index
type ProgressCreateIndex struct {
	Datname      string
	Relation     string
	Phase        string
	BlocksDone   int64
	BlocksTotal  int64
	TuplesDone   int64
	TuplesTotal  int64
	LockersDone  int64
	LockersTotal int64
}

// ProgressAnalyze from pg_stat_progress_analyze
type ProgressAnalyze struct {
	Datname     string
	Relation    string
	Phase       string
	SampleScans int64
	SampleTotal int64
}

// DatabaseXIDAge tracks transaction ID age for wraparound risk assessment
type DatabaseXIDAge struct {
	Datname    string
	Age        int64   // age(datfrozenxid)
	PctToLimit float64 // percentage toward 2^31 wraparound
	FrozenXID  int64   // datfrozenxid value
	MinMXID    int64   // datminmxid for multixact
	MinMXIDAge int64   // age of oldest multixact
}

// IdleInTransaction tracks sessions stuck in idle-in-transaction state
type IdleInTransaction struct {
	Datname     string
	PID         int
	User        string
	Application string
	Duration    string
	Query       string // last query before going idle
	WaitEvent   string
}

// StaleStatsTable tracks tables with outdated statistics
type StaleStatsTable struct {
	Schema           string
	Table            string
	RowEstimate      int64
	LastAnalyze      *time.Time
	LastAutoAnalyze  *time.Time
	ModsSinceAnalyze int64
	DaysSinceAnalyze int
}

// DuplicateIndex identifies indexes with redundant column definitions
type DuplicateIndex struct {
	Schema      string
	Table       string
	Index1      string
	Index2      string
	Columns     string
	Index1Size  int64
	Index2Size  int64
	Index1Scans int64
	Index2Scans int64
}

// InvalidIndex identifies indexes that failed to build
type InvalidIndex struct {
	Schema    string
	Table     string
	Name      string
	SizeBytes int64
	DDL       string
	Reason    string // "invalid" or "not ready"
}

// FKMissingIndex identifies foreign keys without supporting indexes
type FKMissingIndex struct {
	Schema       string
	Table        string
	Constraint   string
	Columns      string
	RefTable     string
	RefColumns   string
	TableRows    int64
	SuggestedDDL string
}

// SequenceHealth tracks sequences approaching exhaustion
type SequenceHealth struct {
	Schema    string
	Name      string
	LastValue int64
	MaxValue  int64
	Increment int64
	PctUsed   float64
	CallsLeft int64 // remaining increments before exhaustion
}

// PreparedXact tracks prepared (2PC) transactions that may be orphaned
type PreparedXact struct {
	Transaction string
	GID         string
	Owner       string
	Database    string
	Prepared    time.Time
	Age         string // duration since prepared
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

	// extensions - robust detection and schema resolution
	res.Extensions.PgStatStatements = hasPgStatStatements(ctx, conn)
	if res.Extensions.PgStatStatements {
		res.Extensions.PgStatStatementsSchema = findPgStatStatementsSchema(ctx, conn)
	}

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
		'shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_connections','max_parallel_workers','wal_buffers','wal_level','max_wal_size','checkpoint_timeout','random_page_cost','seq_page_cost','effective_io_concurrency','autovacuum','autovacuum_naptime','track_io_timing','track_functions') order by name`)
	if err == nil {
		for rows.Next() {
			var s Setting
			_ = rows.Scan(&s.Name, &s.Val, &s.Unit, &s.Source)
			res.Settings = append(res.Settings, s)
		}
		rows.Close()
	}

	// table stats (exclude system schemas) with table size
	rows, err = conn.Query(ctx, `select schemaname, relname, seq_scan, idx_scan, n_live_tup, n_dead_tup,
				pg_total_relation_size(format('%I.%I', schemaname, relname)) as size_bytes
				from pg_stat_all_tables
				where schemaname not in ('pg_catalog','information_schema')
					and schemaname not like 'pg_toast%'
					and schemaname not like 'pg_temp_%'`)
	if err == nil {
		for rows.Next() {
			var t TableStat
			_ = rows.Scan(&t.Schema, &t.Name, &t.SeqScans, &t.IdxScans, &t.NLiveTup, &t.NDeadTup, &t.SizeBytes)
			t.Database = res.ConnInfo.CurrentDB
			// rough bloat heuristic
			if t.NLiveTup > 0 {
				t.BloatPct = float64(t.NDeadTup) / float64(t.NLiveTup+t.NDeadTup) * 100
			}
			res.Tables = append(res.Tables, t)
		}
		rows.Close()
		// Backfill any missing user tables from pg_class for coverage
		present := make(map[string]struct{}, len(res.Tables))
		for _, t := range res.Tables {
			if t.Database == res.ConnInfo.CurrentDB {
				present[t.Schema+"."+t.Name] = struct{}{}
			}
		}
		if rows2, err2 := conn.Query(ctx, `select n.nspname as schemaname,
				c.relname,
				coalesce(c.reltuples::bigint, 0) as n_live_tup,
				pg_total_relation_size(c.oid) as size_bytes
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where c.relkind in ('r','m','p')
			  and n.nspname not in ('pg_catalog','information_schema')
			  and n.nspname not like 'pg_toast%'
			  and n.nspname not like 'pg_temp_%'`); err2 == nil {
			for rows2.Next() {
				var schema, name string
				var nlive, size int64
				_ = rows2.Scan(&schema, &name, &nlive, &size)
				key := schema + "." + name
				if _, ok := present[key]; ok {
					continue
				}
				res.Tables = append(res.Tables, TableStat{Database: res.ConnInfo.CurrentDB, Schema: schema, Name: name, SeqScans: 0, IdxScans: 0, NLiveTup: nlive, NDeadTup: 0, SizeBytes: size})
			}
			rows2.Close()
		}
	}

	// Fallback: if no rows (permissions or empty stats), derive from pg_class/pg_namespace
	if len(res.Tables) == 0 {
		if rows, err := conn.Query(ctx, `select n.nspname as schemaname,
				c.relname,
				0::bigint as seq_scan,
				0::bigint as idx_scan,
				coalesce(c.reltuples::bigint, 0) as n_live_tup,
				0::bigint as n_dead_tup,
				pg_total_relation_size(c.oid) as size_bytes
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where c.relkind in ('r','m','p')
			  and n.nspname not in ('pg_catalog','information_schema')
			  and n.nspname not like 'pg_toast%'
			  and n.nspname not like 'pg_temp_%'
			order by size_bytes desc
			limit 1000`); err == nil {
			for rows.Next() {
				var t TableStat
				_ = rows.Scan(&t.Schema, &t.Name, &t.SeqScans, &t.IdxScans, &t.NLiveTup, &t.NDeadTup, &t.SizeBytes)
				t.Database = res.ConnInfo.CurrentDB
				res.Tables = append(res.Tables, t)
			}
			rows.Close()
		}
	}

	// index stats and size
	rows, err = conn.Query(ctx, `select s.schemaname, s.relname, s.indexrelname, s.idx_scan,
		pg_relation_size(format('%I.%I', s.schemaname, s.indexrelname)),
		pg_get_indexdef(ci.oid)
		from pg_stat_all_indexes s
		join pg_class ci on ci.relname = s.indexrelname
		join pg_namespace n on n.oid = ci.relnamespace and n.nspname = s.schemaname`)
	if err == nil {
		for rows.Next() {
			var i IndexStat
			_ = rows.Scan(&i.Schema, &i.Table, &i.Name, &i.Scans, &i.SizeBytes, &i.DDL)
			i.Database = res.ConnInfo.CurrentDB
			res.Indexes = append(res.Indexes, i)
		}
		rows.Close()
	}

	// unused indexes (idx_scan=0 and size > some threshold)
	for _, idx := range res.Indexes {
		if idx.Scans == 0 && idx.SizeBytes > 8*1024*1024 { // >8MB
			res.IndexUnused = append(res.IndexUnused, IndexUnused{Database: idx.Database, Schema: idx.Schema, Table: idx.Table, Name: idx.Name, SizeBytes: idx.SizeBytes})
		}
	}

	// missing index hints (heuristic based on high seq_scan and low idx_scan)
	for _, t := range res.Tables {
		if t.SeqScans > 1000 && t.IdxScans < 100 { // simple heuristic
			res.MissingIndexes = append(res.MissingIndexes, MissingIndexHint{Schema: t.Schema, Table: t.Name, Columns: "(unknown)", EstBenefit: "High (heuristic)"})
		}
	}

	// If cfg.DBs provided, append per-DB tables/indexes by connecting to each DB
	if len(cfg.DBs) > 0 {
		baseURL := cfg.URL
		for _, db := range cfg.DBs {
			if db == "" || db == res.ConnInfo.CurrentDB {
				continue
			}
			// Build URL for target DB by replacing current_database()
			targetURL := baseURL
			// naive replace: if path component exists, swap last segment; otherwise append
			// This is a simple heuristic; for complex URLs, users should pass a URL to the target DB directly.
			if i := strings.LastIndex(targetURL, "/"); i != -1 {
				targetURL = targetURL[:i+1] + db
			} else {
				targetURL += "/" + db
			}
			ctxDB, cancelDB := context.WithTimeout(ctx, 10*time.Second)
			dbConn, err := pgx.Connect(ctxDB, targetURL)
			cancelDB()
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("db '%s': %v", db, err))
				continue
			}
			// Collect tables (exclude system schemas)
			if rows, err := dbConn.Query(ctx, `select schemaname, relname, seq_scan, idx_scan, n_live_tup, n_dead_tup,
								pg_total_relation_size(format('%I.%I', schemaname, relname)) as size_bytes
								from pg_stat_all_tables
								where schemaname not in ('pg_catalog','information_schema')
									and schemaname not like 'pg_toast%'
									and schemaname not like 'pg_temp_%'`); err == nil {
				for rows.Next() {
					var t TableStat
					_ = rows.Scan(&t.Schema, &t.Name, &t.SeqScans, &t.IdxScans, &t.NLiveTup, &t.NDeadTup, &t.SizeBytes)
					t.Database = db
					if t.NLiveTup > 0 {
						t.BloatPct = float64(t.NDeadTup) / float64(t.NLiveTup+t.NDeadTup) * 100
					}
					res.Tables = append(res.Tables, t)
				}
				rows.Close()
			}
			// Collect indexes
			if rows, err := dbConn.Query(ctx, `select s.schemaname, s.relname, s.indexrelname, s.idx_scan,
				pg_relation_size(format('%I.%I', s.schemaname, s.indexrelname)),
				pg_get_indexdef(ci.oid)
				from pg_stat_all_indexes s
				join pg_class ci on ci.relname = s.indexrelname
				join pg_namespace n on n.oid = ci.relnamespace and n.nspname = s.schemaname`); err == nil {
				for rows.Next() {
					var i IndexStat
					_ = rows.Scan(&i.Schema, &i.Table, &i.Name, &i.Scans, &i.SizeBytes, &i.DDL)
					i.Database = db
					res.Indexes = append(res.Indexes, i)
				}
				rows.Close()
			}
			// Derive unused indexes for that DB
			for _, idx := range res.Indexes {
				if idx.Database == db && idx.Scans == 0 && idx.SizeBytes > 8*1024*1024 {
					res.IndexUnused = append(res.IndexUnused, IndexUnused{Database: db, Schema: idx.Schema, Table: idx.Table, Name: idx.Name, SizeBytes: idx.SizeBytes})
				}
			}

			// Collect lowest index usage tables for that DB
			{
				q := `select schemaname, relname,
					coalesce(100.0 * idx_scan / nullif(seq_scan + idx_scan, 0), 0.0) as index_usage_pct,
					n_live_tup
				  from pg_stat_user_tables
				  where n_live_tup > 10000
				  order by index_usage_pct asc nulls last
				  limit 50`
				if rows, err := dbConn.Query(ctx, q); err == nil {
					for rows.Next() {
						var iu IndexUsage
						_ = rows.Scan(&iu.Schema, &iu.Table, &iu.IndexUsagePct, &iu.Rows)
						iu.Database = db
						res.IndexUsageLow = append(res.IndexUsageLow, iu)
					}
					rows.Close()
				}
			}

			// Collect tables with index counts for that DB
			if rows, err := dbConn.Query(ctx, `select t.schemaname, t.relname,
				count(i.indexrelid) as index_count,
				pg_total_relation_size(format('%I.%I', t.schemaname, t.relname)) as size_bytes,
				t.n_live_tup,
				t.n_dead_tup,
				coalesce(100.0 * t.n_dead_tup / nullif(t.n_live_tup + t.n_dead_tup, 0), 0.0) as bloat_pct
			from pg_stat_user_tables t
			left join pg_stat_user_indexes i on i.schemaname = t.schemaname and i.relname = t.relname
			group by t.schemaname, t.relname, t.n_live_tup, t.n_dead_tup
			order by size_bytes desc
			limit 100`); err == nil {
				for rows.Next() {
					var tic TableIndexCount
					_ = rows.Scan(&tic.Schema, &tic.Name, &tic.IndexCount, &tic.SizeBytes, &tic.RowCount, &tic.DeadRows, &tic.BloatPct)
					tic.Database = db
					res.TablesWithIndexCount = append(res.TablesWithIndexCount, tic)
				}
				rows.Close()
			}
			dbConn.Close(ctx)
		}
	}

	// pg_stat_statements if available
	if res.Extensions.PgStatStatements {
		// Get stats reset time
		var statsReset time.Time
		// Try pg_stat_statements_info first (PG13+)
		err := queryRow(ctx, conn, `SELECT stats_reset FROM pg_stat_statements_info`, &statsReset)
		if err != nil {
			// Fallback to pg_stat_database for older versions
			_ = queryRow(ctx, conn, `SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()`, &statsReset)
		}
		res.Statements.StatsResetTime = statsReset
		if !statsReset.IsZero() {
			res.Statements.StatsDuration = time.Since(statsReset)
		}

		// Check if a time window filter is configured
		var sinceFilter time.Time
		if cfg.StatsSince != "" {
			dur, err := time.ParseDuration(cfg.StatsSince)
			if err == nil {
				sinceFilter = time.Now().Add(-dur)
			}
		}

		// If filter is set and later than stats reset, skip collection
		if !sinceFilter.IsZero() && !statsReset.IsZero() && sinceFilter.After(statsReset) {
			res.Statements.SkippedReason = fmt.Sprintf("pg_stat_statements data is older than the requested window (%s).", cfg.StatsSince)
		} else {
			hasIO := hasPSSIOCols(ctx, conn, res.Extensions.PgStatStatementsSchema)
			hasBlk := hasPSSBlockCols(ctx, conn, res.Extensions.PgStatStatementsSchema)
			// Top by total execution time
			if sts, ok := fetchPSS(ctx, conn, res.Extensions.PgStatStatementsSchema, orderByTotal, hasIO, hasBlk); ok {
				res.Statements.TopByTotalTime = sts
			}
			// Top by CPU time (approx = total - IO)
			if hasIO {
				if sts, ok := fetchPSS(ctx, conn, res.Extensions.PgStatStatementsSchema, orderByCPUApprox, hasIO, hasBlk); ok {
					res.Statements.TopByCPU = sts
				}
			}
			// Top by IO time
			if hasIO {
				if sts, ok := fetchPSS(ctx, conn, res.Extensions.PgStatStatementsSchema, orderByIO, hasIO, hasBlk); ok {
					res.Statements.TopByIO = sts
				}
			}
			// Alternative IO ranking by block counts if IO time not available
			if !hasIO && hasBlk {
				if sts, ok := fetchPSS(ctx, conn, res.Extensions.PgStatStatementsSchema, orderByIOBlocks, false, hasBlk); ok {
					res.Statements.TopByIOBlocks = sts
				}
			}
			// Top by calls
			if sts, ok := fetchPSS(ctx, conn, res.Extensions.PgStatStatementsSchema, orderByCalls, hasIO, hasBlk); ok {
				res.Statements.TopByCalls = sts
			}
			res.Statements.Available = len(res.Statements.TopByTotalTime) > 0 || len(res.Statements.TopByCalls) > 0

			// Calculate calls per hour for all collected statements
			if hours := res.Statements.StatsDuration.Hours(); hours > 0 {
				for i := range res.Statements.TopByTotalTime {
					res.Statements.TopByTotalTime[i].CallsPerHour = res.Statements.TopByTotalTime[i].Calls / hours
				}
				for i := range res.Statements.TopByCPU {
					res.Statements.TopByCPU[i].CallsPerHour = res.Statements.TopByCPU[i].Calls / hours
				}
				for i := range res.Statements.TopByCalls {
					res.Statements.TopByCalls[i].CallsPerHour = res.Statements.TopByCalls[i].Calls / hours
				}
				for i := range res.Statements.TopByIO {
					res.Statements.TopByIO[i].CallsPerHour = res.Statements.TopByIO[i].Calls / hours
				}
				for i := range res.Statements.TopByIOBlocks {
					res.Statements.TopByIOBlocks[i].CallsPerHour = res.Statements.TopByIOBlocks[i].Calls / hours
				}
			}
		}
	}

	// Best-effort EXPLAIN plan collection per list (slowest and most frequent), each up to planPerListCap
	reParam := regexp.MustCompile(`\$\d+`)
	collectAdvice := func(sts []Statement) []Statement {
		limit := planPerListCap
		if len(sts) == 0 {
			return sts
		}
		// Compute medians for frequency baselines (CallsPerHour preferred, else Calls)
		median := func(vals []float64) float64 {
			if len(vals) == 0 {
				return 0
			}
			vv := make([]float64, 0, len(vals))
			vv = append(vv, vals...)
			// insertion sort is fine for <=20 elements
			for i := 1; i < len(vv); i++ {
				x := vv[i]
				j := i - 1
				for j >= 0 && vv[j] > x {
					vv[j+1] = vv[j]
					j--
				}
				vv[j+1] = x
			}
			n := len(vv)
			if n%2 == 1 {
				return vv[n/2]
			}
			return (vv[n/2-1] + vv[n/2]) / 2.0
		}
		var callsVals []float64
		var cphVals []float64
		for i := range sts {
			if sts[i].Calls > 0 {
				callsVals = append(callsVals, sts[i].Calls)
			}
			if sts[i].CallsPerHour > 0 {
				cphVals = append(cphVals, sts[i].CallsPerHour)
			}
		}
		callsMed := median(callsVals)
		cphMed := median(cphVals)

		// Dynamic threshold for what is considered "slow" based on volume.
		// Baseline: 1000ms is slow. More calls/rows => stricter (lower) threshold, but never below 200ms.
		calcThresholdMs := func(s Statement) float64 {
			base := 1000.0
			floor := 200.0
			// Frequency factor relative to medians
			ratio := 1.0
			if cphMed > 0 && s.CallsPerHour > 0 {
				ratio = s.CallsPerHour / cphMed
			} else if cphMed == 0 && callsMed > 0 && s.Calls > 0 {
				ratio = s.Calls / callsMed
			}
			freqMul := 1.0
			// tiered tighten: 1.5x -> 0.85x, 3x -> 0.7x, 10x -> 0.5x
			if ratio >= 10 {
				freqMul = 0.5
			} else if ratio >= 3 {
				freqMul = 0.7
			} else if ratio >= 1.5 {
				freqMul = 0.85
			}
			// Rows factor (average rows per call)
			rowsMul := 1.0
			if s.Rows >= 10000 {
				rowsMul = 0.7
			} else if s.Rows >= 1000 {
				rowsMul = 0.85
			}
			thr := base * freqMul * rowsMul
			if thr < floor {
				thr = floor
			}
			return thr
		}

		seenLocal := make(map[string]bool)
		taken := 0
		// A query is suspect if its mean time exceeds the dynamic threshold
		isSuspect := func(s Statement) bool {
			thr := calcThresholdMs(s)
			return s.MeanTime >= thr
		}
		for i := 0; i < len(sts); i++ {
			qTrim := strings.TrimSpace(sts[i].Query)
			if qTrim == "" || seenLocal[qTrim] {
				continue
			}
			seenLocal[qTrim] = true
			qUp := strings.ToUpper(qTrim)
			// Safe subset only: allow SELECT and WITH (CTE) queries
			if !(strings.HasPrefix(qUp, "SELECT") || strings.HasPrefix(qUp, "WITH")) {
				continue
			}
			suspect := isSuspect(sts[i])
			// Mark as needs attention up-front if suspect, even if plan collection fails later
			if suspect {
				sts[i].NeedsAttention = true
			}
			// If over main budget and not suspect, skip planning; suspects are always planned (unlimited overage)
			if taken >= limit && !suspect {
				continue
			}
			var planRows pgx.Rows
			var err error
			// Parameterized query path: use PREPARE/EXPLAIN EXECUTE with NULL args to avoid brittle substitutions
			if strings.Contains(qTrim, "$") {
				prepName := fmt.Sprintf("__pghealth_prep_%d", i)
				ctxPrep, cancelPrep := context.WithTimeout(ctx, 3*time.Second)
				_, errPrep := conn.Exec(ctxPrep, "PREPARE "+prepName+" AS "+qTrim)
				cancelPrep()
				if errPrep == nil {
					// determine parameter count by max $N occurrence
					maxParam := 0
					matches := reParam.FindAllString(qTrim, -1)
					for _, m := range matches {
						if len(m) > 1 {
							// m like $12
							numStr := m[1:]
							if n, errN := strconv.Atoi(numStr); errN == nil && n > maxParam {
								maxParam = n
							}
						}
					}
					// build NULL argument list matching parameter count
					argList := ""
					if maxParam > 0 {
						nulls := make([]string, maxParam)
						for k := 0; k < maxParam; k++ {
							nulls[k] = "NULL"
						}
						argList = "(" + strings.Join(nulls, ", ") + ")"
					}
					ctxPlan, cancel := context.WithTimeout(ctx, 5*time.Second)
					planRows, err = conn.Query(ctxPlan, "EXPLAIN EXECUTE "+prepName+argList)
					cancel()
					// cleanup
					ctxDel, cancelDel := context.WithTimeout(ctx, 1*time.Second)
					_, _ = conn.Exec(ctxDel, "DEALLOCATE "+prepName)
					cancelDel()
					if err != nil {
						// Fallback: replace parameters with NULL for a generic plan
						qForExplain := reParam.ReplaceAllString(qTrim, "NULL")
						ctxPlan2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
						planRows, err = conn.Query(ctxPlan2, "EXPLAIN "+qForExplain)
						cancel2()
					}
				} else {
					// Fallback: replace parameters with NULL for a generic plan
					qForExplain := reParam.ReplaceAllString(qTrim, "NULL")
					ctxPlan, cancel := context.WithTimeout(ctx, 5*time.Second)
					planRows, err = conn.Query(ctxPlan, "EXPLAIN "+qForExplain)
					cancel()
				}
			} else {
				// Non-parameterized
				ctxPlan, cancel := context.WithTimeout(ctx, 5*time.Second)
				planRows, err = conn.Query(ctxPlan, "EXPLAIN "+qTrim)
				cancel()
			}
			if err != nil {
				// Plan failed; if it is suspect, keep NeedsAttention as set, but don't count against planning limit
				continue
			}
			var planLines []string
			var seqOn []string
			hasSort := false
			hasJoin := false
			joinType := ""
			hasBitmap := false
			hasParallel := false
			hasCTE := false
			for planRows.Next() {
				var line string
				_ = planRows.Scan(&line)
				planLines = append(planLines, line)
				up := strings.ToUpper(line)
				if strings.Contains(up, "SEQ SCAN ON ") {
					idx := strings.Index(up, "SEQ SCAN ON ")
					if idx >= 0 {
						rest := strings.TrimSpace(line[idx+len("SEQ SCAN ON "):])
						name := rest
						if j := strings.IndexAny(rest, " (\t"); j >= 0 {
							name = rest[:j]
						}
						seqOn = append(seqOn, name)
					}
				}
				if strings.HasPrefix(strings.TrimSpace(up), "SORT ") || strings.Contains(up, " SORT ") {
					hasSort = true
				}
				if strings.Contains(up, "BITMAP ") {
					hasBitmap = true
				}
				if strings.Contains(up, " NESTED LOOP ") {
					hasJoin = true
					joinType = "Nested Loop"
				} else if strings.Contains(up, " HASH JOIN ") {
					hasJoin = true
					joinType = "Hash Join"
				} else if strings.Contains(up, " MERGE JOIN ") {
					hasJoin = true
					joinType = "Merge Join"
				} else if strings.Contains(up, " JOIN ") {
					hasJoin = true
					if joinType == "" {
						joinType = "Join"
					}
				}
				if strings.Contains(up, "PARALLEL ") {
					hasParallel = true
				}
				if strings.Contains(up, "CTE ") || strings.Contains(up, "WITH ") {
					hasCTE = true
				}
			}
			planRows.Close()
			advice := &PlanAdvice{}
			if len(planLines) > 0 {
				advice.Plan = strings.Join(planLines, "\n")
			}
			// Highlights
			for _, tname := range seqOn {
				advice.Highlights = append(advice.Highlights, fmt.Sprintf("Seq Scan on %s", tname))
			}
			if hasBitmap {
				advice.Highlights = append(advice.Highlights, "Bitmap scan present")
			}
			if hasSort {
				advice.Highlights = append(advice.Highlights, "Explicit Sort in plan")
			}
			if hasJoin {
				if joinType != "" {
					advice.Highlights = append(advice.Highlights, joinType)
				} else {
					advice.Highlights = append(advice.Highlights, "Join present")
				}
			}
			if hasParallel {
				advice.Highlights = append(advice.Highlights, "Parallel operation(s)")
			}
			if hasCTE {
				advice.Highlights = append(advice.Highlights, "CTE in plan")
			}
			// Suggestions
			findTable := func(name string) (TableStat, bool) {
				for _, t := range res.Tables {
					if strings.EqualFold(t.Name, name) {
						return t, true
					}
				}
				return TableStat{}, false
			}
			hasAnyIndex := func(name string) bool {
				for _, idx := range res.Indexes {
					if strings.EqualFold(idx.Table, name) {
						return true
					}
				}
				return false
			}
			if len(seqOn) > 0 {
				for _, tn := range seqOn {
					if ts, ok := findTable(tn); ok {
						if ts.NLiveTup > 100000 { // large table heuristic
							advice.Suggestions = append(advice.Suggestions, fmt.Sprintf("Large table %s scanned sequentially — consider adding/using an index on predicate/join columns.", tn))
							advice.CanBeIndexed = true
						} else {
							advice.Suggestions = append(advice.Suggestions, fmt.Sprintf("Sequential scan on %s — verify if intentional (small table) or add an index.", tn))
							advice.CanBeIndexed = true
						}
						if !hasAnyIndex(tn) {
							advice.Suggestions = append(advice.Suggestions, fmt.Sprintf("No indexes found on %s — create indexes on frequently filtered or joined columns.", tn))
							advice.CanBeIndexed = true
						}
					} else {
						advice.Suggestions = append(advice.Suggestions, fmt.Sprintf("Sequential scan on %s — consider index on predicate columns.", tn))
						advice.CanBeIndexed = true
					}
				}
			}
			if hasBitmap {
				advice.Suggestions = append(advice.Suggestions, "Consider composite/covering indexes to reduce Bitmap Heap rechecks when appropriate.")
				advice.CanBeIndexed = true
			}
			if hasSort {
				advice.Suggestions = append(advice.Suggestions, "Add or adjust an index matching ORDER BY to avoid Sort when appropriate; review work_mem as needed.")
				advice.CanBeIndexed = true
			}
			if hasJoin {
				advice.Suggestions = append(advice.Suggestions, "Ensure join keys are indexed on both sides (consider composite indexes for multi-column joins).")
				advice.CanBeIndexed = true
			}
			if hasCTE {
				advice.Suggestions = append(advice.Suggestions, "If CTE is not reused, consider inlining it (PostgreSQL may materialize it depending on version/settings).")
				advice.CanBeRefactored = true
			}
			if !advice.CanBeIndexed && len(seqOn) > 0 {
				advice.CanBeRefactored = true
				advice.Suggestions = append(advice.Suggestions, "Query uses sequential scans but no clear index path was found. Consider refactoring the query for better performance.")
			}
			if advice.Plan != "" || len(advice.Suggestions) > 0 || len(advice.Highlights) > 0 {
				sts[i].Advice = advice
				// Do not set NeedsAttention based on presence of a plan; it's governed by thresholds only.
				if taken < limit {
					taken++
				}
			}
		}
		return sts
	}
	if len(res.Statements.TopByTotalTime) > 0 {
		res.Statements.TopByTotalTime = collectAdvice(res.Statements.TopByTotalTime)
	}
	if len(res.Statements.TopByCalls) > 0 {
		res.Statements.TopByCalls = collectAdvice(res.Statements.TopByCalls)
	}

	// Healthchecks collection
	// Overall connection count
	_ = queryRow(ctx, conn, `select count(*) from pg_stat_activity`, &res.TotalConnections)

	// Connections by client (address, user, application)
	if rows, err := conn.Query(ctx, `select
			coalesce(host(client_addr), 'local') as client_addr,
			coalesce(usename, '') as usename,
			coalesce(application_name, '') as application_name,
			count(*) as cnt
		from pg_stat_activity
		where usename is not null
		group by 1, 2, 3
		order by cnt desc`); err == nil {
		for rows.Next() {
			var c ClientConn
			if err := rows.Scan(&c.Address, &c.User, &c.Application, &c.Count); err == nil {
				res.ConnectionsByClient = append(res.ConnectionsByClient, c)
			}
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
			  limit 50`
		if rows, err := conn.Query(ctx, q); err == nil {
			for rows.Next() {
				var iu IndexUsage
				_ = rows.Scan(&iu.Schema, &iu.Table, &iu.IndexUsagePct, &iu.Rows)
				iu.Database = res.ConnInfo.CurrentDB
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
				  limit 50`); err == nil {
				for rows.Next() {
					var iu IndexUsage
					_ = rows.Scan(&iu.Schema, &iu.Table, &iu.IndexUsagePct, &iu.Rows)
					iu.Database = res.ConnInfo.CurrentDB
					res.IndexUsageLow = append(res.IndexUsageLow, iu)
				}
				rows.Close()
			}
		}
	}

	// Tables with index counts
	if rows, err := conn.Query(ctx, `select t.schemaname, t.relname,
			count(i.indexrelid) as index_count,
			pg_total_relation_size(format('%I.%I', t.schemaname, t.relname)) as size_bytes,
			t.n_live_tup,
			t.n_dead_tup,
			coalesce(100.0 * t.n_dead_tup / nullif(t.n_live_tup + t.n_dead_tup, 0), 0.0) as bloat_pct
		from pg_stat_user_tables t
		left join pg_stat_user_indexes i on i.schemaname = t.schemaname and i.relname = t.relname
		group by t.schemaname, t.relname, t.n_live_tup, t.n_dead_tup
		order by size_bytes desc
		limit 100`); err == nil {
		for rows.Next() {
			var tic TableIndexCount
			_ = rows.Scan(&tic.Schema, &tic.Name, &tic.IndexCount, &tic.SizeBytes, &tic.RowCount, &tic.DeadRows, &tic.BloatPct)
			tic.Database = res.ConnInfo.CurrentDB
			res.TablesWithIndexCount = append(res.TablesWithIndexCount, tic)
		}
		rows.Close()
	}

	// Advanced table bloat analysis
	if rows, err := conn.Query(ctx, `select schemaname, relname,
			coalesce(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 0.0) as bloat_pct,
			pg_total_relation_size(format('%I.%I', schemaname, relname)) * 
			coalesce(n_dead_tup::float8 / nullif(n_live_tup + n_dead_tup, 0), 0.0) as wasted_bytes,
			last_vacuum, last_analyze
		from pg_stat_user_tables
		where n_live_tup + n_dead_tup > 10000
		order by wasted_bytes desc
		limit 50`); err == nil {
		for rows.Next() {
			var tbs TableBloatStat
			var lastVacuum, lastAnalyze *time.Time
			_ = rows.Scan(&tbs.Schema, &tbs.Name, &tbs.EstimatedBloat, &tbs.WastedBytes, &lastVacuum, &lastAnalyze)
			tbs.LastVacuum = lastVacuum
			tbs.LastAnalyze = lastAnalyze
			res.TableBloatStats = append(res.TableBloatStats, tbs)
		}
		rows.Close()
	}

	// Index bloat analysis
	if rows, err := conn.Query(ctx, `select s.schemaname, s.relname, s.indexrelname,
			0.0 as estimated_bloat, -- Placeholder for actual bloat calculation
			pg_relation_size(s.indexrelid) as size_bytes,
			s.idx_scan
		from pg_stat_user_indexes s
		where pg_relation_size(s.indexrelid) > 10485760 -- > 10MB
		order by size_bytes desc
		limit 50`); err == nil {
		for rows.Next() {
			var ibs IndexBloatStat
			_ = rows.Scan(&ibs.Schema, &ibs.Table, &ibs.Name, &ibs.EstimatedBloat, &ibs.WastedBytes, &ibs.Scans)
			res.IndexBloatStats = append(res.IndexBloatStats, ibs)
		}
		rows.Close()
	}

	// Replication statistics
	if rows, err := conn.Query(ctx, `select application_name, state, sync_state, sync_priority,
			coalesce(write_lag::text, '00:00:00') as write_lag,
			coalesce(flush_lag::text, '00:00:00') as flush_lag,
			coalesce(replay_lag::text, '00:00:00') as replay_lag
		from pg_stat_replication
		order by sync_priority desc`); err == nil {
		for rows.Next() {
			var rs ReplicationStat
			_ = rows.Scan(&rs.Name, &rs.State, &rs.SyncState, &rs.SyncPriority, &rs.WriteLag, &rs.FlushLag, &rs.ReplayLag)
			res.ReplicationStats = append(res.ReplicationStats, rs)
		}
		rows.Close()
	}

	// Wait events (top)
	if rows, err := conn.Query(ctx, `select coalesce(wait_event_type,'none') as type, coalesce(wait_event,'none') as event, count(*)
		from pg_stat_activity
		where wait_event is not null
		group by 1,2
		order by 3 desc
		limit 20`); err == nil {
		for rows.Next() {
			var w WaitEventStat
			_ = rows.Scan(&w.Type, &w.Event, &w.Count)
			res.WaitEvents = append(res.WaitEvents, w)
		}
		rows.Close()
	}

	// Top functions by total time (if view available)
	if rows, err := conn.Query(ctx, `select schemaname, funcname, calls, coalesce(total_time,0), coalesce(self_time,0)
		from pg_stat_user_functions
		order by total_time desc nulls last limit 20`); err == nil {
		for rows.Next() {
			var f FunctionStat
			_ = rows.Scan(&f.Schema, &f.Name, &f.Calls, &f.TotalTime, &f.SelfTime)
			res.FunctionStats = append(res.FunctionStats, f)
		}
		rows.Close()
	}

	// WAL statistics (if view exists)
	{
		var hasWAL bool
		_ = queryRow(ctx, conn, `select exists(select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='pg_catalog' and c.relname='pg_stat_wal')`, &hasWAL)
		if hasWAL {
			var ws WALStat
			if err := conn.QueryRow(ctx, `select wal_records, wal_fpi, wal_bytes, stats_reset from pg_stat_wal`).Scan(&ws.Records, &ws.FullPage, &ws.Bytes, &ws.StatsReset); err == nil {
				res.WAL = &ws
			}
		}
	}

	// Progress: CREATE INDEX (if view exists)
	if rows, err := conn.Query(ctx, `select a.datname, p.relid::regclass::text as relation, p.phase,
		coalesce(p.blocks_done,0), coalesce(p.blocks_total,0), coalesce(p.tuples_done,0), coalesce(p.tuples_total,0),
		coalesce(p.lockers_done,0), coalesce(p.lockers_total,0)
		from pg_stat_progress_create_index p join pg_stat_activity a on a.pid=p.pid
		order by a.datname, relation`); err == nil {
		for rows.Next() {
			var pr ProgressCreateIndex
			_ = rows.Scan(&pr.Datname, &pr.Relation, &pr.Phase, &pr.BlocksDone, &pr.BlocksTotal, &pr.TuplesDone, &pr.TuplesTotal, &pr.LockersDone, &pr.LockersTotal)
			res.ProgressCreateIndex = append(res.ProgressCreateIndex, pr)
		}
		rows.Close()
	}

	// Progress: ANALYZE (if view exists)
	if rows, err := conn.Query(ctx, `select a.datname, p.relid::regclass::text as relation, p.phase,
		coalesce(p.sample_blks_scanned,0), coalesce(p.sample_blks_total,0)
		from pg_stat_progress_analyze p join pg_stat_activity a on a.pid=p.pid
		order by a.datname, relation`); err == nil {
		for rows.Next() {
			var pa ProgressAnalyze
			_ = rows.Scan(&pa.Datname, &pa.Relation, &pa.Phase, &pa.SampleScans, &pa.SampleTotal)
			res.ProgressAnalyze = append(res.ProgressAnalyze, pa)
		}
		rows.Close()
	}

	// Checkpoint statistics
	if rows, err := conn.Query(ctx, `select checkpoints_req, checkpoints_timed,
			checkpoint_write_time, checkpoint_sync_time,
			buffers_checkpoint, buffers_clean
		from pg_stat_bgwriter`); err == nil {
		if rows.Next() {
			_ = rows.Scan(&res.CheckpointStats.RequestedCheckpoints, &res.CheckpointStats.ScheduledCheckpoints,
				&res.CheckpointStats.CheckpointWriteTime, &res.CheckpointStats.CheckpointSyncTime,
				&res.CheckpointStats.BuffersCheckpoint, &res.CheckpointStats.BuffersWritten)
		}
		rows.Close()
	}

	// Memory statistics
	// 1) bgwriter counters (approximate buffer allocation stats)
	if rows, err := conn.Query(ctx, `select buffers_alloc, buffers_checkpoint + buffers_clean + buffers_backend
		from pg_stat_bgwriter`); err == nil {
		if rows.Next() {
			_ = rows.Scan(&res.MemoryStats.SharedBuffersUsed, &res.MemoryStats.SharedBuffersTotal)
		}
		rows.Close()
	}
	// 2) block size and shared_buffers config
	{
		var blockSizeStr string
		_ = queryRow(ctx, conn, `select current_setting('block_size')`, &blockSizeStr)
		// Try to parse as integer bytes; if fails and contains kB suffix, strip and multiply
		if n, err := strconv.ParseInt(strings.TrimSpace(blockSizeStr), 10, 64); err == nil && n > 0 {
			res.MemoryStats.BlockSizeBytes = n
		} else {
			// e.g., "8kB"
			bs := strings.ToLower(strings.TrimSpace(blockSizeStr))
			if strings.HasSuffix(bs, "kb") {
				v := strings.TrimSuffix(bs, "kb")
				if n2, err2 := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err2 == nil && n2 > 0 {
					res.MemoryStats.BlockSizeBytes = n2 * 1024
				}
			}
		}
		var sbCount int64
		_ = queryRow(ctx, conn, `select setting::bigint from pg_settings where name='shared_buffers'`, &sbCount)
		res.MemoryStats.SharedBuffersSetting = sbCount
		if res.MemoryStats.BlockSizeBytes > 0 && sbCount > 0 {
			res.MemoryStats.SharedBuffersBytes = sbCount * res.MemoryStats.BlockSizeBytes
		}
	}
	// 3) pg_buffercache usage if available
	{
		var hasBuffercache bool
		_ = queryRow(ctx, conn, `select exists(select 1 from pg_extension where extname='pg_buffercache')`, &hasBuffercache)
		res.MemoryStats.BuffercacheAvailable = hasBuffercache
		if hasBuffercache {
			var used int64
			_ = queryRow(ctx, conn, `select count(*) from pg_buffercache`, &used)
			res.MemoryStats.BuffercacheUsedBuffers = used
			if res.MemoryStats.BlockSizeBytes > 0 && used > 0 {
				res.MemoryStats.BuffercacheUsedBytes = used * res.MemoryStats.BlockSizeBytes
			}
		}
	}
	// 4) Temp file usage from pg_stat_database for current DB
	{
		var tf, tb int64
		if err := conn.QueryRow(ctx, `select coalesce(temp_files,0), coalesce(temp_bytes,0) from pg_stat_database where datname=current_database()`).Scan(&tf, &tb); err == nil {
			res.MemoryStats.TempFilesCurrentDB = tf
			res.MemoryStats.TempBytesCurrentDB = tb
		}
	}

	// IO statistics
	if rows, err := conn.Query(ctx, `select heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
			toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit,
			coalesce(blk_read_time, 0), coalesce(blk_write_time, 0)
		from pg_stat_database
		where datname = current_database()`); err == nil {
		if rows.Next() {
			_ = rows.Scan(&res.IOStats.HeapBlksRead, &res.IOStats.HeapBlksHit,
				&res.IOStats.IdxBlksRead, &res.IOStats.IdxBlksHit,
				&res.IOStats.ToastBlksRead, &res.IOStats.ToastBlksHit,
				&res.IOStats.TidxBlksRead, &res.IOStats.TidxBlksHit,
				&res.IOStats.ReadTime, &res.IOStats.WriteTime)
		}
		rows.Close()
	}

	// Lock statistics
	if rows, err := conn.Query(ctx, `select locktype, mode, granted, count(*) as count,
			array_agg(pid) as waiting_pids
		from pg_locks
		where not granted
		group by locktype, mode, granted
		order by count desc
		limit 20`); err == nil {
		for rows.Next() {
			var ls LockStat
			_ = rows.Scan(&ls.LockType, &ls.Mode, &ls.Granted, &ls.Count, &ls.WaitingPIDs)
			res.LockStats = append(res.LockStats, ls)
		}
		rows.Close()
	}

	// Temporary file statistics
	if rows, err := conn.Query(ctx, `select datname, pid, temp_files, temp_bytes
		from pg_stat_activity
		where temp_files > 0 or temp_bytes > 0
		order by temp_bytes desc
		limit 20`); err == nil {
		for rows.Next() {
			var tfs TempFileStat
			_ = rows.Scan(&tfs.Datname, &tfs.PID, &tfs.Files, &tfs.Bytes)
			res.TempFileStats = append(res.TempFileStats, tfs)
		}
		rows.Close()
	}

	// Extension statistics for current DB
	if rows, err := conn.Query(ctx, `select e.extname, e.extversion, obj_description(e.oid, 'pg_extension'),
			n.nspname
		from pg_extension e
		left join pg_namespace n on n.oid = e.extnamespace
		order by e.extname`); err == nil {
		for rows.Next() {
			var es ExtensionStat
			_ = rows.Scan(&es.Name, &es.Version, &es.Description, &es.Schema)
			es.Database = res.ConnInfo.CurrentDB
			res.ExtensionStats = append(res.ExtensionStats, es)
		}
		rows.Close()
	}

	// Per-DB extensions: if cfg.DBs provided, check each DB for installed extensions
	if len(cfg.DBs) > 0 {
		baseURL := cfg.URL
		for _, db := range cfg.DBs {
			// Skip current DB; already collected
			if db == res.ConnInfo.CurrentDB {
				continue
			}
			// Build URL for target DB (naive last path segment swap)
			targetURL := swapDBInURL(baseURL, db)
			if targetURL == "" {
				continue
			}
			if c2, err := pgx.Connect(ctx, targetURL); err == nil {
				if rows, err := c2.Query(ctx, `select e.extname, e.extversion, obj_description(e.oid, 'pg_extension'),
					n.nspname
				from pg_extension e
				left join pg_namespace n on n.oid = e.extnamespace
				order by e.extname`); err == nil {
					for rows.Next() {
						var es ExtensionStat
						_ = rows.Scan(&es.Name, &es.Version, &es.Description, &es.Schema)
						es.Database = db
						res.ExtensionStats = append(res.ExtensionStats, es)
					}
					rows.Close()
				}
				c2.Close(ctx)
			}
		}
	}

	// ============================================================
	// Additional Health Checks
	// ============================================================

	// 1. XID Wraparound Risk - Transaction ID age per database
	// Maximum XID age before wraparound is ~2 billion (2^31)
	const xidMax = 2147483647 // 2^31 - 1
	if rows, err := conn.Query(ctx, `SELECT datname,
			age(datfrozenxid) as xid_age,
			datfrozenxid::text::bigint as frozen_xid,
			datminmxid::text::bigint as min_mxid,
			mxid_age(datminmxid) as mxid_age
		FROM pg_database
		WHERE datallowconn
		ORDER BY age(datfrozenxid) DESC`); err == nil {
		for rows.Next() {
			var x DatabaseXIDAge
			_ = rows.Scan(&x.Datname, &x.Age, &x.FrozenXID, &x.MinMXID, &x.MinMXIDAge)
			x.PctToLimit = float64(x.Age) / float64(xidMax) * 100
			res.XIDAge = append(res.XIDAge, x)
		}
		rows.Close()
	}

	// 2. Idle-in-Transaction sessions (potential blockers and resource holders)
	if rows, err := conn.Query(ctx, `SELECT datname, pid, usename, application_name,
			(now() - state_change)::text as duration,
			left(query, 200) as query,
			coalesce(wait_event, '') as wait_event
		FROM pg_stat_activity
		WHERE state = 'idle in transaction'
		  AND (now() - state_change) > interval '5 minutes'
		ORDER BY (now() - state_change) DESC
		LIMIT 20`); err == nil {
		for rows.Next() {
			var it IdleInTransaction
			_ = rows.Scan(&it.Datname, &it.PID, &it.User, &it.Application, &it.Duration, &it.Query, &it.WaitEvent)
			res.IdleInTransaction = append(res.IdleInTransaction, it)
		}
		rows.Close()
	}

	// 3. Stale Statistics - Tables that haven't been analyzed recently
	if rows, err := conn.Query(ctx, `SELECT schemaname, relname,
			n_live_tup as row_estimate,
			last_analyze,
			last_autoanalyze,
			n_mod_since_analyze as mods_since_analyze,
			COALESCE(
				EXTRACT(epoch FROM (now() - COALESCE(last_analyze, last_autoanalyze)))::int / 86400,
				999
			) as days_since_analyze
		FROM pg_stat_user_tables
		WHERE n_live_tup > 1000
		  AND (last_analyze IS NULL AND last_autoanalyze IS NULL
		       OR COALESCE(last_analyze, last_autoanalyze) < now() - interval '7 days')
		ORDER BY n_live_tup DESC
		LIMIT 50`); err == nil {
		for rows.Next() {
			var st StaleStatsTable
			_ = rows.Scan(&st.Table, &st.Schema, &st.RowEstimate, &st.LastAnalyze, &st.LastAutoAnalyze, &st.ModsSinceAnalyze, &st.DaysSinceAnalyze)
			// Swap schema/table - query returns schemaname first
			st.Schema, st.Table = st.Table, st.Schema
			res.StaleStatsTables = append(res.StaleStatsTables, st)
		}
		rows.Close()
	}

	// 4. Duplicate Indexes - Indexes with identical column definitions
	if rows, err := conn.Query(ctx, `WITH index_cols AS (
			SELECT n.nspname as schema,
				   t.relname as table_name,
				   i.relname as index_name,
				   pg_get_indexdef(i.oid) as index_def,
				   array_to_string(array_agg(a.attname ORDER BY x.n), ', ') as columns,
				   pg_relation_size(i.oid) as size_bytes,
				   COALESCE(s.idx_scan, 0) as scans
			FROM pg_index ix
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_class t ON t.oid = ix.indrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
			CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n)
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
			WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
			GROUP BY n.nspname, t.relname, i.relname, i.oid, s.idx_scan
		)
		SELECT a.schema, a.table_name, a.index_name, b.index_name,
			   a.columns, a.size_bytes, b.size_bytes, a.scans, b.scans
		FROM index_cols a
		JOIN index_cols b ON a.schema = b.schema
			AND a.table_name = b.table_name
			AND a.columns = b.columns
			AND a.index_name < b.index_name
		ORDER BY a.size_bytes + b.size_bytes DESC
		LIMIT 20`); err == nil {
		for rows.Next() {
			var di DuplicateIndex
			_ = rows.Scan(&di.Schema, &di.Table, &di.Index1, &di.Index2, &di.Columns,
				&di.Index1Size, &di.Index2Size, &di.Index1Scans, &di.Index2Scans)
			res.DuplicateIndexes = append(res.DuplicateIndexes, di)
		}
		rows.Close()
	}

	// 5. Invalid Indexes - Failed concurrent index builds
	if rows, err := conn.Query(ctx, `SELECT n.nspname as schema,
			t.relname as table_name,
			i.relname as index_name,
			pg_relation_size(i.oid) as size_bytes,
			pg_get_indexdef(i.oid) as ddl,
			CASE WHEN NOT ix.indisvalid THEN 'invalid'
				 WHEN NOT ix.indisready THEN 'not ready'
				 ELSE 'unknown' END as reason
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE (NOT ix.indisvalid OR NOT ix.indisready)
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY pg_relation_size(i.oid) DESC`); err == nil {
		for rows.Next() {
			var ii InvalidIndex
			_ = rows.Scan(&ii.Schema, &ii.Table, &ii.Name, &ii.SizeBytes, &ii.DDL, &ii.Reason)
			res.InvalidIndexes = append(res.InvalidIndexes, ii)
		}
		rows.Close()
	}

	// 6. Foreign Keys Missing Indexes - FK columns without supporting index
	if rows, err := conn.Query(ctx, `WITH fk_columns AS (
			SELECT c.conname as constraint_name,
				   n.nspname as schema,
				   t.relname as table_name,
				   array_to_string(array_agg(a.attname ORDER BY x.n), ', ') as columns,
				   t2.relname as ref_table,
				   array_to_string(array_agg(a2.attname ORDER BY x.n), ', ') as ref_columns,
				   t.reltuples::bigint as table_rows,
				   t.oid as table_oid
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_class t2 ON t2.oid = c.confrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			CROSS JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS x(attnum, ref_attnum, n)
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
			JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum = x.ref_attnum
			WHERE c.contype = 'f'
			  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
			GROUP BY c.conname, n.nspname, t.relname, t2.relname, t.reltuples, t.oid
		)
		SELECT f.schema, f.table_name, f.constraint_name, f.columns, f.ref_table, f.ref_columns, f.table_rows,
			   'CREATE INDEX ON ' || quote_ident(f.schema) || '.' || quote_ident(f.table_name) ||
			   ' (' || f.columns || ')' as suggested_ddl
		FROM fk_columns f
		WHERE NOT EXISTS (
			SELECT 1 FROM pg_index ix
			JOIN pg_class ci ON ci.oid = ix.indexrelid
			WHERE ix.indrelid = f.table_oid
			  AND (
				  -- Check if FK columns are a prefix of index columns
				  string_to_array(f.columns, ', ') <@ (
					  SELECT array_agg(a.attname ORDER BY x.n)
					  FROM unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n)
					  JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum
				  )
			  )
		)
		ORDER BY f.table_rows DESC
		LIMIT 30`); err == nil {
		for rows.Next() {
			var fk FKMissingIndex
			_ = rows.Scan(&fk.Schema, &fk.Table, &fk.Constraint, &fk.Columns, &fk.RefTable, &fk.RefColumns, &fk.TableRows, &fk.SuggestedDDL)
			res.FKMissingIndexes = append(res.FKMissingIndexes, fk)
		}
		rows.Close()
	}

	// 7. Sequence Exhaustion Risk
	// Note: pg_sequences view available in PG10+
	if rows, err := conn.Query(ctx, `SELECT schemaname, sequencename,
			last_value,
			max_value,
			increment_by,
			CASE WHEN max_value > 0 AND last_value > 0
				 THEN (last_value::float8 / max_value::float8 * 100)
				 ELSE 0 END as pct_used,
			CASE WHEN increment_by > 0
				 THEN ((max_value - last_value) / increment_by)
				 ELSE 0 END as calls_left
		FROM pg_sequences
		WHERE last_value IS NOT NULL
		  AND max_value > 0
		  AND (last_value::float8 / max_value::float8) > 0.5
		ORDER BY (last_value::float8 / max_value::float8) DESC
		LIMIT 20`); err == nil {
		for rows.Next() {
			var sq SequenceHealth
			_ = rows.Scan(&sq.Schema, &sq.Name, &sq.LastValue, &sq.MaxValue, &sq.Increment, &sq.PctUsed, &sq.CallsLeft)
			res.SequenceHealth = append(res.SequenceHealth, sq)
		}
		rows.Close()
	}

	// 8. Prepared Transactions (2PC) - Can block vacuum and hold locks
	if rows, err := conn.Query(ctx, `SELECT transaction::text, gid, owner, database,
			prepared,
			(now() - prepared)::text as age
		FROM pg_prepared_xacts
		ORDER BY prepared ASC`); err == nil {
		for rows.Next() {
			var px PreparedXact
			_ = rows.Scan(&px.Transaction, &px.GID, &px.Owner, &px.Database, &px.Prepared, &px.Age)
			res.PreparedXacts = append(res.PreparedXacts, px)
		}
		rows.Close()
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

// swapDBInURL naively replaces the last path segment of a libpq URL with the target DB.
// It keeps query params and credentials intact. If parsing fails, returns empty string.
func swapDBInURL(url string, db string) string {
	// Handle simple postgres://user:pass@host:port/db?params
	// We avoid importing net/url to keep dependencies lean; do a minimal split.
	// Find path start after host: the first '/' after '://' occurrence.
	idx := strings.Index(url, "://")
	if idx == -1 {
		return ""
	}
	// find the first '/' after '://'
	slash := strings.Index(url[idx+3:], "/")
	if slash == -1 {
		// no path -> append
		return url + "/" + db
	}
	head := url[:idx+3+slash] // up to '/'
	rest := url[idx+3+slash+1:]
	// rest may contain db and query params
	qmark := strings.Index(rest, "?")
	if qmark == -1 {
		// replace entire rest with db
		return head + "/" + db
	}
	// keep query string
	return head + "/" + db + rest[qmark:]
}

type pssOrder int

const (
	orderByTotal pssOrder = iota
	orderByCPUApprox
	orderByIO
	orderByCalls
	orderByIOBlocks
)

// fetchPSS tries new (total_exec_time/mean_exec_time) first, then old (total_time/mean_time)
func fetchPSS(ctx context.Context, conn *pgx.Conn, schema string, ord pssOrder, includeIO bool, includeBlk bool) ([]Statement, bool) {
	if sts, ok := fetchPSSVariant(ctx, conn, schema, "total_exec_time", "mean_exec_time", ord, includeIO, includeBlk); ok {
		return sts, true
	}
	if sts, ok := fetchPSSVariant(ctx, conn, schema, "total_time", "mean_time", ord, includeIO, includeBlk); ok {
		return sts, true
	}
	return nil, false
}

func fetchPSSVariant(ctx context.Context, conn *pgx.Conn, schema, colTotal, colMean string, ord pssOrder, includeIO bool, includeBlk bool) ([]Statement, bool) {
	orderExpr := ""
	switch ord {
	case orderByTotal:
		orderExpr = colTotal
	case orderByCPUApprox:
		if includeIO {
			orderExpr = fmt.Sprintf("(%s - blk_read_time - blk_write_time)", colTotal)
		} else {
			orderExpr = colTotal
		}
	case orderByIO:
		if includeIO {
			orderExpr = "(blk_read_time + blk_write_time)"
		} else {
			orderExpr = colTotal
		}
	case orderByCalls:
		orderExpr = "calls"
	case orderByIOBlocks:
		if includeBlk {
			orderExpr = "(coalesce(shared_blks_read,0)+coalesce(shared_blks_written,0)+coalesce(local_blks_read,0)+coalesce(local_blks_written,0)+coalesce(temp_blks_read,0)+coalesce(temp_blks_written,0))"
		} else {
			orderExpr = colTotal
		}
	}
	fromRel := qualifiedPSS(schema)
	selectIO := ""
	if includeIO {
		selectIO = ", blk_read_time, blk_write_time"
	}
	selectBlk := ""
	if includeBlk {
		selectBlk = ", shared_blks_read, shared_blks_written, local_blks_read, local_blks_written, temp_blks_read, temp_blks_written"
	}
	q := fmt.Sprintf(`select query, calls, %s as total_time, %s as mean_time, rows%s%s from %s order by %s desc nulls last limit 20`, colTotal, colMean, selectIO, selectBlk, fromRel, orderExpr)
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	var out []Statement
	for rows.Next() {
		var st Statement
		// Build scan targets dynamically based on selected columns
		scanArgs := []any{&st.Query, &st.Calls, &st.TotalTime, &st.MeanTime, &st.Rows}
		if includeIO {
			scanArgs = append(scanArgs, &st.BlkReadTime, &st.BlkWriteTime)
		}
		if includeBlk {
			scanArgs = append(scanArgs, &st.SharedBlksRead, &st.SharedBlksWrite, &st.LocalBlksRead, &st.LocalBlksWrite, &st.TempBlksRead, &st.TempBlksWrite)
		}
		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		if includeIO {
			st.IOTime = st.BlkReadTime + st.BlkWriteTime
			st.CPUTime = st.TotalTime - st.IOTime
		} else {
			st.IOTime = 0
			st.CPUTime = st.TotalTime
		}
		// Filter out trivial utility statements
		q := strings.ToUpper(strings.TrimSpace(st.Query))
		if strings.HasPrefix(q, "COMMIT") || strings.HasPrefix(q, "BEGIN") || strings.HasPrefix(q, "DISCARD ALL") {
			continue
		}
		out = append(out, st)
	}
	return out, true
}

func qualifiedPSS(schema string) string {
	if schema == "" {
		return "pg_stat_statements"
	}
	return quoteIdent(schema) + ".pg_stat_statements"
}

func quoteIdent(s string) string {
	out := `"`
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out += "\"" // double quotes
		}
		out += string(s[i])
	}
	out += `"`
	return out
}

func findPgStatStatementsSchema(ctx context.Context, conn *pgx.Conn) string {
	var schema string
	_ = queryRow(ctx, conn, `select n.nspname from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relname='pg_stat_statements' limit 1`, &schema)
	return schema
}

func hasPSSIOCols(ctx context.Context, conn *pgx.Conn, schema string) bool {
	// Check whether blk_read_time and blk_write_time exist in the view
	var has bool
	if schema == "" {
		_ = queryRow(ctx, conn, `select exists(
			select 1 from information_schema.columns
			where table_name='pg_stat_statements' and column_name in ('blk_read_time','blk_write_time')
			group by table_name having count(*)=2)`, &has)
		return has
	}
	// schema-qualified check
	q := `select exists(
			select 1 from information_schema.columns
			where table_schema=$1 and table_name='pg_stat_statements' and column_name in ('blk_read_time','blk_write_time')
			group by table_schema, table_name having count(*)=2)`
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	row := conn.QueryRow(ctx2, q, schema)
	_ = row.Scan(&has)
	return has
}

func hasPSSBlockCols(ctx context.Context, conn *pgx.Conn, schema string) bool {
	// Check for block counters columns presence
	var has bool
	if schema == "" {
		_ = queryRow(ctx, conn, `select exists(
			select 1 from information_schema.columns
			where table_name='pg_stat_statements' and column_name in ('shared_blks_read','shared_blks_written','local_blks_read','local_blks_written','temp_blks_read','temp_blks_written')
			group by table_name having count(*)=6)`, &has)
		return has
	}
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	row := conn.QueryRow(ctx2, `select exists(
		select 1 from information_schema.columns
		where table_schema=$1 and table_name='pg_stat_statements' and column_name in ('shared_blks_read','shared_blks_written','local_blks_read','local_blks_written','temp_blks_read','temp_blks_written')
		group by table_schema, table_name having count(*)=6)`, schema)
	_ = row.Scan(&has)
	return has
}
