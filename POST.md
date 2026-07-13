# The Database Health Check Nobody Prioritizes (Until Performance Tanks)

Postgres has been part of my stack for as long as I can remember. I've designed schemas, tuned queries, argued about ORMs, migrated terabytes between versions. If you're building anything that needs a relational database today, Postgres is pretty much the default choice — and for good reason. It's solid, battle-tested, extensible, and the ecosystem around it keeps growing. (Though I'll admit, for embedded scenarios and local-first apps I'm a big fan of SQLite — right tool for the right job.)

Over the years I've come to appreciate that when things go sideways with performance, the database is often where the biggest wins hide. Not always, but often enough that skipping the database health check is a missed opportunity.

And this keeps coming up in a specific scenario. A team inherits a product, or gets brought in to help with performance, or a system that worked fine at one scale starts creaking at another. The codebase tells you part of the story. The database tells you the rest — and sometimes the more actionable part.

Getting a clear picture of the database state is one of the cornerstones of incremental re-architecture. Before you touch a line of application code, before you start proposing new services or refactoring data access layers, you need to know what the database is actually going through. Which queries are hammering it, where the bloat is, whether vacuum is keeping up, how close you are to resource limits. That assessment can reshape the entire optimization plan.

## The superuser problem

Here's a wrinkle that comes up more often than you'd expect. You're brought onto a project to help with optimization. You need to look at database health. And you don't have superuser access.

Depending on the organization, getting elevated privileges on a production database can take anywhere from a few hours to never. Security policies, compliance requirements, organizational bureaucracy — there are real reasons for it, but it doesn't help you when you need to move fast.

What I've found is that you can still get a surprisingly useful picture without superuser. Most of the important `pg_stat_*` views are accessible to regular users. If someone grants you `pg_monitor` membership, even better — but it's not strictly required.

## The metrics that actually matter

There's a core set of metrics that tells you most of what you need to know. Not all of it, but enough to catch the common problems and build your optimization roadmap.

**Cache hit ratio.** This one is fundamental. Postgres keeps frequently accessed data in shared buffers, and you want the vast majority of reads to come from cache rather than disk. If your cache hit ratio drops below 95%, something needs attention — either `shared_buffers` is too small, or your working set has outgrown available memory.

**Connection usage.** Check how many connections are active versus your `max_connections` setting. If you're routinely above 80%, you're playing with fire. And while you're at it, look at idle-in-transaction sessions — those hold locks and eat connections for no good reason.

**Table bloat and vacuum activity.** Dead tuples accumulate as you update and delete rows. Autovacuum is supposed to clean them up, but it doesn't always keep up. Tables with more than 20% dead tuples are bloated, and severely bloated tables (50%+) can tank your query performance. Check `last_vacuum` and `last_autoanalyze` timestamps — if tables with millions of rows haven't been analyzed in over a week, your query planner is working with stale statistics.

**Unused and duplicate indexes.** Every index has a maintenance cost. It slows down writes, consumes storage, and makes vacuum work harder. If an index hasn't been scanned since the last stats reset and it's taking up hundreds of megabytes, it's a candidate for removal. Duplicate indexes — two indexes covering the same columns on the same table — are pure waste.

**Missing indexes on foreign keys.** This one surprised me. Postgres doesn't automatically create indexes on foreign key columns. If you have foreign keys without supporting indexes, you're potentially causing sequential scans on joins and slowing down cascading deletes significantly.

**XID wraparound age.** This is the silent killer. Postgres uses transaction IDs that wrap around at about 2 billion. If your databases get too close to that limit because vacuum hasn't been freezing old rows, Postgres will shut itself down to prevent data corruption. If you see XID age creeping above 50% of the limit, treat it seriously.

**Wait events and locks.** Looking at what your backends are actually waiting on tells you where the bottlenecks are. Heavy IO waits might point to undersized memory or slow storage. Lock waits suggest contention issues. This is the kind of thing you can query from `pg_stat_activity` without any special privileges.

## Extensions worth installing

A few extensions make a significant difference for observability:

**pg_stat_statements** — if your Postgres instance doesn't have this enabled, stop reading and go enable it. It tracks execution statistics for all queries: total time, call count, rows returned, block IO. It's the single most useful extension for understanding what your database is actually doing. Without it, you're guessing.

**pg_buffercache** — lets you inspect the contents of the shared buffer cache. Useful for understanding what's actually being cached and whether your shared_buffers sizing makes sense.

And don't overlook the **pg_monitor** role. It's not an extension, but granting it to your application's diagnostic user gives you access to server statistics that would otherwise require superuser. It's a reasonable middle ground that most security teams should be willing to approve.

## Automating the routine

For repetitive health checks, I built a small tool called **pghealth**. If you've ever worked with Oracle, think of it as a lightweight take on AWR (Automatic Workload Repository) reports — but for Postgres and without the Oracle price tag. It connects to a Postgres instance, runs a collection of diagnostic queries, analyzes the results against known thresholds, and generates a self-contained HTML report.

The main reason I built it: I didn't want to maintain a growing collection of SQL scripts and I needed something that works without prerequisites. No extensions to install, no agents to deploy, no superuser required. It's a single binary — point it at a connection string, get a report. It gracefully degrades when permissions are limited, noting what it couldn't access rather than failing.

It covers most of what I described above — cache ratios, bloat analysis, index health, connection patterns, vacuum status, wait events, replication lag, sequence exhaustion, and if `pg_stat_statements` is available, it pulls top queries by time, CPU, IO, and call frequency, along with estimated execution plans for the slowest ones.

The tool is open source: [github.com/koltyakov/pghealth](https://github.com/koltyakov/pghealth)

It's not trying to replace proper monitoring stacks like Prometheus + Grafana or pganalyze. It's for those moments when you need a quick, thorough snapshot of where things stand — when joining a new project, during incident response, or as part of a periodic review.

---

I'm curious about your experience. If you're working with Postgres — whether as a DBA or as a developer who needs to understand what's happening under the hood — **what metrics do you check first when assessing a system you didn't build?** Is there something you always look at that I haven't mentioned?

Would love to hear what's in your toolkit.
