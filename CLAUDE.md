# MM-Ready Project Instructions

## Project Overview

mm-ready is a Python CLI tool that scans a PostgreSQL database and generates a
compatibility report for converting it to multi-master replication using
pgEdge Spock 5. It runs from macOS (or any machine with Python 3.10+) against
any connectable PostgreSQL instance.

## Key Principles

- **Trust source code over documentation.** The Spock source code at
  ~/PROJECTS/spock/ is the authoritative reference. Spock documentation is
  frequently wrong or out of date. Always verify claims against the C source.

- **Scan mode vs Audit mode.** The tool has two operational modes:
  - `scan` (default) — pre-Spock readiness assessment of a vanilla PostgreSQL
    database that does NOT have Spock installed. This is the primary use case.
  - `audit` — post-Spock health check of a database that already has Spock
    installed and running.

  Checks are tagged with `mode = "scan"`, `mode = "audit"`, or `mode = "both"`.
  Scan-mode checks must never assume Spock is installed. Audit-mode checks may
  query Spock catalog tables (spock.subscription, spock.repset_table, etc.).

- **Severity levels.** CRITICAL = must fix before Spock install.
  WARNING = should fix or review. CONSIDER = should investigate, may need
  action depending on context. INFO = pure awareness, no action required.

## Architecture

```
mm_ready/
  cli.py             # Argument parsing, subcommands (scan, audit, monitor, list-checks)
  scanner.py         # Orchestrator: discovers checks, runs them, builds ScanReport
  registry.py        # Auto-discovers BaseCheck subclasses from checks/ directory
  connection.py      # psycopg2 connection from CLI args or DSN
  models.py          # Severity, Finding, CheckResult, ScanReport dataclasses
  checks/
    base.py          # BaseCheck abstract class (name, category, description, mode)
    schema/          # Table structure checks (PKs, FKs, constraints, types, etc.)
    replication/     # WAL, slots, workers, encoding, audit-mode subscription health
    config/          # PG version, GUCs (wal_level, track_commit_timestamp, etc.)
    extensions/      # Installed extensions, snowflake, pg_stat_statements
    sql_patterns/    # Queries in pg_stat_statements (TRUNCATE, DDL, advisory locks)
    functions/       # Stored procedures, triggers, views
    sequences/       # Sequence types and audit
  reporters/
    json_reporter.py
    markdown_reporter.py
    html_reporter.py
  monitor/           # Long-running observation mode (pgstat snapshots + log parsing)
```

## Adding a New Check

1. Create a new `.py` file in the appropriate `checks/` subdirectory.
2. Subclass `BaseCheck` and set `name`, `category`, `description`, and `mode`.
3. Implement `run(self, conn) -> list[Finding]`.
4. The check is auto-discovered by the registry — no registration needed.

Example:
```python
from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity

class MyCheck(BaseCheck):
    name = "my_check"
    category = "schema"
    description = "Check for something important"
    mode = "scan"  # or "audit" or "both"

    def run(self, conn) -> list[Finding]:
        with conn.cursor() as cur:
            cur.execute("SELECT ...")
            rows = cur.fetchall()
        findings = []
        # ... build findings ...
        return findings
```

## Important Spock Facts (verified from source code)

- Tables without PKs go to `default_insert_only` replication set automatically.
  UPDATE/DELETE on these tables are silently dropped by the output plugin.
- REPLICA IDENTITY FULL is NOT supported as a standalone replication identity
  by Spock. `get_replication_identity()` returns InvalidOid for FULL without PK.
- Delta-Apply columns MUST have NOT NULL constraints (spock_apply_heap.c:613-627).
- Trigger firing: apply workers run with `session_replication_role='replica'`.
  Both ENABLE REPLICA and ENABLE ALWAYS triggers fire during apply.
- Encoding: source code requires same encoding on both sides, NOT UTF-8 specifically.
- TRUNCATE RESTART IDENTITY: source code passes `restart_seqs` through (docs say
  "not supported" but code handles it).
- Deferrable indexes: silently skipped for conflict resolution via
  `IsIndexUsableForInsertConflict()` checking `indimmediate`.
- Spock 5 supports PostgreSQL 15, 16, 17, 18 (PG 18 added in 5.0.3).

## Testing

A Docker-based test environment is available:
```bash
# Start pgEdge Postgres with Northwind DB
docker run -d --name mmready-test \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=northwind \
  -p 5499:5432 \
  ghcr.io/pgedge/pgedge-postgres:18.1-spock5.0.4-standard-1

# Configure for external access and pg_stat_statements
docker exec mmready-test psql -U postgres \
  -c "ALTER SYSTEM SET listen_addresses = '*';" \
  -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"
docker restart mmready-test

# Load Northwind schema and test workload
curl -sL https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql -o /tmp/northwind.sql
docker cp /tmp/northwind.sql mmready-test:/tmp/northwind.sql
docker exec mmready-test psql -U postgres -d northwind -f /tmp/northwind.sql
docker exec mmready-test psql -U postgres -d northwind \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
docker cp tests/northwind_workload.sql mmready-test:/tmp/workload.sql
docker exec mmready-test psql -U postgres -d northwind -f /tmp/workload.sql

# Run scan
mm-ready scan --host localhost --port 5499 --dbname northwind \
  --user postgres --password postgres --format html --output report.html
```

The workload file `tests/northwind_workload.sql` is idempotent — the DB is
unchanged after each run (inserts are deleted, updates are reverted).

## Code Style

- Python 3.10+ with `from __future__ import annotations`.
- Type hints on function signatures.
- Dataclasses for data models.
- SQL queries use `pg_catalog` system tables, not `information_schema` (for
  performance and access to internal details).
- Check findings should include actionable remediation text.
- Reference specific Spock source file + line when citing behaviour.
