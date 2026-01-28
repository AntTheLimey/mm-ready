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

- **Operational modes.** The tool has three main modes:
  - `scan` (default) — pre-Spock readiness assessment of a vanilla PostgreSQL
    database that does NOT have Spock installed. This is the primary use case.
  - `audit` — post-Spock health check of a database that already has Spock
    installed and running.
  - `analyze` — offline analysis of a `pg_dump --schema-only` SQL file without
    a database connection. Useful for Customer Success when customers send
    schema dumps. Runs 19 of the 56 checks (those that can work from schema
    structure alone); the remaining 37 are marked as skipped.

  Checks are tagged with `mode = "scan"`, `mode = "audit"`, or `mode = "both"`.
  Scan-mode checks must never assume Spock is installed. Audit-mode checks may
  query Spock catalog tables (spock.subscription, spock.repset_table, etc.).

- **Severity levels.** CRITICAL = must fix before Spock install.
  WARNING = should fix or review. CONSIDER = should investigate, may need
  action depending on context. INFO = pure awareness, no action required.

## Architecture

```
mm_ready/
  cli.py             # Argument parsing, subcommands (scan, audit, monitor, analyze, list-checks)
  scanner.py         # Orchestrator: discovers checks, runs them, builds ScanReport
  analyzer.py        # Offline analysis: runs static checks against ParsedSchema
  schema_parser.py   # Parses pg_dump --schema-only SQL into in-memory model
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

A Docker-based test environment is available. The test schema is fully
self-contained — no external database dumps are needed.

```bash
# Start pgEdge Postgres
docker run -d --name mmready-test \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mmready \
  -p 5499:5432 \
  ghcr.io/pgedge/pgedge-postgres:18.1-spock5.0.4-standard-1

# Configure for external access and pg_stat_statements
docker exec mmready-test psql -U postgres \
  -c "ALTER SYSTEM SET listen_addresses = '*';" \
  -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"
docker restart mmready-test

# Load test schema, enable pg_stat_statements, then run workload
docker exec mmready-test psql -U postgres -d mmready \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
docker cp tests/test_schema_setup.sql mmready-test:/tmp/schema.sql
docker exec mmready-test psql -U postgres -d mmready -f /tmp/schema.sql
docker cp tests/test_workload.sql mmready-test:/tmp/workload.sql
docker exec mmready-test psql -U postgres -d mmready -f /tmp/workload.sql

# Run scan
mm-ready scan --host localhost --port 5499 --dbname mmready \
  --user postgres --password postgres --format html --output report.html
```

- `tests/test_schema_setup.sql` — idempotent setup of the `mmr_` prefixed
  schema, including tables that trigger every scan-mode check.
- `tests/test_workload.sql` — idempotent workload that populates
  pg_stat_statements and exercises the mmr_ tables. The database is unchanged
  after each run (inserts are deleted, updates are reverted).

## Offline Analysis (analyze mode)

The `analyze` subcommand parses a pg_dump SQL file and runs schema-structural
checks without a database connection:

```bash
mm-ready analyze --file customer_schema.sql --format html -v
```

The schema parser (`schema_parser.py`) extracts:
- Tables (columns, constraints, UNLOGGED, INHERITS, PARTITION BY)
- Constraints (PK, UNIQUE, FK with CASCADE options, EXCLUDE, DEFERRABLE)
- Indexes (unique, method, columns)
- Sequences (data type, ownership)
- Extensions, ENUM types, Rules

The analyzer (`analyzer.py`) runs 19 static checks that can operate on parsed
schema structure. Checks requiring live database access (GUCs, pg_stat, Spock
catalogs, etc.) are marked as skipped with reason "Requires live database
connection".

## Code Style

- Python 3.10+ with `from __future__ import annotations`.
- Type hints on function signatures.
- Dataclasses for data models.
- SQL queries use `pg_catalog` system tables, not `information_schema` (for
  performance and access to internal details).
- Check findings should include actionable remediation text.
- Reference specific Spock source file + line when citing behaviour.
