# mm-ready

A database scanner that assesses PostgreSQL readiness for
[pgEdge Spock 5](https://www.pgedge.com/) multi-master replication.

Point it at any PostgreSQL database and get a detailed report of schema,
configuration, extension, and SQL pattern issues that need to be addressed
before (or after) deploying Spock.

## Features

- **56 automated checks** across 7 categories — schema, replication, config,
  extensions, SQL patterns, functions, and sequences
- **Two operational modes:**
  - `scan` — pre-Spock readiness assessment (vanilla PostgreSQL, no Spock needed)
  - `audit` — post-Spock health check (database with Spock already running)
- **Three output formats:** HTML, Markdown, JSON
- **Timestamped reports** — output filenames include a timestamp so previous
  scans are never overwritten
- **Monitor mode** — observe SQL activity over a time window via
  `pg_stat_statements` snapshots and PostgreSQL log parsing
- **Plugin architecture** — add a new check by dropping a Python file in the
  `checks/` directory; auto-discovered at runtime
- **Source-code-verified** — check logic is verified against the Spock C source
  code, not just documentation

## Quick Install

```bash
git clone <repo-url> && cd MM_Ready
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

Requires **Python 3.10+** and **psycopg2** (installed automatically via
`psycopg2-binary`).

## Usage

### Scan (pre-Spock readiness)

```bash
# Using individual connection parameters
mm-ready scan \
  --host db.example.com --port 5432 \
  --dbname myapp --user postgres --password secret \
  --format html --output report.html

# Using a DSN
mm-ready scan --dsn "postgresql://postgres:secret@db.example.com/myapp" \
  --format json --output report.json

# Minimal — defaults to scan, HTML format, writes to ./reports/
mm-ready --host localhost --dbname myapp --user postgres
```

### Audit (post-Spock health check)

```bash
mm-ready audit \
  --host db.example.com --dbname myapp --user postgres --password secret \
  --format html --output audit.html
```

### Monitor (observe activity over time)

```bash
mm-ready monitor \
  --host db.example.com --dbname myapp --user postgres --password secret \
  --duration 3600 --format html --output monitor.html
```

### List available checks

```bash
mm-ready list-checks              # All checks
mm-ready list-checks --mode scan  # Scan-mode only
mm-ready list-checks --mode audit # Audit-mode only
```

## Output

When `--output` is specified, the filename automatically includes a timestamp:

```
report.html  -->  report_20260127_131504.html
```

This means you can re-run scans without losing previous results. You can also
pass a directory path and the tool will generate a filename automatically:

```bash
mm-ready scan ... --output ./reports/
# Creates: ./reports/mm-ready_20260127_131504.html
```

### Severity Levels

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Must be resolved before Spock installation can proceed |
| **WARNING** | Should be reviewed; may cause issues in multi-master operation |
| **CONSIDER** | Should be investigated; may need action depending on context |
| **INFO** | Informational — pure awareness items, no action required |

### Readiness Verdict

The report includes an overall verdict:

- **READY** — no critical or warning issues found
- **CONDITIONALLY READY** — no critical issues, but warnings should be reviewed
- **NOT READY** — critical issues must be resolved first

## Check Categories

### Schema (22 checks)

Analyzes table structure for Spock compatibility:

| Check | Severity | What it detects |
|-------|----------|-----------------|
| `primary_keys` | WARNING | Tables missing primary keys (routed to insert-only replication) |
| `tables_update_delete_no_pk` | CRITICAL | Tables with UPDATE/DELETE activity but no PK (changes silently lost) |
| `deferrable_constraints` | CRITICAL/WARNING | Deferrable PK/unique constraints (silently skipped by Spock conflict resolution) |
| `exclusion_constraints` | WARNING | Exclusion constraints (not enforceable cross-node) |
| `foreign_keys` | WARNING/CONSIDER | Foreign key relationships requiring replication set coordination |
| `sequence_pks` | WARNING | PKs using standard sequences (need Snowflake migration) |
| `unlogged_tables` | WARNING | UNLOGGED tables (not written to WAL) |
| `large_objects` | WARNING | Large object usage (not supported by logical decoding) |
| `generated_columns` | CONSIDER | Generated/stored columns |
| `partitioned_tables` | CONSIDER | Partition strategy review |
| `inheritance` | WARNING | Table inheritance |
| `column_defaults` | WARNING | Volatile defaults (now(), random()) |
| `numeric_columns` | WARNING/CONSIDER | Delta-Apply candidates and NOT NULL requirements |
| `multiple_unique_indexes` | CONSIDER | Multiple unique indexes affecting conflict resolution |
| `enum_types` | CONSIDER | ENUM types requiring DDL coordination |
| `rules` | WARNING/CONSIDER | Rules on tables |
| `row_level_security` | WARNING | RLS policies (apply worker bypasses RLS) |
| `event_triggers` | WARNING/INFO | Event triggers interacting with DDL replication |
| `notify_listen` | WARNING | NOTIFY/LISTEN (not replicated) |
| `tablespace_usage` | CONSIDER | Non-default tablespaces (must exist on all nodes) |
| `temp_tables` | INFO | Functions creating temporary tables |
| `missing_fk_indexes` | WARNING | Foreign key columns without indexes (slow cascades, lock contention) |

### Replication (12 checks)

Validates PostgreSQL replication configuration:

| Check | Mode | What it detects |
|-------|------|-----------------|
| `wal_level` | scan | Must be 'logical' |
| `max_replication_slots` | scan | Sufficient slots for Spock |
| `max_worker_processes` | scan | Sufficient workers |
| `max_wal_senders` | scan | Sufficient WAL senders |
| `database_encoding` | scan | Encoding consistency requirement |
| `multiple_databases` | scan | Multiple databases in instance |
| `hba_config` | scan | pg_hba.conf replication entries |
| `repset_membership` | audit | Tables not in any replication set |
| `subscription_health` | audit | Disabled subscriptions, inactive slots |
| `conflict_log` | audit | Conflict history analysis |
| `exception_log` | audit | Apply error analysis |
| `stale_replication_slots` | audit | Inactive replication slots retaining WAL |

### Config (8 checks)

PostgreSQL server settings:

| Check | Mode | What it detects |
|-------|------|-----------------|
| `pg_version` | scan | PostgreSQL version compatibility (15, 16, 17, 18) |
| `track_commit_timestamp` | scan | Must be 'on' for conflict resolution |
| `parallel_apply` | scan | Worker configuration summary |
| `shared_preload_libraries` | audit | Spock in shared_preload_libraries |
| `spock_gucs` | audit | Spock-specific GUC settings (conflict resolution, AutoDDL) |
| `timezone_config` | scan | Timezone settings (UTC recommended for commit timestamps) |
| `idle_transaction_timeout` | scan | Idle-in-transaction timeout (blocks VACUUM, causes bloat) |
| `pg_minor_version` | audit | PostgreSQL minor version (all nodes should match) |

### Extensions (4 checks)

| Check | What it detects |
|-------|-----------------|
| `installed_extensions` | All extensions with compatibility notes |
| `snowflake_check` | pgEdge Snowflake availability |
| `pg_stat_statements_check` | pg_stat_statements availability for SQL analysis |
| `lolor_check` | LOLOR extension for large object replication |

### SQL Patterns (5 checks)

Analyzes `pg_stat_statements` for problematic patterns:

| Check | What it detects |
|-------|-----------------|
| `truncate_cascade` | TRUNCATE CASCADE and RESTART IDENTITY |
| `ddl_statements` | DDL in tracked queries |
| `advisory_locks` | Advisory lock usage (node-local) |
| `concurrent_indexes` | CREATE INDEX CONCURRENTLY |
| `temp_table_queries` | CREATE TEMP TABLE patterns |

### Functions (3 checks)

| Check | What it detects |
|-------|-----------------|
| `stored_procedures` | Write operations in functions |
| `trigger_functions` | ENABLE REPLICA / ENABLE ALWAYS triggers |
| `views_audit` | Materialized views requiring refresh coordination |

### Sequences (2 checks)

| Check | What it detects |
|-------|-----------------|
| `sequence_audit` | Full sequence inventory |
| `sequence_data_types` | smallint/integer sequences (overflow risk) |

## Architecture

```
mm_ready/
  cli.py             # CLI entry point and subcommands
  scanner.py         # Check orchestrator
  registry.py        # Auto-discovery of BaseCheck subclasses
  connection.py      # Database connection management
  models.py          # Data models (Finding, CheckResult, ScanReport)
  checks/
    base.py          # BaseCheck abstract class
    schema/          # 22 schema checks
    replication/     # 12 replication checks (scan + audit)
    config/          # 8 configuration checks
    extensions/      # 4 extension checks
    sql_patterns/    # 5 SQL pattern checks
    functions/       # 3 function/trigger checks
    sequences/       # 2 sequence checks
  reporters/
    json_reporter.py
    markdown_reporter.py
    html_reporter.py
  monitor/
    pgstat_collector.py  # pg_stat_statements snapshots
    log_parser.py        # PostgreSQL log file parsing
    observer.py          # Monitor mode orchestrator
```

## Adding or Modifying Checks

To modify an existing check's severity or logic, edit the corresponding file
in `checks/` directly — no rebuild or registration step required.

A starter template is provided at `mm_ready/templates/check_template.py` — copy
it into the appropriate `checks/` subdirectory and customize. Or create a file
from scratch:

```python
from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity

class MyCustomCheck(BaseCheck):
    name = "my_custom_check"
    category = "schema"
    description = "Description shown in list-checks"
    mode = "scan"  # "scan", "audit", or "both"

    def run(self, conn) -> list[Finding]:
        with conn.cursor() as cur:
            cur.execute("SELECT ...")
            rows = cur.fetchall()

        findings = []
        for row in rows:
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title="Issue found",
                detail="Detailed explanation...",
                object_name="schema.table",
                remediation="How to fix it.",
            ))
        return findings
```

No registration required. The check is discovered automatically at runtime.

## Documentation

- [Quickstart Guide](docs/quickstart.md) — Get running in under 5 minutes
- [Checks Reference](docs/checks-reference.md) — Detailed documentation of all 56 checks
- [Architecture](docs/architecture.md) — Internal design, module overview, data flow

## Requirements

- Python 3.10+
- psycopg2-binary >= 2.9
- Target database: PostgreSQL 15, 16, 17, or 18
- Read-only access to `pg_catalog`, `pg_stat_statements` (optional), and
  `pg_hba_file_rules` (optional)

## License

Proprietary. All rights reserved.
