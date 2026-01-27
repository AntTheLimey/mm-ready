# Architecture

This document describes the internal architecture of mm-ready, a PostgreSQL
readiness scanner for pgEdge Spock 5 multi-master replication.

## Module Overview

```
mm_ready/
  __init__.py              # Package version (0.1.0)
  cli.py                   # CLI entry point, argument parsing, output routing
  scanner.py               # Check orchestrator — discovers, runs, aggregates
  registry.py              # Auto-discovery of BaseCheck subclasses
  connection.py            # Database connection factory (read-only, autocommit)
  models.py                # Data models: Severity, Finding, CheckResult, ScanReport
  checks/
    base.py                # Abstract BaseCheck class
    schema/                # 22 schema checks
    replication/           # 12 replication checks (scan + audit)
    config/                # 8 configuration checks
    extensions/            # 4 extension checks
    sql_patterns/          # 5 SQL pattern checks
    functions/             # 3 function/trigger checks
    sequences/             # 2 sequence checks
  reporters/
    json_reporter.py       # Machine-readable JSON output
    markdown_reporter.py   # Human-readable Markdown output
    html_reporter.py       # Styled standalone HTML report
  monitor/
    observer.py            # Monitor mode orchestrator
    pgstat_collector.py    # pg_stat_statements snapshot & delta
    log_parser.py          # PostgreSQL log file parser
```

## Data Flow

```
User invokes CLI
       │
       ▼
   cli.py ─── build_parser() ─── argparse subcommands
       │
       ├── list-checks ──► registry.discover_checks() ──► print to stdout
       │
       ├── scan / audit
       │     │
       │     ├── connection.connect() ──► psycopg2 (read-only, autocommit)
       │     ├── scanner.run_scan()
       │     │     ├── registry.discover_checks(mode, categories)
       │     │     ├── for each check: check.run(conn) ──► list[Finding]
       │     │     └── aggregate into ScanReport
       │     ├── _render_report(report, format) ──► reporter module
       │     └── _write_output() ──► file (timestamped) or stdout
       │
       └── monitor
             ├── connection.connect()
             ├── observer.run_monitor()
             │     ├── Run standard scan-mode checks
             │     ├── pgstat_collector.collect_over_duration()
             │     │     ├── take_snapshot() [before]
             │     │     ├── sleep(duration)
             │     │     └── take_snapshot() [after] ──► StatsDelta
             │     ├── log_parser.parse_log_file() ──► LogAnalysis
             │     └── Convert deltas/analysis into Findings
             ├── _render_report()
             └── _write_output()
```

## Core Modules

### cli.py

Entry point registered as the `mm-ready` console script via `pyproject.toml`.

Responsibilities:
- Build argparse parser with four subcommands: `scan`, `audit`, `monitor`,
  `list-checks`
- Connection arguments: `--dsn` or `--host/--port/--dbname/--user/--password`
- Output arguments: `--format` (json/markdown/html), `--output` (file path)
- Route commands to handler functions
- Generate timestamped output filenames (`report.html` becomes
  `report_20260127_131504.html`)

The `_render_report()` function uses lazy imports to load only the requested
reporter module.

### scanner.py

Orchestrates check execution for scan and audit modes.

`run_scan(conn, host, port, dbname, categories, mode, verbose)`:
1. Creates a `ScanReport` with metadata (database, host, timestamp, PG version)
2. Calls `registry.discover_checks()` with mode and category filters
3. Iterates through checks, calling `check.run(conn)` on each
4. Wraps results in `CheckResult` objects, capturing errors if a check fails
5. Returns the completed `ScanReport`

Individual check failures are caught and recorded as errors — they do not stop
the scan.

### registry.py

Plugin discovery system. No manual registration required.

`discover_checks(categories, mode)`:
1. Uses `pkgutil.walk_packages()` to recursively import all modules under
   `mm_ready.checks`
2. Traverses the `BaseCheck` class hierarchy via `__subclasses__()`
3. Instantiates each concrete subclass
4. Filters by category and/or mode if requested
5. Returns checks sorted by `(category, name)`

This means adding a new check is as simple as creating a Python file in the
appropriate `checks/` subdirectory with a class that subclasses `BaseCheck`.

### connection.py

Database connection factory.

`connect(host, port, dbname, user, password, dsn)`:
- Accepts either a DSN string or individual connection parameters
- Falls back to `PGPASSWORD` environment variable if no password provided
- Configures the connection as **read-only** with **autocommit** enabled
- Returns a `psycopg2` connection object

`get_pg_version(conn)`: Returns the PostgreSQL version string.

### models.py

Data structures used throughout the application.

**`Severity`** (enum): `CRITICAL`, `WARNING`, `CONSIDER`, `INFO` — with custom
ordering so CRITICAL sorts first.

**`Finding`** (dataclass):
- `severity`, `check_name`, `category`, `title`, `detail`
- Optional: `object_name`, `remediation`, `metadata` dict

**`CheckResult`** (dataclass):
- `check_name`, `category`, `description`
- `findings` list, `error` string, `skipped` flag, `skip_reason`

**`ScanReport`** (dataclass):
- `database`, `host`, `port`, `timestamp`, `pg_version`
- `results` list, `scan_mode`, `spock_target`
- Computed properties: `findings`, `critical_count`, `warning_count`,
  `consider_count`, `info_count`, `checks_passed`, `checks_total`

### checks/base.py

Abstract base class that all checks must subclass.

```python
class BaseCheck:
    name = ""           # Unique identifier (auto-set to class name if empty)
    category = ""       # schema, replication, config, extensions, etc.
    description = ""    # Human-readable summary
    mode = "scan"       # "scan", "audit", or "both"

    def run(self, conn) -> list[Finding]:
        raise NotImplementedError
```

Uses `__init_subclass__()` to auto-set `name` from the class name if not
explicitly provided.

A ready-to-use template is provided at `templates/check_template.py`. Copy it
into the appropriate category subdirectory, rename the class, fill in the
metadata, and implement `run()`. See [Adding Custom Checks](#adding-custom-checks)
below or the template file itself for detailed instructions.

## Reporters

All reporters implement a single function: `render(report: ScanReport) -> str`.

### json_reporter.py

Produces structured JSON with three top-level keys:
- `meta`: tool version, timestamp, database info, PG version
- `summary`: total checks, passed, critical/warning/info counts
- `results`: array of check results with nested findings

### markdown_reporter.py

Produces a Markdown document with:
- Header with database and version info
- Summary table with check counts
- Readiness verdict: **READY**, **CONDITIONALLY READY**, or **NOT READY**
- Findings grouped by severity (CRITICAL first), then by category
- Error section if any checks failed

### html_reporter.py

Produces a standalone HTML document with embedded CSS and JavaScript:
- Fixed left sidebar with collapsible tree navigation (severity → category)
- Scroll tracking via IntersectionObserver highlights active section in sidebar
- Summary cards with severity-colored badges
- Semantic color scheme: red (critical), amber (warning), teal (consider), blue (info)
- Findings grouped by severity → category with anchor-based navigation
- To Do checklist collecting CRITICAL, WARNING, and CONSIDER remediations
- Interactive checkboxes with live completion counter
- Print-friendly layout (sidebar hidden)
- Uses `html.escape()` for output safety

## Monitor Subsystem

### observer.py

Three-phase monitoring orchestrator:

1. **Phase 1**: Run all standard scan-mode checks (same as `mm-ready scan`)
2. **Phase 2**: If `pg_stat_statements` is available, collect snapshots before
   and after the observation window, then compute deltas to identify new or
   changed queries
3. **Phase 3**: If a log file path is provided, parse it for
   replication-relevant SQL patterns

Converts `StatsDelta` and `LogAnalysis` objects into standard `Finding` objects
for inclusion in the report.

### pgstat_collector.py

Snapshot-based observation of `pg_stat_statements`:

- `is_available(conn)`: Checks if the extension is installed and queryable
- `take_snapshot(conn)`: Reads all rows, keyed by `queryid` (or query text)
- `collect_over_duration(conn, duration, verbose)`: Takes before/after
  snapshots, returns a `StatsDelta` with new queries and changed query metrics

### log_parser.py

PostgreSQL log file parser with pattern classification:

- Handles multi-line log entries (tab-indented continuation lines)
- Classifies statements into: DDL, TRUNCATE CASCADE, CREATE INDEX CONCURRENTLY,
  temp tables, advisory locks
- Returns a `LogAnalysis` object with categorized statement lists
- Handles encoding issues with `errors="replace"`

## Design Principles

1. **Read-only safety** — Database connections are configured read-only. The
   tool never modifies the target database.

2. **Plugin architecture** — New checks are auto-discovered. No registration,
   no imports to update. Drop a file in `checks/` and it works.

3. **Error resilience** — Individual check failures are captured and reported
   but do not stop the scan. The report includes an error section.

4. **Scan vs Audit separation** — Each check declares its mode (`scan`,
   `audit`, or `both`). Scan mode assumes no Spock installed. Audit mode
   assumes Spock is running and checks its health.

5. **Multiple output formats** — Strategy pattern for reporters. JSON for
   automation, Markdown for terminals/tickets, HTML for stakeholders.

6. **Timestamped output** — Output filenames include a timestamp so repeated
   scans never overwrite previous results.

7. **Minimal dependencies** — Only `psycopg2-binary` is required. All other
   functionality uses the Python standard library.
