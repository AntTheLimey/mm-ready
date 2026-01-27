# mm-ready

A database scanner that assesses PostgreSQL readiness for
[pgEdge Spock 5](https://www.pgedge.com/) multi-master replication.

Point it at any PostgreSQL database and get a detailed report of schema,
configuration, extension, and SQL pattern issues that need to be addressed
before (or after) deploying Spock.

## Features

- **56 automated checks** across 8 categories — schema, replication, config,
  extensions, SQL patterns, functions, sequences, and more
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
mm-ready scan \
  --host db.example.com --port 5432 \
  --dbname myapp --user postgres --password secret \
  --format html --output report.html
```

### Audit (post-Spock health check)

```bash
mm-ready audit \
  --host db.example.com --dbname myapp --user postgres --password secret \
  --format html --output audit.html
```

### Monitor (observe activity over time)

```bash
# Observe SQL activity for 5 minutes, then report patterns found
mm-ready monitor \
  --host db.example.com --dbname myapp --user postgres --password secret \
  --duration 300
```

## Severity Levels

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Must be resolved before Spock installation can proceed |
| **WARNING** | Should be reviewed; may cause issues in multi-master operation |
| **CONSIDER** | Should be investigated; may need action depending on context |
| **INFO** | Informational — pure awareness items, no action required |

## Readiness Verdict

The report includes an overall verdict:

- **READY** — no critical or warning issues found
- **CONDITIONALLY READY** — no critical issues, but warnings should be reviewed
- **NOT READY** — critical issues must be resolved first

## Next Steps

- [Quickstart Guide](quickstart.md) — Get running in under 5 minutes
- [Checks Reference](checks-reference.md) — Detailed documentation of all 56 checks
- [Architecture](architecture.md) — Internal design, module overview, data flow
