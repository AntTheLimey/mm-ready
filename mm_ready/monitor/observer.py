"""Monitor mode orchestrator — combines scan with time-based observation."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from mm_ready.connection import get_pg_version
from mm_ready.models import CheckResult, Finding, ScanReport, Severity
from mm_ready.monitor.log_parser import LogAnalysis, parse_log_file
from mm_ready.monitor.pgstat_collector import (
    collect_over_duration,
)
from mm_ready.monitor.pgstat_collector import (
    is_available as pgstat_available,
)
from mm_ready.registry import discover_checks


def run_monitor(
    conn,
    host: str,
    port: int,
    dbname: str,
    duration: int = 3600,
    log_file: str | None = None,
    verbose: bool = False,
) -> ScanReport:
    """Run a full scan plus time-based observation.

    1. Run all standard checks (same as scan mode)
    2. If pg_stat_statements is available, observe for `duration` seconds
    3. If log_file is provided, parse it for SQL patterns
    4. Add observation findings to the report
    """
    report = ScanReport(
        database=dbname,
        host=host,
        port=port,
        timestamp=datetime.now(timezone.utc),
        pg_version=get_pg_version(conn),
        scan_mode="monitor",
    )

    # Phase 1: standard checks (scan-mode only)
    checks = discover_checks(mode="scan")
    total = len(checks)
    if verbose:
        print(f"Phase 1: Running {total} standard checks...", file=sys.stderr)

    for i, check in enumerate(checks, 1):
        if verbose:
            print(f"  [{i}/{total}] {check.category}/{check.name}", file=sys.stderr)
        result = CheckResult(
            check_name=check.name,
            category=check.category,
            description=check.description,
        )
        try:
            result.findings = check.run(conn)
        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
        report.results.append(result)

    # Phase 2: pg_stat_statements observation
    if pgstat_available(conn):
        if verbose:
            print(f"\nPhase 2: Observing pg_stat_statements for {duration}s...", file=sys.stderr)

        delta = collect_over_duration(conn, duration, verbose=verbose)
        obs_result = _build_pgstat_result(delta)
        report.results.append(obs_result)
    else:
        if verbose:
            print("\nPhase 2: pg_stat_statements not available, skipping.", file=sys.stderr)
        report.results.append(
            CheckResult(
                check_name="pgstat_observation",
                category="monitor",
                description="pg_stat_statements observation",
                skipped=True,
                skip_reason="pg_stat_statements not available",
            )
        )

    # Phase 3: log file analysis
    if log_file:
        if verbose:
            print(f"\nPhase 3: Parsing log file: {log_file}", file=sys.stderr)
        try:
            analysis = parse_log_file(log_file)
            log_result = _build_log_result(analysis)
            report.results.append(log_result)
        except Exception as exc:
            report.results.append(
                CheckResult(
                    check_name="log_analysis",
                    category="monitor",
                    description="PostgreSQL log file analysis",
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
    elif verbose:
        print("\nPhase 3: No log file specified, skipping log analysis.", file=sys.stderr)

    if verbose:
        print(
            f"\nDone. {report.critical_count} critical, "
            f"{report.warning_count} warnings, {report.info_count} info.",
            file=sys.stderr,
        )

    return report


def _build_pgstat_result(delta) -> CheckResult:
    """Convert pg_stat_statements delta into findings."""
    result = CheckResult(
        check_name="pgstat_observation",
        category="monitor",
        description=f"SQL activity observed over {delta.duration_seconds:.0f} seconds",
    )

    # Report new queries that appeared during observation
    if delta.new_queries:
        result.findings.append(
            Finding(
                severity=Severity.INFO,
                check_name="pgstat_observation",
                category="monitor",
                title=f"{len(delta.new_queries)} new query pattern(s) appeared during observation",
                detail=(
                    "New queries detected:\n"
                    + "\n".join(
                        f"  [{q.calls} calls] {q.query[:150]}" for q in delta.new_queries[:20]
                    )
                ),
                object_name="(queries)",
                metadata={"new_query_count": len(delta.new_queries)},
            )
        )

    # Check observed queries for replication-relevant patterns
    import re

    for entry in delta.changed_queries[:50]:
        query = entry["query"]
        calls = entry["delta_calls"]

        if re.search(r"TRUNCATE.*CASCADE", query, re.IGNORECASE):
            result.findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name="pgstat_observation",
                    category="monitor",
                    title=f"TRUNCATE CASCADE observed live ({calls} calls)",
                    detail=f"Query: {query[:200]}",
                    object_name="(observed)",
                    remediation="TRUNCATE CASCADE only applies on provider side.",
                )
            )

        if re.search(r"CREATE\s+INDEX\s+CONCURRENTLY", query, re.IGNORECASE):
            result.findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name="pgstat_observation",
                    category="monitor",
                    title=f"CREATE INDEX CONCURRENTLY observed live ({calls} calls)",
                    detail=f"Query: {query[:200]}",
                    object_name="(observed)",
                    remediation="Must be done manually on each node.",
                )
            )

    # Summary
    active = len(delta.changed_queries)
    result.findings.append(
        Finding(
            severity=Severity.INFO,
            check_name="pgstat_observation",
            category="monitor",
            title=f"Observation summary: {active} active query patterns over {delta.duration_seconds:.0f}s",
            detail=(
                f"Observed {active} query patterns with activity. "
                f"New patterns: {len(delta.new_queries)}."
            ),
            object_name="(monitor)",
            metadata={
                "duration": delta.duration_seconds,
                "active_patterns": active,
                "new_patterns": len(delta.new_queries),
            },
        )
    )

    return result


def _build_log_result(analysis: LogAnalysis) -> CheckResult:
    """Convert log analysis into findings."""
    result = CheckResult(
        check_name="log_analysis",
        category="monitor",
        description="PostgreSQL log file analysis",
    )

    if analysis.truncate_cascade:
        result.findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name="log_analysis",
                category="monitor",
                title=f"TRUNCATE CASCADE in logs ({len(analysis.truncate_cascade)} occurrences)",
                detail="\n".join(
                    f"  Line {s.line_number}: {s.statement[:150]}"
                    for s in analysis.truncate_cascade[:10]
                ),
                object_name="(log)",
                remediation="TRUNCATE CASCADE only applies on provider side.",
            )
        )

    if analysis.concurrent_indexes:
        result.findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name="log_analysis",
                category="monitor",
                title=f"CREATE INDEX CONCURRENTLY in logs ({len(analysis.concurrent_indexes)} occurrences)",
                detail="\n".join(
                    f"  Line {s.line_number}: {s.statement[:150]}"
                    for s in analysis.concurrent_indexes[:10]
                ),
                object_name="(log)",
                remediation="Must be done manually on each node.",
            )
        )

    if analysis.ddl_statements:
        result.findings.append(
            Finding(
                severity=Severity.INFO,
                check_name="log_analysis",
                category="monitor",
                title=f"DDL statements in logs ({len(analysis.ddl_statements)} occurrences)",
                detail="\n".join(
                    f"  Line {s.line_number}: {s.statement[:150]}"
                    for s in analysis.ddl_statements[:20]
                ),
                object_name="(log)",
                remediation="DDL must be coordinated across nodes or use Spock DDL replication.",
            )
        )

    if analysis.advisory_locks:
        result.findings.append(
            Finding(
                severity=Severity.INFO,
                check_name="log_analysis",
                category="monitor",
                title=f"Advisory locks in logs ({len(analysis.advisory_locks)} occurrences)",
                detail="Advisory locks are node-local and not replicated.",
                object_name="(log)",
            )
        )

    if analysis.create_temp_table:
        result.findings.append(
            Finding(
                severity=Severity.INFO,
                check_name="log_analysis",
                category="monitor",
                title=f"CREATE TEMP TABLE in logs ({len(analysis.create_temp_table)} occurrences)",
                detail="Temporary tables are session-local and not replicated.",
                object_name="(log)",
            )
        )

    # Summary
    result.findings.append(
        Finding(
            severity=Severity.INFO,
            check_name="log_analysis",
            category="monitor",
            title=f"Log analysis: {analysis.total_statements} statements parsed",
            detail=(
                f"Parsed {analysis.total_statements} statements from log file.\n"
                f"DDL: {len(analysis.ddl_statements)}, "
                f"TRUNCATE CASCADE: {len(analysis.truncate_cascade)}, "
                f"Concurrent indexes: {len(analysis.concurrent_indexes)}, "
                f"Advisory locks: {len(analysis.advisory_locks)}, "
                f"Temp tables: {len(analysis.create_temp_table)}"
            ),
            object_name="(log)",
        )
    )

    return result
