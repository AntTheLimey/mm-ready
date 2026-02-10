"""Scanner orchestrator — discovers and runs checks, collects results."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

from mm_ready.connection import get_pg_version
from mm_ready.models import CheckResult, ScanReport
from mm_ready.registry import discover_checks


def run_scan(
    conn: Any,
    host: str,
    port: int,
    dbname: str,
    categories: list[str] | None = None,
    mode: str = "scan",
    verbose: bool = False,
    exclude: set[str] | None = None,
    include_only: set[str] | None = None,
) -> ScanReport:
    """Execute all discovered checks against the database.

    Args:
        conn: psycopg2 connection.
        host: Display hostname for the report.
        port: Display port for the report.
        dbname: Database name for the report.
        categories: Optional list of categories to filter checks.
        mode: "scan" for pre-Spock readiness, "audit" for post-Spock audit.
        verbose: Print progress to stderr.
        exclude: Optional set of check names to exclude.
        include_only: Optional set of check names to include (whitelist mode).

    Returns:
        ScanReport with all results.
    """
    mode_label = "Readiness scan" if mode == "scan" else "Spock audit"
    report = ScanReport(
        database=dbname,
        host=host,
        port=port,
        timestamp=datetime.now(timezone.utc),
        pg_version=get_pg_version(conn),
        scan_mode=mode,
    )

    checks = discover_checks(
        categories=categories, mode=mode, exclude=exclude, include_only=include_only
    )
    total = len(checks)

    if verbose:
        print(f"{mode_label}: running {total} checks against {dbname}...", file=sys.stderr)

    for i, check in enumerate(checks, 1):
        if verbose:
            print(
                f"  [{i}/{total}] {check.category}/{check.name}: {check.description}",
                file=sys.stderr,
            )

        result = CheckResult(
            check_name=check.name,
            category=check.category,
            description=check.description,
        )

        try:
            result.findings = check.run(conn)
        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            if verbose:
                print(f"    ERROR: {result.error}", file=sys.stderr)

        report.results.append(result)

    if verbose:
        print(
            f"Done. {report.critical_count} critical, "
            f"{report.warning_count} warnings, "
            f"{report.consider_count} consider, "
            f"{report.info_count} info.",
            file=sys.stderr,
        )

    return report
