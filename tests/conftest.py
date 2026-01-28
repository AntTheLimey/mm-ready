"""Shared fixtures for mm-ready tests."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from mm_ready.models import CheckResult, Finding, ScanReport, Severity


def make_finding(
    severity: Severity = Severity.INFO,
    check_name: str = "test_check",
    category: str = "schema",
    title: str = "Test finding",
    detail: str = "Test detail",
    **kwargs,
) -> Finding:
    """Factory for creating Finding instances with sensible defaults."""
    return Finding(
        severity=severity,
        check_name=check_name,
        category=category,
        title=title,
        detail=detail,
        **kwargs,
    )


@pytest.fixture
def empty_report() -> ScanReport:
    """ScanReport with no results."""
    return ScanReport(
        database="testdb",
        host="localhost",
        port=5432,
        timestamp=datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc),
        pg_version="PostgreSQL 17.0",
    )


@pytest.fixture
def sample_report() -> ScanReport:
    """ScanReport with a mix of severities, errors, and skipped checks."""
    report = ScanReport(
        database="testdb",
        host="localhost",
        port=5432,
        timestamp=datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc),
        pg_version="PostgreSQL 17.0",
    )

    # Check with CRITICAL finding
    report.results.append(CheckResult(
        check_name="wal_level",
        category="replication",
        description="WAL level check",
        findings=[make_finding(
            severity=Severity.CRITICAL,
            check_name="wal_level",
            category="replication",
            title="wal_level is not 'logical'",
            detail="Current value: replica",
            remediation="ALTER SYSTEM SET wal_level = 'logical';",
        )],
    ))

    # Check with WARNING finding
    report.results.append(CheckResult(
        check_name="primary_keys",
        category="schema",
        description="Primary key check",
        findings=[make_finding(
            severity=Severity.WARNING,
            check_name="primary_keys",
            category="schema",
            title="Table missing primary key",
            detail="public.orders has no PK",
            object_name="public.orders",
            remediation="Add a primary key to public.orders.",
        )],
    ))

    # Check with CONSIDER finding
    report.results.append(CheckResult(
        check_name="enum_types",
        category="schema",
        description="Enum type check",
        findings=[make_finding(
            severity=Severity.CONSIDER,
            check_name="enum_types",
            category="schema",
            title="ENUM type found",
            detail="public.status_type has 3 values",
            object_name="public.status_type",
            remediation="Use Spock DDL replication for enum modifications.",
        )],
    ))

    # Check with INFO finding (no remediation)
    report.results.append(CheckResult(
        check_name="pg_version",
        category="config",
        description="PG version check",
        findings=[make_finding(
            severity=Severity.INFO,
            check_name="pg_version",
            category="config",
            title="PostgreSQL 17.0",
            detail="Supported by Spock 5",
        )],
    ))

    # Passing check (no findings)
    report.results.append(CheckResult(
        check_name="exclusion_constraints",
        category="schema",
        description="Exclusion constraint check",
    ))

    # Errored check
    report.results.append(CheckResult(
        check_name="hba_config",
        category="replication",
        description="HBA config check",
        error="PermissionError: pg_hba_file_rules not accessible",
    ))

    # Skipped check
    report.results.append(CheckResult(
        check_name="pgstat_observation",
        category="monitor",
        description="pg_stat_statements observation",
        skipped=True,
        skip_reason="pg_stat_statements not available",
    ))

    return report
