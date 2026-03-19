"""Integration tests — run a full scan against the Docker test database.

These tests require a PostgreSQL instance with the test schema loaded.

Locally (Docker):

    docker run -d --name mmready-test \
      -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mmready \
      -p 5499:5432 \
      ghcr.io/pgedge/pgedge-postgres:18.1-spock5.0.4-standard-1

In CI, the database runs as a service container on port 5432.
Connection details are read from PG* environment variables with
local defaults (localhost:5499).

Tests are skipped automatically if the database is not reachable.
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from psycopg2.extensions import connection

from mm_ready.models import ScanReport

# Connection defaults — CI sets PG* env vars, local uses Docker on 5499
_DB_HOST = os.environ.get("PGHOST", "localhost")
_DB_PORT = int(os.environ.get("PGPORT", "5499"))
_DB_NAME = os.environ.get("PGDATABASE", "mmready")
_DB_USER = os.environ.get("PGUSER", "postgres")
_DB_PASS = os.environ.get("PGPASSWORD", "postgres")

# Try to connect; skip entire module if unavailable
try:
    from mm_ready.connection import connect

    _conn = connect(
        host=_DB_HOST,
        port=_DB_PORT,
        dbname=_DB_NAME,
        user=_DB_USER,
        password=_DB_PASS,
        dsn=None,
    )
    _conn.close()
    _db_available = True
except Exception:
    _db_available = False

pytestmark = pytest.mark.skipif(
    not _db_available, reason=f"Test database not available on {_DB_HOST}:{_DB_PORT}"
)


@pytest.fixture(scope="module")
def db_conn() -> Generator[connection, None, None]:
    """Provide a module-scoped test database connection for integration tests.

    Yields:
        A live database connection to the Postgres test instance at localhost:5499
        (dbname="mmready", user="postgres"). The connection is closed after the
        consuming tests complete.
    """
    from mm_ready.connection import connect

    conn = connect(
        host=_DB_HOST,
        port=_DB_PORT,
        dbname=_DB_NAME,
        user=_DB_USER,
        password=_DB_PASS,
        dsn=None,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def scan_report(db_conn: connection) -> ScanReport:
    """Run a full database scan using the provided connection and return the scan report.

    Parameters:
        db_conn: Active database connection used to perform the scan.

    Returns:
        scan_report: Object containing scan results and metadata (including findings, checks_total, pg_version, and scan_mode).
    """
    from mm_ready.scanner import run_scan

    return run_scan(db_conn, host=_DB_HOST, port=_DB_PORT, dbname=_DB_NAME)


class TestFullScan:
    """Tests for full scan."""

    def test_check_count(self, scan_report: ScanReport) -> None:
        """Verify check count."""
        assert scan_report.checks_total == 48

    def test_no_errors(self, scan_report: ScanReport) -> None:
        """Verify no errors."""
        errors = [r for r in scan_report.results if r.error]
        assert len(errors) == 0, f"Checks with errors: {[(r.check_name, r.error) for r in errors]}"

    def test_has_findings(self, scan_report: ScanReport) -> None:
        """Verify has findings."""
        assert len(scan_report.findings) > 0

    def test_pg_version_populated(self, scan_report: ScanReport) -> None:
        """Verify pg version populated."""
        assert scan_report.pg_version != ""

    def test_scan_mode(self, scan_report: ScanReport) -> None:
        """Verify scan mode."""
        assert scan_report.scan_mode == "scan"


class TestReporterOutput:
    """Tests for reporter output."""

    def test_json_renders(self, scan_report: ScanReport) -> None:
        """Verify json renders."""
        import json

        from mm_ready.reporters.json_reporter import render

        output = render(scan_report)
        data = json.loads(output)
        assert data["summary"]["total_checks"] == 48

    def test_markdown_renders(self, scan_report: ScanReport) -> None:
        """Verify markdown renders."""
        from mm_ready.reporters.markdown_reporter import render

        output = render(scan_report)
        assert len(output) > 100

    def test_html_renders(self, scan_report: ScanReport) -> None:
        """Verify html renders."""
        from mm_ready.reporters.html_reporter import render

        output = render(scan_report)
        assert "<!DOCTYPE html>" in output or "<!doctype html>" in output.lower()
        assert len(output) > 1000
