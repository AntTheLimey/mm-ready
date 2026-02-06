"""Integration tests — run a full scan against the Docker test database.

These tests require the mmready-test Docker container to be running:

    docker run -d --name mmready-test \
      -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mmready \
      -p 5499:5432 \
      ghcr.io/pgedge/pgedge-postgres:18.1-spock5.0.4-standard-1

Tests are skipped automatically if the database is not reachable.
"""

from __future__ import annotations

import pytest

# Try to connect; skip entire module if unavailable
try:
    from mm_ready.connection import connect

    _conn = connect(
        host="localhost",
        port=5499,
        dbname="mmready",
        user="postgres",
        password="postgres",
        dsn=None,
    )
    _conn.close()
    _db_available = True
except Exception:
    _db_available = False

pytestmark = pytest.mark.skipif(
    not _db_available, reason="Test database not available on localhost:5499"
)


@pytest.fixture(scope="module")
def db_conn():
    """
    Provide a module-scoped test database connection for integration tests.

    Yields:
        A live database connection to the Postgres test instance at localhost:5499
        (dbname="mmready", user="postgres"). The connection is closed after the
        consuming tests complete.
    """
    from mm_ready.connection import connect

    conn = connect(
        host="localhost",
        port=5499,
        dbname="mmready",
        user="postgres",
        password="postgres",
        dsn=None,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def scan_report(db_conn):
    """
    Run a full database scan using the provided connection and return the scan report.

    Parameters:
        db_conn: Active database connection used to perform the scan.

    Returns:
        scan_report: Object containing scan results and metadata (including findings, checks_total, pg_version, and scan_mode).
    """
    from mm_ready.scanner import run_scan

    return run_scan(db_conn, host="localhost", port=5499, dbname="mmready")


class TestFullScan:
    def test_check_count(self, scan_report):
        assert scan_report.checks_total == 48

    def test_no_errors(self, scan_report):
        errors = [r for r in scan_report.results if r.error]
        assert len(errors) == 0, f"Checks with errors: {[(r.check_name, r.error) for r in errors]}"

    def test_has_findings(self, scan_report):
        assert len(scan_report.findings) > 0

    def test_pg_version_populated(self, scan_report):
        assert scan_report.pg_version != ""

    def test_scan_mode(self, scan_report):
        assert scan_report.scan_mode == "scan"


class TestReporterOutput:
    def test_json_renders(self, scan_report):
        import json

        from mm_ready.reporters.json_reporter import render

        output = render(scan_report)
        data = json.loads(output)
        assert data["summary"]["total_checks"] == 48

    def test_markdown_renders(self, scan_report):
        from mm_ready.reporters.markdown_reporter import render

        output = render(scan_report)
        assert len(output) > 100

    def test_html_renders(self, scan_report):
        from mm_ready.reporters.html_reporter import render

        output = render(scan_report)
        assert "<!DOCTYPE html>" in output or "<!doctype html>" in output.lower()
        assert len(output) > 1000
