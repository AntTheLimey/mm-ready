"""Tests for mm_ready.reporters — JSON, Markdown, HTML rendering."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from conftest import make_finding

from mm_ready.models import CheckResult, ScanReport, Severity
from mm_ready.reporters.html_reporter import render as render_html
from mm_ready.reporters.json_reporter import render as render_json
from mm_ready.reporters.markdown_reporter import render as render_markdown

# -- JSON Reporter ------------------------------------------------------------


class TestJSONReporter:
    def test_valid_json(self, sample_report):
        output = render_json(sample_report)
        data = json.loads(output)
        assert isinstance(data, dict)

    def test_has_required_keys(self, sample_report):
        data = json.loads(render_json(sample_report))
        assert "meta" in data
        assert "summary" in data
        assert "results" in data

    def test_summary_counts(self, sample_report):
        data = json.loads(render_json(sample_report))
        s = data["summary"]
        assert s["critical"] == 1
        assert s["warnings"] == 1
        assert s["consider"] == 1
        assert s["info"] == 1
        assert s["total_checks"] == 6  # 7 total minus 1 skipped

    def test_results_count(self, sample_report):
        data = json.loads(render_json(sample_report))
        assert len(data["results"]) == 7

    def test_finding_fields(self, sample_report):
        data = json.loads(render_json(sample_report))
        # First result is wal_level with a CRITICAL finding
        wal = data["results"][0]
        assert wal["check_name"] == "wal_level"
        finding = wal["findings"][0]
        assert finding["severity"] == "CRITICAL"
        assert finding["title"] == "wal_level is not 'logical'"

    def test_error_reported(self, sample_report):
        data = json.loads(render_json(sample_report))
        hba = next(r for r in data["results"] if r["check_name"] == "hba_config")
        assert "PermissionError" in hba["error"]

    def test_meta_fields(self, sample_report):
        data = json.loads(render_json(sample_report))
        assert data["meta"]["database"] == "testdb"
        assert data["meta"]["pg_version"] == "PostgreSQL 17.0"


# -- Markdown Reporter --------------------------------------------------------


class TestMarkdownReporter:
    def test_contains_header(self, sample_report):
        output = render_markdown(sample_report)
        assert "# mm-ready" in output or "# MM-Ready" in output.upper() or "testdb" in output

    def test_contains_severity_sections(self, sample_report):
        output = render_markdown(sample_report)
        assert "CRITICAL" in output
        assert "WARNING" in output

    def test_contains_finding_titles(self, sample_report):
        output = render_markdown(sample_report)
        assert "wal_level is not 'logical'" in output
        assert "Table missing primary key" in output


# -- HTML Reporter ------------------------------------------------------------


class TestHTMLReporter:
    def test_valid_html_structure(self, sample_report):
        output = render_html(sample_report)
        assert "<!DOCTYPE html>" in output or "<!doctype html>" in output.lower()
        assert "</html>" in output

    def test_contains_severity_badges(self, sample_report):
        output = render_html(sample_report)
        assert "badge-critical" in output
        assert "badge-warning" in output
        assert "badge-consider" in output
        assert "badge-info" in output

    def test_contains_finding_content(self, sample_report):
        output = render_html(sample_report)
        assert (
            "wal_level is not &#x27;logical&#x27;" in output
            or "wal_level is not 'logical'" in output
        )

    def test_todo_section_present(self, sample_report):
        output = render_html(sample_report)
        assert "To Do" in output
        assert "todo-group" in output

    def test_sidebar_present(self, sample_report):
        output = render_html(sample_report)
        assert "sidebar" in output


# -- Verdict Logic (tested via reporters) -------------------------------------


def _make_report_with_severities(*severities: Severity) -> ScanReport:
    """
    Create a minimal ScanReport containing one CheckResult per provided severity.

    Parameters:
        *severities (Severity): One or more Severity values; for each provided severity the report will include a CheckResult named "check_<index>" whose single finding has that severity. The order of severities determines the numeric suffix of the generated check names.

    Returns:
        ScanReport: A ScanReport whose `results` list contains one CheckResult per provided severity, each with a single finding set to the corresponding severity.
    """
    report = ScanReport(
        database="testdb",
        host="localhost",
        port=5432,
        timestamp=datetime(2026, 1, 27, tzinfo=timezone.utc),
        pg_version="PostgreSQL 17.0",
    )
    for i, sev in enumerate(severities):
        report.results.append(
            CheckResult(
                check_name=f"check_{i}",
                category="schema",
                description=f"Check {i}",
                findings=[make_finding(severity=sev, check_name=f"check_{i}")],
            )
        )
    return report


class TestVerdictLogic:
    def test_ready_no_findings(self):
        report = ScanReport(
            database="db",
            host="h",
            port=5432,
            timestamp=datetime(2026, 1, 27, tzinfo=timezone.utc),
        )
        report.results.append(
            CheckResult(
                check_name="x",
                category="c",
                description="d",
            )
        )
        md = render_markdown(report)
        assert "READY" in md
        # Should not say NOT READY or CONDITIONALLY
        assert "NOT READY" not in md
        assert "CONDITIONALLY" not in md

    def test_not_ready_with_critical(self):
        report = _make_report_with_severities(Severity.CRITICAL)
        md = render_markdown(report)
        assert "NOT READY" in md

    def test_conditionally_ready_with_warning(self):
        report = _make_report_with_severities(Severity.WARNING)
        md = render_markdown(report)
        assert "CONDITIONALLY READY" in md

    def test_ready_with_only_consider_and_info(self):
        report = _make_report_with_severities(Severity.CONSIDER, Severity.INFO)
        md = render_markdown(report)
        lines = md.upper()
        assert "NOT READY" not in lines
        assert "CONDITIONALLY" not in lines

    def test_verdict_in_html_too(self):
        report = _make_report_with_severities(Severity.CRITICAL, Severity.WARNING)
        html = render_html(report)
        assert "NOT READY" in html

    def test_verdict_in_json_not_present(self):
        """JSON reporter doesn't include verdict (it's computed client-side)."""
        report = _make_report_with_severities(Severity.CRITICAL)
        data = json.loads(render_json(report))
        # JSON has counts but no verdict field — that's by design
        assert "verdict" not in data.get("summary", {})
