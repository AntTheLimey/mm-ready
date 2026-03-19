"""Tests for mm_ready.models — Severity, Finding, CheckResult, ScanReport."""

from __future__ import annotations

from datetime import datetime, timezone

from conftest import make_finding

from mm_ready.models import CheckResult, Finding, ScanReport, Severity

# -- Severity ordering -------------------------------------------------------


class TestSeverityOrdering:
    """Tests for severity ordering."""

    def test_critical_less_than_warning(self) -> None:
        """Verify critical less than warning."""
        assert Severity.CRITICAL < Severity.WARNING

    def test_warning_less_than_consider(self) -> None:
        """Verify warning less than consider."""
        assert Severity.WARNING < Severity.CONSIDER

    def test_consider_less_than_info(self) -> None:
        """Verify consider less than info."""
        assert Severity.CONSIDER < Severity.INFO

    def test_critical_less_than_info(self) -> None:
        """Verify critical less than info."""
        assert Severity.CRITICAL < Severity.INFO

    def test_same_severity_not_less(self) -> None:
        """Verify same severity not less."""
        assert not (Severity.WARNING < Severity.WARNING)

    def test_info_not_less_than_critical(self) -> None:
        """Verify info not less than critical."""
        assert not (Severity.INFO < Severity.CRITICAL)

    def test_sorted_order(self) -> None:
        """Verify sorted order."""
        severities = [Severity.INFO, Severity.CRITICAL, Severity.CONSIDER, Severity.WARNING]
        assert sorted(severities) == [
            Severity.CRITICAL,
            Severity.WARNING,
            Severity.CONSIDER,
            Severity.INFO,
        ]


# -- Finding defaults ---------------------------------------------------------


class TestFinding:
    """Tests for finding."""

    def test_defaults(self) -> None:
        """Verify defaults."""
        f = Finding(
            severity=Severity.WARNING,
            check_name="test",
            category="schema",
            title="title",
            detail="detail",
        )
        assert f.object_name == ""
        assert f.remediation == ""
        assert f.metadata == {}

    def test_metadata_independent(self) -> None:
        """Each Finding gets its own metadata dict."""
        f1 = make_finding()
        f2 = make_finding()
        f1.metadata["key"] = "value"
        assert "key" not in f2.metadata


# -- ScanReport properties ----------------------------------------------------


class TestScanReportEmpty:
    """Tests for scan report empty."""

    def test_empty_counts(self, empty_report: ScanReport) -> None:
        """Verify empty counts."""
        assert empty_report.critical_count == 0
        assert empty_report.warning_count == 0
        assert empty_report.consider_count == 0
        assert empty_report.info_count == 0
        assert empty_report.checks_total == 0
        assert empty_report.checks_passed == 0
        assert empty_report.findings == []


class TestScanReportCounts:
    """Tests for scan report counts."""

    def test_critical_count(self, sample_report: ScanReport) -> None:
        """Verify critical count."""
        assert sample_report.critical_count == 1

    def test_warning_count(self, sample_report: ScanReport) -> None:
        """Verify warning count."""
        assert sample_report.warning_count == 1

    def test_consider_count(self, sample_report: ScanReport) -> None:
        """Verify consider count."""
        assert sample_report.consider_count == 1

    def test_info_count(self, sample_report: ScanReport) -> None:
        """Verify info count."""
        assert sample_report.info_count == 1

    def test_checks_total(self, sample_report: ScanReport) -> None:
        """Verify checks total."""
        # 7 total results minus 1 skipped = 6 that actually ran
        assert sample_report.checks_total == 6

    def test_checks_skipped(self, sample_report: ScanReport) -> None:
        """Verify checks skipped."""
        assert sample_report.checks_skipped == 1

    def test_checks_passed(self, sample_report: ScanReport) -> None:
        """Verify checks passed."""
        # Only exclusion_constraints has no findings, no error, not skipped
        assert sample_report.checks_passed == 1

    def test_findings_flattened(self, sample_report: ScanReport) -> None:
        """Verify findings flattened."""
        assert len(sample_report.findings) == 4

    def test_checks_passed_excludes_errored(self) -> None:
        """Verify checks passed excludes errored."""
        report = ScanReport(
            database="db",
            host="h",
            port=5432,
            timestamp=datetime.now(timezone.utc),
        )
        report.results.append(
            CheckResult(
                check_name="x",
                category="c",
                description="d",
                error="something failed",
            )
        )
        assert report.checks_passed == 0

    def test_checks_passed_excludes_skipped(self) -> None:
        """Verify checks passed excludes skipped."""
        report = ScanReport(
            database="db",
            host="h",
            port=5432,
            timestamp=datetime.now(timezone.utc),
        )
        report.results.append(
            CheckResult(
                check_name="x",
                category="c",
                description="d",
                skipped=True,
            )
        )
        assert report.checks_passed == 0
