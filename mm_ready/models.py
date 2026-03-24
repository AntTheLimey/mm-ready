"""Data models for findings and check results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class Severity(enum.Enum):
    """Ordered severity levels for replication readiness findings."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    CONSIDER = "CONSIDER"
    INFO = "INFO"

    def __lt__(self, other: object) -> bool:
        """Compare severity ordering (CRITICAL < WARNING < CONSIDER < INFO)."""
        if not isinstance(other, Severity):
            return NotImplemented
        order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.CONSIDER: 2, Severity.INFO: 3}
        return order[self] < order[other]


@dataclass
class Finding:
    """A single issue discovered by a check, with severity and remediation."""

    severity: Severity
    check_name: str
    category: str
    title: str
    detail: str
    object_name: str = ""
    remediation: str = ""
    metadata: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())


@dataclass
class CheckResult:
    """Result of running a single check, containing findings or an error."""

    check_name: str
    category: str
    description: str
    findings: list[Finding] = field(default_factory=lambda: list[Finding]())
    error: str | None = None
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class ScanReport:
    """Aggregated report from a scan, audit, analyze, or monitor run."""

    database: str
    host: str
    port: int
    timestamp: datetime
    results: list[CheckResult] = field(default_factory=lambda: list[CheckResult]())
    pg_version: str = ""
    spock_target: str = "5.0"
    scan_mode: str = "scan"

    @property
    def findings(self) -> list[Finding]:
        """Return all findings flattened from every check result."""
        all_findings: list[Finding] = []
        for r in self.results:
            all_findings.extend(r.findings)
        return all_findings

    @property
    def critical_count(self) -> int:
        """Return the number of CRITICAL findings."""
        return sum(1 for f in self.findings if f.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        """Return the number of WARNING findings."""
        return sum(1 for f in self.findings if f.severity == Severity.WARNING)

    @property
    def consider_count(self) -> int:
        """Return the number of CONSIDER findings."""
        return sum(1 for f in self.findings if f.severity == Severity.CONSIDER)

    @property
    def info_count(self) -> int:
        """Return the number of INFO findings."""
        return sum(1 for f in self.findings if f.severity == Severity.INFO)

    @property
    def checks_passed(self) -> int:
        """Return the number of checks that ran with no findings or errors."""
        return sum(1 for r in self.results if not r.findings and not r.error and not r.skipped)

    @property
    def checks_total(self) -> int:
        """Returns the number of checks that actually ran (excludes skipped)."""
        return sum(1 for r in self.results if not r.skipped)

    @property
    def checks_skipped(self) -> int:
        """Returns the number of skipped checks."""
        return sum(1 for r in self.results if r.skipped)
