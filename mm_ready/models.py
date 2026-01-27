"""Data models for findings and check results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class Severity(enum.Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    CONSIDER = "CONSIDER"
    INFO = "INFO"

    def __lt__(self, other):
        order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.CONSIDER: 2, Severity.INFO: 3}
        return order[self] < order[other]


@dataclass
class Finding:
    severity: Severity
    check_name: str
    category: str
    title: str
    detail: str
    object_name: str = ""
    remediation: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class CheckResult:
    check_name: str
    category: str
    description: str
    findings: list[Finding] = field(default_factory=list)
    error: str | None = None
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class ScanReport:
    database: str
    host: str
    port: int
    timestamp: datetime
    results: list[CheckResult] = field(default_factory=list)
    pg_version: str = ""
    spock_target: str = "5.0"
    scan_mode: str = "scan"

    @property
    def findings(self) -> list[Finding]:
        all_findings = []
        for r in self.results:
            all_findings.extend(r.findings)
        return all_findings

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.WARNING)

    @property
    def consider_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.CONSIDER)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.INFO)

    @property
    def checks_passed(self) -> int:
        return sum(1 for r in self.results if not r.findings and not r.error and not r.skipped)

    @property
    def checks_total(self) -> int:
        return len(self.results)
