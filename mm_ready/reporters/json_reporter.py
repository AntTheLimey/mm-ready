"""JSON report renderer."""

from __future__ import annotations

import json

from mm_ready.models import ScanReport


def render(report: ScanReport) -> str:
    """Render a ScanReport as a JSON string."""
    data = {
        "meta": {
            "tool": "mm-ready",
            "version": "0.1.0",
            "timestamp": report.timestamp.isoformat(),
            "database": report.database,
            "host": report.host,
            "port": report.port,
            "pg_version": report.pg_version,
            "spock_target": report.spock_target,
        },
        "summary": {
            "total_checks": report.checks_total,
            "checks_passed": report.checks_passed,
            "critical": report.critical_count,
            "warnings": report.warning_count,
            "consider": report.consider_count,
            "info": report.info_count,
        },
        "results": [],
    }

    for result in report.results:
        entry = {
            "check_name": result.check_name,
            "category": result.category,
            "description": result.description,
            "passed": len(result.findings) == 0 and not result.error,
            "skipped": result.skipped,
            "error": result.error,
        }
        if result.skipped:
            entry["skip_reason"] = result.skip_reason
        entry["findings"] = [
            {
                "severity": f.severity.value,
                "title": f.title,
                "detail": f.detail,
                "object_name": f.object_name,
                "remediation": f.remediation,
                "metadata": f.metadata,
            }
            for f in result.findings
        ]
        data["results"].append(entry)

    return json.dumps(data, indent=2, default=str)
