"""Audit check: verify key Spock GUC settings."""

from __future__ import annotations

from typing import TypedDict

from psycopg2.extensions import connection

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class _GucSpec(TypedDict):
    name: str
    recommended: str
    severity: Severity
    detail: str


class SpockGucsCheck(BaseCheck):
    """Check: Verify key Spock configuration parameters (GUCs)."""

    name = "spock_gucs"
    category = "config"
    description = "Verify key Spock configuration parameters (GUCs)"
    mode = "audit"

    # Key Spock GUCs to check, with expected/recommended values
    GUCS: list[_GucSpec] = [
        {
            "name": "spock.conflict_resolution",
            "recommended": "last_update_wins",
            "severity": Severity.WARNING,
            "detail": (
                "Controls how Spock resolves UPDATE/UPDATE conflicts. "
                "'last_update_wins' uses commit timestamps (requires "
                "track_commit_timestamp=on) to keep the most recent change."
            ),
        },
        {
            "name": "spock.save_resolutions",
            "recommended": "on",
            "severity": Severity.INFO,
            "detail": (
                "When enabled, conflict resolutions are logged to "
                "spock.conflict_history for analysis."
            ),
        },
        {
            "name": "spock.enable_ddl_replication",
            "recommended": "on",
            "severity": Severity.WARNING,
            "detail": (
                "Controls whether DDL statements are automatically captured "
                "and replicated (AutoDDL). When enabled, DDL classified as "
                "LOGSTMT_DDL by PostgreSQL is intercepted and sent to "
                "subscribers. Note: TRUNCATE, VACUUM, and ANALYZE are NOT "
                "captured by AutoDDL regardless of this setting."
            ),
        },
        {
            "name": "spock.include_ddl_repset",
            "recommended": "on",
            "severity": Severity.INFO,
            "detail": (
                "When enabled alongside enable_ddl_replication, tables created "
                "via DDL are automatically added to the appropriate replication "
                "set (default for tables with PKs, default_insert_only otherwise)."
            ),
        },
        {
            "name": "spock.allow_ddl_from_functions",
            "recommended": "on",
            "severity": Severity.INFO,
            "detail": (
                "When enabled, DDL executed inside functions and procedures is "
                "also captured by AutoDDL. Without this, only top-level DDL "
                "statements are replicated."
            ),
        },
    ]

    def run(self, conn: connection) -> list[Finding]:
        """Check configured Spock GUCs and produce a Finding for each setting.

        For every GUC in self.GUCS this method reads the current value and appends one Finding:
        - if the GUC cannot be read, an INFO Finding indicates the GUC is not available;
        - if the current value differs from the recommended value, a Finding with the GUC's configured severity is produced and includes the current/recommended values, detail, remediation, and metadata;
        - if the current value matches the recommendation, an INFO Finding is produced with detail and current value metadata.

        Returns:
            list[Finding]: A list of Finding objects, one per configured GUC.
        """
        findings: list[Finding] = []

        for guc in self.GUCS:
            guc_name = guc["name"]
            guc_recommended = guc["recommended"]
            guc_severity = guc["severity"]
            guc_detail = guc["detail"]

            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_setting(%s);", (guc_name,))
                    row = cur.fetchone()
                    value = str(row[0]) if row else None
            except Exception:
                findings.append(
                    Finding(
                        severity=Severity.INFO,
                        check_name=self.name,
                        category=self.category,
                        title=f"GUC '{guc_name}' not available",
                        detail=(
                            f"Could not read '{guc_name}'. Spock may not be "
                            "loaded in shared_preload_libraries."
                        ),
                        object_name=guc_name,
                    )
                )
                continue

            if value != guc_recommended:
                findings.append(
                    Finding(
                        severity=guc_severity,
                        check_name=self.name,
                        category=self.category,
                        title=f"{guc_name} = '{value}' (recommended: '{guc_recommended}')",
                        detail=f"{guc_detail}\n\nCurrent value: '{value}'.",
                        object_name=guc_name,
                        remediation=(
                            f"Consider setting:\n"
                            f"  ALTER SYSTEM SET {guc_name} = '{guc_recommended}';"
                        ),
                        metadata={"current": value, "recommended": guc_recommended},
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.INFO,
                        check_name=self.name,
                        category=self.category,
                        title=f"{guc_name} = '{value}' (OK)",
                        detail=guc_detail,
                        object_name=guc_name,
                        metadata={"current": value},
                    )
                )

        return findings
