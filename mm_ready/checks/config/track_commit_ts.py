"""Check track_commit_timestamp is enabled."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TrackCommitTimestampCheck(BaseCheck):
    name = "track_commit_timestamp"
    category = "config"
    description = "track_commit_timestamp must be on for Spock conflict resolution"

    def run(self, conn) -> list[Finding]:
        """
        Check PostgreSQL's track_commit_timestamp setting and report a Finding if it is not enabled.

        Parameters:
            conn: A DB-API/psycopg2-compatible database connection used to execute the query.

        Returns:
            list[Finding]: An empty list if `track_commit_timestamp` is "on"; otherwise a list containing one `Severity.CRITICAL` Finding that records the current value, explains why the setting is required for Spock conflict resolution, and provides remediation steps.
        """
        with conn.cursor() as cur:
            cur.execute("SHOW track_commit_timestamp;")
            val = cur.fetchone()[0]

        findings = []
        if val != "on":
            findings.append(
                Finding(
                    severity=Severity.CRITICAL,
                    check_name=self.name,
                    category=self.category,
                    title=f"track_commit_timestamp is '{val}' — must be 'on'",
                    detail=(
                        f"track_commit_timestamp = '{val}'. Spock uses commit "
                        "timestamps for last-update-wins conflict resolution. This "
                        "is a PostgreSQL server setting that should be configured "
                        "before installing Spock."
                    ),
                    object_name="track_commit_timestamp",
                    remediation=(
                        "Configure before installing Spock:\n"
                        "  ALTER SYSTEM SET track_commit_timestamp = on;\n"
                        "Then restart PostgreSQL. No Spock installation is needed "
                        "for this change — it is a standard PostgreSQL setting."
                    ),
                    metadata={"current_value": val},
                )
            )
        return findings
