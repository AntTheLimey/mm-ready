"""Check track_commit_timestamp is enabled."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TrackCommitTimestampCheck(BaseCheck):
    name = "track_commit_timestamp"
    category = "config"
    description = "track_commit_timestamp must be on for Spock conflict resolution"

    def run(self, conn) -> list[Finding]:
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
