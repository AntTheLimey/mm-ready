"""Check timezone configuration for commit timestamp consistency across nodes."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TimezoneConfigCheck(BaseCheck):
    name = "timezone_config"
    category = "config"
    description = "Timezone settings — UTC recommended for consistent commit timestamps"

    def run(self, conn) -> list[Finding]:
        """
        Check the server's timezone and log_timezone settings and produce findings recommending UTC when appropriate.

        Parameters:
                conn: A DB connection object providing a cursor() context manager used to execute "SHOW timezone;" and "SHOW log_timezone;".

        Returns:
                list[Finding]: A list containing one or more Findings:
                - A CONSIDER Finding for `timezone` if the server timezone is not "UTC".
                - A CONSIDER Finding for `log_timezone` if the log timezone is not "UTC".
                - An INFO Finding confirming both settings are UTC when both are "UTC".
        """
        with conn.cursor() as cur:
            cur.execute("SHOW timezone;")
            tz = cur.fetchone()[0]

            cur.execute("SHOW log_timezone;")
            log_tz = cur.fetchone()[0]

        findings = []

        if tz != "UTC":
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"timezone = '{tz}' (recommended: UTC)",
                    detail=(
                        f"Server timezone is set to '{tz}'. In multi-master replication, "
                        "Spock's last-update-wins conflict resolution relies on commit "
                        "timestamps (track_commit_timestamp). While PostgreSQL stores "
                        "timestamps in UTC internally, using UTC as the server timezone "
                        "avoids confusion in logs, monitoring, and debugging across nodes "
                        "in different geographic locations."
                    ),
                    object_name="timezone",
                    remediation=(
                        "Set all cluster nodes to UTC:\n"
                        "  ALTER SYSTEM SET timezone = 'UTC';\n"
                        "  SELECT pg_reload_conf();"
                    ),
                    metadata={"current": tz},
                )
            )

        if log_tz != "UTC":
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"log_timezone = '{log_tz}' (recommended: UTC)",
                    detail=(
                        f"Log timezone is '{log_tz}'. Using UTC for log timestamps "
                        "across all nodes makes it easier to correlate events and "
                        "troubleshoot replication issues."
                    ),
                    object_name="log_timezone",
                    remediation=(
                        "  ALTER SYSTEM SET log_timezone = 'UTC';\n  SELECT pg_reload_conf();"
                    ),
                    metadata={"current": log_tz},
                )
            )

        if tz == "UTC" and log_tz == "UTC":
            findings.append(
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="Timezone and log_timezone are both UTC (OK)",
                    detail="Both timezone settings are UTC, which is the recommended configuration for multi-master clusters.",
                    object_name="timezone",
                )
            )

        return findings
