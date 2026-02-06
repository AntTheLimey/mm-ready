"""Check that wal_level is set to 'logical'."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class WalLevelCheck(BaseCheck):
    name = "wal_level"
    category = "replication"
    description = "wal_level must be 'logical' for Spock replication"

    def run(self, conn) -> list[Finding]:
        """
        Check the PostgreSQL server's wal_level and produce a Finding if it is not set to 'logical'.
        
        Queries the server with "SHOW wal_level;" and, if the value is not "logical", returns a single `Finding` with severity CRITICAL that includes the current value, explanatory detail, remediation steps, and metadata.
        
        Returns:
            list[Finding]: Empty when wal_level is "logical"; otherwise a list containing one `Finding` describing the issue and how to remediate it.
        """
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level;")
            wal_level = cur.fetchone()[0]

        findings = []
        if wal_level != "logical":
            findings.append(
                Finding(
                    severity=Severity.CRITICAL,
                    check_name=self.name,
                    category=self.category,
                    title=f"wal_level is '{wal_level}' — must be 'logical'",
                    detail=(
                        f"Current wal_level is '{wal_level}'. Spock requires "
                        "wal_level = 'logical' to enable logical decoding of the "
                        "write-ahead log. This is a PostgreSQL server setting that "
                        "should be configured before installing Spock."
                    ),
                    object_name="wal_level",
                    remediation=(
                        "Configure before installing Spock:\n"
                        "  ALTER SYSTEM SET wal_level = 'logical';\n"
                        "Then restart PostgreSQL. No Spock installation is needed "
                        "for this change — it is a standard PostgreSQL setting."
                    ),
                    metadata={"current_value": wal_level},
                )
            )
        return findings