"""Check max_wal_senders configuration for Spock replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MaxWalSendersCheck(BaseCheck):
    name = "max_wal_senders"
    category = "replication"
    description = "Sufficient max_wal_senders for Spock logical replication"

    def run(self, conn) -> list[Finding]:
        """
        Check that max_wal_senders is sufficiently large for Spock logical replication.
        
        When the server setting `max_wal_senders` is less than 10, returns a WARNING Finding that describes the current `max_wal_senders` value, the number of active WAL senders, recommended minimum (10), remediation steps, and metadata (`current` and `active`). Returns an empty list when the setting meets or exceeds 10.
        
        Returns:
            list[Finding]: A list of findings; contains one WARNING Finding when `max_wal_senders < 10`, otherwise an empty list.
        """
        query = """
            SELECT
                current_setting('max_wal_senders')::int AS max_senders,
                (SELECT count(*) FROM pg_stat_replication) AS active_senders;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            max_senders, active_senders = cur.fetchone()

        findings = []

        if max_senders < 10:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"max_wal_senders is {max_senders} (recommend >= 10)",
                    detail=(
                        f"max_wal_senders is set to {max_senders} with {active_senders} "
                        "currently active. Each Spock subscription requires a WAL sender "
                        "process. In a multi-master topology with N nodes, each node needs "
                        "at least N-1 senders plus headroom for initial sync and backups."
                    ),
                    object_name="max_wal_senders",
                    remediation=(
                        "Increase max_wal_senders to at least 10:\n"
                        "  ALTER SYSTEM SET max_wal_senders = 10;\n"
                        "Requires a PostgreSQL restart."
                    ),
                    metadata={
                        "current": max_senders,
                        "active": active_senders,
                    },
                )
            )

        return findings