"""Check pg_hba.conf for replication connection entries."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class HbaConfigCheck(BaseCheck):
    name = "hba_config"
    category = "replication"
    description = "pg_hba.conf must allow replication connections between nodes"

    def run(self, conn) -> list[Finding]:
        # pg_hba_file_rules is available in PG >= 15
        query = """
            SELECT
                line_number, type, database, user_name, address, netmask, auth_method
            FROM pg_catalog.pg_hba_file_rules
            ORDER BY line_number;
        """
        findings = []
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        except Exception:
            findings.append(Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title="Could not read pg_hba_file_rules",
                detail=(
                    "Unable to query pg_hba_file_rules. This view requires superuser "
                    "or pg_read_all_settings privilege, and is available in PostgreSQL 15+."
                ),
                object_name="pg_hba.conf",
                remediation="Manually verify pg_hba.conf allows replication connections.",
            ))
            return findings

        # Check for replication database entries
        replication_entries = [
            r for r in rows if r[2] and "replication" in (r[2] if isinstance(r[2], list) else [r[2]])
        ]

        if not replication_entries:
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title="No replication entries found in pg_hba.conf",
                detail=(
                    "No pg_hba.conf rules were found granting access to the 'replication' "
                    "database. Spock requires replication connections between nodes."
                ),
                object_name="pg_hba.conf",
                remediation=(
                    "Add replication entries to pg_hba.conf, e.g.:\n"
                    "host replication spock_user 0.0.0.0/0 scram-sha-256"
                ),
            ))
        else:
            findings.append(Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Found {len(replication_entries)} replication entry/entries in pg_hba.conf",
                detail=(
                    f"pg_hba.conf has {len(replication_entries)} replication access rule(s). "
                    "Verify these allow connections from all Spock peer nodes."
                ),
                object_name="pg_hba.conf",
                remediation="Ensure all peer node IPs are covered by replication rules.",
                metadata={"entry_count": len(replication_entries)},
            ))
        return findings
