"""Audit check: report PostgreSQL minor version for cross-node consistency."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PgMinorVersionCheck(BaseCheck):
    name = "pg_minor_version"
    category = "config"
    description = "PostgreSQL minor version — all cluster nodes should match"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_setting('server_version');")
            full_version, server_version = cur.fetchone()

        return [Finding(
            severity=Severity.CONSIDER,
            check_name=self.name,
            category=self.category,
            title=f"PostgreSQL {server_version}",
            detail=(
                f"Server version: {server_version}\n"
                f"Full version string: {full_version}\n\n"
                "All nodes in a Spock cluster should run the same PostgreSQL "
                "minor version. Minor version mismatches can introduce subtle "
                "behavioral differences and complicate troubleshooting. Verify "
                "this version matches all other cluster nodes."
            ),
            object_name="pg_version",
            remediation=(
                "Ensure all cluster nodes are upgraded to the same minor version "
                "during maintenance windows. Apply minor upgrades to all nodes "
                "before resuming normal operation."
            ),
            metadata={"server_version": server_version},
        )]
