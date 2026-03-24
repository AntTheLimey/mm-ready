"""Audit check: report PostgreSQL minor version for cross-node consistency."""

from __future__ import annotations

from psycopg2.extensions import connection

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PgMinorVersionCheck(BaseCheck):
    """Check: PostgreSQL minor version — all cluster nodes should match."""

    name = "pg_minor_version"
    category = "config"
    description = "PostgreSQL minor version — all cluster nodes should match"
    mode = "audit"

    def run(self, conn: connection) -> list[Finding]:
        """Query the connected PostgreSQL server and return a Finding that reports its minor version and full version string.

        Parameters:
            conn: A live database connection used to query the server version.

        Returns:
            list[Finding]: A single-item list containing a Finding that describes the server's reported `server_version`, includes the full version string in the detail, and sets `metadata["server_version"]` to the reported minor version.
        """
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_setting('server_version');")
            row = cur.fetchone()
            full_version = str(row[0]) if row else ""
            server_version = str(row[1]) if row else ""

        return [
            Finding(
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
            )
        ]
