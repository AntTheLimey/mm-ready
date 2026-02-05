"""Check if LOLOR extension is needed and properly configured for large object replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class LolorCheck(BaseCheck):
    name = "lolor_check"
    category = "extensions"
    description = "LOLOR extension — required for replicating large objects"

    def run(self, conn) -> list[Finding]:
        findings = []

        # Check if large objects exist or OID columns are present
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_catalog.pg_largeobject_metadata;")
            lob_count = cur.fetchone()[0]

            cur.execute("""
                SELECT count(*)
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
                  AND a.atttypid = 'oid'::regtype;
            """)
            oid_col_count = cur.fetchone()[0]

        has_lo_usage = lob_count > 0 or oid_col_count > 0
        if not has_lo_usage:
            return findings

        # Check if LOLOR is installed
        with conn.cursor() as cur:
            cur.execute("""
                SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'lolor';
            """)
            installed = cur.fetchone()

        if not installed:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title="Large objects detected but LOLOR extension is not installed",
                    detail=(
                        f"Found {lob_count} large object(s) and {oid_col_count} OID-type "
                        "column(s). PostgreSQL's logical decoding does not support large "
                        "objects. The LOLOR (Large Object Logical Replication) extension "
                        "is required to replicate large objects with Spock."
                    ),
                    object_name="lolor",
                    remediation=(
                        "Install and configure the LOLOR extension:\n"
                        "  CREATE EXTENSION lolor;\n"
                        "  ALTER SYSTEM SET lolor.node = <unique_node_id>;\n"
                        "  -- Restart PostgreSQL\n"
                        "Each node must have a unique lolor.node value (1 to 2^28). "
                        "Then add lolor tables to the replication set:\n"
                        "  SELECT spock.repset_add_table('default', 'lolor.pg_largeobject');\n"
                        "  SELECT spock.repset_add_table('default', 'lolor.pg_largeobject_metadata');"
                    ),
                    metadata={"lob_count": lob_count, "oid_col_count": oid_col_count},
                )
            )
            return findings

        # LOLOR installed — check lolor.node configuration
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('lolor.node');")
                node_val = cur.fetchone()[0]
        except Exception:
            node_val = None

        if not node_val or node_val == "0":
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title="LOLOR installed but lolor.node is not configured",
                    detail=(
                        f"LOLOR extension v{installed[0]} is installed, but lolor.node "
                        "is not set (or set to 0). Each node must have a unique lolor.node "
                        "value for large object replication to work correctly."
                    ),
                    object_name="lolor.node",
                    remediation=(
                        "Set a unique node identifier:\n"
                        "  ALTER SYSTEM SET lolor.node = <unique_id>;\n"
                        "  -- Restart PostgreSQL\n"
                        "The value must be unique across all nodes (1 to 2^28)."
                    ),
                )
            )
        else:
            findings.append(
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title=f"LOLOR extension installed (v{installed[0]}, node={node_val})",
                    detail=(
                        f"LOLOR is installed and lolor.node is set to {node_val}. "
                        "Ensure this value is unique across all cluster nodes and that "
                        "lolor.pg_largeobject and lolor.pg_largeobject_metadata are "
                        "members of a replication set."
                    ),
                    object_name="lolor",
                    metadata={"version": installed[0], "node": node_val},
                )
            )

        return findings
