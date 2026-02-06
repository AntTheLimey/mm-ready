"""Check for large object usage — recommend LOLOR extension."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class LargeObjectsCheck(BaseCheck):
    name = "large_objects"
    category = "schema"
    description = "Large object (LOB) usage — logical decoding does not support them"

    def run(self, conn) -> list[Finding]:
        # Check if any large objects exist
        """
        Detect large-object usage and OID-typed columns that may reference large objects and produce findings about replication issues.
        
        Parameters:
            conn: A DB connection with a cursor() context manager used to run queries.
        
        Returns:
            list[Finding]: A list of Finding objects describing detected large objects and OID columns that may not replicate via logical decoding; returns an empty list if no issues are found.
        """
        query = "SELECT count(*) FROM pg_catalog.pg_largeobject_metadata;"
        with conn.cursor() as cur:
            cur.execute(query)
            lob_count = cur.fetchone()[0]

        findings = []
        if lob_count > 0:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Database contains {lob_count} large object(s)",
                    detail=(
                        f"Found {lob_count} large object(s) in pg_largeobject_metadata. "
                        "PostgreSQL's logical decoding facility does not support decoding "
                        "changes to large objects. These will not be replicated by Spock."
                    ),
                    object_name="pg_largeobject",
                    remediation=(
                        "Migrate large objects to use the LOLOR extension for replication-safe "
                        "large object management, or store binary data in BYTEA columns.\n\n"
                        "To use LOLOR:\n"
                        "  CREATE EXTENSION lolor;\n"
                        "  ALTER SYSTEM SET lolor.node = <unique_node_id>;  -- unique per node, 1 to 2^28\n"
                        "  -- Restart PostgreSQL\n"
                        "  SELECT spock.repset_add_table('default', 'lolor.pg_largeobject');\n"
                        "  SELECT spock.repset_add_table('default', 'lolor.pg_largeobject_metadata');"
                    ),
                    metadata={"lob_count": lob_count},
                )
            )

        # Also check for columns using OID type (commonly used with large objects)
        oid_query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND a.atttypid = 'oid'::regtype
            ORDER BY n.nspname, c.relname, a.attname;
        """
        with conn.cursor() as cur:
            cur.execute(oid_query)
            rows = cur.fetchall()

        for schema_name, table_name, col_name in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"OID column '{fqn}.{col_name}' may reference large objects",
                    detail=(
                        f"Column '{col_name}' on table '{fqn}' uses the OID data type, "
                        "which is commonly used to reference large objects. If used for LOB "
                        "references, these will not replicate through logical decoding."
                    ),
                    object_name=f"{fqn}.{col_name}",
                    remediation=(
                        "If this column references large objects, migrate to LOLOR or "
                        "BYTEA. LOLOR requires lolor.node to be set uniquely per node "
                        "and its tables added to a replication set. "
                        "If the column is used for other purposes, this finding can be ignored."
                    ),
                )
            )
        return findings