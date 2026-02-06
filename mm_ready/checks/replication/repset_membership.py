"""Audit check: verify replication set membership for all user tables."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class RepsetMembershipCheck(BaseCheck):
    name = "repset_membership"
    category = "replication"
    description = "Verify all user tables are in a Spock replication set"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        # Check if spock schema exists
        """
        Check whether user tables are members of any Spock replication set and report findings.
        
        If the Spock schema is missing, returns a single INFO finding indicating the check was skipped.
        If the query against spock.repset_table fails, returns a single WARNING finding describing the error.
        Otherwise, returns a WARNING finding for each user table that is not a member of any replication set; each finding includes the table's fully qualified name and remediation SQL.
        
        Parameters:
        	conn: A DB-API connection object providing a .cursor() context manager.
        
        Returns:
        	list[Finding]: Findings describing a skipped check, a query failure, or one finding per user table missing from any replication set.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_namespace WHERE nspname = 'spock'
                    );
                """)
                has_spock = cur.fetchone()[0]
        except Exception:
            has_spock = False

        if not has_spock:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="Spock schema not found — skipping repset membership check",
                    detail="The spock schema does not exist in this database.",
                    object_name="spock",
                )
            ]

        # Find user tables not in any replication set
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND NOT EXISTS (
                  SELECT 1
                  FROM spock.repset_table rt
                  WHERE rt.set_reloid = c.oid
              )
            ORDER BY n.nspname, c.relname;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        except Exception as e:
            return [
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title="Could not query spock.repset_table",
                    detail=f"Error querying replication set membership: {e}",
                    object_name="spock.repset_table",
                )
            ]

        findings = []
        for schema_name, table_name in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Table '{fqn}' is not in any replication set",
                    detail=(
                        f"Table '{fqn}' exists but is not a member of any Spock "
                        "replication set. This table will NOT be replicated to "
                        "other nodes. If this is intentional (e.g. node-local "
                        "temp/staging data), no action is needed."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"Add the table to a replication set:\n"
                        f"  SELECT spock.repset_add_table('default', '{fqn}');\n"
                        "Or for insert-only tables:\n"
                        f"  SELECT spock.repset_add_table('default_insert_only', '{fqn}');"
                    ),
                )
            )

        return findings