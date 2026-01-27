"""Check for exclusion constraints — not supported by logical replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ExclusionConstraintsCheck(BaseCheck):
    name = "exclusion_constraints"
    category = "schema"
    description = "Exclusion constraints — not enforceable across Spock nodes"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype = 'x'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, con.conname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, constraint_name in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title=f"Exclusion constraint '{constraint_name}' on '{fqn}'",
                detail=(
                    f"Table '{fqn}' has exclusion constraint '{constraint_name}'. "
                    "Exclusion constraints are evaluated locally on each node. In a "
                    "multi-master topology, two nodes could independently accept rows "
                    "that would violate the exclusion constraint if evaluated globally, "
                    "leading to replication conflicts or data inconsistencies."
                ),
                object_name=f"{fqn}.{constraint_name}",
                remediation=(
                    "Review whether this exclusion constraint can be replaced with "
                    "application-level logic, or ensure that only one node writes data "
                    "that could conflict under this constraint."
                ),
            ))

        return findings
