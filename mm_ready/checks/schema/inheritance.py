"""Check for table inheritance — poorly supported in logical replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class InheritanceCheck(BaseCheck):
    name = "inheritance"
    category = "schema"
    description = "Table inheritance (non-partition) — not well supported in logical replication"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                pn.nspname AS parent_schema,
                pc.relname AS parent_table,
                cn.nspname AS child_schema,
                cc.relname AS child_table
            FROM pg_catalog.pg_inherits i
            JOIN pg_catalog.pg_class pc ON pc.oid = i.inhparent
            JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
            JOIN pg_catalog.pg_class cc ON cc.oid = i.inhrelid
            JOIN pg_catalog.pg_namespace cn ON cn.oid = cc.relnamespace
            WHERE pc.relkind = 'r'  -- exclude partitioned tables (relkind='p')
              AND cc.relkind = 'r'
              AND pn.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY pn.nspname, pc.relname, cn.nspname, cc.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for parent_schema, parent_table, child_schema, child_table in rows:
            parent_fqn = f"{parent_schema}.{parent_table}"
            child_fqn = f"{child_schema}.{child_table}"
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title=f"Table inheritance: '{child_fqn}' inherits from '{parent_fqn}'",
                detail=(
                    f"Table '{child_fqn}' uses traditional table inheritance from "
                    f"'{parent_fqn}'. Logical replication does not replicate through "
                    "inheritance hierarchies — each table is replicated independently. "
                    "Queries against the parent that include child data via inheritance "
                    "may behave differently across nodes."
                ),
                object_name=child_fqn,
                remediation=(
                    "Consider migrating from table inheritance to declarative partitioning "
                    "(if appropriate) or separate standalone tables."
                ),
                metadata={"parent": parent_fqn},
            ))
        return findings
