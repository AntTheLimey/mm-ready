"""Audit views, especially materialized views."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ViewsCheck(BaseCheck):
    name = "views_audit"
    category = "functions"
    description = "Views and materialized views — refresh coordination in multi-master"

    def run(self, conn) -> list[Finding]:
        # Materialized views
        mat_query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS view_name,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS size
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'm'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        # Regular views count
        view_query = """
            SELECT count(*)
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'v'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast');
        """
        with conn.cursor() as cur:
            cur.execute(mat_query)
            mat_rows = cur.fetchall()
            cur.execute(view_query)
            view_count = cur.fetchone()[0]

        findings = []

        for schema_name, view_name, size in mat_rows:
            fqn = f"{schema_name}.{view_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Materialized view '{fqn}' (size: {size})",
                    detail=(
                        f"Materialized view '{fqn}' ({size}). Materialized views are not "
                        "replicated — each node maintains its own copy. REFRESH MATERIALIZED VIEW "
                        "must be executed independently on each node, and the underlying data "
                        "may differ between nodes depending on replication lag."
                    ),
                    object_name=fqn,
                    remediation=(
                        "Plan to coordinate REFRESH MATERIALIZED VIEW across all nodes. "
                        "Consider scheduling refreshes rather than triggering them from application code."
                    ),
                    metadata={"size": size},
                )
            )

        if view_count > 0:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Database has {view_count} regular view(s)",
                    detail=(
                        f"Found {view_count} regular (non-materialized) views. Regular views "
                        "are query definitions and don't store data, so they don't need replication. "
                        "However, ensure view definitions are identical on all nodes."
                    ),
                    object_name="(views)",
                    remediation="Ensure view definitions are created identically on all nodes via DDL replication.",
                    metadata={"view_count": view_count},
                )
            )
        return findings
