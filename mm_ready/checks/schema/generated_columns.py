"""Check for generated (computed/stored) columns."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class GeneratedColumnsCheck(BaseCheck):
    name = "generated_columns"
    category = "schema"
    description = "Generated/stored columns — replication behavior differences"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                a.attgenerated AS gen_type,
                pg_get_expr(d.adbin, d.adrelid) AS expression
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attgenerated != ''
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, a.attname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, col_name, gen_type, expression in rows:
            fqn = f"{schema_name}.{table_name}"
            gen_label = "STORED" if gen_type == "s" else "VIRTUAL"
            findings.append(Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Generated column '{fqn}.{col_name}' ({gen_label})",
                detail=(
                    f"Column '{col_name}' on table '{fqn}' is a {gen_label} generated column "
                    f"with expression: {expression}. Generated columns are recomputed on the "
                    "subscriber side. If the expression depends on functions or data that "
                    "differs across nodes, values may diverge."
                ),
                object_name=f"{fqn}.{col_name}",
                remediation=(
                    "Verify the generation expression produces identical results on all nodes. "
                    "Avoid expressions that depend on volatile functions or node-local state."
                ),
                metadata={"gen_type": gen_label, "expression": expression},
            ))
        return findings
