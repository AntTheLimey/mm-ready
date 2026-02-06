"""Check for volatile column defaults that may produce different values per node."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ColumnDefaultsCheck(BaseCheck):
    name = "column_defaults"
    category = "schema"
    description = "Volatile column defaults (now(), random(), etc.) — may differ across nodes"

    # Patterns that indicate volatile defaults
    VOLATILE_PATTERNS = [
        "now()",
        "current_timestamp",
        "current_date",
        "current_time",
        "clock_timestamp()",
        "statement_timestamp()",
        "transaction_timestamp()",
        "timeofday()",
        "random()",
        "gen_random_uuid()",
        "uuid_generate_",
        "pg_current_xact_id()",
    ]

    def run(self, conn) -> list[Finding]:
        """
        Scan the connected PostgreSQL database for columns that have volatile default expressions and return findings for each match.

        This method queries the system catalogs for regular table columns with explicit default expressions, ignores columns without defaults and defaults using sequence `nextval(...)`, and detects defaults that match known volatile patterns (for example: now(), random(), gen_random_uuid(), uuid_generate_*). For each matching column it produces a Finding describing the potentially divergent default behavior across nodes.

        Returns:
            list[Finding]: A list of Findings for columns with volatile defaults. Each Finding uses Severity.CONSIDER and includes the original default expression in `metadata["default_expr"]`.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                pg_get_expr(d.adbin, d.adrelid) AS default_expr
            FROM pg_catalog.pg_attrdef d
            JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            JOIN pg_catalog.pg_class c ON c.oid = d.adrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND NOT a.attisdropped
              AND a.attgenerated = ''
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, a.attname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, col_name, default_expr in rows:
            if not default_expr:
                continue
            expr_lower = default_expr.lower()

            # Skip nextval — handled by sequence_pks check
            if "nextval(" in expr_lower:
                continue

            matched = any(p in expr_lower for p in self.VOLATILE_PATTERNS)
            if not matched:
                continue

            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Volatile default on '{fqn}.{col_name}'",
                    detail=(
                        f"Column '{col_name}' on table '{fqn}' has a volatile default: "
                        f"{default_expr}. In multi-master replication, if a row is inserted "
                        "without specifying this column, each node could compute a different "
                        "default value. However, Spock replicates the actual inserted value, "
                        "so this is only an issue if the same row is independently inserted "
                        "on multiple nodes."
                    ),
                    object_name=f"{fqn}.{col_name}",
                    remediation=(
                        "Ensure the application always provides an explicit value for this column, "
                        "or accept that conflict resolution may be needed for concurrent inserts."
                    ),
                    metadata={"default_expr": default_expr},
                )
            )
        return findings
