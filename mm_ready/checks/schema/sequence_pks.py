"""Check for primary keys backed by standard sequences (need snowflake migration)."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class SequencePrimaryKeysCheck(BaseCheck):
    name = "sequence_pks"
    category = "schema"
    description = "Primary keys using standard sequences — must migrate to pgEdge snowflake"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(c.relname), a.attname) AS seq_name
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_constraint con ON con.conrelid = c.oid AND con.contype = 'p'
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND (
                  pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(c.relname), a.attname) IS NOT NULL
                  OR a.attidentity != ''
              )
            ORDER BY n.nspname, c.relname, a.attname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, col_name, seq_name in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.CRITICAL,
                    check_name=self.name,
                    category=self.category,
                    title=f"PK column '{fqn}.{col_name}' uses a standard sequence",
                    detail=(
                        f"Primary key column '{col_name}' on table '{fqn}' is backed by "
                        f"sequence '{seq_name or 'identity column'}'. In a multi-master setup, "
                        "standard sequences will produce conflicting values across nodes. "
                        "Must migrate to pgEdge snowflake sequences."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"Convert '{fqn}.{col_name}' to use the pgEdge snowflake extension "
                        "for globally unique ID generation. See: pgEdge snowflake documentation."
                    ),
                    metadata={"column": col_name, "sequence": seq_name},
                )
            )
        return findings
