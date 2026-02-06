"""Check for UNLOGGED tables — not replicated by Spock."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class UnloggedTablesCheck(BaseCheck):
    name = "unlogged_tables"
    category = "schema"
    description = "UNLOGGED tables — not written to WAL and cannot be replicated"

    def run(self, conn) -> list[Finding]:
        """
        Identify UNLOGGED tables (tables not written to the write-ahead log) outside standard system schemas and produce a Finding for each.

        Parameters:
            conn: A DB connection object that provides a cursor() context manager for executing queries.

        Returns:
            list[Finding]: A list of Finding objects, one per UNLOGGED table found. Each Finding contains the table's fully-qualified name in `object_name`, a warning `severity`, explanatory `detail`, and a `remediation` suggesting converting the table to LOGGED.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relpersistence = 'u'
              AND c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"UNLOGGED table '{fqn}'",
                    detail=(
                        f"Table '{fqn}' is UNLOGGED. Unlogged tables are not written to the "
                        "write-ahead log and therefore cannot be replicated by Spock. Data in "
                        "this table will exist only on the local node."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"If this table needs to be replicated, convert it: "
                        f"ALTER TABLE {fqn} SET LOGGED;"
                    ),
                )
            )
        return findings
