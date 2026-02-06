"""Check for tables missing primary keys."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PrimaryKeysCheck(BaseCheck):
    name = "primary_keys"
    category = "schema"
    description = "Tables without primary keys — affects Spock replication behaviour"

    def run(self, conn) -> list[Finding]:
        """
        Identify all regular tables that lack a primary key and return a Finding for each.
        
        Parameters:
            conn: A DB-API compatible connection to the PostgreSQL instance used to query system catalogs.
        
        Returns:
            list[Finding]: A list of Finding objects, one per table without a primary key. Each Finding explains how Spock handles tables without primary keys (placed into the default_insert_only replication set where only INSERT and TRUNCATE are replicated) and includes remediation guidance.
        """
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
                  FROM pg_catalog.pg_constraint con
                  WHERE con.conrelid = c.oid
                    AND con.contype = 'p'
              )
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
                    title=f"Table '{fqn}' has no primary key",
                    detail=(
                        f"Table '{fqn}' lacks a primary key. Spock automatically places "
                        "tables without primary keys into the 'default_insert_only' "
                        "replication set. In this set, only INSERT and TRUNCATE operations "
                        "are replicated — UPDATE and DELETE operations are silently filtered "
                        "out by the Spock output plugin and never sent to subscribers."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"Add a primary key to '{fqn}' if UPDATE/DELETE replication is "
                        "needed. If the table is genuinely insert-only (e.g. an event log), "
                        "no action is required — it will replicate correctly in the "
                        "default_insert_only replication set."
                    ),
                )
            )
        return findings