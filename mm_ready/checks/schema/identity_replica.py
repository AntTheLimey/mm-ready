"""Check for tables without PKs that have UPDATE/DELETE activity."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class UpdateDeleteNoPkCheck(BaseCheck):
    name = "tables_update_delete_no_pk"
    category = "schema"
    description = (
        "Tables without primary keys that have UPDATE/DELETE activity — "
        "these operations are silently dropped by Spock"
    )

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                s.n_tup_upd AS updates,
                s.n_tup_del AS deletes,
                s.n_tup_ins AS inserts
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_stat_user_tables s
                ON s.schemaname = n.nspname AND s.relname = c.relname
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_constraint con
                  WHERE con.conrelid = c.oid AND con.contype = 'p'
              )
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, updates, deletes, inserts in rows:
            fqn = f"{schema_name}.{table_name}"

            if updates > 0 or deletes > 0:
                # CRITICAL: this table has UPDATE/DELETE activity but no PK.
                # Spock's output plugin silently drops UPDATE/DELETE for
                # tables in the default_insert_only replication set.
                findings.append(
                    Finding(
                        severity=Severity.CRITICAL,
                        check_name=self.name,
                        category=self.category,
                        title=f"Table '{fqn}' has UPDATE/DELETE activity but no primary key",
                        detail=(
                            f"Table '{fqn}' has no primary key and shows "
                            f"{updates:,} UPDATE(s) and {deletes:,} DELETE(s) "
                            f"(plus {inserts:,} INSERT(s)) since the last stats reset. "
                            "Spock places tables without primary keys into the "
                            "'default_insert_only' replication set, where UPDATE and "
                            "DELETE operations are silently filtered out by the output "
                            "plugin (spock_output_plugin.c). This means changes would "
                            "be LOST on subscriber nodes."
                        ),
                        object_name=fqn,
                        remediation=(
                            f"Add a primary key to '{fqn}' so it can be placed in the "
                            "'default' replication set and replicate all DML operations. "
                            "Note: REPLICA IDENTITY FULL is NOT a substitute — Spock's "
                            "get_replication_identity() returns InvalidOid for FULL "
                            "without a PK."
                        ),
                        metadata={
                            "updates": updates,
                            "deletes": deletes,
                            "inserts": inserts,
                        },
                    )
                )
            elif inserts > 0:
                # INFO: insert-only table without PK — fine for default_insert_only.
                findings.append(
                    Finding(
                        severity=Severity.INFO,
                        check_name=self.name,
                        category=self.category,
                        title=f"Table '{fqn}' is insert-only with no PK (OK for replication)",
                        detail=(
                            f"Table '{fqn}' has no primary key but only shows INSERT "
                            f"activity ({inserts:,} inserts, 0 updates, 0 deletes). "
                            "This table will be placed in the 'default_insert_only' "
                            "replication set, which correctly replicates INSERT and "
                            "TRUNCATE operations."
                        ),
                        object_name=fqn,
                        metadata={"inserts": inserts},
                    )
                )
            # Tables with zero activity are skipped — no findings needed.

        return findings
