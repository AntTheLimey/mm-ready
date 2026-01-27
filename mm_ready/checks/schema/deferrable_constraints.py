"""Check for deferrable unique/PK constraints — Spock skips them for conflict resolution."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class DeferrableConstraintsCheck(BaseCheck):
    name = "deferrable_constraints"
    category = "schema"
    description = "Deferrable unique/PK constraints — silently skipped by Spock conflict resolution"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                con.contype AS constraint_type,
                con.condeferrable AS is_deferrable,
                con.condeferred AS is_deferred
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype IN ('p', 'u')
              AND con.condeferrable = true
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, con.conname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        type_labels = {"p": "PRIMARY KEY", "u": "UNIQUE"}

        findings = []
        for schema_name, table_name, con_name, con_type, is_deferrable, is_deferred in rows:
            fqn = f"{schema_name}.{table_name}"
            con_label = type_labels.get(con_type, con_type)

            severity = Severity.CRITICAL if con_type == "p" else Severity.WARNING

            findings.append(Finding(
                severity=severity,
                check_name=self.name,
                category=self.category,
                title=f"Deferrable {con_label} '{con_name}' on '{fqn}'",
                detail=(
                    f"Table '{fqn}' has a DEFERRABLE {con_label} constraint "
                    f"'{con_name}' (initially {'DEFERRED' if is_deferred else 'IMMEDIATE'}). "
                    "Spock's conflict resolution checks indimmediate on indexes via "
                    "IsIndexUsableForInsertConflict() and silently SKIPS deferrable "
                    "indexes. This means conflicts on this constraint will NOT be "
                    "detected during replication apply, potentially causing "
                    "duplicate key violations or data inconsistencies."
                ),
                object_name=f"{fqn}.{con_name}",
                remediation=(
                    f"If possible, make the constraint non-deferrable:\n"
                    f"  ALTER TABLE {fqn} ALTER CONSTRAINT {con_name} NOT DEFERRABLE;\n"
                    "If deferral is required by the application, be aware that Spock "
                    "will not use this constraint for conflict detection."
                ),
                metadata={
                    "constraint_type": con_label,
                    "initially_deferred": is_deferred,
                },
            ))

        return findings
