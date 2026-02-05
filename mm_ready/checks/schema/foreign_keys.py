"""Check foreign key relationships for replication ordering awareness."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ForeignKeysCheck(BaseCheck):
    name = "foreign_keys"
    category = "schema"
    description = "Foreign key relationships — replication ordering and cross-node considerations"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                rn.nspname AS ref_schema,
                rc.relname AS ref_table,
                confdeltype AS delete_action,
                confupdtype AS update_action
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE con.contype = 'f'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, con.conname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        if not rows:
            return []

        action_labels = {
            "a": "NO ACTION",
            "r": "RESTRICT",
            "c": "CASCADE",
            "n": "SET NULL",
            "d": "SET DEFAULT",
        }

        # Group by table for a summary finding
        findings = []
        cascade_fks = []
        for schema_name, table_name, con_name, ref_schema, ref_table, del_act, upd_act in rows:
            fqn = f"{schema_name}.{table_name}"
            ref_fqn = f"{ref_schema}.{ref_table}"
            del_label = action_labels.get(del_act, del_act)
            upd_label = action_labels.get(upd_act, upd_act)

            if del_act == "c" or upd_act == "c":
                cascade_fks.append((fqn, con_name, ref_fqn, del_label, upd_label))

        # Report CASCADE FKs specifically — they can cause issues
        for fqn, con_name, ref_fqn, del_label, upd_label in cascade_fks:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"CASCADE foreign key '{con_name}' on '{fqn}'",
                    detail=(
                        f"Foreign key '{con_name}' on '{fqn}' references '{ref_fqn}' with "
                        f"ON DELETE {del_label} / ON UPDATE {upd_label}. CASCADE actions are "
                        "executed locally on each node, meaning the cascaded changes happen "
                        "independently on provider and subscriber, which can lead to conflicts "
                        "in a multi-master setup."
                    ),
                    object_name=fqn,
                    remediation=(
                        "Review CASCADE behavior. In multi-master, consider handling cascades "
                        "in application logic or ensuring operations flow through a single node."
                    ),
                    metadata={"constraint": con_name, "references": ref_fqn},
                )
            )

        # Summary finding about FK count
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Database has {len(rows)} foreign key constraint(s)",
                detail=(
                    f"Found {len(rows)} foreign key constraints. Ensure all referenced tables "
                    "are included in the replication set, and that replication ordering will "
                    "satisfy referential integrity."
                ),
                object_name="(database)",
                remediation="Ensure all FK-related tables are in the same replication set.",
                metadata={"fk_count": len(rows), "cascade_count": len(cascade_fks)},
            )
        )
        return findings
