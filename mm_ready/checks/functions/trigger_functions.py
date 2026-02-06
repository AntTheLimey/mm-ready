"""Check triggers that may conflict with replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TriggerFunctionsCheck(BaseCheck):
    name = "trigger_functions"
    category = "functions"
    description = "Triggers — ENABLE REPLICA and ENABLE ALWAYS both fire during Spock apply"

    def run(self, conn) -> list[Finding]:
        """
        Identify triggers that may conflict with replication and produce a Finding for each discovered trigger.

        Queries PostgreSQL system catalogs for non-internal triggers (excluding pg_catalog, information_schema, spock, pg_toast) and evaluates each trigger's enabled mode to determine potential replication-related concerns.

        Returns:
            list[Finding]: One Finding per trigger containing severity, check_name, category, title, detail, object_name, remediation (non-empty for warnings), and metadata with keys `"timing"`, `"event"`, `"function"`, and `"enabled"`.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                t.tgname AS trigger_name,
                CASE t.tgtype & 66
                    WHEN 2 THEN 'BEFORE'
                    WHEN 64 THEN 'INSTEAD OF'
                    ELSE 'AFTER'
                END AS timing,
                CASE
                    WHEN t.tgtype & 4 > 0 THEN 'INSERT'
                    WHEN t.tgtype & 8 > 0 THEN 'DELETE'
                    WHEN t.tgtype & 16 > 0 THEN 'UPDATE'
                    WHEN t.tgtype & 32 > 0 THEN 'TRUNCATE'
                    ELSE 'UNKNOWN'
                END AS event,
                pn.nspname || '.' || p.proname AS func_name,
                t.tgenabled AS enabled
            FROM pg_catalog.pg_trigger t
            JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
            JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
            WHERE NOT t.tgisinternal
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, t.tgname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        enabled_labels = {
            "O": "ORIGIN (fires on non-replica sessions)",
            "D": "DISABLED",
            "R": "REPLICA (fires during replica apply)",
            "A": "ALWAYS (fires in all sessions)",
        }

        findings = []
        for schema_name, table_name, trig_name, timing, event, func_name, enabled in rows:
            fqn = f"{schema_name}.{table_name}"
            enabled_label = enabled_labels.get(enabled, enabled)

            # Spock apply workers run with session_replication_role='replica'
            # (confirmed: spock_apply.c:3742). Both ENABLE REPLICA and ENABLE ALWAYS
            # triggers fire during apply. ORIGIN-mode triggers do NOT fire during apply.
            if enabled == "A":
                severity = Severity.WARNING
                concern = (
                    "This trigger fires ALWAYS — it WILL fire on subscriber nodes when "
                    "Spock applies replicated changes. The trigger function will execute "
                    "on both the originating node and all subscriber nodes, which may "
                    "cause duplicate side effects or conflicts."
                )
            elif enabled == "R":
                severity = Severity.WARNING
                concern = (
                    "This trigger fires in REPLICA mode — it WILL fire on subscriber nodes "
                    "when Spock applies replicated changes (Spock apply workers run with "
                    "session_replication_role='replica'). Review the trigger function for "
                    "side effects that should not occur during replication apply."
                )
            elif enabled == "O":
                severity = Severity.INFO
                concern = (
                    "This trigger fires on ORIGIN only (default). It will NOT fire when "
                    "Spock applies replicated changes on subscriber nodes."
                )
            elif enabled == "D":
                severity = Severity.INFO
                concern = "This trigger is DISABLED."
            else:
                severity = Severity.INFO
                concern = f"Trigger enabled mode: {enabled_label}."

            findings.append(
                Finding(
                    severity=severity,
                    check_name=self.name,
                    category=self.category,
                    title=f"Trigger '{trig_name}' on '{fqn}' ({timing} {event}, {enabled_label})",
                    detail=f"Trigger '{trig_name}' calls {func_name}. {concern}",
                    object_name=f"{fqn}.{trig_name}",
                    remediation=(
                        "For most triggers, ORIGIN mode (default 'O') is correct — it only "
                        "fires on the node where the write originates. Use ENABLE REPLICA or "
                        "ENABLE ALWAYS only when the trigger must also fire during replication apply."
                    )
                    if severity != Severity.INFO
                    else "",
                    metadata={
                        "timing": timing,
                        "event": event,
                        "function": func_name,
                        "enabled": enabled,
                    },
                )
            )
        return findings
