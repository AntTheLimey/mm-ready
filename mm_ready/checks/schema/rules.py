"""Check for rules on tables — rules interact poorly with logical replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class RulesCheck(BaseCheck):
    name = "rules"
    category = "schema"
    description = "Rules on tables — can cause unexpected behaviour with logical replication"

    def run(self, conn) -> list[Finding]:
        """
        Detects rules defined on regular tables that may interfere with logical replication and reports findings for each rule.

        Each Finding describes the rule (fully-qualified table and rule name), the affected event (SELECT/UPDATE/INSERT/DELETE when mappable), a severity (WARNING when the rule is an INSTEAD rule, CONSIDER otherwise), a human-readable title and detailed explanation of replication implications, a remediation suggestion, and metadata containing the event and `is_instead` flag.

        Returns:
            list[Finding]: A list of Finding objects, one per detected rule, containing severity, check_name, category, title, detail, object_name, remediation, and metadata (keys: "event", "is_instead").
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                r.rulename AS rule_name,
                r.ev_type AS event_type,
                r.is_instead AS is_instead
            FROM pg_catalog.pg_rewrite r
            JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND r.rulename != '_RETURN'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname, r.rulename;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        event_labels = {"1": "SELECT", "2": "UPDATE", "3": "INSERT", "4": "DELETE"}

        findings = []
        for schema_name, table_name, rule_name, event_type, is_instead in rows:
            fqn = f"{schema_name}.{table_name}"
            event = event_labels.get(str(event_type), str(event_type))

            severity = Severity.WARNING if is_instead else Severity.CONSIDER

            findings.append(
                Finding(
                    severity=severity,
                    check_name=self.name,
                    category=self.category,
                    title=f"{'INSTEAD ' if is_instead else ''}Rule '{rule_name}' on '{fqn}' ({event})",
                    detail=(
                        f"Table '{fqn}' has {'an INSTEAD' if is_instead else 'a'} rule "
                        f"'{rule_name}' on {event} events. "
                        "Rules rewrite queries before execution, which means the WAL "
                        "records the rewritten operations, not the original SQL. On the "
                        "subscriber side, the Spock apply worker replays the row-level "
                        "changes from WAL, and the subscriber's rules will also fire on "
                        "the applied changes — potentially causing double-application or "
                        "unexpected side effects."
                        + (
                            " INSTEAD rules are particularly dangerous as they completely "
                            "replace the original operation."
                            if is_instead
                            else ""
                        )
                    ),
                    object_name=f"{fqn}.{rule_name}",
                    remediation=(
                        "Consider converting rules to triggers (which can be controlled "
                        "via session_replication_role), or disable rules on subscriber "
                        "nodes. Review whether the rule's effect should apply on both "
                        "provider and subscriber."
                    ),
                    metadata={"event": event, "is_instead": is_instead},
                )
            )

        return findings
