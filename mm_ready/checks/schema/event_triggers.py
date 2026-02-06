"""Check for event triggers — fire on DDL events, interact with Spock DDL replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class EventTriggersCheck(BaseCheck):
    name = "event_triggers"
    category = "schema"
    description = "Event triggers — fire on DDL events, may interact with Spock DDL replication"

    def run(self, conn) -> list[Finding]:
        """
        Inspect PostgreSQL event triggers and report findings about their enabled modes with respect to DDL replication.

        Queries pg_event_trigger and produces a Finding for each non-disabled event trigger describing whether it will fire during replication apply and what remediation (if any) is suggested. Each finding's metadata contains the trigger's event name and raw enabled code.

        Parameters:
            conn: A DB connection/cursor provider used to query pg_catalog.pg_event_trigger.

        Returns:
            list[Finding]: A list of findings for each non-disabled event trigger. Each Finding includes severity, title, detail, object_name, remediation, and metadata {"event": <event>, "enabled": <enabled_code>}.
        """
        query = """
            SELECT
                evtname AS trigger_name,
                evtevent AS event,
                evtenabled AS enabled
            FROM pg_catalog.pg_event_trigger
            ORDER BY evtname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        enabled_labels = {
            "O": "origin/local",
            "D": "disabled",
            "R": "replica",
            "A": "always",
        }

        findings = []
        for trigger_name, event, enabled in rows:
            if enabled == "D":
                continue  # Skip disabled triggers

            label = enabled_labels.get(enabled, enabled)

            if enabled == "A":
                # ENABLE ALWAYS — correct for DDL-automation triggers (e.g.
                # auto-adding new tables to replication sets), but risky for
                # triggers with side effects that shouldn't run on every node.
                severity = Severity.CONSIDER
                detail = (
                    f"Event trigger '{trigger_name}' fires on '{event}' events "
                    f"(enabled mode: {label}). Spock's apply worker runs with "
                    "session_replication_role='replica' (spock_apply.c:3742), so "
                    "only ENABLE ALWAYS triggers fire during replication apply.\n\n"
                    "If this trigger is used for DDL automation (e.g. automatically "
                    "adding new tables to replication sets), ENABLE ALWAYS is the "
                    "CORRECT and REQUIRED setting. If this trigger has side effects "
                    "that should only run once (e.g. sending notifications, writing "
                    "audit logs), it should NOT be ENABLE ALWAYS."
                )
                remediation = (
                    "If this trigger automates replication set management, ENABLE "
                    "ALWAYS is correct — no change needed. If it has side effects "
                    "that should not fire during replication apply, change to "
                    f"'origin' mode: ALTER EVENT TRIGGER {trigger_name} ENABLE;"
                )
            elif enabled == "R":
                severity = Severity.WARNING
                detail = (
                    f"Event trigger '{trigger_name}' fires on '{event}' events "
                    f"(enabled mode: {label}). ENABLE REPLICA triggers fire when "
                    "session_replication_role='replica', which is the mode Spock's "
                    "apply worker uses. This trigger WILL fire during replication apply."
                )
                remediation = (
                    "Review whether this trigger should fire during replication "
                    "apply. If not, set to origin mode: ALTER EVENT TRIGGER "
                    f"{trigger_name} ENABLE;"
                )
            else:
                severity = Severity.INFO
                detail = (
                    f"Event trigger '{trigger_name}' fires on '{event}' events "
                    f"(enabled mode: {label}). Origin-mode triggers only fire for "
                    "locally-originated DDL, not replicated DDL. This is the default "
                    "and generally correct setting."
                )
                remediation = ""

            findings.append(
                Finding(
                    severity=severity,
                    check_name=self.name,
                    category=self.category,
                    title=f"Event trigger '{trigger_name}' on {event} (enabled: {label})",
                    detail=detail,
                    object_name=trigger_name,
                    remediation=remediation,
                    metadata={"event": event, "enabled": enabled},
                )
            )

        return findings
