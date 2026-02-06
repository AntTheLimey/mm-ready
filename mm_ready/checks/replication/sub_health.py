"""Audit check: verify Spock subscription health."""

import logging

import psycopg2

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity

logger = logging.getLogger(__name__)


class SubscriptionHealthCheck(BaseCheck):
    name = "subscription_health"
    category = "replication"
    description = "Check health of Spock subscriptions"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        # Check if spock schema exists
        """
        Assess Spock subscription and replication-slot health for the connected database node.

        This method checks for the presence of the `spock` schema, enumerates entries in `spock.subscription`, and inspects the corresponding replication slots to produce findings about disabled subscriptions, inactive replication slots, query failures, or informational states (no spock schema or no subscriptions).

        Parameters:
            conn (psycopg2.extensions.connection): Database connection to the PostgreSQL node being audited.

        Returns:
            list[Finding]: A list of findings describing detected issues or informational results. Possible findings include:
              - INFO when the `spock` schema is absent or no subscriptions are configured;
              - WARNING when subscriptions cannot be queried or a replication slot is inactive;
              - CRITICAL when a subscription is disabled.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_namespace WHERE nspname = 'spock'
                    );
                """)
                has_spock = cur.fetchone()[0]
        except Exception:
            has_spock = False

        if not has_spock:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="Spock schema not found — skipping subscription health check",
                    detail="The spock schema does not exist in this database.",
                    object_name="spock",
                )
            ]

        # Query subscription status
        query = """
            SELECT
                sub_name,
                sub_enabled,
                sub_slot_name,
                sub_replication_sets,
                sub_forward_origins
            FROM spock.subscription
            ORDER BY sub_name;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        except Exception as e:
            return [
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title="Could not query spock.subscription",
                    detail=f"Error querying subscriptions: {e}",
                    object_name="spock.subscription",
                )
            ]

        if not rows:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="No Spock subscriptions found",
                    detail="This node has no Spock subscriptions configured.",
                    object_name="spock.subscription",
                )
            ]

        findings = []
        for sub_name, sub_enabled, slot_name, _repsets, _fwd_origins in rows:
            if not sub_enabled:
                findings.append(
                    Finding(
                        severity=Severity.CRITICAL,
                        check_name=self.name,
                        category=self.category,
                        title=f"Subscription '{sub_name}' is DISABLED",
                        detail=(
                            f"Subscription '{sub_name}' exists but is disabled. "
                            "This means no data is being replicated from the provider "
                            "node through this subscription."
                        ),
                        object_name=sub_name,
                        remediation=(
                            f"Re-enable the subscription:\n"
                            f"  SELECT spock.alter_subscription_enable('{sub_name}');"
                        ),
                        metadata={"slot_name": slot_name},
                    )
                )

            # Check replication slot health
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT active, restart_lsn, confirmed_flush_lsn
                        FROM pg_replication_slots
                        WHERE slot_name = %s;
                    """,
                        (slot_name,),
                    )
                    slot_row = cur.fetchone()
            except psycopg2.Error as e:
                logger.warning(
                    "Failed to query replication slot '%s': %s",
                    slot_name,
                    e,
                )
                slot_row = None

            if slot_row:
                active, restart_lsn, flush_lsn = slot_row
                if not active:
                    findings.append(
                        Finding(
                            severity=Severity.WARNING,
                            check_name=self.name,
                            category=self.category,
                            title=f"Replication slot '{slot_name}' is inactive",
                            detail=(
                                f"Replication slot '{slot_name}' for subscription "
                                f"'{sub_name}' is not active. This could indicate "
                                "a connection issue with the provider node."
                            ),
                            object_name=slot_name,
                            remediation="Check network connectivity and provider node status.",
                            metadata={"restart_lsn": str(restart_lsn), "flush_lsn": str(flush_lsn)},
                        )
                    )

        return findings
