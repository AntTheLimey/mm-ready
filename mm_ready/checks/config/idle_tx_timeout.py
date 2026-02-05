"""Check idle-in-transaction session timeout configuration."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class IdleTransactionTimeoutCheck(BaseCheck):
    name = "idle_transaction_timeout"
    category = "config"
    description = (
        "Idle-in-transaction timeout — long idle transactions block VACUUM and cause bloat"
    )

    def run(self, conn) -> list[Finding]:
        findings = []

        with conn.cursor() as cur:
            cur.execute("SHOW idle_in_transaction_session_timeout;")
            idle_tx_timeout = cur.fetchone()[0]

            # idle_session_timeout added in PG14
            try:
                cur.execute("SHOW idle_session_timeout;")
                idle_session_timeout = cur.fetchone()[0]
            except Exception:
                idle_session_timeout = None

        if idle_tx_timeout == "0":
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title="idle_in_transaction_session_timeout is not set",
                    detail=(
                        "idle_in_transaction_session_timeout is disabled (0). "
                        "Connections that remain idle in an open transaction hold "
                        "transaction IDs (XIDs) that prevent VACUUM from reclaiming "
                        "dead tuples, leading to table bloat. In replication "
                        "environments this is amplified because bloat on any node "
                        "affects replication performance."
                    ),
                    object_name="idle_in_transaction_session_timeout",
                    remediation=(
                        "Set a reasonable timeout (e.g. 5 minutes):\n"
                        "  ALTER SYSTEM SET idle_in_transaction_session_timeout = '5min';\n"
                        "  SELECT pg_reload_conf();"
                    ),
                    metadata={"current": idle_tx_timeout},
                )
            )

        if idle_session_timeout is not None and idle_session_timeout == "0":
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title="idle_session_timeout is not set",
                    detail=(
                        "idle_session_timeout is disabled (0). Idle connections "
                        "consume backend slots and shared memory. In a multi-master "
                        "cluster, connection pool exhaustion on any node can cause "
                        "replication apply workers to stall."
                    ),
                    object_name="idle_session_timeout",
                    remediation=(
                        "Consider setting a session timeout for non-interactive connections:\n"
                        "  ALTER SYSTEM SET idle_session_timeout = '30min';\n"
                        "  SELECT pg_reload_conf();"
                    ),
                    metadata={"current": idle_session_timeout},
                )
            )

        return findings
