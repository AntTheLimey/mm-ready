"""Check max_replication_slots is sufficient for Spock."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MaxReplicationSlotsCheck(BaseCheck):
    name = "max_replication_slots"
    category = "replication"
    description = "Sufficient replication slots for Spock node connections"

    def run(self, conn) -> list[Finding]:
        with conn.cursor() as cur:
            cur.execute("SHOW max_replication_slots;")
            max_slots = int(cur.fetchone()[0])

            cur.execute("SELECT count(*) FROM pg_catalog.pg_replication_slots;")
            used_slots = cur.fetchone()[0]

        findings = []
        # Spock needs at least 1 slot per peer node. Recommend headroom.
        if max_slots < 10:
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title=f"max_replication_slots = {max_slots} (currently {used_slots} in use)",
                detail=(
                    f"max_replication_slots is set to {max_slots} with {used_slots} currently "
                    "in use. Spock requires at least one replication slot per peer node, plus "
                    "slots for any other logical replication consumers. A multi-master cluster "
                    "with N nodes needs N-1 slots per node at minimum."
                ),
                object_name="max_replication_slots",
                remediation=(
                    "Set max_replication_slots to at least 10 (or more for larger clusters) "
                    "in postgresql.conf. Requires a restart."
                ),
                metadata={"current_value": max_slots, "used": used_slots},
            ))
        return findings
