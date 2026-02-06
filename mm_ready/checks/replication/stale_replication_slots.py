"""Audit check: detect stale or inactive replication slots retaining WAL."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class StaleReplicationSlotsCheck(BaseCheck):
    name = "stale_replication_slots"
    category = "replication"
    description = "Inactive replication slots — retaining WAL and risk filling disk"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        """
        Detects inactive PostgreSQL replication slots that are retaining WAL and returns findings for each.
        
        Each returned Finding represents an inactive slot that prevents WAL cleanup and includes severity based on retained WAL:
        - Severity.CRITICAL for > 1024 MB retained
        - Severity.WARNING for > 100 MB retained
        - Severity.CONSIDER otherwise
        
        The Finding's metadata contains: `slot_type`, `wal_retained_mb` (rounded), `restart_lsn`, and `confirmed_flush_lsn`. Titles, details, object_name, and remediation SQL are provided to identify and address slots that should be dropped or for which the subscriber should be restarted.
        
        Returns:
            list[Finding]: A list of findings for inactive replication slots that are retaining WAL.
        """
        query = """
            SELECT
                slot_name,
                slot_type,
                active,
                restart_lsn,
                confirmed_flush_lsn,
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS wal_retained_bytes
            FROM pg_catalog.pg_replication_slots
            ORDER BY wal_retained_bytes DESC NULLS LAST;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for slot_name, slot_type, active, restart_lsn, flush_lsn, wal_bytes in rows:
            if active:
                continue

            # Inactive slot — flag based on WAL retained
            wal_mb = (wal_bytes or 0) / (1024 * 1024)

            if wal_mb > 1024:
                severity = Severity.CRITICAL
            elif wal_mb > 100:
                severity = Severity.WARNING
            else:
                severity = Severity.CONSIDER

            findings.append(
                Finding(
                    severity=severity,
                    check_name=self.name,
                    category=self.category,
                    title=f"Inactive replication slot '{slot_name}' retaining {wal_mb:.0f} MB of WAL",
                    detail=(
                        f"Replication slot '{slot_name}' ({slot_type}) is inactive "
                        f"and preventing WAL cleanup. restart_lsn={restart_lsn}, "
                        f"confirmed_flush_lsn={flush_lsn}. "
                        f"Retained WAL: {wal_mb:.1f} MB.\n\n"
                        "Inactive slots retain WAL segments indefinitely, which can "
                        "fill the disk. This typically indicates a subscriber that is "
                        "down, unreachable, or has been removed without cleaning up "
                        "its slot."
                    ),
                    object_name=slot_name,
                    remediation=(
                        f"If the subscriber is permanently gone, drop the slot:\n"
                        f"  SELECT pg_drop_replication_slot('{slot_name}');\n"
                        "If the subscriber is temporarily down, restart it to resume "
                        "consuming WAL. Monitor disk space in the meantime."
                    ),
                    metadata={
                        "slot_type": slot_type,
                        "wal_retained_mb": round(wal_mb, 1),
                        "restart_lsn": str(restart_lsn),
                        "confirmed_flush_lsn": str(flush_lsn),
                    },
                )
            )

        return findings