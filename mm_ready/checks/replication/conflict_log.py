"""Audit check: review Spock conflict log for recent conflicts."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ConflictLogCheck(BaseCheck):
    name = "conflict_log"
    category = "replication"
    description = "Review Spock conflict log for recent replication conflicts"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        # Check if spock schema and conflict history table exist
        """
        Inspect Spock's conflict history and produce Findings describing any recent replication conflicts.

        This method checks for the existence of the spock.conflict_history table, and if present aggregates conflicts by table, conflict type, and resolution (limited to 50 rows). Possible single-entry Findings returned describe a missing table, a query error, or an empty conflict table. When conflicts are found the method returns an aggregate Finding with the total conflict count followed by one Finding per aggregated row with per-table conflict details and metadata.

        Parameters:
            conn: A DB-API compatible connection or cursor context used to execute queries against the PostgreSQL instance.

        Returns:
            findings (list[Finding]): A list of Findings representing the audit results:
                - Single INFO Finding if the spock.conflict_history table is not found.
                - Single WARNING Finding if the conflict query fails.
                - Single INFO Finding if the table exists but contains no records.
                - Otherwise, an aggregate Finding with total_conflicts in metadata followed by one WARNING Finding per aggregated table row containing conflict_type, resolution, count, and last_conflict metadata.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_tables
                        WHERE schemaname = 'spock'
                          AND tablename = 'conflict_history'
                    );
                """)
                has_table = cur.fetchone()[0]
        except Exception:
            has_table = False

        if not has_table:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="No spock.conflict_history table found",
                    detail=(
                        "The spock.conflict_history table does not exist. This is "
                        "normal if Spock is not installed or conflict logging is not "
                        "configured."
                    ),
                    object_name="spock.conflict_history",
                )
            ]

        # Get conflict summary
        query = """
            SELECT
                ch_reloid::regclass::text AS table_name,
                ch_conflict_type AS conflict_type,
                ch_conflict_resolution AS resolution,
                count(*) AS conflict_count,
                max(ch_timestamp) AS last_conflict
            FROM spock.conflict_history
            GROUP BY ch_reloid, ch_conflict_type, ch_conflict_resolution
            ORDER BY count(*) DESC
            LIMIT 50;
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
                    title="Could not query spock.conflict_history",
                    detail=f"Error querying conflict log: {e}",
                    object_name="spock.conflict_history",
                )
            ]

        if not rows:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="No replication conflicts found",
                    detail="The spock.conflict_history table contains no records.",
                    object_name="spock.conflict_history",
                )
            ]

        findings = []
        total_conflicts = sum(r[3] for r in rows)

        findings.append(
            Finding(
                severity=Severity.WARNING if total_conflicts > 0 else Severity.INFO,
                check_name=self.name,
                category=self.category,
                title=f"{total_conflicts:,} total replication conflict(s) recorded",
                detail=(
                    f"The conflict history shows {total_conflicts:,} total conflicts "
                    "across all tables. Review the per-table breakdown below."
                ),
                object_name="spock.conflict_history",
                metadata={"total_conflicts": total_conflicts},
            )
        )

        for table_name, conflict_type, resolution, count, last_conflict in rows:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"{count:,} '{conflict_type}' conflicts on '{table_name}'",
                    detail=(
                        f"Table '{table_name}' has {count:,} '{conflict_type}' conflicts "
                        f"resolved by '{resolution}'. Last conflict: {last_conflict}."
                    ),
                    object_name=table_name,
                    metadata={
                        "conflict_type": conflict_type,
                        "resolution": resolution,
                        "count": count,
                        "last_conflict": str(last_conflict),
                    },
                )
            )

        return findings
