"""Audit check: review Spock exception log for apply errors."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ExceptionLogCheck(BaseCheck):
    name = "exception_log"
    category = "replication"
    description = "Review Spock exception log for replication apply errors"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        # Check if spock schema and exception tables exist
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_tables
                        WHERE schemaname = 'spock'
                          AND tablename = 'exception_log'
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
                    title="No spock.exception_log table found",
                    detail=(
                        "The spock.exception_log table does not exist. This is "
                        "normal if Spock is not installed or exception logging is "
                        "not configured."
                    ),
                    object_name="spock.exception_log",
                )
            ]

        # Get exception summary
        query = """
            SELECT
                remote_origin AS origin,
                table_name,
                error_message,
                count(*) AS error_count,
                max(exception_time) AS last_error
            FROM spock.exception_log
            GROUP BY remote_origin, table_name, error_message
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
                    title="Could not query spock.exception_log",
                    detail=f"Error querying exception log: {e}",
                    object_name="spock.exception_log",
                )
            ]

        if not rows:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="No replication exceptions found",
                    detail="The spock.exception_log table contains no records.",
                    object_name="spock.exception_log",
                )
            ]

        findings = []
        total_errors = sum(r[3] for r in rows)

        findings.append(
            Finding(
                severity=Severity.CRITICAL if total_errors > 0 else Severity.INFO,
                check_name=self.name,
                category=self.category,
                title=f"{total_errors:,} total replication exception(s) recorded",
                detail=(
                    f"The exception log shows {total_errors:,} total apply errors. "
                    "These represent rows that could not be applied on this node. "
                    "Each exception means data divergence between nodes."
                ),
                object_name="spock.exception_log",
                metadata={"total_errors": total_errors},
            )
        )

        for origin, table_name, error_msg, count, last_error in rows:
            findings.append(
                Finding(
                    severity=Severity.CRITICAL,
                    check_name=self.name,
                    category=self.category,
                    title=f"{count:,} exception(s) on '{table_name}' from origin {origin}",
                    detail=(
                        f"Table '{table_name}' has {count:,} apply exception(s) "
                        f"from origin {origin}. Error: {error_msg[:300]}. "
                        f"Last occurrence: {last_error}."
                    ),
                    object_name=table_name,
                    remediation=(
                        "Review the exception_log_detail table for full row data. "
                        "Resolve the underlying issue and re-apply or manually fix "
                        "the affected rows."
                    ),
                    metadata={
                        "origin": str(origin),
                        "error": error_msg[:500],
                        "count": count,
                        "last_error": str(last_error),
                    },
                )
            )

        return findings
