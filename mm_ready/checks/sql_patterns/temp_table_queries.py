"""Check for CREATE TEMP TABLE in SQL patterns."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TempTableQueriesCheck(BaseCheck):
    name = "temp_table_queries"
    category = "sql_patterns"
    description = "CREATE TEMP TABLE in SQL — session-local, not replicated"

    def run(self, conn) -> list[Finding]:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* 'CREATE\\s+(TEMP|TEMPORARY)\\s+TABLE'
                    ORDER BY calls DESC;
                """)
                rows = cur.fetchall()
        except Exception:
            return []

        findings = []
        if rows:
            findings.append(
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title=f"CREATE TEMP TABLE detected ({len(rows)} pattern(s))",
                    detail=(
                        "Temporary tables are session-local and not replicated. This is "
                        "usually expected behavior, but flagged for awareness.\n\n"
                        "Patterns:\n"
                        + "\n".join(f"  [{r[1]} calls] {r[0][:150]}" for r in rows[:10])
                    ),
                    object_name="(queries)",
                    remediation="",
                )
            )
        return findings
