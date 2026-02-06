"""Check for CREATE TEMP TABLE in SQL patterns."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TempTableQueriesCheck(BaseCheck):
    name = "temp_table_queries"
    category = "sql_patterns"
    description = "CREATE TEMP TABLE in SQL — session-local, not replicated"

    def run(self, conn) -> list[Finding]:
        """
        Run the temp-table detection check against pg_stat_statements using the provided DB connection.

        This executes a query against pg_stat_statements to find statements that match
        CREATE TEMP/TEMPORARY TABLE. If matching rows are found a single Finding is
        returned describing the detection; otherwise an empty list is returned. On any
        error while querying, the function returns an empty list.

        Parameters:
            conn: A DB connection object that provides a context-managed cursor (i.e., supports `with conn.cursor():`) and `execute`/`fetchall` for running SQL queries.

        Returns:
            list[Finding]: A list containing one Finding when CREATE TEMP TABLE patterns are detected (the Finding uses Severity.INFO and includes a title with the match count, a detail note and up to 10 pattern snippets showing call counts and 150-character query excerpts), or an empty list if no patterns are found or an error occurs.
        """
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
