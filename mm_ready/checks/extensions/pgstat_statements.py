"""Check pg_stat_statements availability for SQL pattern analysis."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PgStatStatementsCheck(BaseCheck):
    name = "pg_stat_statements_check"
    category = "extensions"
    description = "pg_stat_statements availability for SQL pattern observation"

    def run(self, conn) -> list[Finding]:
        """
        Check for the pg_stat_statements extension and report its availability and queryability.

        If pg_stat_statements is installed and queryable, returns an INFO finding with the extension version and the number of tracked statements. If it is installed but cannot be queried, returns a WARNING finding explaining the access issue. If it is not installed, returns a CONSIDER finding describing limited analysis and steps to install.

        Parameters:
            conn: A live PostgreSQL connection object used to query extension and stats.

        Returns:
            findings (list[Finding]): A list of findings describing the pg_stat_statements status and any relevant metadata or remediation.
        """
        with conn.cursor() as cur:
            cur.execute("""
                SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'pg_stat_statements';
            """)
            installed = cur.fetchone()

        findings = []
        if installed:
            # Check if we can actually query it
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM pg_stat_statements;")
                    stmt_count = cur.fetchone()[0]
                findings.append(
                    Finding(
                        severity=Severity.INFO,
                        check_name=self.name,
                        category=self.category,
                        title=f"pg_stat_statements available ({stmt_count} statements tracked)",
                        detail=(
                            f"pg_stat_statements v{installed[0]} is installed with {stmt_count} "
                            "statements tracked. SQL pattern checks will use this data."
                        ),
                        object_name="pg_stat_statements",
                        metadata={"version": installed[0], "statement_count": stmt_count},
                    )
                )
            except Exception as e:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=self.name,
                        category=self.category,
                        title="pg_stat_statements installed but not queryable",
                        detail=(
                            f"pg_stat_statements is installed but could not be queried: {e}. "
                            "Ensure the current user has access to this view."
                        ),
                        object_name="pg_stat_statements",
                        remediation="Grant access to pg_stat_statements for the scanning user.",
                    )
                )
        else:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title="pg_stat_statements is NOT installed",
                    detail=(
                        "pg_stat_statements is not installed. SQL pattern checks will be limited. "
                        "Installing it enables richer analysis of executed SQL patterns."
                    ),
                    object_name="pg_stat_statements",
                    remediation=(
                        "Install pg_stat_statements:\n"
                        "1. Add to shared_preload_libraries in postgresql.conf\n"
                        "2. Restart PostgreSQL\n"
                        "3. Run: CREATE EXTENSION pg_stat_statements;"
                    ),
                )
            )
        return findings
