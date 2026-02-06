"""Check for temporary table definitions (in schemas, not runtime)."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TempTablesCheck(BaseCheck):
    name = "temp_tables"
    category = "schema"
    description = "TEMPORARY tables — session-local, never replicated"

    def run(self, conn) -> list[Finding]:
        # Temp tables are session-scoped and won't appear in pg_class for other sessions.
        # We check for functions/procedures that CREATE TEMP TABLE instead.
        """
        Find functions and procedures whose source contains CREATE TEMP/TEMPORARY TABLE statements.
        
        Searches user-visible schemas (excluding standard system schemas) for functions or procedures whose source code matches a case-insensitive pattern for `CREATE TEMP` / `CREATE TEMPORARY` table and returns a Finding for each match describing the object and recommended review.
        
        Parameters:
            conn: A DBAPI-compatible connection used to execute the inspection query.
        
        Returns:
            findings (list[Finding]): Findings describing each function or procedure that creates temporary tables.
        """
        query = r"""
            SELECT
                n.nspname AS schema_name,
                p.proname AS func_name
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND p.prokind IN ('f', 'p')
              AND p.prosrc ~* 'CREATE\s+(TEMP|TEMPORARY)\s+TABLE'
            ORDER BY n.nspname, p.proname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, func_name in rows:
            fqn = f"{schema_name}.{func_name}"
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Function '{fqn}' creates temporary tables",
                    detail=(
                        f"Function '{fqn}' contains CREATE TEMP/TEMPORARY TABLE statements. "
                        "Temporary tables are session-local and are not replicated. This is "
                        "usually fine, but be aware that temp table data will differ across nodes."
                    ),
                    object_name=fqn,
                    remediation="Review to confirm temp table usage is intentional and node-local.",
                )
            )
        return findings