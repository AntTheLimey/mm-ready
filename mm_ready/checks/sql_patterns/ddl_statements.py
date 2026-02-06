"""Detect DDL statements in tracked SQL for replication awareness."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class DdlStatementsCheck(BaseCheck):
    name = "ddl_statements"
    category = "sql_patterns"
    description = "DDL statements — must use Spock DDL replication or manual coordination"

    DDL_PATTERNS = [
        "CREATE TABLE",
        "ALTER TABLE",
        "DROP TABLE",
        "CREATE INDEX",
        "DROP INDEX",
        "CREATE VIEW",
        "DROP VIEW",
        "ALTER VIEW",
        "CREATE FUNCTION",
        "DROP FUNCTION",
        "ALTER FUNCTION",
        "CREATE PROCEDURE",
        "DROP PROCEDURE",
        "ALTER PROCEDURE",
        "CREATE TRIGGER",
        "DROP TRIGGER",
        "CREATE TYPE",
        "DROP TYPE",
        "ALTER TYPE",
        "CREATE SCHEMA",
        "DROP SCHEMA",
        "CREATE SEQUENCE",
        "ALTER SEQUENCE",
        "DROP SEQUENCE",
    ]

    def run(self, conn) -> list[Finding]:
        """
        Check pg_stat_statements for queries that match known DDL patterns and return findings describing any matches.
        
        Queries pg_stat_statements for up to 50 distinct statements that match the module's DDL_PATTERNS and, if matches are found, returns a single CONSIDER-level Finding that summarizes the number of matches, lists the top patterns with call counts and snippets, and includes remediation guidance. If pg_stat_statements cannot be accessed, returns a single INFO-level Finding indicating it is unavailable.
        
        Parameters:
            conn: A DB connection object supporting context-manager cursors with execute/fetchall.
        
        Returns:
            list[Finding]: An empty list when no DDL patterns are found; otherwise a list containing one Finding describing the detected DDL patterns or the unavailability of pg_stat_statements.
        """
        try:
            with conn.cursor() as cur:
                # Build regex pattern
                pattern = "|".join(self.DDL_PATTERNS)
                cur.execute(
                    """
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* %s
                    ORDER BY calls DESC
                    LIMIT 50;
                """,
                    (pattern,),
                )
                rows = cur.fetchall()
        except Exception:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="Cannot check DDL patterns — pg_stat_statements unavailable",
                    detail="pg_stat_statements is not available.",
                    object_name="pg_stat_statements",
                )
            ]

        findings = []
        if rows:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Found {len(rows)} DDL statement pattern(s) in pg_stat_statements",
                    detail=(
                        "DDL statements are not automatically replicated by default. "
                        "Spock's AutoDDL feature (spock.enable_ddl_replication=on) can "
                        "automatically replicate DDL classified as LOGSTMT_DDL by "
                        "PostgreSQL.\n\n"
                        "AutoDDL does NOT replicate:\n"
                        "  - TRUNCATE (classified as LOGSTMT_MISC, replicated via replication sets)\n"
                        "  - VACUUM, ANALYZE (classified as LOGSTMT_ALL, must run on each node)\n\n"
                        "AutoDDL DOES replicate (when enabled):\n"
                        "  - CREATE/ALTER/DROP TABLE, INDEX, VIEW, FUNCTION, SEQUENCE, etc.\n"
                        "  - CLUSTER, REINDEX (classified as LOGSTMT_DDL)\n\n"
                        "Top DDL patterns:\n"
                        + "\n".join(f"  [{r[1]} calls] {r[0][:120]}" for r in rows[:10])
                    ),
                    object_name="(queries)",
                    remediation=(
                        "Enable Spock AutoDDL for automatic DDL propagation:\n"
                        "  ALTER SYSTEM SET spock.enable_ddl_replication = on;\n"
                        "  ALTER SYSTEM SET spock.include_ddl_repset = on;\n"
                        "Or use spock.replicate_ddl_command() for manual DDL propagation. "
                        "VACUUM and ANALYZE must always be run independently on each node."
                    ),
                    metadata={"ddl_count": len(rows)},
                )
            )
        return findings