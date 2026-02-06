"""Check for advisory lock usage — node-local only."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class AdvisoryLocksCheck(BaseCheck):
    name = "advisory_locks"
    category = "sql_patterns"
    description = "Advisory lock usage — locks are node-local, not replicated"

    def run(self, conn) -> list[Finding]:
        """
        Search pg_stat_statements for queries that call PostgreSQL advisory lock functions and produce Findings for each detected usage.
        
        Parameters:
            conn: A DB-API compatible database connection with a working cursor() method.
        
        Returns:
            list[Finding]: A list of Finding objects describing detected advisory lock usage. Returns an empty list if the statistics query cannot be executed.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* 'pg_advisory_lock|pg_try_advisory_lock'
                    ORDER BY calls DESC;
                """)
                rows = cur.fetchall()
        except Exception:
            return []

        findings = []
        for query_text, calls in rows:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Advisory lock usage detected ({calls} call(s))",
                    detail=(
                        f"Query: {query_text[:200]}\n\n"
                        "Advisory locks are node-local in PostgreSQL. They are not replicated "
                        "and provide no cross-node coordination. If your application uses advisory "
                        "locks for mutual exclusion, this will not work across a multi-master cluster."
                    ),
                    object_name="(query)",
                    remediation=(
                        "If advisory locks are used for application-level coordination, "
                        "implement a distributed locking mechanism instead."
                    ),
                    metadata={"calls": calls},
                )
            )
        return findings