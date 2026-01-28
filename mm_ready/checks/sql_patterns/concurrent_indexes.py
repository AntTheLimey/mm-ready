"""Check for CREATE INDEX CONCURRENTLY usage — must be done manually per node."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ConcurrentIndexesCheck(BaseCheck):
    name = "concurrent_indexes"
    category = "sql_patterns"
    description = "CREATE INDEX CONCURRENTLY — must be created manually on each node"

    def run(self, conn) -> list[Finding]:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* 'CREATE\\s+INDEX\\s+CONCURRENTLY'
                    ORDER BY calls DESC;
                """)
                rows = cur.fetchall()
        except Exception:
            return []

        findings = []
        if rows:
            findings.append(Finding(
                severity=Severity.WARNING,
                check_name=self.name,
                category=self.category,
                title=f"CREATE INDEX CONCURRENTLY detected ({len(rows)} pattern(s))",
                detail=(
                    "CREATE INDEX CONCURRENTLY statements were found in SQL history. "
                    "Concurrent indexes must be created by hand on each node in a "
                    "Spock cluster — they cannot be replicated via DDL replication.\n\n"
                    "Patterns found:\n" +
                    "\n".join(f"  [{r[1]} calls] {r[0][:150]}" for r in rows[:10])
                ),
                object_name="(queries)",
                remediation=(
                    "Plan to execute CREATE INDEX CONCURRENTLY manually on each node. "
                    "Do not rely on DDL replication for these operations."
                ),
                metadata={"pattern_count": len(rows)},
            ))
        return findings
