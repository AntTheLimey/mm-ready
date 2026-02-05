"""Check for TRUNCATE ... CASCADE and RESTART IDENTITY usage."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TruncateCascadeCheck(BaseCheck):
    name = "truncate_cascade"
    category = "sql_patterns"
    description = "TRUNCATE ... CASCADE and RESTART IDENTITY — replication behaviour caveats"

    def run(self, conn) -> list[Finding]:
        findings = []

        # Check if pg_stat_statements is available
        try:
            with conn.cursor() as cur:
                # Check for TRUNCATE CASCADE
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* 'TRUNCATE.*CASCADE'
                    ORDER BY calls DESC;
                """)
                cascade_rows = cur.fetchall()

                # Check for TRUNCATE RESTART IDENTITY
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* 'TRUNCATE.*RESTART'
                    ORDER BY calls DESC;
                """)
                restart_rows = cur.fetchall()

        except Exception:
            return [
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title="Cannot check TRUNCATE patterns — pg_stat_statements unavailable",
                    detail="pg_stat_statements is not available. Cannot check for TRUNCATE CASCADE or RESTART IDENTITY usage.",
                    object_name="pg_stat_statements",
                    remediation="",
                )
            ]

        for query_text, calls in cascade_rows:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"TRUNCATE CASCADE detected ({calls} call(s))",
                    detail=(
                        f"Query executed {calls} time(s): {query_text[:200]}...\n\n"
                        "TRUNCATE ... CASCADE will only apply the CASCADE option on the "
                        "provider. The subscriber ALWAYS applies TRUNCATE with DROP_RESTRICT "
                        "behavior — this is hardcoded in spock_apply.c:1707. Only the "
                        "explicitly named table(s) will be truncated on subscribers; "
                        "cascaded truncates of referencing tables will NOT propagate.\n\n"
                        "Note: TRUNCATE is replicated through replication sets (not AutoDDL). "
                        "AutoDDL does not handle TRUNCATE — PostgreSQL classifies it as "
                        "LOGSTMT_MISC, not LOGSTMT_DDL."
                    ),
                    object_name="(query)",
                    remediation=(
                        "Explicitly TRUNCATE all related tables rather than relying on CASCADE. "
                        "List every table that CASCADE would affect:\n"
                        "  TRUNCATE parent_table, child_table1, child_table2;"
                    ),
                    metadata={"calls": calls, "query": query_text[:500]},
                )
            )

        for query_text, calls in restart_rows:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"TRUNCATE RESTART IDENTITY detected ({calls} call(s))",
                    detail=(
                        f"Query executed {calls} time(s): {query_text[:200]}...\n\n"
                        "TRUNCATE ... RESTART IDENTITY resets the sequence(s) associated "
                        "with the truncated table. The Spock source code "
                        "(spock_apply_heap.c) passes the restart_seqs flag through to "
                        "ExecuteTruncateGuts(), so this IS replicated — despite Spock "
                        "documentation stating otherwise. However, in multi-master setups "
                        "resetting sequences on all nodes could cause ID collisions unless "
                        "pgEdge Snowflake sequences are in use."
                    ),
                    object_name="(query)",
                    remediation=(
                        "If using standard PostgreSQL sequences, avoid RESTART IDENTITY "
                        "across replicated nodes or switch to pgEdge Snowflake IDs first. "
                        "If already using Snowflake IDs, this pattern is safe."
                    ),
                    metadata={"calls": calls, "query": query_text[:500]},
                )
            )

        return findings
