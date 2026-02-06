"""Check for LISTEN/NOTIFY usage — not replicated by logical replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class NotifyListenCheck(BaseCheck):
    name = "notify_listen"
    category = "schema"
    description = "LISTEN/NOTIFY usage — notifications are not replicated by Spock"

    def run(self, conn) -> list[Finding]:
        """
        Detects usage of NOTIFY or pg_notify in database functions and recent statements and reports findings about their replication implications.
        
        Parameters:
            conn: A DB-API connection to the inspected PostgreSQL database; used to query pg_proc/pg_namespace and pg_stat_statements.
        
        Returns:
            findings (list[Finding]): A list of Finding objects describing:
                - functions (schema.function) that contain NOTIFY/pg_notify (severity: WARNING), and
                - queries recorded in pg_stat_statements that contain NOTIFY/pg_notify (severity: CONSIDER).
        """
        findings = []

        # pg_listening_channels() only shows channels of the current session.
        # Instead, check for NOTIFY in functions and pg_stat_statements.

        # Check functions that use pg_notify or NOTIFY
        query_funcs = """
            SELECT
                n.nspname AS schema_name,
                p.proname AS func_name,
                pg_get_functiondef(p.oid) AS func_def
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND (
                  prosrc ~* 'pg_notify' OR
                  prosrc ~* '\\bNOTIFY\\b'
              )
            ORDER BY n.nspname, p.proname;
        """
        with conn.cursor() as cur:
            cur.execute(query_funcs)
            func_rows = cur.fetchall()

        for schema_name, func_name, _func_def in func_rows:
            fqn = f"{schema_name}.{func_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Function '{fqn}' uses NOTIFY/pg_notify",
                    detail=(
                        f"Function '{fqn}' contains NOTIFY or pg_notify() calls. "
                        "LISTEN/NOTIFY is a PostgreSQL inter-process communication "
                        "mechanism that is NOT replicated by logical replication. "
                        "If application components rely on notifications triggered by "
                        "data changes, those notifications will only fire on the node "
                        "where the change originates — not on subscriber nodes."
                    ),
                    object_name=fqn,
                    remediation=(
                        "If notifications are used as part of the application architecture, "
                        "ensure that listeners connect to all nodes, or implement an "
                        "application-level notification mechanism that works across nodes."
                    ),
                )
            )

        # Check pg_stat_statements for NOTIFY usage
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE query ~* '\\bNOTIFY\\b'
                       OR query ~* 'pg_notify'
                    ORDER BY calls DESC;
                """)
                stmt_rows = cur.fetchall()

            for query_text, calls in stmt_rows:
                findings.append(
                    Finding(
                        severity=Severity.CONSIDER,
                        check_name=self.name,
                        category=self.category,
                        title=f"NOTIFY pattern in queries ({calls} call(s))",
                        detail=(
                            f"Query executed {calls} time(s): {query_text[:200]}...\n\n"
                            "NOTIFY calls are not replicated by Spock. Subscribers will "
                            "not receive these notifications."
                        ),
                        object_name="(query)",
                        metadata={"calls": calls},
                    )
                )
        except Exception:
            pass  # pg_stat_statements not available

        return findings