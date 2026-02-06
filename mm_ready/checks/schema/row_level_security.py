"""Check for row-level security policies — apply worker context implications."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class RowLevelSecurityCheck(BaseCheck):
    name = "row_level_security"
    category = "schema"
    description = "Row-level security policies — apply worker runs as superuser, bypasses RLS"

    def run(self, conn) -> list[Finding]:
        """
        Detect tables that have row-level security (RLS) enabled and produce findings describing each table's RLS configuration.

        For each non-system table with RLS enabled, creates a Finding with the table's fully qualified name, whether RLS is forced, the number of policies, a warning-level message about the Spock apply worker bypassing RLS, and a remediation suggestion.

        Returns:
            list[Finding]: A list of Finding objects — one per table with RLS enabled. Each Finding includes `object_name` (schema.table), `severity` set to WARNING, and `metadata` containing `rls_forced` (bool) and `policy_count` (int).
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                c.relrowsecurity AS rls_enabled,
                c.relforcerowsecurity AS rls_forced,
                (SELECT count(*)
                 FROM pg_catalog.pg_policy p
                 WHERE p.polrelid = c.oid) AS policy_count
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND c.relrowsecurity = true
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, _rls_enabled, rls_forced, policy_count in rows:
            fqn = f"{schema_name}.{table_name}"
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Row-level security on '{fqn}' ({policy_count} policies)",
                    detail=(
                        f"Table '{fqn}' has RLS enabled"
                        f"{' (FORCE)' if rls_forced else ''} with {policy_count} "
                        "policy(ies). The Spock apply worker runs as superuser, which "
                        "bypasses RLS policies by default. This means all replicated "
                        "rows will be applied regardless of RLS policies on the "
                        "subscriber. If RLS is used to partition data visibility per "
                        "node, this will not work as expected."
                    ),
                    object_name=fqn,
                    remediation=(
                        "If RLS is used for tenant isolation or data filtering, ensure "
                        "that the replication design accounts for the apply worker "
                        "bypassing RLS. Consider using replication sets to control which "
                        "data is replicated to which nodes instead."
                    ),
                    metadata={
                        "rls_forced": rls_forced,
                        "policy_count": policy_count,
                    },
                )
            )

        return findings
