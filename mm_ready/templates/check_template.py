"""Template for creating a new mm-ready check.

INSTRUCTIONS:
  1. Copy this file from mm_ready/templates/ into the appropriate category subdirectory:
       checks/schema/       — table/column/constraint structure
       checks/replication/  — WAL, slots, subscriptions, conflict logs
       checks/config/       — postgresql.conf settings
       checks/extensions/   — installed/available extensions
       checks/sql_patterns/ — pg_stat_statements query analysis
       checks/functions/    — stored procedures, triggers, views
       checks/sequences/    — sequence inventory and data types

  2. Rename the file to match your check (e.g. my_check.py).

  3. Rename the class and fill in the four class attributes:
       name        — unique identifier (snake_case)
       category    — must match the subdirectory name
       description — one-line summary shown by `mm-ready list-checks`
       mode        — "scan" (pre-Spock), "audit" (post-Spock), or "both"

  4. Implement the `run()` method. Return an empty list if the check passes.

  5. That's it. The check is auto-discovered at runtime — no registration
     or import changes needed.

GUIDELINES:
  - The database connection is READ-ONLY. Never modify the target database.
  - Use psycopg2 cursor via `with conn.cursor() as cur:`.
  - Exclude system schemas: pg_catalog, information_schema, pg_toast.
  - If your check depends on an optional extension (e.g. pg_stat_statements),
    handle the case where it's unavailable and return an informative Finding
    or an empty list.
  - Choose severity carefully:
      CRITICAL  — will cause data loss or prevent Spock from working
      WARNING   — should be reviewed; may cause issues in production
      CONSIDER  — should be investigated; may need action depending on context
      INFO      — awareness item, no action required
"""

from __future__ import annotations

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MyCustomCheck(BaseCheck):
    # --- Check metadata (required) -----------------------------------------
    name = "my_custom_check"  # unique snake_case identifier — rename when you copy
    category = "schema"  # must match the subdirectory you place this file in
    description = "One-line description of what this check detects"
    mode = "scan"  # "scan", "audit", or "both"

    def run(self, conn) -> list[Finding]:
        # 1. Query the database -----------------------------------------------
        query = """
            SELECT schemaname, tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        # 2. Analyse results and build findings --------------------------------
        findings = []
        for schema, table in rows:
            fqn = f"{schema}.{table}"

            # Example: report a finding per table.
            # Replace this logic with your actual detection.
            findings.append(Finding(
                severity=Severity.INFO,
                check_name=self.name,
                category=self.category,
                title=f"Found table: {fqn}",
                detail=(
                    f"The table '{fqn}' was found. Replace this with a "
                    "meaningful explanation of why this matters for Spock "
                    "multi-master replication."
                ),
                object_name=fqn,
                remediation="Describe the concrete steps to resolve this issue.",
            ))

        # 3. Return findings (empty list = check passed) -----------------------
        return findings
