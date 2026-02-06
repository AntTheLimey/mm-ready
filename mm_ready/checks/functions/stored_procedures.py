"""Audit stored procedures for potential replication issues."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class StoredProceduresCheck(BaseCheck):
    name = "stored_procedures"
    category = "functions"
    description = "Audit stored procedures/functions for write operations and DDL"

    def run(self, conn) -> list[Finding]:
        """
        Audit user-defined functions and procedures for potential write operations and non-replicated side effects.

        Connects to the provided database connection, inspects user-defined functions/procedures in non-system schemas, and scans their source for patterns that indicate write operations or DDL (for example: INSERT, UPDATE, CREATE, DROP, EXECUTE). For each routine that contains such patterns, produces a Finding describing the routine, the detected write patterns, and remediation guidance; if any routines are present, also returns a summary informational Finding with the total count.

        Parameters:
            conn: A DB-API compatible connection object providing a cursor() context manager that supports execute() and fetchall().

        Returns:
            list[Finding]: A list of Finding objects. Includes one Finding per routine that contains potential write/DDL operations (severity = Severity.CONSIDER) with metadata about the routine and matched patterns, and an informational Finding (severity = Severity.INFO) summarizing the total number of audited routines when any routines are found.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                p.proname AS func_name,
                p.prokind AS kind,
                l.lanname AS language,
                p.provolatile AS volatility,
                pg_get_functiondef(p.oid) AS func_def
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_catalog.pg_language l ON l.oid = p.prolang
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND l.lanname IN ('plpgsql', 'sql', 'plpython3u', 'plperl', 'plv8')
            ORDER BY n.nspname, p.proname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        kind_labels = {"f": "function", "p": "procedure", "a": "aggregate", "w": "window"}
        vol_labels = {"i": "IMMUTABLE", "s": "STABLE", "v": "VOLATILE"}

        findings = []
        write_patterns = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "CREATE ",
            "ALTER ",
            "DROP ",
            "EXECUTE ",
            "PERFORM ",
        ]

        for schema_name, func_name, kind, language, volatility, func_def in rows:
            fqn = f"{schema_name}.{func_name}"
            kind_label = kind_labels.get(kind, kind) or kind
            vol_label = vol_labels.get(volatility, volatility) or volatility

            if not func_def:
                continue

            func_upper = func_def.upper()
            found_writes = [p for p in write_patterns if p in func_upper]

            if found_writes:
                findings.append(
                    Finding(
                        severity=Severity.CONSIDER,
                        check_name=self.name,
                        category=self.category,
                        title=f"{kind_label.title()} '{fqn}' ({language}, {vol_label}) contains write operations",
                        detail=(
                            f"{kind_label.title()} '{fqn}' written in {language} ({vol_label}) "
                            f"contains potential write operations: {', '.join(found_writes)}. "
                            "Write operations inside functions/procedures are replicated through "
                            "the WAL (row-level changes), not by replaying the function call. "
                            "However, side effects like DDL, NOTIFY, or external calls are not replicated."
                        ),
                        object_name=fqn,
                        remediation=(
                            "Review this function for side effects that won't replicate: DDL, "
                            "NOTIFY/LISTEN, advisory locks, temp tables, external system calls."
                        ),
                        metadata={
                            "kind": kind_label,
                            "language": language,
                            "volatility": vol_label,
                            "write_patterns": found_writes,
                        },
                    )
                )

        # Summary
        if rows:
            findings.append(
                Finding(
                    severity=Severity.INFO,
                    check_name=self.name,
                    category=self.category,
                    title=f"Found {len(rows)} user-defined function(s)/procedure(s)",
                    detail=f"Audited {len(rows)} functions/procedures across all user schemas.",
                    object_name="(functions)",
                    metadata={"total_count": len(rows)},
                )
            )
        return findings
