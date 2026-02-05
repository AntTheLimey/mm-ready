"""Check for multiple databases in the instance."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MultipleDatabasesCheck(BaseCheck):
    name = "multiple_databases"
    category = "replication"
    description = "More than one user database in the instance — Spock supports one DB per instance"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT datname
            FROM pg_catalog.pg_database
            WHERE datistemplate = false
              AND datname NOT IN ('postgres')
            ORDER BY datname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        db_names = [r[0] for r in rows]

        findings = []
        if len(db_names) > 1:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Instance has {len(db_names)} user database(s): {', '.join(db_names)}",
                    detail=(
                        f"Found {len(db_names)} non-template databases (excluding 'postgres'): "
                        f"{', '.join(db_names)}. pgEdge Spock officially supports one database "
                        "per PostgreSQL instance. Multiple databases may require separate "
                        "instances for multi-master replication."
                    ),
                    object_name="(instance)",
                    remediation=(
                        "Plan to separate databases into individual PostgreSQL instances, "
                        "one per database, for Spock multi-master replication."
                    ),
                    metadata={"databases": db_names},
                )
            )
        return findings
