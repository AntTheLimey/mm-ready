"""Check for non-default tablespace usage — tablespaces are local to each node."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class TablespaceUsageCheck(BaseCheck):
    name = "tablespace_usage"
    category = "schema"
    description = "Non-default tablespace usage — tablespaces must exist on all nodes"

    def run(self, conn) -> list[Finding]:
        """
        Finds database objects that use non-default (local) tablespaces and returns a Finding for each tablespace with the objects that use it.

        Returns:
            list[Finding]: A list of findings grouped by tablespace. Each Finding describes the tablespace name, the count of objects using it, up to the first 10 example objects in the detail text, and includes metadata with `object_count` and up to 20 `objects`.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                ts.spcname AS tablespace_name,
                c.relkind
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_tablespace ts ON ts.oid = c.reltablespace
            WHERE c.relkind IN ('r', 'i', 'm')
              AND c.reltablespace != 0
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY ts.spcname, n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        if not rows:
            return []

        kind_labels = {"r": "table", "i": "index", "m": "materialized view"}

        # Group by tablespace
        tablespaces: dict[str, list[str]] = {}
        for schema_name, table_name, ts_name, relkind in rows:
            fqn = f"{schema_name}.{table_name}"
            kind = kind_labels.get(relkind, relkind)
            tablespaces.setdefault(ts_name, []).append(f"{fqn} ({kind})")

        findings = []
        for ts_name, objects in tablespaces.items():
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Tablespace '{ts_name}' used by {len(objects)} object(s)",
                    detail=(
                        f"Tablespace '{ts_name}' is used by {len(objects)} object(s): "
                        f"{', '.join(objects[:10])}"
                        f"{'...' if len(objects) > 10 else ''}.\n\n"
                        "Tablespaces are local to each PostgreSQL instance. When setting "
                        "up Spock replication, the same tablespace names must exist on "
                        "all nodes, though they can point to different physical paths."
                    ),
                    object_name=ts_name,
                    remediation=(
                        f"Ensure tablespace '{ts_name}' is created on all Spock nodes "
                        "before initializing replication."
                    ),
                    metadata={"object_count": len(objects), "objects": objects[:20]},
                )
            )

        return findings
