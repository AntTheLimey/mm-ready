"""Check for ENUM types — DDL changes to enums require coordination."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class EnumTypesCheck(BaseCheck):
    name = "enum_types"
    category = "schema"
    description = "ENUM types — DDL changes to enums require multi-node coordination"

    def run(self, conn) -> list[Finding]:
        """
        Identify ENUM types in the database and produce findings that warn that ENUM DDL changes require coordinated application across nodes.
        
        Parameters:
            conn: A DB-API compatible connection object used to execute a query and fetch ENUM type definitions.
        
        Returns:
            list[Finding]: A Finding for each discovered ENUM type containing the fully-qualified type name, label count, a short sample of labels, severity, remediation guidance, and related metadata.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                t.typname AS type_name,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            GROUP BY n.nspname, t.typname
            ORDER BY n.nspname, t.typname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, type_name, labels in rows:
            fqn = f"{schema_name}.{type_name}"
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"ENUM type '{fqn}' ({len(labels)} values)",
                    detail=(
                        f"ENUM type '{fqn}' has {len(labels)} values: "
                        f"{', '.join(labels[:10])}"
                        f"{'...' if len(labels) > 10 else ''}. "
                        "In multi-master replication, ALTER TYPE ... ADD VALUE is a DDL "
                        "change that must be applied on all nodes. Spock can replicate DDL "
                        "through the ddl_sql replication set, but ENUM modifications must "
                        "be coordinated carefully to avoid type mismatches during apply."
                    ),
                    object_name=fqn,
                    remediation=(
                        "Plan ENUM modifications to be applied through Spock's DDL "
                        "replication (spock.replicate_ddl) to ensure all nodes stay in sync. "
                        "Alternatively, consider using a lookup table instead of ENUMs for "
                        "values that change frequently."
                    ),
                    metadata={"label_count": len(labels), "labels": labels[:20]},
                )
            )

        return findings