"""Check for foreign key columns without supporting indexes."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MissingFkIndexesCheck(BaseCheck):
    name = "missing_fk_indexes"
    category = "schema"
    description = "Foreign key columns without indexes — slow cascades and lock contention"

    def run(self, conn) -> list[Finding]:
        # Find FK columns on the referencing (child) side that lack a matching index.
        # This is the standard "missing FK index" query adapted for Spock context.
        """
        Locate foreign key constraints whose referencing (child) columns do not have supporting indexes and produce Finding objects describing each missing-index case.

        Parameters:
                conn: A DBAPI-compatible connection to the PostgreSQL database used to query system catalogs.

        Returns:
                list[Finding]: A list of Findings, one per foreign key constraint that lacks an index on its referencing columns. Each Finding includes severity, check name, category, title, detail, remediation SQL, and metadata with the constraint name and column list.
        """
        query = """
            SELECT
                cn.nspname AS schema_name,
                cc.relname AS table_name,
                co.conname AS constraint_name,
                array_agg(a.attname ORDER BY x.ordinality) AS fk_columns
            FROM pg_catalog.pg_constraint co
            JOIN pg_catalog.pg_class cc ON cc.oid = co.conrelid
            JOIN pg_catalog.pg_namespace cn ON cn.oid = cc.relnamespace
            CROSS JOIN LATERAL unnest(co.conkey) WITH ORDINALITY AS x(attnum, ordinality)
            JOIN pg_catalog.pg_attribute a
              ON a.attrelid = co.conrelid AND a.attnum = x.attnum
            WHERE co.contype = 'f'
              AND cn.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_index i
                  WHERE i.indrelid = co.conrelid
                    AND (i.indkey::int2[])[0:array_length(co.conkey, 1)-1]
                        = co.conkey
              )
            GROUP BY cn.nspname, cc.relname, co.conname
            ORDER BY cn.nspname, cc.relname, co.conname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, con_name, fk_cols in rows:
            fqn = f"{schema_name}.{table_name}"
            col_list = ", ".join(fk_cols)
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"No index on FK columns '{fqn}' ({col_list})",
                    detail=(
                        f"Foreign key constraint '{con_name}' on '{fqn}' references "
                        f"columns ({col_list}) that have no supporting index. Without "
                        "an index, DELETE and UPDATE on the referenced (parent) table "
                        "require a sequential scan of the child table while holding a "
                        "lock. In multi-master replication, this causes longer lock "
                        "hold times and increases the likelihood of conflicts."
                    ),
                    object_name=fqn,
                    remediation=(f"Create an index:\n  CREATE INDEX ON {fqn} ({col_list});"),
                    metadata={"constraint": con_name, "columns": fk_cols},
                )
            )

        return findings
