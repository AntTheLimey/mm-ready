"""Audit all sequences for multi-master migration planning."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class SequenceAuditCheck(BaseCheck):
    name = "sequence_audit"
    category = "sequences"
    description = "All sequences, types, and ownership — need snowflake migration plan"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS sequence_name,
                s.seqtypid::regtype AS data_type,
                s.seqstart AS start_value,
                s.seqincrement AS increment,
                s.seqmin AS min_value,
                s.seqmax AS max_value,
                s.seqcycle AS is_cycle,
                d.refobjid IS NOT NULL AS is_owned,
                CASE WHEN d.refobjid IS NOT NULL THEN
                    (SELECT relname FROM pg_class WHERE oid = d.refobjid)
                ELSE NULL END AS owner_table,
                CASE WHEN d.refobjid IS NOT NULL THEN
                    (SELECT attname FROM pg_attribute
                     WHERE attrelid = d.refobjid AND attnum = d.refobjsubid)
                ELSE NULL END AS owner_column
            FROM pg_catalog.pg_sequence s
            JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_depend d
                ON d.objid = s.seqrelid
                AND d.deptype = 'a'
                AND d.classid = 'pg_class'::regclass
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        if not rows:
            return []

        findings = []
        for row in rows:
            (
                schema_name,
                seq_name,
                data_type,
                start_val,
                increment,
                _min_val,
                _max_val,
                is_cycle,
                is_owned,
                owner_table,
                owner_col,
            ) = row

            fqn = f"{schema_name}.{seq_name}"
            ownership = (
                f"owned by {owner_table}.{owner_col}" if is_owned else "not owned by any column"
            )

            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"Sequence '{fqn}' ({data_type}, {ownership})",
                    detail=(
                        f"Sequence '{fqn}': type={data_type}, start={start_val}, "
                        f"increment={increment}, cycle={'yes' if is_cycle else 'no'}, "
                        f"{ownership}. Standard sequences produce overlapping values in "
                        "multi-master setups. Must migrate to pgEdge snowflake sequences "
                        "or implement another globally-unique ID strategy."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"Migrate sequence '{fqn}' to use pgEdge snowflake for globally "
                        "unique ID generation across all cluster nodes."
                    ),
                    metadata={
                        "data_type": str(data_type),
                        "start": start_val,
                        "increment": increment,
                        "cycle": is_cycle,
                        "owner_table": owner_table,
                        "owner_column": owner_col,
                    },
                )
            )

        return findings
