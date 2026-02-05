"""Check sequence data types — smallint/integer sequences may overflow in multi-master."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class SequenceDataTypesCheck(BaseCheck):
    name = "sequence_data_types"
    category = "sequences"
    description = "Sequence data types — smallint/integer may overflow faster in multi-master"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS seq_name,
                s.seqtypid::regtype::text AS data_type,
                s.seqmax AS max_value,
                s.seqstart AS start_value,
                s.seqincrement AS increment
            FROM pg_catalog.pg_sequence s
            JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, seq_name, data_type, max_value, _start_value, increment in rows:
            fqn = f"{schema_name}.{seq_name}"

            if data_type in ("smallint", "integer"):
                type_max = 32767 if data_type == "smallint" else 2147483647
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=self.name,
                        category=self.category,
                        title=f"Sequence '{fqn}' uses {data_type} (max {type_max:,})",
                        detail=(
                            f"Sequence '{fqn}' is defined as {data_type} with max value "
                            f"{max_value:,}. In a multi-master setup with pgEdge Snowflake "
                            "sequences, the ID space is partitioned across nodes and includes "
                            "a node identifier component. Smaller integer types can exhaust "
                            "their range much faster. Consider upgrading to bigint."
                        ),
                        object_name=fqn,
                        remediation=(
                            "Alter the column and sequence to use bigint:\n"
                            "  ALTER TABLE ... ALTER COLUMN ... TYPE bigint;\n"
                            "This allows room for Snowflake-style globally unique IDs."
                        ),
                        metadata={
                            "data_type": data_type,
                            "max_value": max_value,
                            "increment": increment,
                        },
                    )
                )

        return findings
