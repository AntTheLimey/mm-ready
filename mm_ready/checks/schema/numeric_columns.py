"""Check for numeric SUM/COUNT columns that may be Delta-Apply candidates."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class NumericColumnsCheck(BaseCheck):
    name = "numeric_columns"
    category = "schema"
    description = "Numeric columns that may be Delta-Apply candidates (counters, balances, etc.)"

    # Column names that suggest accumulator/counter patterns
    SUSPECT_PATTERNS = [
        "count",
        "total",
        "sum",
        "balance",
        "quantity",
        "qty",
        "amount",
        "tally",
        "counter",
        "num_",
        "cnt",
        "running_",
        "cumulative",
        "aggregate",
        "accrued",
        "inventory",
    ]

    def run(self, conn) -> list[Finding]:
        """
        Scan the database catalog for numeric columns whose names suggest they are counters or accumulators, and produce findings about their suitability for Delta-Apply.

        Parameters:
            conn: A DB-API connection used to query the database catalog for table and column metadata.

        Returns:
            findings (list[Finding]): A list of Finding objects describing columns that match suspect name patterns. Each finding indicates severity (nullable columns produce a WARNING; NOT NULL columns produce a CONSIDER), includes a descriptive title and detail, and provides remediation guidance and metadata (column name, data type, nullable).
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                format_type(a.atttypid, a.atttypmod) AS data_type,
                a.attnotnull AS is_not_null
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
              AND a.atttypid IN (
                  'integer'::regtype, 'bigint'::regtype, 'smallint'::regtype,
                  'numeric'::regtype, 'real'::regtype, 'double precision'::regtype
              )
            ORDER BY n.nspname, c.relname, a.attname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, col_name, data_type, is_not_null in rows:
            col_lower = col_name.lower()
            matched = any(p in col_lower for p in self.SUSPECT_PATTERNS)
            if not matched:
                continue

            fqn = f"{schema_name}.{table_name}"

            if not is_not_null:
                # Delta-apply requires NOT NULL (spock_apply_heap.c:613-627)
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=self.name,
                        category=self.category,
                        title=f"Delta-Apply candidate '{fqn}.{col_name}' allows NULL",
                        detail=(
                            f"Column '{col_name}' on table '{fqn}' is numeric ({data_type}) "
                            "and its name suggests it may be an accumulator or counter. "
                            "If configured for Delta-Apply in Spock, the column MUST have a "
                            "NOT NULL constraint. The Spock apply worker "
                            "(spock_apply_heap.c:613-627) checks this and will reject "
                            "delta-apply on nullable columns."
                        ),
                        object_name=f"{fqn}.{col_name}",
                        remediation=(
                            f"If this column will use Delta-Apply, add a NOT NULL constraint:\n"
                            f"  ALTER TABLE {fqn} ALTER COLUMN {col_name} SET NOT NULL;\n"
                            "Ensure existing rows have no NULL values first."
                        ),
                        metadata={"column": col_name, "data_type": data_type, "nullable": True},
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.CONSIDER,
                        check_name=self.name,
                        category=self.category,
                        title=f"Potential Delta-Apply column: '{fqn}.{col_name}' ({data_type})",
                        detail=(
                            f"Column '{col_name}' on table '{fqn}' is numeric ({data_type}) "
                            "and its name suggests it may be an accumulator or counter. In "
                            "multi-master replication, concurrent updates to such columns can "
                            "cause conflicts. Delta-Apply can resolve this by applying the "
                            "delta (change) rather than the absolute value. This column has a "
                            "NOT NULL constraint, so it meets the Delta-Apply prerequisite."
                        ),
                        object_name=f"{fqn}.{col_name}",
                        remediation=(
                            "Investigate whether this column receives concurrent "
                            "increment/decrement updates from multiple nodes. If so, "
                            "configure it for Delta-Apply in Spock."
                        ),
                        metadata={"column": col_name, "data_type": data_type, "nullable": False},
                    )
                )
        return findings
